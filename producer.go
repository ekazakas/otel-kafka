package otelkafka

import (
	"context"
	"fmt"
	"github.com/ekazakas/otel-kafka/internal"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
	"go.opentelemetry.io/otel/semconv/v1.34.0/messagingconv"
	"go.opentelemetry.io/otel/trace"
)

type (
	Producer struct {
		*kafka.Producer

		cfg                        otelConfig
		messageCounter             metric.Int64Counter
		messageSizeHistogram       metric.Int64Histogram
		operationDurationHistogram metric.Float64Histogram
	}
)

// NewProducer Returns either new kafka.Producer with given configuration, or
// an error if provided configuration is not valid
func NewProducer(config kafka.ConfigMap, opts ...Option) (*Producer, error) {
	producer, err := kafka.NewProducer(&config)
	if err != nil {
		return nil, err
	}

	return WrapOTELProducer(producer, append(opts, withConfig(config))...)
}

// WrapOTELProducer Decorates provided Producer instance with OTEL features
func WrapOTELProducer(producer *kafka.Producer, opts ...Option) (*Producer, error) {
	cfg := newOTELConfig(opts...)
	meter := cfg.MeterProvider.Meter("kafka_producer")

	messageCounter, err := messagingconv.NewClientSentMessages(meter)
	if err != nil {
		return nil, fmt.Errorf("failed to create produced messages counter metric: %w", err)
	}

	messageSizeHistogram, err := meter.Int64Histogram(
		"messaging.client.sent.message.size",
		metric.WithDescription("Size of messages produced by a producer client."),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create message size metric: %w", err)
	}

	operationDurationHistogram, err := messagingconv.NewClientOperationDuration(
		meter,
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create client operation duration metric: %w", err)
	}

	return &Producer{
		Producer:                   producer,
		cfg:                        cfg,
		messageCounter:             messageCounter.Inst(),
		messageSizeHistogram:       messageSizeHistogram,
		operationDurationHistogram: operationDurationHistogram.Inst(),
	}, nil
}

// Produce calls the underlying Producer.Produce and traces the request.
func (p *Producer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	start := time.Now()

	// If there's a span context in the message, use that as the parent context.
	carrier := NewMessageCarrier(msg)
	ctx := p.cfg.Propagator.Extract(context.Background(), carrier)

	lowCardinalityAttrs := []attribute.KeyValue{
		semconv.MessagingOperationName("produce"),
		semconv.MessagingOperationTypeSend,
		semconv.MessagingSystemKafka,
		semconv.ServerAddress(p.cfg.bootstrapServerHost),
		semconv.MessagingDestinationName(*msg.TopicPartition.Topic),
	}

	highCardinalityAttrs := []attribute.KeyValue{
		semconv.MessagingKafkaMessageKey(string(msg.Key)),
		semconv.MessagingMessageBodySize(internal.GetMessageSize(msg)),
	}
	attrs := append(lowCardinalityAttrs, highCardinalityAttrs...)

	opts := []trace.SpanStartOption{
		trace.WithAttributes(attrs...),
		trace.WithSpanKind(trace.SpanKindProducer),
	}

	ctx, span := p.cfg.tracer.Start(ctx, fmt.Sprintf("%s publish", *msg.TopicPartition.Topic), opts...)
	p.cfg.Propagator.Inject(ctx, carrier)

	tracedDeliveryChan := make(chan kafka.Event)
	tracedDeliveryFunc := func(targetDeliveryChan chan kafka.Event) {
		evt := <-tracedDeliveryChan
		if resMsg, ok := evt.(*kafka.Message); ok {
			if err := resMsg.TopicPartition.Error; err != nil {
				span.RecordError(err)
				span.SetAttributes(semconv.ErrorTypeKey.String("publish_error"))
				span.SetStatus(codes.Error, err.Error())

				lowCardinalityAttrs = append(lowCardinalityAttrs, semconv.ErrorTypeKey.String("publish_error"))
			} else {
				if resMsg.TopicPartition.Partition >= 0 {
					partitionIDAttr := semconv.MessagingDestinationPartitionID(fmt.Sprintf("%d", resMsg.TopicPartition.Partition))

					span.SetAttributes(partitionIDAttr)
					span.SetAttributes(semconv.MessagingKafkaOffset(int(resMsg.TopicPartition.Offset)))

					lowCardinalityAttrs = append(lowCardinalityAttrs, partitionIDAttr)
				}

				span.SetStatus(codes.Ok, "Success")
			}
		}

		p.messageCounter.Add(ctx, 1, metric.WithAttributes(lowCardinalityAttrs...))
		p.messageSizeHistogram.Record(ctx, int64(internal.GetMessageSize(msg)), metric.WithAttributes(lowCardinalityAttrs...))
		p.operationDurationHistogram.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(lowCardinalityAttrs...))
		span.End()

		if targetDeliveryChan != nil {
			targetDeliveryChan <- evt
		}
	}

	go tracedDeliveryFunc(deliveryChan)

	err := p.Producer.Produce(msg, tracedDeliveryChan)
	if err != nil {
		span.RecordError(err)
	}

	return err
}
