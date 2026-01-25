package otelkafka

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ekazakas/otel-kafka/internal"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
	"go.opentelemetry.io/otel/semconv/v1.34.0/messagingconv"
	"go.opentelemetry.io/otel/trace"
)

type (
	// Producer is a wrapper around kafka.Producer that adds OpenTelemetry tracing and metrics.
	Producer struct {
		*kafka.Producer

		cfg                        otelConfig
		messageCounter             metric.Int64Counter
		messageSizeHistogram       metric.Int64Histogram
		operationDurationHistogram metric.Float64Histogram
	}
)

// NewProducer returns a new kafka.Producer instance with OpenTelemetry features,
// configured with the provided kafka.ConfigMap and otelkafka.Option(s).
// It returns an error if the Kafka producer cannot be created or if metrics
// initialization fails.
func NewProducer(config kafka.ConfigMap, opts ...Option) (*Producer, error) {
	producer, err := kafka.NewProducer(&config)
	if err != nil {
		return nil, err
	}

	return WrapOTELProducer(producer, append(opts, withConfig(config))...)
}

// WrapOTELProducer decorates an existing kafka.Producer instance with
// OpenTelemetry tracing and metrics capabilities.
// It returns the wrapped Producer or an error if metrics initialization fails.
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

// Produce calls the underlying kafka.Producer to send a message to Kafka,
// while also tracing the production operation and recording relevant metrics.
// The OpenTelemetry context is propagated through Kafka message headers.
// The `deliveryChan` is used to deliver the production result asynchronously.
func (p *Producer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	start := time.Now()

	// If there's a span context in the message, use that as the parent context.
	carrier := NewMessageCarrier(msg)
	ctx := p.cfg.Propagator.Extract(context.Background(), carrier)

	optional := make([]attribute.KeyValue, 0, 1)
	if msg.TopicPartition.Topic != nil {
		optional = append(optional, semconv.MessagingDestinationName(*msg.TopicPartition.Topic))
	}

	metricAttrs := append(
		[]attribute.KeyValue{
			semconv.MessagingOperationName("produce"),
			semconv.MessagingOperationTypeSend,
			semconv.MessagingSystemKafka,
			semconv.ServerAddress(p.cfg.bootstrapServerHost),
		},
		optional...,
	)

	spanAttrs := []attribute.KeyValue{
		semconv.MessagingKafkaMessageKey(string(msg.Key)),
		semconv.MessagingMessageBodySize(internal.GetMessageSize(msg)),
	}
	if p.cfg.attributeInjectFunc != nil {
		spanAttrs = append(spanAttrs, p.cfg.attributeInjectFunc(msg)...)
	}

	opts := []trace.SpanStartOption{
		trace.WithAttributes(append(metricAttrs, spanAttrs...)...),
		trace.WithSpanKind(trace.SpanKindProducer),
	}

	topic := ""
	if msg.TopicPartition.Topic != nil {
		topic = *msg.TopicPartition.Topic
	}

	ctx, span := p.cfg.tracer.Start(ctx, strings.TrimSpace(fmt.Sprintf("produce %s", topic)), opts...)
	p.cfg.Propagator.Inject(ctx, carrier)

	tracedDeliveryChan := make(chan kafka.Event, 1)

	if err := p.Producer.Produce(msg, tracedDeliveryChan); err != nil {
		defer span.End()

		span.RecordError(err)
		span.SetAttributes(semconv.ErrorTypeKey.String("produce_error"))
		span.SetStatus(codes.Error, err.Error())

		return err
	}

	go func() {
		defer span.End()

		evt := <-tracedDeliveryChan
		if resMsg, ok := evt.(*kafka.Message); ok {
			if err := resMsg.TopicPartition.Error; err != nil {
				span.RecordError(err)
				span.SetAttributes(semconv.ErrorTypeKey.String("produce_error"))
				span.SetStatus(codes.Error, err.Error())

				metricAttrs = append(metricAttrs, semconv.ErrorTypeKey.String("produce_error"))
			} else {
				if resMsg.TopicPartition.Partition >= 0 {
					partitionIDAttr := semconv.MessagingDestinationPartitionID(fmt.Sprintf("%d", resMsg.TopicPartition.Partition))

					span.SetAttributes(partitionIDAttr)
					span.SetAttributes(semconv.MessagingKafkaOffset(int(resMsg.TopicPartition.Offset)))

					metricAttrs = append(metricAttrs, partitionIDAttr)
				}

				span.SetStatus(codes.Ok, "Success")
			}
		}

		p.messageCounter.Add(ctx, 1, metric.WithAttributes(metricAttrs...))
		p.messageSizeHistogram.Record(ctx, int64(internal.GetMessageSize(msg)), metric.WithAttributes(metricAttrs...))
		p.operationDurationHistogram.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(metricAttrs...))

		if deliveryChan != nil {
			deliveryChan <- evt
		}
	}()

	return nil
}
