package otelkafka

import (
	"context"
	"fmt"
	"github.com/ekazakas/otel-kafka/internal"
	"go.opentelemetry.io/otel/codes"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
	"go.opentelemetry.io/otel/semconv/v1.34.0/messagingconv"
	"go.opentelemetry.io/otel/trace"
)

type (
	Consumer struct {
		*kafka.Consumer

		cfg                  otelConfig
		messageCounter       metric.Int64Counter
		messageSizeHistogram metric.Int64Histogram
	}
)

// NewConsumer Returns either new kafka.Consumer with given configuration, or
// an error if provided configuration is not valid
func NewConsumer(config kafka.ConfigMap, opts ...Option) (*Consumer, error) {
	consumer, err := kafka.NewConsumer(&config)
	if err != nil {
		return nil, err
	}

	return WrapOTELConsumer(consumer, append(opts, withConfig(config))...)
}

// WrapOTELConsumer Decorates provided Consumer instance with OTEL features
func WrapOTELConsumer(consumer *kafka.Consumer, opts ...Option) (*Consumer, error) {
	cfg := newOTELConfig(opts...)
	meter := cfg.MeterProvider.Meter("kafka_consumer")

	messageCounter, err := messagingconv.NewClientConsumedMessages(meter)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumed messages counter metric: %w", err)
	}

	messageSizeHistogram, err := meter.Int64Histogram(
		"messaging.client.consumed.message.size",
		metric.WithDescription("Size of messages consumed by a consumer client."),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create message size metric: %w", err)
	}

	return &Consumer{
		Consumer:             consumer,
		cfg:                  cfg,
		messageCounter:       messageCounter.Inst(),
		messageSizeHistogram: messageSizeHistogram,
	}, nil
}

// Poll the consumer for messages or events using underlying vintedkafka.Consumer. Messages will be traced
func (c *Consumer) Poll(timeoutMs int) (event kafka.Event) {
	e := c.Consumer.Poll(timeoutMs)

	switch e := e.(type) {
	case *kafka.Message:
		lowCardinalityAttrs, highCardinalityAttrs := getConsumedMessageAttrs(e, &c.cfg)
		ctx, span := c.startSpan(e, lowCardinalityAttrs, highCardinalityAttrs)
		defer span.End()

		span.SetStatus(codes.Ok, "Success")

		c.messageCounter.Add(ctx, 1, metric.WithAttributes(lowCardinalityAttrs...))
		c.messageSizeHistogram.Record(ctx, int64(internal.GetMessageSize(e)), metric.WithAttributes(lowCardinalityAttrs...))
	}

	return e
}

// ReadMessage polls the consumer for a message using underlying vintedkafka.Consumer. Messages will be traced
func (c *Consumer) ReadMessage(timeout time.Duration) (*kafka.Message, error) {
	msg, err := c.Consumer.ReadMessage(timeout)
	if err != nil {
		return nil, err
	}

	lowCardinalityAttrs, highCardinalityAttrs := getConsumedMessageAttrs(msg, &c.cfg)

	// latest span is stored to be closed when the next message is polled or when the consumer is closed
	ctx, span := c.startSpan(msg, lowCardinalityAttrs, highCardinalityAttrs)
	defer span.End()

	span.SetStatus(codes.Ok, "Success")

	c.messageCounter.Add(ctx, 1, metric.WithAttributes(lowCardinalityAttrs...))
	c.messageSizeHistogram.Record(ctx, int64(internal.GetMessageSize(msg)), metric.WithAttributes(lowCardinalityAttrs...))

	return msg, nil
}

func (c *Consumer) startSpan(msg *kafka.Message, lowCardinalityAttrs []attribute.KeyValue, highCardinalityAttrs []attribute.KeyValue) (context.Context, trace.Span) {
	carrier := NewMessageCarrier(msg)
	parentSpanContext := c.cfg.Propagator.Extract(context.Background(), carrier)

	if c.cfg.attributeInjectFunc != nil {
		highCardinalityAttrs = append(highCardinalityAttrs, c.cfg.attributeInjectFunc(msg)...)
	}

	attrs := append(lowCardinalityAttrs, highCardinalityAttrs...)
	opts := []trace.SpanStartOption{
		trace.WithAttributes(attrs...),
		trace.WithSpanKind(trace.SpanKindConsumer),
	}
	newCtx, span := c.cfg.tracer.Start(parentSpanContext, fmt.Sprintf("%s consume", *msg.TopicPartition.Topic), opts...)

	// Inject current span context, so consumers can use it to propagate span.
	c.cfg.Propagator.Inject(newCtx, carrier)

	return newCtx, span
}

func getConsumedMessageAttrs(msg *kafka.Message, cfg *otelConfig) (lowCardinalityAttrs []attribute.KeyValue, highCardinalityAttrs []attribute.KeyValue) {
	lowCardinalityAttrs = []attribute.KeyValue{
		semconv.MessagingOperationName("consume"),
		semconv.MessagingOperationTypeReceive,
		semconv.MessagingSystemKafka,
		semconv.ServerAddress(cfg.bootstrapServerHost),
		semconv.MessagingDestinationName(*msg.TopicPartition.Topic),
		semconv.MessagingConsumerGroupName(cfg.consumerGroup),
	}

	highCardinalityAttrs = []attribute.KeyValue{
		semconv.MessagingKafkaOffset(int(msg.TopicPartition.Offset)),
		semconv.MessagingKafkaMessageKey(string(msg.Key)),
		semconv.MessagingMessageID(strconv.FormatInt(int64(msg.TopicPartition.Offset), 10)),
		semconv.MessagingDestinationPartitionID(strconv.Itoa(int(msg.TopicPartition.Partition))),
		semconv.MessagingMessageBodySize(internal.GetMessageSize(msg)),
	}

	return lowCardinalityAttrs, highCardinalityAttrs
}
