package otelkafka

import (
	"context"
	"fmt"
	"github.com/ekazakas/otel-kafka/internal"
	"go.opentelemetry.io/otel/codes"
	"strconv"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
	"go.opentelemetry.io/otel/semconv/v1.34.0/messagingconv"
	"go.opentelemetry.io/otel/trace"
)

type (
	// Consumer is a wrapper around kafka.Consumer that adds OpenTelemetry tracing and metrics.
	Consumer struct {
		*kafka.Consumer

		cfg    otelConfig
		spanCh chan struct{}
		spanWg sync.WaitGroup

		messageCounter             metric.Int64Counter
		messageSizeHistogram       metric.Int64Histogram
		operationDurationHistogram metric.Float64Histogram
	}
)

// NewConsumer returns a new kafka.Consumer instance with OpenTelemetry features,
// configured with the provided kafka.ConfigMap and otelkafka.Option(s).
// It returns an error if the Kafka consumer cannot be created or if metrics
// initialization fails.
func NewConsumer(config kafka.ConfigMap, opts ...Option) (*Consumer, error) {
	consumer, err := kafka.NewConsumer(&config)
	if err != nil {
		return nil, err
	}

	return WrapOTELConsumer(consumer, append(opts, withConfig(config))...)
}

// WrapOTELConsumer decorates an existing kafka.Consumer instance with
// OpenTelemetry tracing and metrics capabilities.
// It returns the wrapped Consumer or an error if metrics initialization fails.
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

	operationDurationHistogram, err := messagingconv.NewClientOperationDuration(meter)
	if err != nil {
		return nil, fmt.Errorf("failed to create operation duration metric: %w", err)
	}

	return &Consumer{
		Consumer:                   consumer,
		cfg:                        cfg,
		spanCh:                     make(chan struct{}, 1000),
		messageCounter:             messageCounter.Inst(),
		messageSizeHistogram:       messageSizeHistogram,
		operationDurationHistogram: operationDurationHistogram.Inst(),
	}, nil
}

// Poll polls the consumer for messages or events using the underlying
// kafka.Consumer. Messages received will be automatically traced
// and associated metrics will be recorded.
func (c *Consumer) Poll(timeoutMs int) (event kafka.Event) {
	c.spanCh <- struct{}{}

	start := time.Now()

	e := c.Consumer.Poll(timeoutMs)

	switch e := e.(type) {
	case *kafka.Message:
		metricAttrs := getMetricAttrs(e, &c.cfg)
		spanAttrs := getSpanAttrs(e)

		ctx, span := c.startSpan(e, metricAttrs, spanAttrs)
		span.SetStatus(codes.Ok, "Success")

		c.spanWg.Add(1)

		go c.watchSpan(span)

		c.messageCounter.Add(ctx, 1, metric.WithAttributes(metricAttrs...))
		c.messageSizeHistogram.Record(ctx, int64(internal.GetMessageSize(e)), metric.WithAttributes(metricAttrs...))
		c.operationDurationHistogram.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(metricAttrs...))
	}

	return e
}

// ReadMessage polls the consumer for a single message using the underlying
// kafka.Consumer. Messages received will be automatically traced
// and associated metrics will be recorded.
func (c *Consumer) ReadMessage(timeout time.Duration) (*kafka.Message, error) {
	c.spanCh <- struct{}{}

	start := time.Now()

	msg, err := c.Consumer.ReadMessage(timeout)
	if err != nil {
		return nil, err
	}

	metricAttrs := getMetricAttrs(msg, &c.cfg)
	spanAttrs := getSpanAttrs(msg)

	ctx, span := c.startSpan(msg, metricAttrs, spanAttrs)
	span.SetStatus(codes.Ok, "Success")

	c.spanWg.Add(1)

	go c.watchSpan(span)

	c.messageCounter.Add(ctx, 1, metric.WithAttributes(metricAttrs...))
	c.messageSizeHistogram.Record(ctx, int64(internal.GetMessageSize(msg)), metric.WithAttributes(metricAttrs...))
	c.operationDurationHistogram.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(metricAttrs...))

	return msg, nil
}

// Close closes the underlying confluent-kafka-go Consumer and waits for
// any active spans to be ended.
func (c *Consumer) Close() error {
	c.spanWg.Wait()

	return c.Consumer.Close()
}

// startSpan extracts the parent span context from the kafka.Message headers,
// creates a new consumer span, injects the new span context back into the
// message headers, and returns the new context and span.
func (c *Consumer) startSpan(msg *kafka.Message, metricAttrs []attribute.KeyValue, spanAttrs []attribute.KeyValue) (context.Context, trace.Span) {
	carrier := NewMessageCarrier(msg)
	parentSpanContext := c.cfg.Propagator.Extract(context.Background(), carrier)

	if c.cfg.attributeInjectFunc != nil {
		spanAttrs = append(spanAttrs, c.cfg.attributeInjectFunc(msg)...)
	}

	opts := []trace.SpanStartOption{
		trace.WithAttributes(append(metricAttrs, spanAttrs...)...),
		trace.WithSpanKind(trace.SpanKindConsumer),
	}
	newCtx, span := c.cfg.tracer.Start(parentSpanContext, fmt.Sprintf("%s consume", *msg.TopicPartition.Topic), opts...)

	c.cfg.Propagator.Inject(newCtx, carrier)

	return newCtx, span
}

// watchSpan monitors a given span and ends it either when a new message is
// polled (signaled via c.spanCh) or after a configured timeout, whichever
// comes first. It decrements the spanWg when the span is ended.
func (c *Consumer) watchSpan(span trace.Span) {
	timer := time.NewTimer(c.cfg.consumerSpanTimeout)
	defer timer.Stop()

	select {
	case <-c.spanCh:
		span.End()
	case <-timer.C:
		span.End()
	}

	c.spanWg.Done()
}

func getMetricAttrs(msg *kafka.Message, cfg *otelConfig) []attribute.KeyValue {
	return []attribute.KeyValue{
		semconv.MessagingOperationName("consume"),
		semconv.MessagingOperationTypeReceive,
		semconv.MessagingSystemKafka,
		semconv.ServerAddress(cfg.bootstrapServerHost),
		semconv.MessagingDestinationName(*msg.TopicPartition.Topic),
		semconv.MessagingConsumerGroupName(cfg.consumerGroup),
	}
}

func getSpanAttrs(msg *kafka.Message) []attribute.KeyValue {
	return []attribute.KeyValue{
		semconv.MessagingKafkaOffset(int(msg.TopicPartition.Offset)),
		semconv.MessagingKafkaMessageKey(string(msg.Key)),
		semconv.MessagingMessageID(strconv.FormatInt(int64(msg.TopicPartition.Offset), 10)),
		semconv.MessagingDestinationPartitionID(strconv.Itoa(int(msg.TopicPartition.Partition))),
		semconv.MessagingMessageBodySize(internal.GetMessageSize(msg)),
	}
}
