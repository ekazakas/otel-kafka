package otelkafka_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	otelkafka "github.com/ekazakas/otel-kafka"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
)

func TestProducer_OpenTelemetry(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Kafka OTEL producer integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	container, err := NewKafkaTestContainer(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		defer cancel()

		require.NoError(t, container.Stop(ctx))
	})

	server, err := container.GetBootstrapServer(ctx)
	require.NoError(t, err)

	host, _, err := net.SplitHostPort(server)
	require.NoError(t, err)

	inMemorySpanExporter := tracetest.NewInMemoryExporter()

	t.Run("producing message without delivery channel", func(t *testing.T) {
		inMemorySpanExporter.Reset()

		testTopic := fmt.Sprintf("topic-%d", rand.IntN(1000))

		traceProvider := trace.NewTracerProvider(trace.WithSyncer(inMemorySpanExporter))
		t.Cleanup(func() {
			require.NoError(t, traceProvider.Shutdown(ctx))
		})

		producer, err := otelkafka.NewProducer(
			kafka.ConfigMap{
				"bootstrap.servers": server,
			},
			otelkafka.WithTracerProvider(traceProvider),
		)
		require.NoError(t, err)
		t.Cleanup(producer.Close)

		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &testTopic,
				Partition: kafka.PartitionAny,
			},
			Value: []byte("test-message-without-delivery-channel"),
		}

		require.NoError(t, producer.Produce(msg, nil))

		producer.Flush(int((time.Second * 3).Milliseconds()))
		require.NoError(t, traceProvider.ForceFlush(ctx))

		require.Eventually(
			t,
			func() bool {
				return len(inMemorySpanExporter.GetSpans()) == 1
			},
			15*time.Second,
			500*time.Millisecond,
		)

		span := inMemorySpanExporter.GetSpans()[0]

		require.Equal(t, fmt.Sprintf("produce %s", testTopic), span.Name)
		require.False(t, span.Parent.IsValid())
		require.True(t, span.SpanContext.IsValid())
		require.Equal(t, codes.Ok, span.Status.Code)
		require.Contains(t, span.Attributes, semconv.MessagingOperationName("produce"))
		require.Contains(t, span.Attributes, semconv.MessagingOperationTypeSend)
		require.Contains(t, span.Attributes, semconv.MessagingSystemKafka)
		require.Contains(t, span.Attributes, semconv.ServerAddress(host))
		require.Contains(t, span.Attributes, semconv.MessagingDestinationName(*msg.TopicPartition.Topic))
		require.Contains(t, span.Attributes, semconv.MessagingKafkaMessageKey(string(msg.Key)))
		require.Contains(t, span.Attributes, semconv.MessagingMessageBodySize(len(msg.Key)+len(msg.Value)))
	})

	t.Run("producing message with delivery channel", func(t *testing.T) {
		inMemorySpanExporter.Reset()

		testTopic := fmt.Sprintf("topic-%d", rand.IntN(1000))

		traceProvider := trace.NewTracerProvider(trace.WithSyncer(inMemorySpanExporter))
		t.Cleanup(func() {
			require.NoError(t, traceProvider.Shutdown(ctx))
		})

		producer, err := otelkafka.NewProducer(
			kafka.ConfigMap{
				"bootstrap.servers": server,
			},
			otelkafka.WithTracerProvider(traceProvider),
		)
		require.NoError(t, err)
		t.Cleanup(producer.Close)

		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &testTopic,
				Partition: kafka.PartitionAny,
			},
			Value: []byte("test-message-with-delivery-channel"),
		}

		deliveryChan := make(chan kafka.Event)
		require.NoError(t, producer.Produce(msg, deliveryChan))

		producer.Flush(int((time.Second * 3).Milliseconds()))
		require.NoError(t, traceProvider.ForceFlush(ctx))

		event := <-deliveryChan
		evt, ok := event.(*kafka.Message)
		require.True(t, ok)
		require.Equal(t, msg.TopicPartition.Topic, evt.TopicPartition.Topic)
		require.Equal(t, int32(0), evt.TopicPartition.Partition)
		require.Equal(t, msg.TopicPartition.Offset, evt.TopicPartition.Offset)

		require.Eventually(
			t,
			func() bool {
				return len(inMemorySpanExporter.GetSpans()) == 1
			},
			15*time.Second,
			500*time.Millisecond,
		)

		span := inMemorySpanExporter.GetSpans()[0]

		require.Equal(t, fmt.Sprintf("produce %s", testTopic), span.Name)
		require.False(t, span.Parent.IsValid())
		require.True(t, span.SpanContext.IsValid())
		require.Equal(t, codes.Ok, span.Status.Code)
		require.Contains(t, span.Attributes, semconv.MessagingOperationName("produce"))
		require.Contains(t, span.Attributes, semconv.MessagingOperationTypeSend)
		require.Contains(t, span.Attributes, semconv.MessagingSystemKafka)
		require.Contains(t, span.Attributes, semconv.ServerAddress(host))
		require.Contains(t, span.Attributes, semconv.MessagingDestinationName(*evt.TopicPartition.Topic))
		require.Contains(t, span.Attributes, semconv.MessagingKafkaMessageKey(string(evt.Key)))
		require.Contains(t, span.Attributes, semconv.MessagingMessageBodySize(len(evt.Key)+len(evt.Value)))
		require.Contains(t, span.Attributes, semconv.MessagingDestinationPartitionID(fmt.Sprintf("%d", evt.TopicPartition.Partition)))
		require.Contains(t, span.Attributes, semconv.MessagingKafkaOffset(int(evt.TopicPartition.Offset)))
	})

	t.Run("producing message with delivery channel and non-existing topic", func(t *testing.T) {
		inMemorySpanExporter.Reset()

		testTopic := fmt.Sprintf("topic-%d", rand.IntN(1000))

		traceProvider := trace.NewTracerProvider(trace.WithSyncer(inMemorySpanExporter))
		t.Cleanup(func() {
			require.NoError(t, traceProvider.Shutdown(ctx))
		})

		producer, err := otelkafka.NewProducer(
			kafka.ConfigMap{
				"bootstrap.servers":        server,
				"allow.auto.create.topics": "false",
				"message.timeout.ms":       5000,
			},
			otelkafka.WithTracerProvider(traceProvider),
		)
		require.NoError(t, err)
		t.Cleanup(producer.Close)

		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &testTopic,
				Partition: kafka.PartitionAny,
			},
			Value: []byte("test-message-with-delivery-channel"),
		}

		deliveryChan := make(chan kafka.Event)
		require.NoError(t, producer.Produce(msg, deliveryChan))

		producer.Flush(int((time.Second * 3).Milliseconds()))
		require.NoError(t, traceProvider.ForceFlush(ctx))

		event := <-deliveryChan
		evt, ok := event.(*kafka.Message)
		require.True(t, ok)
		require.Equal(t, msg.TopicPartition.Topic, evt.TopicPartition.Topic)
		require.Equal(t, msg.TopicPartition.Partition, evt.TopicPartition.Partition)

		require.Eventually(
			t,
			func() bool {
				return len(inMemorySpanExporter.GetSpans()) == 1
			},
			15*time.Second,
			500*time.Millisecond,
		)

		span := inMemorySpanExporter.GetSpans()[0]

		require.Equal(t, fmt.Sprintf("produce %s", testTopic), span.Name)
		require.False(t, span.Parent.IsValid())
		require.True(t, span.SpanContext.IsValid())
		require.Equal(t, codes.Error, span.Status.Code)
		require.Contains(t, span.Attributes, semconv.MessagingOperationName("produce"))
		require.Contains(t, span.Attributes, semconv.MessagingOperationTypeSend)
		require.Contains(t, span.Attributes, semconv.MessagingSystemKafka)
		require.Contains(t, span.Attributes, semconv.ServerAddress(host))
		require.Contains(t, span.Attributes, semconv.MessagingDestinationName(*evt.TopicPartition.Topic))
		require.Contains(t, span.Attributes, semconv.MessagingKafkaMessageKey(string(evt.Key)))
		require.Contains(t, span.Attributes, semconv.MessagingMessageBodySize(len(evt.Key)+len(evt.Value)))
		require.Contains(t, span.Attributes, semconv.ErrorTypeKey.String("produce_error"))
	})
}
