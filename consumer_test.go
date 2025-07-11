package otelkafka_test

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	otelkafka "github.com/ekazakas/otel-kafka"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
	"math/rand/v2"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestConsumer_OpenTelemetry(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Kafka OTEL consumer integration test in short mode")
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

	consumerGroup := "test-group"

	inMemorySpanExporter := tracetest.NewInMemoryExporter()

	t.Run("polling message", func(t *testing.T) {
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
			otelkafka.WithPropagator(propagation.TraceContext{}),
		)
		require.NoError(t, err)
		t.Cleanup(producer.Close)

		outMsg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &testTopic,
				Partition: kafka.PartitionAny,
			},
			Value: []byte("test-message-without-delivery-channel"),
		}

		require.NoError(t, producer.Produce(outMsg, nil))
		producer.Flush(int((time.Second * 3).Milliseconds()))

		consumer, err := otelkafka.NewConsumer(
			kafka.ConfigMap{
				"bootstrap.servers": server,
				"group.id":          consumerGroup,
				"auto.offset.reset": "earliest",
			},
			otelkafka.WithTracerProvider(traceProvider),
			otelkafka.WithPropagator(propagation.TraceContext{}),
		)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, consumer.Close())
		})

		require.NoError(t, consumer.SubscribeTopics([]string{testTopic}, nil))

		consumedMessages := make([]*kafka.Message, 0, 1)
	ConsumeLoop:
		for {
			select {
			case <-time.After(10 * time.Second):
				break ConsumeLoop
			default:
				ev := consumer.Poll(100)
				if ev == nil {
					continue ConsumeLoop
				}

				switch e := ev.(type) {
				case *kafka.Message:
					consumedMessages = append(consumedMessages, e)

					break ConsumeLoop
				case kafka.Error:
					t.Errorf("%% Error: %v: %v\n", e.Code(), e)

					if e.Code() == kafka.ErrAllBrokersDown {
						t.Fatal("All Kafka brokers down")
					}
				}
			}
		}

		require.Len(t, consumedMessages, 1)
		inMsg := consumedMessages[0]
		require.Equal(t, outMsg.TopicPartition.Topic, inMsg.TopicPartition.Topic)

		require.NoError(t, traceProvider.ForceFlush(ctx))

		exportedSpans := inMemorySpanExporter.GetSpans()
		require.Len(t, exportedSpans, 2)

		producerSpan := exportedSpans[0]

		require.Equal(t, fmt.Sprintf("%s publish", testTopic), producerSpan.Name)
		require.False(t, producerSpan.Parent.IsValid())
		require.True(t, producerSpan.SpanContext.IsValid())
		require.Equal(t, codes.Ok, producerSpan.Status.Code)
		require.Contains(t, producerSpan.Attributes, semconv.MessagingOperationName("produce"))
		require.Contains(t, producerSpan.Attributes, semconv.MessagingOperationTypeSend)
		require.Contains(t, producerSpan.Attributes, semconv.MessagingSystemKafka)
		require.Contains(t, producerSpan.Attributes, semconv.ServerAddress(host))
		require.Contains(t, producerSpan.Attributes, semconv.MessagingDestinationName(*inMsg.TopicPartition.Topic))
		require.Contains(t, producerSpan.Attributes, semconv.MessagingKafkaMessageKey(string(inMsg.Key)))
		require.Contains(t, producerSpan.Attributes, semconv.MessagingMessageBodySize(len(inMsg.Key)+len(inMsg.Value)))

		consumerSpan := exportedSpans[1]

		require.Equal(t, fmt.Sprintf("%s consume", testTopic), consumerSpan.Name)
		require.True(t, consumerSpan.Parent.IsValid())
		require.True(t, consumerSpan.SpanContext.IsValid())
		require.Equal(t, codes.Ok, consumerSpan.Status.Code)
		require.Contains(t, consumerSpan.Attributes, semconv.MessagingOperationName("consume"))
		require.Contains(t, consumerSpan.Attributes, semconv.MessagingOperationTypeReceive)
		require.Contains(t, consumerSpan.Attributes, semconv.MessagingSystemKafka)
		require.Contains(t, consumerSpan.Attributes, semconv.ServerAddress(host))
		require.Contains(t, consumerSpan.Attributes, semconv.MessagingDestinationName(*inMsg.TopicPartition.Topic))
		require.Contains(t, consumerSpan.Attributes, semconv.MessagingConsumerGroupName(consumerGroup))
		require.Contains(t, consumerSpan.Attributes, semconv.MessagingKafkaOffset(int(inMsg.TopicPartition.Offset)))
		require.Contains(t, consumerSpan.Attributes, semconv.MessagingKafkaMessageKey(string(inMsg.Key)))
		require.Contains(t, consumerSpan.Attributes, semconv.MessagingMessageID(strconv.FormatInt(int64(inMsg.TopicPartition.Offset), 10)))
		require.Contains(t, consumerSpan.Attributes, semconv.MessagingDestinationPartitionID(strconv.Itoa(int(inMsg.TopicPartition.Partition))))
	})
}
