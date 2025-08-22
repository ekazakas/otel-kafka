package main

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	otelkafka "github.com/ekazakas/otel-kafka"
	"github.com/ekazakas/otel-kafka/examples"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	successLog := log.New(os.Stdout, "[otel-kafka]", log.LstdFlags)
	failureLog := log.New(os.Stderr, "[otel-kafka]", log.LstdFlags)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serviceName := "example-producer"

	tracerProvider, err := examples.CreateTracer(serviceName)
	if err != nil {
		failureLog.Panicf("Failed to create TracerProvider: %s", err)
	}
	defer func() {
		successLog.Println("Shutting down TracerProvider")

		if err := tracerProvider.Shutdown(ctx); err != nil {
			failureLog.Panicf("Failed to shutdown TracerProvider: %s", err)
		}

		successLog.Println("TracerProvider shutdown complete")
	}()

	meterProvider, err := examples.CreateMeterProvider(serviceName)
	if err != nil {
		failureLog.Panicf("Failed to create MeterProvider: %s", err)
	}
	defer func() {
		successLog.Println("Shutting down MeterProvider")

		if err := meterProvider.Shutdown(ctx); err != nil {
			failureLog.Panicf("Failed to shutdown MeterProvider: %s", err)
		}

		successLog.Println("MeterProvider shutdown complete")
	}()

	otel.SetTracerProvider(tracerProvider)
	otel.SetMeterProvider(meterProvider)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	producer, err := otelkafka.NewProducer(
		kafka.ConfigMap{
			"bootstrap.servers": os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
		},
	)
	if err != nil {
		failureLog.Panicf("Failed to create Producer: %s", err)
	}
	defer func() {
		successLog.Println("Shutting down Producer")

		producer.Close()

		successLog.Println("Producer shutdown complete")
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go examples.ServeMetrics(ctx, 2224, successLog, failureLog)

	topic := os.Getenv("KAFKA_TOPIC")

	for i := 0; ; i++ {
		select {
		case <-sigChan:
			return
		default:
			time.Sleep(1 * time.Second)

			func() {
				ctx, span := tracerProvider.Tracer("example-producer").Start(ctx, "produce message")
				defer span.End()

				deliveryChan := make(chan kafka.Event)
				defer close(deliveryChan)

				message := &kafka.Message{
					TopicPartition: kafka.TopicPartition{
						Topic:     &topic,
						Partition: kafka.PartitionAny,
					},
					Value: []byte(fmt.Sprintf("message-%d", i)),
					Key:   []byte(fmt.Sprintf("key-%d", i)),
					Headers: []kafka.Header{
						{
							Key:   "header",
							Value: []byte("header values are binary"),
						},
					},
				}

				otel.GetTextMapPropagator().Inject(ctx, otelkafka.NewMessageCarrier(message))

				err = producer.Produce(message, deliveryChan)
				if err != nil {
					failureLog.Printf("Error producing message. Error: %s", err)

					return
				}

				e := <-deliveryChan
				if m, ok := e.(*kafka.Message); ok {
					if m.TopicPartition.Error != nil {
						failureLog.Printf("Delivery failed for message: %s", m.TopicPartition.Error)
					} else {
						successLog.Printf("Delivered message to topic %s partition %d at offset %d", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
					}
				}
			}()
		}
	}
}
