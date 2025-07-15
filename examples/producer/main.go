package main

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	otelkafka "github.com/ekazakas/otel-kafka"
	"github.com/ekazakas/otel-kafka/examples"
	"go.opentelemetry.io/otel/propagation"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serviceName := "example-consumer"

	traceProvider, err := examples.CreateTracer(serviceName)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := traceProvider.Shutdown(ctx); err != nil {
			panic(err)
		}
	}()

	meterProvider, err := examples.CreateMeterProvider(serviceName)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := meterProvider.Shutdown(ctx); err != nil {
			panic(err)
		}
	}()

	propagator := propagation.TraceContext{}

	producer, err := otelkafka.NewProducer(
		kafka.ConfigMap{
			"bootstrap.servers": os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
		},
		otelkafka.WithTracerProvider(traceProvider),
		otelkafka.WithMeterProvider(meterProvider),
		otelkafka.WithPropagator(propagator),
	)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go examples.ServeMetrics(ctx, 2223)

	topic := os.Getenv("KAFKA_TOPIC")

	successLog := log.New(os.Stdout, "[otel-kafka]", log.LstdFlags)
	failureLog := log.New(os.Stderr, "[otel-kafka]", log.LstdFlags)

	go func() {
		ctx, span := traceProvider.Tracer("example-producer").Start(ctx, "produce message")
		defer span.End()

		for i := 0; ; i++ {
			time.Sleep(1 * time.Second)

			func() {
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

				propagator.Inject(ctx, otelkafka.NewMessageCarrier(message))

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
						successLog.Printf("Delivered messageto topic %s partition %d at offset %d", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
					}
				}
			}()
		}
	}()

	<-sigChan
}
