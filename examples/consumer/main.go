package main

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	otelkafka "github.com/ekazakas/otel-kafka"
	"github.com/ekazakas/otel-kafka/examples"
	"go.opentelemetry.io/otel/propagation"
	"log"
	"os"
	"os/signal"
	"syscall"
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

	consumer, err := otelkafka.NewConsumer(
		kafka.ConfigMap{
			"bootstrap.servers": os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
			"group.id":          "example-consumer-group",
			"auto.offset.reset": "earliest",
		},
		otelkafka.WithTracerProvider(traceProvider),
		otelkafka.WithMeterProvider(meterProvider),
		otelkafka.WithPropagator(propagation.TraceContext{}),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			panic(err)
		}
	}()

	if err := consumer.SubscribeTopics([]string{os.Getenv("KAFKA_TOPIC")}, nil); err != nil {
		panic(err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go examples.ServeMetrics(ctx, 2224)

	successLog := log.New(os.Stdout, "[otel-kafka]", log.LstdFlags)
	failureLog := log.New(os.Stderr, "[otel-kafka]", log.LstdFlags)

	successLog.Println("Starting consumer loop")

	run := true
	for run {
		select {
		case sig := <-sigChan:
			successLog.Printf("Received signal %s, shutting down...", sig)

			run = false
		default:
			ev := consumer.Poll(1000)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				if e.TopicPartition.Error != nil {
					failureLog.Printf("Delivery failed for message: %s", e.TopicPartition.Error)
				} else {
					successLog.Printf("Delivered message %s from %s with offset %d.", e.Value, *e.TopicPartition.Topic, e.TopicPartition.Offset)
				}
			case kafka.Error:
				failureLog.Printf("Error when polling messages: %s", e)
			default:
				successLog.Printf("Ignored message: %s", e)
			}
		}
	}
}
