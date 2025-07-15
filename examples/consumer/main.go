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
)

func main() {
	successLog := log.New(os.Stdout, "[otel-kafka]", log.LstdFlags)
	failureLog := log.New(os.Stderr, "[otel-kafka]", log.LstdFlags)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serviceName := "example-consumer"

	tracerProvider, err := examples.CreateTracer(serviceName)
	if err != nil {
		failureLog.Panicf("Failed to create tracer provider: %s", err)
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
		failureLog.Panicf("Failed to create meter provider: %s", err)
	}
	defer func() {
		successLog.Println("Shutting down MeterProvider")

		if err := meterProvider.Shutdown(ctx); err != nil {
			failureLog.Panicf("Failed to shutdown MeterProvider: %s", err)
		}

		successLog.Println("MeterProvider shutdown complete")
	}()

	consumer, err := otelkafka.NewConsumer(
		kafka.ConfigMap{
			"bootstrap.servers": os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
			"group.id":          "example-consumer-group",
			"auto.offset.reset": "earliest",
		},
		otelkafka.WithTracerProvider(tracerProvider),
		otelkafka.WithMeterProvider(meterProvider),
		otelkafka.WithPropagator(propagation.TraceContext{}),
	)
	if err != nil {
		failureLog.Panicf("Failed to create Consumer: %s", err)
	}
	defer func() {
		successLog.Println("Shutting down Consumer")

		if err := consumer.Close(); err != nil {
			log.Panicf("Failed to close Consumer: %s", err)
		}

		successLog.Println("Consumer shutdown complete")
	}()

	if err := consumer.SubscribeTopics([]string{os.Getenv("KAFKA_TOPIC")}, rebalanceCallback(successLog)); err != nil {
		log.Panicf("Failed to subscribe to topics: %s", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go examples.ServeMetrics(ctx, 2223, successLog, failureLog)

	successLog.Println("Starting consumer loop")

	for {
		select {
		case sig := <-sigChan:
			successLog.Printf("Received signal %s, shutting down...", sig)

			return
		default:
			ev := consumer.Poll(1000)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				if e.TopicPartition.Error != nil {
					failureLog.Printf("Reception failed for message: %s", e.TopicPartition.Error)
				} else {
					successLog.Printf("Received message %s from %s with offset %d.", e.Value, *e.TopicPartition.Topic, e.TopicPartition.Offset)
				}
			case kafka.Error:
				failureLog.Printf("Error when polling messages: %s", e)
			default:
				successLog.Printf("Ignored message: %s", e)
			}
		}
	}
}

func rebalanceCallback(logger *log.Logger) func(c *kafka.Consumer, event kafka.Event) error {
	return func(c *kafka.Consumer, event kafka.Event) error {
		switch ev := event.(type) {
		case kafka.AssignedPartitions:
			logger.Printf("Assigned partitions. Count: %d, protocol: %s", len(ev.Partitions), c.GetRebalanceProtocol())
		case kafka.RevokedPartitions:
			logger.Printf("Revoked partitions. Count: %d, protocol: %s", len(ev.Partitions), c.GetRebalanceProtocol())
		default:
			return fmt.Errorf("unexpected event: %v", event)
		}

		return nil
	}
}
