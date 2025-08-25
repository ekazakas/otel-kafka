[bugs]: https://github.com/ekazakas/otel-kafka/issues?q=is%3Aopen+is%3Aissue+label%3ABug
[contributing]: CONTRIBUTING.md

# otel-kafka

OpenTelemetry instrumentation library for [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go).

---

[![GoDoc](https://godoc.org/github.com/ekazakas/otel-kafka?status.svg)](https://godoc.org/github.com/ekazakas/otel-kafka)
[![Tests](https://github.com/ekazakas/otel-kafka/workflows/Tests/badge.svg)](https://github.com/ekazakas/otel-kafka/actions?query=workflow%3ATests)
[![Go Report Card](https://goreportcard.com/badge/github.com/ekazakas/otel-kafka)](https://goreportcard.com/report/github.com/ekazakas/otel-kafka)

[Installation] | [Contributing]

## Installation

```shell
go get -u github.com/ekazakas/otel-kafka
```

**Note:** otel-kafka uses [Go Modules](https://go.dev/wiki/Modules) to manage dependencies.

## Producer

Complete example located [here](https://github.com/ekazakas/otel-kafka/examples/producer/main.go)

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

producer, err := otelkafka.NewProducer(
    kafka.ConfigMap{
        "bootstrap.servers": "localhost:9092",
    },
    otelkafka.WithTracerProvider(tracerProvider),
    otelkafka.WithMeterProvider(meterProvider),
    otelkafka.WithPropagator(propagation.TraceContext{}),
)
if err != nil {
    panic(err)
}
defer func() {
    producer.Close()
}()
```

## Consumer

Complete example located [here](https://github.com/ekazakas/otel-kafka/examples/consumer/main.go)

```go
consumer, err := otelkafka.NewConsumer(
    kafka.ConfigMap{
        "bootstrap.servers": "localhost:9092",
        "group.id":          "example-consumer-group",
        "auto.offset.reset": "earliest",
    },
    otelkafka.WithTracerProvider(tracerProvider),
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
```

## What is otel-kafka?

otel-kafka is an OpenTelemetry instrumentation library designed for confluent-kafka-go applications. It provides observability by integrating with the OpenTelemetry framework, allowing for the collection of traces and metrics related to Kafka operations. This enables better monitoring, troubleshooting, and performance analysis of Kafka-based systems.

It is designed to work within an application and offers functionalities such as:

* Instrumenting Kafka producers and consumers
* Propagating OpenTelemetry context (trace and span IDs) within Kafka message headers
* Collecting traces related to Kafka operations
* Collecting metrics related to Kafka operations
* Providing configuration options for instrumentation behavior

## Why otel-kafka?

When building modern applications that rely on Kafka, ensuring proper observability is crucial for understanding system behavior, identifying bottlenecks, and troubleshooting issues. otel-kafka is here to help with that.

otel-kafka provides the following benefits:

* Seamless OpenTelemetry Integration: Easily instruments your confluent-kafka-go applications to work with the OpenTelemetry ecosystem.
* Comprehensive Observability: Enables the collection of vital traces and metrics from your Kafka operations, offering deep insights into message flow and performance.
* Simplified Troubleshooting: By propagating OpenTelemetry context (like trace and span IDs) through Kafka message headers, it makes distributed tracing across your Kafka consumers and producers straightforward.
* Performance Analysis: The collected telemetry data helps in monitoring the health and performance of your Kafka interactions, allowing for proactive optimization.
* Configurable Instrumentation: Offers options to customize how Kafka operations are instrumented, giving you control over the data collected.

In essence, otel-kafka simplifies the process of gaining visibility into your Kafka-driven microservices, allowing you to focus on building robust and high-performing applications.