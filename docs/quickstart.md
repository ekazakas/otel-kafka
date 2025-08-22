---
title: Quickstart
layout: home
---

Quickstart
===========================

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