---
title: Introduction
layout: home
---

Introduction
===========================

[![Build status](https://github.com/ekazakas/otel-kafka/actions/workflows/test.yaml/badge.svg)](https://github.com/ekazakas/otel-kafka/actions/workflows/test.yml)

OpenTelemetry instrumentation library for [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go).

## Dependencies

`otel-kafka` depends on [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go), and [opentelemetry-go](https://github.com/open-telemetry/opentelemetry-go).

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

## License

`otel-kafka` is licensed under the **[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)** (the
"License"); you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.