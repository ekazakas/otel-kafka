package otelkafka

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	mnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	tnoop "go.opentelemetry.io/otel/trace/noop"
	"testing"
	"time"
)

func TestConfig_Valid(t *testing.T) {
	tracerProvider := tnoop.NewTracerProvider()
	meterProvider := mnoop.NewMeterProvider()
	propagator := propagation.TraceContext{}
	attributeInjector := func(msg *kafka.Message) []attribute.KeyValue {
		return []attribute.KeyValue{
			attribute.String("test-key", "test-value"),
		}
	}

	bootstrapHost := "localhost"
	bootstrapPort := 9092
	consumerGroup := "test-consumer-group"
	timeout := 10 * time.Second
	tCfg := kafka.ConfigMap{
		"bootstrap.servers": fmt.Sprintf("%s:%d", bootstrapHost, bootstrapPort),
		"group.id":          consumerGroup,
	}

	config := newOTELConfig(
		WithTracerProvider(tracerProvider),
		WithMeterProvider(meterProvider),
		WithPropagator(propagator),
		WithCustomAttributeInjector(attributeInjector),
		withConfig(tCfg),
	)

	require.Equal(t, tracerProvider, config.TracerProvider)
	require.Equal(t, meterProvider, config.MeterProvider)
	require.Equal(t, propagator, config.Propagator)
	require.Equal(t, attributeInjector(nil)[0], config.attributeInjectFunc(nil)[0])
	require.Equal(t, consumerGroup, config.consumerGroup)
	require.Equal(t, timeout, config.consumerSpanTimeout)
	require.Equal(t, bootstrapHost, config.bootstrapServerHost)
	require.Equal(t, bootstrapPort, config.bootstrapServerPort)
}

func TestConfig_WithEmptyConfigMap(t *testing.T) {
	config := newOTELConfig(withConfig(kafka.ConfigMap{}))
	require.Equal(t, "", config.consumerGroup)
	require.Equal(t, "", config.bootstrapServerHost)
}

func TestConfig_WithConsumerGroup(t *testing.T) {
	consumerGroup := "test-consumer-group"
	tCfg := kafka.ConfigMap{
		"group.id": consumerGroup,
	}

	config := newOTELConfig(withConfig(tCfg))
	require.Equal(t, consumerGroup, config.consumerGroup)
	require.Equal(t, "", config.bootstrapServerHost)
}
