package otelkafka

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"net"
	"strconv"
	"strings"
)

const (
	defaultTracerName      = "go.opentelemetry.io/contrib/instrumentation/github.com/confluentinc/confluent-kafka-go/otel-kafka"
	instrumentationVersion = "1.0.0"
	consumerGroupKey       = "group.id"
	bootstrapServersKey    = "bootstrap.servers"
)

type (
	otelConfig struct {
		TracerProvider trace.TracerProvider
		MeterProvider  metric.MeterProvider
		Propagator     propagation.TextMapPropagator

		tracer              trace.Tracer
		consumerGroup       string
		bootstrapServerHost string
		bootstrapServerPort int32
		attributeInjectFunc func(msg *kafka.Message) []attribute.KeyValue
	}
)

// newOTELConfig returns a otelConfig with all Options set.
func newOTELConfig(opts ...Option) otelConfig {
	cfg := otelConfig{
		Propagator:     otel.GetTextMapPropagator(),
		TracerProvider: otel.GetTracerProvider(),
		MeterProvider:  otel.GetMeterProvider(),
	}

	for _, opt := range opts {
		opt.apply(&cfg)
	}

	cfg.tracer = cfg.TracerProvider.Tracer(
		defaultTracerName,
		trace.WithInstrumentationVersion(instrumentationVersion),
	)

	return cfg
}

// Option interface used for setting optional otelConfig properties.
type Option interface {
	apply(*otelConfig)
}

type optionFunc func(*otelConfig)

func (fn optionFunc) apply(c *otelConfig) {
	fn(c)
}

// WithTracerProvider specifies a tracer provider to use for creating a tracer.
// If none is specified, the global provider is used.
func WithTracerProvider(tracerProvider trace.TracerProvider) Option {
	return optionFunc(func(cfg *otelConfig) {
		if tracerProvider != nil {
			cfg.TracerProvider = tracerProvider
		}
	})
}

// WithMeterProvider specifies meter provider.
func WithMeterProvider(meterProvider metric.MeterProvider) Option {
	return optionFunc(func(cfg *otelConfig) {
		if meterProvider != nil {
			cfg.MeterProvider = meterProvider
		}
	})
}

// WithPropagator specifies propagators to use for extracting
// information from the Kafka messages. If none are specified, global
// ones will be used.
func WithPropagator(propagator propagation.TextMapPropagator) Option {
	return optionFunc(func(cfg *otelConfig) {
		if propagator != nil {
			cfg.Propagator = propagator
		}
	})
}

// WithCustomAttributeInjector provides custom attribute injection function for Kafka message.
func WithCustomAttributeInjector(fn func(msg *kafka.Message) []attribute.KeyValue) Option {
	return optionFunc(func(cfg *otelConfig) {
		cfg.attributeInjectFunc = fn
	})
}

// withConfig extracts the otelConfig information from kafka.ConfigMap for the client.
func withConfig(kCfg kafka.ConfigMap) Option {
	return optionFunc(func(cfg *otelConfig) {
		if consumerGroupValue, err := kCfg.Get(consumerGroupKey, ""); err == nil {
			if consumerGroup, ok := consumerGroupValue.(string); ok {
				if trimmedConsumerGroup := strings.TrimSpace(consumerGroup); len(trimmedConsumerGroup) > 0 {
					cfg.consumerGroup = trimmedConsumerGroup
				}
			}
		}

		if bootstrapServersValue, err := kCfg.Get(bootstrapServersKey, ""); err == nil {
			if bootstrapServers, ok := bootstrapServersValue.(string); ok {
				for _, addr := range strings.Split(strings.TrimSpace(bootstrapServers), ",") {
					if host, port, err := net.SplitHostPort(addr); err == nil {
						cfg.bootstrapServerHost = host

						if portNumber, err := strconv.Atoi(port); err == nil {
							cfg.bootstrapServerPort = int32(portNumber)
						}

						break
					}
				}
			}
		}
	})
}
