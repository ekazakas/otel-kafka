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
	"time"
)

const (
	defaultTracerName      = "go.opentelemetry.io/contrib/instrumentation/github.com/confluentinc/confluent-kafka-go/otel-kafka"
	instrumentationVersion = "1.0.0"
	consumerGroupKey       = "group.id"
	bootstrapServersKey    = "bootstrap.servers"
)

type (
	// otelConfig holds the configuration for OpenTelemetry instrumentation.
	otelConfig struct {
		TracerProvider trace.TracerProvider
		MeterProvider  metric.MeterProvider
		Propagator     propagation.TextMapPropagator

		tracer              trace.Tracer
		consumerGroup       string
		consumerSpanTimeout time.Duration
		bootstrapServerHost string
		bootstrapServerPort int32
		attributeInjectFunc func(msg *kafka.Message) []attribute.KeyValue
	}
)

// newOTELConfig returns an otelConfig with all Options applied.
// It initializes default values for TracerProvider, MeterProvider, and Propagator
// if not explicitly set by options.
func newOTELConfig(opts ...Option) otelConfig {
	cfg := otelConfig{
		Propagator:          otel.GetTextMapPropagator(),
		TracerProvider:      otel.GetTracerProvider(),
		MeterProvider:       otel.GetMeterProvider(),
		consumerSpanTimeout: 5 * time.Second,
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

// Option is an interface for setting optional otelConfig properties.
type Option interface {
	apply(*otelConfig)
}

// optionFunc is a function type that implements the Option interface.
type optionFunc func(*otelConfig)

// apply implements the Option interface for optionFunc.
func (fn optionFunc) apply(c *otelConfig) {
	fn(c)
}

// WithTracerProvider specifies a TracerProvider to use for creating a Tracer.
// If not specified, the global TracerProvider is used.
func WithTracerProvider(tracerProvider trace.TracerProvider) Option {
	return optionFunc(func(cfg *otelConfig) {
		if tracerProvider != nil {
			cfg.TracerProvider = tracerProvider
		}
	})
}

// WithMeterProvider specifies a MeterProvider to use for creating a Meter.
// If not specified, the global MeterProvider is used.
func WithMeterProvider(meterProvider metric.MeterProvider) Option {
	return optionFunc(func(cfg *otelConfig) {
		if meterProvider != nil {
			cfg.MeterProvider = meterProvider
		}
	})
}

// WithConsumerSpanTimeout specifies the time.Duration after which a consumer
// span should be closed if no new message is polled. A value of 0 or less
// means no timeout will be applied.
func WithConsumerSpanTimeout(timeout time.Duration) Option {
	return optionFunc(func(cfg *otelConfig) {
		if timeout > 0 {
			cfg.consumerSpanTimeout = timeout
		}
	})
}

// WithPropagator specifies the TextMapPropagator to use for extracting
// and injecting OpenTelemetry context from/into Kafka message headers.
// If no propagator is specified, the global TextMapPropagator is used.
func WithPropagator(propagator propagation.TextMapPropagator) Option {
	return optionFunc(func(cfg *otelConfig) {
		if propagator != nil {
			cfg.Propagator = propagator
		}
	})
}

// WithCustomAttributeInjector provides a custom function to inject additional
// OpenTelemetry attributes into the consumer span based on the kafka.Message.
func WithCustomAttributeInjector(fn func(msg *kafka.Message) []attribute.KeyValue) Option {
	return optionFunc(func(cfg *otelConfig) {
		cfg.attributeInjectFunc = fn
	})
}

// withConfig extracts relevant OpenTelemetry configuration details from
// a kafka.ConfigMap, such as the consumer group ID and
// bootstrap server host and port.
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
