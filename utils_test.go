package otelkafka_test

import (
	"context"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	testkafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

const (
	kafkaImage = "confluentinc/confluent-local:7.5.0"
	kafkaPort  = "9093/tcp"
)

type KafkaTestContainer struct {
	kafka testcontainers.Container
}

func NewKafkaTestContainer(ctx context.Context) (*KafkaTestContainer, error) {
	kafka, err := testkafka.Run(
		ctx,
		kafkaImage,
		testkafka.WithClusterID("otel-kafka-test-cluster"),
	)

	if err != nil {
		return nil, fmt.Errorf("failed to start Kafka container: %w", err)
	}

	return &KafkaTestContainer{
		kafka: kafka,
	}, nil
}

func (c *KafkaTestContainer) Stop(ctx context.Context) error {
	return c.kafka.Terminate(ctx)
}

func (c *KafkaTestContainer) GetBootstrapServer(ctx context.Context) (string, error) {
	host, err := c.kafka.Host(ctx)
	if err != nil {
		return "", err
	}

	portMap, err := c.kafka.Ports(ctx)
	if err != nil {
		return "", err
	}

	if portBindings, found := portMap[kafkaPort]; found {
		return host + ":" + portBindings[0].HostPort, nil
	}

	return "", fmt.Errorf("couldn't find %s port in Kafka container port map: %v", kafkaPort, portMap)
}
