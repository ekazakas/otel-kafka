package otelkafka_test

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	otelkafka "github.com/ekazakas/otel-kafka"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMessageCarrier_GetKeys(t *testing.T) {
	carrier := otelkafka.NewMessageCarrier(&kafka.Message{
		Headers: []kafka.Header{
			{
				Key:   "test_key",
				Value: []byte("test_value"),
			},
		},
	})

	require.Equal(t, "test_value", carrier.Get("test_key"))
	require.Equal(t, "", carrier.Get("test_non_existing_key"))
}

func TestMessageCarrier_SetKeys(t *testing.T) {
	msg := &kafka.Message{}

	carrier := otelkafka.NewMessageCarrier(msg)
	carrier.Set("test_dup_key", "test_dup_value1")
	carrier.Set("test_dup_key", "test_dup_value2")
	carrier.Set("test_unique_key", "test_unique_value")

	expectedHeaders := []kafka.Header{
		{
			Key:   "test_dup_key",
			Value: []byte("test_dup_value2"),
		}, {
			Key:   "test_unique_key",
			Value: []byte("test_unique_value"),
		},
	}

	require.Equal(t, expectedHeaders, msg.Headers)
}

func TestMessageCarrier_GetAllKeys(t *testing.T) {
	carrier := otelkafka.NewMessageCarrier(&kafka.Message{
		Headers: []kafka.Header{
			{
				Key:   "test_key",
				Value: []byte("test_value"),
			},
		},
	})

	require.Equal(t, []string{"test_key"}, carrier.Keys())
}
