package internal

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGetMessageSize(t *testing.T) {
	msg := &kafka.Message{
		Headers: []kafka.Header{
			{
				Key:   "first_key",
				Value: []byte("test_value 1"),
			}, {
				Key:   "second_key",
				Value: []byte("test_value 2"),
			},
		},
		Key:   []byte("test_key"),
		Value: []byte("test_value"),
	}

	require.Equal(t, headersLength(msg.Headers)+len(msg.Key)+len(msg.Value), GetMessageSize(msg))
}

func headersLength(headers []kafka.Header) int {
	length := 0
	for _, header := range headers {
		length += len(header.Key) + len(header.Value)
	}

	return length
}
