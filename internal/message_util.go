package internal

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// GetMessageSize calculates the size of a kafka.Message in bytes.
// It sums the lengths of the message's key, value, and all header keys and values.
func GetMessageSize(msg *kafka.Message) (size int) {
	for _, header := range msg.Headers {
		size += len(header.Key) + len(header.Value)
	}

	return size + len(msg.Value) + len(msg.Key)
}
