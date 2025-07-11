package internal

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func GetMessageSize(msg *kafka.Message) (size int) {
	for _, header := range msg.Headers {
		size += len(header.Key) + len(header.Value)
	}

	return size + len(msg.Value) + len(msg.Key)
}
