package otelkafka

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// MessageCarrier implements the opentelemetry-go propagation.TextMapCarrier interface
// for confluent-kafka-go messages.
type MessageCarrier struct {
	msg *kafka.Message
}

// NewMessageCarrier creates a new MessageCarrier for the given kafka.Message.
func NewMessageCarrier(msg *kafka.Message) *MessageCarrier {
	return &MessageCarrier{
		msg: msg,
	}
}

// Get returns the value associated with the passed key.
func (c *MessageCarrier) Get(key string) string {
	for _, h := range c.msg.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}

	return ""
}

// Set sets the key-value pair in the carrier.
// If a key already exists, its value is updated.
func (c *MessageCarrier) Set(key string, value string) {
	for i := 0; i < len(c.msg.Headers); i++ {
		if c.msg.Headers[i].Key == key {
			c.msg.Headers = append(c.msg.Headers[:i], c.msg.Headers[i+1:]...)
			i--
		}
	}

	c.msg.Headers = append(c.msg.Headers, kafka.Header{
		Key:   key,
		Value: []byte(value),
	})
}

// Keys returns a slice of all keys in the carrier.
func (c *MessageCarrier) Keys() []string {
	out := make([]string, len(c.msg.Headers))
	for i, h := range c.msg.Headers {
		out[i] = h.Key
	}

	return out
}
