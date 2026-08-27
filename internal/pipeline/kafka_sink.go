package pipeline

import (
	"context"

	"github.com/holgeradam/kafka-testdata-generator/internal/producer"
)

// KafkaSink produces each Outgoing as a Kafka record to a fixed topic through
// a producer. It is the produce-mode adapter at the Output sink seam.
type KafkaSink struct {
	topic    string
	producer *producer.Producer
}

// NewKafkaSink wraps the given producer and sends records to topic.
func NewKafkaSink(topic string, producer *producer.Producer) *KafkaSink {
	return &KafkaSink{topic: topic, producer: producer}
}

// Send produces one record with the Outgoing's Key and Payload as value.
func (s *KafkaSink) Send(ctx context.Context, o Outgoing) error {
	return s.producer.Send(ctx, s.topic, o.Key, o.Payload)
}

// Close closes the underlying producer.
func (s *KafkaSink) Close() error {
	s.producer.Close()
	return nil
}
