package producer

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer wraps a franz-go client for producing messages.
type Producer struct {
	client *kgo.Client
}

// New creates a new Producer connected to the given broker.
func New(broker string) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.DisableIdempotentWrite(),
		kgo.MaxBufferedRecords(1000),
		kgo.ProducerLinger(10*time.Millisecond),
	)
	if err != nil {
		return nil, fmt.Errorf("creating kafka client: %w", err)
	}
	return &Producer{client: client}, nil
}

// Send produces a single message to the given topic with the provided key and value.
func (p *Producer) Send(ctx context.Context, topic string, key, value []byte) error {
	record := &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: value,
	}

	results := p.client.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("producing message: %w", err)
	}
	return nil
}

// Close closes the producer and flushes remaining messages.
func (p *Producer) Close() {
	p.client.Close()
}
