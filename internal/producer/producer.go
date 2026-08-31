package producer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Acks is the acknowledgement level requested from a Kafka broker.
type Acks int

const (
	// AcksLeader (1) asks the leader to acknowledge once it has written the
	// record locally. Lower latency, at-most-once-ish on leader failover.
	// Idempotency cannot be combined with this level, so it stays disabled.
	AcksLeader Acks = iota + 1
	// AcksAll asks every in-sync replica to acknowledge, giving broker-level
	// durability. franz-go enables its idempotent producer at this level.
	AcksAll
)

// String is the CLI spelling of the ack level.
func (a Acks) String() string {
	if a == AcksAll {
		return "all"
	}
	return "1"
}

// ParseAcks parses a CLI spelling of an ack level, case-insensitively.
func ParseAcks(s string) (Acks, error) {
	switch strings.ToLower(s) {
	case "1":
		return AcksLeader, nil
	case "all":
		return AcksAll, nil
	default:
		return 0, fmt.Errorf("invalid ack level %q: use 1 or all", s)
	}
}

// Options configures the producer. Every zero-field default equals the value
// the tool hardcoded before options existed, so an Options{} call behaves
// identically to the original producer.
type Options struct {
	// Acks is the acknowledgement level. Defaults to AcksLeader.
	Acks Acks
	// Linger is how long the producer batches before sending. Defaults to 10ms.
	Linger time.Duration
	// MaxBufferedRecords caps records buffered before Send blocks. Defaults to 1000.
	MaxBufferedRecords int
}

// DefaultOptions returns the pre-options hardcoded behaviour: acks=1, linger
// 10ms, max buffered records 1000.
func DefaultOptions() Options {
	return Options{
		Acks:               AcksLeader,
		Linger:             10 * time.Millisecond,
		MaxBufferedRecords: 1000,
	}
}

// Producer wraps a franz-go client for producing messages.
type Producer struct {
	client *kgo.Client
	opts   Options
}

// newClient is the kgo client constructor, overridable in tests to observe the
// options New builds without requiring a broker.
var newClient = kgo.NewClient

// ackConfig derives the franz-go acknowledgement level and idempotency flag from
// an Options. Idempotency is owned here, never an independent knob: it is forced
// on exactly when the brokered acks=all level requires it, so callers cannot
// express the combination franz-go rejects.
func ackConfig(o Options) (acks kgo.Acks, disableIdempotency bool) {
	if o.Acks == AcksAll {
		return kgo.AllISRAcks(), false
	}
	return kgo.LeaderAck(), true
}

// buildOpts maps Options onto the franz-go option set New passes to the client.
func buildOpts(o Options) []kgo.Opt {
	acks, disableIdempotency := ackConfig(o)
	opts := []kgo.Opt{
		kgo.RequiredAcks(acks),
		kgo.ProducerLinger(o.Linger),
		kgo.MaxBufferedRecords(o.MaxBufferedRecords),
	}
	// Idempotency is coupled to the ack level (see ackConfig): we never set
	// DisableIdempotentWrite independently of the acks decision.
	if disableIdempotency {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}
	return opts
}

// New creates a new Producer connected to the given broker. Using zero Options
// behaves identically to the original hardcoded producer.
//
// acks/idempotency coupling: franz-go's idempotent producer requires acks=all.
// Rather than expose idempotency as an independent knob that callers could set
// into a combination franz-go rejects (issue #7), New derives it from the ack
// level via ackConfig and never sets it independently. Default acks=1 therefore
// ships with idempotent write disabled (see ADR-0001).
func New(broker string, opts Options) (*Producer, error) {
	def := DefaultOptions()
	if opts.Acks == 0 {
		opts.Acks = def.Acks
	}
	if opts.Linger == 0 {
		opts.Linger = def.Linger
	}
	if opts.MaxBufferedRecords == 0 {
		opts.MaxBufferedRecords = def.MaxBufferedRecords
	}

	clientOpts := append([]kgo.Opt{kgo.SeedBrokers(broker)}, buildOpts(opts)...)
	client, err := newClient(clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating kafka client: %w", err)
	}
	return &Producer{client: client, opts: opts}, nil
}

// Ping checks if the broker is reachable.
func (p *Producer) Ping(ctx context.Context) error {
	return p.client.Ping(ctx)
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
