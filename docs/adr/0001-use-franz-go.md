# Use franz-go as Kafka client

We need a Go library to produce messages to Kafka. franz-go is a pure-Go Kafka client with no C dependencies, high performance, and active maintenance. It supports all producer features we need (acks, batching, timeouts) and has a clean API.

**Considered Options**: `segmentio/kafka-go` (simpler API but less mature producer), `confluentinc/confluent-kafka-go` (wraps librdkafka, C dependency, heavier build). franz-go gives us the best balance of performance, simplicity, and build portability.

## Acks levels and idempotency

Amended (2026-08-24): the original default of `acks=all` was traded for `acks=1`. franz-go's idempotent producer requires `acks=all`; keeping idempotency enabled would force the higher-latency setting, so the initial release disables idempotent write to keep `acks=1`.

The trade-off is now explicit rather than buried in code:

- **`acks=1` (default)**: lower latency, at-most-once-ish semantics - records can be lost on leader failover. Idempotency is disabled because it cannot be combined with this ack level.
- **`acks=all` (opt-in via `-acks all`)**: broker-acknowledged durability; franz-go then enables its idempotent producer automatically. Higher latency per record.

The coupling is an invariant owned by `producer.New`: idempotency is derived from the ack level, never set independently, so callers cannot construct a combination franz-go rejects. Other transport tuning (linger, buffered records) is named in `Options` with today's values as defaults but stays code-level until a real need for CLI exposure appears.

Consequence worth naming: with idempotency off under `acks=1`, retries can duplicate or drop records during failover. This tool generates disposable test data, so that trade is deliberate; consumers needing stronger guarantees should run with `-acks all`.

