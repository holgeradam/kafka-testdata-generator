# Use franz-go as Kafka client

We need a Go library to produce messages to Kafka. franz-go is a pure-Go Kafka client with no C dependencies, high performance, and active maintenance. It supports all producer features we need (acks, batching, timeouts) and has a clean API.

**Considered Options**: `segmentio/kafka-go` (simpler API but less mature producer), `confluentinc/confluent-kafka-go` (wraps librdkafka, C dependency, heavier build). franz-go gives us the best balance of performance, simplicity, and build portability.
