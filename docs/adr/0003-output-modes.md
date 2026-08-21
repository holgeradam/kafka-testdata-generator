# Output modes and continuous mode

By default, generated payloads are produced to Kafka and aggregated stats (total, acked, failed) are printed to stderr. The `-dry-run` flag switches to console-only output: payloads stream to stdout as NDJSON (one JSON object per line) with no Kafka interaction; any Kafka-related flags produce a warning and are disregarded. Stats still print to stderr.

Streaming is one-by-one in both modes, enabling piping: `kafka-testdata-generator -dry-run -count 10 | jq '.orderId'` works as expected.

Continuous mode is triggered by `-count 0`, which runs indefinitely until interrupted (Ctrl+C). This is documented but requires no extra flag.
