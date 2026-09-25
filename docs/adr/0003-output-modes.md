# Output modes and continuous mode

By default, generated payloads are produced to Kafka and aggregated stats (total, acked, failed) are printed to stderr. The `-dry-run` flag switches to console-only output: payloads stream to stdout as NDJSON (one JSON object per line) with no Kafka interaction; any Kafka-related flags produce a warning and are disregarded. Stats still print to stderr.

Streaming is one-by-one in both modes, enabling piping: `kafka-testdata-generator -dry-run -count 10 | jq '.orderId'` works as expected.

Continuous mode is triggered by `-count 0`, which runs indefinitely until interrupted (Ctrl+C). This is documented but requires no extra flag.

In Dry run, when `-key` is set, the extracted Key value is echoed to stderr ahead of the stats. Stderr otherwise carries only warnings and stats; stdout carries only Payload NDJSON lines.

Amended (2026-09-25, issue #92): a record's Headers are echoed to stderr too, as `Headers: {...}` after its Key, a JSON object of each header's text in name order and a null header as `null`. stdout still carries only Payload NDJSON lines, so pipelines such as `| jq` are unaffected.
