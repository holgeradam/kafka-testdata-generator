# Single payload pipeline with pluggable output sink

## Status

Accepted (2026-08-23)

## Context

The CLI had two runner functions, `runDryRun` and `runProduce`, that were the same loop written twice: Count guard, context cancellation check, Payload generation, Key extraction, marshal, stats. Roughly 80% shared skeleton. The copies had already diverged - Dry run printed the extracted Key to stderr while produce mode marshaled it to bytes - a latent bug factory. Every loop concern (signal handling, rate limiting, stats) existed in two places; the signal-handling bug fixed in commit 2fb3a85 had to be fixed twice.

The produce path was untestable without a live broker because the Kafka client call sat inline in the loop with no seam for a fake. Each runner also took 7 positional parameters (Data Clumps).

## Decision

One deep **Pipeline module** (`internal/pipeline`) owns a run: generate each Payload from the Message schema, extract the Key, marshal JSON once at a single call site, deliver bytes to an Output sink, honour rate limiting and context cancellation, and accumulate stats.

- Interface: `pipeline.New(cfg, sink)` + `Run(ctx) Stats`. The Config struct replaces the parameter clumps.
- **Output sink seam**: one small interface with two production adapters - `StdoutSink` (Dry run: NDJSON lines to stdout) and `KafkaSink` (wraps `internal/producer`). Tests add a fake sink as the second adapter, making produce-path behaviours hermetic.
- **Dependency direction**: main.go parses flags and constructs the sink ("accept dependencies, don't create them"). Pipeline never imports franz-go or knows brokers exist. Signal wiring, the pre-loop broker Ping, and flag validation stay at the process edge in main.go.
- Stats return from `Run`; printing stays in main.
- Dry run keeps the `Key:` stderr echo (documented in ADR-0003).

Deliberately deferred: an Encoder/wire-format seam. Marshalling must stay at exactly one call site inside the Pipeline so a future Format seam (JSON vs AVRO, see issue for AVRO support) can be introduced when its second adapter actually exists - not before (one adapter = hypothetical seam).

Rate limiting remains inline `time.Sleep`. A sleeper seam is hypothetical until pacing tests demand it; revisit then.

## Consequences

- Loop concerns concentrate: signal handling, Key extraction, stats exist once (locality).
- Produce-path SIGINT, send-failure counting, and missing-Key skip become unit-testable through a fake sink (leverage).
- The twin-loop duplication class of bug is structurally removed.
- Existing E2E tests against the binary keep passing unchanged; new unit tests target the Pipeline interface.
