# kafka-testdata-generator

A CLI tool that reads an AsyncAPI specification, generates random test data conforming to the schema, and produces it to Kafka topics.

## Features

- Parses AsyncAPI 2.x specifications (YAML and JSON)
- Generates realistic test data based on JSON Schema constraints
- Supports all standard JSON Schema types and formats
- Produces to Kafka with configurable broker and topic
- Produces Confluent-framed Avro from an explicit value avsc via a Schema Registry
- Dry-run mode for console output without Kafka
- Deterministic generation with seed control
- Rate limiting for controlled test data production

## Installation

```bash
go install github.com/holgeradam/kafka-testdata-generator/cmd/kafka-testdata-generator@latest
```

Or build from source:

```bash
git clone https://github.com/holgeradam/kafka-testdata-generator.git
cd kafka-testdata-generator
go build -o kafka-testdata-generator ./cmd/kafka-testdata-generator
```

## Usage

### Basic Usage

Generate 10 test records (default) and produce to Kafka:

```bash
kafka-testdata-generator -spec examples/order.asyncapi.yaml -channel orders.created
```

### Dry Run (Console Output)

Generate records without producing to Kafka:

```bash
kafka-testdata-generator -spec examples/order.asyncapi.yaml -channel orders.created -dry-run
```

### Piping Output

Stream one record per line for piping to other tools:

```bash
kafka-testdata-generator -spec examples/order.asyncapi.yaml -channel orders.created -dry-run -count 5 | jq '.orderId'
```

### Continuous Mode

Generate records indefinitely until interrupted (Ctrl+C):

```bash
kafka-testdata-generator -spec examples/order.asyncapi.yaml -channel orders.created -count 0
```

### Deterministic Generation

Use a fixed seed **and** a fixed clock for fully reproducible results. The seed
drives the random values, while the clock (`-now`) anchors date-formatted
fields, so byte-identical output requires both to be pinned. Reproducibility
holds within one version of the tool: the values a seed produces may change
between releases.

Dates and timestamps fall within the 365 days before `-now`.

```bash
kafka-testdata-generator -spec examples/order.asyncapi.yaml -channel orders.created -seed 12345 -now 2026-01-02T03:04:05Z -dry-run
```

### Rate Limiting

Control the rate of record generation:

```bash
kafka-testdata-generator -spec examples/order.asyncapi.yaml -channel orders.created -rate 100ms
```

### Acknowledgement Level

Choose the broker acknowledgement level (see "Acks and Durability"):

```bash
kafka-testdata-generator -spec examples/order.asyncapi.yaml -channel orders.created -acks all
```

### AVRO Wire Format

Produce Confluent-framed Avro (magic byte `0x00` + registry schema ID + Avro binary) to a
Schema Registry-backed topic, using an explicit value avsc:

```bash
kafka-testdata-generator -spec examples/order.asyncapi.yaml -channel orders.created \
  -format avro -avro-schema order.avsc -registry http://localhost:8081
```

- Generation follows the avsc, not the AsyncAPI JSON Schema.
- The value avsc is registered under `<channel>-value`; the returned schema ID is what gets
  framed on the wire, so any Confluent-compatible consumer can deserialize the records.
- `-registry` is required only when producing (never in `-dry-run`); under `-format json` it is
  rejected.
- Pass `-avro-key-schema key.avsc` to generate message keys from a key avsc: it registers under
  `<channel>-key` and each key is framed with its own schema ID, so consumers deserialize it
  against the key avsc. Without it, records are payload-only (null key).
- Dry run renders generated avro values in the Avro JSON encoding - the readable spec-defined text
  form, with logical types in their human-readable representation (dates as calendar days,
  timestamps as ISO 8601 instants, decimals as base-10 strings) - straight from the local avsc,
  never contacting a registry; a supplied `-registry` draws the standard dry-run warning and is
  ignored.
- Spec key bindings are ignored under `-format avro` (warning), and `-key` field extraction does
  not apply to AVRO (flag error): under `-format avro` the key comes exclusively from
  `-avro-key-schema`.

## CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `-spec` | (required) | Path to AsyncAPI spec file |
| `-channel` | (required) | Kafka topic/channel to produce to |
| `-broker` | `localhost:9092` | Kafka broker address |
| `-count` | `10` | Number of records to generate (0 = infinite) |
| `-rate` | `10ms` | Minimum time between messages |
| `-key` | `` | Field name to extract as Kafka message key (`-format json` only; overrides binding-derived keys; see Key section) |
| `-dry-run` | `false` | Generate without producing to Kafka |
| `-seed` | current time | Random seed for reproducibility |
| `-now` | current time | Clock for date fields (RFC3339) |
| `-acks` | `1` | Kafka acknowledgement level: `1` (leader) or `all` (all in-sync replicas) |
| `-format` | `json` | Output wire format: `json` (default) or `avro` |
| `-avro-schema` | `` | Path to value avsc file (required with `-format avro`) |
| `-avro-key-schema` | `` | Path to key avsc file (the AVRO key source; mutually exclusive with `-key`) |
| `-registry` | `` | Confluent Schema Registry base URL (required with `-format avro` when producing) |

### Acks and Durability

When producing to Kafka you can choose the acknowledgment level with `-acks`:

- **`-acks 1` (default)**: the leader acknowledges after writing the record locally. Lower latency, but records can be lost if the leader fails over before replicas catch up.
- **`-acks all`**: every in-sync replica must acknowledge before success is reported. Stronger durability at higher latency, and it enables Kafka's idempotent producer (server-side duplicate suppression).

This tool generates disposable test data, so `1` is a sensible default; use `all` when the produced records need to survive a broker failover.

### Key source and serialization

In **JSON mode**, the key source is chosen in this order:

1. **`-key fieldName`** (CLI override): extracts the named top-level field from the generated payload. When both `-key` and a binding-derived key are present, the CLI flag wins (with a warning).
2. **`message.bindings.kafka.key`** (AsyncAPI binding): generates a key value from the message binding's schema independently of payload fields, using the same generator as the payload.
3. **Neither**: produces a null key (Kafka convention, random partition), with an info message on stderr.

JSON key bytes are serialized as plain-scalar values: a string as UTF-8 bytes (e.g. `cust-1`), a number as its decimal text, and an object or array as JSON. This matches standard Kafka key conventions where the key is the raw serialized value, not a JSON wrapper.

In **AVRO mode** the key comes exclusively from `-avro-key-schema`: a key value is generated from the
key avsc per record, registered under `<channel>-key`, and framed like the payload (magic byte +
key schema ID + Avro binary), so a Confluent consumer decodes it against the key avsc. Field
extraction does not apply here - `-key` under `-format avro` is a flag error (as is combining it
with `-avro-key-schema`), and spec key bindings are ignored with a warning. With no `-avro-key-schema`,
records are payload-only (null key).

## AsyncAPI Specification

The tool reads AsyncAPI 2.x specifications and extracts message schemas from channels. It supports:

- `publish` and `subscribe` operations
- Channel-level message definitions
- `$ref` references to component messages
- Nested JSON Schema objects and arrays
- All standard JSON Schema types: `string`, `integer`, `number`, `boolean`, `array`, `object`
- Format constraints: `uuid`, `email`, `date-time`, `date`, `uri`, `url`
- Numeric constraints: `minimum`, `maximum`, `exclusiveMinimum`, `exclusiveMaximum`
- Array constraints: `minItems`, `maxItems`
- Object constraints: `required` fields
- Enum values and const
- `allOf`, `oneOf`, `anyOf` composition

### Supported Pattern Syntax

`string` fields with a `pattern` are synthesized to conform to that regex.
The generator supports the following documented subset; anything else
surfaces a typed `UnsupportedPatternError` rather than a non-conforming value:

| Construct | Meaning | Supported? |
|-----------|---------|-----------|
| `abc` | literal characters | ✓ |
| `[A-Z]`, `[a-z]`, `[0-9]` | character classes (and ranges thereof) | ✓ |
| `\d`, `\w`, `\s` | digit, word, whitespace | ✓ |
| `{n}` | exactly `n` times | ✓ |
| `{n,m}` | between `n` and `m` times | ✓ |
| `*`, `+`, `?` | zero-or-more, one-or-more, optional | ✓ |
| `(...)` | group | ✓ |
| `\|` | alternation | ✓ |
| `^`, `$` | anchors (ignored for synthesis) | ✓ |
| `.` | any character | ✗ |
| `[^...]` | negated class | ✗ |
| `\D`, `\W`, `\S`, `\b`, ... | other escapes | ✗ |
| `{n,}` | unbounded quantifier | ✗ |

### Field Name Heuristics

String fields without a `format` or `pattern` get realistic values chosen from
their field name, in both wire formats. The name is split into words
(`customerEmailAddress` -> `customer`, `email`, `address`; `customer_id` ->
`customer`, `id`) and rules match whole words or their regular plurals, in this
order:

| Word | Value |
|------|-------|
| `email` | email address |
| `id`, `uuid`, `guid` | UUID |
| `firstname`, or `first` + `name` | first name |
| `lastname`, `surname`, or `last` + `name` | surname |
| `name`, `fullname` | full name |
| `phone`, `telephone` | phone number |
| `city` | city name |
| `country` | country name |
| `street` | street address |
| `status` | status value |
| `description` | description |
| `currency` | ISO 4217 currency code |
| `url`, `uri` | URL |
| `sku` | SKU |

Other names get random text. Whole-word matching means `width` or `capacity`
stay random rather than becoming a UUID or a city. Array items, map values and
`oneOf`/`anyOf` or Avro union branches use the name of the enclosing field, so an
array named `emails` holds email addresses and a nullable `email` gets one when
not null.

## Statistics

The tool prints statistics to stderr after completion:

```
Stats [kafka]: total=10 acked=10 failed=0 elapsed=150ms
```

Or in dry-run mode:

```
Stats [dry-run]: total=10 acked=10 failed=0 elapsed=12ms
```

## Development

### Testing

Run unit and integration tests:

```bash
make test
```

### Testing with Kafka

Spin up a local Kafka cluster (KRaft mode, no Zookeeper), run the tool against it, then shut down:

```bash
make test-kafka
```

This will:
1. Start a single-node Kafka cluster in a container
2. Wait for Kafka to be healthy
3. Generate 5 test records and produce them to Kafka
4. Wait for you to press Enter
5. Shut down Kafka and wipe all data

The Makefile auto-detects your container runtime and uses **Podman** or **Docker**, whichever is installed.

### Docker Compose

The `docker-compose.yml` runs a single-node Kafka cluster in KRaft mode on `localhost:9092`. The cluster starts with a blank slate every time (no persistent volumes).

Works with both `docker compose` and `podman compose` (which delegates to an external compose provider such as `docker-compose`).

## Future Features

- Environment variable configuration
- SASL authentication and TLS support
- Topic auto-creation
- Configurable ack levels (`acks=all`)
- Advanced key extraction with JSON path

## License

This project is licensed under the Business Source License 1.1 (BSL 1.1).

- **Personal use**: Free
- **Professional/commercial use**: Requires a commercial license
- **Conversion**: After 4 years, each version converts to Apache License 2.0

See [LICENSE](LICENSE) for full terms.
