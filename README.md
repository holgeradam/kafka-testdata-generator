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
kafka-testdata-generator -spec examples/order.asyncapi.yaml -topic orders.created
```

### Dry Run (Console Output)

Generate records without producing to Kafka:

```bash
kafka-testdata-generator -spec examples/order.asyncapi.yaml -topic orders.created -dry-run
```

### Piping Output

Stream one record per line for piping to other tools:

```bash
kafka-testdata-generator -spec examples/order.asyncapi.yaml -topic orders.created -dry-run -count 5 | jq '.orderId'
```

### Continuous Mode

Generate records indefinitely until interrupted (Ctrl+C):

```bash
kafka-testdata-generator -spec examples/order.asyncapi.yaml -topic orders.created -count 0
```

### Deterministic Generation

Use a fixed seed **and** a fixed clock for fully reproducible results. The seed
drives the random values, while the clock (`-now`) anchors date-formatted
fields, so byte-identical output requires both to be pinned. Reproducibility
holds within one version of the tool: the values a seed produces may change
between releases.

Dates and timestamps fall within the 365 days before `-now`.

```bash
kafka-testdata-generator -spec examples/order.asyncapi.yaml -topic orders.created -seed 12345 -now 2026-01-02T03:04:05Z -dry-run
```

### Rate Limiting

Control the rate of record generation:

```bash
kafka-testdata-generator -spec examples/order.asyncapi.yaml -topic orders.created -rate 100ms
```

### Acknowledgement Level

Choose the broker acknowledgement level (see "Acks and Durability"):

```bash
kafka-testdata-generator -spec examples/order.asyncapi.yaml -topic orders.created -acks all
```

### AVRO Wire Format

Produce Confluent-framed Avro (magic byte `0x00` + registry schema ID + Avro binary) to a
Schema Registry-backed topic, using an explicit value avsc:

```bash
kafka-testdata-generator -spec examples/order.asyncapi.yaml -topic orders.created \
  -format avro -avro-schema order.avsc -registry http://localhost:8081
```

- Generation follows the avsc, not the AsyncAPI JSON Schema.
- The value avsc is registered under `<topic>-value`; the returned schema ID is what gets
  framed on the wire, so any Confluent-compatible consumer can deserialize the records.
- `-registry` is required only when producing (never in `-dry-run`); under `-format json` it is
  rejected.
- Pass `-avro-key-schema key.avsc` to generate message keys from a key avsc: it registers under
  `<topic>-key` and each key is framed with its own schema ID, so consumers deserialize it
  against the key avsc. Without it, records are payload-only (null key).
- Dry run renders generated avro values in the Avro JSON encoding - the readable spec-defined text
  form, with logical types in their human-readable representation (dates as calendar days,
  timestamps as ISO 8601 instants, decimals as base-10 strings) - straight from the local avsc,
  never contacting a registry; a supplied `-registry` draws the standard dry-run warning and is
  ignored. The Key echo on stderr uses the same encoding against the key avsc, so a string Key
  appears quoted (`Key: "cust-1"`) and a record Key as an Avro JSON object.
- Logical types: `date`, `time-millis`, `time-micros`, `timestamp-millis`, `timestamp-micros`,
  `local-timestamp-millis`, `local-timestamp-micros`, `decimal` and `uuid` generate values of
  their semantic kind. Any other logical type, such as `timestamp-nanos`, `big-decimal` or
  `duration`, is ignored and its base type governs, as the Avro spec requires of readers; the
  encoded bytes stay registry-valid because the serializer treats it the same way. A supported
  logical type on the wrong base type (say `date` on a string) is a malformed avsc and fails.
- Spec key bindings are ignored under `-format avro` (warning): the AVRO key comes exclusively
  from `-avro-key-schema`, which `-keyPath` requires there.

## CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `-spec` | (required) | Path to AsyncAPI spec file |
| `-topic` | (required) | Kafka topic to produce to |
| `-broker` | `localhost:9092` | Kafka broker address |
| `-count` | `10` | Number of records to generate (0 = infinite) |
| `-rate` | `10ms` | Minimum time between messages |
| `-keyPath` | `` | Path in the payload where the generated Key is planted, e.g. `customer.id` (requires a key schema) |
| `-dry-run` | `false` | Generate without producing to Kafka |
| `-seed` | random | Random seed for reproducibility |
| `-now` | current time | Clock for date fields (RFC3339) |
| `-acks` | `1` | Kafka acknowledgement level: `1` (leader) or `all` (all in-sync replicas) |
| `-format` | `json` | Output wire format: `json` or `avro` |
| `-avro-schema` | `` | Path to value avsc file (required with `-format avro`) |
| `-avro-key-schema` | `` | Path to key avsc file (the AVRO key schema) |
| `-registry` | `` | Confluent Schema Registry base URL (required with `-format avro` when producing) |

### Acks and Durability

When producing to Kafka you can choose the acknowledgment level with `-acks`:

- **`-acks 1` (default)**: the leader acknowledges after writing the record locally. Lower latency, but records can be lost if the leader fails over before replicas catch up.
- **`-acks all`**: every in-sync replica must acknowledge before success is reported. Stronger durability at higher latency, and it enables Kafka's idempotent producer (server-side duplicate suppression).

This tool generates disposable test data, so `1` is a sensible default; use `all` when the produced records need to survive a broker failover.

### Keys

The **key schema** generates the Key: `message.bindings.kafka.key` in JSON mode,
the key avsc (`-avro-key-schema`) under `-format avro`. With no key schema, records
carry a null key (Kafka convention, random partition), with an info message on stderr.

`-keyPath` mirrors the generated Key into the payload, so the record's key and the
payload field hold the same value:

```bash
kafka-testdata-generator -spec order.yaml -topic orders.created -keyPath customer.id -dry-run
```

The path is a dotted name with optional array indexing (`customer.id`, `items[0].sku`),
and it requires a key schema. Before the run starts, the path is checked against the
payload schema and rejected when a value there is not guaranteed in every record or
cannot hold the Key:

- a property that is not in its parent's `required`
- an array index at or beyond `minItems`
- a step under `oneOf` or `anyOf`, whose branch differs per record
- a path deeper than the `$ref` recursion budget, where generation truncates
- a type that the key schema's type does not fit (an `integer` key does fit a `number` field)

Each rejection names the step it failed at, so the run stops with a message rather
than producing records whose key is missing from the payload.

JSON key bytes are serialized as plain-scalar values: a string as UTF-8 bytes (e.g. `cust-1`),
a number as its decimal text, and an object or array as JSON. This matches standard Kafka key
conventions where the key is the raw serialized value, not a JSON wrapper. In **AVRO mode** the
Key comes from the key avsc, is registered under `<topic>-key` and framed like the payload.
`-keyPath` works there too and requires `-avro-key-schema`; since only record fields are
guaranteed in Avro, a path stepping into a union, an array or a map is rejected, and the type
at the path must be the key avsc's type (same primitive kind and logical overlay, or the same
full name for a record, enum or fixed).

> **Renamed:** `-key` became `-keyPath` and changed meaning. It used to extract a payload field
> as the key; it now plants the generated Key into the payload and requires a key schema.
> Passing `-key` stops with that explanation.

> **Renamed:** `-channel` became `-topic`. It names the Kafka topic to produce to, which a spec
> entry may bind under another key (`bindings.kafka.topic`). Passing `-channel` stops with that
> explanation.

## AsyncAPI Specification

The tool reads AsyncAPI 2.x specifications and extracts the message schemas the spec declares
for the Kafka topic. AsyncAPI describes a Kafka topic in an entry under its `channels:` key;
`-topic` finds the entry whose Kafka binding names it, or else the one keyed by its name:

```yaml
channels:
  orders-v1:                 # not a Kafka topic name: the binding below names it
    bindings:
      kafka:
        topic: orders        # -topic orders finds this entry
    publish:
      message: {...}
```

Every message the spec declares for the Kafka topic is one Message type: the `publish` and
`subscribe` operations' messages, each variant of a `message.oneOf`, and those of every entry
bound to the Kafka topic. A component message referenced more than once counts once. A run on a
Kafka topic with several Message types stops and lists them for now; mixing them is planned
(#74). AsyncAPI 3.0 documents are refused at load (3.0 support is tracked in #76).

Every spec mistake stops the run with an error naming the message: a broken `$ref`, a missing
payload, or a Key binding that is declared but not a schema. It supports:

- `publish` and `subscribe` operations, and `message.oneOf`
- `$ref` wherever AsyncAPI allows one: messages, bindings (entry and message level), the kafka
  binding, the Key schema and the payload schema, as JSON Pointers (`~1`, `~0` and
  percent-escapes decode)
- Recursive schemas (a `$ref` cycle), generated within a depth budget
- Nested JSON Schema objects and arrays
- All standard JSON Schema types: `string`, `integer`, `number`, `boolean`, `array`, `object`
- All 19 string formats JSON Schema 2020-12 defines: `date-time`, `date`, `time`,
  `duration`, `email`, `idn-email`, `hostname`, `idn-hostname`, `ipv4`, `ipv6`,
  `uri`, `uri-reference`, `iri`, `iri-reference`, `uuid`, `uri-template`,
  `json-pointer`, `relative-json-pointer`, `regex` (plus `url` as an alias of
  `uri`). A format outside that set, such as OpenAPI's `password` or `byte`, is
  an annotation rather than a constraint: it is ignored, and the field falls
  through to `pattern` and then the field-name heuristics
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
| `ip`, `ipAddress`, `ipv4` | IPv4 from the RFC 5737 documentation ranges |
| `username`, `login`, `handle`, or `user` + `name` | handle (`alice.roberts42`) |
| `filename`, or `file` + `name` | file name with extension (`report-4821.pdf`) |
| `countrycode`, or `country` + `code` | ISO 3166-1 alpha-2 code |
| `zip`, `postcode`, or `postal` + `code` | 5-digit postal code |
| `state`, `region`, `province` | region name |
| `address`, `addressline` | street address with city |
| `company`, `organization`, `employer` | company name |
| `title`, `jobtitle` | job title |
| `hostname`, `host`, `domain` | host under an RFC 2606 example domain |
| `language`, `locale` | BCP 47 tag (`en-US`) |
| `timezone`, `tz` | IANA time zone (`Europe/Berlin`) |
| `iban` | IBAN with valid ISO 13616 check digits |

A name word only means a person when no more specific category claims the
field: `companyName` is a company, `fileName` a file, `cityName` a city, while
`customerName` and a bare `name` are people.

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
