# kafka-testdata-generator

A CLI tool that reads an AsyncAPI specification, generates random test data conforming to the schema, and produces it to Kafka topics.

## Features

- Parses AsyncAPI 2.x and 3.0 specifications (YAML and JSON)
- Generates realistic test data based on JSON Schema constraints
- Supports all standard JSON Schema types and formats
- Produces to Kafka with configurable broker and topic
- Produces Confluent-framed Avro via a Schema Registry, from Avro payloads in the spec or an
  explicit value avsc
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
Schema Registry-backed topic. A spec that declares its payloads in Avro needs no AVRO flags:

```yaml
channels:
  orders:
    address: orders.created
    messages:
      created:
        bindings: {kafka: {key: {type: string, logicalType: uuid}}}   # the key avsc
        payload:
          schemaFormat: 'application/vnd.apache.avro;version=1.9.0'  # 2.x: message.schemaFormat
          schema: {$ref: '#/components/schemas/OrderCreated'}        # the value avsc
```

```bash
kafka-testdata-generator -spec orders.yaml -topic orders.created -registry http://localhost:8081
```

For a spec whose payloads are JSON Schema, pass the value avsc as a file:

```bash
kafka-testdata-generator -spec examples/order.asyncapi.yaml -topic orders.created \
  -avro-schema order.avsc -registry http://localhost:8081
```

- The Wire format follows wherever the Payload's schema comes from: Avro payloads in the spec,
  or `-avro-schema`, mean avro. `-format` is optional and only confirms it; `-format json`
  against Avro payloads or beside `-avro-schema` stops the run. One source per schema:
  `-avro-schema` beside Avro payloads, or `-avro-key-schema` beside an Avro Key binding, stops
  the run naming both.
- Avro `schemaFormat`s read: `application/vnd.apache.avro[+json|+yaml];version=1.x.y`. `$ref`s
  inside the avsc are expanded, a named type referenced again becomes a reference by its full
  name, and a `$ref` cycle is refused (Avro expresses recursion by name).
- A Kafka topic has one payload format: Message types mixing Avro and JSON Schema stop the run.
  Several Avro Message types are mixed per record, as in JSON mode, and register as a union
  (TopicNameStrategy): each record registers under its full name, a named type they define
  identically (say `com.acme.Address`) registers once under its full name and is referenced,
  and `<topic>-value` holds the union of the records' names, referencing each. Every record is
  framed with the union's ID. A named type defined differently in two Message types stops the
  run naming both. The Message types must declare the same Key binding, or none, and
  `-keyPath` and Topic parameter locations must hold in each. Dry run shows each record alone,
  without the union's wrapper.
- The Kafka message binding's registry fields must match the Confluent framing:
  `schemaIdLocation: payload`, `schemaIdPayloadEncoding: confluent` (or `4`), and
  `schemaLookupStrategy: TopicNameStrategy` (or `TopicIdStrategy`); anything else stops the run.
- Generation follows the avsc, not the AsyncAPI JSON Schema.
- The value avsc is registered under `<topic>-value`; the returned schema ID is what gets
  framed on the wire, so any Confluent-compatible consumer can deserialize the records.
- `-registry` is required only when producing (never in `-dry-run`); in a JSON run it is
  rejected.
- The key avsc, the spec's Avro Key binding or `-avro-key-schema key.avsc`, generates message
  keys: it registers under `<topic>-key` and each key is framed with its own schema ID, so
  consumers deserialize it against the key avsc. Without one, records are payload-only (null
  key).
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
- Beside JSON Schema payloads, spec key bindings are ignored under AVRO (warning): the AVRO key
  then comes exclusively from `-avro-key-schema`, which `-keyPath` requires there.

## CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `-spec` | (required) | Path to AsyncAPI spec file |
| `-topic` | (required) | Kafka topic to produce to |
| `-broker` | `localhost:9092` | Kafka broker address |
| `-count` | `10` | Number of records to generate (0 = infinite) |
| `-rate` | `10ms` | Minimum time between messages |
| `-keyPath` | `` | Path in the payload where the generated Key is planted, e.g. `customer.id` (requires a key schema) |
| `-records-per-key` | `1` | Average number of records sharing one Key, i.e. one Entity (requires a key schema above 1) |
| `-dry-run` | `false` | Generate without producing to Kafka |
| `-seed` | random | Random seed for reproducibility |
| `-now` | current time | Clock for date fields (RFC3339) |
| `-acks` | `1` | Kafka acknowledgement level: `1` (leader) or `all` (all in-sync replicas) |
| `-format` | inferred | Wire format: `json` or `avro`; inferred from the spec's payloads and `-avro-schema` when omitted |
| `-avro-schema` | `` | Path to value avsc file, for a spec whose payloads are JSON Schema; makes the run AVRO |
| `-avro-key-schema` | `` | Path to key avsc file (the AVRO key schema), unless the spec declares an Avro Key binding |
| `-registry` | `` | Confluent Schema Registry base URL (required to produce AVRO) |

### Acks and Durability

When producing to Kafka you can choose the acknowledgment level with `-acks`:

- **`-acks 1` (default)**: the leader acknowledges after writing the record locally. Lower latency, but records can be lost if the leader fails over before replicas catch up.
- **`-acks all`**: every in-sync replica must acknowledge before success is reported. Stronger durability at higher latency, and it enables Kafka's idempotent producer (server-side duplicate suppression).

This tool generates disposable test data, so `1` is a sensible default; use `all` when the produced records need to survive a broker failover.

### Keys

The **key schema** generates the Key: `message.bindings.kafka.key` in JSON mode,
the key avsc under AVRO (the spec's Avro Key binding, or `-avro-key-schema`). With no key schema, records
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
`-keyPath` works there too and requires a key avsc; since only record fields are
guaranteed in Avro, a path stepping into a union, an array or a map is rejected, and the type
at the path must be the key avsc's type (same primitive kind and logical overlay, or the same
full name for a record, enum or fixed).

`-records-per-key N` reuses Keys, so one Entity - one order, say - carries several records, as it
does on a real Kafka topic: each record starts a new Entity with probability 1/N and otherwise
reuses the Key of one of the 1,000 most recent Entities. Combined with several Message types, one
order's Key appears on its OrderCreated and its OrderUpdated records. `-keyPath` plants whichever
Key a record got, and it works the same with an AVRO key avsc. Records of an Entity come in no
particular order yet. The default of 1 gives every record a fresh Key.

```bash
kafka-testdata-generator -spec order.yaml -topic orders.created -records-per-key 4 -keyPath customer.id -dry-run
```

> **Renamed:** `-key` became `-keyPath` and changed meaning. It used to extract a payload field
> as the key; it now plants the generated Key into the payload and requires a key schema.
> Passing `-key` stops with that explanation.

> **Renamed:** `-channel` became `-topic`. It names the Kafka topic to produce to, which a spec
> entry may bind under another key (`bindings.kafka.topic`). Passing `-channel` stops with that
> explanation.

## AsyncAPI Specification

The tool reads AsyncAPI 2.x and 3.0 specifications and extracts the message schemas the spec
declares for the Kafka topic. AsyncAPI describes a Kafka topic in an entry under its `channels:`
key. `-topic` finds the entry whose Kafka binding names it, or else, in 2.x, the one keyed by its
name:

```yaml
channels:
  orders-v1:                 # not a Kafka topic name: the binding below names it
    bindings:
      kafka:
        topic: orders        # -topic orders finds this entry
    publish:
      message: {...}
```

In 3.0 the entry's `address` names the Kafka topic when no binding does; the entry's key never
does. An entry with a `null` or absent address names no Kafka topic, as 3.0 treats it as unknown:

```yaml
channels:
  ordersCreated:             # the channel id: not a Kafka topic name
    address: orders.created  # -topic orders.created finds this entry
    messages:
      OrderCreated: {$ref: '#/components/messages/OrderCreated'}
```

`examples/order.asyncapi.v3.yaml` restates `examples/order.asyncapi.yaml` in 3.0, and both give
the same output for the same `-seed` and `-now`.

Every message the spec declares for the Kafka topic is one Message type, across every entry
bound to it. In 2.x those are the `publish` and `subscribe` operations' messages and each variant
of a `message.oneOf`; in 3.0 every message in the entry's `messages` (operations are not read, as
the entry lists every message sent to it). A component message referenced more than once counts
once.

In JSON mode a Kafka topic's Message types are **mixed**: each record is of one, picked from the
seeded stream, so `-seed` still reproduces the exact sequence and a Kafka topic with one Message
type behaves as it always did. The Message types must declare the same Key binding, or none - a
Key identifies one Entity, such as one order, across its OrderCreated and OrderUpdated records -
and `-keyPath` must be guaranteed in every Message type's payload; either mistake stops the run
naming the Message types involved. Under AVRO from `-avro-schema` the file governs the payload, so
the spec's JSON Schema Message types play no part.

Every spec mistake stops the run with an error naming the message: a broken `$ref`, a missing
payload, a Key binding that is declared but not a schema, or a payload in a format the tool does
not read. It supports:

- `publish` and `subscribe` operations, and `message.oneOf` (2.x); channel `messages` (3.0)
- Message `traits`, inline or `$ref`, merged with JSON Merge Patch in the order listed, as each
  version specifies: in 2.x a trait overrides the message's own field, in 3.0 the message's own
  field wins. A trait can declare the Key binding, for instance
- Payloads in JSON Schema: no `schemaFormat`, the AsyncAPI Schema format of the spec's version
  (`application/vnd.aai.asyncapi[+json|+yaml];version=2.x.y`, or `3.x.y` in a 3.0 spec) or JSON
  Schema draft-07 (`application/schema+json;version=draft-07`, or `+yaml`). 2.x declares the
  format as the message's `schemaFormat`, 3.0 as a payload `{schemaFormat, schema}`. Any other
  format, such as a Protobuf payload, stops the run naming it. Avro payloads are read too, and
  make the run AVRO (see AVRO Wire Format)
- Templated Kafka topics such as `orders.{region}`, with their Topic parameters (below): a 3.0
  address, or a 2.x spec entry key
- `$ref` wherever AsyncAPI allows one: messages, bindings (entry and message level), the kafka
  binding, the Key schema and the payload schema, as JSON Pointers (`~1`, `~0` and
  percent-escapes decode)
- Recursive schemas (a `$ref` cycle), generated within a depth budget
- Nested JSON Schema objects and arrays
- All standard JSON Schema types: `string`, `integer`, `number`, `boolean`, `array`, `object`,
  `null`
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

### Headers

A message's `headers` schema, a JSON Schema object, generates **Headers** for every record of
that Message type, in JSON and AVRO mode alike (2.x `message.headers`, including from a trait,
or 3.0 `headers`, a Schema or a JSON Schema Multi Format Schema). Each property becomes one Kafka
record header, its value encoded plain-scalar as a JSON Key is - a string as UTF-8, a number as
decimal text, a boolean as `true`/`false`, an object or array as JSON text, `null` as a null
header - in name order:

```yaml
messages:
  created:
    headers:
      type: object
      required: [tenant, attempt]
      properties:
        tenant: {type: string, enum: [acme, globex]}
        attempt: {type: integer, minimum: 1, maximum: 3}
    payload: {$ref: '#/components/schemas/OrderCreated'}
```

A Dry run shows each record's Headers on stderr after its Key, stdout keeping the Payload lines
alone:

```
Headers: {"attempt":"2","tenant":"acme"}
{"orderId":"..."}
```

A Message type without `headers` gives records without headers. Headers that are not an object,
or in a format other than JSON Schema, stop the run naming the Message type. Under AVRO from
`-avro-schema` the records are of no Message type in the spec, so its headers are ignored with a
warning.

### Topic Parameters

A 3.0 address, or a 2.x spec entry key, can be a template with **Topic parameters**, such as
`region` in `orders.{region}`, for a family of Kafka topics. `-topic` fills them: `-topic
orders.eu` finds the entry and gives `region` the value `eu`. A 3.0 parameter declaring an `enum`
must hold the value, and a parameter declaring a payload `location` has the value planted into
every record, in every Message type:

```yaml
channels:
  regional:
    address: 'orders.{region}'
    parameters:
      region:
        enum: [eu, us]                        # -topic orders.apac stops the run
        location: '$message.payload#/region'  # every record carries region: "eu"
    messages:
      created: {$ref: '#/components/messages/OrderCreated'}
```

```bash
kafka-testdata-generator -spec regional.yaml -topic orders.eu -dry-run
```

In 2.x the template is the spec entry key, and a parameter declares a `schema` instead of an
`enum`. The value must conform to the whole schema (`enum`, `pattern`, `maxLength` and so on), and
since a Topic parameter is a string, a schema that does not allow a string stops the run. A spec
entry that declares `bindings.kafka.topic` stands for that Kafka topic, taken literally, in both
versions, so its key is not a template:

```yaml
channels:
  orders.{region}:
    parameters:
      region:
        schema: {type: string, enum: [eu, us]}  # -topic orders.apac stops the run
        location: '$message.payload#/region'    # every record carries region: "eu"
    publish:
      message: {$ref: '#/components/messages/OrderCreated'}
```

Before any record is generated, the location is checked as `-keyPath` is: generation must put a
field there in every record (required at every step, no `oneOf`/`anyOf` on the way), and the value
must conform to that field's schema - its `pattern`, `enum`, `format`, `maxLength` and so on - so
every record still conforms. Under AVRO the location is walked through the value avsc
instead: record fields only, ending in a `string`, a `uuid` or an `enum` holding the value. Every
mistake stops the run with its own error:

- a value outside the parameter's `enum`, one its 2.x `schema` does not accept, or one the field
  there does not accept
- a 2.x parameter `schema` that does not allow a string, such as `type: integer`
- a location that is not guaranteed in some Message type, or that overlaps `-keyPath` or another
  parameter's location
- a header location (`$message.header#/...`): the tool does not plant Topic parameters into
  Headers yet
- a `-topic` that fills templates in different ways, such as `orders.eu` against both
  `orders.{region}` and `{env}.eu`, or against a template and a literal `orders.eu` address or key

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
