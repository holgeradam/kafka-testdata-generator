# Kafka Testdata Generator

A CLI tool that reads an AsyncAPI specification, generates random test data conforming to the schema, and produces it to Kafka topics.

## Language

**AsyncAPI spec**:
A YAML or JSON document describing Kafka topics, their message schemas, and channel structure. The tool reads this to determine what data to generate.
_Avoid_: schema, contract, API spec

**Channel**:
A Kafka topic as described in the AsyncAPI spec. The tool produces to exactly one channel per run.
_Avoid_: topic (use "Kafka topic" when referring to the broker-level concept)

**Message schema**:
A JSON Schema embedded in the AsyncAPI spec's channel message payload. Defines the structure and constraints of generated test data.
_Avoid_: payload schema

**Payload**:
The data part of a single generated record, conforming to the schema that governs the active Wire format (see Conformance).
_Avoid_: message, record, event (use "payload" for the data, "message" for the Kafka envelope)

**Conformance**:
The generator's output promise, defined per Wire format. Every Payload honors every constraint of the schema that governs that format - the Message schema (JSON Schema) in JSON mode, the avsc in AVRO mode; anything the governing schema cannot honor stops the run with a typed error rather than emitting non-conforming data. Validation never runs in the generation path.
_Avoid_: payload validation, schema checking

**Key**:
The Kafka message key, paired with the Payload as one record. It is generated from the **Key schema** and encoded by the Encoder, which owns Key encoding so the Pipeline never knows format conventions. With no Key schema the record carries a null Key. In JSON mode Key bytes are plain-scalar: string as UTF-8, number as decimal text, structured value as JSON; in AVRO mode the Key is Confluent-framed under `<channel>-key`.
_Avoid_: partition key

**Key schema**:
The schema that governs Key generation, supplied per Wire format: `message.bindings.kafka.key` in JSON mode, the key avsc (`-avro-key-schema`) under AVRO. It describes the Key alone; nothing in it says which Payload field the Key corresponds to.
_Avoid_: key binding (JSON-only term), key avsc (AVRO-only term)

**Key plan**:
The run's rule for the Key: generate it from the Key schema and, when a **Key path** is configured, plant that value into the Payload so both hold it. Built once at the process edge, where it refuses a Key path the run cannot honour, then applied to each generated Payload.
_Avoid_: key source, key strategy

**Key path**:
Where in the Payload the generated Key is mirrored (`-keyPath`), as a dotted path with optional array indexing, e.g. `customer.id` or `items[0].sku`. Accepted only where generation guarantees a value in every record and the type there can hold the Key; both are checked before the run starts.

**Dry run**:
Mode where the tool generates records and prints them to stdout without producing to Kafka. Kafka and registry-related flags are disregarded with a warning. Each Encoder renders its records readably for the active Wire format; AVRO Dry run renders from the avsc without contacting a registry. When a Key is configured, its value is echoed to stderr ahead of the stats.
_Avoid_: console mode, stdout mode

**Pipeline**:
The deep module driving a run: generates each record for the active Wire format, hands it to the format's Encoder for byte encoding, and delivers the bytes to the configured Output sink until Count is reached or the context is cancelled. Owns signal-safe looping, rate limiting, and stats. Format-blind: it never knows JSON from AVRO. Depends on a single-method **ValueGenerator** seam for record generation, and on an optional **Key plan** for the Key; `*generator.Generator` and `*keyplan.Plan` satisfy them, and tests substitute fakes.
_Avoid_: runner, loop, producer loop

**ValueGenerator**:
The seam between the Pipeline and record generation: one method, `Value(schema) (any, error)`, promises a Payload honouring the Message schema or a typed conformance error. Adapters pass the deletion test: `*generator.Generator` in production, a fixed-payload fake in Pipeline tests. Error Paths are reported in JSON Path (RFC 9535) form rooted at `$`, e.g. `$.orderId` or `$.items[0].sku`, with no fabricated root name.
_Avoid_: generator interface, data source

**Synthesizer**:
The seeded, clock-aware source of every leaf value and random decision in a run, shared by the Payload and the Key. The schema walkers decide the shape a schema demands; the Synthesizer decides the values inside it, including readable values chosen from field names.
_Avoid_: faker, value source, random generator

**Encoder**:
The Wire-format seam that turns a generated record (Key + Payload) into bytes. One adapter exists per format: JsonEncoder for JSON mode (its Encode serves both Dry run and produce), and under AVRO two - AvroEncoder for producing, and AvroDisplayEncoder for Dry run. Each adapter owns how both the Key and the Payload are encoded for that format, and how they render for Dry run. AvroEncoder also owns the schema-registry interaction: it registers the exact value avsc under `<channel>-value` and, when a key avsc is supplied, the key avsc under `<channel>-key`, then frames payloads and keys with the registry-assigned schema IDs. AvroDisplayEncoder renders the Avro JSON encoding from the local avsc and never touches a registry. The Pipeline never sees format conventions.
_Avoid_: serializer, marshaler, codec

**Wire format**:
The byte shape of produced records and how they render for Dry run. Today JSON (NDJSON); AVRO uses the Confluent wire format (magic byte + big-endian schema ID + Avro binary) and registers schemas in a registry. The Wire format decides which schema governs generation (see Conformance).
_Avoid_: format, encoding, output format

**avsc**:
An explicit Apache Avro schema (JSON) supplied by the user for AVRO mode. It is always provided explicitly, never derived from the Message schema. In AVRO mode the avsc - a value avsc for the Payload and a key avsc for the Key - governs generation (see Conformance).
_Avoid_: avro schema (only when unambiguous), Avro serialization schema

**Avro model**:
The in-memory form of an avsc produced by `avro.Parse` in `internal/avro`: shared nodes for records, enums, and fixed, plus primitives, unions, arrays, maps, and the in-scope logical types (timestamp-*, local-timestamp-*, date, time-*, decimal, uuid); a logical type outside that set is ignored and its base type governs. Parsing delegates to the same gogen-avro parser the Confluent Go Avro encoder path uses, so a schema the encoder accepts parses identically into the model; anything the model cannot honour surfaces a `ParseError`. Its root type and raw bytes drive generation in AVRO mode (`avro.Generator`) and registry registration (`AvroEncoder`) the way the Message schema does in JSON mode.
_Avoid_: parsed schema, avro schema model, generation model

**Output sink**:
The destination seam where generated bytes go: stdout as NDJSON in Dry run, a Kafka topic otherwise. Adapters sit behind one small interface; tests may substitute fakes.
_Avoid_: destination, target, backend

**Count**:
Number of payloads to generate. Default is 10. Value of 0 means run indefinitely until interrupted.
_Avoid_: iterations, messages
