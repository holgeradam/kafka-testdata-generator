# Kafka Testdata Generator

A CLI tool that reads an AsyncAPI specification, generates random test data conforming to the schema, and produces it to Kafka topics.

## Language

**AsyncAPI spec**:
A YAML or JSON document describing Kafka topics and the messages on them. The tool reads this to determine what data to generate.
_Avoid_: schema, contract, API spec

**Kafka topic**:
The Kafka topic a run produces to, exactly one per run, named by `-topic`. The AsyncAPI spec describes it in an entry under its `channels:` key: the entry whose Kafka binding (`bindings.kafka.topic`) names it, or else the entry keyed by its name (AsyncAPI 2.x) or whose address it is (AsyncAPI 3.0). "Channel" is only AsyncAPI's syntax for that entry, never a name for the Kafka topic.
_Avoid_: channel, topic (alone)

**Topic parameter**:
A named placeholder in a Kafka topic's address template, such as `region` in `orders.{region}` (a 3.0 address, or a 2.x spec entry key without a Kafka binding). `-topic` fills it, the value must satisfy the parameter's declaration (a 3.0 enum, a 2.x schema, which must allow a string), and a declared payload location receives it in every message, checked before the run starts like a Key path.
_Avoid_: channel parameter, address parameter, variable

**Message**:
The meaningful content produced as one unit: a Key and a Payload. A run generates one message per Count, and a Kafka record carries it.
_Avoid_: event, record

**Kafka record**:
Kafka's content-agnostic transport unit: key bytes, value bytes and metadata, as the broker stores them. The term for technical contexts where the content does not matter: producing, framing, acknowledgement.
_Avoid_: message (in a technical context)

**Message type**:
A kind of message the spec declares for a Kafka topic, e.g. OrderCreated or OrderUpdated: its Message schema and its Key binding. A Kafka topic carries one or more, and each message is of one. In JSON mode each record's Message type is picked from the seeded stream, so a Kafka topic's Message types mix across its records; they must declare the same Key binding, or none, because a Key identifies one Entity across them. Under AVRO the avsc governs instead.
_Avoid_: event type, message schema, message (alone)

**Message schema**:
A JSON Schema embedded in the AsyncAPI spec as the payload of a Message type. Defines the structure and constraints of generated test data.
_Avoid_: payload schema

**Payload**:
The data part of a single generated message, conforming to the schema that governs the active Wire format (see Conformance).
_Avoid_: message, record, event (use "Payload" for the data, "message" for Key plus Payload, "Kafka record" for Kafka's transport unit)

**Conformance**:
The generator's output promise, defined per Wire format. Every Payload honors every constraint of the schema that governs that format - the chosen Message type's Message schema (JSON Schema) in JSON mode, the avsc in AVRO mode; anything the governing schema cannot honor stops the run with a typed error rather than emitting non-conforming data. Validation never runs in the generation path.
_Avoid_: payload validation, schema checking

**Key**:
The key of a message, paired with its Payload. It is generated from the **Key schema** and encoded by the Encoder, which owns Key encoding so the Pipeline never knows format conventions. With no Key schema the message carries a null Key. In JSON mode Key bytes are plain-scalar: string as UTF-8, number as decimal text, structured value as JSON; in AVRO mode the Key is Confluent-framed under `<topic>-key`, and a Dry run shows it in the Avro JSON encoding of the key avsc, like the Payload.
_Avoid_: partition key

**Key schema**:
The schema that governs Key generation, supplied per Wire format: `message.bindings.kafka.key` in JSON mode, the key avsc (`-avro-key-schema`) under AVRO. It describes the Key alone; nothing in it says which Payload field the Key corresponds to.
_Avoid_: key binding (JSON-only term), key avsc (AVRO-only term)

**Key plan**:
The run's rule for the Key: generate it from the Key schema and, when a **Key path** is configured, plant that value into the Payload so both hold it. With `-records-per-key` above 1 it reuses Keys, so an **Entity** recurs across records. Built once at the process edge, where it refuses a Key path the run cannot honour, then applied to each generated Payload.
_Avoid_: key source, key strategy

**Entity**:
The thing a Key identifies across messages, such as one order. With `-records-per-key N` each message starts a new Entity with probability 1/N and otherwise reuses the Key of one of the most recent Entities, so several messages of any Message type share an Entity's Key - N on average. The default of 1 gives every message its own Entity. No lifecycle order yet: an Entity's messages come in no particular order.
_Avoid_: aggregate, object

**Key path**:
Where in the Payload the generated Key is mirrored (`-keyPath`), as a dotted path with optional array indexing, e.g. `customer.id` or `items[0].sku`. Accepted only where generation guarantees a value in every message and the type there can hold the Key; both are checked before the run starts, against whichever schema language governs the Payload.

**Dry run**:
Mode where the tool generates messages and prints them to stdout without producing to Kafka. Kafka and registry-related flags are disregarded with a warning. Each Encoder renders its messages readably for the active Wire format; AVRO Dry run renders from the avsc without contacting a registry. When a Key is configured, its value is echoed to stderr ahead of the stats, in the same encoding the Payload is shown in: plain-scalar in JSON mode, the Avro JSON encoding of the key avsc under AVRO (a string Key therefore appears quoted).
_Avoid_: console mode, stdout mode

**Run plan**:
The validated description of one run, built from the command line before anything is generated: which spec and Kafka topic, the Wire format, Count and pacing, the generator, the **Key plan**, and the diagnostics the run carries. Building it performs no network I/O; the Output sink and the Encoder are constructed from it afterwards, so a rejected run never dials a broker or a registry.
_Avoid_: config, options, args

**Pipeline**:
The deep module driving a run: generates each message for the active Wire format, hands it to the format's Encoder for byte encoding, and delivers the bytes to the configured Output sink until Count is reached or the context is cancelled. Owns signal-safe looping, rate limiting, and stats. Format-blind: it never knows JSON from AVRO. Depends on a single-method **ValueGenerator** seam for Payload generation, and on an optional **Key plan** for the Key; the Wire format's generator and `*keyplan.Plan` satisfy them, and tests substitute fakes. It holds no schema of any language.
_Avoid_: runner, loop, producer loop

**ValueGenerator**:
The seam between the Pipeline and Payload generation: one method, `Value() (any, error)`, promises a Payload honouring the schema that governs the active Wire format (see Conformance), or a typed conformance error. The Wire format binds that schema when it builds the generator: the Message types' Message schemas in JSON mode, one picked per record, the value avsc in AVRO mode. Adapters pass the deletion test: one per Wire format in production, a fixed-payload fake in Pipeline tests. Error Paths are reported in JSON Path (RFC 9535) form rooted at `$`, e.g. `$.orderId` or `$.items[0].sku`, with no fabricated root name.
_Avoid_: generator interface, data source

**Synthesizer**:
The seeded, clock-aware source of every leaf value and random decision in a run, shared by the Payload and the Key. The schema walkers decide the shape a schema demands; the Synthesizer decides the values inside it, including readable values chosen from field names.
_Avoid_: faker, value source, random generator

**Encoder**:
The Wire-format seam that turns a generated message (Key + Payload) into the bytes of a Kafka record. One adapter exists per format: JsonEncoder for JSON mode (its Encode serves both Dry run and produce), and under AVRO two - AvroEncoder for producing, and AvroDisplayEncoder for Dry run. Each adapter owns how both the Key and the Payload are encoded for that format, and how they render for Dry run. AvroEncoder also owns the schema-registry interaction: it registers the exact value avsc under `<topic>-value` and, when a key avsc is supplied, the key avsc under `<topic>-key`, then frames payloads and keys with the registry-assigned schema IDs. AvroDisplayEncoder renders the Avro JSON encoding from the local avsc and never touches a registry. The Pipeline never sees format conventions.
_Avoid_: serializer, marshaler, codec

**Wire format**:
The byte shape of produced Kafka records and how messages render for Dry run. Today JSON (NDJSON); AVRO uses the Confluent wire format (magic byte + big-endian schema ID + Avro binary) and registers schemas in a registry. Each Wire format is one adapter that owns everything format-specific about a run: the rules about its own flags, which schema governs generation of the Payload (see Conformance) and of the Key, and the Encoders for produce and Dry run. The Run plan only picks the adapter by name.
_Avoid_: format, encoding, output format

**avsc**:
An explicit Apache Avro schema (JSON) supplied by the user for AVRO mode. It is always provided explicitly, never derived from the Message schema. In AVRO mode the avsc - a value avsc for the Payload and a key avsc for the Key - governs generation (see Conformance).
_Avoid_: avro schema (only when unambiguous), Avro serialization schema

**Avro model**:
The in-memory form of an avsc produced by `avro.Parse` in `internal/avro`: shared nodes for records, enums, and fixed, plus primitives, unions, arrays, maps, and the in-scope logical types (timestamp-*, local-timestamp-*, date, time-*, decimal, uuid); a logical type outside that set is ignored and its base type governs. The avsc is parsed once, by the same codec that encodes AVRO messages, and the model is built from that parse, so Dry run and produce always agree about which avsc they accept; anything the model cannot honour, including a known logical type the codec would silently drop, surfaces a `ParseError`. Its root type drives generation in AVRO mode (`avro.Generator`), its raw bytes are what gets registered, and the codec's parse is what records are encoded against, the way the Message schema drives JSON mode.
_Avoid_: parsed schema, avro schema model, generation model

**Output sink**:
The destination seam where generated bytes go: stdout as NDJSON in Dry run, a Kafka topic otherwise. Adapters sit behind one small interface; tests may substitute fakes.
_Avoid_: destination, target, backend

**Count**:
Number of messages to generate. Default is 10. Value of 0 means run indefinitely until interrupted.
_Avoid_: iterations
