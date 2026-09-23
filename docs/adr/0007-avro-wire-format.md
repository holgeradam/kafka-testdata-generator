# AVRO as a second wire format behind an Encoder seam

Enterprise Kafka deployments hold AVRO topics under a Schema Registry. The tool produced only
JSON (NDJSON) by design: ADR-0004 deliberately deferred an Encoder seam until a second adapter
was real. AVRO is that second adapter, and this ADR records the design decisions that shape it.

## Decisions

### 1. Record-level Encoder seam

The seam ADR-0004 deferred now exists at the Pipeline's marshal call site. It is record-level -
it encodes the Key and the Payload together - because each Wire format couples Key encoding to
its own conventions; keeping Key inside the Encoder stops AVRO's key rules from leaking into the
format-blind Pipeline. One adapter per format: `JsonEncoder` (unchanged behaviour) and
`AvroEncoder`.

### 2. Confluent wire format

AVRO wire bytes use the Confluent wire format: magic byte `0x00`, a 4-byte big-endian schema ID
assigned by the registry, then the Avro binary-encoded datum. This is the format enterprise
consumers and Confluent-compatible registries (Karapace included) expect, and the format the
chosen schema-registry client produces.

### 3. Explicit avsc, generation follows the avsc

AVRO mode is a fully separate path: the user supplies an explicit avsc (never derived from the
AsyncAPI Message schema, because JSON Schema does not map losslessly to Avro). Generation in
AVRO mode follows the avsc directly - a value avsc for the Payload and a key avsc for the Key -
mirroring how applications actually produce records. The AsyncAPI JSON Schema payload governs
generation only in JSON mode.

Amended (2026-09-08, AVRO vertical 3): vertical 3 ships Payload-only. The value avsc drives
generation and encoding; the `-avro-key-schema` file is parsed and validated up front but its
encoding lands in the later Key vertical (5), so the Key keeps the plain-scalar or null contract
until then.

Amended (2026-09-11, AVRO vertical 5): the Key vertical lands. Under `-format avro` the Key is
generated from the key avsc (via the Pipeline's **KeyGenerator** seam), registered under
`<channel>-key`, and framed with its own schema ID exactly like the Payload. `-key` field
extraction does not apply to AVRO: it is a flag error on its own, and mutually exclusive with
`-avro-key-schema` when combined. With no key avsc, records are payload-only (null key).

Amended (2026-09-20, issue #45): the model gains the `uuid` logical type (string
base) and `local-timestamp-millis`/`-micros` (long base), the three the
serializer encodes beyond the original set. A logical type the model does not
know is no longer a parse error: it is ignored and the base type governs, as the
Avro spec requires of readers. The serializer treats such an overlay the same
way, so the bytes stay registry-valid: `timestamp-nanos` encodes as a long,
`big-decimal` as bytes, `duration` as a 12-byte fixed. A logical type the model
does know, declared on the wrong base type, is still a malformed avsc and stops
Parse.

### 4. Conformance is per Wire format

Conformance (ADR-0006) is now defined per format: JSON mode honors the Message schema; AVRO mode
honors the avsc. Whichever schema governs a mode, anything it cannot honor stops the run with a
typed error rather than emitting non-conforming data.

### 5. Registry client and serializer: pure-Go confluent-avro-go, franz-go stays

Franz-go stays the Kafka producer (ADR-0001). The registry client and the generic Avro encoder
come from `confluentinc/confluent-avro-go/v2` (Apache-2.0), a pure-Go module with no cgo or
librdkafka dependency. The whole interaction runs inside the `AvroEncoder`: one HTTP round-trip
to the registry registers/looks up the schema ID up front, then every record is framed with that
ID and Avro-encoded by the generic marshaller. Because the encoder registers and honors the
exact user avsc, the bytes that go on the wire are always registry-valid Confluent form.

Amended (2026-09-08, AVRO vertical 2): avsc parsing depends on `actgardner/gogen-avro/v10`
directly, because that is the exact schema parser the Confluent Go Avro serde delegates to.
Deferring the full `confluent-kafka-go` module to the serializer path kept the CGO/librdkafka
requirement and that module's large dependency tree out of the build until byte encoding is real,
while the parse model stays byte-for-byte consistent with what the serializer encodes against.

Amended (2026-09-08, AVRO vertical 3): the serializer path lands on `confluent-avro-go/v2` rather
than `confluent-kafka-go`'s serde/avro. Deriving a `schema.AvroSchema` from the serialized map
value - what that serde's map path enforces - cannot register an explicit user avsc for map
values, which ADR decision 3 requires. Generic encoding registers and honors the exact avsc
instead, and drops the cgo/librdkafka build entirely (the confluent-kafka-go dependency existed
only for encoding; the whole vertical is now pure Go).

Amended (2026-09-23, issue #65): the model is built from `confluent-avro-go`'s parse and
`gogen-avro` is dropped. The vertical 2 rationale - gogen-avro as "the exact schema parser the
Confluent Go Avro serde delegates to" - stopped holding when vertical 3 moved encoding to
`confluent-avro-go`, a hamba/avro fork with its own parser. From then on one avsc was parsed by two
parsers that could disagree: Dry run accepted avsc files that produce could not encode (invalid
names or field defaults, which the encoder's parse refused, and decimals whose precision or scale
the codec silently dropped, which failed on the first record). Now each avsc is parsed once, with a
fresh name cache (the codec's default cache is process-global and leaks named types between
parses). The model carries that parse for the encoder, and a known logical type the codec does not
honour stops `Parse` with a `ParseError` instead of reaching the generator.

### 6. CLI flags and registry requirement

- `-format json|avro` (default `json`); the Avro flags are invalid for `json`.
- `-avro-schema <file.avsc>` (value avsc) and `-avro-key-schema <file.avsc>` (key avsc); required
  for AVRO generation.
- `-registry <url>` required only when `-format avro` and producing - never in Dry run.
- `-avro-key-schema` is the only AVRO key source: `-key` is a flag error under AVRO, and
  combining them fails the mutual-exclusion check (flag-validation error).

Amended (2026-09-20, ADR-0009): `-key` no longer exists, and its successor `-keyPath` does not
compete with the key avsc: the avsc still generates the Key, and the path only says where that
value is mirrored into the payload. Under `-format avro`, `-keyPath` therefore *requires*
`-avro-key-schema` instead of excluding it. The avsc checker validates the path first: only
record fields are guaranteed in Avro, so a step into a union, an array or a map is refused, and
the type at the path must be the key avsc's type (same primitive kind and logical overlay, or
the same full name for a named type).

Amended (2026-09-23, issue #29): the Wire format seam owns these rules. Each format is one
adapter in `internal/wire` (`jsonwire`, `avrowire`) with a `Check` phase for its flag rules
before any file is read and a `Build` phase that loads its own schemas and wires generation, the
Key source and the Encoders. The rules above are unchanged; they now live in the adapter they
concern, and `-keyPath` requiring a key schema is checked by each format against its own Key
source. The encoders of decision 1 moved out of `internal/pipeline` with them.

### 7. Dry run never contacts a registry

Dry run generates from the local avsc and renders records readably, without any registry
round-trip. It requires the avsc but never the registry URL.

## Consequences

- AVRO lands incrementally: a first vertical introduces the Encoder seam plus `-format` with the
  JSON adapter only, then AVRO generation, Confluent framing for produce, Dry-run display, and
  Key handling arrive as subsequent verticals (tracked as child tickets of the AVRO issue).
- The Key contract shifts from "field extracted from the Payload" to "source and encoding owned
  by the Encoder", which also unblocks the AsyncAPI `bindings.kafka.key` feature.
- The original AVRO issue's plan to derive the writer schema from the JSON Schema payload is
  superseded by the explicit-avsc, avsc-follows-generation model recorded here.
