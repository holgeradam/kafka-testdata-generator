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

### 4. Conformance is per Wire format

Conformance (ADR-0006) is now defined per format: JSON mode honors the Message schema; AVRO mode
honors the avsc. Whichever schema governs a mode, anything it cannot honor stops the run with a
typed error rather than emitting non-conforming data.

### 5. Registry client, keep franz-go

Franz-go stays the Kafka producer (ADR-0001). The `confluent-kafka-go` schema-registry package
provides the registry client and its `AvroSerializer` (Apache-2.0); this reintroduces a
C-linked dependency on the registry path, which ADR-0001 records as an accepted amendment. The
whole serializer runs inside the `AvroEncoder`, accepting an HTTP round-trip to the registry to
register/look up the schema ID before encoding.

### 6. CLI flags and registry requirement

- `-format json|avro` (default `json`); the Avro flags are invalid for `json`.
- `-avro-schema <file.avsc>` (value avsc) and `-avro-key-schema <file.avsc>` (key avsc); required
  for AVRO generation.
- `-registry <url>` required only when `-format avro` and producing - never in Dry run.
- `-avro-key-schema` and `-key` are mutually exclusive under AVRO (flag-validation error).

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
