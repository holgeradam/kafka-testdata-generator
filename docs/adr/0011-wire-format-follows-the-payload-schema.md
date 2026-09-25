# The Wire format follows where the Payload's schema comes from

## Status

Accepted (2026-09-25, issues #90 and #91, decisions of #84). Amends ADR-0007 decisions 3 and 6.

## Context

AVRO mode took its avsc only from files: `-format avro -avro-schema order.avsc`, because the
tool read JSON Schema payloads alone and JSON Schema does not map losslessly to Avro. AsyncAPI
specs can carry the avsc themselves: a 2.x message declares an Avro `schemaFormat` for its
payload, a 3.0 payload is a Multi Format Schema `{schemaFormat, schema}`, and the Kafka message
binding's `key` may be an Avro schema. Such a spec describes an AVRO topic completely, so a run
from it should need no AVRO flags, as a JSON run needs none.

Once a spec can declare its payloads in either format, `-format` could either select the Wire
format, as before, or merely confirm one the spec already implies. Selecting would allow
`-format json` against Avro payloads, which the tool has no way to honour: it never converts
between schema languages.

## Decisions

1. **The Wire format is inferred.** Avro payloads in the spec, or `-avro-schema`, mean avro; any
   other run is json. The rule is one sentence: the Wire format follows wherever the Payload's
   schema comes from.
2. **`-format` only confirms.** Omitted, the inferred format applies. Given, it must agree:
   `-format json` against Avro payloads, or beside `-avro-schema`, stops the run naming why.
   `-format avro` with a JSON Schema spec still needs `-avro-schema`. `-format` never converts.
3. **One source of truth per schema.** Beside Avro payloads, `-avro-schema` is an error naming
   both sources, and so is `-avro-key-schema` beside an Avro Key binding. `-avro-key-schema` is
   allowed with Avro payloads that declare no Key binding: it is then the only Key source.
4. **A Kafka topic has one payload format.** Message types declaring Avro and JSON Schema
   payloads for one Kafka topic stop the run, naming the Message types per format, since a
   Kafka topic is produced in one Wire format.
5. **The format's rules run once the spec is read.** They used to run before any file was read;
   now the spec's payloads decide which format's rules apply. Planning stays pure: the spec and
   avsc files are read, nothing is dialed (ADR-0010 decision 2).
6. **The spec's avsc is registered as read.** `$ref`s inside it are expanded, a named type
   reached a second time becomes a reference by its full name, and a `$ref` cycle is refused,
   since Avro expresses recursion by name. The expanded avsc registers under `<topic>-value`,
   its Key binding under `<topic>-key`, exactly as the files do.
7. **Several Avro Message types register as a union** (#91, #84 decisions 4-6). They are mixed
   per record, as JSON mode mixes, and TopicNameStrategy keeps one subject per Kafka topic, so
   `<topic>-value` holds a union: each record registers under its full name, and the union
   lists the records by name with a schema reference to each. A named type several Message
   types define identically registers once under its full name, before the records, which
   reference it; defined differently, it stops the run. References name the version the
   registry holds each subject at. Every record encodes against the union and is framed with
   its ID; Dry run renders the record alone. To reach the Encoder, the Pipeline's Payload
   generator now returns each Payload with the index of its Message type.

## Consequences

- An AVRO run from a spec with Avro payloads needs only `-spec`, `-topic` and, to produce,
  `-registry`.
- The Run plan no longer picks the adapter by the `-format` value alone; it infers it from the
  spec and the flags, then hands the adapter the spec's Message types for its `Check`.
- Shared named types that refer to each other cannot be separate subjects, and stop the run;
  so does a shared named type without a namespace used inside a namespaced record, which Avro
  cannot name.
- Error ordering changed for broken invocations: a spec that fails to load is now reported
  before a Wire format flag rule.
