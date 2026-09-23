# The key schema produces the Key; -keyPath plants it into the Payload

## Status

Accepted (2026-09-20). Amends ADR-0004 (missing-Key skip) and, for AVRO, ADR-0007 decision 6 (issue #52).

## Context

A run had three Key sources that could not all be true at once: `-key` extracted a field from the generated Payload, `bindings.kafka.key` generated a Key from the binding schema, and `-avro-key-schema` generated one from the key avsc. `pipeline.Config` carried all three, and the precedence rule was re-derived as booleans in `Run` and again in `main.go`.

Underneath sat a semantic split nobody had reconciled: an extracted Key always matched a Payload field, while a generated Key matched nothing. Users who expect a record's key to be the entity id carried in the value got that only with `-key`.

A Kafka key and value are unrelated by design, and neither a kafka binding nor a key avsc says which Payload field it corresponds to. So the relation cannot be inferred from the schemas; it has to be stated.

## Decisions

1. **The key schema produces the Key; the Payload gets a copy.** No key schema means a null Key. With a key schema, the Key is generated from it. `-keyPath` states where that value is planted into the Payload, so Key and Payload hold the same value by construction rather than by coincidence.
2. **`-keyPath` requires a key schema**, and Payload extraction is gone. `-key` is renamed and kept only to fail with guidance, because an old invocation would otherwise change meaning silently.
3. **A path that is not guaranteed is refused at startup.** Every object step must be `required`, an array step must sit below `minItems`, no step may be an alternative (`oneOf`/`anyOf`, or an Avro union), and the path must stay inside the `$ref` depth budget (ADR-0005). The alternative, planting into whatever the record happens to have, would mean the Key silently missing from some Payloads, which is the disagreement this design removes.
4. **Type compatibility is checked at startup too**, comparing the key schema with the schema at the path, so a run can never emit a Payload violating its own schema. Consequence: the per-record missing-Key skip and its `Failed` increment disappear from ADR-0004's amendment, since the case is now impossible by construction.
5. **The whole Key value is planted**, scalar or record alike, and the startup check demands a compatible shape at the path.
6. **One Key plan module** (`internal/keyplan`) owns all of it: path parsing, the startup checks in its constructor, and one `Apply(payload) (key, error)` per record. `pipeline.Config` holds a single optional `KeyPlan`, so the Pipeline sheds the JSON-only path walker (94 of its 273 lines) and stops knowing key rules at all.
7. **Path parsing and planting are format-blind; validation is not.** Both schema walkers emit plain maps and slices along a validated path (rejecting alternatives is what keeps this true), so planting needs no format knowledge. A `Checker` adapter per schema language validates within its own language: `generator.KeyChecker` over the Message schema, and an avsc checker over the Avro model (issue #52).

## Consequences

- No precedence rule remains, because the sources no longer compete: one key schema, optionally mirrored. Both precedence warnings are gone; the null-Key info message stays.
- Breaking: `-key fieldName` without a key schema used to produce Keys and is now a flag error. Specs need `bindings.kafka.key` (or `-avro-key-schema` under AVRO).
- Key generation draws from the run's one Synthesizer (ADR-0008 decision 4), after the Payload, because planting writes into it.
- Dry run keeps the `Key:` stderr echo (ADR-0003), which now also proves the planted value.

Amended (2026-09-23, issue #75): the plan can reuse Keys. With `-records-per-key N` above 1, its Generator is wrapped in a pool of Entities (`keyplan.Reuse`): each record starts a new Entity, with a fresh Key from the Key schema, with probability 1/N, drawn from the run's Synthesizer, and otherwise reuses the Key of one of the 1,000 most recent Entities. Planting is unchanged, so Key and Payload still agree for a reused Key. The pool is format-blind like the rest of the plan, so an AVRO key avsc gets the same reuse. N = 1 is the Generator itself and draws nothing, so the default run is exactly what it was. Lifecycle order (an Entity's created record first) is left to a later feature.
