# One Synthesizer owns leaf values; walkers own structure

## Status

Accepted (2026-09-16). Amends ADR-0006 decisions 2 and 4.

## Context

The JSON Schema walker (`internal/generator`) and the avsc walker (`internal/avro`) each hand-rolled seeded value synthesis: UUIDs, random strings, phone and SKU shapes, field-name heuristics, and a seed-plus-clock constructor. The copies diverged in visible output. Identically named fields gave realistic values under JSON and random strings under AVRO (#28), date windows differed (365 days vs 3650), and AVRO lost field names inside unions, so a nullable `email` got random text. Design session in #31.

## Decisions

1. **Leaf values vs structure.** A **Synthesizer** (`internal/synth`) owns the seed, the clock and every leaf value, and it is the only source of randomness in a run. The walkers own structure: traversal, optionality, sizes, depth budgets and typed schema errors. Walkers take their random structure decisions (optional presence, array length, branch choice) from the Synthesizer too, so one seed governs all output. Typed error families and depth budgets stay per walker because their semantics are genuinely per schema language.
2. **Typed methods, native Go values.** The interface is a handful of typed methods (`Text`, `Semantic`, `Instant`, `Pattern`, `Int`, `Float`, `Chance`, `Pick`, `Bytes`) returning native values. Each walker converts to its wire convention: JSON renders an instant as RFC 3339 text, AVRO passes `time.Time`. A single request method returning `any` was rejected: walkers would type-assert every result, and invalid combinations would need runtime errors.
3. **Determinism per build.** The same binary with the same `-seed` and `-now` replays the same output. Values for a seed may change between releases. This amends ADR-0006 decision 4, which did not say whether seeded output is stable across versions, and frees draw order to change.
4. **One shared stream per run.** Payload and Key generation draw from one Synthesizer, interleaved, in both formats. The rejected alternatives, independent streams per role and identical same-seed streams, were weighed. Note that none of them makes a generated Key match a Payload field. That requirement belongs to the Key source design (#30).
5. **Nested values inherit the enclosing field name.** The nearest named field flows through union, oneOf, anyOf, allOf and `$ref` branches, array items and map values.
6. **Whole-word matching.** `Text` splits a field name into words (camelCase, snake_case, kebab-case, acronyms, digits) and matches whole words or their regular plurals. The old substring matching sent `width` and `valid` to UUIDs, `capacity` to cities and `security` to URLs.
7. **One time window.** Every date and timestamp falls within the 365 days before `-now`.
8. **The pattern engine lives in the Synthesizer.** Synthesizing a string from a regex is not specific to JSON Schema. Only reading the `pattern` keyword is. `Pattern` reports an unsupported construct without a location, and the JSON walker wraps it into `UnsupportedPatternError` with the JSON Path. The documented subset from ADR-0006 decision 2 is unchanged.
