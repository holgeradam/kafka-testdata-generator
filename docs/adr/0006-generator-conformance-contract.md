# Payload generator conformance contract

## Status

Accepted (2026-08-23)

## Context

The generator's de-facto promise - "generated payloads conform to the Message schema" - is false in several places: unknown types silently produce `nil`, regex patterns fall through to non-conforming random strings (sandcastle #2), unchecked type assertions panic on valid specs, shallow `allOf` merging lets later subschemas overwrite earlier constraints, and field-name heuristics can override explicit schema constraints. Separately, determinism is overstated: `New(seed)` hides an internal `baseTime = time.Now()`, so the same seed produces different output whenever `date`/`date-time` formats appear.

## Decisions

### 1. Known-constraint conformance

The interface promises: every constraint kind the generator understands is always honoured; any constraint it cannot honour surfaces as a typed error (`UnsupportedSchemaError` family naming the offending keyword and path). No validation runs in the generation/streaming path - the typed-error boundary makes quietly-bad output impossible without paying double-maintenance for an embedded validator.

### 2. Regex subset synthesizer

Patterns are synthesized by a hand-rolled engine supporting a documented subset: literals, character classes `[A-Z]` `[a-z]` `[0-9]`, escapes `\d \w \s`, quantifiers `{n}` `{n,m}` `*` `+` `?`, groups, alternation, anchors (ignored for synthesis). Anything outside the set yields `UnsupportedPatternError` naming the construct. The supported-syntax table ships in the README. Evaluating a third-party regex-generator library is tracked as a separate triage issue; adoption would sit behind a small wrapper without changing this contract.

### 3. Property testing through a real validator

Test-only dependency on a maintained JSON Schema validator (wrapped in a single test helper so swapping costs one file). Golden property tests generate N payloads per schema fixture and validate each against its Message schema - the interface literally becomes the test surface. Test-only placement contains the dependency risk: rot breaks CI, never users.

### 4. Explicit clock

`New(seed int64, now time.Time)` - both inputs explicit, no hidden state. A new `-now` flag (RFC3339, default wall-clock) feeds it from the CLI, consistent with ADR-0002's defaults philosophy. The determinism promise is corrected everywhere it appears: **identical output requires fixing both `-seed` and `-now`; with only `-seed`, date-formatted fields vary between runs.**

### 5. Proper `allOf` merging

Numeric bounds intersect (`max(minimums)`, `min(maxima)`), `required` unions, `properties` merge recursively; irreconcilable conflicts (distinct `const` values, clashing `type`) yield a typed error. This removes the last place the conformance promise would be false by construction.

### 6. Interface cleanup

`Value(schema, fieldName)` returns `(any, error)` - panics on valid schemas become impossible. Constraint precedence is explicit and documented in the README: `example`/`examples` > `const` > `enum` > `format` > `pattern` > field-name heuristics > fallback; schema constraints always beat name heuristics. The undocumented empty-string root sentinel is replaced by a named `RootField` constant.

## Consequences

- Sandcastle #2 closes properly; silently-violating output classes are structurally eliminated.
- Determinism claims match observable behaviour; reproducible runs need `-seed` plus `-now`.
- Sequencing: lands after #12 (which moves normalization out of the generator); #13 (recursive depth walker) builds on the resulting `(any, error)` signature to avoid rework.
