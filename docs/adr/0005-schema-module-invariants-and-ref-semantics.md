# Schema module invariants and $ref resolution semantics

## Status

Accepted (2026-08-23). Decisions 1 and 5 amended (2026-09-23, issue #73). Decision 2 extended to trait merges (2026-09-24, issue #81) and to AsyncAPI 3.0 (2026-09-24, issue #82).

## Context

`Document.PayloadSchema` returns a raw `map[string]any` whose safety depends on invisible facts: "all `$ref`s resolved", "shape normalized". Nothing enforces either. Ref resolution has three defects:

1. Cycle detection uses a single global `seen` map across sibling branches, so diamond references (two properties `$ref`ing the same component) get their `$ref` silently deleted, producing empty schemas and dropping generated fields (sandcastle #3).
2. Recursive `$ref`s cannot terminate under eager expansion, which motivated the seen-map hack in the first place.
3. `resolveRef` marshals and unmarshals the entire document to a navigable map on every ref lookup.

Additionally, `generator.normalizeSchema` silently defaults a missing `type` to `"string"`, masking malformed schemas; types should come from the schema, never be implied.

## Options considered

### Return type

| Option | Assessment |
| --- | --- |
| (a) Invariant-guaranteed map | Chosen. Cheap; callers keep simple signatures; the module guarantees "every non-cyclic `$ref` resolved, shape normalized". |
| (b) Accessor type wrapping the map | Rejected: accessors forwarding to map lookups fail the deletion test - complexity moves without concentrating. |
| (c) Typed JSON Schema AST | Rejected: speculative generality for a test-data tool. |

Revisit (b) only if the generator develops a need for rich schema queries.

### Recursive references

JSON Schema expresses recursion by keeping `$ref` in place (`child: {$ref: '#'}`); eager expansion of such a document is infinite - bounded spec text, unbounded expanded structure. Real payloads use this routinely (category trees, comment threads, org charts, bill-of-materials), and AsyncAPI's own meta-schema is recursive.

| Option | Assessment |
| --- | --- |
| (a) Preserve cyclic `$ref`s | Chosen. Expand refs along each path; when a ref targets a node already on the current expansion stack, keep the `$ref` node as-is. Diamonds resolve correctly because each sibling starts a fresh path. Honest representation of the schema. |
| (b) Depth-budget pre-expansion | Rejected: produces a finite map that lies about deep structure. |
| Global seen-set deletion (previous behaviour) | Rejected: corrupts diamonds and misrepresents recursion. |

Until the generator learns to walk preserved `$ref`s with a depth budget (tracked as a follow-up issue), `PayloadSchema` returns an explicit `UnsupportedRecursionError` for cyclic schemas instead of silently generating partial payloads.

## Decision

1. `PayloadSchema` continues to return `map[string]any`, now under an enforced invariant: non-cyclic refs resolved, cycles preserved as `$ref` nodes, shape normalized.
2. Resolution walks each path with an expansion stack (per-path cycle detection). The document is converted to a navigable map once at `Load`, not per ref.
3. Normalization moves into the asyncapi module. A missing or unknown `type` is never defaulted; it surfaces as a typed unsupported-schema error at generation time.
4. External (`non-#/`) refs remain rejected with a clear error.
5. The triplicated resolve-or-decode message-extraction block collapses into one internal helper.

## Consequences

- Diamond-ref corruption (sandcastle #3) is fixed at the seam where it belongs; locality for all future ref work.
- Cyclic schemas fail loudly and early instead of crashing (stack overflow) or corrupting output.
- Recursive data generation arrives in a second, separately reviewable step (depth-budget walker in the generator).
- The generator loses hidden normalization behaviour; its interface contract moves toward "conforms to the Message schema or a typed error".

## Amendment (2026-09-23, issue #73)

Cyclic `$ref`s still stay `$ref` nodes, but they now point into the returned schema instead of the spec. Each cycle's target travels in the schema's own `$defs`, keyed by the target's JSON Pointer so two targets never collide, and the cyclic node becomes `{"$ref": "#/$defs/<pointer>"}`. Inside a `$defs` entry every `$ref` is rewritten to a local one rather than expanded, so each `$ref` the generator follows inside a cycle still costs one step of its depth budget. Seeded output is byte-identical to the spec-pointing form.

A schema is therefore self-contained: the generator and the JSON key checker resolve `#/$defs/…` locally and treat `$defs` as a definitions container, never as a keyword. The callback into the spec (`Document.ResolveRef`, `generator.SetRefResolver`, `generator.RefResolver`, `wire.Options.ResolveRef`) is gone, which answers #34's question of whether `ResolveRef` stays exported.

Decision 5 is finished rather than reopened. One walk over the decoded spec reads a Kafka topic's Message types, resolving a `$ref` wherever the spec may use one: a spec entry's bindings, an operation's message, a `oneOf` variant, a message's bindings, the kafka binding, the key and the schemas. References are JSON Pointers (RFC 6901): percent-encoding decodes, and `~1`/`~0` unescape. What the walk cannot read is an error naming the Message type; a broken reference is never replaced by another operation's message or reported as "no message". The typed struct decode and its `mapToStruct` round-trip, and `generator.normalizeSchema`, went with the old mechanisms. Decision 1's invariant-guaranteed `map[string]any` return is untouched.


## Amendment (2026-09-24, issue #81)

A message's `traits` are merged into it with JSON Merge Patch (RFC 7386) before the walk reads it; in 2.x a trait overrides the message's own field. Merge Patch knows nothing of `$ref`, so the merge resolves one wherever both the message and the trait hold an object under the same key, and a trait extends a referenced binding instead of being shadowed by the `$ref`. Where only one side declares a value it is copied as written, `$ref` included, and resolved by the walk as before. Two identical `$ref`s merge to that `$ref` unexpanded, so a message and a trait naming the same cyclic schema never follow the cycle; merges that descend more than 64 levels stop with an error. Merging copies, so the decoded spec stays unchanged for every other Message type.

## Amendment (2026-09-24, issue #82)

The walk now has two front ends, one per AsyncAPI version, over one back end. The 2.x front end reads a spec entry's publish and subscribe messages; the 3.0 front end reads a channel's `messages` map and finds its Kafka topic in `bindings.kafka.topic` or `address`. Both hand each message to the same trait merge, schemaFormat check, `$ref` resolution and Key binding, so every rule in this ADR holds for both, and there is no lossy 3.0-to-2.x conversion: errors name the spec paths as written. The only difference in the back end is the trait precedence each version specifies: in 2.x the trait wins, in 3.0 the message does, and there the message's own nulls are values rather than deletions. A 3.0 payload is a plain schema or a Multi Format Schema `{schemaFormat, schema}`; a plain one is passed on as declared, `$ref` and all, so its cycles resolve exactly as a 2.x payload's.
