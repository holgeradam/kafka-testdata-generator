# Schema module invariants and $ref resolution semantics

## Status

Accepted (2026-08-23)

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
