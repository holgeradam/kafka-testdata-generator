# Avro Library Evaluation (Issue #15)

## Question

Can an existing Go Avro library generate random test data from schemas, or do we need to write our own?

## Survey

### Libraries evaluated

| Library | Schema types? | Random generation? | Seed support? | Status |
|---------|--------------|-------------------|---------------|--------|
| `linkedin/goavro/v2` | No (maps only) | No | N/A | Maintenance mode |
| `hamba/avro/v2` | Yes (rich AST) | No | N/A | **Archived** Jan 2026 |
| `iskorotkov/avro/v2` | Yes (same AST as hamba) | No | N/A | **Active** (v2.34.0, Aug 2026) |
| `twmb/avro` | Yes (flat SchemaNode) | No | N/A | Active (v1.8.0, 2026) |
| `z5labs/avro-go` | Yes (canonical form) | No | N/A | Nascent (0 stars) |

### Key finding

**No Go library generates random data from Avro schemas.** All evaluated libraries are codecs - they encode/decode existing data but don't create it. The `AvroRandom()` method referenced in the original issue does not exist in goavro.

For JSON Schema, two libraries exist (`go-chaff`, `schemagen`) but neither is mature enough to replace the current generator.

## Recommendation

**Write our own Avro random generator** using `iskorotkov/avro/v2` as the schema AST foundation.

### Why iskorotkov/avro/v2

- Actively maintained fork of hamba/avro (drop-in compatible)
- Rich typed schema AST: `RecordSchema`, `ArraySchema`, `MapSchema`, `UnionSchema`, `EnumSchema`, `FixedSchema`, `PrimitiveSchema` with `LogicalSchema`
- Type-switchable `Schema` interface - perfect for recursive walking
- Used by Apache Pulsar and Iceberg-Go ecosystem
- Security fixes and performance improvements over hamba/avro

### Architecture

The Avro random generator walks the parsed schema tree and fills fields using a seeded `math/rand`:

```
AvroGenerator
  ├── parses .avsc via avro.Parse()
  ├── walks schema tree (Record → Fields → primitives/complex)
  ├── generates values per type:
  │   ├── string → random string (or format-aware: email, uuid, date)
  │   ├── int/long/float/double → random within bounds
  │   ├── boolean → random coin flip
  │   ├── enum → random symbol from enum symbols
  │   ├── array → random count (1-5) of items
  │   ├── map → random count (1-5) of entries
  │   ├── union → random branch (weighted toward non-null)
  │   └── fixed → random bytes of specified size
  └── deterministic: same seed + same schema = identical output
```

### Feeds into

This generator is the proof-of-concept for #10 vertical 2 (avsc parsing → Avro generation model). When #10 is picked up, the prototype code refactors into `internal/generator/avro.go`.

## Prototype

The prototype (`cmd/avro-gen-eval/main.go`) tests two things:

1. **Baseline** (`-mode=pattern`): current Generator determinism with 50 iterations
2. **Avro generator** (`-mode=avro`): new Avro random generator determinism and diversity

Both run on a throwaway branch (`prototype/avro-gen-eval`).

## Prototype results (seed=42, 50 iterations)

| Metric | Pattern baseline | Avro generator |
|--------|------------------|----------------|
| Determinism | PASS (byte-identical) | PASS (byte-identical) |
| Diversity | 31/50 unique records | 50/50 unique records |
| Nullable union | **FAIL** - `oneOf` with null branch errors | PASS (via union types) |

### Key finding

The current JSON Schema generator does **not** honor `type: "null"` or a nullable `oneOf` branch - it returns `generator: unsupported type "null"`. In the baseline, records that randomly selected the null branch of `shippingAddress.oneOf` failed entirely, dropping the field (and reducing diversity to 31/50). The Avro generator handles this naturally through its union type support, which weights toward the non-null branch.

This confirms two things for #10:
1. The Avro generator prototype is a viable foundation
2. The current generator has a latent nullable-type gap the Avro path must not inherit

## Bonus finding: flaky `TestScenarioDeterministic`

Adding the avro dependency exposed a latent flake in `TestScenarioDeterministic` (`cmd/kafka-testdata-generator/main_test.go`). The test compared two runs with a fixed `-seed` but **no `-now`**, so `format: date-time` fields (`createdAt`, `updatedAt`) used the wall clock. When the second boundary ticked between runs, the fields differed by one second.

Fixed by passing `-now 2026-01-02T03:04:05Z` to both runs, matching the documented determinism contract (ADR-0006 Decision 4: "a fixed seed AND a fixed now"). `TestScenarioDeterministicWithNow` also now filters the non-deterministic wall-clock `elapsed=` stats line from its comparison, like its sibling test.
