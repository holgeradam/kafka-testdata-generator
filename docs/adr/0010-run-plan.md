# argv becomes a Run plan; main is the process edge

## Status

Accepted (2026-09-22). Amends ADR-0004 (stats and cleanup at the process edge).

## Context

Everything between flag parsing and `pipeline.Run` lived in `main.go`: 14 flags, four `flag.Value` types, every validation rule, spec and avsc loading, generator, Key plan and encoder selection. None of it was reachable from a test. `main_test.go` had grown to 47 scenarios that each compiled the binary, ran it, and matched substrings of stderr, which made error wording load-bearing test API and a full package run cost about 38 seconds.

#27 was the consequence: a warning that contradicted the tool's own behaviour survived because the warning and the behaviour were asserted in separate scenarios that never met.

Two defects came from the same shape. `os.Exit(1)` after `defer sink.Close()` skips the defer, so the franz-go producer was neither flushed nor closed when the Key plan, the encoder or the run failed. And the sink was built before the encoder, which fixed the order in which a broker and a registry failure could be reported.

## Decisions

1. **`internal/runplan` owns the rules.** `Plan(args []string) (*Run, error)` turns argv into a validated run: flags, validation, spec and avsc loading, the generator and the Key plan. Every rule is a table row in a test that spawns no process.

   Amended (2026-09-23, issue #29): the format-specific rules, avsc loading, generator and encoder selection moved to the Wire format adapters in `internal/wire` (ADR-0007). `runplan` still declares every flag, runs the format-agnostic rules and spec loading, and calls the selected adapter; its table keeps every row, since `*runplan.Error` is the adapters' error type.
2. **Planning is pure; construction is on demand.** `Plan` performs no network I/O. `Run.NewSink` dials the broker and `Run.NewEncoder` talks to the registry, called by the process edge after planning. Warnings are data on the `Run`, not writes to stderr during planning.
3. **One typed error.** `*runplan.Error` names the offending flag and wraps the cause (`keyplan.PathError`, `avro.ParseError`, `generator.UnsupportedSchemaError`). Tests assert with `errors.As`, so message wording stops being test API. Validation fails on the first rejected rule.
4. **`main` is `os.Exit(run(...))`.** `run(ctx, name, args, stdout, stderr) int` wires signals, builds sink and encoder, defers cleanup, drives the Pipeline, prints stats and returns an exit code. No `os.Exit` inside means every defer runs on every path, so the leak class is structurally gone rather than patched. Writers are parameters, so a test observes output without a process.
5. **Broker before registry, as a rule rather than an accident.** An unreachable broker fails before anything is registered, so a run that cannot produce never adds a schema version to a shared registry. Registering an identical schema is idempotent, so this matters exactly while someone iterates on an avsc, and a compensating delete is not safely possible: the client deletes whole subjects only, registries often forbid deletes, and other clients may already reference the ID.
6. **The E2E suite keeps what needs a process.** Signal handling, piping, determinism across runs, exit codes, the dry-run registry tripwire and the broker-before-registry guard stay. The scenarios that existed only to assert a rejected flag combination became table rows. The survivors share one binary built once per package.

## Consequences

- `main.go` drops from 454 lines to about 100, and holds no validation rule.
- The package's test suite goes from 47 process spawns (~38s) to 28 (~2.4s).
- Error wording is free to change; tests assert flags and typed causes.
- A future wire format module (#29) has a single place to plug into, and a test surface to land against.
