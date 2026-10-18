# Diagnostics go through a Logger seam; product output does not

## Status

Accepted (2026-10-18, issue #121). Builds on #120, which sent failure causes to a `Config.Warn` writer.

## Context

Diagnostics were ad-hoc writes to stderr: strings with "Warning:" baked in on the Run plan, a `Warn` writer on the Pipeline, and `Error:` and "Interrupted" lines in `main`. There was no level, no way to quiet them and no seam to test them against.

## Decisions

1. **Only diagnostics are logs.** Warnings, the cause of each failed record, the null-Key notice and the interrupt notice. The Dry run `Key:` and `Headers:` lines and the Stats line are the tool's product: unchanged, and unaffected by the level.
2. **A Logger interface of our own** (`internal/logging`: Debug, Info, Warn, Error with alternating key/value attrs). The Pipeline, the Run plan and the Wire formats depend only on it. There is no package-level logger. A `Recorder` is the fake for tests.
3. **One adapter, `internal/logging/zaplog`,** writes zap console text to stderr: level, message, attrs, no timestamp or caller. No zap type leaves it. JSON output can be added behind the same seam.
4. **`-log-level debug|info|warn|error`,** default `info`, validated in the Run plan like every flag rule (ADR-0010): a bad value is a usage error, exit 2.
5. **Plan purity holds.** The Run plan and `wire.Parts` carry `logging.Diagnostic` values (level, message, attrs) instead of strings, and still do no I/O. `main` builds the Logger from the plan's level, emits the diagnostics, and gives the Logger to the Pipeline. `plan.Warnings` and `Config.Warn` are gone.
6. **Fatal errors keep their plain line.** `main` always prints `Error: ...` (and the usage block where it applies) whatever the level, and also logs an error-level record. At the default level that is one duplicate line, accepted.
7. **Levels in use:** the null-Key notice is info, ignored spec declarations and disregarded options are warn, a failed record under `-continueOnError` is error, and the interrupt notice is warn. `-log-level error` therefore hides warnings and never the `Error:` line, the Stats or the Dry run output.

## Consequences

- Tests assert on records (level, message, attrs) rather than on stderr text.
- Message wording is kept; the "Warning:" prefix is now the level the Logger prints (`WARN`).
- zap is the only new dependency, and only the adapter imports it.
