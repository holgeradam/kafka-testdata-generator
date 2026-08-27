# AGENTS.md

Instructions for AI agents working in this repo.

## Agent skills

### Issue tracker

GitHub Issues in holgeradam/kafka-testdata-generator via gh CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

Five canonical triage roles using default label strings (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: `CONTEXT.md` at repo root + `docs/adr/`. Read before exploring; use glossary vocabulary. See `docs/agents/domain.md`.

## Working method

Test-first development: write the test that fails on the bug or feature first, watch it fail (red), then implement the minimum to pass (green), then refactor. Applies to all feature work and bug fixes, not just tickets carrying an explicit TDD note.
