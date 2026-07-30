# Context Map

This file tells engineering skills which domain context applies to each part of
the repository. Read it before choosing a `CONTEXT.md` or ADR set.

## How to use this map

1. Match the files or topic in the task to the context below.
2. Read that context's `CONTEXT.md`.
3. Read the listed ADR directory for decisions that touch the work.
4. If no context matches, stop and ask whether a new context should be added.

## CLI/core

Context: `CONTEXT.md`

ADRs: `docs/adr/`

Applies to files and topics under:

- `src/`
- `tests/`
- `examples/`
- `Cargo.toml`
- `Cargo.lock`
- `docs/`
- repository workflows and release process

Use this context for CLI behavior, load definitions, sources, destinations,
load reports, rejected records, artifacts, local verification, GitOps, and
release packaging.
