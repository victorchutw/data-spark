# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Before exploring, read these

- `CONTEXT-MAP.md` at the repo root
- the relevant context's `CONTEXT.md`
- the relevant context's ADR directory
- `docs/adr/` for system-wide architectural decisions that touch the area being
  changed

If any of these files do not exist, proceed silently. The domain-modeling skill
creates or updates domain artifacts only when terms or decisions actually get
resolved.

## File structure

This is a multi-context repo. The root context map identifies which context docs
and ADRs apply to the area being changed:

```text
/
|-- CONTEXT-MAP.md
|-- CONTEXT.md
|-- docs/adr/
`-- docs/research/
```

Current contexts:

| Context | Context doc | ADRs | Applies to |
| ------- | ----------- | ---- | ---------- |
| CLI/core | `CONTEXT.md` | `docs/adr/` | `src/`, `tests/`, `Cargo.toml`, `Cargo.lock`, `docs/`, repository workflows, release process |

For the current repository shape, most work is in `CLI/core`. If a future
context is added, update `CONTEXT-MAP.md` first, then update this table.

## Use the glossary's vocabulary

When output names a domain concept, use the term as defined in the selected
context's `CONTEXT.md`. Do not drift to synonyms the glossary explicitly avoids.

If the concept needed is not in the glossary yet, note it for domain-modeling
rather than silently inventing new language.

## Flag ADR conflicts

If output contradicts an existing ADR, surface it explicitly rather than silently
overriding it.
