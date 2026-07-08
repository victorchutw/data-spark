# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Before exploring, read these

- `CONTEXT.md` at the repo root
- `docs/adr/` for architectural decisions that touch the area being changed

If any of these files do not exist, proceed silently. The domain-modeling skill creates or updates domain artifacts only when terms or decisions actually get resolved.

## File structure

This is a single-context repo:

```text
/
|-- CONTEXT.md
|-- docs/adr/
`-- docs/research/
```

## Use the glossary's vocabulary

When output names a domain concept, use the term as defined in `CONTEXT.md`. Do not drift to synonyms the glossary explicitly avoids.

If the concept needed is not in the glossary yet, note it for domain-modeling rather than silently inventing new language.

## Flag ADR conflicts

If output contradicts an existing ADR, surface it explicitly rather than silently overriding it.
