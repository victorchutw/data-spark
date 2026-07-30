# Documentation

Data Spark's documentation serves three audiences out of one tree: people
running loads, the maintainer and agents changing this repository, and anyone
asking why the tool behaves the way it does. Start with the section that
matches what you are doing.

## Running loads

The [root README](../README.md) is the front page: what Data Spark does, how to
install the single binary, and a quickstart load. From there:

| Where | What it holds |
| --- | --- |
| [../examples/](../examples/) | Small, self-contained, runnable load definitions with their fixture files, one per feature area. `cargo test --locked` runs every one of them against the built binary, so an example that stops working fails CI. |
| [guides/](guides/) | Task-oriented walkthroughs, each starting from one of those examples and working through one feature at a time: [schema pinning](guides/schema-pinning.md), [rejected records](guides/rejected-records.md), [declared types](guides/declared-types.md), and [execution tuning](guides/execution-tuning.md). |
| [reference/](reference/) | The two versioned contracts documented key by key: the [Load Definition Reference](reference/load-definition.md) (the YAML you write) and the [Load Report Reference](reference/load-report.md) (the JSON every load writes). |

What changed between releases is in [../CHANGELOG.md](../CHANGELOG.md).

## Changing this repository

| Where | What it holds |
| --- | --- |
| [../AGENTS.md](../AGENTS.md) | The entry point for an agent working here: which language to discuss in, and pointers to the working agreements below. |
| [agents/](agents/) | Those working agreements: the [issue tracker](agents/issue-tracker.md) this project plans in, the [triage labels](agents/triage-labels.md) it uses, the [GitOps workflow](agents/gitops.md) from issue to branch to PR to release, and how to consume the [domain docs](agents/domain.md). |
| [../CONTEXT-MAP.md](../CONTEXT-MAP.md) | Which domain context applies to which part of the tree. Read it before picking a glossary or an ADR set. |
| [../CONTEXT.md](../CONTEXT.md) | The `CLI/core` glossary: the ubiquitous language the code, the docs, and the load report all speak, each term with the synonyms to avoid. |

## Why the tool behaves this way

[adr/](adr/) holds the architecture decision records. One decision per file,
numbered in the order it was taken and named after the decision itself — so
[`0033-persist-pinned-schemas-as-versioned-yaml-files.md`](adr/0033-persist-pinned-schemas-as-versioned-yaml-files.md)
states its position in its filename, while the body carries the reasoning and
the alternatives rejected. Each file carries a `status` field, and every decision
so far is `accepted`. The reference and guide pages link the ADRs that govern
the behavior they describe, which is the usual way in.

[research/](research/) holds dated background notes written while making some
of those decisions — a
[survey of comparable data movement tools](research/github-data-movement-tools-2026-07-08.md)
and the
[AI agent loop practice](research/ai-agent-loop-engineering-2026-07-08.md)
behind the workflow in `agents/gitops.md`. They record what was true on their
research date and are not kept current; an ADR, not a research note, is the
decision.
