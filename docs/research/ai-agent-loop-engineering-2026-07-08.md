# AI Agent Loop Engineering Research

Date: 2026-07-08

This note surveys current AI agent loop engineering practice and turns it into a
development workflow for Data Spark. The goal is not to make the repository fully
autonomous. The goal is to let an agent safely take a well-scoped GitHub Issue
from `ready-for-agent` to a reviewable pull request, with clear stop conditions,
verification, and human gates.

## Research Lens

The strongest sources converge on the same pattern: useful agent loops are
engineered systems around the model, not just longer prompts. The model should
act, observe environment feedback, decide the next step, and repeat only inside
explicit boundaries.

Key findings:

- Anthropic separates predictable workflows from autonomous agents. Their advice
  is to start with the simplest system that works, then add autonomy only when
  the task requires flexible model-driven decisions. For coding work, the
  strongest patterns are orchestrator-workers for multi-file exploration and
  evaluator-optimizer for iterative implementation plus critique.
- Anthropic's eval guidance is directly relevant to coding agents: turn failures
  into tests, run automated checks early, combine automated checks with human
  review, and treat evals as part of the system rather than a final cleanup step.
- HumanLayer's 12-factor agents framing is useful for engineering the loop
  boundary: own the context window, own control flow, keep agents small, make
  tools structured outputs, keep execution state visible, and involve humans
  through explicit tool or workflow calls rather than implicit guessing.
- Claude Code, Codex, and GitHub Copilot guidance all emphasize the same
  software-engineering loop: explore the codebase, plan, implement, run tests,
  lint/build, open a draft PR, then ask for review. Repository instructions such
  as `AGENTS.md` and issue descriptions are part of the agent's context contract.
- Recent "loop engineering" writing from Addy Osmani, Claire Vo/Lenny's
  Newsletter, Andrew Ng coverage, MindStudio, and industry reporting describes
  the operational pieces every recurring loop needs: automation trigger,
  isolated worktree, project skills or instructions, connectors to real tools,
  subagents or reviewers, persistent state, clear stop criteria, and cost
  controls.

The practical conclusion for this repository: use loops to close the gap between
issue, implementation, verification, and PR review. Do not use loops to bypass
human product judgment, ADR ownership, release approval, or review of risky
Data Movement behavior.

## Fit For Data Spark

Data Spark is a small Rust CLI with a large contract surface. The main risk is
not UI polish or distributed system scale. The main risk is breaking load
contracts: load definitions, source and destination semantics, schema decisions,
rejected records, load artifacts, load reports, exit codes, and release
packaging.

The workflow should therefore optimize for:

- issue clarity before coding;
- small vertical slices;
- contract tests over broad speculative refactors;
- explicit ADR checks when behavior changes;
- repeatable local verification;
- draft PRs with human review before merge;
- no hidden agent state outside GitHub Issues, PRs, docs, and git history.

## Recommended Loop Architecture

Data Spark should use five loops, each with a different owner and cadence.

| Loop | Owner | Trigger | Output | Human gate |
| --- | --- | --- | --- | --- |
| Triage loop | Agent + maintainer | New or changed issue | One state label and a ready/not-ready brief | Maintainer approves ambiguous scope |
| Spec loop | Agent + maintainer | Issue needs shaping | Agent-ready issue with acceptance criteria | Maintainer confirms scope |
| Implementation loop | Agent | `ready-for-agent` issue | Branch, commits, passing checks, draft PR | Maintainer reviews PR |
| Review loop | Separate reviewer agent + human | Draft or ready PR | Findings, fixes, final review checklist | Human merge decision |
| Release loop | Human + agent assist | Release issue | Version bump PR and tag-ready checklist | Human creates tag |

The implementation loop can run unattended for short periods. The other loops
should be allowed to stop early and request human input.

## Issue Readiness Contract

An issue is `ready-for-agent` only when it contains:

- Problem statement in Data Spark vocabulary from `CONTEXT.md`.
- Desired user-visible behavior.
- Acceptance criteria that can be verified from the CLI, tests, generated
  artifacts, or docs.
- Out-of-scope notes.
- Expected files or areas, when known.
- Test expectation, especially for load definitions, load reports, schema drift,
  rejected records, destination writes, or release packaging.
- ADR expectation: no ADR needed, update existing ADR, or create a new ADR.
- Release impact: none, release note, or release issue required.

If any of these are missing and the agent cannot infer them safely from existing
docs, the issue should move to `needs-info`, not implementation.

## Implementation Loop

The default agent goal for an issue:

1. Read `AGENTS.md`, `CONTEXT.md`, `docs/agents/*`, and ADRs touching the change.
2. Read the GitHub Issue and comments.
3. Create or use a short-lived branch from `main`.
4. Move the issue to `in-progress`.
5. Explore only the relevant code and tests.
6. Write a short plan in the working notes or PR body.
7. Implement the smallest vertical slice satisfying the issue.
8. Add or update tests before relying on manual inspection.
9. Run local checks:
   - `cargo fmt --check`
   - `cargo clippy --locked --all-targets -- -D warnings`
   - `cargo test --locked`
   - `cargo build --release --locked --bin data-spark`
10. Iterate until checks pass or a stop condition fires.
11. Open a draft PR with `Closes #<issue-number>`.
12. Keep the PR draft while behavior is still changing.
13. Mark ready for review only after checks pass and the PR checklist is honest.

Recommended branch naming:

```text
agent/<issue-number>-<short-slug>
```

Recommended parallel work isolation:

```bash
git worktree add ../data-spark-<issue-number> -b agent/<issue-number>-<short-slug> main
```

Use one issue per worktree. Do not let two agents edit the same checkout unless
one is read-only.

## Review Loop

Run a separate reviewer pass before human merge. The reviewer should inspect the
diff as a code reviewer, not as the implementer.

Reviewer checklist:

- Does the change satisfy the issue and only the issue?
- Does it preserve Data Spark vocabulary from `CONTEXT.md`?
- Does it conflict with any ADR?
- Does it alter load definition or load report contracts? If yes, are versioning
  and tests handled?
- Are source, destination, load mode, schema, rejected record, write atomicity,
  retry, and artifact behaviors covered where relevant?
- Are errors deterministic and useful for CLI users?
- Are credentials still kept out of load definitions and generated reports?
- Do tests prove the user-visible behavior, not just implementation details?
- Did `Cargo.lock` change only when dependency changes are intentional?
- Is the PR body accurate, including release impact?

Codex or another reviewer agent can be used for this pass, but the final merge
decision remains human.

## Stop Conditions

An agent must stop and ask for human judgment when:

- the issue has missing or conflicting acceptance criteria;
- the change conflicts with an ADR;
- a new product concept is needed but not in `CONTEXT.md`;
- credentials, external accounts, paid APIs, or production data are required;
- the same test or build failure repeats three times without a new hypothesis;
- the agent would need destructive git commands or broad unrelated refactors;
- the change affects release automation, binary publishing, or repository
  security settings;
- the agent cannot prove the change with local checks or focused tests.

The stop report should include what was tried, the exact blocker, affected files,
and the smallest human decision needed.

## Verification Matrix

| Change type | Required verification |
| --- | --- |
| CLI behavior | Contract test with `assert_cmd`, stdout assertions, exit code assertions |
| Load definition parsing | YAML fixture or generated temp file, success and failure cases |
| Load report fields | JSON assertions that ignore dynamic IDs/timestamps but check contract fields |
| Source connector | Local deterministic fixture, row count, byte count, schema decision |
| Destination connector | Read destination back and assert records/types, not only file existence |
| Schema drift or validation | Accepted and rejected record cases, reject threshold behavior |
| Write atomicity | Test destination state before and after failed load when practical |
| Retry behavior | Deterministic fake or unit boundary; no real network dependency in default CI |
| Docs-only | Link/source sanity plus vocabulary check against `CONTEXT.md` |
| Release | Full local checks plus release issue checklist |

For this repository, integration tests in `tests/cli_load_contract.rs` are the
main safety net. Prefer adding focused contract tests there before adding new
frameworks.

## Automation Candidates

These loops are useful and low-risk:

- Daily issue triage: list `needs-triage` issues, compare against the readiness
  contract, propose label changes and missing info. Do not edit labels unless the
  result is unambiguous.
- CI failure summarizer: when a PR check fails, summarize failing command,
  likely cause, and whether it is in PR scope.
- Aging PR reviewer: inspect draft PRs older than a threshold, summarize stale
  blockers and next action.
- Docs drift scan: compare `CONTEXT.md`, ADRs, and implementation behavior after
  large PRs; create a follow-up issue instead of silently editing docs.
- Dependency review assistant: when `Cargo.toml` or `Cargo.lock` changes,
  summarize new dependencies and why they are needed.

These loops should not be automated yet:

- auto-merging PRs;
- cutting releases;
- changing repository settings;
- running networked connector tests with real credentials;
- broad architecture refactors without an approved issue and ADR direction.

## Suggested Agent Prompts

Triage loop:

```text
Read AGENTS.md, CONTEXT.md, docs/agents/*, and the issue. Decide whether the
issue is needs-info, ready-for-agent, ready-for-human, wontfix, or needs-triage.
Return only: recommended label, missing information, acceptance criteria draft,
ADR/domain-doc impact, and whether a human decision is required.
```

Implementation loop:

```text
Take issue #N from ready-for-agent to a draft PR. Use Data Spark vocabulary,
preserve ADR decisions, implement the smallest vertical slice, add focused
contract tests, run fmt/clippy/test/release build, and stop if a stop condition
fires. Do not merge.
```

Reviewer loop:

```text
Review this PR as a Data Spark maintainer. Prioritize bugs, contract regressions,
missing tests, ADR conflicts, credential leaks, and release risks. Findings first
with file/line references. Ignore style nits unless they affect maintainability
or correctness.
```

CI repair loop:

```text
Inspect the failing check for this PR. If the failure is caused by the PR and is
within scope, make the smallest fix and rerun the failing command. Stop after
three repeated failures or if the failure is external.
```

## Process Metrics

Track these lightly in GitHub, not in a separate system:

- first-pass CI success rate;
- PRs reopened or reverted after merge;
- time from `ready-for-agent` to draft PR;
- number of review findings per PR;
- issues moved back to `needs-info`;
- repeated failure categories that should become tests or AGENTS guidance.

When a failure repeats, prefer updating one of these durable artifacts:

- issue template or readiness checklist;
- `AGENTS.md` or `docs/agents/*`;
- ADR;
- contract test;
- small helper script for checks.

## Recommended Next Repository Changes

1. Add an issue template for agent-ready implementation issues.
2. Add review guidance to `AGENTS.md` so Codex/GitHub reviewers focus on Data
   Spark contract risks.
3. Add a small `scripts/check.sh` wrapper for the four local checks in
   `docs/agents/gitops.md`.
4. Consider a GitHub Action or scheduled external loop that comments on stale
   `needs-triage` issues with a readiness summary.

These are process improvements, not prerequisites for current development.

## Sources

- https://www.anthropic.com/engineering/building-effective-agents
- https://www.anthropic.com/engineering/demystifying-evals-for-ai-agents
- https://github.com/humanlayer/12-factor-agents
- https://www.humanlayer.dev/blog/12-factor-agents
- https://code.claude.com/docs/en/best-practices
- https://openai.github.io/openai-agents-python/
- https://developers.openai.com/codex/integrations/github
- https://docs.github.com/en/copilot/get-started/best-practices
- https://github.blog/news-insights/product-news/github-copilot-meet-the-new-coding-agent/
- https://agents.md/
- https://addyosmani.com/blog/loop-engineering/
- https://addyosmani.com/blog/ai-coding-workflow/
- https://www.lennysnewsletter.com/p/how-to-design-ai-agent-loops-schedules
- https://www.mindstudio.ai/blog/what-is-loop-engineering-ai-coding-agents
- https://adtmag.com/articles/2026/07/01/loop-engineering-emerges-as-developers-put-ai-coding-agents-on-repeat.aspx
- https://simonwillison.net/2024/Dec/20/building-effective-agents/
