# GitOps Workflow

Data Spark work requests are tracked in GitHub Issues and external pull
requests. Changes ship through pull requests, and releases are cut from
signed-off git tags. The repository should have one source of truth for work
state: labels and PR state in GitHub.

## State labels

Each open issue or external PR in the triage queue should have exactly one
primary state label:

| State | Label | Meaning |
| ----- | ----- | ------- |
| Needs triage | `needs-triage` | The issue has not been evaluated yet. |
| Needs info | `needs-info` | The issue is waiting on more information. |
| Ready for agent | `ready-for-agent` | The issue is fully specified and can be picked up by an AFK agent. |
| Ready for human | `ready-for-human` | The issue is fully specified but needs human implementation or judgment. |
| In progress | `in-progress` | Someone or an agent is actively working on it. |
| In review | `in-review` | A PR is open and ready for review. |
| Blocked | `blocked` | Work cannot continue until a named blocker is resolved. |
| Wontfix | `wontfix` | The issue will not be actioned. |

Type labels such as `bug`, `documentation`, and `enhancement` may coexist with
the state label.

## State transitions

```text
needs-triage
  -> needs-info
  -> ready-for-agent
  -> ready-for-human
  -> wontfix

ready-for-agent / ready-for-human
  -> in-progress
  -> blocked
  -> in-review
  -> closed by merged PR

needs-info
  -> needs-triage after info arrives
  -> wontfix if not actionable

blocked
  -> in-progress when the blocker clears
  -> wontfix if there is no viable path
```

When moving an issue manually, remove the previous state label in the same
command:

```bash
gh issue edit 4 \
  --remove-label "needs-triage,needs-info,ready-for-agent,ready-for-human,in-progress,in-review,blocked,wontfix" \
  --add-label "in-progress"
```

When moving an external PR through triage, use the same labels with
`gh pr edit`.

## Implementation flow

1. Start from an issue with `ready-for-agent` or `ready-for-human`.
2. Create a short-lived branch from `main`.
3. Move the issue to `in-progress`.
4. Implement the smallest vertical slice that satisfies the issue.
5. Run local checks before opening a PR.
6. Open a PR whose body contains `Closes #<issue-number>`.
7. Keep the PR as draft while still changing behavior. Draft PRs keep referenced issues in `in-progress`.
8. Mark the PR ready for review when it is ready to merge. The issue status sync workflow moves referenced issues to `in-review`.
9. Merge only after CI passes.
10. Let GitHub close the issue through the `Closes #<issue-number>` reference.

## External PR triage

External PRs are a request surface for this repo. Triage them with the same
canonical labels as issues, but do not treat owner, member, or collaborator PRs
as incoming requests.

For an external PR:

1. Read the PR body, comments, labels, and diff.
2. Decide whether it is `needs-info`, `ready-for-agent`, `ready-for-human`, or
   `wontfix`.
3. Use `needs-info` when the PR cannot be evaluated without reporter input.
4. Use `ready-for-agent` only when an AFK agent can finish or adapt it without
   hidden context.
5. Use `ready-for-human` when maintainer judgment is required.
6. Keep implementation and review decisions in the PR thread.

## AI agent development loop

Agents should treat implementation as a bounded loop: understand the issue,
explore the relevant code, plan the smallest vertical slice, edit, verify,
review, and stop when the work is either proven or blocked. The research-backed
workflow is documented in
`docs/research/ai-agent-loop-engineering-2026-07-08.md`.

For agent-executable work, prefer this path:

1. Confirm the issue has a clear problem statement, acceptance criteria, test
   expectation, ADR/domain impact, out-of-scope notes, and release impact.
2. If that information is missing, move the issue to `needs-info` instead of
   guessing.
3. Use one short-lived branch or worktree per issue.
4. Add or update focused tests before relying on manual inspection.
5. Run the local checks before opening or updating a PR.
6. Keep the PR draft while behavior is still changing.
7. Run a separate review pass focused on bugs, contract regressions, ADR
   conflicts, credentials, and missing tests.
8. Stop for human judgment if the work conflicts with an ADR, needs a new domain
   concept, requires credentials or production data, changes release automation,
   or repeats the same failing check three times.

Agents may open draft PRs and respond to review comments, but must not merge PRs
or cut releases.

## Local checks

Run these before opening or updating a PR:

```bash
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo build --release --locked --bin data-spark
```

## Release flow

Releases are tag-driven. Do not build and upload release binaries manually.

1. Open a release issue using the release issue template.
2. Confirm every included issue is closed or explicitly deferred.
3. Update `Cargo.toml` to the release version.
4. Confirm local checks pass.
5. Merge the release PR into `main`.
6. Create and push an annotated tag:

```bash
git checkout main
git pull --ff-only
git tag -a v0.1.0 -m "Release v0.1.0"
git push origin v0.1.0
```

For prerelease pipeline validation, use a SemVer prerelease version in both `Cargo.toml` and the tag:

```bash
git checkout main
git pull --ff-only
git tag -a v0.1.0-alpha.1 -m "Release v0.1.0-alpha.1"
git push origin v0.1.0-alpha.1
```

The release workflow validates that the tag version matches `Cargo.toml`, runs the Rust checks, builds one Linux x86_64 release binary, smoke-tests `--help`, and publishes a GitHub Release with a single binary asset named `data-spark-linux-x86_64`. Stable tags such as `v0.1.0` are marked latest; prerelease tags such as `v0.1.0-alpha.1` are marked prerelease and are not latest.

## GitHub automation

- `.github/workflows/issue-default-label.yml` adds `needs-triage` to new issues when no state label is present. External PR labels are applied by triage, not by this workflow.
- `.github/workflows/issue-status-sync.yml` moves referenced issues to `in-progress` for draft PRs and `in-review` for ready PRs.
- `.github/workflows/ci.yml` gates pushes and PRs with Rust formatting, linting, tests, and release build checks.
- `.github/workflows/release.yml` creates the single-binary GitHub Release from `v`-prefixed SemVer tags.

## Repository settings

`main` should be protected after the workflows are present on GitHub:

- Require pull requests before merging.
- Require status checks before merging.
- Require the `rust` CI check.
- Require branches to be up to date before merging.
- Block force pushes and branch deletion.

Release publishing should stay limited to `.github/workflows/release.yml` with `contents: write`; routine CI and issue automation should use narrower permissions.
