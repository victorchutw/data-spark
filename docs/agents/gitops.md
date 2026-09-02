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
6. Open a draft PR whose body contains `Closes #<issue-number>`.
7. Keep the PR as draft while still changing behavior. Draft PRs keep referenced issues in `in-progress`, and Copilot auto-review stays quiet until the PR is marked ready.
8. Mark the PR ready for review only when CI is green and behavior is no longer changing. The issue status sync workflow moves referenced issues to `in-review`, and Copilot reviews the ready-for-review snapshot.
9. Settle the Copilot review as described in "Copilot code review" below.
10. Merge only after the required `rust` and `copilot-reviewed` checks pass
    and every review conversation is resolved; the `main` branch ruleset
    enforces all three.
11. Follow "Merge authorization and post-merge cleanup" below. An agent stops
    at merge-ready unless the maintainer authorizes that specific merge.
12. Let GitHub close the issue through the `Closes #<issue-number>` reference.
13. After GitHub reports the PR merged, return the checkout to a clean, current
    `main` as described below, regardless of who performed the merge.

## Merge authorization and post-merge cleanup

An agent may merge a PR only after the maintainer explicitly authorizes that
specific merge. Authorization is single-use: it does not carry to another PR,
permit bypassing a required check, or permit merging with an unresolved review
conversation. Without that authorization, report the PR as merge-ready and
stop.

After any PR merge, require a clean worktree, then synchronize the primary
checkout:

```bash
git switch main
git pull --ff-only origin main
git status --short --branch
test "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)"
```

Cleanup is complete only when the worktree is clean, `main` tracks
`origin/main` without ahead/behind divergence, and both revisions are equal. If
the pull cannot fast-forward, stop and ask for explicit approval before moving
the local `main` branch; never reset or rebase it automatically.

## Copilot code review

Copilot auto-review is enabled by the maintainer's account-level setting, not
by a repository ruleset. Observed behavior on this repo:

- A PR opened ready for review is reviewed immediately, in parallel with the
  first CI run. A PR opened as draft is reviewed only when it is marked ready
  for review.
- A review takes about 2-3 minutes. The `rust` CI job takes about 3
  minutes with a warm dependency cache (2m39s measured on an empty
  change; a source change adds its own compile time) and over an hour
  on a cold cache (toolchain bump, `duckdb` upgrade, or cache
  eviction), so review comments and warm-cache CI completion land in
  the same few-minute window.
- Copilot does not re-review later pushes. It reviews the snapshot that was
  current when the review was requested.

Because there is no automatic re-review, open PRs as drafts and mark them
ready only in their intended final state, so Copilot reviews the code that
will actually merge. After marking a PR ready:

1. Wait for the Copilot review before treating the PR as settled.
2. Handle comments in one batch: fix real findings in a single push, and
   reply to and resolve the threads that need no code change.
3. Re-request a Copilot review manually if the fixes change behavior
   substantially.

Two of the `main` ruleset's requirements make settling mechanical rather
than customary:

- Every review conversation must be resolved before merging, so an unhandled
  Copilot thread blocks the merge even when CI is green.
- The `copilot-reviewed` commit status, posted by
  `.github/workflows/copilot-review-gate.yml`, must be `success`. The gate
  reports `success` once any submitted Copilot review exists on the PR —
  deliberately not per-SHA freshness, because Copilot does not re-review
  pushes, so requiring a review of the current head would deadlock every PR
  after its first post-review push. Fork PRs receive the status as `success`
  with a waiver description instead, because the automatic Copilot review
  fires from the maintainer's account settings and never for external
  authors. The status normally flips inside the gate run that fires when the
  PR becomes ready for review: that run waits for the review, because the
  run a Copilot review submission itself triggers comes from a
  non-collaborator bot actor and can sit behind manual workflow approval.
  If the wait window is missed, any push, any maintainer reply that submits
  a review, or a re-run of the gate re-posts the status.

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
8. After marking the PR ready, wait for the Copilot review and settle it as
   described in "Copilot code review" before reporting the work done.
9. Stop for human judgment if the work conflicts with an ADR, needs a new domain
   concept, requires credentials or production data, changes release automation,
   or repeats the same failing check three times.

Agents may open draft PRs and respond to review comments. Merge authority and
checkout cleanup follow "Merge authorization and post-merge cleanup" above;
agents must not cut releases.

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
3. Update every place that names the version: `Cargo.toml`, the `data-spark`
   entry in `Cargo.lock` — run any `cargo` command to regenerate it, or
   `cargo build --locked` fails — and the `As of v<version>:` line above the
   README's feature summary.
4. Move every `Unreleased` entry in `CHANGELOG.md` under a new
   `## [<version>] - <YYYY-MM-DD>` heading dated with the release date, leaving
   `Unreleased` empty. In the link definitions at the bottom of the file, add
   the new version's GitHub Release link and re-point `[Unreleased]` at the new
   tag, so its compare link starts from the release being cut. The release PR
   carries this change, so the tagged commit already describes what it ships.
5. Confirm local checks pass.
6. Merge the release PR into `main`.
7. Wait for the post-merge `main` CI run to finish; it saves the Rust build
   cache that the tag run restores.
8. Create and push an annotated tag:

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

The release workflow validates that the tag version matches `Cargo.toml`, runs the Rust checks, builds one Linux x86_64 release binary, smoke-tests `--help` and that the built binary's `--version` output carries the tag's version, and publishes a GitHub Release with a single binary asset named `data-spark-linux-x86_64`. Stable tags such as `v0.1.0` are marked latest; prerelease tags such as `v0.1.0-alpha.1` are marked prerelease and are not latest.

The release workflow never saves a build cache: its `Swatinem/rust-cache@v2`
step is restore-only (`save-if: "false"`, with a `shared-key` equal to the
`ci.yml` job id) and consumes the caches that `ci.yml` saves on pushes to
`main`. That is why the flow above waits for the post-merge `main` CI run
before pushing the tag: that run saves the cache for the release commit's
lockfile, and the tag run then builds warm in minutes. If no cache matches —
eviction or a stable toolchain bump — the run falls back toward a cold build:
over an hour, but still correct.

## GitHub automation

- `.github/workflows/issue-default-label.yml` adds `needs-triage` to new issues when no state label is present. External PR labels are applied by triage, not by this workflow.
- `.github/workflows/issue-status-sync.yml` moves referenced issues to `in-progress` for draft PRs and `in-review` for ready PRs. Fork PRs skip the job: their read-only token cannot edit labels, and external PRs should not drive this repo's issue state.
- `.github/workflows/ci.yml` gates pushes and PRs with Rust formatting, linting, tests, and release build checks. Tests run with `--include-ignored` against a live SQL Server container started in-job (ADR-0066).
- `.github/workflows/cargo-audit.yml` runs a weekly (plus manual `workflow_dispatch`) non-blocking `cargo audit` against `Cargo.lock`. The project-local `.cargo/audit.toml` carries four accepted advisories: ADR-0069's three stale-rustls advisories plus the unreachable `rkyv` advisory accepted in #145. The sensor records each vulnerability advisory ID set in a machine-readable block. With no exact-title open tracking issue, a non-empty set opens the same `needs-triage` issue as before. With one open, the sensor compares against the latest block in its own issue body or workflow-authored comments: an unchanged set stays quiet, while a changed set posts the full native report with explicit new and cleared ID lists. A fully cleared set gets the same update and leaves the issue open. Human-authored text is ignored during comparison, and updates never modify the existing issue's labels. The run stays green for findings, so an external advisory publication never reddens `main` or touches the merge gate.
- `.github/workflows/copilot-review-gate.yml` posts the `copilot-reviewed` commit status on PR head SHAs: `pending` until a Copilot review is submitted on the PR, `success` after, and `success` with a waiver description for fork PRs. It is status-only and must never check out PR code, because its `pull_request_target` trigger carries the base-repo write token.
- `.github/workflows/release.yml` creates the single-binary GitHub Release from `v`-prefixed SemVer tags. Its cache step is restore-only (`save-if: "false"`), consuming the Rust build caches that `ci.yml` saves on `main`; a missing cache degrades to a cold but correct build. Its test step mirrors `ci.yml`'s live SQL Server wiring in full (ADR-0066).

## Repository settings

`main` is protected by a single branch ruleset named `main merge gate`, not
by classic branch protection. Run exactly one protection system in steady
state; when swapping systems, briefly overlap them rather than leaving a gap
with neither active. The ruleset requires:

- A pull request before merging, with zero required approving reviews — a
  solo maintainer cannot approve their own PRs, so any nonzero count would
  deadlock the repo — and every review conversation resolved.
- The `rust` and `copilot-reviewed` status checks, with the branch up to
  date with `main` before merging. Strict up-to-dateness costs an occasional
  branch update on this serial solo flow and guarantees the merged result is
  the one CI tested. A branch update re-triggers both checks, and the gate
  re-posts `copilot-reviewed` from the already-submitted review, so updating
  cannot deadlock a reviewed PR.
- No force pushes and no branch deletion.

The `copilot-reviewed` requirement sequences any recreation of this setup:
the gate workflow must be on `main` before a ruleset requiring its context
activates, or every open PR blocks on a status nothing posts.

Repository admins can bypass the ruleset for pull requests only — the
conscious escape hatch (`gh pr merge --admin`, or the bypass confirmation in
the merge box) for Copilot outages and emergencies. Direct pushes to `main`
stay blocked for everyone, admins included.

Release publishing should stay limited to `.github/workflows/release.yml` with `contents: write`; routine CI and issue automation should use narrower permissions.
