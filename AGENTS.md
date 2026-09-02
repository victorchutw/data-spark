## Communication

Discuss with the maintainer in Traditional Chinese (zh-TW). Repository artifacts — code, identifiers, comments, commit messages, PR descriptions, ADRs, and `docs/` — stay in English to match the existing convention, unless the maintainer asks otherwise.

## Agent skills

### Issue tracker

Issues, specs, and tickets are tracked in GitHub Issues for `victorchutw/data-spark`; external PRs are a triage surface. See `docs/agents/issue-tracker.md`.

### Triage labels

This repo uses the default triage label vocabulary: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, and `wontfix`. See `docs/agents/triage-labels.md`.

### GitOps workflow

Work starts from GitHub Issues, moves through issue state labels and PRs, and releases from `v`-prefixed SemVer tags to a single Linux x86_64 binary asset. See `docs/agents/gitops.md`.

### Agent loop

For issue implementation, PR review, CI repair, Copilot review settling, merge authorization, or post-merge cleanup — including work resumed in a new session — read and run the bounded loop in `docs/agents/gitops.md` and its research basis in `docs/research/ai-agent-loop-engineering-2026-07-08.md`. Keep loop state in GitHub and git history; continue until required checks pass and every review thread is resolved, or a documented stop condition fires.

### Domain docs

This is a multi-context repo. Start with root `CONTEXT-MAP.md`, then read the relevant context docs and ADRs. The current context is `CLI/core`. See `docs/agents/domain.md`.
