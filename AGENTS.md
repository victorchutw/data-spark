## Agent skills

### Issue tracker

Issues and PRDs are tracked in GitHub Issues for `victorchutw/data-spark`; external PRs are not a triage surface. See `docs/agents/issue-tracker.md`.

### Triage labels

This repo uses the default triage label vocabulary: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, and `wontfix`. See `docs/agents/triage-labels.md`.

### GitOps workflow

Work starts from GitHub Issues, moves through issue state labels and PRs, and releases from `v*.*.*` tags to a single Linux x86_64 binary asset. See `docs/agents/gitops.md`.

### Domain docs

This is a single-context repo with one root `CONTEXT.md` and root `docs/adr/` directory. See `docs/agents/domain.md`.
