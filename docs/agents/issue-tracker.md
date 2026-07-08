# Issue tracker: GitHub

Issues and PRDs for this repo live as GitHub Issues in
`victorchutw/data-spark`. Use the `gh` CLI for all issue and pull request
operations.

## Request surfaces

- GitHub Issues are always a triage surface.
- External GitHub PRs are also a triage surface.
- Collaborator PRs are implementation work in progress, not incoming requests.

## Conventions

- **Create an issue**: `gh issue create --title "..." --body "..."`. Use a heredoc or `--body-file -` for multi-line bodies.
- **Read an issue**: `gh issue view <number> --comments`, fetching labels with `--json` when triage state matters.
- **List issues**: `gh issue list --state open --json number,title,body,labels,comments --jq '[.[] | {number, title, body, labels: [.labels[].name], comments: [.comments[].body]}]'` with appropriate `--label` and `--state` filters.
- **Comment on an issue**: `gh issue comment <number> --body "..."`
- **Apply / remove labels**: `gh issue edit <number> --add-label "..."` / `--remove-label "..."`
- **Close**: `gh issue close <number> --comment "..."`

Infer the repo from `git remote -v`; `gh` does this automatically when run inside this clone.
Use the label strings from `docs/agents/triage-labels.md`.

## Pull requests as a triage surface

**PRs as a request surface: yes.**

External PRs are part of the triage queue and should run through the same labels
and states as issues. Ignore collaborators' in-flight PRs.

Treat a PR as external only when `authorAssociation` is `CONTRIBUTOR`,
`FIRST_TIME_CONTRIBUTOR`, or `NONE`. Drop `OWNER`, `MEMBER`, and
`COLLABORATOR`.

Use the `gh pr` equivalents when processing PRs:

- **Read a PR**: `gh pr view <number> --comments` and `gh pr diff <number>`
  for the diff.
- **List external PRs for triage**: `gh pr list --state open --json number,title,body,labels,author,authorAssociation,comments`, then filter by `authorAssociation`.
- **Comment / label / close**: `gh pr comment`, `gh pr edit --add-label` /
  `--remove-label`, and `gh pr close`.

GitHub shares one number space across issues and PRs, so a bare `#42` may be
either. Resolve with `gh pr view 42` and fall back to `gh issue view 42` when
needed.

## When a skill says "publish to the issue tracker"

Create a GitHub issue with `gh issue create`.

## When a skill says "fetch the relevant ticket"

Resolve whether the number is a PR or issue first. Try
`gh pr view <number> --comments`, then fall back to
`gh issue view <number> --comments`.
