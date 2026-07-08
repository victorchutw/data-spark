# Issue 09: Harden artifact redirection and local-file failure paths

Status: ready-for-agent

## Parent

.scratch/local-file-to-duckdb-parquet/PRD.md

## What to build

Harden the local-file slice around artifact placement and failure behavior. Users should be able to redirect the artifact directory from the load definition or CLI, and local-file failures should stop before misleading destination writes. Empty, header-only, missing-file, malformed-file, and destination failure cases should all produce useful reports, summaries, artifacts, and exit statuses.

This slice makes the first implementation reliable enough for external orchestrators and local troubleshooting.

## Acceptance criteria

- [ ] A user can redirect the artifact directory from the load definition.
- [ ] A user can redirect the artifact directory from the CLI.
- [ ] Redirected artifact directories contain `load-report.json` and rejected-record artifacts when applicable.
- [ ] Missing local source files fail before destination writing.
- [ ] Empty CSV and JSONL source behavior is explicit and covered by tests.
- [ ] Header-only CSV source behavior is explicit and covered by tests.
- [ ] Malformed local-file failures produce useful load reports and human-readable summaries.
- [ ] Destination write failures produce useful load reports and human-readable summaries.
- [ ] Failure reports include exit status, error summary, destination write atomicity, and relevant counts available at the time of failure.
- [ ] CLI-level tests cover default artifact placement, redirected artifact placement, missing source files, empty sources, header-only CSV, malformed source data, and destination write failure reporting.

## Blocked by

- .scratch/local-file-to-duckdb-parquet/issues/02-load-local-csv-to-parquet-full-refresh.md
- .scratch/local-file-to-duckdb-parquet/issues/03-load-local-jsonl-to-parquet-full-refresh.md
- .scratch/local-file-to-duckdb-parquet/issues/04-load-local-csv-to-duckdb-full-refresh.md
- .scratch/local-file-to-duckdb-parquet/issues/05-load-local-jsonl-to-duckdb-full-refresh.md
- .scratch/local-file-to-duckdb-parquet/issues/07-write-rejected-records-and-apply-reject-thresholds.md
