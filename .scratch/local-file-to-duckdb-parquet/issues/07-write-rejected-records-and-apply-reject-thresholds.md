# Issue 07: Write rejected records and apply reject thresholds

Status: ready-for-agent

## Parent

.scratch/local-file-to-duckdb-parquet/PRD.md

## What to build

Add rejected-record handling across the local-file load paths. Records that cannot be written without violating the dataset schema or load rules should be captured as rejected records with useful context. The default reject threshold is `0`, so any rejected record fails the load unless the load definition explicitly configures a higher threshold.

This slice turns parse, type coercion, field presence, and required-field failures into inspectable load artifacts rather than opaque load failures.

## Acceptance criteria

- [ ] Invalid source records become rejected records when they violate schema or load rules.
- [ ] Rejected-record artifacts are written under the load's artifact directory when rejected records exist.
- [ ] Rejected-record artifacts include enough source context and error information to troubleshoot the failed records.
- [ ] The default reject threshold is `0`.
- [ ] A load with one rejected record and no explicit threshold fails.
- [ ] A load with rejected records at or below an explicitly configured threshold can complete according to the configured load rules.
- [ ] Rejected-record counts are recorded in the load report.
- [ ] The load summary reports rejected-record counts in human-readable form.
- [ ] Tests cover malformed CSV, malformed JSONL, type coercion failure, missing required fields, default threshold failure, and configured threshold success.
- [ ] Failure tests verify that rejected-record handling respects destination write boundaries and reported write atomicity.

## Blocked by

- .scratch/local-file-to-duckdb-parquet/issues/06-pin-dataset-schemas-and-enforce-drift-policy.md
