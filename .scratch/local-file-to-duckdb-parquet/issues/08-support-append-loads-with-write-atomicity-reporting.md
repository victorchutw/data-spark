# Issue 08: Support append loads with write atomicity reporting

Status: ready-for-agent

## Parent

.scratch/local-file-to-duckdb-parquet/PRD.md

## What to build

Add append load mode for DuckDB and Parquet destinations across the supported local CSV and JSONL sources. A user should be able to choose append in the load definition and have new records added without replacing existing destination data. Because append loads can have partial writes by nature, each load must remain traceable through load id, load report, artifact directory, and destination write atomicity reporting.

This slice completes the first-slice load-mode surface while preserving the PRD's reporting and troubleshooting guarantees.

## Acceptance criteria

- [ ] A `version: 1` load definition can select append load mode.
- [ ] CSV to Parquet append adds records without replacing existing destination data.
- [ ] JSONL to Parquet append adds records without replacing existing destination data.
- [ ] CSV to DuckDB append adds records without replacing existing destination table data.
- [ ] JSONL to DuckDB append adds records without replacing existing destination table data.
- [ ] The load report records append load mode, row counts, rejected-record counts, exit status, and destination write atomicity.
- [ ] Append loads are traceable by load id and artifact directory.
- [ ] Failure behavior does not claim atomic destination changes when the connector cannot guarantee them.
- [ ] CLI-level tests cover append success for all first-slice source and destination combinations.
- [ ] Failure tests cover append load reporting when validation or destination writing fails.

## Blocked by

- .scratch/local-file-to-duckdb-parquet/issues/02-load-local-csv-to-parquet-full-refresh.md
- .scratch/local-file-to-duckdb-parquet/issues/03-load-local-jsonl-to-parquet-full-refresh.md
- .scratch/local-file-to-duckdb-parquet/issues/04-load-local-csv-to-duckdb-full-refresh.md
- .scratch/local-file-to-duckdb-parquet/issues/05-load-local-jsonl-to-duckdb-full-refresh.md
- .scratch/local-file-to-duckdb-parquet/issues/07-write-rejected-records-and-apply-reject-thresholds.md
