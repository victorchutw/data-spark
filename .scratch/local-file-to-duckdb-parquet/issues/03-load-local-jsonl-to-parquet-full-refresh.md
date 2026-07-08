# Issue 03: Load local JSONL to Parquet full refresh

Status: ready-for-agent

## Parent

.scratch/local-file-to-duckdb-parquet/PRD.md

## What to build

Add a complete local JSONL source to Parquet directory destination path using full refresh. A user should be able to reuse the v1 load contract from the CSV slice, point the load definition at a JSONL source, and receive the same BI-ready destination, artifact, load report, load summary, and exit-status behavior.

This slice proves that source-specific parsing can plug into the same schema inference, Arrow `RecordBatch`, Parquet writing, artifact, and report contracts.

## Acceptance criteria

- [ ] A `version: 1` load definition can name a local JSONL source and Parquet directory destination.
- [ ] The JSONL source reads one record per line from a local file path.
- [ ] Malformed JSONL records fail with a useful error when rejected-record handling is not yet configured to accept them.
- [ ] The load infers a dataset schema from observed JSONL records by default.
- [ ] JSONL records move through the same Arrow `RecordBatch` execution contract used by CSV loads.
- [ ] A full refresh writes a readable Parquet directory for the destination dataset.
- [ ] Existing destination data is replaced according to full refresh semantics.
- [ ] The load report records source, destination, full refresh load mode, schema decision, row counts, byte counts where available, timings, exit status, and destination write atomicity.
- [ ] Stdout includes a human-readable load summary with the load outcome and core counts.
- [ ] Integration tests invoke the CLI end to end and verify the Parquet destination with an external Parquet-capable reader.

## Blocked by

- .scratch/local-file-to-duckdb-parquet/issues/02-load-local-csv-to-parquet-full-refresh.md
