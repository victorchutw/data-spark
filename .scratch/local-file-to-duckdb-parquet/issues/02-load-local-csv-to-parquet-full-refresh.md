# Issue 02: Load local CSV to Parquet full refresh

Status: ready-for-agent

## Parent

.scratch/local-file-to-duckdb-parquet/PRD.md

## What to build

Add the first complete data movement path: a `version: 1` load definition reads a local CSV source and writes a BI-ready Parquet directory destination using full refresh. The load should infer a dataset schema, move records through Arrow `RecordBatch` execution, write the destination dataset, produce load artifacts, emit a `report_version: 1` load report, print a load summary, and return the correct exit status.

This is the first real tracer bullet through source reading, schema inference, execution, destination writing, artifact creation, report writing, summary output, and CLI tests.

## Acceptance criteria

- [ ] A `version: 1` load definition can name a local CSV source and Parquet directory destination.
- [ ] The CSV source reads records from a local file path and reports a clear failure for malformed CSV syntax.
- [ ] The load infers a dataset schema from the CSV source by default.
- [ ] Records move through the internal execution path as Arrow `RecordBatch` batches.
- [ ] A full refresh writes a readable Parquet directory for the destination dataset.
- [ ] Existing destination data is replaced according to full refresh semantics.
- [ ] The load report records source, destination, full refresh load mode, schema decision, row counts, byte counts where available, timings, exit status, and destination write atomicity.
- [ ] The artifact directory includes `load-report.json`.
- [ ] Stdout includes a human-readable load summary with the load outcome and core counts.
- [ ] Integration tests invoke the CLI end to end and verify the Parquet destination with an external Parquet-capable reader.

## Blocked by

- .scratch/local-file-to-duckdb-parquet/issues/01-scaffold-versioned-cli-load-contract.md
