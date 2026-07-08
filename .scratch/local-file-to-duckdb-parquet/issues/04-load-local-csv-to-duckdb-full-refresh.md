# Issue 04: Load local CSV to DuckDB full refresh

Status: ready-for-agent

## Parent

.scratch/local-file-to-duckdb-parquet/PRD.md

## What to build

Add a complete local CSV source to DuckDB table destination path using full refresh. A user should be able to use the same v1 load definition contract and CSV reading behavior from the Parquet slice, but write the resulting BI-ready dataset into a DuckDB table. The load must preserve artifact, report, summary, schema decision, row-count, exit-status, and write-atomicity behavior.

This slice proves that destination-specific writing can plug into the shared source, schema inference, Arrow `RecordBatch`, artifact, and report contracts.

## Acceptance criteria

- [ ] A `version: 1` load definition can name a local CSV source and DuckDB table destination.
- [ ] The CSV source reuses the same local-file and schema inference behavior used for CSV to Parquet.
- [ ] Records move through the internal execution path as Arrow `RecordBatch` batches.
- [ ] A full refresh writes a readable DuckDB table for the destination dataset.
- [ ] Existing destination table data is replaced according to full refresh semantics.
- [ ] The load report records source, destination, full refresh load mode, schema decision, row counts, byte counts where available, timings, exit status, and destination write atomicity.
- [ ] Destination write atomicity is reported honestly for the DuckDB full refresh path.
- [ ] The artifact directory includes `load-report.json`.
- [ ] Stdout includes a human-readable load summary with the load outcome and core counts.
- [ ] Integration tests invoke the CLI end to end and verify the DuckDB destination with DuckDB itself.

## Blocked by

- .scratch/local-file-to-duckdb-parquet/issues/02-load-local-csv-to-parquet-full-refresh.md
