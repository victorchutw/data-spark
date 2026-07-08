# Issue 05: Load local JSONL to DuckDB full refresh

Status: ready-for-agent

## Parent

.scratch/local-file-to-duckdb-parquet/PRD.md

## What to build

Add a complete local JSONL source to DuckDB table destination path using full refresh. A user should be able to combine the JSONL source behavior from the Parquet path with the DuckDB destination behavior from the CSV path and get the same v1 load contract, artifact, load report, load summary, schema decision, row-count, exit-status, and write-atomicity behavior.

This slice proves that the first implementation supports all four required first-slice source and destination combinations.

## Acceptance criteria

- [ ] A `version: 1` load definition can name a local JSONL source and DuckDB table destination.
- [ ] The JSONL source reuses the same local-file and schema inference behavior used for JSONL to Parquet.
- [ ] Records move through the internal execution path as Arrow `RecordBatch` batches.
- [ ] A full refresh writes a readable DuckDB table for the destination dataset.
- [ ] Existing destination table data is replaced according to full refresh semantics.
- [ ] The load report records source, destination, full refresh load mode, schema decision, row counts, byte counts where available, timings, exit status, and destination write atomicity.
- [ ] Destination write atomicity is reported honestly for the DuckDB full refresh path.
- [ ] Stdout includes a human-readable load summary with the load outcome and core counts.
- [ ] Integration tests invoke the CLI end to end and verify the DuckDB destination with DuckDB itself.
- [ ] The integration test matrix covers CSV and JSONL sources against both Parquet and DuckDB destinations.

## Blocked by

- .scratch/local-file-to-duckdb-parquet/issues/03-load-local-jsonl-to-parquet-full-refresh.md
- .scratch/local-file-to-duckdb-parquet/issues/04-load-local-csv-to-duckdb-full-refresh.md
