---
name: verify
description: Verify Data Spark CLI load behavior through real commands and external destination readers.
---

# Verify Data Spark CLI

1. Build the current CLI:
   ```bash
   cargo build --locked --bin data-spark
   ```
2. Create an isolated `mktemp -d` workspace with source files and `version: 1` YAML load definitions.
3. Run the real surface twice, first with `load_mode: full_refresh`, then with `load_mode: append`:
   ```bash
   target/debug/data-spark load --output-dir <artifacts> <definition.yml>
   ```
4. Read destinations independently with Python DuckDB (`duckdb` module):
   - Parquet: `SELECT ... FROM read_parquet('<directory>/*.parquet')`
   - DuckDB: connect to the database file and query the dataset table.
5. Read `<artifact-root>/<load-id>/load-report.json` and compare its load ID, artifact directory, counts, exit status, and destination-write posture with CLI stdout and the observed destination.
6. Probe failure boundaries by triggering a pinned-schema rejection and a DuckDB append with an incompatible destination schema; confirm existing destination records remain and the report distinguishes `not_applicable` from `best_effort` write atomicity.

Use only temporary local paths. Capture stdout, report facts, external-reader results, and process exit codes as verification evidence.
