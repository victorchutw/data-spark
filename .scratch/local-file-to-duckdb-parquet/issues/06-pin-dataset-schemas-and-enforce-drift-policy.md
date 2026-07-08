# Issue 06: Pin dataset schemas and enforce drift policy

Status: ready-for-agent

## Parent

.scratch/local-file-to-duckdb-parquet/PRD.md

## What to build

Extend the working local-file load paths so users can pin a dataset schema and reuse it on repeat loads. When a pinned schema exists, loads should validate observed source records against it, fail fast on schema drift by default, and allow additive nullable schema drift only when the load definition explicitly permits it.

This slice makes repeatable BI-ready datasets stable across all first-slice source and destination combinations.

## Acceptance criteria

- [ ] A load definition can request use of a pinned dataset schema for a repeatable load.
- [ ] A first-time load can produce or persist a pinned schema in a form that later loads can use.
- [ ] A repeat load with records matching the pinned schema succeeds for local CSV to Parquet.
- [ ] A repeat load with records matching the pinned schema succeeds for local JSONL to Parquet.
- [ ] A repeat load with records matching the pinned schema succeeds for local CSV to DuckDB.
- [ ] A repeat load with records matching the pinned schema succeeds for local JSONL to DuckDB.
- [ ] Schema drift against a pinned schema fails fast by default before destination writing.
- [ ] Additive nullable schema drift can continue only when explicitly allowed by drift policy.
- [ ] The load report records the schema decision and drift status for inferred, pinned, drift-failed, and additive-drift loads.
- [ ] CLI-level tests cover inferred schema, pinned schema reuse, default drift failure, and explicit additive nullable drift.

## Blocked by

- .scratch/local-file-to-duckdb-parquet/issues/02-load-local-csv-to-parquet-full-refresh.md
- .scratch/local-file-to-duckdb-parquet/issues/03-load-local-jsonl-to-parquet-full-refresh.md
- .scratch/local-file-to-duckdb-parquet/issues/04-load-local-csv-to-duckdb-full-refresh.md
- .scratch/local-file-to-duckdb-parquet/issues/05-load-local-jsonl-to-duckdb-full-refresh.md
