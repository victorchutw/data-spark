# GitHub Data Movement Tools Research

Date: 2026-07-08

This note surveys mainstream and design-relevant GitHub projects for a portable data movement CLI written in Rust. Stars are approximate counts observed on GitHub on the research date.

## Selection Lens

The target product is a single-binary tool that moves data from sources to BI-ready destinations. The most relevant examples are not only the largest data platforms, but the projects that prove a useful product shape: connector catalogs, declarative jobs, incremental loading, schema handling, observability, and portable execution.

## Project Signals

| Project | Approx. stars | Shape | Core features worth studying |
| --- | ---: | --- | --- |
| rclone | 58.2k | Go CLI for file/cloud sync | Huge remote catalog, familiar rsync-like verbs, provider abstraction, virtual backends, local/cloud parity. |
| Airflow | 46.1k | Python workflow orchestrator | Scheduling, dependencies, production monitoring UI, retry semantics. Useful as an integration target, not as the core CLI shape. |
| Nushell | 39.9k | Rust structured shell | Structured pipeline UX, file/URL loading into typed data, plugin model. Useful for CLI ergonomics. |
| DuckDB | 39.2k | Embedded analytical database | BI-friendly local OLAP target, direct import/export, SQL surface, broad file/cloud/lakehouse support through extensions. |
| Prefect | 22.8k | Python orchestration framework | Script-to-production workflow model, scheduling/caching/retries/monitoring. Useful for future orchestration integration. |
| Vector | 22.2k | Rust observability pipeline | Single operational binary, source/transform/sink topology, delivery guarantees, buffering, performance discipline. |
| Airbyte | 21.6k | ELT platform | 600+ connectors, connector builder/CDK, self-hosted/cloud split, orchestration integrations. |
| Dagster | 15.8k | Data asset orchestrator | Asset-centric model, observability around produced tables/datasets/reports. Useful for BI-facing metadata thinking. |
| dbt-core | 13.4k | SQL transformation framework | Model/test/document transformation layer after loading. Useful as a downstream handoff target. |
| Debezium | 12.9k | CDC platform | Database log capture, committed row-level change events, Kafka/Kafka Connect durability model. |
| Miller | 9.9k | Go tabular data CLI | Streaming CSV/TSV/JSON processing, Unix pipe compatibility, single binary, larger-than-memory workflows. |
| SeaTunnel | 9.5k | Distributed data integration | Batch/stream/CDC at scale, large connector-oriented distributed architecture. Useful as upper-bound scope reference. |
| DataFusion | 9k | Rust query engine library | Arrow-native SQL/DataFrame engine, CSV/Parquet/JSON/Avro support, vectorized multithreaded execution, extensibility. |
| Redpanda Connect | 8.7k | Go stream processor | Declarative YAML pipelines, connector catalog, CDC, mapping language, at-least-once delivery, metrics/tracing, stateless horizontal scaling. |
| pgloader | 6.5k | PostgreSQL migration loader | Error-tolerant bulk load, rejected-row files, type/data reformatting, schema migration into Postgres. |
| dlt | 5.6k | Python loading library | Incremental loading, schema evolution, data contracts, pipeline inspection, embeddable library shape. |
| ingestr | 3.8k | CLI/SDK ingestion tool | Simple no-code command flags, append/merge/delete+insert incremental modes, many source/destination connectors, Arrow IPC transport. |
| qsv | 3.7k | Rust data-wrangling CLI | Fast tabular commands, schema inference, validation, SQL over CSV/Parquet/JSONL/Arrow, Parquet/Postgres/SQLite outputs, feature-gated binaries. |
| Meltano | 2.5k | Declarative integration engine | Code-first project config, Singer taps/targets ecosystem, plugin hub, reusable connector packaging. |
| Sling | 867 | Go single-binary EL CLI | Closest product shape: database/file/API movement, YAML/JSON configs, connection discovery/testing, env-based connections, wildcard replication, custom SQL streams, flattening, pre/post SQL. |

## Feature Patterns

1. Connector abstraction is the product surface.
   Airbyte, Meltano, Sling, ingestr, SeaTunnel, Redpanda Connect, and rclone all win by making "source to destination" a repeatable contract. A Rust implementation should define narrow `Source`, `Sink`, and `Stream` contracts early, then keep connector-specific quirks outside the execution engine.

2. Declarative jobs beat long command lines once the task is repeatable.
   Sling and Redpanda Connect show the useful split: quick CLI flags for one-off jobs, YAML/JSON for jobs that belong in git. The MVP should support both, but avoid building a UI first.

3. BI readiness means typed, queryable outputs.
   The strongest local-first targets are DuckDB and Parquet. BigQuery is the first cloud warehouse destination because it gives the tool a BI-native hosted path without turning the product into a connector marketplace.

4. Incremental loading is a first-class line, not a later option.
   dlt, ingestr, Airbyte, Debezium, and Redpanda Connect all treat incremental state as core. The design should model load modes explicitly: `full_refresh`, `append`, `merge/upsert`, and later CDC.

5. Schema handling is where tools become trustworthy.
   Useful baseline: infer schema, preview schema, pin schema, detect drift, choose compatible coercions, and produce rejected-row/error reports. pgloader's rejected-data behavior is especially worth copying.

6. Streaming and bounded memory should be a default architecture.
   Miller, qsv, Redpanda Connect, Vector, and DataFusion all point toward chunked/streaming execution. A Rust version should use Arrow `RecordBatch` or a similar batch abstraction internally rather than row-by-row dynamic maps as the main path.

7. Observability should exist without a server.
   Logs, progress counters, row counts, byte counts, error counts, schema drift warnings, retry summaries, and machine-readable load reports are enough for a single binary MVP. Metrics endpoints and tracing can come later.

8. Do not start with orchestration.
   Airflow, Dagster, and Prefect are important downstream integrations, but embedding a scheduler or workflow server into the first version would blur the tool. The first integration point should be "this CLI exits with a useful status and writes a load report."

## Recommended MVP Cut

The pragmatic first product is a batch ELT CLI:

- Sources: local files, HTTP/S3-compatible objects, PostgreSQL, MySQL, SQL Server, SQLite, DuckDB.
- Destinations: DuckDB, Parquet directory, PostgreSQL, SQLite, BigQuery.
- SQL Server scope: source connector in v1; destination support is deferred until the first relational destination write paths are proven.
- BigQuery write path: stage Parquet or newline-delimited JSON, then run a BigQuery batch load job; defer the Storage Write API until near-real-time or high-frequency small-batch loading is needed.
- Formats: CSV, JSONL, Parquet; add Excel later if needed.
- Execution model: Arrow `RecordBatch` is the internal data exchange format, processed by a small custom bounded-memory batch pipeline; DataFusion is deferred as an optional transform/query layer.
- Parallelism: v1 defaults to one dataset per load and conservative connector-specific parallelism. Users can explicitly tune `parallelism`; database sources avoid parallel full table scans by default, while S3-compatible objects and BigQuery staging may use safe upload/load parallelism.
- Transform scope: v1 supports structural transforms needed for loading, such as field selection, renaming, type coercion, JSON flattening, and basic timestamp/decimal handling. Analytical transforms such as arbitrary SQL, joins, aggregations, and business calculations are delegated to dbt, DuckDB, BigQuery SQL, or a future DataFusion layer.
- Job interface: YAML load definitions are the canonical form for repeatable loads; command flags support one-off loads as an on-ramp and can emit a YAML skeleton, but complex behavior and credentials stay out of flags.
- Versioning: YAML load definitions require `version: 1`, and JSON load reports require `report_version: 1`.
- Orchestration: v1 does not include a built-in scheduler. External orchestrators such as cron, CI, Airflow, and Dagster invoke the CLI and consume exit codes, YAML load definitions, and JSON load reports.
- Packaging: primary distribution is single-file native binaries from GitHub Releases for Linux x86_64/aarch64 and macOS x86_64/aarch64. Windows is beta for v1. `cargo install` may exist for Rust users but is not the primary installation path.
- Connections: load definitions use connection references only; credentials resolve from environment variables, local connection profiles, or secure prompts. Local profiles store non-sensitive settings and credential references, not secret values; encrypted secret storage is deferred beyond v1. Profiles live in the platform config directory, with `DATA_SPARK_CONFIG_DIR` for overrides.
- Load modes: full refresh and append first, then merge load within v1. Merge loads require a resolved merge key: users either provide one or explicitly request strict DB metadata key discovery.
- Write atomicity: full refresh and merge use staging-then-commit where the destination can support it; connectors that cannot guarantee atomic commits must mark `atomicity: best_effort` in the load report. Append may be partially written and is traced through load id and load report.
- Retry behavior: v1 retries only clearly transient operations such as network timeouts, 429/5xx responses, and BigQuery job polling. It does not automatically retry after the commit boundary when write atomicity is unclear. Retry attempts are recorded in the load report.
- Schema: infer by default, preview before load, allow overrides, allow pinning for repeatable BI-ready datasets, fail fast on drift by default, allow explicit additive nullable drift, and write rejected rows.
- Validation: v1 validates schema and load rules before writing, including field presence, type coercion, non-null merge keys, and required fields. Invalid records become rejected records. The default reject threshold is `0`, so any rejected record fails the load unless the user explicitly relaxes the threshold.
- Reporting: JSON load report for every load, plus a human-readable load summary on stdout. The report includes load id, source, destination, load mode, schema decision, row counts, byte counts, rejected record counts, drift status, timings, exit status, and error summary. Load artifacts default to `.data-spark/runs/{load_id}/` and can be redirected with `--output-dir` or `artifacts.dir`.

## Current Design Boundary

The product is batch-first for v1. Schemas are inferred by default, but users can override and pin them for repeatable BI-ready loads. Schema drift fails fast by default, with an explicit opt-in path for additive nullable fields. Merge load belongs in v1, after full refresh and append are implemented, and merge loads require a resolved merge key. YAML load definitions are canonical for repeatable loads, while command flags support one-off loads and YAML skeleton generation without becoming a second full configuration language. YAML load definitions require `version: 1`, and JSON load reports require `report_version: 1`. Load definitions contain connection references, not credentials, and v1 local profiles avoid storing secret values. Connection profiles live in the platform config directory and can be relocated with `DATA_SPARK_CONFIG_DIR`. Every load writes artifacts under `.data-spark/runs/{load_id}/` by default, including `load-report.json` and any rejected records. Arrow `RecordBatch` is the internal data exchange format, but v1 starts with a small custom batch pipeline instead of embedding DataFusion in the mandatory path. v1 transform support is limited to structural transforms needed for loading. v1 validation is limited to schema and load-rule checks needed before writing records, and the default reject threshold is `0`. Destination writes use atomic commits where possible and explicitly report best-effort writes otherwise. v1 only retries clearly transient operations and records retry attempts in the load report. v1 parallelism defaults are conservative and connector-specific. v1 does not include a built-in scheduler; external orchestrators invoke the CLI. v1 distribution is GitHub Release binaries first, with Cargo install secondary. The next design boundary is the first implementation slice for a tracer-bullet MVP.

## Sources

- https://github.com/rclone/rclone
- https://github.com/apache/airflow
- https://github.com/nushell/nushell
- https://github.com/duckdb/duckdb
- https://duckdb.org/docs/current/data/overview
- https://github.com/PrefectHQ/prefect
- https://github.com/vectordotdev/vector
- https://github.com/airbytehq/airbyte
- https://github.com/dagster-io/dagster
- https://github.com/dbt-labs/dbt-core
- https://github.com/debezium/debezium
- https://github.com/johnkerl/miller
- https://github.com/apache/seatunnel
- https://github.com/apache/datafusion
- https://github.com/redpanda-data/connect
- https://github.com/dimitri/pgloader
- https://github.com/dlt-hub/dlt
- https://github.com/bruin-data/ingestr
- https://github.com/dathere/qsv
- https://github.com/meltano/meltano
- https://github.com/slingdata-io/sling-cli
- https://docs.cloud.google.com/bigquery/docs/loading-data
- https://docs.cloud.google.com/bigquery/docs/batch-loading-data
- https://docs.cloud.google.com/bigquery/docs/write-api-streaming
