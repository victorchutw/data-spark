# PRD: Local CSV/JSONL to DuckDB and Parquet

Status: ready-for-agent

Date: 2026-07-08

Feature slug: local-file-to-duckdb-parquet

## Problem Statement

Data Spark needs its first executable data movement path to prove the core product contract before adding broader v1 connectors and cloud destinations. A user with local operational files should be able to define a repeatable load, run it with a portable CLI, and receive BI-ready datasets plus trustworthy diagnostics without standing up a service, scheduler, credential store, or connector marketplace.

Today the repo has accepted product and architecture decisions, but no Rust source scaffold. The first implementation slice must turn those decisions into a concrete, testable product increment: local CSV and JSONL sources become typed DuckDB tables or Parquet directories, and every load produces predictable artifacts, reports, summaries, schema behavior, rejected-record handling, and exit behavior.

## Solution

Build the first tracer-bullet implementation slice of Data Spark as a batch-first Rust CLI that loads local CSV and JSONL sources into DuckDB tables and Parquet directories. Repeatable loads are described by `version: 1` YAML load definitions. The execution path uses Arrow `RecordBatch` internally, infers dataset schemas by default, supports schema pinning for repeatability, validates records before destination writes, writes rejected records, emits a `report_version: 1` JSON load report, and prints a human-readable load summary to stdout.

The slice is accepted when a user can run a versioned load definition for each supported source/destination combination, inspect the destination dataset with external tools, and inspect `.data-spark/runs/{load_id}/` for `load-report.json` plus any rejected-record artifacts. The CLI must return useful exit codes so external orchestrators can treat success and failure without scraping stdout.

This PRD intentionally does not reopen the accepted ADRs. It translates them into requirements for the first implementation slice.

## User Stories

1. As a data practitioner, I want to load a local CSV file into a DuckDB table, so that I can query operational data locally with BI-friendly tooling.
2. As a data practitioner, I want to load a local JSONL file into a DuckDB table, so that semi-structured exported records can become a typed dataset.
3. As a data practitioner, I want to load a local CSV file into a Parquet directory, so that I can create a columnar dataset for downstream analysis.
4. As a data practitioner, I want to load a local JSONL file into a Parquet directory, so that line-delimited records can become a portable analytical dataset.
5. As a repeat user, I want a YAML load definition to be the canonical repeatable interface, so that the load can live in git and run the same way later.
6. As a repeat user, I want every YAML load definition to declare `version: 1`, so that future contract changes do not silently reinterpret my load.
7. As a new user, I want schema inference by default, so that I can get from a source file to a BI-ready dataset without writing a schema first.
8. As a BI maintainer, I want to pin an inferred dataset schema, so that repeat loads keep the destination dataset stable.
9. As a BI maintainer, I want schema drift against a pinned schema to fail fast by default, so that downstream dashboards are not changed silently.
10. As a BI maintainer, I want an explicit additive nullable drift policy, so that low-risk source expansion can be accepted deliberately.
11. As a data practitioner, I want field presence and type coercion to be validated before destination writing, so that invalid records are caught at the load boundary.
12. As a data practitioner, I want records that violate schema or load rules to become rejected records, so that I can inspect what failed without losing context.
13. As a cautious operator, I want the default reject threshold to be `0`, so that unexpected rejected records fail the load loudly.
14. As an exploratory user, I want to configure a higher reject threshold, so that tolerant local loads can continue while still recording rejected records.
15. As an operator, I want a unique load id for each load, so that reports, destination writes, and troubleshooting artifacts can be correlated.
16. As an operator, I want load artifacts under `.data-spark/runs/{load_id}/` by default, so that each load leaves an inspectable local record.
17. As an operator, I want to redirect the artifact directory from the load definition or CLI, so that CI and local workflows can collect artifacts where they need them.
18. As an automation author, I want every load to write a JSON load report, so that orchestration can consume machine-readable results.
19. As an automation author, I want every JSON load report to declare `report_version: 1`, so that report readers can depend on an explicit contract.
20. As an automation author, I want the load report to include source, destination, load mode, schema decision, row counts, byte counts, rejected record counts, drift status, timings, exit status, and error summary, so that I can diagnose a load without parsing console text.
21. As a CLI user, I want stdout to contain a human-readable load summary, so that I can see the outcome quickly in a terminal.
22. As an orchestrator user, I want success and failure to be reflected in process exit status, so that cron, CI, Airflow, or Dagster can respond correctly.
23. As a data practitioner, I want full refresh loads for DuckDB and Parquet destinations, so that I can replace a destination dataset with the current source records.
24. As a data practitioner, I want append loads where the destination supports them, so that I can add records without replacing existing destination data.
25. As an operator, I want the load report to state destination write atomicity, so that I understand whether a failed load may have changed the destination.
26. As an operator, I want append loads to be traceable through load id and load report, so that partial writes can be investigated if they occur.
27. As a developer, I want the core execution pipeline to exchange Arrow `RecordBatch` values, so that connectors and destinations share typed, bounded-memory batches.
28. As a developer, I want a small custom batch pipeline for the first slice, so that the product contract can be proven without mandatory DataFusion planner behavior.
29. As a developer, I want CSV and JSONL readers to produce the same internal batch contract, so that source-specific parsing does not leak into destination writers.
30. As a developer, I want DuckDB and Parquet writers to consume the same internal batch contract, so that destinations can be tested against shared load semantics.
31. As a user with malformed source data, I want parse and coercion errors to be reported with enough record context, so that I can fix the source data or schema.
32. As a user with empty or header-only source files, I want a clear failure or empty-load result according to the load definition, so that the outcome is not ambiguous.
33. As a user with missing local files, I want the load to fail before destination writing, so that bad paths do not create misleading artifacts or partial datasets.
34. As a user with existing DuckDB tables, I want the chosen load mode to determine whether the dataset is replaced or appended, so that destination changes are predictable.
35. As a user with existing Parquet directories, I want the chosen load mode to determine how files are replaced or appended, so that downstream readers see the intended dataset.
36. As a local user, I want one dataset per load by default, so that first-slice behavior stays simple and predictable.
37. As a local user, I want conservative load parallelism by default, so that local files and destinations are not stressed unexpectedly.
38. As a maintainer, I want first-slice behavior to be covered by CLI-level tests, so that future connector work cannot break the core load contract unnoticed.
39. As a maintainer, I want contract tests for report and load definition versions, so that v1 artifacts remain readable by external tools.
40. As a maintainer, I want rejected-record files to be stable enough for troubleshooting, so that users can inspect failures without reading internal logs.

## Implementation Decisions

- Data Spark starts as a portable Rust command-line tool distributed as a single binary.
- The first implementation slice is limited to batch data movement from local CSV and JSONL sources to DuckDB and Parquet destinations.
- YAML load definitions are the canonical interface for repeatable loads; command-line flags may exist as an on-ramp but must not become a second full configuration language.
- YAML load definitions must declare `version: 1`; unsupported or missing versions fail before source reading or destination writing.
- JSON load reports must declare `report_version: 1`; every load writes a report even when the load fails.
- The load definition model must describe source connector, destination connector, dataset identity, load mode, schema choices, drift policy, reject threshold, and artifact directory.
- The first source connectors are local CSV and local JSONL. They read local filesystem paths and produce Arrow `RecordBatch` batches.
- The first destination connectors are DuckDB table and Parquet directory. They consume Arrow `RecordBatch` batches and report destination write atomicity.
- Arrow `RecordBatch` is the internal data exchange format across source reading, validation, structural transform, rejected-record handling, and destination writing.
- The execution engine starts as a small custom bounded-memory batch pipeline rather than a mandatory DataFusion execution path.
- Schema inference is the default for first-time loads. Users can override inferred fields and pin a dataset schema for repeatable loads.
- Pinned schemas are reused on later loads to protect BI-ready datasets from silent type or field changes.
- Schema drift against a pinned schema fails fast by default. Additive nullable drift can continue only when explicitly allowed.
- v1 structural transforms for this slice are limited to what loading requires: field selection, field renaming, type coercion, JSON flattening, and basic timestamp and decimal handling.
- Load validation runs before writing valid records to the destination and covers field presence, type coercion, and required fields. Merge-key validation is not required in this slice because merge loads are out of scope.
- Invalid records become rejected records. Rejected-record artifacts must include enough source context and error information to troubleshoot failures.
- The default reject threshold is `0`, meaning any rejected record fails the load unless the load definition explicitly configures a higher threshold.
- Each load creates a load id and writes artifacts under `.data-spark/runs/{load_id}/` by default, including `load-report.json` and rejected-record files when applicable.
- Users can redirect artifacts with CLI output directory options or an artifact directory in the YAML load definition.
- Stdout is reserved for a human-readable load summary. Automation should read the JSON load report and process exit code.
- The JSON load report includes load id, source, destination, load mode, schema decision, row counts, byte counts, rejected record counts, drift status, timings, exit status, error summary, retry attempts, and destination write atomicity.
- Full refresh should use staging-then-commit where the destination can support an atomic commit. Connectors that cannot guarantee atomicity must report `best_effort`.
- Append loads may have partial writes by nature. They must be traceable through load id, report data, and destination write atomicity rather than presented as atomic changes.
- Automatic retries are not central to this local-file slice. If implemented for local transient operations, retry attempts must be recorded in the load report and must not hide commit-boundary uncertainty.
- The default execution shape is one dataset per load with conservative parallelism.
- No credential values are accepted in load definitions or flags. This slice should not require credentials because all sources and destinations are local.
- The first implementation should keep later connector expansion in mind by preserving clear source, destination, execution, schema, validation, artifact, report, and summary boundaries.

## Testing Decisions

- The primary testing seam is the highest observable product contract: invoke the CLI with a `version: 1` load definition, then assert the destination dataset, artifact directory, JSON load report, rejected-record files, stdout summary, and exit status.
- Tests should assert external behavior and contract shape rather than internal implementation details such as private structs, function names, or batching internals.
- Golden or snapshot-style assertions are appropriate for stable report and summary fields, but volatile fields such as timings and generated load ids should be normalized or pattern-matched.
- Contract tests must cover missing and unsupported load definition versions.
- Contract tests must cover `report_version: 1` on successful and failed loads.
- Source-to-destination integration tests must cover CSV to DuckDB, JSONL to DuckDB, CSV to Parquet, and JSONL to Parquet.
- Schema tests must cover inferred schemas, pinned schemas, schema drift failure, and explicit additive nullable drift.
- Rejected-record tests must cover parse errors, type coercion failures, required field failures, default reject threshold `0`, and a configured higher reject threshold.
- Artifact tests must cover default artifact placement and redirected artifact placement.
- Load summary tests must verify that stdout remains human-readable and does not need to be scraped for machine-readable automation.
- Load report tests must verify row counts, byte counts, rejected record counts, schema decision, drift status, destination write atomicity, exit status, and error summary.
- Destination tests must inspect DuckDB tables with DuckDB itself and Parquet directories with a Parquet-capable reader rather than trusting only Data Spark internals.
- Failure tests must verify that missing local files fail before destination writing.
- Failure tests must verify that validation failures respect destination write boundaries and report atomicity honestly.
- No prior Rust test suite exists in this repo yet, so this slice establishes the first testing pattern for later source and destination connectors.

## Out of Scope

- Networked sources, including HTTP, S3-compatible objects, PostgreSQL, MySQL, SQL Server, SQLite, and DuckDB database sources.
- Cloud destinations, including BigQuery, Snowflake, Redshift, Databricks, and other cloud warehouse destinations.
- BigQuery batch load jobs and staging behavior.
- Merge loads and resolved merge key behavior.
- CDC, streaming replication, near-real-time delivery, and log-based capture.
- Built-in scheduling, orchestration service state, workflow UI, and history retention service.
- Credential profiles, credential references, secure prompts, encrypted profile storage, and platform config directory behavior.
- Analytical transforms such as arbitrary SQL, joins, aggregations, calculations, and business modeling.
- DataFusion as a mandatory execution path.
- A connector marketplace or broad connector catalog.
- Release packaging, GitHub Release automation, and Windows beta distribution.
- Multiple datasets in a single load definition.
- Broad data quality checks such as statistical assertions, business validations, and uniqueness checks beyond load rules.

## Further Notes

- Source of truth artifacts: `CONTEXT.md`, `docs/research/github-data-movement-tools-2026-07-08.md`, and accepted ADRs under `docs/adr/`.
- The most directly scoped ADRs are ADR 0026 for versioned load definitions and reports, and ADR 0027 for the first local file to DuckDB and Parquet slice.
- Later v1 capabilities remain visible as roadmap work, but this PRD is intentionally scoped to the first executable BI-ready load path.
- This PRD is ready for `to-issues` to break into independently implementable issues.
