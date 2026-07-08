# Issue 01: Scaffold versioned CLI load contract

Status: ready-for-agent

## Parent

.scratch/local-file-to-duckdb-parquet/PRD.md

## What to build

Create the first runnable Data Spark CLI contract for repeatable loads. A user should be able to invoke the CLI with a YAML load definition, have `version: 1` interpreted as the only supported load definition version, receive a load id, see a human-readable load summary on stdout, and find a `report_version: 1` JSON load report in the load's artifact directory. Missing or unsupported load definition versions must fail before any source reading or destination writing would occur.

This slice establishes the product boundary that later source and destination slices plug into: load definition parsing, load lifecycle, artifact directory creation, load report writing, summary output, and process exit behavior.

## Acceptance criteria

- [x] A CLI command accepts a path to a YAML load definition and runs through a load lifecycle.
- [x] A load definition with `version: 1` is accepted as the v1 contract.
- [x] A load definition with a missing version fails before source or destination work starts.
- [x] A load definition with an unsupported version fails before source or destination work starts.
- [x] Every CLI invocation creates or attempts to create a load id and artifact directory for that load.
- [x] Every completed or failed load writes a JSON load report with `report_version: 1`.
- [x] The load report includes load id, source summary, destination summary, load mode, timings, exit status, and error summary where applicable.
- [x] Stdout contains a human-readable load summary and is not required for machine-readable automation.
- [x] Process exit status distinguishes success from failure.
- [x] Contract-level tests cover successful v1 parsing, missing version failure, unsupported version failure, report writing, summary output, and exit status.

## Blocked by

None - can start immediately.
