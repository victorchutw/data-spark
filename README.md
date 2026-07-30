# Data Spark

Data Spark is a portable data movement tool for turning operational data into
BI-ready datasets. A load is described once in a small YAML load definition
and run with a single command from a single Linux x86_64 binary. Every load —
success or failure — writes a machine-readable JSON load report alongside its
other load artifacts, so the outcome is always inspectable by people and
automation alike.

## Features

As of v0.2.0:

- **Sources**: local CSV and JSONL files.
- **Destinations**: DuckDB databases and Parquet datasets.
- **Load modes**: full refresh and append.
- **Schema inference** from source records, with schema pinning and a drift
  policy that keeps a BI-ready dataset stable across repeated loads.
- **Schema overrides and declared types**: wall-clock timestamps, instant
  timestamps, and exact decimals enter a schema only through explicit
  declaration, never through inference.
- **Structural transforms**: flatten mappings for nested values, field
  selection, and rename mappings.
- **Rejected records**: records that cannot be written without violating the
  dataset schema are streamed to a rejected-records artifact, with a
  configurable reject threshold that decides when the load fails.
- **Chunked execution**: loads read, validate, and write in bounded chunks, so
  memory stays flat regardless of source size.
- **Retry**: transient write failures are retried with backoff, and every
  attempt is recorded in the load report.
- **Parallelism**: chunk writes can run in parallel, capped by the limit each
  destination connector declares per load mode.
- **Versioned contracts**: load definitions (YAML) and load reports (JSON)
  each carry an explicit contract version.

## Install

Data Spark ships as a single Linux x86_64 binary attached to each
[GitHub Release](https://github.com/victorchutw/data-spark/releases).
Download the `data-spark-linux-x86_64` asset from the latest release — from
the release page in a browser, or with the GitHub CLI:

```bash
gh release download --repo victorchutw/data-spark --pattern data-spark-linux-x86_64
```

Verify the download against the SHA256 published in the release notes, make it
executable, and check that it runs:

```bash
sha256sum data-spark-linux-x86_64
# compare the output with the SHA256 line in the release notes
chmod +x data-spark-linux-x86_64
./data-spark-linux-x86_64 --help
```

Optionally install it into a directory on your `PATH` (the quickstart below
invokes `data-spark` directly). `~/.local/bin` is a common choice — `install
-D` creates it if needed, but confirm the directory is on your `PATH`:

```bash
install -D -m 0755 data-spark-linux-x86_64 ~/.local/bin/data-spark
```

## Quickstart: your first load

This quickstart is also a runnable example —
[examples/csv-to-duckdb-full-refresh](examples/csv-to-duckdb-full-refresh/) holds
the same two files, and the test suite runs them on every build.

Create a small CSV source:

```bash
cat > customers.csv <<'EOF'
customer_id,name,signup_date,total_spend
1,Ada,2026-01-05,42.50
2,Grace,2026-02-11,7.25
3,Katherine,2026-03-02,120.00
EOF
```

Create a load definition that full-refreshes the CSV into a DuckDB database:

```bash
cat > customers-load.yml <<'EOF'
version: 1
source:
  connector: local_file
  path: customers.csv
  format: csv
destination:
  connector: duckdb
  path: customers.duckdb
dataset: customers
load_mode: full_refresh
EOF
```

Run the load:

```bash
data-spark load customers-load.yml
```

The command prints a load summary and exits 0 on success (a failed load exits
1 and still writes its report):

```text
Data Spark load c0eeb484-7210-43db-b6f4-0c6d2470f026
Status: succeeded
Load mode: full_refresh
Source: connector=local_file, path=customers.csv, format=csv
Destination: connector=duckdb, path=customers.duckdb
Records read: 3
Records written: 3
Records rejected: 0
Artifact directory: .data-spark/runs/c0eeb484-7210-43db-b6f4-0c6d2470f026
Load report: .data-spark/runs/c0eeb484-7210-43db-b6f4-0c6d2470f026/load-report.json
```

The `customers` dataset now lives in `customers.duckdb`, ready to query with
any DuckDB client.

Each load writes its artifacts to its own artifact directory —
`.data-spark/runs/<load-id>/` by default, or under the directory given with
`data-spark load --output-dir <dir>` — and the load report lands there as
`load-report.json`. A trimmed example of the report above (the full report
also records source and destination summaries, byte counts, rejected-record
facts, execution details, and timings):

```json
{
  "report_version": 1,
  "load_id": "c0eeb484-7210-43db-b6f4-0c6d2470f026",
  "dataset": "customers",
  "load_mode": "full_refresh",
  "schema_decision": {
    "mode": "inferred",
    "fields": [
      { "name": "customer_id", "type": "int64", "nullable": true },
      { "name": "name", "type": "utf8", "nullable": true },
      { "name": "signup_date", "type": "utf8", "nullable": true },
      { "name": "total_spend", "type": "float64", "nullable": true }
    ],
    "drift_status": "not_applicable"
  },
  "row_counts": {
    "source": 3,
    "written": 3,
    "rejected": 0
  },
  "destination_write": {
    "atomicity": "atomic",
    "strategy": "transactional_replace"
  },
  "exit_status": "succeeded",
  "process_exit_code": 0
}
```

## Documentation

[examples/](examples/) holds small, self-contained, runnable examples — the four
source and destination pairs, both load modes, schema pinning and drift
policies, structural transforms, declared types, rejected records, and chunked
execution — and the test suite loads every one of them, so none of them can
rot.

[docs/guides/](docs/guides/) works through one feature at a time, each guide
starting from one of those examples: [schema
pinning](docs/guides/schema-pinning.md), [rejected
records](docs/guides/rejected-records.md), [declared
types](docs/guides/declared-types.md), and [execution
tuning](docs/guides/execution-tuning.md). Both contracts are documented key by
key in the [Load Definition Reference](docs/reference/load-definition.md) and
the [Load Report Reference](docs/reference/load-report.md).

The repository also carries maintainer- and agent-facing documentation:
[CONTEXT.md](CONTEXT.md) defines the ubiquitous language used throughout this
README, and [docs/adr/](docs/adr/) records the architecture decisions behind
the behavior described above.

## Versioning

Releases follow [SemVer](https://semver.org/) and are cut from `v`-prefixed
tags. The load definition contract (`version` in the YAML) and the load report
contract (`report_version` in the JSON) are versioned independently of the
binary, so both writers and readers can rely on the declared version.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
