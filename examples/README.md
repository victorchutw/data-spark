# Examples

Each directory here is a small, self-contained, runnable example: a fixture file,
one or more load definitions, and a `README.md` saying what it demonstrates. They
are not illustrations — `cargo test --locked` loads every one of them with the
real binary and checks its load report, so an example that stops working fails CI
([tests/examples.rs](../tests/examples.rs)).

| Example | Demonstrates |
| --- | --- |
| [csv-to-duckdb-full-refresh](csv-to-duckdb-full-refresh/) | The README quickstart: CSV into a DuckDB table, full refresh, inferred schema. |
| [csv-to-parquet-append](csv-to-parquet-append/) | Two days of CSV appended to a Parquet dataset directory, format resolved from the path extension. |
| [jsonl-to-duckdb-append](jsonl-to-duckdb-append/) | Two days of JSONL appended to a DuckDB table, with types inferred from JSON values. |
| [csv-to-duckdb-merge](csv-to-duckdb-merge/) | Two days of CSV merged into a DuckDB table by `customer_id`: day 2 updates one record whole and inserts another. |
| [jsonl-to-parquet-full-refresh](jsonl-to-parquet-full-refresh/) | JSONL into a Parquet dataset directory; a second load replaces rather than accumulates. |
| [pinned-schema-additive-drift](pinned-schema-additive-drift/) | A pinned schema bootstrapped by the first load, then extended by `drift_policy: allow_additive_nullable`. |
| [pinned-schema-fail-on-drift](pinned-schema-fail-on-drift/) | The same drift under the default `fail` policy — **a documented failure**: the second load exits 1 with `schema_drift`. |
| [structural-transform](structural-transform/) | Flatten mapping, field selection, and rename mapping together. |
| [declared-types](declared-types/) | Wall-clock timestamps, instant timestamps, and exact decimals declared through `schema.overrides`. |
| [rejected-records](rejected-records/) | A nonzero `reject_threshold` and the `rejected-records.jsonl` artifact. |
| [chunked-execution](chunked-execution/) | A small `chunk_rows` bound committing three chunks, the parallelism clamp, and the retry policy echo. |

## Running them

Every example addresses its files relative to its own directory, so the binary
runs from inside one. A load also writes into that directory: the destination
dataset, the artifact directory (`.data-spark/runs/<load-id>/` by default), and —
for the pinned-schema examples — the pinned schema file the first load
bootstraps. Copy the example somewhere writable first, which is what the test
does, and the repository tree stays clean:

```bash
cp -r examples/csv-to-duckdb-full-refresh /tmp/
cd /tmp/csv-to-duckdb-full-refresh
data-spark load customers-load.yml
```

The fixtures are a handful of records each: these examples demonstrate contracts,
not scale.

## Reference

The keys these definitions use are documented in the [Load Definition
Reference](../docs/reference/load-definition.md), and the reports they produce in
the [Load Report Reference](../docs/reference/load-report.md). The
[guides](../docs/guides/) start from these examples and work through one feature
at a time.
