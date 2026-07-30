# CSV to DuckDB, full refresh

The smallest complete load, and the one the [README
quickstart](../../README.md#quickstart-your-first-load) walks through: three
records in a local CSV file become the `customers` table in a DuckDB database.
`customers-load.yml` names a `local_file` source and a `duckdb` destination, so
the dataset schema is inferred from the observed source records — the two
numeric-looking columns land as `int64` and `float64`, the date column as
`utf8`, because dates enter a schema only as declared types (see
[declared-types](../declared-types/)). The `full_refresh` load mode replaces
the destination dataset with the source's current records, in one transaction,
so running the load again leaves three records rather than six.

```bash
data-spark load customers-load.yml
```

The load exits `0`, prints a load summary, and writes its load report to
`.data-spark/runs/<load-id>/load-report.json`.
