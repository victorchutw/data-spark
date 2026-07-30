# CSV to DuckDB, full refresh

The smallest complete load, and the one the [README
quickstart](../../README.md#quickstart-your-first-load) walks through:
`customers-load.yml` reads three records from a local CSV file and materializes
them as the `customers` table in a DuckDB database. Nothing declares a schema,
so it is inferred from the observed source records — the two numeric fields as
`int64` and `float64`, the date field as `utf8`, because inference only ever
reaches `boolean`, `int64`, `float64`, and `utf8`. The `full_refresh` load mode
replaces the destination dataset in one transaction, an atomic commit the load
report states as `atomic` / `transactional_replace`.

Run it from a copy of this directory ([why](../README.md#running-them)):

```bash
data-spark load customers-load.yml
```

The load exits `0`, prints a load summary, and writes its load report to
`.data-spark/runs/<load-id>/load-report.json`.
