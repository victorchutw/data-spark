# JSONL to DuckDB, append

Two days of orders arrive as JSON Lines — one JSON object per line — and day 2 is
added to the `orders` table in `analytics.duckdb` without changing day 1's
records. JSONL values carry their own JSON types, so inference reads `order_id`
as `int64` and `amount` as `float64` from the values themselves rather than from
text. Run them in order:

```bash
data-spark load load-day-1.yml
data-spark load load-day-2.yml
```

`load-day-1.yml` uses `full_refresh` because a DuckDB append writes into a table
that already exists: the first load is what creates it, with three records.
`load-day-2.yml` then appends two more, and the table holds five. A DuckDB append
runs one auto-committed `INSERT ... BY NAME` per chunk — a `best_effort` write
with the `insert` strategy — and matches records to the existing table by field
name, so day 2's records must fit the columns day 1 created.
