# JSONL to DuckDB, append

Two days of orders arrive as JSON Lines — one JSON object per line — and day 2
is added to the `orders` table in `analytics.duckdb` without changing day 1's
records. JSONL values carry their own JSON types, so inference reads `order_id`
as `int64` and `amount` as `float64` from the values themselves rather than from
text. `load-day-1.yml` uses `full_refresh` because a DuckDB append writes into a
table that already exists: the first load is what creates it.

Run them in order, from a copy of this directory
([why](../README.md#running-them)):

```bash
data-spark load load-day-1.yml
data-spark load load-day-2.yml
```

The table ends up holding five records. A DuckDB append commits once per chunk —
a `best_effort` write with the `insert` strategy — and matches records to the
existing table by field name, so day 2's records must fit the fields day 1
created.
