# Declared types: timestamps and decimals

Inference only ever reaches `boolean`, `int64`, `float64`, and `utf8`, so
timestamps and exact decimals enter a dataset schema one way only: declared. The
`schema.overrides` block in `load.yml` declares all three declared types over an
inferred CSV schema — `paid_at` as a wall-clock timestamp (`timestamp`), what a
clock read, whose text carries no UTC offset; `recorded_at` as an instant
timestamp (`timestamptz`), one absolute moment, so `2026-07-03T06:00:00+02:00` is
stored as the UTC instant it spells; and `amount` as a decimal with declared
precision and scale (`decimal(12,2)`), exact and never rounded, so `120.00`
rescales losslessly while a third fractional digit would reject the record
instead. `load_mode` is left out to take its `full_refresh` default.

Run it from a copy of this directory ([why](../README.md#running-them)):

```bash
data-spark load load.yml
```

The `payments` table in `payments.duckdb` gets DuckDB's matching column types —
`TIMESTAMP`, `TIMESTAMP WITH TIME ZONE`, and `DECIMAL(12,2)` — and the load
report echoes the overrides beside the resolved schema. Declared types are
per-record promises: a value that does not parse becomes a rejected record, as in
[rejected-records](../rejected-records/).
