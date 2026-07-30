# Declared types: timestamps and decimals

Inference only ever reaches `boolean`, `int64`, `float64`, and `utf8`, so
timestamps and exact decimals enter a dataset schema one way only: declared. The
`schema.overrides` block in `load.yml` declares all three declared types over an
inferred CSV schema, and `load_mode` is left out to take its `full_refresh`
default:

```bash
data-spark load load.yml
```

- `paid_at` is a **wall-clock timestamp** (`timestamp`) — what a clock read, with
  no timezone and no absolute moment, so its text must not carry a UTC offset.
- `recorded_at` is an **instant timestamp** (`timestamptz`) — one absolute
  moment, so its text must end in `Z` or an offset, and `2026-07-03T06:00:00+02:00`
  is stored as the UTC instant it spells.
- `amount` is a **decimal** with declared precision and scale
  (`decimal(12,2)`) — exact, and never rounded: `120.00` and `7.25` rescale
  losslessly, while a third fractional digit would reject the record rather than
  round it.

The `payments` table in `payments.duckdb` gets DuckDB's matching column types,
and the load report echoes the overrides beside the resolved schema. Declared
types are per-record promises: a value that does not parse becomes a rejected
record, as in [rejected-records](../rejected-records/).
