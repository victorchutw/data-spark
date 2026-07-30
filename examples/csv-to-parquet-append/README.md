# CSV to Parquet, append

Two days of events arrive as two CSV files, and the `append` load mode adds each
day's records to the `events-dataset` Parquet directory without changing the
records already there. Neither definition declares a source `format`: it
resolves from the path extension. Run them in order:

```bash
data-spark load load-day-1.yml
data-spark load load-day-2.yml
```

The first load writes three records, the second adds two, and the dataset
directory holds five records across two `part-*.parquet` files. Each append
stages one complete part file per chunk and renames it into place, so the load
report states a `best_effort` write with the `staged_part_append` strategy: a
failure mid-load leaves the chunks that already committed, and the load report
says how many.
