# JSONL to Parquet, full refresh

An inventory count in JSON Lines becomes the `inventory-dataset` Parquet
directory. The `full_refresh` load mode replaces the destination dataset with
the source's current records, so this example is worth running twice:

```bash
data-spark load load.yml
data-spark load load.yml
```

Both loads write three records, and after the second the dataset still holds
three — a full refresh replaces, it does not accumulate. The Parquet
destination stages every part file in a unique staging directory and swaps it in
with a single rename, so the load report states a `best_effort` write with the
`staging_then_replace` strategy: nothing becomes visible before that rename, and
a failure before it leaves the previous dataset in place.
