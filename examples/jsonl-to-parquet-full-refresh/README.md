# JSONL to Parquet, full refresh

An inventory count in JSON Lines becomes the `inventory-dataset` Parquet
directory. `load.yml` uses the `full_refresh` load mode, which replaces the
destination dataset with the source's current records — worth demonstrating
twice.

Run it twice, from a copy of this directory ([why](../README.md#running-them)):

```bash
data-spark load load.yml
data-spark load load.yml
```

Both loads write three records, and afterwards the dataset still holds three: a
full refresh replaces, it does not accumulate. Every part file is staged in a
unique staging directory and swapped in with a single rename, which the load
report states as a `best_effort` write with the `staging_then_replace` strategy —
nothing becomes visible before that rename, and a failure before it leaves the
previous dataset in place.
