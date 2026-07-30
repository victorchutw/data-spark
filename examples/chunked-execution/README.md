# Chunked execution: chunk bound, parallelism, retry

Every load reads, validates, writes, and commits in bounded chunks, and the
`execution` block in `load.yml` sets all three knobs that govern it at once:
`chunk_rows: 2` bounds a chunk to two surviving records, so this five-record
source moves as three chunks instead of the one a default `chunk_rows: 65536`
would hold; `parallelism: 4` asks for four concurrent chunk writes; and the
`retry` block replaces the default policy with five attempts per retry unit and
a 100 ms first backoff clamped at 1000 ms.

Run it from a copy of this directory ([why](../README.md#running-them)):

```bash
data-spark load load.yml
```

`readings-dataset` ends up holding three `part-*.parquet` files, five records in
total: an append commits per chunk, and the Parquet destination commits a chunk
by renaming one complete part file into the dataset directory. The load report
states what actually ran — `batch_count: 3` chunks committed, `chunk_rows: 2`,
and the retry policy echoed with the three values above beside an empty
`attempts` array, because no shipped connector classifies a failure as transient
and nothing was ever retried.

The parallelism the report states is `1`, not the `4` the definition asks for,
beside `connector_parallelism_limit: 1`: a destination connector declares the
maximum parallelism it allows per load mode, and that limit is a hard cap on the
configured value rather than an error to exceed. Both shipped destinations
declare `1`, so every load ships serial today and the report shows both numbers
to make the clamp legible. Tuning these knobs — and reading the report to
confirm what took effect — is the
[execution tuning guide](../../docs/guides/execution-tuning.md).
