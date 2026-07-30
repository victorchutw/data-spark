# Rejected records under a reject threshold

Four measurements, two of them unloadable. `load.yml` narrows `reading` to
`float64` and declares `station` required, which turns two records into rejected
records: the `n/a` reading cannot become a number, and the record with no station
holds a null in a required field. Narrowing a text-inferred field to its true
type while rejecting the few values that made inference widen is what schema
overrides are for, and `reject_threshold: 2` says how much of it this load
tolerates.

Run it from a copy of this directory ([why](../README.md#running-them)):

```bash
data-spark load load.yml
```

The load exits `0` and writes the two surviving records to
`measurements-dataset`, because the rejected count is at — not above — the
threshold. The rejected records stream to `rejected-records.jsonl` in the load's
artifact directory as the source is read, one JSON object per rejected record
carrying its source line number, a rejection code (`type_coercion_failed` and
`missing_required_field` here), the offending field when one is known, a message,
and the record content the load could recover. The load report names that artifact and states
the count beside the records read and written.
