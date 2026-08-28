# Merging records by key

A full refresh replaces the whole dataset and an append only ever adds, but a
dataset that tracks current state — customers, inventory, account balances —
wants day 2 to *correct* day 1: update the records that changed, insert the
ones that are new, and leave everything else alone. That is `load_mode:
merge`, a keyed upsert
([ADR-0057](../adr/0057-land-merge-loads-on-duckdb-only-and-decline-elsewhere.md)):
you declare which fields identify a record, and each surviving source record
either replaces its match or inserts. Merge never deletes — a record absent
from the source stays in the destination.

This guide works through the mode from a runnable example: the keys, the
bootstrap, what happens to null and duplicate keys, and the report fields
that say what the merge did.

## Start from the example

[examples/csv-to-duckdb-merge](../../examples/csv-to-duckdb-merge/) loads two
days of customers into a DuckDB table. Copy it somewhere writable and run
both loads — a load writes into the directory it runs from
([why](../../examples/README.md#running-them)):

```bash
cp -r examples/csv-to-duckdb-merge /tmp/
cd /tmp/csv-to-duckdb-merge
data-spark load load-day-1.yml
data-spark load load-day-2.yml
```

Every command below was run, and every YAML fragment and report excerpt is
taken from a real load against the release binary.

A merge load is the load mode plus one block naming the merge keys:

```yaml
load_mode: merge
merge:
  keys: [customer_id]
```

## The first load bootstraps the table

Merge writes into a table that already exists — it probes the destination
before anything else and fails with `destination_write_failed` ("before
merge") when the table is missing, exactly like an append. That is why
`load-day-1.yml` is a `full_refresh`: the first load creates the table, and
the definition switches to `merge` from day 2 on. No table is ever created
implicitly
([ADR-0059](../adr/0059-execute-duckdb-merge-as-a-staged-terminal-transaction.md)).

## Matched records are replaced whole

Day 1 loads three customers. Day 2 carries two records: customer 2 again,
and a new customer 4:

```csv
customer_id,name,tier
2,Grace Hopper,gold
4,Katherine,silver
```

The merge matches on `customer_id`, so customer 2 is updated and customer 4
inserted — and the update is whole-record: every non-key field takes the
source value. Day 2's record moves customer 2 from `Grace,silver` to
`Grace Hopper,gold` in one piece; there is no per-field patching. Customers
1 and 3, absent from day 2, stay untouched. The report counts the partition:

```json
"destination_write": {
  "atomicity": "atomic",
  "strategy": "transactional_merge",
  "merge": {
    "updated": 1,
    "inserted": 1
  }
}
```

`updated + inserted` always equals `row_counts.written`. The whole merge
commits once, terminally: the source records stage inside a transaction and
one commit makes everything visible together, so a failure anywhere leaves
the destination exactly as it was
([ADR-0059](../adr/0059-execute-duckdb-merge-as-a-staged-terminal-transaction.md)).

## Keys name dataset fields

`merge.keys` speaks the dataset's field names — what the load materializes
after `transform.flatten`, `transform.select`, and `transform.rename` — so a
rename target or a flatten output is a perfectly good key, and a multi-field
key is just a longer list:

```yaml
transform:
  rename:
    id: customer_id
load_mode: merge
merge:
  keys: [customer_id, region]
```

A key naming no dataset field fails the load with `unknown_merge_key_field`
before anything is read into the destination — including the case where a
JSONL batch simply never carries the field. Declaring keys is not optional:
`load_mode: merge` without the block fails with `missing_merge_keys`, and an
empty or duplicated key list — or a `merge` block on a non-merge load —
fails with `invalid_merge_config`
([ADR-0009](../adr/0009-require-resolved-merge-keys.md) is why the keys are
always yours to declare: nothing is inferred).

## A null key is a rejected record

A record with null in any key field cannot be matched — a null never equals
anything — so it is a rejected record with code `null_merge_key`, written to
`rejected-records.jsonl` and counted against
[`reject_threshold`](../reference/load-definition.md#reject_threshold) like
any other rejection:

```json
{"line":3,"code":"null_merge_key","field":"customer_id","source_field":null,"message":"merge key \"customer_id\" is null","record":{"customer_id":null,"name":"Katherine"}}
```

A CSV empty cell, a JSONL `null`, and a JSONL absent field are all the
pipeline's existing null — no new null definition. At the default threshold
of `0`, one null-key record fails the whole load with
`reject_threshold_exceeded` before the destination is touched; raise the
threshold to let the survivors merge while the rejects land in the artifact.

## Duplicate source keys fail the load

Two surviving records with the same key tuple would make "update the match"
ambiguous, so the load fails with `duplicate_merge_keys` rather than letting
source order silently pick a winner
([ADR-0058](../adr/0058-fail-merge-loads-on-duplicate-merge-keys.md)):

```json
"error_summary": {
  "code": "duplicate_merge_keys",
  "message": "2 surviving records share a merge key tuple with another surviving record for DuckDB table customers in crm.duckdb"
}
```

The check runs inside the merge transaction, before the real table is
touched: the rollback leaves the destination byte-for-byte as it was. If
your source legitimately carries several versions of a record, deduplicate
upstream — the load will not choose for you.

## Duplicate destination keys all match

A merge key is a matching rule, not a uniqueness constraint on the
destination. If the destination already contains several records with the
same key tuple, one surviving source record matches every one of them.
Every matching destination record is replaced whole; there is no
destination-side duplicate-key gate. The currently shipped DuckDB
destination follows this contract.

The report counts this from the source perspective: `updated` is the number
of surviving source records that matched at least one destination record, not
the number of destination records replaced. One source record can therefore
replace two destination records while reporting `updated: 1`. The source
partition still holds — `updated + inserted` equals `row_counts.written` —
even when the number of replaced destination records is greater than
`updated`.

This does not relax the source-side rule above. Two surviving source records
with the same key tuple still fail with `duplicate_merge_keys`, so no source
order chooses which values replace the matching destination records.

## Merge is DuckDB-only today

The `parquet` destination declines the mode with
`unsupported_load_mode_for_destination` before any I/O:

```json
"error_summary": {
  "code": "unsupported_load_mode_for_destination",
  "message": "parquet destination does not support load mode: merge (supported load modes: full_refresh, append)"
}
```

A Parquet dataset directory is immutable part files with no record identity
to update; an unknown mode string (a typo, say) still fails as
`unsupported_load_mode`
([ADR-0057](../adr/0057-land-merge-loads-on-duckdb-only-and-decline-elsewhere.md)).

## What the load report says

| Field | What it tells you |
| --- | --- |
| `merge` | The keys you declared, echoed — `null` on non-merge loads. |
| `destination_write.atomicity` / `.strategy` | `atomic` / `transactional_merge`: one terminal commit. |
| `destination_write.merge.updated` | Surviving records that matched an existing record and replaced it whole. |
| `destination_write.merge.inserted` | Surviving records that matched nothing and inserted. |
| `row_counts.written` | Always `updated + inserted`. |
| `rejected_records` | Null-key records land here as `null_merge_key`. |

Both are documented key by key in the
[Load Report Reference](../reference/load-report.md).

## Where to look next

- [`merge` in the Load Definition Reference](../reference/load-definition.md#merge)
  — the block's full contract, including every failure code.
- [Rejected records](rejected-records.md) — thresholds and the artifact that
  `null_merge_key` rejections land in.
- [Execution tuning](execution-tuning.md) — chunking applies to merge too;
  the staged records accumulate in the transaction and commit once.
