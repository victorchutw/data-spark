# CSV to DuckDB, merge

Two days of customers land in the `customers` table of `crm.duckdb` as a keyed
upsert: day 2 updates the record whose `customer_id` it shares and inserts the
one it does not, leaving every other record in place. `load-day-1.yml` uses
`full_refresh` because a merge writes into a table that already exists: the
first load is what creates it. `load-day-2.yml` switches to `load_mode: merge`
and declares the key:

```yaml
merge:
  keys: [customer_id]
```

Run them in order, from a copy of this directory
([why](../README.md#running-them)):

```bash
data-spark load load-day-1.yml
data-spark load load-day-2.yml
```

The table ends up holding four records. Customer 2 was replaced whole — day 2
carries `tier: gold`, so the update moves both `name` and `tier` — customer 4
is new, and customers 1 and 3, absent from day 2, stay untouched: merge never
deletes. A merge commits once, terminally — an `atomic` write with the
`transactional_merge` strategy — and the report counts the partition under
`destination_write.merge`: `{"updated": 1, "inserted": 1}`, which always sums
to `row_counts.written`.
