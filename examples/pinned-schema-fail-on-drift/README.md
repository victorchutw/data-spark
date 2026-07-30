# Pinned schema that fails on drift

**This example fails on purpose.** It is the same shape as
[pinned-schema-additive-drift](../pinned-schema-additive-drift/) under the
default drift policy, `fail`: day 1 has `invoice_id`, `customer`, and
`total_due`, day 2 adds `currency`, and that added field is enough to stop the
second load.

```bash
data-spark load load-day-1.yml
data-spark load load-day-2.yml   # exits 1: schema_drift
```

The first load bootstraps `invoices.schema.yml` from its own inference and writes
two records to the `invoices` table in `billing.duckdb`. The second load reads
the pin, compares the observed record shape against it by field name, and finds
drift the policy does not permit, so it fails with `schema_drift` and
`drift_status: failed_on_drift`, naming `currency` in the report's drift detail.
Schema drift is judged before any destination write, so the failure changes
nothing: the table still holds day 1's two records, and the pin is untouched. Fix
it by declaring the field you want (`drift_policy:
allow_additive_nullable`), or by treating the drift as the upstream change it
reports.
