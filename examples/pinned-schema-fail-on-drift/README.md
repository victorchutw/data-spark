# Pinned schema that fails on drift

**This example fails on purpose.** It is
[pinned-schema-additive-drift](../pinned-schema-additive-drift/) under the
default drift policy, `fail`: `load-day-1.yml` reads `invoice_id`, `customer`,
and `total_due`, `load-day-2.yml` reads the same three plus `currency`, and that
one added field is enough to stop the second load.

Run them in order, from a copy of this directory
([why](../README.md#running-them)):

```bash
data-spark load load-day-1.yml
data-spark load load-day-2.yml   # exits 1: schema_drift
```

The first load bootstraps `invoices.schema.yml` from its own inference and writes
two records to the `invoices` table in `billing.duckdb`. The second load reads
the pin, compares the observed record shape against it by field name, and finds
drift the policy does not permit, so it fails with `schema_drift` and
`drift_status: failed_on_drift`, naming `currency` in the report's drift detail.
Drift is judged before any destination write, so the failure changes nothing: the
table still holds day 1's two records, and the pin is untouched.
