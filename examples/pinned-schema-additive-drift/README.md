# Pinned schema with additive drift allowed

A pinned schema keeps a BI-ready dataset stable across loads, and
`drift_policy: allow_additive_nullable` decides what happens when the source
grows a field. Day 1 has `shipment_id`, `city`, and `weight_kg`; day 2 adds
`carrier`. Run them in order:

```bash
data-spark load load-day-1.yml
data-spark load load-day-2.yml
```

The pinned schema file works like a lockfile bootstrapped by the first load:
`shipments.schema.yml` does not exist yet, so the first load persists the schema
it inferred and reports the decision as `inferred`. The second load reads that
pin, finds one added nullable field, accepts it because the policy permits
additive schema drift, extends the pin to carry `carrier`, and reports
`drift_status: additive_fields_added` with `carrier` in `added_fields`. Every
other kind of drift — a pinned field the source lost, a type that does not widen
— still fails the load; see
[pinned-schema-fail-on-drift](../pinned-schema-fail-on-drift/) for the stricter
default policy. Both loads full-refresh the dataset, so afterwards
`shipments-dataset` holds day 2's two records under the widened schema.
