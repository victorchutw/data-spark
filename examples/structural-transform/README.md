# Structural transform: flatten, select, rename

Nested source records rarely have the shape a BI-ready dataset wants. The
`transform` block in `load.yml` reshapes them with all three structural
transforms, applied in the order the contract fixes — flatten mapping, then
field selection, then rename mapping:

```bash
data-spark load load.yml
```

The flatten mapping extracts the values at the source paths `customer.id` and
`customer.name` into two added dataset fields; it is purely additive, so the
`customer` object still materializes as JSON text until something removes it.
Field selection is that something: it keeps the five named fields — dropping the
raw `customer` object and the `channel` field — and its order becomes the
dataset field order. The rename mapping then maps `placed_at` to `placed_on`.
The `orders` table in `analytics.duckdb` ends up with `order_id`,
`customer_id`, `customer_name`, `amount`, and `placed_on`. Transforms run before
everything that consumes a schema, so pinned schemas, overrides, drift
comparison, and rejected records all speak these dataset names — and a source
field left out of `select` is invisible to drift.
