# Structural transform: flatten, select, rename

Nested source records rarely have the shape a BI-ready dataset wants. The
`transform` block in `load.yml` reshapes them with all three structural
transforms, in the order the contract fixes: the flatten mapping extracts the
values at the source paths `customer.id` and `customer.name` into two added
dataset fields; field selection then keeps five fields — dropping the raw
`customer` object and `channel` — and its order becomes the dataset field order;
the rename mapping finally maps `placed_at` to `placed_on`.

Run it from a copy of this directory ([why](../README.md#running-them)):

```bash
data-spark load load.yml
```

The `orders` table in `analytics.duckdb` ends up with `order_id`, `customer_id`,
`customer_name`, `amount`, and `placed_on`. Transforms apply before everything
that consumes a schema, so pinned schemas, schema overrides, drift comparison,
and rejected records all speak these dataset names.
