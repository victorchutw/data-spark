# Pinning a schema and choosing a drift policy

A load infers its schema from the records it observes, which is what makes a
first load a one-liner ([ADR-0006](../adr/0006-infer-schemas-by-default.md)).
A BI-ready dataset wants the opposite: the same fields, the same types, in the
same order, load after load. A pinned schema is how you get that, and the
drift policy decides what happens when the source stops agreeing with it.

This guide works through both from a runnable example, and ends at the report
fields that tell you which of them happened.

## Start from the example

[examples/pinned-schema-additive-drift](../../examples/pinned-schema-additive-drift/)
loads two days of shipments, the second day carrying one field the first did
not. Copy it somewhere writable and load day 1 — a load writes into the
directory it runs from ([why](../../examples/README.md#running-them)):

```bash
cp -r examples/pinned-schema-additive-drift /tmp/
cd /tmp/pinned-schema-additive-drift
data-spark load load-day-1.yml
```

Pinning is two keys in the load definition:

```yaml
schema:
  pinned_path: shipments.schema.yml
  drift_policy: allow_additive_nullable
```

## The first load writes the pin

`shipments.schema.yml` does not exist yet, so the load persists the schema it
resolved and reports the decision as `inferred` — it is this load's inference
that became the pin
([ADR-0033](../adr/0033-persist-pinned-schemas-as-versioned-yaml-files.md)):

```yaml
version: 1
fields:
- name: shipment_id
  type: int64
  nullable: true
- name: city
  type: utf8
  nullable: true
- name: weight_kg
  type: float64
  nullable: true
```

Two report keys say the bootstrap happened:

```json
{
  "drift_status": "not_applicable",
  "pinned_schema_path": "shipments.schema.yml",
  "pinned_schema_persisted": true
}
```

The pin is written after the reject threshold is judged and before the first
destination write, so a load that fails never leaves a pin behind for records
it refused. Treat the file the way you treat a lockfile: the tool maintains
it, you commit it next to the load definition, and hand edits stay possible
because every load validates against the file rather than trusting what a
previous load did.

The pin is not a load artifact — it lives at `schema.pinned_path`, not in the
artifact directory, precisely because it is reused across loads.

## Later loads compare against it

With the file in place, a load reads it and matches observed records to
pinned fields **by name, never by position**
([ADR-0034](../adr/0034-validate-pinned-schemas-by-name-with-lattice-widening.md)).
A field's observed type is accepted when it widens to the pinned type under
the same lattice inference uses — an all-null field fits any pinned type,
`int64` widens to `float64`, anything widens to `utf8` — while declared types
are exactly equal or different, never widened. Matching loads then
materialize records in the pin's field order, so a source that reorders its
fields cannot reorder the destination dataset.

Three things count as schema drift, and all three are judged before the
destination is touched:

- a pinned field that **no observed record carries** — including in JSONL,
  where a field missing from one record merely reads as null, so a silently
  renamed source field is caught instead of quietly becoming an all-null
  field;
- an **added field** the drift policy does not permit;
- **duplicate dataset field names**, which cannot be matched to the pin at
  all.

A field the source added *outside* a `transform.select` list is none of
these: selection makes it invisible, so it yields no drift under any policy.

## Choosing the drift policy

| `drift_policy` | What it does |
| --- | --- |
| `fail` (default) | Any schema drift fails the load with `schema_drift`. |
| `allow_additive_nullable` | New nullable fields beyond the pin are accepted and the pin is rewritten to carry them. Every other drift still fails. |

Failing is the default because a BI-ready dataset changing shape silently is
worse than a load stopping ([ADR-0007](../adr/0007-fail-fast-on-schema-drift.md)).
Choose `allow_additive_nullable` when the source is expected to grow columns
you want the destination dataset to grow with — and keep in mind that it
admits *additions* only.

Now load day 2, whose source gained a `carrier` field:

```bash
data-spark load load-day-2.yml
```

The policy admits the added field, and the report names what the load added
on top of the whole resolved schema:

```json
{
  "drift_status": "additive_fields_added",
  "added_fields": [
    {
      "name": "carrier",
      "type": "utf8",
      "nullable": true
    }
  ],
  "pinned_schema_path": "shipments.schema.yml",
  "pinned_schema_persisted": true
}
```

`shipments.schema.yml` now carries `carrier` too. The rewrite is the point:
if `carrier` disappears from the source again, the next load sees a pinned
field no record carries and stops, instead of silently matching the older
three-field pin.

## When drift fails the load

[examples/pinned-schema-fail-on-drift](../../examples/pinned-schema-fail-on-drift/)
is the same story under the default policy — `drift_policy: fail` — and its
second load is meant to fail:

```bash
cp -r examples/pinned-schema-fail-on-drift /tmp/
cd /tmp/pinned-schema-fail-on-drift
data-spark load load-day-1.yml
data-spark load load-day-2.yml   # exits 1
```

```text
Error: schema drift against pinned schema invoices.schema.yml: added fields: currency
```

The report says the same thing in fields a program can branch on — the drift
detail names what moved:

```json
{
  "drift_status": "failed_on_drift",
  "drift": {
    "missing_fields": [],
    "added_fields": [
      "currency"
    ]
  },
  "pinned_schema_path": "invoices.schema.yml"
}
```

Nothing happened to the destination dataset: the table still holds day 1's
records, the pin is unchanged, `row_counts` are all `0`, and
`destination_write.atomicity` is `not_applicable`, because drift is decided
before any write is attempted
([ADR-0019](../adr/0019-validate-schema-and-load-rules-before-writing.md)).

From there you have three deliberate moves:

- **Fix the source** if the change was a mistake, and load again.
- **Accept the new shape.** Switch to `allow_additive_nullable` if the drift
  is additive, or hand-edit the pin (it is a plain YAML contract), or delete
  it and let the next load bootstrap a new one — which redefines the dataset,
  so pair it with a `full_refresh` unless you want two shapes in one dataset.
- **Ignore the new field** with a `transform.select` list that does not name
  it.

There is no pin migration. A pin recorded before you configured a
`transform` compares against the transformed shape and fails as drift — the
transformed names are the dataset's names
([ADR-0040](../adr/0040-apply-structural-transforms-before-schema-pinning.md)).

## Shape drift is not the same as a value that does not fit

The pin governs the dataset's shape; whether an individual value can be
stored as its pinned type is judged per record
([ADR-0035](../adr/0035-reject-pinned-value-misfits-per-record.md)). Take a
pin holding `account_id` as `int64` and a later source that spells one
account as `007`. The shape still matches — the report says `drift_status:
none` — but that value does not fit, so the record becomes a rejected record:

```json
{"line":2,"code":"type_coercion_failed","field":"account_id","source_field":null,"message":"value \"007\" does not fit pinned type int64 for field \"account_id\"","record":{"account_id":"007","balance":"3.00"}}
```

Whether that fails the load is then the reject threshold's call, which
defaults to `0` — see the [rejected records guide](rejected-records.md).

## Overrides work with the pin, never on it

Per-field overrides correct **inference**, so they apply exactly where
inference decides a field's shape: a load with no pin, the load that
bootstraps one, and the added fields an additive policy admits
([ADR-0038](../adr/0038-apply-per-field-overrides-to-inferred-schemas-never-the-pin.md)).
The overridden schema is what gets materialized, persisted as the pin, and
compared later — so you declare a type once and the pin carries it from then
on.

A field the pin already governs takes nothing from an override, but the
override must **agree** with the pinned field. A contradiction is a broken
definition rather than drift, and the load says which two statements
disagree:

```text
Error: schema override for field "amount" contradicts pinned schema payments.schema.yml: pinned type decimal(12,2), override type decimal(10,2)
```

```json
{
  "field": "amount",
  "pinned": {
    "type": "decimal(12,2)",
    "nullable": true
  },
  "override": {
    "type": "decimal(10,2)"
  }
}
```

That is `schema_override_conflict`, with `drift_status: not_applicable`: a
definition and a hand-edited pin cannot drift apart unnoticed.

One consequence deserves its own line, because it surprises people: a pinned
`timestamp`, `timestamptz`, or `decimal(p,s)` field needs its override
re-declared in **every** load, since no inference can ever produce a declared
type. Omit it and the load fails as `schema_drift` naming the pinned and
effective types — the [declared types guide](declared-types.md) shows the
failure and the fix.

## What the load report says

Everything above lands in one object, `schema_decision`
([reference](../reference/load-report.md#schema_decision)):

| Key | What to read it for |
| --- | --- |
| `mode` | `inferred` on a pin bootstrap, `pinned` once the file governs the load. |
| `drift_status` | `not_applicable`, `none`, `additive_fields_added`, or `failed_on_drift`. |
| `fields` | The dataset schema in the order records materialize. |
| `pinned_schema_path` | The pin this load read or wrote. |
| `pinned_schema_persisted` | Present, and `true`, only when this load wrote that file. |
| `added_fields` | What an additive policy just added to the pin. |
| `drift` | On a failure, what drifted. |
| `conflict` | On an override conflict, which two statements disagree. |

A `failed_on_drift` load is worth alerting on; `additive_fields_added` is
worth noticing, because the pin in your repository just changed.

## Where to look next

- [Load Definition Reference: `schema`](../reference/load-definition.md#schema)
  — every key, its defaults, and its failure codes.
- [Load Report Reference: `schema_decision`](../reference/load-report.md#schema_decision)
  — every posture the decision can take, with complete reports.
- [Declared types](declared-types.md) — the types that only ever enter a
  schema through an override.
- [Rejected records](rejected-records.md) — what happens to the values a pin
  refuses.
