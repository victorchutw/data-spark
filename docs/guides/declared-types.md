# Declaring timestamps and decimals

Schema inference reaches four types: `boolean`, `int64`, `float64`, and
`utf8`. Timestamps and exact decimals are deliberately not among them — they
enter a dataset schema only when a load definition declares them
([ADR-0042](../adr/0042-add-field-types-through-declaration-never-inference.md)).
Declaring one is a promise about every value in the field, so the values that
break the promise become rejected records rather than quietly changing shape.

This guide declares all three declared types, checks what the destination
stores, and works through the text each of them accepts and refuses.

## Start from the example

[examples/declared-types](../../examples/declared-types/) loads three payments
whose timestamps and amounts all want declaring. Copy it somewhere writable
and load it ([why](../../examples/README.md#running-them)):

```bash
cp -r examples/declared-types /tmp/
cd /tmp/declared-types
data-spark load load.yml
```

Every command below was run, and every YAML fragment and report excerpt is
taken from a real load against the release binary.

`payments.csv` holds three payments, two timestamps apart:

```text
payment_id,paid_at,recorded_at,amount
4001,2026-07-01 09:30:00,2026-07-01T07:30:00Z,120.00
4002,2026-07-02 14:05:30.250,2026-07-02T12:05:30.250Z,7.25
4003,2026-07-03 08:00:00,2026-07-03T06:00:00+02:00,1450.75
```

Left to inference, all three of those fields would land as `utf8`, `utf8`, and
`float64`. Three overrides say what they actually are:

```yaml
schema:
  overrides:
    - name: paid_at
      type: timestamp
    - name: recorded_at
      type: timestamptz
    - name: amount
      type: decimal(12,2)
```

The report states the schema the load resolved, with each declared type
printed exactly as it was declared:

```json
[
  {
    "name": "payment_id",
    "type": "int64",
    "nullable": true
  },
  {
    "name": "paid_at",
    "type": "timestamp",
    "nullable": true
  },
  {
    "name": "recorded_at",
    "type": "timestamptz",
    "nullable": true
  },
  {
    "name": "amount",
    "type": "decimal(12,2)",
    "nullable": true
  }
]
```

The `payments` table in `payments.duckdb` gets DuckDB's matching column types
— `paid_at` as `TIMESTAMP`, `recorded_at` as `TIMESTAMP WITH TIME ZONE`, and
`amount` as `DECIMAL(12,2)`.

## Why inference stops at four types

Guessing a timestamp format from data means guessing between `03/04/2026` as
March and as April, and guessing a decimal's scale means picking a rounding
rule nobody wrote down. A type that depends on which values a delivery
happened to contain is also a type that changes between loads. So declaration
is the only way in, and the load definition stays the declaration of record.

Two inference guards work in the same direction, and both are common reasons
to reach for a declaration:

- Numeric-looking text that does not parse to a finite number — `NaN`, `inf`,
  or a magnitude like `1e400` — keeps a whole field as `utf8`
  ([ADR-0031](../adr/0031-infer-non-finite-numeric-text-as-text.md)), because
  non-finite values poison BI aggregates.
- Zero-padded numeric text such as `007` keeps its field `utf8`
  ([ADR-0032](../adr/0032-infer-zero-padded-numeric-text-as-text.md)),
  because reading it as a number destroys the padding on the first load —
  zip codes and account numbers are text that happens to look numeric.

Both are inference declining a reading that would lose information. When you
know better, override the field to the type you want, and the few values that
made inference widen become rejected records — see the
[rejected records guide](rejected-records.md).

## Wall-clock or instant?

The two timestamp types differ in one question: does this stamp identify a
moment in time, or only what a clock read
([ADR-0043](../adr/0043-split-timestamps-into-wall-clock-and-utc-instant-types.md))?

| Type | Meaning | Offset in the text |
| --- | --- | --- |
| `timestamp` | A wall-clock timestamp: what a clock read, with no timezone and no absolute moment. | Must not carry one. |
| `timestamptz` | An instant timestamp: one absolute moment, normalized to UTC. | Must end in `Z`/`z` or `±hh:mm`. |

A store's opening time is a wall-clock timestamp; a payment's settlement is
an instant. Choosing wrongly is not a formatting detail — a reader of the
dataset cannot tell afterwards whether `09:00` meant a local reading or a UTC
moment.

Both types accept one strict menu and store microseconds: a four-digit year,
zero-padded fixed-width fields, `YYYY-MM-DD`, one space or `T`/`t`
separator, `HH:MM:SS`, and an optional fraction of 1 to 6 digits. Nothing
else — no month names, no D/M/Y order, no date-only text, no epoch numbers.

Every `timestamptz` value is normalized when it is parsed, so different
spellings of one moment store the same value. Querying the example's table
with the session timezone set to UTC shows it: `2026-07-03T06:00:00+02:00`
went in and the instant `04:00` came back, while `paid_at` reads back exactly
as it was written.

```sql
SET TimeZone = 'UTC';
SELECT payment_id, paid_at, recorded_at, amount
FROM payments
ORDER BY payment_id;
```

| `payment_id` | `paid_at` | `recorded_at` | `amount` |
| --- | --- | --- | --- |
| 4001 | 2026-07-01 09:30:00 | 2026-07-01 07:30:00+00 | 120.00 |
| 4002 | 2026-07-02 14:05:30.25 | 2026-07-02 12:05:30.25+00 | 7.25 |
| 4003 | 2026-07-03 08:00:00 | 2026-07-03 04:00:00+00 | 1450.75 |

Two things to read out of those values. An instant renders in whatever
timezone the client is set to, so set it explicitly when comparing text; and
`14:05:30.250` came back as `14:05:30.25` because the storage is microseconds,
where a trailing zero carries no information.

## Decimals are exact, and never rounded

`decimal(p,s)` declares an exact numeric with `p` total digits and `s`
fractional ones, `1 <= p <= 38` and `0 <= s <= p`
([ADR-0044](../adr/0044-parse-declared-decimals-strictly-and-never-round.md)).
Only the canonical spelling is accepted in a declaration — both parameters,
no spaces — so the type prints byte-identically in reports and pinned schema
files. A malformed declaration such as `decimal(2,5)` fails the load before
any data is read, never per record.

Accepted value text is an optional sign, plain base-10 digits, and at most
one decimal point with at least one digit on each side. Fewer fractional
digits than the scale rescale losslessly — `1.2` into `decimal(6,2)` stores
`1.20`. Everything else rejects the record, and the message says which rule
it broke:

```text
value "1450.755" does not fit overridden type decimal(6,2) for field "amount": the value has 3 fractional digits, more than scale 2 allows; values are never rounded
value "99999.99" does not fit overridden type decimal(6,2) for field "amount": the value overflows decimal(6,2): the integer part allows at most 4 digits
value "1e3" does not fit overridden type decimal(6,2) for field "amount": exponent notation is not accepted as decimal text
value ".5" does not fit overridden type decimal(6,2) for field "amount": the text is not plain decimal digits with an optional sign and decimal point
```

Rounding never happens in a load: an amount with more fractional digits than
the scale is a question about your data, and a load is not the place to answer
it silently.

In JSONL, strings and integers fit a decimal field — `"12.34"` and `12` both
load, the integer rescaling to `12.00` — while a JSON float does not:

```text
value 12.34 does not fit overridden type decimal(6,2) for field "amount": JSON floats do not fit a declared decimal field: their exact digits were already lost to IEEE parsing
```

If a JSONL source carries exact amounts, have the producer quote them.

## When a timestamp value does not fit

The same per-record discipline applies, with the cause named. These three are
the mistakes worth recognizing on sight — an offset where a wall-clock field
forbids one, date-only text, and an epoch number:

```text
value "2026-07-01T09:30:00Z" does not fit overridden type timestamp for field "paid_at": the text carries a UTC offset, which wall-clock timestamp text must not
value "2026-07-02" does not fit overridden type timestamptz for field "recorded_at": date-only text has no time part
value "1785311868" does not fit overridden type timestamptz for field "recorded_at": epoch numbers are not accepted as timestamp text
```

More than six fractional digits rejects too, rather than truncating. Each
rejected record counts against `reject_threshold`, which defaults to `0`, so
a declaration meeting one bad value fails the load until you say otherwise.

## Declared types and pinned schemas

A pinned schema carries declared types verbatim, so the pin a declared load
bootstraps reads back as the declaration did:

```yaml
version: 1
fields:
- name: payment_id
  type: int64
  nullable: true
- name: paid_at
  type: timestamp
  nullable: true
- name: recorded_at
  type: timestamptz
  nullable: true
- name: amount
  type: decimal(12,2)
  nullable: true
```

Because no inference can ever produce a declared type, **the pin does not
relieve you of the override**. Drop the `overrides` block from a definition
whose pin holds declared types and the next load fails, naming every field
whose declaration went missing:

```text
Error: schema drift against pinned schema payments.schema.yml: field "paid_at" is pinned as timestamp but its effective type is utf8; field "recorded_at" is pinned as timestamptz but its effective type is utf8; field "amount" is pinned as decimal(12,2) but its effective type is float64; declared types take effect only through schema.overrides — the override may be missing
```

The report says it in a shape a program can read
([reference](../reference/load-report.md#drift-detail)):

```json
{
  "undeclared_fields": [
    {
      "name": "paid_at",
      "pinned_type": "timestamp",
      "effective_type": "utf8"
    },
    {
      "name": "recorded_at",
      "pinned_type": "timestamptz",
      "effective_type": "utf8"
    },
    {
      "name": "amount",
      "pinned_type": "decimal(12,2)",
      "effective_type": "float64"
    }
  ]
}
```

The fix is to put the overrides back. Keep the declaration and the pin
agreeing, too: declared types compare as exactly equal or different, so
`decimal(10,2)` against a pinned `decimal(12,2)` is an override conflict, not
widenable drift — the [schema pinning guide](schema-pinning.md) shows that
failure.

## What the load report says

| Field | What to read it for |
| --- | --- |
| [`schema_decision.fields`](../reference/load-report.md#schema_decision) | The resolved types, each declared type printed as declared. |
| [`schema_decision.overrides`](../reference/load-report.md#transform-and-overrides-echoes) | The overrides echoed as the definition wrote them. |
| [`schema_decision.drift.undeclared_fields`](../reference/load-report.md#drift-detail) | Declared pinned fields whose override is missing. |
| [`rejected_records`](../reference/load-report.md#rejected_records) | Where the values that did not parse went. |

## Where to look next

- [Load Definition Reference: field type vocabulary](../reference/load-definition.md#field-type-vocabulary)
  — every type, and the full accept-and-reject rules for timestamps and
  decimals.
- [Load Definition Reference: `schema.overrides`](../reference/load-definition.md#schemaoverrides)
  — the key that declares them.
- [Rejected records](rejected-records.md) — the artifact the rejected values
  land in.
- [Schema pinning](schema-pinning.md) — keeping a declared schema stable
  across loads.
