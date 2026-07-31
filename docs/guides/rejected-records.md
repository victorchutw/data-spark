# Tolerating rejected records

A rejected record is a source record that cannot be written without violating
the dataset schema or the load rules. Pure inference never produces one — a
schema built from whatever the records happen to hold cannot be violated by
them. Rejections start once you take a position: a pinned schema, a per-field
override, a declared type, or a source line that does not parse at all.

The reject threshold is how you say how many of those a load may tolerate
before it fails. This guide sets one, reads the resulting artifact, and shows
what changes when the threshold is too low — and what happens when nothing in
the source survives.

## Start from the example

[examples/rejected-records](../../examples/rejected-records/) loads four
measurements, two of which cannot be stored. Copy it somewhere writable and
load it ([why](../../examples/README.md#running-them)):

```bash
cp -r examples/rejected-records /tmp/
cd /tmp/rejected-records
data-spark load load.yml
```

Every command below was run, and every YAML fragment and report excerpt is
taken from a real load against the release binary.

`measurements.csv` is where the trouble is — one reading spelled `n/a`, one
record with no station:

```text
station,reading,taken_on
north,12.5,2026-07-01
north,n/a,2026-07-01
,18.25,2026-07-02
south,7.0,2026-07-02
```

The definition takes two positions and then says how much it tolerates:

```yaml
schema:
  overrides:
    - name: station
      nullable: false
    - name: reading
      type: float64
reject_threshold: 2
```

Without the overrides this loads clean: inference would read `reading` as
`utf8` because `n/a` is one of its values, and a null station would be an
ordinary null. Narrowing `reading` to its true type and declaring `station`
required is exactly what makes two records unloadable — and narrowing on
purpose, then rejecting the few values that made inference widen, is what
overrides are for
([ADR-0038](../adr/0038-apply-per-field-overrides-to-inferred-schemas-never-the-pin.md)).

The load exits `0`, because two rejections is *at* the threshold, not above
it, and the load summary states both counts and where the rejected records
went:

```text
Records read: 4
Records written: 2
Records rejected: 2
```

## What makes a record rejected

| Code | Raised when |
| --- | --- |
| `malformed_csv_record` | A CSV record does not parse as a record of the header's fields. |
| `malformed_jsonl_record` | A JSONL line is not a JSON object. |
| `type_coercion_failed` | A value does not fit the pinned or overridden type of its field — including a declared timestamp or decimal that does not parse. |
| `missing_required_field` | A field declared `nullable: false` is null, or absent, in this record. |
| `null_merge_key` | A merge key field is null, or absent, in this record — merge keys are implicitly non-null, because a null never equals anything under key equality (see the [merge loads guide](merge-loads.md)). |

Judgement is per record and the first violation wins, so one record yields at
most one rejection even when two of its fields are unloadable — a null in a
field that is both a merge key and schema-required names the merge-key rule. These codes
live in the artifact, not in the report's `error_summary` — a load that
rejects records within its threshold has no error at all.

## Reading the rejected-records artifact

Rejected records stream to `rejected-records.jsonl` in the load's artifact
directory while the source is read, one JSON object per rejected record
([ADR-0036](../adr/0036-write-rejected-records-jsonl-under-a-flat-reject-threshold.md)).
The example produces exactly two lines:

```json
{"line":3,"code":"type_coercion_failed","field":"reading","source_field":null,"message":"value \"n/a\" does not fit overridden type float64 for field \"reading\"","record":{"station":"north","reading":"n/a","taken_on":"2026-07-01"}}
{"line":4,"code":"missing_required_field","field":"station","source_field":null,"message":"required field \"station\" is null","record":{"station":null,"reading":"18.25","taken_on":"2026-07-02"}}
```

| Key | What it gives you |
| --- | --- |
| `line` | The source line the record came from — the CSV header is line 1, so these two are the third and fourth lines of the file. |
| `code` | One of the codes above. Branch on this. |
| `field` | The dataset field at fault, or `null` where no single field is (a malformed record). |
| `source_field` | The source name of that field when a rename mapping changed it, `null` otherwise. |
| `message` | Why the value did not fit, in words. Wording is not a contract. |
| `record` | The record content the load could recover. |

`record` holds whatever survived parsing, which is not always an object: a
malformed CSV record carries the fields it did find as a list
(`"record":["4","1","234.00"]` for a record with one field too many), and a
truncated JSON line carries the raw text.

No records rejected means no artifact — the report's
`rejected_records.artifact` is `null` and nothing is written. The artifact is
written for failing and completing loads alike, so it is there when you most
want it.

## Setting the threshold

`reject_threshold` is a top-level count and defaults to `0`
([ADR-0020](../adr/0020-default-reject-threshold-zero.md)), so out of the box
a single unloadable record fails the load. That default is deliberate: a
dataset that quietly lost records is worse than a load that stopped.

```yaml
reject_threshold: 2
```

It is a flat count of records, not a ratio, and it is judged over the whole
input: the load reads the entire source and counts every rejection before it
writes anything
([ADR-0045](../adr/0045-resolve-loads-in-two-streaming-source-passes.md)).

- **At or below the threshold**, the load completes under its configured
  rules and writes only the surviving records.
- **Above it**, the load fails with `reject_threshold_exceeded` — before the
  pinned schema is persisted and before the destination is touched.

Set the number to what you are willing to lose from one load, not to a rate
you expect over time. `0` for a dataset that has to be complete; a small
number for a source with known-dirty records you intend to inspect in the
artifact; a large number only while exploring.

To see the failing side, change the threshold in the example's `load.yml` to
`0` and load it again:

```bash
data-spark load load.yml   # exits 1
```

```text
Error: rejected 2 of 4 records, exceeding the reject threshold of 0
```

Same two rejections, same artifact — and nothing else. `row_counts` reports
`source: 4, written: 0, rejected: 2`, `destination_write.atomicity` is
`not_applicable`, `execution.record_format` is `not_started`, and
`measurements-dataset` was never created. The threshold is a gate in front of
the write phase, not a limit discovered part way through it.

## When nothing in the source survives

Two behaviors are worth knowing before a bad delivery day surprises you.

**The threshold gate still comes first.** A JSONL file whose every line is
unparseable, loaded with the default threshold, fails as an ordinary breach
and says how complete the damage is:

```text
Error: rejected 2 of 2 records, exceeding the reject threshold of 0
```

**Raise the threshold above the whole file and what decides is whether the
source still offers a shape.** JSONL carries no header, so a file with no
parseable record offers no field names to infer a schema from, and the load
fails for that reason instead:

```text
Error: JSONL source bad.jsonl must include at least one record with fields
```

That is `malformed_jsonl`, with `schema_decision.mode: not_evaluated` — and
the artifact still holds every rejected line.

Every other all-rejected source still offers a shape, and this is the trap: a
CSV header carries the field names whatever the records do, and JSONL records
that parse but reject carry their own. Such a load, under a threshold that
tolerates every rejection, **succeeds**, materializing the schema with no
records in it — which for `full_refresh` means replacing the destination
dataset with an empty one. Keep `reject_threshold` at `0`, or comfortably
below a whole delivery, and a dataset cannot be emptied *by rejection* — the
default is exactly this guard
([ADR-0020](../adr/0020-default-reject-threshold-zero.md)).

The threshold promises no more than that, because rejection is not the only
route to an empty full refresh. A delivery that is empty to begin with — a
header-only CSV — rejects nothing, so it completes at any threshold,
including `0`, and empties the dataset the same way. That is not a gap in the
gate but the mode's meaning
([ADR-0056](../adr/0056-mirror-the-source-on-zero-survivor-full-refreshes.md)):
a full refresh replaces the destination dataset with the source's current
records, and a source whose current records are zero is faithfully mirrored
by an empty dataset. The load cannot tell that case from a delivery that went
missing upstream — so when an empty delivery is abnormal for your pipeline,
guard it upstream, before the load runs.

## Rejections are not drift

A source whose *shape* changed against a pinned schema is schema drift, and
drift is decided before per-record validation runs, so a drift-failed load
reports drift rather than a pile of rejections
([ADR-0035](../adr/0035-reject-pinned-value-misfits-per-record.md)). The
reverse is the case the [schema pinning guide](schema-pinning.md) ends on: a
shape that still matches, holding one value that does not fit, is a rejected
record and nothing more.

## What the load report says

| Field | What to read it for |
| --- | --- |
| [`row_counts`](../reference/load-report.md#row_counts) | `source`, `written`, `rejected`. On a completing load, `written` is `source` minus `rejected`. |
| [`rejected_records.count`](../reference/load-report.md#rejected_records) | The same rejection count, beside the artifact. |
| [`rejected_records.artifact`](../reference/load-report.md#rejected_records) | Where the rejected records are, or `null`. |
| [`error_summary.code`](../reference/load-report.md#error_summary) | `reject_threshold_exceeded` when the count broke the threshold. |

A threshold breach reports the same two `rejected_records` fields as a
completing load: what it had rejected when it stopped, and the file it wrote
them to.

## Where to look next

- [Load Definition Reference: `reject_threshold`](../reference/load-definition.md#reject_threshold)
  — the key, its default, and what interacts with it.
- [Load Report Reference: `rejected_records`](../reference/load-report.md#rejected_records)
  — the report contract around rejections.
- [Declared types](declared-types.md) — the parse rules behind most
  `type_coercion_failed` messages.
- [Schema pinning](schema-pinning.md) — where per-record positions come from
  in the first place.
