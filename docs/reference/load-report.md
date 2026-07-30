# Load Report Reference — Version 1

A load report is the machine-readable record of one load's outcome,
diagnostics, and measurements
([ADR-0014](../adr/0014-json-load-reports.md)). It is a versioned contract:
every report declares `report_version: 1`
([ADR-0026](../adr/0026-version-load-definitions-and-reports.md)), so an
external orchestrator can react to a load without scraping console text.

A load writes exactly one report — `load-report.json` in the load's artifact
directory — whether it succeeded or failed. The one exception is a failure to
produce the load artifacts themselves: an artifact directory that cannot be
created, or an artifact write that fails, aborts the load with exit code `1`
and no report.

stdout carries the human-readable load summary instead. It is for people;
everything below is the contract for programs.

The YAML side of the contract — the load definition a load runs from — is
documented in the [Load Definition Reference](load-definition.md).

## Conventions in this reference

- Every JSON block below is real output, taken unedited from a load against
  the release binary. The one exception is the retry attempt entry under
  [`retry`](#retry), which no shipped connector can produce; it is marked
  where it appears.
- Complete reports appear under [Complete examples](#complete-examples).
  Blocks elsewhere show one subtree and name the field they show.
- `load_id`, `artifact_dir`, and the three `timings` values are different in
  every load, and the one value that embeds them — the `rejected_records`
  artifact path — is shown with `<load_id>` standing in for the id. Every
  other value shown is exactly what the binary wrote.
- `binary_version` is different in every release: it names the binary that
  wrote the block it appears in, so it tracks the release the examples were
  captured against rather than the release you run.

## Where the report is written

The report lands in the load's artifact directory
([ADR-0015](../adr/0015-store-load-artifacts-under-data-spark-runs.md)),
which is always `<root>/<load_id>`. The root resolves in this order:

1. `--output-dir <dir>` on the command line, the one-off redirect.
2. `artifacts.dir` in the load definition.
3. `.data-spark/runs`, the default.

The directory holds the artifacts of exactly one load:

```text
.data-spark/runs/<load_id>/
├── load-report.json        # always
└── rejected-records.jsonl  # only when records were rejected
```

It is created before any failure is acted on, so even a load that fails on
its definition has somewhere to put its report. A definition that could not
be parsed cannot contribute its `artifacts.dir`, so such a load falls back to
`--output-dir` or the default root.

The pinned schema file is not a load artifact: it lives wherever
`schema.pinned_path` points, because it is reused across loads.

## Top-level shape

Every key below is present in every report. `null` is used where a value is
genuinely unavailable, so readers never have to distinguish an absent key
from an empty one at the top level.

| Field | Type | Meaning |
| --- | --- | --- |
| `report_version` | integer | The load report version. Always `1`. |
| `binary_version` | string | The version of the binary that wrote the report. |
| `load_id` | string | The load's unique id. |
| `artifact_dir` | string | The artifact directory this report was written into. |
| `source_summary` | object | The definition's `source` block, echoed. |
| `destination_summary` | object | The definition's `destination` block, echoed. |
| `dataset` | string or null | The dataset name, echoed. |
| `load_mode` | string | The load mode, echoed. |
| `schema_decision` | object | What schema the load resolved, and what drift it found. |
| `row_counts` | object | Records read, written, and rejected. |
| `byte_counts` | object | Source and destination bytes. |
| `rejected_records` | object | Rejected-record count and artifact. |
| `destination_write` | object | The write atomicity the destination provided. |
| `execution` | object | How the load ran: chunks, parallelism, retry. |
| `timings` | object | Start, finish, and duration. |
| `exit_status` | string | `succeeded` or `failed`. |
| `process_exit_code` | integer | `0` or `1`; the process's exit code. |
| `error_summary` | object or null | The failure that ended the load; `null` on success. |

## Identity and location

### `report_version`

The load report version — the contract version that decides how the rest of
the document is interpreted
([ADR-0026](../adr/0026-version-load-definitions-and-reports.md)). The only
value this binary writes is `1`. A reader should check it first and refuse a
report whose version it does not know.

### `binary_version`

The version of the Data Spark binary that wrote the report — the value
`data-spark --version` prints, baked in at build time
([ADR-0055](../adr/0055-surface-the-binary-version-as-version-output-and-top-level-report-provenance.md)).
It is provenance, not contract: `report_version` says how to read the
report, `binary_version` says which build wrote it, and the two are
versioned independently. An archived report can be attributed to its
release long after the load that wrote it.

### `load_id`

The load's unique id, a UUID generated at load start. It is also the leaf
name of `artifact_dir` and the id printed in the load summary's first line,
so a report can be traced back to its console output and its artifacts.

### `artifact_dir`

The artifact directory, written exactly as the load resolved it — a relative
root stays relative, an absolute root stays absolute; see [Where the report
is written](#where-the-report-is-written). The report itself is always
`<artifact_dir>/load-report.json`.

## Echoed context

The report restates the definition it ran, so a stored report is
self-describing. These fields are echoes: they are what the definition said,
not what the load resolved it to.

### `source_summary` and `destination_summary`

`source_summary` carries the definition's `source` block: `connector`, `path`,
and `format`. `format` is `null` when the definition omitted it — the echo is
the definition as written, so the format resolved from the path extension
does not appear here:

```json
{
  "connector": "local_file",
  "path": "mixed.jsonl",
  "format": null
}
```

`destination_summary` carries the definition's `destination` block:
`connector` and `path`.

A block the definition omitted is echoed as `{}` — so a definition with no
`destination` reports its `source` and an empty `destination_summary`. Both
are `{}` when the load failed before the definition parsed at all: a
definition file that could not be read, or that is not valid YAML for the
contract.

### `dataset`

The dataset name the definition declared, or `null` when it declared none. A
`duckdb` destination requires it (it names the table); a `parquet`
destination does not, and `null` is the normal value there.

### `load_mode`

The load mode as written, or `full_refresh` when the definition omitted it.
This is an echo, not a validation result: a definition naming a mode this
binary does not support still reports that mode, alongside an
`unsupported_load_mode` error summary.

## `schema_decision`

Governing ADRs:
[ADR-0006](../adr/0006-infer-schemas-by-default.md),
[ADR-0007](../adr/0007-fail-fast-on-schema-drift.md),
[ADR-0033](../adr/0033-persist-pinned-schemas-as-versioned-yaml-files.md),
[ADR-0034](../adr/0034-validate-pinned-schemas-by-name-with-lattice-widening.md),
[ADR-0038](../adr/0038-apply-per-field-overrides-to-inferred-schemas-never-the-pin.md),
[ADR-0040](../adr/0040-apply-structural-transforms-before-schema-pinning.md).

What schema the load decided to materialize, how that decision was reached,
and what drift it found on the way.

| Key | Present when | Meaning |
| --- | --- | --- |
| `mode` | always | `inferred`, `pinned`, or `not_evaluated`. |
| `fields` | a schema was resolved, and in every `pinned` posture | The dataset schema in output order. |
| `drift_status` | `mode` is not `not_evaluated` | The drift outcome; see below. |
| `pinned_schema_path` | the definition set `schema.pinned_path`, and `mode` is not `not_evaluated` | The pinned schema file the load read or wrote. |
| `pinned_schema_persisted` | this load wrote that file | Always `true` when present. |
| `added_fields` | `drift_status` is `additive_fields_added` | The fields this load added to the pin. |
| `drift` | `drift_status` is `failed_on_drift` | What the drift was; see [drift detail](#drift-detail). |
| `conflict` | the load failed with `schema_override_conflict` | The override that contradicts the pin. |
| `overrides` | the definition declared `schema.overrides`, and `mode` is not `not_evaluated` | The overrides, echoed as written. |
| `transform` | the definition declared `transform`, and `mode` is not `not_evaluated` | The structural transform, echoed as written. |

Each entry of `fields` — and of `added_fields` — carries `name`, `type`, and
`nullable`, in the order the load materializes them. `type` is the dataset
field type name: `boolean`, `int64`, `float64`, `utf8` — the four inference
can reach — plus the declared types `timestamp`, `timestamptz`, and
`decimal(p,s)`, which enter a schema only through declaration
([ADR-0042](../adr/0042-add-field-types-through-declaration-never-inference.md)).

### `mode`

| Value | Meaning |
| --- | --- |
| `inferred` | The schema came from the observed source records ([ADR-0006](../adr/0006-infer-schemas-by-default.md)). A load that bootstraps a pin is `inferred`: it is this load's inference that becomes the pin. |
| `pinned` | The schema came from an existing pinned schema file ([ADR-0033](../adr/0033-persist-pinned-schemas-as-versioned-yaml-files.md)). |
| `not_evaluated` | No schema was resolved: the load failed before schema resolution, or its source offered no shape to resolve one from — an all-unparseable JSONL file, which has no header to fall back on. |

### `drift_status`

| Value | Meaning |
| --- | --- |
| `not_applicable` | No pin comparison ran: an inferred load, a pin bootstrap, or a failure before the comparison. |
| `none` | The source shape matched the pin exactly. |
| `additive_fields_added` | The source had fields the pin did not, `drift_policy: allow_additive_nullable` permitted them, and the pin was extended to carry them. |
| `failed_on_drift` | The comparison found drift the policy does not permit; the load failed with `schema_drift`. |

### Postures

A plain inferred load — no pinning configured:

```json
{
  "mode": "inferred",
  "fields": [
    {
      "name": "order_id",
      "type": "int64",
      "nullable": true
    },
    {
      "name": "customer",
      "type": "utf8",
      "nullable": true
    },
    {
      "name": "amount",
      "type": "float64",
      "nullable": true
    },
    {
      "name": "placed_at",
      "type": "utf8",
      "nullable": true
    }
  ],
  "drift_status": "not_applicable"
}
```

The first load of a definition that names a `schema.pinned_path` bootstraps
the pin from its own inference, so it reports `mode: inferred` with the two
pin keys:

```json
{
  "mode": "inferred",
  "fields": [
    {
      "name": "order_id",
      "type": "int64",
      "nullable": true
    },
    {
      "name": "customer",
      "type": "utf8",
      "nullable": true
    },
    {
      "name": "amount",
      "type": "float64",
      "nullable": true
    },
    {
      "name": "placed_at",
      "type": "utf8",
      "nullable": true
    }
  ],
  "drift_status": "not_applicable",
  "pinned_schema_path": "pinned-orders.schema.yml",
  "pinned_schema_persisted": true
}
```

The next load of the same definition reads that file and compares against it.
An unchanged source shape drifts by nothing, and the pin is not rewritten —
so `pinned_schema_persisted` is absent:

```json
{
  "mode": "pinned",
  "fields": [
    {
      "name": "order_id",
      "type": "int64",
      "nullable": true
    },
    {
      "name": "customer",
      "type": "utf8",
      "nullable": true
    },
    {
      "name": "amount",
      "type": "float64",
      "nullable": true
    },
    {
      "name": "placed_at",
      "type": "utf8",
      "nullable": true
    }
  ],
  "drift_status": "none",
  "pinned_schema_path": "pinned-orders.schema.yml"
}
```

A source that gained a field, loaded under `drift_policy:
allow_additive_nullable`: `fields` is the whole extended schema, and
`added_fields` names just what this load added. The pin was rewritten to
carry them, so a field that disappears again is caught as drift next time:

```json
{
  "mode": "pinned",
  "fields": [
    {
      "name": "order_id",
      "type": "int64",
      "nullable": true
    },
    {
      "name": "customer",
      "type": "utf8",
      "nullable": true
    },
    {
      "name": "amount",
      "type": "float64",
      "nullable": true
    },
    {
      "name": "placed_at",
      "type": "utf8",
      "nullable": true
    },
    {
      "name": "channel",
      "type": "utf8",
      "nullable": true
    }
  ],
  "drift_status": "additive_fields_added",
  "added_fields": [
    {
      "name": "channel",
      "type": "utf8",
      "nullable": true
    }
  ],
  "pinned_schema_path": "pinned-orders.schema.yml",
  "pinned_schema_persisted": true
}
```

A load that failed before it could finish resolving a schema reports the
posture it was configured for, with no comparison run. An inference posture
that never finished has no `fields` — nothing had been resolved yet — while a
pinned posture still states the pin's own fields, because those were read
from the file. A load that failed *after* resolution finished, such as a
reject threshold breach, carries the `fields` it had resolved. Here an
override named a field the source does not have, on a definition whose pin
did not exist yet:

```json
{
  "mode": "inferred",
  "drift_status": "not_applicable",
  "pinned_schema_path": "new-pin.schema.yml",
  "overrides": [
    {
      "name": "nope",
      "type": "int64"
    }
  ]
}
```

A load that resolved no schema — an unreadable or invalid definition, an
unsupported connector, an unsupported load mode, or a source that offered no
shape to resolve one from — reports the decision as unmade:

```json
{
  "mode": "not_evaluated"
}
```

### Drift detail

`drift` appears when `drift_status` is `failed_on_drift`, and states what the
comparison found. It carries one of three shapes.

Shape drift — pinned fields the source no longer has (`missing_fields`),
source fields the pin does not have (`added_fields`), or both
([ADR-0034](../adr/0034-validate-pinned-schemas-by-name-with-lattice-widening.md)).
Both are lists of names, not of field objects, and either is empty when only
the other kind of drift occurred:

```json
{
  "missing_fields": [
    "placed_at"
  ],
  "added_fields": [
    "channel"
  ]
}
```

Duplicate dataset field names (`duplicate_fields`), which cannot be matched
against the pin at all:

```json
{
  "duplicate_fields": [
    "a"
  ]
}
```

A pinned declared type with no override re-declaring it
(`undeclared_fields`,
[ADR-0042](../adr/0042-add-field-types-through-declaration-never-inference.md)):
declared types enter a schema only through declaration, so each entry names
the field, its `pinned_type`, and the `effective_type` the source would
otherwise produce:

```json
{
  "undeclared_fields": [
    {
      "name": "amount",
      "pinned_type": "decimal(10,2)",
      "effective_type": "float64"
    }
  ]
}
```

### `conflict`

An override that contradicts the pinned field it names is a broken
definition, not drift
([ADR-0038](../adr/0038-apply-per-field-overrides-to-inferred-schemas-never-the-pin.md)),
so it fails with `schema_override_conflict` and `drift_status:
not_applicable`. `conflict` names the `field`, its `pinned` properties, and
the `override` — which lists only the properties the override actually set:

```json
{
  "field": "amount",
  "pinned": {
    "type": "float64",
    "nullable": true
  },
  "override": {
    "type": "int64",
    "nullable": false
  }
}
```

### `transform` and `overrides` echoes

When the definition declared a structural transform or per-field overrides,
every schema decision the load reached echoes them as written, with unset
keys omitted — the decisions its failures carry included, which is what makes
a failed load's decision readable. A load that resolved no schema reports
`mode: not_evaluated`, and that decision carries no echoes. The
transform echo carries `flatten`, `select`, and `rename` exactly as the
definition set them:

```json
{
  "flatten": {
    "customer.name": "customer_name"
  },
  "select": [
    "id",
    "amount",
    "customer_name"
  ],
  "rename": {
    "id": "order_id"
  }
}
```

```json
[
  {
    "name": "amount",
    "type": "decimal(10,2)"
  },
  {
    "name": "order_id",
    "nullable": false
  }
]
```

The echoes describe the definition; `fields` describes the result. Overrides
apply in the dataset namespace — after the transform renamed and selected
fields — so an override's `name` matches a `fields` entry, not necessarily a
source field ([ADR-0040](../adr/0040-apply-structural-transforms-before-schema-pinning.md)).

## Measurements

### `row_counts`

| Key | Meaning |
| --- | --- |
| `source` | Records read from the source. |
| `written` | Records the destination accepted. |
| `rejected` | Records that could not be written without violating the schema or the load rules. |

On a successful load, `written` is `source` minus `rejected`:

```json
{
  "source": 3,
  "written": 2,
  "rejected": 1
}
```

On a failed load the counts are honest about how far the load got. `source`
is `0` when the failure preceded the read finishing, and `written` counts
only what the destination had already committed — for an append that failed
part way, the committed chunk prefix:

```json
{
  "source": 2,
  "written": 1,
  "rejected": 0
}
```

### `byte_counts`

| Key | Meaning |
| --- | --- |
| `source` | The source file's size in bytes, measured when it was opened. |
| `destination` | The bytes the destination wrote, or `null` when it has no honest count. |

```json
{
  "source": 284,
  "destination": 1568
}
```

A `parquet` destination measures the part files it wrote. A `duckdb`
destination reports `null`: a table, unlike a file directory, has no
measurable on-disk extent of its own
([ADR-0030](../adr/0030-bundled-duckdb-destination-with-arrow-native-transactional-replace.md)).

Both counts are `null` in every failure report.

### `timings`

| Key | Meaning |
| --- | --- |
| `started_unix_ms` | Load start, in milliseconds since the Unix epoch. |
| `finished_unix_ms` | Load finish, on the same clock. |
| `duration_ms` | `finished_unix_ms` minus `started_unix_ms`. |

The clock is the system wall clock, so the timestamps are comparable across
loads only as far as that clock is. The window opens before the definition is
read and closes once the destination write has finished — the rejected
records are streamed inside it, but assembling and writing the report itself
falls outside it, so `duration_ms` measures the load, not the reporting.

## `rejected_records`

Governing ADRs:
[ADR-0020](../adr/0020-default-reject-threshold-zero.md),
[ADR-0035](../adr/0035-reject-pinned-value-misfits-per-record.md),
[ADR-0036](../adr/0036-write-rejected-records-jsonl-under-a-flat-reject-threshold.md).

| Key | Meaning |
| --- | --- |
| `count` | How many records were rejected. Mirrors `row_counts.rejected`. |
| `artifact` | The rejected-records file, or `null` when nothing was rejected. |

```json
{
  "count": 1,
  "artifact": ".data-spark/runs/<load_id>/rejected-records.jsonl"
}
```

The artifact is `rejected-records.jsonl` inside this load's artifact
directory, one JSON object per rejected record. It is streamed while the
source is read, so it exists before the report that names it — including on
failures. A load that rejected more records than `reject_threshold` tolerates
fails with `reject_threshold_exceeded`, and reports exactly the same two
fields: the records it had rejected before it stopped, and the artifact it
wrote them to.

## `destination_write`

Governing ADRs:
[ADR-0021](../adr/0021-report-destination-write-atomicity.md),
[ADR-0047](../adr/0047-commit-full-refresh-terminally-and-append-per-chunk.md).

What the destination can say about the visibility of what it wrote. Write
atomicity is a property of the destination and the load mode, so the
destination itself states it and the report carries it verbatim.

| Key | Present when | Meaning |
| --- | --- | --- |
| `atomicity` | always | `atomic`, `best_effort`, or `not_applicable`. |
| `strategy` | `atomicity` is not `not_applicable` | The named write strategy the destination used. |

| `atomicity` | Meaning |
| --- | --- |
| `atomic` | The write became visible completely or not at all. |
| `best_effort` | Partial destination changes are possible; what `row_counts.written` states was committed. |
| `not_applicable` | Nothing was committed — the load never wrote, or its write was rolled back. |

The strategies the shipped destinations report:

| Destination | Load mode | `atomicity` | `strategy` |
| --- | --- | --- | --- |
| `duckdb` | `full_refresh` | `atomic` | `transactional_replace` |
| `duckdb` | `append` | `best_effort` | `insert` |
| `parquet` | `full_refresh` | `best_effort` | `staging_then_replace` |
| `parquet` | `append` | `best_effort` | `staged_part_append` |

A full refresh commits once, terminally, so a full refresh that failed before
that commit leaves the destination dataset unchanged and reports
`not_applicable`:

```json
{
  "atomicity": "not_applicable"
}
```

An append commits per chunk
([ADR-0047](../adr/0047-commit-full-refresh-terminally-and-append-per-chunk.md)),
so a failure after the first commit has genuinely changed the destination
dataset, and says so:

```json
{
  "atomicity": "best_effort",
  "strategy": "insert"
}
```

An append that fails before committing anything reports `not_applicable` like
any other untouched destination, and a full refresh that fails *after* its
terminal commit — a connection that would not close cleanly, say — still
reports the `atomic` write it had already made. The commit boundary, not the
load mode, decides what this field says.

## `execution`

Governing ADRs:
[ADR-0016](../adr/0016-arrow-recordbatch-internal-format.md),
[ADR-0046](../adr/0046-resolve-then-stream-connector-ports-with-chunked-sessions.md),
[ADR-0047](../adr/0047-commit-full-refresh-terminally-and-append-per-chunk.md),
[ADR-0050](../adr/0050-report-retry-as-a-policy-echo-with-per-failed-attempt-entries.md),
[ADR-0053](../adr/0053-report-parallelism-as-the-effective-value-beside-the-connector-limit.md).

How the load actually ran. The object takes one of two postures, and
`record_format` names which.

| Key | Present when | Meaning |
| --- | --- | --- |
| `record_format` | always | `arrow_record_batch` or `not_started`. |
| `batch_count` | always | Chunks committed to the destination. |
| `chunk_rows` | `record_format` is `arrow_record_batch` | The effective chunk bound. |
| `parallelism` | `record_format` is `arrow_record_batch` | The effective load parallelism. |
| `connector_parallelism_limit` | `record_format` is `arrow_record_batch` | The destination connector's limit for this load mode. |
| `retry` | always in the `arrow_record_batch` posture; in `not_started` only when attempts were recorded | The retry policy echo and the attempts log. |

### The `arrow_record_batch` posture

The load entered the write phase: records were exchanged as Arrow
`RecordBatch` chunks ([ADR-0016](../adr/0016-arrow-recordbatch-internal-format.md)).
Every successful load has this posture, and so does every failure that
happened once the destination session was open.

```json
{
  "record_format": "arrow_record_batch",
  "batch_count": 2,
  "chunk_rows": 2,
  "parallelism": 1,
  "connector_parallelism_limit": 1,
  "retry": {
    "max_attempts": 3,
    "initial_delay_ms": 200,
    "max_delay_ms": 5000,
    "attempts": []
  }
}
```

`batch_count` counts chunks the destination committed — not chunks
attempted. On a successful load that is every chunk of the source; on a
failed append it is the committed prefix; on a full refresh that failed
before its terminal commit it is `0`, because a full refresh commits once at
the end
([ADR-0047](../adr/0047-commit-full-refresh-terminally-and-append-per-chunk.md)).

`chunk_rows` is the effective chunk bound: `execution.chunk_rows` from the
definition, or the default `65536`
([ADR-0046](../adr/0046-resolve-then-stream-connector-ports-with-chunked-sessions.md)).
A chunk holds at most this many surviving records, which is what keeps a
load's memory use bounded independently of source size.

`parallelism` is the effective load parallelism, and
`connector_parallelism_limit` is the maximum the destination connector
declares for this load mode
([ADR-0053](../adr/0053-report-parallelism-as-the-effective-value-beside-the-connector-limit.md)).
The effective value is `min(configured, limit)`, and the limit is also the
default when the definition configures nothing
([ADR-0052](../adr/0052-configure-parallelism-as-one-scalar-clamped-to-the-connector-limit.md)).
Reporting both makes a clamp legible: the example above ran with
`parallelism: 4` in its definition and `1` in effect. Both shipped
destinations declare a limit of `1` for every load mode
([ADR-0023](../adr/0023-conservative-parallelism-defaults.md)), so today
every load runs serially.

### The `not_started` posture

The load failed before any record batch was exchanged — a definition error, a
schema decision that failed, a breached reject threshold, a pin that could
not be written, a destination session that would not open. The chunk bound,
parallelism, and limit are omitted rather than reported as values that never
took effect:

```json
{
  "record_format": "not_started",
  "batch_count": 0
}
```

### `retry`

The retry policy this load ran under, echoed with its resolved defaults, plus
the log of failed attempts
([ADR-0050](../adr/0050-report-retry-as-a-policy-echo-with-per-failed-attempt-entries.md)).

| Key | Meaning |
| --- | --- |
| `max_attempts` | Attempts allowed per retry unit, the first attempt included. |
| `initial_delay_ms` | The wait before the second attempt. |
| `max_delay_ms` | The ceiling on the exponential backoff. |
| `attempts` | One entry per failed attempt, empty when nothing failed. |

`attempts` is empty on every load whose units all succeeded first time, which
is every load the shipped connectors can produce: no shipped failure is
classified transient
([ADR-0048](../adr/0048-classify-transience-at-the-originator-and-retry-only-provably-uncommitted-units.md)),
so nothing is ever retried, and the `not_started` posture therefore never
carries `retry` at all today. The entry shape below is part of the version 1
contract for the connectors that will — it is the only block in this
reference no load can currently produce:

```json
{
  "operation": "write_chunk",
  "chunk_index": 4,
  "attempt": 1,
  "error": {
    "code": "destination_write_failed",
    "message": "…"
  },
  "delay_before_retry_ms": 200
}
```

| Key | Present when | Meaning |
| --- | --- | --- |
| `operation` | always | The retry unit: `begin` or `write_chunk`. |
| `chunk_index` | `operation` is `write_chunk` | The 0-based index of the chunk. |
| `attempt` | always | The 1-based attempt within that unit. |
| `error` | always | The failure that attempt hit, in the `error_summary` shape. |
| `delay_before_retry_ms` | another attempt followed | The wait before it. |

Entries are ordered deterministically — `begin` first, then by chunk index,
then by attempt — so a report does not depend on the wall-clock order in
which concurrent work happened to fail.

## Outcome

### `exit_status` and `process_exit_code`

| `exit_status` | `process_exit_code` | Meaning |
| --- | --- | --- |
| `succeeded` | `0` | The load moved its records. |
| `failed` | `1` | The load did not complete. |

The two always agree, and `process_exit_code` is the code the process
actually exited with — so an orchestrator may branch on either. The only
exit code with no report behind it is the `1` of a load that could not write
its artifacts at all.

### `error_summary`

`null` on success. On failure, the failure that ended the load:

| Key | Meaning |
| --- | --- |
| `code` | The stable machine-readable failure code. |
| `message` | The human-readable message, also printed by the load summary. |

```json
{
  "code": "unsupported_load_mode",
  "message": "unsupported load mode: merge"
}
```

Branch on `code`; `message` names paths, fields, and counts, and its wording
is not a contract. A load reports the first failure it hit: checks run in a
fixed order and the first one to fail ends the load
([ADR-0019](../adr/0019-validate-schema-and-load-rules-before-writing.md)).

The codes this binary emits, in the order their checks run. The
definition-level codes are explained in context, key by key, in the
[Load Definition Reference](load-definition.md):

| Phase | `code` | Raised when |
| --- | --- | --- |
| Definition | `load_definition_read_failed` | The definition file could not be read. |
| Definition | `invalid_load_definition_yaml` | The file is not valid YAML for the contract, including unknown keys ([ADR-0037](../adr/0037-reject-unknown-fields-in-versioned-yaml-contracts.md)). |
| Definition | `missing_load_definition_version` | No `version` key. |
| Definition | `unsupported_load_definition_version` | A `version` other than `1`. |
| Definition | `missing_source` | No `source` block. |
| Definition | `missing_destination` | No `destination` block. |
| Definition | `unsupported_load_mode` | The load mode is unknown, or the destination does not support it. |
| Definition | `unsupported_source_connector` | The source connector is not `local_file`. |
| Definition | `unsupported_destination_connector` | The destination connector is neither `duckdb` nor `parquet`. |
| Definition | `missing_dataset` | A `duckdb` destination without a `dataset`. |
| Definition | `invalid_transform_config` | The `transform` block is empty or malformed. |
| Definition | `invalid_schema_config` | The `schema` block is empty, incomplete, or contradictory. |
| Definition | `unsupported_drift_policy` | A `drift_policy` other than `fail` or `allow_additive_nullable`. |
| Definition | `unsupported_override_type` | An override names a type outside the type vocabulary. |
| Definition | `pinned_schema_read_failed` | The pinned schema file exists but could not be read. |
| Definition | `invalid_pinned_schema` | The pinned schema file is not a valid pin. |
| Read | `unsupported_source_format` | The resolved source format is neither `csv` nor `jsonl`. |
| Read | `source_read_failed` | The source file could not be opened or read. |
| Read | `malformed_csv` | The CSV header could not be parsed, or names no fields. |
| Read | `malformed_jsonl` | No JSONL record has any field, so no schema can be inferred. Rejected records reach this code only when [the threshold tolerates them all](load-definition.md#when-every-record-is-rejected). |
| Read | `unknown_transform_field` | The transform names a field absent from the observed source shape. |
| Read | `transform_name_collision` | Two dataset fields would end up with the same name. |
| Read | `unknown_override_field` | An override names a field absent from the dataset shape. |
| Read | `schema_override_conflict` | An override contradicts the pinned field it names. |
| Read | `schema_drift` | The source shape drifted from the pin in a way the policy forbids. |
| Read | `reject_threshold_exceeded` | More records were rejected than `reject_threshold` tolerates. |
| Write | `pinned_schema_write_failed` | The pinned schema file could not be persisted — after validation, before the first destination write. |
| Write | `record_batch_creation_failed` | A chunk could not be assembled from its records. |
| Write | `schema_coercion_failed` | A value could not be materialized as its planned type. |
| Write | `source_changed_during_load` | The second source pass saw a source that no longer matches the first. |
| Write | `destination_write_failed` | The destination refused or failed a write, commit, or session. |

Rejected records carry their own codes inside `rejected-records.jsonl`; they
are not `error_summary` codes.

## Compatibility within `report_version: 1`

[ADR-0026](../adr/0026-version-load-definitions-and-reports.md) fixes the
versioning contract: a report declares the version that decides how it is
read. What follows is not a further decision but a description — how version
1 has actually been extended so far, and what a reader can therefore lean on.

A reader of version 1 reports may rely on:

- `report_version` being present and `1`.
- Every [top-level field](#top-level-shape) being present in every report,
  with the stated type or `null`.
- `exit_status` and `process_exit_code` agreeing, and `error_summary` being
  `null` exactly when `exit_status` is `succeeded`.
- The report existing at `<artifact_dir>/load-report.json` for every load
  that could write its artifacts.
- Field names, not field order.

A reader should tolerate, without failing:

- New keys anywhere in the document. Within version 1, new information has
  always arrived as new keys — write atomicity
  ([ADR-0021](../adr/0021-report-destination-write-atomicity.md)),
  rejected-record facts
  ([ADR-0036](../adr/0036-write-rejected-records-jsonl-under-a-flat-reject-threshold.md)),
  the pinning and drift keys
  ([ADR-0033](../adr/0033-persist-pinned-schemas-as-versioned-yaml-files.md),
  [ADR-0034](../adr/0034-validate-pinned-schemas-by-name-with-lattice-widening.md)),
  the chunking and retry keys
  ([ADR-0046](../adr/0046-resolve-then-stream-connector-ports-with-chunked-sessions.md),
  [ADR-0050](../adr/0050-report-retry-as-a-policy-echo-with-per-failed-attempt-entries.md)),
  the parallelism keys
  ([ADR-0053](../adr/0053-report-parallelism-as-the-effective-value-beside-the-connector-limit.md)),
  and the binary-version provenance
  ([ADR-0055](../adr/0055-surface-the-binary-version-as-version-output-and-top-level-report-provenance.md))
  were all added this way, without a version bump.
- New values in `mode`, `drift_status`, `atomicity`, `strategy`,
  `record_format`, and `error_summary.code` — for instance, the strategy name
  of a destination connector that does not exist yet.
- Optional keys being absent. The tables above state when each is present;
  a key that is absent means the load has nothing to say, never `null` by
  another name.

Removing a key, renaming one, or changing what an existing key means would be
a breaking change, and would come with a new `report_version`.

## Complete examples

Both reports below come from two real loads of the same definition, one after
the other. Only `load_id`, the `artifact_dir` that embeds it, and the three
`timings` values are specific to those two loads; everything else is
reproducible from the definition and the two source snapshots shown.

The definition pins its schema and fails on drift:

```yaml
version: 1
source:
  connector: local_file
  path: orders.jsonl
  format: jsonl
destination:
  connector: duckdb
  path: analytics.duckdb
dataset: orders
load_mode: full_refresh
schema:
  pinned_path: orders.schema.yml
  drift_policy: fail
```

### A load that succeeded

The first load: three JSONL records into a DuckDB table, with the pin
bootstrapped from this load's own inference. `orders.jsonl` holds:

```json
{"order_id": 1001, "customer": "Ada", "amount": 42.5, "placed_at": "2026-01-05T09:30:00Z"}
{"order_id": 1002, "customer": "Grace", "amount": 7.25, "placed_at": "2026-01-06T14:00:00Z"}
{"order_id": 1003, "customer": "Edsger", "amount": 19.99, "placed_at": "2026-01-07T11:15:00Z"}
```

```json
{
  "report_version": 1,
  "binary_version": "0.2.1",
  "load_id": "c9ec9c01-8ce3-454a-b536-d8c0c71b4765",
  "artifact_dir": ".data-spark/runs/c9ec9c01-8ce3-454a-b536-d8c0c71b4765",
  "source_summary": {
    "connector": "local_file",
    "path": "orders.jsonl",
    "format": "jsonl"
  },
  "destination_summary": {
    "connector": "duckdb",
    "path": "analytics.duckdb"
  },
  "dataset": "orders",
  "load_mode": "full_refresh",
  "schema_decision": {
    "mode": "inferred",
    "fields": [
      {
        "name": "order_id",
        "type": "int64",
        "nullable": true
      },
      {
        "name": "customer",
        "type": "utf8",
        "nullable": true
      },
      {
        "name": "amount",
        "type": "float64",
        "nullable": true
      },
      {
        "name": "placed_at",
        "type": "utf8",
        "nullable": true
      }
    ],
    "drift_status": "not_applicable",
    "pinned_schema_path": "orders.schema.yml",
    "pinned_schema_persisted": true
  },
  "row_counts": {
    "source": 3,
    "written": 3,
    "rejected": 0
  },
  "byte_counts": {
    "source": 279,
    "destination": null
  },
  "rejected_records": {
    "count": 0,
    "artifact": null
  },
  "destination_write": {
    "atomicity": "atomic",
    "strategy": "transactional_replace"
  },
  "execution": {
    "record_format": "arrow_record_batch",
    "batch_count": 1,
    "chunk_rows": 65536,
    "parallelism": 1,
    "connector_parallelism_limit": 1,
    "retry": {
      "max_attempts": 3,
      "initial_delay_ms": 200,
      "max_delay_ms": 5000,
      "attempts": []
    }
  },
  "timings": {
    "started_unix_ms": 1785396627637,
    "finished_unix_ms": 1785396627896,
    "duration_ms": 259
  },
  "exit_status": "succeeded",
  "process_exit_code": 0,
  "error_summary": null
}
```

### A load that failed

The next load of the same definition, after the source file lost `placed_at`
and gained `channel`. `orders.jsonl` now holds:

```json
{"order_id": 1004, "customer": "Alan", "amount": 3.5, "channel": "web"}
{"order_id": 1005, "customer": "Barbara", "amount": 120.0, "channel": "store"}
{"order_id": 1006, "customer": "Donald", "amount": 55.25, "channel": "web"}
```

The pin is unchanged, the destination table is untouched, and the drift
detail says exactly what moved.

```json
{
  "report_version": 1,
  "binary_version": "0.2.1",
  "load_id": "4d07ddaf-2731-4e8f-8b4a-518093a781b0",
  "artifact_dir": ".data-spark/runs/4d07ddaf-2731-4e8f-8b4a-518093a781b0",
  "source_summary": {
    "connector": "local_file",
    "path": "orders.jsonl",
    "format": "jsonl"
  },
  "destination_summary": {
    "connector": "duckdb",
    "path": "analytics.duckdb"
  },
  "dataset": "orders",
  "load_mode": "full_refresh",
  "schema_decision": {
    "mode": "pinned",
    "fields": [
      {
        "name": "order_id",
        "type": "int64",
        "nullable": true
      },
      {
        "name": "customer",
        "type": "utf8",
        "nullable": true
      },
      {
        "name": "amount",
        "type": "float64",
        "nullable": true
      },
      {
        "name": "placed_at",
        "type": "utf8",
        "nullable": true
      }
    ],
    "drift_status": "failed_on_drift",
    "drift": {
      "missing_fields": [
        "placed_at"
      ],
      "added_fields": [
        "channel"
      ]
    },
    "pinned_schema_path": "orders.schema.yml"
  },
  "row_counts": {
    "source": 0,
    "written": 0,
    "rejected": 0
  },
  "byte_counts": {
    "source": null,
    "destination": null
  },
  "rejected_records": {
    "count": 0,
    "artifact": null
  },
  "destination_write": {
    "atomicity": "not_applicable"
  },
  "execution": {
    "record_format": "not_started",
    "batch_count": 0
  },
  "timings": {
    "started_unix_ms": 1785396634976,
    "finished_unix_ms": 1785396634977,
    "duration_ms": 1
  },
  "exit_status": "failed",
  "process_exit_code": 1,
  "error_summary": {
    "code": "schema_drift",
    "message": "schema drift against pinned schema orders.schema.yml: missing fields: placed_at; added fields: channel"
  }
}
```
