# Load Definition Reference — Version 1

A load definition is the saved YAML description of a repeatable load: its
source, destination, load mode, schema choices, and related rules
([ADR-0010](../adr/0010-yaml-load-definitions.md)). It is a versioned
contract: every definition declares `version: 1`
([ADR-0026](../adr/0026-version-load-definitions-and-reports.md)), and
parsing is strict — a key the contract does not declare fails the load
instead of being silently ignored
([ADR-0037](../adr/0037-reject-unknown-fields-in-versioned-yaml-contracts.md)).

A definition runs with:

```bash
data-spark load [--output-dir <dir>] <definition.yml>
```

A load — success or failure — writes a JSON load report
(`load-report.json`) into its artifact directory and exits `0` on success or
`1` on failure. The one exception is a failure to produce the load artifacts
themselves — an artifact directory that cannot be created, or an artifact
write that fails — which aborts the load with exit code `1` and no report.
The failure codes named throughout this reference appear in the report's
`error_summary.code`. The load report contract is documented in the
[Load Report Reference](load-report.md).

## Conventions in this reference

- Complete examples are runnable load definitions, proven against the real
  binary.
- Blocks whose first line is `# Fragment — not a complete load definition.`
  show a single key in isolation and do not run on their own.

## Top-level keys

| Key | Required | Default | Purpose |
| --- | --- | --- | --- |
| `version` | yes | — | The load definition version; only `1` is supported. |
| `source` | yes | — | Where records are read from. |
| `destination` | yes | — | Where records are written to. |
| `dataset` | for `duckdb` | — | The dataset name; addresses the DuckDB table. |
| `load_mode` | no | `full_refresh` | How the load changes the destination dataset. |
| `transform` | no | none | The structural transform: flatten mapping, field selection, rename mapping. |
| `schema` | no | inference | Schema pinning, drift policy, and per-field overrides. |
| `execution` | no | all defaults | Chunk bound, load parallelism, retry policy. |
| `reject_threshold` | no | `0` | Rejected records tolerated before the load fails. |
| `artifacts` | no | `.data-spark/runs` | The root under which the artifact directory is created. |

A complete definition using every top-level key (the sections below refer
back to it):

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
transform:
  flatten:
    customer.id: customer_id
    customer.name: customer_name
  select: [order_id, customer_id, customer_name, amount, ordered_at]
  rename:
    ordered_at: ordered_at_utc
schema:
  pinned_path: orders.schema.yml
  drift_policy: fail
  overrides:
    - name: amount
      type: decimal(10,2)
    - name: ordered_at_utc
      type: timestamptz
      nullable: false
execution:
  chunk_rows: 65536
  parallelism: 1
  retry:
    max_attempts: 3
    initial_delay_ms: 200
    max_delay_ms: 5000
reject_threshold: 0
artifacts:
  dir: .data-spark/runs
```

### Validation order

An invalid definition fails before any source is read or destination touched
([ADR-0019](../adr/0019-validate-schema-and-load-rules-before-writing.md)).
Checks run in a fixed order, and the first failure wins: YAML parsing
(including unknown keys) → `version` → `source` and `destination` presence →
load mode → source connector → destination connector → destination support
for the mode → `transform` → `schema` configuration → the pinned schema file
→ the source read (which validates the resolved source format first) → the
destination write.

## `version`

Governing ADRs:
[ADR-0026](../adr/0026-version-load-definitions-and-reports.md),
[ADR-0037](../adr/0037-reject-unknown-fields-in-versioned-yaml-contracts.md).

The load definition version. Required; the only supported value is `1`.

```yaml
# Fragment — not a complete load definition.
version: 1
```

- Omitting `version` fails the load with `missing_load_definition_version`.
- Any other non-negative integer fails with
  `unsupported_load_definition_version` and a message naming the declared
  version.
- A negative or non-integer value fails YAML parsing
  (`invalid_load_definition_yaml`).

A newer definition that carries keys this contract does not declare fails on
the unknown key first (see [Strictness](#strictness-unknown-keys-are-rejected)),
so an older binary meeting a newer definition fails on the key it cannot
honor rather than ignoring it.

## `source`

Governing ADRs:
[ADR-0027](../adr/0027-start-with-local-file-to-duckdb-and-parquet-slice.md),
[ADR-0045](../adr/0045-resolve-loads-in-two-streaming-source-passes.md).

Where the load reads records from. Omitting the block fails the load with
`missing_source`.

```yaml
# Fragment — not a complete load definition.
source:
  connector: local_file
  path: orders.jsonl
  format: jsonl
```

| Key | Required | Meaning |
| --- | --- | --- |
| `connector` | yes | The source connector name. `local_file` is the only supported value; anything else fails with `unsupported_source_connector`. |
| `path` | yes | The file the load reads, absolute or relative to the working directory. |
| `format` | no | The source format: `csv` or `jsonl`. Absent, the format resolves from the path's extension. |

Format resolution: the explicit `format` wins; otherwise the path extension
is used as written (`orders.jsonl` resolves to `jsonl`; an uppercase
`orders.CSV` resolves to `CSV`, which is not a supported format). A resolved
format other than `csv` or `jsonl` fails the load with
`unsupported_source_format` before the file is opened.

- **CSV**: the first record is the header naming the source fields. A record
  that does not parse as a record of the header's fields becomes a rejected
  record (`malformed_csv_record`).
- **JSONL**: one JSON object per line. A line that is not a JSON object
  becomes a rejected record (`malformed_jsonl_record`).

A `path` that cannot be read fails the load with `source_read_failed`. The
source file is read in two streaming passes
([ADR-0045](../adr/0045-resolve-loads-in-two-streaming-source-passes.md)):
every whole-input decision — schema inference, pin comparison, the reject
threshold — is made over the full input before any record is written.

## `destination`

Governing ADRs:
[ADR-0027](../adr/0027-start-with-local-file-to-duckdb-and-parquet-slice.md),
[ADR-0030](../adr/0030-bundled-duckdb-destination-with-arrow-native-transactional-replace.md),
[ADR-0021](../adr/0021-report-destination-write-atomicity.md).

Where the load writes records to. Omitting the block fails the load with
`missing_destination`.

```yaml
# Fragment — not a complete load definition.
destination:
  connector: duckdb
  path: analytics.duckdb
```

| Key | Required | Meaning |
| --- | --- | --- |
| `connector` | yes | The destination connector name: `duckdb` or `parquet`. Anything else fails with `unsupported_destination_connector`. |
| `path` | yes | Destination addressing; connector-specific, see below. |

### `duckdb`

`path` is the DuckDB database file. The file and its parent directories are
created when missing. A `duckdb` destination requires the top-level
[`dataset`](#dataset) key: `dataset` names the table the records materialize
as ([ADR-0030](../adr/0030-bundled-duckdb-destination-with-arrow-native-transactional-replace.md)).
Identifier content is left to DuckDB — the name is quoted, with embedded `"`
doubled, and no allowlist is applied.

- Full refresh replaces the table in one explicit transaction — the load
  report states `atomicity: atomic`, `strategy: transactional_replace`.
- Append runs one auto-committed `INSERT ... BY NAME` per chunk —
  `best_effort` / `insert`.

### `parquet`

`path` is the dataset directory; records land as `part-*.parquet` files
inside it. `dataset` is optional for a `parquet` destination and is echoed in
the load report.

- Full refresh stages every part in a unique staging directory and replaces
  the destination directory in a single terminal rename — `best_effort` /
  `staging_then_replace`.
- Append stages and renames one complete part file per chunk —
  `best_effort` / `staged_part_append`.

A complete definition loading a CSV file into a Parquet dataset directory in
append mode, with the source format resolved from the path extension:

```yaml
version: 1
source:
  connector: local_file
  path: events.csv
destination:
  connector: parquet
  path: events-dataset
load_mode: append
```

## `dataset`

Governing ADR:
[ADR-0030](../adr/0030-bundled-duckdb-destination-with-arrow-native-transactional-replace.md).

The dataset name — the named collection of records the load produces.

```yaml
# Fragment — not a complete load definition.
dataset: orders
```

Required and non-empty for a `duckdb` destination, where it names the
destination table; omitting it (or declaring it empty) fails the load with
`missing_dataset`. Optional otherwise. The name is echoed in the load report.

## `load_mode`

Governing ADRs:
[ADR-0047](../adr/0047-commit-full-refresh-terminally-and-append-per-chunk.md),
[ADR-0021](../adr/0021-report-destination-write-atomicity.md).

How the load changes the destination dataset. Optional; the default is
`full_refresh`. Any other value fails the load with `unsupported_load_mode`.
Both shipped destination connectors support both modes.

```yaml
# Fragment — not a complete load definition.
load_mode: append
```

- **`full_refresh`** replaces the destination dataset with the source's
  current records. The commit boundary is terminal regardless of chunk
  count: nothing becomes visible before the single commit at the end of the
  load, and a failure before it leaves the destination untouched.
- **`append`** adds the source's records to the destination dataset without
  changing existing records, committing once per chunk: a failure after `k`
  committed chunks leaves exactly that prefix visible, traceable through the
  load report.

## `transform`

Governing ADRs:
[ADR-0039](../adr/0039-fixed-order-select-then-rename-transform-block.md),
[ADR-0040](../adr/0040-apply-structural-transforms-before-schema-pinning.md),
[ADR-0041](../adr/0041-flatten-declared-source-paths-into-added-dataset-fields.md).

The structural transform: reshapes observed source fields into the dataset
shape. Optional. The evaluation order is fixed by the contract — **flatten
mapping → field selection → rename mapping** — and the transform applies
before everything that consumes a schema: pinned schemas, schema overrides,
drift comparison, and per-record validation all speak the transformed
dataset names
([ADR-0040](../adr/0040-apply-structural-transforms-before-schema-pinning.md)).

```yaml
# Fragment — not a complete load definition.
transform:
  flatten:
    customer.id: customer_id
  select: [order_id, customer_id, amount]
  rename:
    amount: amount_usd
```

A `transform` block must declare at least one of the three keys; an empty
block is a definition error (`invalid_transform_config`), as is every
misconfiguration below unless another code is named.

### `transform.flatten` — flatten mapping

A map of source path → dataset field name. Extracts the value at each
declared source path into an added dataset field
([ADR-0041](../adr/0041-flatten-declared-source-paths-into-added-dataset-fields.md)).

- Requires a JSONL source: declaring `flatten` for a CSV source is a
  definition error, because CSV cells hold no addressable structure.
- A source path is dot notation with at least two non-empty segments
  (`customer.id`, `payment.card.brand`). Paths address object keys only —
  no array indexing — and take their depth from the declaration. A source
  key containing a literal dot is unaddressable.
- Flattening is purely additive: the parent source field keeps materializing
  as JSON text, and field selection remains the only mechanism that removes
  fields.
- Extraction is total and never rejects a record: a scalar leaf yields the
  value; a null anywhere on the path, a missing leaf key, or an intermediate
  segment that is not an object yields null; an object or array leaf yields
  its compact JSON text.
- Outputs are added in declaration order after the observed fields and are
  ordinary fields to selection and renaming.
- Output names must be unique, must appear in a declared `select` list (no
  no-op extraction), and may never shadow an observed source field — the
  last check fails at read time (`transform_name_collision`).
- A duplicate path key fails YAML parsing.

### `transform.select` — field selection

A list of source field names (post-flatten) to keep. The list order fixes
the dataset field order.

- Entries must be unique, and the list must name at least one field.
- A select entry naming no observed field fails the load at read time
  (`unknown_transform_field`).
- Selection shields unselected source fields from drift: a new source field
  outside the select list is invisible and yields no drift under any policy.

### `transform.rename` — rename mapping

A map of source field name → dataset field name, applied simultaneously over
the selected (or, without `select`, full post-flatten) field set — so swaps
like `{A: B, B: A}` are legal, and unmapped fields pass through under their
source names.

- Keys always mean source names; with a declared `select` list, every rename
  key must be in it (no implicit selection).
- Identity renames, empty targets, duplicate targets, and duplicate keys are
  errors (duplicate keys fail YAML parsing).
- A rename key naming no observed field fails at read time
  (`unknown_transform_field`); a target colliding with a pass-through field
  name — reachable only without `select` — fails there too
  (`transform_name_collision`).

## `schema`

The schema block controls how the dataset schema is decided: pinning it
across loads, the drift policy, and per-field overrides. Optional; without
it, the load infers the schema from observed source records
([ADR-0006](../adr/0006-infer-schemas-by-default.md)).

A `schema` block is valid with `pinned_path`, `overrides`, or both;
`drift_policy` requires `pinned_path`. An empty block, an empty
`pinned_path`, or an empty `overrides` list is a definition error
(`invalid_schema_config`).

Inference observes four types — `boolean`, `int64`, `float64`, `utf8` —
widening within that lattice: integers widen to floats, anything widens to
text, and an all-null field materializes as `utf8`. Inferred fields are
nullable. Numeric-looking text infers conservatively: non-finite numeric
text such as `NaN` or `inf` stays `utf8`
([ADR-0031](../adr/0031-infer-non-finite-numeric-text-as-text.md)), and
zero-padded numeric text such as `007` stays `utf8`
([ADR-0032](../adr/0032-infer-zero-padded-numeric-text-as-text.md)).

### `schema.pinned_path`

Governing ADRs:
[ADR-0033](../adr/0033-persist-pinned-schemas-as-versioned-yaml-files.md),
[ADR-0034](../adr/0034-validate-pinned-schemas-by-name-with-lattice-widening.md),
[ADR-0035](../adr/0035-reject-pinned-value-misfits-per-record.md).

The path of the pinned schema file the load reuses across loads to keep a
BI-ready dataset stable.

```yaml
# Fragment — not a complete load definition.
schema:
  pinned_path: orders.schema.yml
```

The file works like a lockfile, bootstrapped by the first load:

- **File absent**: the load persists the schema it resolved — inferred,
  transformed, and overridden — as the new pin. The write happens after the
  reject threshold gate and before the first destination write, so a failed
  load never persists a pin for records it refused.
- **File present**: the load parses it and validates observed records
  against it by field name, not position. A field's observed type is
  accepted when it widens to the pinned type under the same lattice
  [inference](#schema) uses: an all-null field matches any pinned type,
  `int64` widens to `float64`, and anything widens to `utf8`. Declared types are exactly equal or different —
  no widening involves them. Matching loads materialize records in the pin's
  field order, so the destination keeps a stable field order even when the
  source reorders fields.

A missing pinned field, an incompatible type, or an extra field not
permitted by the drift policy fails the load with `schema_drift` before any
destination write. A pinned field that no observed record carries at all is
missing-field drift under every policy — even for JSONL, where a field
absent from an individual record reads as null — so a silently renamed
source field is caught rather than quietly becoming an all-null field.

Value-level misfits are judged per record, not per load: a value that does
not widen to its pinned type becomes a rejected record
(`type_coercion_failed`), and a null in a `nullable: false` pinned field
becomes a rejected record (`missing_required_field`), both counted against
the [reject threshold](#reject_threshold)
([ADR-0035](../adr/0035-reject-pinned-value-misfits-per-record.md)).

A `pinned_path` that exists but cannot be read fails with
`pinned_schema_read_failed`; a pin that cannot be persisted fails with
`pinned_schema_write_failed`.

#### The pinned schema file

The pinned schema file is itself a versioned strict YAML contract
(`invalid_pinned_schema` on violation): `version: 1`, at least one field,
unique field names, types from the vocabulary below, and no unknown keys
([ADR-0037](../adr/0037-reject-unknown-fields-in-versioned-yaml-contracts.md)).
`nullable` defaults to `true`; a `nullable: false` field is a required
field. The file below is the pin the [complete example](#top-level-keys)
bootstraps, exactly as the binary persists it:

```yaml
version: 1
fields:
- name: order_id
  type: int64
  nullable: true
- name: customer_id
  type: int64
  nullable: true
- name: customer_name
  type: utf8
  nullable: true
- name: amount
  type: decimal(10,2)
  nullable: true
- name: ordered_at_utc
  type: timestamptz
  nullable: false
```

The tool maintains the file, git versions it alongside the load definition,
and hand edits stay possible because loads validate against the file rather
than trust past runs. There is no pin migration: a pin recorded before a
`transform` was configured compares against the transformed shape and fails
as schema drift under the `fail` policy.

### `schema.drift_policy`

Governing ADRs:
[ADR-0007](../adr/0007-fail-fast-on-schema-drift.md),
[ADR-0034](../adr/0034-validate-pinned-schemas-by-name-with-lattice-widening.md).

The rule that decides whether a load may continue when schema drift is
detected against the pinned schema. Requires `pinned_path`.

```yaml
# Fragment — not a complete load definition.
schema:
  pinned_path: orders.schema.yml
  drift_policy: allow_additive_nullable
```

| Value | Meaning |
| --- | --- |
| `fail` (default) | Any schema drift fails the load with `schema_drift`. |
| `allow_additive_nullable` | Additive schema drift — new nullable fields beyond the pin — is accepted; the load rewrites the pinned schema file with the added fields. Every other drift still fails. |

Any other value fails the load with `unsupported_drift_policy`. Rewriting
the pin on additive drift means a field that later disappears again is
caught as drift instead of silently matching the older pin.

### `schema.overrides`

Governing ADRs:
[ADR-0038](../adr/0038-apply-per-field-overrides-to-inferred-schemas-never-the-pin.md),
[ADR-0042](../adr/0042-add-field-types-through-declaration-never-inference.md).

Per-field corrections to the inferred schema: a list of entries naming a
dataset field (post-transform) and replacing its inferred type, nullability,
or both.

```yaml
# Fragment — not a complete load definition.
schema:
  overrides:
    - name: amount
      type: decimal(10,2)
    - name: customer_id
      nullable: false
```

| Key | Required | Meaning |
| --- | --- | --- |
| `name` | yes | The dataset field the override names. Unique across entries. |
| `type` | no | A type from the [field type vocabulary](#field-type-vocabulary); an unknown name fails with `unsupported_override_type`. |
| `nullable` | no | `true` or `false`; `false` declares a required field. |

Every entry must set at least one of `type` or `nullable`
(`invalid_schema_config` otherwise).

An override is a durable correction to inference, so it applies wherever
inference decides a field's shape: an inference-driven load, the first load
that bootstraps a pin, and the added fields an additive drift policy
admits — and the overridden schema is what gets materialized, persisted as
the pin, and compared on later loads. Conflict rules:

- An override naming a field absent from the dataset shape fails the load
  with `unknown_override_field` — including a source field that a transform
  dropped or renamed.
- A field already governed by an existing pinned schema takes nothing from
  an override, but the override must agree with the pinned field; a
  contradiction fails the load with `schema_override_conflict` (an override
  conflict), so a definition and a hand-edited pin cannot drift apart
  unnoticed.

Overridden fields validate per record exactly like pinned fields: values
that do not widen to the overridden type become rejected records
(`type_coercion_failed`), and nulls in a field overridden `nullable: false`
become rejected records (`missing_required_field`) — which means a load with
overrides can reject records even though pure inference never does.
Narrowing overrides are deliberate: correcting a text-inferred field to its
true numeric type while rejecting the few values that made inference widen
is the capability's core scenario.

A complete definition applying overrides to an inferred CSV schema — no pin,
a wall-clock timestamp, a decimal, and a required field:

```yaml
version: 1
source:
  connector: local_file
  path: customers.csv
  format: csv
destination:
  connector: duckdb
  path: crm.duckdb
dataset: customers
schema:
  overrides:
    - name: customer_id
      nullable: false
    - name: signup_at
      type: timestamp
    - name: balance
      type: decimal(10,2)
```

### Field type vocabulary

The types that `schema.overrides` entries and pinned schema files may name.
The first four are inference-reachable; the last three are declared types
([ADR-0042](../adr/0042-add-field-types-through-declaration-never-inference.md)):
they enter a schema only through explicit declaration — `schema.overrides`,
and the pinned schema files a declared load bootstraps or extends — never
through inference.

| Type | Meaning |
| --- | --- |
| `boolean` | True or false. |
| `int64` | 64-bit signed integer. |
| `float64` | 64-bit floating point. |
| `utf8` | Text. |
| `timestamp` | Wall-clock timestamp ([ADR-0043](../adr/0043-split-timestamps-into-wall-clock-and-utc-instant-types.md)). |
| `timestamptz` | Instant timestamp, normalized to UTC ([ADR-0043](../adr/0043-split-timestamps-into-wall-clock-and-utc-instant-types.md)). |
| `decimal(p,s)` | Exact numeric with declared precision and scale, never rounded ([ADR-0044](../adr/0044-parse-declared-decimals-strictly-and-never-round.md)). |

Because no inference ever produces a declared type, a pinned declared-type
field requires its re-declaring override in every load: omitting the
override fails the load as `schema_drift` naming the pinned and effective
types with a missing-override hint. The load definition stays the
declaration of record.

**Timestamps**
([ADR-0043](../adr/0043-split-timestamps-into-wall-clock-and-utc-instant-types.md)).
Both types accept one strict menu — a four-digit year, zero-padded
fixed-width fields, `YYYY-MM-DD`, one space or `T`/`t` separator,
`HH:MM:SS`, and an optional fraction of 1 to 6 digits — and store
microseconds.

- `timestamp` states what a clock read without identifying an instant; its
  text must not carry a UTC offset. `2026-07-01 09:30:00` and
  `2026-07-01T09:30:00.250` fit; `2026-07-01T09:30:00Z` rejects.
- `timestamptz` identifies one absolute moment; its text must end in `Z`/`z`
  or `±hh:mm`, and every offset is normalized to the UTC instant it spells
  at parse time, so different spellings of one instant store equal values.

Everything else rejects per record with the cause named: date-only text,
more than 6 fractional digits (rejection, never truncation), epoch numbers —
including JSON numbers, which never fit a timestamp field — leap-second
text, and every locale-dependent form.

**Decimals**
([ADR-0044](../adr/0044-parse-declared-decimals-strictly-and-never-round.md)).
Only the canonical `decimal(p,s)` spelling is accepted in declarations —
both parameters mandatory, `1 <= p <= 38`, `0 <= s <= p`, no spaces, signs,
or leading zeros — so reports and pinned schema files print every declared
type byte-identical to its declaration. Accepted value text is an optional
sign followed by plain base-10 digits with at most one decimal point
carrying at least one digit on each side (`1.` and `.5` reject): no exponent
notation, no thousands separators, no whitespace. Fewer fractional digits
than the scale rescale losslessly (`1.2` into `decimal(10,2)` stores
`1.20`); more fractional digits reject per record, because rounding never
happens in a load; a magnitude of more than `p` digits after scaling rejects
as overflow (exactly `p` digits fits). In JSONL, strings and integers fit,
while JSON floats reject per record — their exact digits were already lost
to IEEE parsing before the load saw them — and booleans, arrays, and objects
reject as shape misfits.

## `execution`

Governing ADRs:
[ADR-0046](../adr/0046-resolve-then-stream-connector-ports-with-chunked-sessions.md),
[ADR-0049](../adr/0049-configure-retry-as-per-unit-attempts-with-fixed-exponential-backoff.md),
[ADR-0052](../adr/0052-configure-parallelism-as-one-scalar-clamped-to-the-connector-limit.md).

How the load executes, as opposed to what it moves. Optional; `execution: {}`
equals an absent block, and every knob defaults independently.

```yaml
# Fragment — not a complete load definition.
execution:
  chunk_rows: 65536
  parallelism: 1
  retry:
    max_attempts: 3
    initial_delay_ms: 200
    max_delay_ms: 5000
```

### `execution.chunk_rows`

The chunk bound: each materialized chunk holds at most this many surviving
records, so peak memory stays flat regardless of source size. A nonzero
unsigned integer; the default is `65536`. Zero, negative, and non-integer
values fail YAML parsing (`invalid_load_definition_yaml`).

### `execution.parallelism`

The load parallelism: the bound on concurrent chunk writes within the load.
A nonzero unsigned integer.

- Absent, the effective parallelism is the destination connector's declared
  Connector Parallelism Limit for the load's mode — the conservative
  connector-specific default ([ADR-0023](../adr/0023-conservative-parallelism-defaults.md)).
  Every shipped connector declares `1`, so shipped loads run serial.
- Present, the effective parallelism is `min(configured, limit)`: the limit
  is a hard cap the configuration can never exceed — never an error to
  exceed — so one definition stays valid across destinations with different
  limits. `parallelism: 1` is the explicit serial form.

Peak memory scales with `parallelism × chunk_rows`; there is no cross-field
validation — the product is the definition author's to own. The load report
states the effective parallelism beside the connector limit.

### `execution.retry`

The retry policy: how many attempts each retry unit is allowed and how long
the load waits between them. All keys are optional and `retry: {}` equals an
absent block.

| Key | Default | Meaning |
| --- | --- | --- |
| `max_attempts` | `3` | Total attempts per retry unit, **including the first**. Nonzero; `max_attempts: 1` is the disable form, `0` fails YAML parsing. |
| `initial_delay_ms` | `200` | The wait before the second attempt, in milliseconds. |
| `max_delay_ms` | `5000` | The clamp on every wait, in milliseconds. |

Backoff is fixed exponential ×2 with a clamp and no jitter: the wait before
attempt `n` (for `n >= 2`) is `min(initial_delay_ms × 2^(n-2), max_delay_ms)`.
The clamp keeps the formula well-defined even when `max_delay_ms <
initial_delay_ms`, so no cross-field validation exists.

Each retry unit — the destination session open and each chunk write — gets
its own attempt budget, and only transient failures that provably left no
committed destination change are retried
([ADR-0048](../adr/0048-classify-transience-at-the-originator-and-retry-only-provably-uncommitted-units.md)).
The shipped local connectors classify no failure as transient, so the policy
changes no local behavior today; the load report echoes the policy and every
retried attempt.

## `reject_threshold`

Governing ADRs:
[ADR-0020](../adr/0020-default-reject-threshold-zero.md),
[ADR-0036](../adr/0036-write-rejected-records-jsonl-under-a-flat-reject-threshold.md).

The number of rejected records the load tolerates before failing. A
non-negative integer; the default is `0`: any rejected record fails the load
unless the definition explicitly allows more.

```yaml
# Fragment — not a complete load definition.
reject_threshold: 10
```

A rejected record is a source record that cannot be written without
violating the chosen schema or load rules: a malformed CSV or JSONL record
(`malformed_csv_record`, `malformed_jsonl_record`), a value misfit against a
pinned or overridden field (`type_coercion_failed`), or a null in a required
field (`missing_required_field`).

The threshold interacts with the rejected-records artifact
([ADR-0036](../adr/0036-write-rejected-records-jsonl-under-a-flat-reject-threshold.md)):

- Rejected records stream to `rejected-records.jsonl` in the artifact
  directory as the source is read — one JSON object per rejected record,
  carrying the source line number, a rejection code, the offending field
  when one is known, a message, and the record content the load could
  recover. When no records reject, no artifact is written.
- More rejected records than the threshold fails the load with
  `reject_threshold_exceeded` — before the pinned schema is persisted and
  before the destination is touched, because the whole input is evaluated
  before any chunk is written.
- A count at or below the threshold completes the load under its configured
  rules, writing only the surviving records.
- The artifact is written for failing and completing loads alike, and the
  load report states the rejected-record count and the artifact path.

### When every record is rejected

The gate is judged over the whole input, so a source whose every record
rejects is not a special case of it. What the load does after the gate
depends on whether a dataset schema can be resolved with no record left to
observe, and that differs by source format.

| Source | Threshold | Outcome |
| --- | --- | --- |
| CSV or JSONL | does not tolerate them | Fails with `reject_threshold_exceeded`, the message naming the full-input counts: `rejected 2 of 2 records, exceeding the reject threshold of 0`. |
| JSONL | tolerates them all | Fails with `malformed_jsonl`. JSONL carries no header, so a file with no parseable record offers no field names, and no schema can be inferred. |
| CSV | tolerates them all | **Succeeds.** The header resolves the dataset schema without needing a record, so the load completes and materializes that schema with no records in it. |

Both failures leave the destination untouched —
`destination_write.atomicity` is `not_applicable` and
`execution.record_format` is `not_started` — and both write the
rejected-records artifact, as any load that rejects a record does.

`schema_decision.mode` on those failures states what the load had resolved
when it stopped, which is a different question from which code fired. A CSV
threshold breach reports `inferred` (or `pinned`) and carries the `fields` it
had resolved, because the header resolves a schema whatever the records do —
and so does a JSONL threshold breach that had at least one parseable record
to infer from. `not_evaluated` belongs to the source that offered no shape at
all, an all-unparseable JSONL file, under either code.

### When a load completes with no surviving records

A load that completes with no survivor — the succeeding CSV case above, or
any other load whose surviving records are zero — enters the write phase like
any other: `execution.record_format` is `arrow_record_batch` and
`batch_count` is `1`, because an empty surviving run still yields one empty
chunk, so an empty dataset materializes its schema
([ADR-0046](../adr/0046-resolve-then-stream-connector-ports-with-chunked-sessions.md)).
[`load_mode`](#load_mode) decides what that does to the destination dataset:

| `load_mode` | Effect |
| --- | --- |
| `full_refresh` | The dataset is replaced by an empty one — a populated dataset loses every record it held. |
| `append` | No records are added, so the dataset keeps every record it already held — when the schema this load resolved still matches the destination, which a zero-survivor load cannot take for granted (see below). |

A load that completes this way is an ordinary write, reporting the posture
its [destination](#destination) and mode always report — `atomic` /
`transactional_replace` for a `duckdb` full refresh, and so on. The
materialization is destination-independent: `duckdb` creates or replaces the
table with no records in it, and `parquet` writes a real part file holding
zero records and the dataset's full field list — including on the append that
added nothing, which lands one more empty part beside the dataset's existing
ones.

A `full_refresh` whose delivery arrived entirely unloadable therefore empties
a dataset and exits `0`. The schema it leaves behind is the one this load
resolved, not the one the dataset had: a field no surviving record observed
infers as `utf8`, like any [all-null field](#schema), so an emptied dataset
can come back with wider types than the populated one had. A
[pinned schema](#schemapinned_path) holds the types — a CSV header still
carries every pinned field, so a zero-survivor load against a pin reports
`pinned` with `drift_status: none` rather than drift.

That widening is also what decides whether an `append` completes at all. An
append writes into a dataset that already exists, so a field this load
resolved as `utf8` where the destination holds a narrower type is a
mismatch: the load fails with `destination_write_failed` and
`batch_count: 0`, leaving the dataset untouched, rather than completing as
the no-op above. An append whose resolved schema still matches — because a
pin or an override declares the types, or because the destination's fields
are `utf8` anyway — adds nothing and leaves the dataset as it was.

Keeping `reject_threshold` at its default `0`, or well below one delivery's
record count, is what keeps a dataset that must never go empty from being
emptied this way.

## `artifacts`

Governing ADR:
[ADR-0015](../adr/0015-store-load-artifacts-under-data-spark-runs.md).

Where load artifacts are written.

```yaml
# Fragment — not a complete load definition.
artifacts:
  dir: ci-artifacts/data-spark
```

| Key | Required | Meaning |
| --- | --- | --- |
| `dir` | no | The root under which each load's artifact directory is created. Default: `.data-spark/runs`. |

Every load creates its own artifact directory, `<root>/<load-id>/`, and
writes `load-report.json` there — plus `rejected-records.jsonl` when any
records were rejected, and nothing else. A load that cannot create its
artifact directory or write its artifacts aborts with exit code `1` and no
report. The `--output-dir` command-line option is the one-off override: it
redirects the artifact root for that invocation and takes precedence over
`artifacts.dir`.

## Strictness: unknown keys are rejected

Governing ADR:
[ADR-0037](../adr/0037-reject-unknown-fields-in-versioned-yaml-contracts.md).

Load definitions parse strictly: a key the contract does not declare is
rejected, recursively through every nested block, under
`invalid_load_definition_yaml` with error text naming the rejected key. A
misspelled key or a future-looking key therefore fails the load before any
source reading or destination writing, instead of silently running with
behavior the author did not intend. The same rule governs pinned schema
files, under `invalid_pinned_schema`.

This complete definition misspells `chunk_rows` inside `execution`:

```yaml
version: 1
source:
  connector: local_file
  path: orders.jsonl
destination:
  connector: parquet
  path: orders-dataset
execution:
  chunk_size: 1000
```

Running it fails with exit code 1, and the load summary and report carry the
error:

```text
Error: failed to parse load definition: execution: unknown field `chunk_size`, expected one of `chunk_rows`, `parallelism`, `retry` at line 9 column 3
```
