# Changelog

All notable changes to Data Spark are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Scope of these entries

An entry describes something a user of Data Spark can observe: a feature, a
behavior change, a fix, a change to one of the versioned contracts, or a
change to what the project publishes about itself — its documentation, its
examples, and its license. Behavior-preserving refactors and repository
automation — CI and release workflow changes — are deliberately absent.

Entries marked **[contract]** add or change a *key* in a versioned contract:
the [load definition YAML](docs/reference/load-definition.md), the
[load report JSON](docs/reference/load-report.md), the pinned schema file, or
the rejected-records artifact. A new failure code is a new *value* of
`error_summary.code`, which a version 1 reader is already required to
tolerate, so failure codes are named in the prose without the mark.

Every key added so far has been additive, which is why no contract version has
been bumped past `1`. One change tightened an existing contract rather than
extending it: rejecting unknown keys ([#54]) fails load definitions that older
binaries accepted.

Each entry links the pull request that made the change. Two entries predate
this repository's use of pull requests, and link their commit instead.

## [Unreleased]

### Fixed

- `CONTEXT.md`'s Merge Load definition lagged the decided merge semantics
  ([ADR-0057](docs/adr/0057-land-merge-loads-on-duckdb-only-and-decline-elsewhere.md)):
  it said matched records are updated, where a merge replaces them whole,
  and it omitted that a merge never deletes, the boundary that decision
  defines the mode by. The entry now carries both and settles in place
  that prose may gloss the behavior as a keyed upsert, never as the mode's
  name — the reading that every shipped merge artifact already uses.
  ([#104])

## [0.4.0] - 2026-07-31

A third load mode: `load_mode: merge` performs a keyed upsert into a DuckDB
table as one staged terminal transaction — matched destination records are
replaced whole, unmatched source records insert, and merge never deletes.
The reject-threshold correction rides along: the zero-threshold promise now
covers exactly the loads it protects, those emptied by rejection.

### Added

- Merge loads: `load_mode: merge` performs a keyed upsert into a DuckDB
  table — destination records whose merge key tuple matches a surviving
  source record are replaced whole, unmatched source records insert, and
  records absent from the source stay; merge never deletes
  ([ADR-0057](docs/adr/0057-land-merge-loads-on-duckdb-only-and-decline-elsewhere.md)).
  **[contract]** The load definition gains an optional top-level `merge`
  block (`keys: [field, ...]`, required exactly when the mode is `merge`),
  and the load report gains a top-level `merge` echo (`null` on non-merge
  loads) plus, on success, `destination_write.merge` with the
  `updated`/`inserted` partition, whose sum always equals
  `row_counts.written`; both contract versions stay `1`. The merge executes
  as a staged terminal transaction — `atomicity: "atomic"`,
  `strategy: "transactional_merge"`, nothing visible before the single
  commit — and requires the destination table to exist: bootstrap with a
  `full_refresh` load first
  ([ADR-0059](docs/adr/0059-execute-duckdb-merge-as-a-staged-terminal-transaction.md)).
  Keys name dataset fields (rename targets and flatten outputs qualify); a
  key naming no dataset field fails as `unknown_merge_key_field`, a record
  holding null in any key field is rejected as `null_merge_key` under the
  ordinary `reject_threshold`, and surviving records sharing one key tuple
  fail the load as `duplicate_merge_keys` — never first-or-last-wins
  ([ADR-0058](docs/adr/0058-fail-merge-loads-on-duplicate-merge-keys.md)).
  Cross-validation adds `missing_merge_keys` and `invalid_merge_config`;
  the `parquet` destination declines the mode as
  `unsupported_load_mode_for_destination`, and `unsupported_load_mode` now
  means exactly "the mode string is unknown". Documented in a new
  [merge loads guide](docs/guides/merge-loads.md), a runnable
  [csv-to-duckdb-merge](examples/csv-to-duckdb-merge/) example, and both
  reference pages. ([#100])

### Fixed

- Two pages promised that keeping `reject_threshold` at `0` protects "any
  dataset that must never go empty" — a guarantee the threshold does not
  provide. It guards only the rejection route: an empty delivery, a
  header-only CSV, rejects nothing, so it completes at any threshold and a
  `full_refresh` mirrors it as an empty dataset. Both pages now scope the
  promise to "emptied by rejection", document the empty-delivery route, and
  carry the decision that no guard is added, because a full refresh mirrors
  the source by definition and an abnormal empty delivery is guarded
  upstream
  ([ADR-0056](docs/adr/0056-mirror-the-source-on-zero-survivor-full-refreshes.md)).
  The every-record-rejected table also claimed a JSONL source whose
  rejections are all tolerated fails; verified against the shipped binary,
  that holds only for an all-unparseable file — records that parse but
  reject offer their field names, and the load completes empty like CSV.
  `CONTEXT.md` gains the Surviving Record term the docs, the code, and the
  decision records already used. ([#98])

## [0.3.0] - 2026-07-30

The binary can now say which release it is: `--version` prints it, and every
load report names the binary that wrote it. The documentation corrections
ride along — the README's feature list and two decision records no longer
contradict the shipped code.

### Added

- `data-spark --version` (and `-V`) prints the binary's version, and
  `data-spark --help` now describes the `load` subcommand. **[contract]**
  Every load report also names the binary that wrote it in a new top-level
  `binary_version` string, on the success and the failure path alike, so an
  archived report can be attributed to the release that produced it;
  `report_version` stays `1`
  ([ADR-0055](docs/adr/0055-surface-the-binary-version-as-version-output-and-top-level-report-provenance.md)).
  ([#95])

### Fixed

- The README's feature list presented retry and parallelism as active. No
  shipped connector classifies a failure as transient, and every shipped
  destination declares a parallelism limit of `1`, so neither does anything on
  a local CSV or JSONL load into DuckDB or Parquet; both bullets now carry the
  shipped-matrix caveat the guides and the load definition reference already
  carried. Two decision records are corrected in place alongside it: ADR-0044
  stated the decimal overflow bound off by one, when exactly `p` digits is
  what `decimal(p,s)` holds, and ADR-0031 still deferred a question that
  ADR-0035 and ADR-0038 had since answered. The behavior was always this;
  only the prose was wrong. ([#94])

## [0.2.1] - 2026-07-30

Documentation, license, and examples. The binary behaves exactly as `0.2.0`
does; what changes is what the project publishes about itself — the terms it
is licensed under, a front page and a documentation index, a key-by-key
reference for the load definition YAML and the load report JSON, ten runnable
examples, a guide per feature area, and this changelog.

### Added

- Dual MIT OR Apache-2.0 licensing and Cargo package metadata, so the source
  terms and the crate's identity are explicit
  ([ADR-0054](docs/adr/0054-license-the-project-under-mit-or-apache-2.0.md)).
  ([#79])
- `README.md` front page: what Data Spark does, the feature summary for the
  current release, install instructions for the single binary, and a
  copy-paste quickstart. ([#80])
- `docs/reference/load-definition.md` — the load definition YAML v1 reference:
  every key, its default, its validation order, and the failure code an
  invalid value produces. ([#81])
- `docs/reference/load-report.md` — the load report JSON v1 reference: every
  field, when it is present, the report postures, and what a version 1 reader
  may rely on. ([#82])
- `examples/` — ten runnable load definitions with their sources, covering
  both formats, both destinations, both load modes, pinning, drift, rejected
  records, transforms, declared types, and chunked execution. A CI test runs
  each one against the built binary. ([#83])
- `docs/guides/` — task-oriented guides for schema pinning, rejected records,
  declared types, and execution tuning. ([#84])
- This changelog, backfilled from the merged pull request history, plus a
  changelog step in the release flow in `docs/agents/gitops.md`. ([#86])
- `docs/README.md` — a documentation index mapping each documentation tree to
  the audience it serves, linked from the README so the front page reaches all
  of them. `CONTEXT.md` gains the schema directive, schema decision, and drift
  status terms that schema pinning left undefined. ([#87])
- The load definition reference documents what a load does when every record is
  rejected — `reject_threshold_exceeded` under a threshold that does not
  tolerate the rejections, `malformed_jsonl` for a JSONL source when it does,
  and a succeeding load for CSV, whose header resolves a schema without a
  record — and what a load that completes with no surviving records does to
  each load mode's destination. The load report reference's `malformed_jsonl`
  failure code points at that interaction. ([#88])

### Fixed

- The load report reference described `schema_decision.mode: not_evaluated` as
  a load that failed before any schema work started. A CSV load that breaches
  `reject_threshold` reports `inferred` — or `pinned` under a pin — instead,
  carrying the fields its header resolved, so the reference now states which
  failures reach which mode. The behavior was always this; only the reference
  was wrong. ([#88])

## [0.2.0] - 2026-07-22

The load definition gains its declarative surface: structural transforms,
schema overrides, declared types, and an `execution` block. Loads now stream
through bounded-memory chunks instead of materializing the whole source.

### Added

- `schema.overrides` — per-field overrides that replace an inferred field's
  type, nullability, or both. An override rewrites inference wherever
  inference decides a field's shape, including the pin bootstrap, but never
  overrides an existing pin; contradicting the pin fails the load with
  `schema_override_conflict`. **[contract]** Adds the `schema.overrides`
  definition key, the `schema_decision.overrides` echo, and the
  `schema_decision.conflict` detail. ([#60])
- `transform.select` and `transform.rename` — field selection in a fixed
  order and simultaneous renaming, resolved before overrides, pin comparison,
  and per-record validation, so everything downstream speaks the transformed
  dataset shape. **[contract]** Adds the `transform` definition block, the
  `schema_decision.transform` echo, and a nullable `source_field` on
  rejected-record lines. ([#61])
- `transform.flatten` — a map of dot-notation source path to dataset field
  name that extracts nested JSONL values into added fields, evaluated before
  selection and renaming. Extraction never rejects a record: a missing or
  non-object step yields null, and an object or array leaf yields compact JSON
  text. **[contract]** Adds the `transform.flatten` definition key and its
  report echo. ([#62])
- Declared-only field types `timestamp` (wall clock), `timestamptz` (instant,
  normalized to UTC), and `decimal(p,s)` (exact, never rounded). They enter a
  schema only through an override or a pin, never through inference, and are
  parsed strictly per value. **[contract]** Extends the field type vocabulary
  that overrides, pinned schemas, and reports share. ([#63])
- Chunked execution — a load resolves its schema in one streaming pass over
  the source, then materializes and writes chunks of at most
  `execution.chunk_rows` records (default `65536`), so peak memory is flat in
  source size. Full refresh still commits once at the end; append commits per
  chunk, so a failure keeps exactly the committed prefix. **[contract]** Adds
  the `execution` definition block with `chunk_rows`, and the
  `execution.chunk_rows` report field. ([#64])
- Retry for write-phase failures — `execution.retry` sets `max_attempts`
  (default `3`; `1` disables), `initial_delay_ms` (default `200`), and
  `max_delay_ms` (default `5000`), applied per retry unit with clamped
  exponential backoff. A failure is retried only when its originator declares
  it transient and the failed attempt provably committed nothing; no shipped
  connector classifies any failure as transient, so the engine ships idle.
  **[contract]** Adds the `execution.retry` definition block and the
  `execution.retry` report object with its per-failed-attempt log. ([#65])
- Parallel chunk writes — `execution.parallelism` bounds how many chunk
  writes run concurrently, clamped to the limit each destination connector
  declares for the load mode. Source reads stay sequential, and the effective
  value is `1` for every shipped connector. **[contract]** Adds the
  `execution.parallelism` definition key and the `execution.parallelism` and
  `execution.connector_parallelism_limit` report fields. ([#66])

### Changed

- `execution.batch_count` counts the chunks committed to the destination, as it
  always did, but a load can now commit more than one, so the value varies with
  `chunk_rows` instead of always being `1`. Write-phase failures report the
  committed chunk count and an honest `row_counts.written`; pre-write failures
  keep their `not_started` posture unchanged. ([#64])
- A source that changes between the resolution pass and the write pass fails
  the load with the new `source_changed_during_load` code instead of writing
  records the resolved plan never saw. ([#64])

## [0.1.0] - 2026-07-16

The first release: local CSV and JSONL files load into Parquet datasets and
DuckDB databases, in full refresh or append, with schema inference, schema
pinning, rejected records, and a JSON load report for every load.

### Added

- The `data-spark load [--output-dir <dir>] <definition>` command, `version: 1`
  YAML load definitions, and a `report_version: 1` JSON load report written
  into a per-load artifact directory for every load, successful or failed.
  **[contract]** Establishes the load definition and load report contracts.
  ([`0e4b7c9`], issue [#2])
- Local CSV source and Parquet destination for `load_mode: full_refresh`:
  schema inference from the source records, an Arrow `RecordBatch` pipeline,
  a staged-then-replaced Parquet dataset, and a human-readable load summary on
  stdout. **[contract]** Adds the `schema_decision`, `row_counts`,
  `byte_counts`, `destination_write`, and `execution` report fields.
  ([`a31287d`], issue [#3])
- Local JSONL source: one record per line, blank lines skipped, fields in
  first-seen key order over the full input, and JSON's native types honored — so
  a JSON string such as `"01234"` stays text instead of being retyped as a
  number. ([#19])
- DuckDB destination connector (`connector: duckdb`), with the engine
  statically bundled into the single binary. Full refresh replaces the table
  in one transactional statement, reported as `atomic` /
  `transactional_replace`. **[contract]** The top-level `dataset` key names
  the table and is required for `duckdb` (new `missing_dataset` failure), the
  report echoes `dataset`, and `byte_counts.destination` may be `null` for a
  destination with no honest byte count. ([#33]; the JSONL leg pinned by
  [#41])
- Schema pinning and a drift policy — `schema.pinned_path` names a
  `version: 1` pinned schema file that the first load writes and later loads
  validate against, comparing by field name with lattice-widening
  compatibility. Drift fails the load with `schema_drift` before any write;
  `schema.drift_policy: allow_additive_nullable` admits purely additive drift
  and rewrites the pin. **[contract]** Adds the `schema.pinned_path` and
  `schema.drift_policy` definition keys, the pinned schema file contract, and
  the `drift_status`, `pinned_schema_path`, `pinned_schema_persisted`,
  `added_fields`, and `drift` fields under `schema_decision`. ([#42])
- Rejected records — a record that cannot be written without violating the
  dataset schema is rejected instead of failing the load, and the top-level
  `reject_threshold` (default `0`) decides how many rejections the load
  tolerates before failing with `reject_threshold_exceeded`. **[contract]**
  Adds the `reject_threshold` definition key, the `rejected_records` report
  object, `row_counts.rejected`, and the `rejected-records.jsonl` artifact.
  ([#43])
- `load_mode: append` for all four source/destination combinations, aligning
  fields by name and validating the existing destination schema before
  writing. **[contract]** Adds the `append` load mode and its
  `destination_write` facts: `best_effort` with `staged_part_append`
  (Parquet) or `insert` (DuckDB). ([#44])
- `artifacts.dir` sets the root that each load's artifact directory is created
  under, with `--output-dir` taking precedence over it. **[contract]** Adds
  the `artifacts` definition block. ([#45])

### Changed

- Default inference reads non-finite numeric text — `inf`, `infinity`, `NaN`
  in any casing, and overflow such as `1e400` — as text rather than
  `float64`, so a numeric-looking column containing one falls to text and
  stores every value verbatim
  ([ADR-0031](docs/adr/0031-infer-non-finite-numeric-text-as-text.md)).
  ([#39])
- Default inference reads zero-padded numeric text such as `00501` as text
  rather than `int64`, so identifier-like columns keep their leading zeros
  ([ADR-0032](docs/adr/0032-infer-zero-padded-numeric-text-as-text.md)).
  ([#40])
- Per-record parse problems no longer fail the whole load as `malformed_csv`
  or `malformed_jsonl`; they become rejected records under
  `reject_threshold`. Header-level failures and a source with no parseable
  record keep the old codes. ([#43])
- An unknown key in a load definition or a pinned schema file now fails the
  load, naming the rejected key, instead of being silently ignored
  ([ADR-0037](docs/adr/0037-reject-unknown-fields-in-versioned-yaml-contracts.md)).
  **[contract]** ([#54])

### Fixed

- A JSONL line that is not valid UTF-8 now rejects that record instead of
  failing the whole read, so the records around it still load. ([#45])
- A source failure that happens before any schema is decided now reports the
  source and rejection counts already established, and applies
  `reject_threshold` to them, instead of falling back to the `not_started`
  posture. ([#45])

## Prerelease validation tags

`v0.1.0-alpha.1` and `v0.1.1-alpha.1` were cut from `main` only to exercise the
tag-driven release pipeline end to end, not to ship a milestone, so what each
one carried is listed under the stable release that followed — `0.1.0` and
`0.2.0` respectively.

[Unreleased]: https://github.com/victorchutw/data-spark/compare/v0.4.0...main
[0.4.0]: https://github.com/victorchutw/data-spark/releases/tag/v0.4.0
[0.3.0]: https://github.com/victorchutw/data-spark/releases/tag/v0.3.0
[0.2.1]: https://github.com/victorchutw/data-spark/releases/tag/v0.2.1
[0.2.0]: https://github.com/victorchutw/data-spark/releases/tag/v0.2.0
[0.1.0]: https://github.com/victorchutw/data-spark/releases/tag/v0.1.0
[`0e4b7c9`]: https://github.com/victorchutw/data-spark/commit/0e4b7c971ee8d82a9114c22ff9d3175c1447c49f
[`a31287d`]: https://github.com/victorchutw/data-spark/commit/a31287dc3bacfd6b2fd3b6d8dec0e51eb0450299
[#2]: https://github.com/victorchutw/data-spark/issues/2
[#3]: https://github.com/victorchutw/data-spark/issues/3
[#19]: https://github.com/victorchutw/data-spark/pull/19
[#33]: https://github.com/victorchutw/data-spark/pull/33
[#39]: https://github.com/victorchutw/data-spark/pull/39
[#40]: https://github.com/victorchutw/data-spark/pull/40
[#41]: https://github.com/victorchutw/data-spark/pull/41
[#42]: https://github.com/victorchutw/data-spark/pull/42
[#43]: https://github.com/victorchutw/data-spark/pull/43
[#44]: https://github.com/victorchutw/data-spark/pull/44
[#45]: https://github.com/victorchutw/data-spark/pull/45
[#54]: https://github.com/victorchutw/data-spark/pull/54
[#60]: https://github.com/victorchutw/data-spark/pull/60
[#61]: https://github.com/victorchutw/data-spark/pull/61
[#62]: https://github.com/victorchutw/data-spark/pull/62
[#63]: https://github.com/victorchutw/data-spark/pull/63
[#64]: https://github.com/victorchutw/data-spark/pull/64
[#65]: https://github.com/victorchutw/data-spark/pull/65
[#66]: https://github.com/victorchutw/data-spark/pull/66
[#79]: https://github.com/victorchutw/data-spark/pull/79
[#80]: https://github.com/victorchutw/data-spark/pull/80
[#81]: https://github.com/victorchutw/data-spark/pull/81
[#82]: https://github.com/victorchutw/data-spark/pull/82
[#83]: https://github.com/victorchutw/data-spark/pull/83
[#84]: https://github.com/victorchutw/data-spark/pull/84
[#86]: https://github.com/victorchutw/data-spark/pull/86
[#87]: https://github.com/victorchutw/data-spark/pull/87
[#88]: https://github.com/victorchutw/data-spark/pull/88
[#94]: https://github.com/victorchutw/data-spark/pull/94
[#95]: https://github.com/victorchutw/data-spark/pull/95
[#98]: https://github.com/victorchutw/data-spark/pull/98
[#100]: https://github.com/victorchutw/data-spark/pull/100
[#104]: https://github.com/victorchutw/data-spark/pull/104
