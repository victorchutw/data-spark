# Data Spark

Data Spark is a portable data movement tool for turning operational data into BI-ready datasets.

## Language

**Data Movement**:
The act of copying data from a source into a destination while preserving enough structure and meaning for later analysis.
_Avoid_: ETL, sync, transfer

**Source**:
A system, file, object store, or API that data is read from.
_Avoid_: Input, upstream

**Destination**:
A system, file, object store, or analytical store that data is written to.
_Avoid_: Output, target, downstream

**Cloud Warehouse Destination**:
A managed analytical destination that stores BI-ready datasets outside the local machine.
_Avoid_: Cloud service, warehouse, hosted target

**Connector**:
A named capability for reading from a source or writing to a destination.
_Avoid_: Adapter, integration, driver

**Connection Reference**:
A name in a load definition that identifies which connection should be used without exposing credential values.
_Avoid_: Connection string, DSN, secret

**Connection Profile**:
A local saved connection configuration that a connection reference can resolve to.
_Avoid_: Config file, credential file

**Credential**:
Sensitive proof that allows a connector to access a source or destination.
_Avoid_: Password, token, secret

**Credential Reference**:
A non-sensitive pointer used to resolve a credential at load time.
_Avoid_: Secret value, embedded credential

**Dataset**:
A named collection of records that is meaningful to a BI user.
_Avoid_: Table, file, stream

**Dataset Schema**:
The expected fields and types for records in a dataset.
_Avoid_: Schema, table definition

**Structural Transform**:
A transformation that changes record shape or representation so a load can produce a BI-ready dataset.
_Avoid_: Data cleaning, mapping, light transform

**Flatten Mapping**:
A structural transform that extracts the values at declared source paths into added dataset fields, leaving existing fields unchanged.
_Avoid_: JSON flattening, unnest, explode

**Field Selection**:
A structural transform that keeps only named source fields of each record, in the declared order.
_Avoid_: Projection, column pruning, field filter

**Rename Mapping**:
A structural transform that maps source field names to dataset field names after field selection, leaving unmapped fields under their source names.
_Avoid_: Alias, column mapping, field mapping

**Source Path**:
The dot-notation address of a nested value inside a source field's structure.
_Avoid_: JSON path, pointer, selector

**Analytical Transform**:
A transformation that derives analytical meaning through joins, aggregations, calculations, or business logic.
_Avoid_: SQL transform, modeling, enrichment

**Record**:
One logical item inside a dataset.
_Avoid_: Row, event, object

**Chunk**:
A bounded run of consecutive records that a load reads, validates, writes, and commits as one unit; the load report's `batch_count` counts chunks, and Arrow `RecordBatch` is the exchange format of one chunk.
_Avoid_: Batch, micro-batch, page, block

**Load**:
One attempt to move records from a source into a destination.
_Avoid_: Job, run, sync

**Load Definition**:
A saved description of a repeatable load, including its source, destination, load mode, schema choices, and related rules.
_Avoid_: Job config, pipeline, workflow

**Load Definition Version**:
The declared contract version that determines how a load definition is interpreted.
_Avoid_: Config version, YAML version

**One-Off Load**:
A load started directly from command-line options without first saving a load definition.
_Avoid_: Ad hoc job, quick run

**Load Report**:
A machine-readable record of a load's outcome, diagnostics, and measurements.
_Avoid_: Run report, log, audit file

**Load Report Version**:
The declared contract version that determines how a load report is interpreted by readers.
_Avoid_: Report schema, output version

**Binary Version**:
The version of the Data Spark binary itself, printed by `--version` and echoed in every load report as provenance for the build that wrote it.
_Avoid_: App version, build number, release version

**Load Summary**:
A human-readable description of a load's outcome.
_Avoid_: Console report, status output

**Load Artifact**:
A file produced by a load for inspection, automation, or troubleshooting.
_Avoid_: Output file, runtime file

**Artifact Directory**:
The directory where load artifacts are written for a specific load.
_Avoid_: Output directory, run directory

**Load Mode**:
The rule that determines how a load changes the destination dataset.
_Avoid_: Strategy, write mode

**Load Validation**:
Checks that decide whether records can be written while honoring the dataset schema and load rules.
_Avoid_: Data quality, validation framework

**Reject Threshold**:
The configured amount of rejected records after which a load fails.
_Avoid_: Error limit, failure threshold

**Write Atomicity**:
The degree to which a destination write is committed as a single visible change.
_Avoid_: Transactionality, consistency guarantee

**Atomic Commit**:
A destination write that either becomes visible completely or does not change the destination dataset.
_Avoid_: Atomic write, transaction

**Best-Effort Write**:
A destination write where partial destination changes can occur if the load fails.
_Avoid_: Partial write, non-atomic write

**Transient Failure**:
A failure that is expected to be temporary and provably left no committed destination change, so the failed unit is safe to attempt again.
_Avoid_: Temporary error, retryable error

**Terminal Failure**:
A failure that must not be automatically retried, including every failure whose commit outcome is uncertain.
_Avoid_: Permanent error, fatal error

**Commit Boundary**:
The point after which a destination write may have become visible.
_Avoid_: Commit phase, point of no return

**Retry Attempt**:
A repeated attempt of a failed operation within the same load.
_Avoid_: Retry, rerun

**Retry Unit**:
The bounded operation a load re-attempts as a whole after a transient failure, with the same input.
_Avoid_: Retryable operation, work item

**Retry Policy**:
The configured rule for how many attempts a retry unit is allowed and how long a load waits between them.
_Avoid_: Retry config, backoff settings

**Load Parallelism**:
The amount of concurrent work allowed within a load.
_Avoid_: Concurrency, workers

**Connector Parallelism Limit**:
The maximum load parallelism a destination connector declares per load mode,
serving both as the hard cap on configured parallelism and as the effective
default when none is configured.
_Avoid_: Worker limit, concurrency cap

**External Orchestrator**:
A system outside Data Spark that decides when load definitions should run.
_Avoid_: Scheduler, workflow engine

**Built-in Scheduler**:
A Data Spark component that would decide when load definitions should run.
_Avoid_: Internal orchestrator, cron

**Full Refresh**:
A load mode that replaces the destination dataset with the source's current records.
_Avoid_: Rebuild, overwrite

**Append Load**:
A load mode that adds new records to the destination dataset without changing existing records.
_Avoid_: Insert-only, incremental append

**Merge Load**:
A load mode that updates matching records and inserts records that do not already exist.
_Avoid_: Upsert, incremental merge

**Merge Key**:
The field or fields used to decide whether a source record matches an existing destination record during a merge load.
_Avoid_: Primary key, unique key, id

**Resolved Merge Key**:
A merge key that has been explicitly provided by the user or explicitly discovered from source metadata.
_Avoid_: Detected key, guessed key

**Key Discovery**:
The act of deriving a merge key from source metadata after the user explicitly asks for it.
_Avoid_: Key inference, automatic key selection

**Rejected Record**:
A source record that cannot be written to the destination dataset without violating the chosen schema or load rules.
_Avoid_: Bad row, error row, dead letter

**Surviving Record**:
A source record that can be written to the destination dataset while honoring the chosen schema and load rules.
_Avoid_: Good row, valid row, kept record

**Staging Object**:
A temporary data object prepared so a destination can load records through its native batch loading path.
_Avoid_: Temp file, intermediate file, upload

**Schema Directive**:
The instruction a load definition gives about how a load arrives at its dataset schema: infer it, infer it and persist it as a new pinned schema, or validate observed records against an existing pinned schema.
_Avoid_: Schema mode, schema config, schema strategy

**Schema Decision**:
The record of which dataset schema a load resolved, how it reached it, and what schema drift it found, reported as the load report's `schema_decision`.
_Avoid_: Schema result, schema outcome, resolved schema

**Inferred Schema**:
A dataset schema produced from observed source records.
_Avoid_: Auto schema, detected schema

**Declared Type**:
A dataset field type that enters a schema only through explicit declaration, never through inference.
_Avoid_: Logical type, manual type, special type

**Wall-Clock Timestamp**:
A datetime that states what a clock read, without identifying a timezone or an absolute moment.
_Avoid_: Naive datetime, local timestamp, timestamp without time zone

**Instant Timestamp**:
A timestamp that identifies one absolute moment, normalized to UTC.
_Avoid_: Zoned datetime, epoch time, absolute timestamp

**Decimal**:
An exact numeric value with a declared precision and scale that is never rounded during a load.
_Avoid_: Float, numeric, money

**Schema Override**:
User-provided changes to an inferred schema before records are loaded.
_Avoid_: Manual schema, schema mapping

**Override Conflict**:
A contradiction between a schema override and the pinned schema field it names.
_Avoid_: Schema mismatch, pin mismatch

**Pinned Schema**:
A dataset schema that is reused across loads to keep a BI-ready dataset stable.
_Avoid_: Fixed schema, locked schema

**Schema Drift**:
A difference between the source's observed record shape and the destination dataset's expected shape.
_Avoid_: Schema change, mismatch

**Drift Policy**:
The rule that decides whether a load may continue when schema drift is detected.
_Avoid_: Drift handling, schema behavior

**Additive Schema Drift**:
Schema drift where the source has additional fields that can be added without changing existing fields.
_Avoid_: Safe drift, new columns

**Drift Status**:
The outcome of comparing a load's observed record shape against its pinned schema: `not_applicable` when no comparison ran, `none` when the shapes matched, `additive_fields_added` when the drift policy admitted new fields, or `failed_on_drift` when it did not.
_Avoid_: Drift result, drift state, drift outcome

**BI-Ready Dataset**:
A dataset that is typed, queryable, and stable enough for business intelligence tools.
_Avoid_: Report-ready data, analytics output
