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

**Analytical Transform**:
A transformation that derives analytical meaning through joins, aggregations, calculations, or business logic.
_Avoid_: SQL transform, modeling, enrichment

**Record**:
One logical item inside a dataset.
_Avoid_: Row, event, object

**Load**:
One attempt to move records from a source into a destination.
_Avoid_: Job, run, sync

**Load Definition**:
A saved description of a repeatable load, including its source, destination, load mode, schema choices, and related rules.
_Avoid_: Job config, pipeline, workflow

**One-Off Load**:
A load started directly from command-line options without first saving a load definition.
_Avoid_: Ad hoc job, quick run

**Load Report**:
A machine-readable record of a load's outcome, diagnostics, and measurements.
_Avoid_: Run report, log, audit file

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

**Staging Object**:
A temporary data object prepared so a destination can load records through its native batch loading path.
_Avoid_: Temp file, intermediate file, upload

**Inferred Schema**:
A dataset schema produced from observed source records.
_Avoid_: Auto schema, detected schema

**Schema Override**:
User-provided changes to an inferred schema before records are loaded.
_Avoid_: Manual schema, schema mapping

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

**BI-Ready Dataset**:
A dataset that is typed, queryable, and stable enough for business intelligence tools.
_Avoid_: Report-ready data, analytics output
