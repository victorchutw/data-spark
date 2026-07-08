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

**Dataset**:
A named collection of records that is meaningful to a BI user.
_Avoid_: Table, file, stream

**Record**:
One logical item inside a dataset.
_Avoid_: Row, event, object

**Load**:
One attempt to move records from a source into a destination.
_Avoid_: Job, run, sync

**Load Mode**:
The rule that determines how a load changes the destination dataset.
_Avoid_: Strategy, write mode

**Full Refresh**:
A load mode that replaces the destination dataset with the source's current records.
_Avoid_: Rebuild, overwrite

**Append Load**:
A load mode that adds new records to the destination dataset without changing existing records.
_Avoid_: Insert-only, incremental append

**Merge Load**:
A load mode that updates matching records and inserts records that do not already exist.
_Avoid_: Upsert, incremental merge

**Rejected Record**:
A source record that cannot be written to the destination dataset without violating the chosen schema or load rules.
_Avoid_: Bad row, error row, dead letter

**Schema Drift**:
A difference between the source's observed record shape and the destination dataset's expected shape.
_Avoid_: Schema change, mismatch

**BI-Ready Dataset**:
A dataset that is typed, queryable, and stable enough for business intelligence tools.
_Avoid_: Report-ready data, analytics output
