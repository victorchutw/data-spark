---
status: accepted
---

# Use Conservative Parallelism Defaults

Data Spark v1 will default to one dataset per load and conservative connector-specific load parallelism, while allowing users to explicitly configure `parallelism`. Database sources will not use parallel full table scans by default, while destinations and object stores such as BigQuery staging or S3-compatible objects may use safe connector-specific upload and load parallelism where it does not change load semantics or overload operational systems.
