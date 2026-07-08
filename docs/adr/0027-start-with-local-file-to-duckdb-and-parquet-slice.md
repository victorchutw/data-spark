---
status: accepted
---

# Start with a Local File to DuckDB and Parquet Slice

Data Spark's first tracer-bullet implementation slice will load local CSV and JSONL sources into DuckDB tables and Parquet directories using `version: 1` YAML load definitions, Arrow `RecordBatch` execution, schema inference and pinning, rejected records, load artifacts, JSON load reports, and human-readable load summaries. This deliberately defers networked sources, cloud destinations, merge loads, credential profiles, and scheduling until the core BI-ready load path is executable end to end.
