---
status: accepted
---

# Use Arrow RecordBatch as the Internal Data Format

Data Spark will use Arrow `RecordBatch` as the internal data exchange format for v1 connectors and execution steps. This gives the tool typed columns, schema-aware batches, bounded-memory streaming, and a direct path to Parquet, DuckDB, BigQuery staging, and DataFusion without building the core pipeline around row-by-row dynamic maps.
