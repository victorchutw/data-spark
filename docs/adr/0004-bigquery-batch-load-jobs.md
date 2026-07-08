---
status: accepted
---

# Use BigQuery Batch Load Jobs for v1 Writes

Data Spark will write to BigQuery in v1 by staging Parquet or newline-delimited JSON and then running a BigQuery batch load job, rather than writing directly through the Storage Write API. This keeps the BigQuery connector aligned with the batch-first product boundary, gives failures and retries a concrete staged artifact to inspect, and postpones streaming-specific delivery semantics until near-real-time loading becomes a product requirement.
