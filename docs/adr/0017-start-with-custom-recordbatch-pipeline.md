---
status: accepted
---

# Start With a Custom RecordBatch Pipeline

Data Spark v1 will start with a small custom Arrow `RecordBatch` pipeline for connector-to-destination movement and will not embed DataFusion in the mandatory execution path. DataFusion remains a strong candidate for a later optional transform or query layer, but keeping it out of the first execution path reduces dependency surface, planner behavior, and error-model complexity while the core data movement contract is still being proven.
