---
status: accepted
---

# Limit v1 to Structural Transforms

Data Spark v1 will support only structural transforms needed to move data into BI-ready datasets: field selection, field renaming, type coercion, JSON flattening, and basic timestamp and decimal handling. Analytical transforms such as arbitrary SQL, joins, aggregations, and business calculations are delegated to dbt, DuckDB, BigQuery SQL, or a future DataFusion transform layer so the first version remains a data movement tool rather than a modeling engine.
