---
status: accepted
---

# Include Merge Loads in v1 After Full Refresh and Append

Data Spark will include merge loads in v1, but implement them after full refresh and append loads are working. This keeps the first execution path simple while still making the v1 product useful for BI datasets that need incremental updates; merge behavior varies across BigQuery, PostgreSQL, SQLite, and DuckDB, so it should be built on top of the simpler load modes rather than ahead of them.
