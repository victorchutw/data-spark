---
status: accepted
---

# Defer Built-in Scheduling and Orchestration

Data Spark v1 will not include a built-in scheduler or orchestration service. External orchestrators such as cron, CI, Airflow, and Dagster will invoke the CLI using YAML load definitions, exit codes, and JSON load reports; this keeps the v1 product a portable data movement binary instead of introducing service state, locking, history retention, APIs, and UI concerns before the load contract is proven.
