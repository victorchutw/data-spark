---
status: accepted
---

# Write JSON Load Reports for Every Load

Data Spark will produce a machine-readable JSON load report for every load and reserve stdout for a human-readable load summary. The load report will include the load id, source, destination, load mode, schema decision, row counts, byte counts, rejected record counts, drift status, timings, exit status, and error summary so CLI loads can be inspected by humans and integrated into Airflow, Dagster, CI, or other automation without scraping console text.
