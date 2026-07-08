---
status: accepted
---

# Store Load Artifacts Under Data Spark Runs

Data Spark will write each load's artifacts to `.data-spark/runs/{load_id}/` by default, including `load-report.json` and any `rejected-records.*` files. Users can redirect the artifact directory with `--output-dir` for one-off loads or `artifacts.dir` in YAML load definitions so CI systems and repeatable loads can collect artifacts explicitly.
