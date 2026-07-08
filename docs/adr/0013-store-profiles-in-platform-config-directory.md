---
status: accepted
---

# Store Connection Profiles in the Platform Config Directory

Data Spark will store local connection profiles in the platform config directory, using `~/.config/data-spark/connections.yaml` as the Linux and macOS default for v1, and will allow `DATA_SPARK_CONFIG_DIR` to override the directory for CI and portable setups. This keeps profiles outside project repositories by default while leaving an explicit escape hatch for controlled environments.
