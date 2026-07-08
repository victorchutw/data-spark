---
status: accepted
---

# Default Reject Threshold to Zero

Data Spark v1 will default the reject threshold to `0`, so any rejected record fails the load unless the user explicitly configures a higher threshold. This makes BI pipelines fail loudly on unexpected bad records while still allowing deliberate partial acceptance for exploratory or tolerant loads.
