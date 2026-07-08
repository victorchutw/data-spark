---
status: accepted
---

# Report Destination Write Atomicity

Data Spark v1 will use staging-then-commit for full refresh and merge loads when a destination can support an atomic commit, while marking connectors that cannot guarantee atomicity as `atomicity: best_effort` in the load report. Append loads are allowed to have partial writes by nature, so they must be traceable through the load id and load report rather than presented as atomic destination changes.
