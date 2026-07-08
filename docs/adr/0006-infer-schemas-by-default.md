---
status: accepted
---

# Infer Schemas by Default With Overrides and Pinning

Data Spark will infer dataset schemas by default so first-time loads stay fast, while allowing users to override inferred fields and pin a schema for repeatable BI-ready loads. This balances CLI convenience with stable downstream BI behavior; requiring every schema up front would slow exploration, while inference without pinning would make repeat loads too fragile.
