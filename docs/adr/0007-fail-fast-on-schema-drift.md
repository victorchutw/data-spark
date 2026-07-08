---
status: accepted
---

# Fail Fast on Schema Drift by Default

Data Spark will fail a load by default when schema drift is detected against a pinned schema, while offering an explicit drift policy for additive nullable fields. This protects BI-ready datasets from silent shape changes, but still gives users a deliberate path for low-risk source expansion when they want the destination schema to grow.
