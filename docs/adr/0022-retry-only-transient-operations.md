---
status: accepted
---

# Retry Only Transient Operations

Data Spark v1 will automatically retry only clearly transient operations such as network timeouts, 429 or 5xx responses, and BigQuery job polling. It will not automatically retry operations after the destination commit boundary when write atomicity is unclear, and every retry attempt must be recorded in the load report so users can diagnose delays and avoid hidden duplicate writes.
