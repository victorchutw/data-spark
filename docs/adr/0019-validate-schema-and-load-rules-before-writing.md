---
status: accepted
---

# Validate Schema and Load Rules Before Writing

Data Spark v1 will validate schema and load rules before writing records to a destination, including field presence, type coercion, non-null merge keys, and required fields. Invalid records become rejected records, and a reject threshold can fail the load; broader data quality checks such as statistical assertions, uniqueness beyond load rules, and business validations are out of scope for v1.
