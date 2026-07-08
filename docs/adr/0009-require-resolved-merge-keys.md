---
status: accepted
---

# Require Resolved Merge Keys for Merge Loads

Data Spark will require every merge load to have a resolved merge key: users either provide the key fields directly, or explicitly request strict key discovery for a database table source. Key discovery may use a primary key, or a single non-null unique key when no primary key exists, but it must fail fast for ambiguous keys, nullable unique keys, views, custom SQL, joined sources, files, objects, and APIs so merge loads never silently guess how records match.
