---
status: accepted
---

# Validate Pinned Schemas by Field Name With Lattice-Widening Type Compatibility

Data Spark will compare observed source records against a pinned schema by field name rather than by position, and will accept a column when its observed type widens to the pinned type under the same lattice that schema inference uses: an all-null column matches any pinned type, integers widen to floats, and anything widens to text. Matching loads materialize records in the pinned schema's field order so the destination dataset keeps a stable column order even when the source reorders fields. A missing field, an incompatible type, or an extra field not permitted by the drift policy fails the load with `schema_drift` before per-value coercion and before destination writing, which keeps schema coercion failures unreachable until rejected-record handling introduces per-record semantics.
