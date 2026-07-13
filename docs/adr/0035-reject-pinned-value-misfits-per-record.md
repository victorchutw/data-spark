---
status: accepted
---

# Reject Pinned Schema Value Misfits Per Record

Data Spark will check value fit against a pinned schema per record, using the same inference lattice as ADR-0034: a cell fits a pinned field iff its observed type widens to the pinned type, a null or absent cell fits any nullable field, and a null or absent cell in a `nullable: false` pinned field violates the record. A record that misfits a pinned type or leaves a non-nullable field null becomes a rejected record under the reject threshold (ADR-0020) instead of failing the load, which makes `nullable: false` representable in pinned schema files and finally gives field presence and required fields per-record semantics. This supersedes the incompatible-type clause of ADR-0034: a column-wide type change now surfaces as that column's records being rejected — non-finite numeric text under a pinned `float64` (the question ADR-0031 deferred) is likewise a rejected record, not a stored non-finite value. Source shape problems stay batch-level `schema_drift` governed by the drift policy — a pinned field absent from every record, an added field the policy does not allow, or duplicate source field names — and shape drift is decided before per-record validation, so a drift-failed load reports drift rather than rejections.
