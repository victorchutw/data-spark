---
status: accepted
---

# Write Rejected Records as a JSONL Artifact Under a Flat Reject Threshold

Data Spark will capture rejected records — parse failures, type misfits against a pinned schema, and null values in non-nullable pinned fields — as a `rejected-records.jsonl` load artifact (ADR-0015), one JSON object per rejected record carrying the source line number, a rejection code, the offending field when one is known (its dataset name, with the source field named alongside when a rename mapping changed it, ADR-0039), a human-readable message, and the record content the load could recover. The load definition configures tolerance as a top-level `reject_threshold` record count that defaults to `0` (ADR-0020); a load whose rejected-record count exceeds the threshold fails with `reject_threshold_exceeded` before the pinned schema is persisted or the destination is touched, while a load at or below the threshold completes under its configured load rules, writing only the surviving records — and the artifact is written for failing and completing loads alike. The load report states the rejected-record count and the artifact path, and the load summary repeats them in human-readable form (ADR-0014).
