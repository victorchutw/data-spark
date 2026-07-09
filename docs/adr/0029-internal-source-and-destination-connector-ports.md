---
status: accepted
---

# Internal Source and Destination Connector Ports

Data Spark's load path reaches every source and destination through two internal trait-object ports in `src/connector.rs`: `Source::read`, returning the materialized Arrow batch, the `schema_decision` shape, and the measured source bytes; and `Destination::write`, taking the batch and a parsed `LoadMode` and returning the bytes written plus the destination's own write facts (`atomicity` / `strategy`, ADR-0021). Two pure factories, `source_connector` and `destination_connector`, validate the connector name only and construct the port without I/O, so an unsupported connector or load mode fails before any source read or destination write (ADR-0019); the source format is validated at the top of `read` so its precedence stays after the connector checks for a doubly-invalid definition. `local_file` and `parquet` are the first two connectors behind these ports, and the orchestrator composes their results into the load report without branching on connector identity, so every future connector in the ADR-0005 / ADR-0027 matrix conforms to the same contract rather than adding inline guards and hand dispatch. The ports are named `Source` / `Destination`, not `Adapter` (`CONTEXT.md` lists Adapter under _Avoid_).

Extension points, deferred until a second connector needs them:

- **Decoder seam** — when an S3 object source (ADR-0027) reuses CSV/JSONL decoding, extract a `Decoder` from `LocalFileSource`; today `local_file` is the only file-like source, so a format port would be speculative.
- **Per-destination mode capability** — when append / merge loads (ADR-0008) land, destinations declare which load modes they support instead of every mode passing a single `LoadMode::parse` gate, because merge into Parquet and BigQuery differ.
- **`source_bytes` → `Option<u64>`** — when a networked source such as SQL Server (ADR-0005) has no file size to measure, source bytes stop being universally present; `local_file` always measures it, so `u64` is honest today.
