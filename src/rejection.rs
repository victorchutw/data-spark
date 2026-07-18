//! Rejected-record deep module: the single home for what a rejected record is
//! and how the `rejected-records.jsonl` artifact renders it.
//!
//! A rejected record (ADR-0019) is a source record that cannot be written to
//! the destination dataset without violating the chosen schema or load rules:
//! a record that fails to parse ([`MALFORMED_CSV_RECORD`],
//! [`MALFORMED_JSONL_RECORD`]), a value that does not fit its pinned type
//! ([`TYPE_COERCION_FAILED`]), or a null in a non-nullable pinned field
//! ([`MISSING_REQUIRED_FIELD`]) — per-record semantics per ADR-0035. Producers
//! (the source readers and the schema module) construct [`RejectedRecord`]s;
//! the orchestrator renders them with [`artifact_jsonl`] and owns the file
//! write, mirroring how pinned schema writes stay with the caller. The
//! artifact contract (ADR-0036): one JSON object per rejected record, in
//! source-line order, carrying the line, a rejection code, the offending
//! field when one is known — its dataset name, with the source named
//! alongside when a rename mapping changed it (ADR-0039) or, for a flatten
//! output, as the declared source path (ADR-0041) — a human-readable
//! message, and the record content the load could recover.

use serde_json::{json, Value};

/// The rejected-records artifact filename inside a load's artifact directory
/// (ADR-0015 names the `rejected-records.*` family).
pub(crate) const REJECTED_RECORDS_FILENAME: &str = "rejected-records.jsonl";

/// A CSV record that failed to parse as a record of the header's fields.
pub(crate) const MALFORMED_CSV_RECORD: &str = "malformed_csv_record";
/// A JSONL line that is not a JSON object.
pub(crate) const MALFORMED_JSONL_RECORD: &str = "malformed_jsonl_record";
/// A value whose observed type does not widen to its pinned field type.
pub(crate) const TYPE_COERCION_FAILED: &str = "type_coercion_failed";
/// A null or absent value in a `nullable: false` pinned field.
pub(crate) const MISSING_REQUIRED_FIELD: &str = "missing_required_field";

/// One rejected record: the source context and error information a
/// troubleshooter needs to find and fix the record (issue #8). `field` names
/// the offending dataset field; `source_field` points back at the source —
/// set when a rename mapping changed that field's name (ADR-0039), and, for
/// a flatten output, carrying the declared source path as written
/// (ADR-0041). `record` is the record content the load could recover
/// — the parsed record as a JSON object under its source names, the raw line
/// text for an unparseable JSONL line, or JSON null when nothing could be
/// recovered.
#[derive(Debug)]
pub(crate) struct RejectedRecord {
    pub(crate) line: u64,
    pub(crate) code: &'static str,
    pub(crate) field: Option<String>,
    pub(crate) source_field: Option<String>,
    pub(crate) message: String,
    pub(crate) record: Value,
}

/// Renders rejected records as the `rejected-records.jsonl` artifact text:
/// one JSON object per line, sorted by source line so the artifact reads in
/// source order even when parse and validation rejections interleave.
pub(crate) fn artifact_jsonl(rejected: &[RejectedRecord]) -> String {
    let mut ordered: Vec<&RejectedRecord> = rejected.iter().collect();
    ordered.sort_by_key(|rejected_record| rejected_record.line);
    let mut text = String::new();
    for rejected_record in ordered {
        let object = json!({
            "line": rejected_record.line,
            "code": rejected_record.code,
            "field": rejected_record.field,
            "source_field": rejected_record.source_field,
            "message": rejected_record.message,
            "record": rejected_record.record,
        });
        text.push_str(&object.to_string());
        text.push('\n');
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_jsonl_renders_one_json_object_per_record_in_source_line_order() {
        // The first rejection's field was renamed (source `id`, dataset
        // `customer_id`), so its artifact line carries the source field; the
        // parse rejection has no field at all, so both keys render null.
        let rejected = [
            RejectedRecord {
                line: 4,
                code: TYPE_COERCION_FAILED,
                field: Some("customer_id".to_string()),
                source_field: Some("id".to_string()),
                message: "value \"abc\" does not fit pinned type int64 for field \"customer_id\""
                    .to_string(),
                record: json!({ "id": "abc", "name": "Ada" }),
            },
            RejectedRecord {
                line: 2,
                code: MALFORMED_JSONL_RECORD,
                field: None,
                source_field: None,
                message: "expected value at line 1 column 28".to_string(),
                record: json!("{\"customer_id\": 2, \"name\": "),
            },
        ];

        let artifact = artifact_jsonl(&rejected);

        // Parse and validation rejections interleave in collection order; the
        // artifact sorts them back into source-line order.
        let lines: Vec<&str> = artifact.lines().collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(
            serde_json::from_str::<Value>(lines[0]).expect("first artifact line is json"),
            json!({
                "line": 2,
                "code": "malformed_jsonl_record",
                "field": null,
                "source_field": null,
                "message": "expected value at line 1 column 28",
                "record": "{\"customer_id\": 2, \"name\": "
            })
        );
        assert_eq!(
            serde_json::from_str::<Value>(lines[1]).expect("second artifact line is json"),
            json!({
                "line": 4,
                "code": "type_coercion_failed",
                "field": "customer_id",
                "source_field": "id",
                "message": "value \"abc\" does not fit pinned type int64 for field \"customer_id\"",
                "record": { "id": "abc", "name": "Ada" }
            })
        );
        assert!(artifact.ends_with('\n'), "artifact ends with a newline");
    }

    #[test]
    fn artifact_jsonl_renders_no_rejections_as_the_empty_artifact() {
        assert_eq!(artifact_jsonl(&[]), "");
    }
}
