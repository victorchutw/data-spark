//! Rejected-record deep module: the single home for what a rejected record is
//! and how the `rejected-records.jsonl` artifact renders it.
//!
//! A rejected record (ADR-0019) is a source record that cannot be written to
//! the destination dataset without violating the chosen schema or load rules:
//! a record that fails to parse ([`MALFORMED_CSV_RECORD`],
//! [`MALFORMED_JSONL_RECORD`]), a value that does not fit its pinned type
//! ([`TYPE_COERCION_FAILED`]), or a null in a non-nullable pinned field
//! ([`MISSING_REQUIRED_FIELD`]) — per-record semantics per ADR-0035. Producers
//! (the source readers and the schema module) construct [`RejectedRecord`]s
//! and stream them into the [`RejectionSink`] during pass 1 (ADR-0045)
//! instead of accumulating them in memory; the sink owns the artifact file.
//! Where a rejection cannot be confirmed until the end-of-input shape
//! verdict — JSONL validation rejections — the record buffers through the
//! line-ordered [`ValidationSpill`] and merges into the artifact after the
//! verdict, so a shape-drift failure's artifact carries only parse
//! rejections. The artifact contract (ADR-0036): one JSON object per
//! rejected record, in source-line order, carrying the line, a rejection
//! code, the offending field when one is known — its dataset name, with the
//! source named alongside when a rename mapping changed it (ADR-0039) or,
//! for a flatten output, as the declared source path (ADR-0041) — a
//! human-readable message, and the record content the load could recover.

use serde_json::{json, Value};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

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
/// A null or absent value in a merge key field: merge keys are implicitly
/// non-null, since a null never equals anything under key equality.
pub(crate) const NULL_MERGE_KEY: &str = "null_merge_key";

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

/// Renders one rejected record as its artifact line, without the trailing
/// newline.
pub(crate) fn artifact_line(rejected: &RejectedRecord) -> String {
    json!({
        "line": rejected.line,
        "code": rejected.code,
        "field": rejected.field,
        "source_field": rejected.source_field,
        "message": rejected.message,
        "record": rejected.record,
    })
    .to_string()
}

/// Streams the `rejected-records.jsonl` artifact of one load during pass 1
/// (ADR-0045): rejections arrive in source-line order and are written as
/// found, so peak memory holds no rejected-record content — only the
/// rejected line numbers, which pass 2 needs to skip and cross-check the
/// records pass 1 rejected. The artifact file is created lazily on the
/// first rejection, so a rejection-free load leaves none.
///
/// I/O failures poison the sink instead of interrupting the pass: the load
/// keeps its semantics, and the orchestrator surfaces the stored error
/// before writing any report — the same abort a failed whole-artifact write
/// produced before streaming.
pub(crate) struct RejectionSink {
    artifact_path: PathBuf,
    spill_path: PathBuf,
    writer: Option<BufWriter<File>>,
    count: u64,
    lines: Vec<u64>,
    io_error: Option<io::Error>,
}

impl RejectionSink {
    pub(crate) fn new(artifact_dir: &Path) -> Self {
        RejectionSink {
            artifact_path: artifact_dir.join(REJECTED_RECORDS_FILENAME),
            spill_path: artifact_dir.join(".rejected-records.spill"),
            writer: None,
            count: 0,
            lines: Vec::new(),
            io_error: None,
        }
    }

    /// Appends one rejection to the artifact. Callers emit in source-line
    /// order, so the artifact reads in source order without a sort.
    pub(crate) fn record(&mut self, rejected: &RejectedRecord) {
        self.count += 1;
        self.lines.push(rejected.line);
        if self.io_error.is_some() {
            return;
        }
        if let Err(error) = self.write_line(&artifact_line(rejected)) {
            self.io_error = Some(error);
        }
    }

    fn write_line(&mut self, line: &str) -> io::Result<()> {
        if self.writer.is_none() {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.artifact_path)?;
            self.writer = Some(BufWriter::new(file));
        }
        let writer = self.writer.as_mut().expect("artifact writer opened");
        writer.write_all(line.as_bytes())?;
        writer.write_all(b"\n")
    }

    /// The number of records this load rejected.
    pub(crate) fn count(&self) -> u64 {
        self.count
    }

    /// The rejected records' source lines, in artifact order — the pass-1
    /// outcomes pass 2 re-checks records against (ADR-0045).
    pub(crate) fn rejected_lines(&self) -> &[u64] {
        &self.lines
    }

    /// Opens the line-ordered spill file for rejections that cannot be
    /// confirmed until the end-of-input verdict (JSONL validation
    /// rejections, ADR-0045).
    pub(crate) fn spill(&self) -> ValidationSpill {
        ValidationSpill {
            path: self.spill_path.clone(),
            writer: None,
            entries: 0,
            io_error: None,
        }
    }

    /// Merges the spilled rejections into the artifact after a passing
    /// verdict, interleaving the two line-ordered streams so the artifact
    /// stays in source-line order. `render` turns one spilled record back
    /// into its rejection under the resolved checks; a `None` render (a
    /// record that no longer violates, which check agreement rules out) is
    /// dropped.
    pub(crate) fn merge_spill(
        &mut self,
        spill: ValidationSpill,
        mut render: impl FnMut(u64, serde_json::Map<String, Value>) -> Option<RejectedRecord>,
    ) {
        let spilled = spill.entries;
        let spill_error = spill.finish();
        if self.io_error.is_none() {
            self.io_error = spill_error;
        }
        if self.io_error.is_some() || spilled == 0 {
            let _ = fs::remove_file(&self.spill_path);
            return;
        }
        if let Err(error) = self.merge_spill_file(&mut render) {
            self.io_error = Some(error);
            // A failed merge must not leave its half-written scratch file in
            // the artifact directory; the load aborts on the stored error.
            let _ = fs::remove_file(self.merged_path());
        }
        let _ = fs::remove_file(&self.spill_path);
    }

    fn merged_path(&self) -> PathBuf {
        self.artifact_path
            .with_file_name(".rejected-records.merged")
    }

    fn merge_spill_file(
        &mut self,
        render: &mut impl FnMut(u64, serde_json::Map<String, Value>) -> Option<RejectedRecord>,
    ) -> io::Result<()> {
        if let Some(mut writer) = self.writer.take() {
            writer.flush()?;
        }

        let merged_path = self.merged_path();
        let mut merged_lines = Vec::new();
        let mut merged_count = 0_u64;
        {
            let mut merged = BufWriter::new(File::create(&merged_path)?);
            let mut artifact = match File::open(&self.artifact_path) {
                Ok(file) => Some(BufReader::new(file).lines()),
                Err(error) if error.kind() == io::ErrorKind::NotFound => None,
                Err(error) => return Err(error),
            };
            let mut spill = BufReader::new(File::open(&self.spill_path)?).lines();

            let mut next_artifact = next_artifact_line(&mut artifact)?;
            let mut next_spill = next_spill_rejection(&mut spill, render)?;
            loop {
                // Ties cannot happen — a record is either a parse or a
                // validation rejection — but the artifact side winning keeps
                // the merge total.
                let artifact_turn = match (&next_artifact, &next_spill) {
                    (None, None) => break,
                    (Some(_), None) => true,
                    (None, Some(_)) => false,
                    (Some((artifact_line, _)), Some(spill_rejection)) => {
                        *artifact_line <= spill_rejection.line
                    }
                };
                let (line, text) = if artifact_turn {
                    let (line, text) = next_artifact.take().expect("artifact line present");
                    next_artifact = next_artifact_line(&mut artifact)?;
                    (line, text)
                } else {
                    let rejection = next_spill.take().expect("spill rejection present");
                    next_spill = next_spill_rejection(&mut spill, render)?;
                    (rejection.line, artifact_line(&rejection))
                };
                merged.write_all(text.as_bytes())?;
                merged.write_all(b"\n")?;
                merged_lines.push(line);
                merged_count += 1;
            }
            merged.flush()?;
        }

        if merged_count == 0 {
            fs::remove_file(&merged_path)?;
            if self.artifact_path.exists() {
                fs::remove_file(&self.artifact_path)?;
            }
        } else {
            fs::rename(&merged_path, &self.artifact_path)?;
        }
        self.lines = merged_lines;
        self.count = merged_count;
        Ok(())
    }

    /// Surrenders the first I/O failure the sink absorbed, flushing the
    /// artifact writer first. The orchestrator calls this once after
    /// execution: a stored error aborts the load without a report, exactly
    /// like a failed whole-artifact write did before streaming.
    pub(crate) fn take_io_error(&mut self) -> Option<io::Error> {
        if let Some(mut writer) = self.writer.take() {
            if let (Err(error), None) = (writer.flush(), &self.io_error) {
                self.io_error = Some(error);
            }
        }
        self.io_error.take()
    }
}

/// The line-ordered spill file for rejections that await the end-of-input
/// verdict (ADR-0045): one JSON object per spilled record, carrying the
/// source line and the parsed record content. Spilled entries either merge
/// into the artifact ([`RejectionSink::merge_spill`]) or are discarded with
/// the verdict failure, so the artifact of a shape-failed load carries only
/// parse rejections.
pub(crate) struct ValidationSpill {
    path: PathBuf,
    writer: Option<BufWriter<File>>,
    entries: u64,
    io_error: Option<io::Error>,
}

impl ValidationSpill {
    /// Spills one record pending the verdict.
    pub(crate) fn record(&mut self, line: u64, object: &serde_json::Map<String, Value>) {
        self.entries += 1;
        if self.io_error.is_some() {
            return;
        }
        if let Err(error) = self.write_entry(line, object) {
            self.io_error = Some(error);
        }
    }

    fn write_entry(
        &mut self,
        line: u64,
        object: &serde_json::Map<String, Value>,
    ) -> io::Result<()> {
        if self.writer.is_none() {
            self.writer = Some(BufWriter::new(File::create(&self.path)?));
        }
        let writer = self.writer.as_mut().expect("spill writer opened");
        let entry = json!({ "line": line, "record": object });
        writer.write_all(entry.to_string().as_bytes())?;
        writer.write_all(b"\n")
    }

    /// Drops the spill without merging — the verdict failed, so the
    /// artifact keeps only the parse rejections already streamed. Any spill
    /// I/O failure is passed into the sink so the load still aborts before
    /// reporting.
    pub(crate) fn discard(self, sink: &mut RejectionSink) {
        let path = self.path.clone();
        let error = self.finish();
        if sink.io_error.is_none() {
            sink.io_error = error;
        }
        let _ = fs::remove_file(path);
    }

    fn finish(mut self) -> Option<io::Error> {
        if let Some(mut writer) = self.writer.take() {
            if let (Err(error), None) = (writer.flush(), &self.io_error) {
                self.io_error = Some(error);
            }
        }
        self.io_error.take()
    }
}

fn next_artifact_line(
    lines: &mut Option<io::Lines<BufReader<File>>>,
) -> io::Result<Option<(u64, String)>> {
    let Some(lines) = lines.as_mut() else {
        return Ok(None);
    };
    let Some(text) = lines.next().transpose()? else {
        return Ok(None);
    };
    let line = serde_json::from_str::<Value>(&text)
        .ok()
        .and_then(|value| value["line"].as_u64())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "rejected-records artifact line without a line number",
            )
        })?;
    Ok(Some((line, text)))
}

fn next_spill_rejection(
    lines: &mut io::Lines<BufReader<File>>,
    render: &mut impl FnMut(u64, serde_json::Map<String, Value>) -> Option<RejectedRecord>,
) -> io::Result<Option<RejectedRecord>> {
    for text in lines.by_ref() {
        let text = text?;
        let invalid = || {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "rejected-records spill line without a line number and record",
            )
        };
        let Value::Object(mut entry) =
            serde_json::from_str::<Value>(&text).map_err(|_| invalid())?
        else {
            return Err(invalid());
        };
        let line = entry["line"].as_u64().ok_or_else(invalid)?;
        let Some(Value::Object(object)) = entry.remove("record") else {
            return Err(invalid());
        };
        if let Some(rejection) = render(line, object) {
            return Ok(Some(rejection));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_rejection(line: u64) -> RejectedRecord {
        RejectedRecord {
            line,
            code: MALFORMED_JSONL_RECORD,
            field: None,
            source_field: None,
            message: "expected value at line 1 column 28".to_string(),
            record: json!("{\"customer_id\": 2, \"name\": "),
        }
    }

    fn validation_rejection(line: u64) -> RejectedRecord {
        RejectedRecord {
            line,
            code: TYPE_COERCION_FAILED,
            field: Some("customer_id".to_string()),
            source_field: Some("id".to_string()),
            message: "value \"abc\" does not fit pinned type int64 for field \"customer_id\""
                .to_string(),
            record: json!({ "id": "abc", "name": "Ada" }),
        }
    }

    #[test]
    fn artifact_line_renders_the_rejection_with_all_artifact_keys() {
        // The rejection's field was renamed (source `id`, dataset
        // `customer_id`), so its artifact line carries the source field.
        assert_eq!(
            serde_json::from_str::<Value>(&artifact_line(&validation_rejection(4)))
                .expect("artifact line is json"),
            json!({
                "line": 4,
                "code": "type_coercion_failed",
                "field": "customer_id",
                "source_field": "id",
                "message": "value \"abc\" does not fit pinned type int64 for field \"customer_id\"",
                "record": { "id": "abc", "name": "Ada" }
            })
        );

        // A parse rejection has no field at all, so both field keys render
        // null.
        assert_eq!(
            serde_json::from_str::<Value>(&artifact_line(&parse_rejection(2)))
                .expect("artifact line is json"),
            json!({
                "line": 2,
                "code": "malformed_jsonl_record",
                "field": null,
                "source_field": null,
                "message": "expected value at line 1 column 28",
                "record": "{\"customer_id\": 2, \"name\": "
            })
        );
    }

    #[test]
    fn sink_streams_rejections_in_arrival_order_and_tracks_their_lines() {
        let work = tempfile::TempDir::new().expect("tempdir");
        let mut sink = RejectionSink::new(work.path());

        sink.record(&parse_rejection(2));
        sink.record(&validation_rejection(4));

        assert_eq!(sink.count(), 2);
        assert_eq!(sink.rejected_lines(), &[2, 4]);
        assert!(sink.take_io_error().is_none());

        let artifact = std::fs::read_to_string(work.path().join(REJECTED_RECORDS_FILENAME))
            .expect("artifact streamed");
        let lines: Vec<&str> = artifact.lines().collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], artifact_line(&parse_rejection(2)));
        assert_eq!(lines[1], artifact_line(&validation_rejection(4)));
        assert!(artifact.ends_with('\n'), "artifact ends with a newline");
    }

    #[test]
    fn sink_without_rejections_leaves_no_artifact_file() {
        let work = tempfile::TempDir::new().expect("tempdir");
        let mut sink = RejectionSink::new(work.path());
        assert_eq!(sink.count(), 0);
        assert!(sink.take_io_error().is_none());
        assert!(!work.path().join(REJECTED_RECORDS_FILENAME).exists());
    }

    #[test]
    fn merge_spill_interleaves_spilled_rejections_into_source_line_order() {
        let work = tempfile::TempDir::new().expect("tempdir");
        let mut sink = RejectionSink::new(work.path());
        let mut spill = sink.spill();

        // Parse rejections stream on lines 2 and 5; validation rejections
        // spill on lines 4 and 7 pending the verdict.
        sink.record(&parse_rejection(2));
        let object = json!({ "id": "abc", "name": "Ada" });
        let object = object.as_object().expect("record object");
        spill.record(4, object);
        sink.record(&parse_rejection(5));
        spill.record(7, object);

        sink.merge_spill(spill, |line, object| {
            let mut rejection = validation_rejection(line);
            rejection.record = Value::Object(object);
            Some(rejection)
        });

        assert_eq!(sink.count(), 4);
        assert_eq!(sink.rejected_lines(), &[2, 4, 5, 7]);
        assert!(sink.take_io_error().is_none());
        assert!(
            !work.path().join(".rejected-records.spill").exists(),
            "the spill file is removed after the merge"
        );

        let artifact = std::fs::read_to_string(work.path().join(REJECTED_RECORDS_FILENAME))
            .expect("artifact merged");
        let lines: Vec<&str> = artifact.lines().collect();
        assert_eq!(lines.len(), 4);
        assert_eq!(lines[0], artifact_line(&parse_rejection(2)));
        assert_eq!(lines[1], artifact_line(&validation_rejection(4)));
        assert_eq!(lines[2], artifact_line(&parse_rejection(5)));
        assert_eq!(lines[3], artifact_line(&validation_rejection(7)));
    }

    #[test]
    fn merge_spill_without_parse_rejections_creates_the_artifact_from_the_spill() {
        let work = tempfile::TempDir::new().expect("tempdir");
        let mut sink = RejectionSink::new(work.path());
        let mut spill = sink.spill();
        let object = json!({ "id": "abc" });
        spill.record(3, object.as_object().expect("record object"));

        sink.merge_spill(spill, |line, _| Some(validation_rejection(line)));

        assert_eq!(sink.count(), 1);
        assert_eq!(sink.rejected_lines(), &[3]);
        assert!(sink.take_io_error().is_none());
        let artifact = std::fs::read_to_string(work.path().join(REJECTED_RECORDS_FILENAME))
            .expect("artifact merged");
        assert_eq!(
            artifact,
            format!("{}\n", artifact_line(&validation_rejection(3)))
        );
    }

    #[test]
    fn discarding_the_spill_keeps_only_the_streamed_parse_rejections() {
        // A shape-verdict failure discards spilled validation rejections:
        // the artifact keeps only the parse rejections already streamed.
        let work = tempfile::TempDir::new().expect("tempdir");
        let mut sink = RejectionSink::new(work.path());
        let mut spill = sink.spill();
        sink.record(&parse_rejection(2));
        let object = json!({ "id": "abc" });
        spill.record(4, object.as_object().expect("record object"));

        spill.discard(&mut sink);

        assert_eq!(sink.count(), 1);
        assert_eq!(sink.rejected_lines(), &[2]);
        assert!(sink.take_io_error().is_none());
        assert!(!work.path().join(".rejected-records.spill").exists());
        let artifact = std::fs::read_to_string(work.path().join(REJECTED_RECORDS_FILENAME))
            .expect("artifact keeps parse rejections");
        assert_eq!(
            artifact,
            format!("{}\n", artifact_line(&parse_rejection(2)))
        );
    }
}
