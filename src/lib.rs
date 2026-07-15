use clap::{Args, Parser, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

mod connector;
mod rejection;
mod schema;

use connector::{
    destination_connector, source_connector, DestinationWrite, DestinationWriteFacts, LoadMode,
    SourceRead,
};

const LOAD_REPORT_VERSION: u8 = 1;
const LOAD_REPORT_FILENAME: &str = "load-report.json";
const SUPPORTED_LOAD_DEFINITION_VERSION: u64 = 1;

#[derive(Parser)]
#[command(name = "data-spark")]
#[command(about = "Portable data movement CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Load(LoadArgs),
}

#[derive(Args)]
struct LoadArgs {
    #[arg(long)]
    output_dir: Option<PathBuf>,
    definition: PathBuf,
}

#[derive(Serialize)]
struct LoadReport {
    report_version: u8,
    load_id: String,
    artifact_dir: String,
    source_summary: Value,
    destination_summary: Value,
    dataset: Option<String>,
    load_mode: String,
    schema_decision: Value,
    row_counts: RowCounts,
    byte_counts: ByteCounts,
    rejected_records: RejectedRecordFacts,
    destination_write: Value,
    execution: Value,
    timings: Timings,
    exit_status: &'static str,
    process_exit_code: u8,
    error_summary: Option<ErrorSummary>,
}

impl LoadReport {
    /// Assemble the report for a load whose execution succeeded.
    fn from_success(
        load_id: String,
        artifact_dir: String,
        timings: Timings,
        details: ExecutionDetails,
    ) -> Self {
        let rejected_records =
            RejectedRecordFacts::facts(details.rejected.len() as u64, &artifact_dir);
        LoadReport {
            report_version: LOAD_REPORT_VERSION,
            load_id,
            artifact_dir,
            source_summary: details.source_summary,
            destination_summary: details.destination_summary,
            dataset: details.dataset,
            load_mode: details.load_mode,
            schema_decision: details.schema_decision,
            row_counts: details.row_counts,
            byte_counts: details.byte_counts,
            rejected_records,
            destination_write: details.destination_write.report_value(),
            execution: details.execution,
            timings,
            exit_status: "succeeded",
            process_exit_code: 0,
            error_summary: None,
        }
    }

    /// Assemble the report for a load that failed: the definition context is
    /// echoed back and every execution field takes its not-run posture,
    /// except the rejection facts, which report the records already rejected
    /// (and their artifact) when the failure happened.
    fn from_failure(
        load_id: String,
        artifact_dir: String,
        timings: Timings,
        failure: ReportableFailure,
    ) -> Self {
        let rejected_count = failure.rejected.len() as u64;
        let rejected_records = RejectedRecordFacts::facts(rejected_count, &artifact_dir);
        LoadReport {
            report_version: LOAD_REPORT_VERSION,
            load_id,
            artifact_dir,
            source_summary: failure.source_summary,
            destination_summary: failure.destination_summary,
            dataset: failure.dataset,
            load_mode: failure.load_mode,
            schema_decision: failure.schema_decision,
            row_counts: RowCounts {
                source: failure.source_rows,
                written: failure.written_records,
                rejected: rejected_count,
            },
            byte_counts: ByteCounts {
                source: None,
                destination: None,
            },
            rejected_records,
            destination_write: failure.destination_write.report_value(),
            execution: json!({
                "record_format": "not_started",
                "batch_count": 0
            }),
            timings,
            exit_status: "failed",
            process_exit_code: 1,
            error_summary: Some(ErrorSummary {
                code: failure.code,
                message: failure.message,
            }),
        }
    }
}

#[derive(Serialize)]
struct RowCounts {
    source: u64,
    written: u64,
    rejected: u64,
}

/// The rejected-record facts a load report states (ADR-0036): how many
/// records the load rejected and, when any were, the `rejected-records.jsonl`
/// artifact they were written to.
#[derive(Serialize)]
struct RejectedRecordFacts {
    count: u64,
    artifact: Option<String>,
}

impl RejectedRecordFacts {
    fn facts(count: u64, artifact_dir: &str) -> Self {
        RejectedRecordFacts {
            count,
            artifact: (count > 0).then(|| {
                path_string(&Path::new(artifact_dir).join(rejection::REJECTED_RECORDS_FILENAME))
            }),
        }
    }
}

#[derive(Serialize)]
struct ByteCounts {
    source: Option<u64>,
    destination: Option<u64>,
}

#[derive(Serialize)]
struct Timings {
    started_unix_ms: u128,
    finished_unix_ms: u128,
    duration_ms: u128,
}

#[derive(Serialize)]
struct ErrorSummary {
    code: &'static str,
    message: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct LoadDefinition {
    version: Option<u64>,
    source: Option<SourceDefinition>,
    destination: Option<DestinationDefinition>,
    dataset: Option<String>,
    load_mode: Option<String>,
    schema: Option<SchemaConfig>,
    /// The number of rejected records this load tolerates before failing.
    /// Defaults to `0`: any rejected record fails the load unless the
    /// definition explicitly allows more (ADR-0020).
    reject_threshold: Option<u64>,
}

/// The `schema` block of a load definition: the path of the pinned schema file
/// the load reuses (ADR-0033) and the drift policy that decides whether a load
/// may continue when schema drift is detected (ADR-0007).
#[derive(Debug, Deserialize, Serialize)]
struct SchemaConfig {
    pinned_path: Option<PathBuf>,
    drift_policy: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct SourceDefinition {
    connector: String,
    path: PathBuf,
    format: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct DestinationDefinition {
    connector: String,
    path: PathBuf,
}

/// The execution facts of a load that succeeded. Failures travel as
/// [`ReportableFailure`] instead. `rejected` carries the rejected records
/// themselves — within the configured reject threshold, or the load would
/// have failed — so the orchestrator can write their artifact.
struct ExecutionDetails {
    source_summary: Value,
    destination_summary: Value,
    dataset: Option<String>,
    load_mode: String,
    schema_decision: Value,
    row_counts: RowCounts,
    byte_counts: ByteCounts,
    rejected: Vec<rejection::RejectedRecord>,
    destination_write: DestinationWriteFacts,
    execution: Value,
}

/// A failure joined with the load definition context (source, destination,
/// dataset, load mode) that a load report echoes back, plus the facts the
/// load had already established when it failed: the schema decision if one
/// had been made (`not_evaluated` otherwise), the source records counted
/// once the read completed (`0` otherwise), and the records rejected before
/// failure, whose artifact is still written.
struct ReportableFailure {
    source_summary: Value,
    destination_summary: Value,
    dataset: Option<String>,
    load_mode: String,
    schema_decision: Value,
    source_rows: u64,
    written_records: u64,
    rejected: Vec<rejection::RejectedRecord>,
    destination_write: DestinationWriteFacts,
    code: &'static str,
    message: String,
}

impl ReportableFailure {
    /// For failures raised before the load definition is parsed (read / YAML),
    /// when no definition context exists yet: empty summaries, no dataset, and
    /// the default load mode.
    fn without_context(code: &'static str, message: String) -> Self {
        ReportableFailure {
            source_summary: json!({}),
            destination_summary: json!({}),
            dataset: None,
            load_mode: "full_refresh".to_string(),
            schema_decision: not_evaluated_schema_decision(),
            source_rows: 0,
            written_records: 0,
            rejected: Vec::new(),
            destination_write: DestinationWriteFacts::not_applicable(),
            code,
            message,
        }
    }
}

/// The schema decision a report carries when the load failed before any schema
/// decision was made.
fn not_evaluated_schema_decision() -> Value {
    json!({ "mode": "not_evaluated" })
}

#[derive(Debug)]
struct LoadFailure {
    code: &'static str,
    message: String,
}

/// A load failure joined with the facts the load had already established when
/// the failure happened, so the report can echo them: the schema decision if
/// one had been made (a failure before any decision reports `not_evaluated`),
/// the source records counted once the read completed, and the records
/// rejected before the failure, whose artifact the orchestrator still writes. Failures raised
/// before or without those facts lift via `From` with none of them.
#[derive(Debug)]
struct ExecutionFailure {
    failure: LoadFailure,
    // Boxed so the Err variant stays small enough to return by value.
    schema_decision: Option<Box<Value>>,
    source_rows: Option<u64>,
    written_records: u64,
    rejected: Vec<rejection::RejectedRecord>,
    destination_write: Box<DestinationWriteFacts>,
}

impl From<LoadFailure> for ExecutionFailure {
    fn from(failure: LoadFailure) -> Self {
        ExecutionFailure {
            failure,
            schema_decision: None,
            source_rows: None,
            written_records: 0,
            rejected: Vec::new(),
            destination_write: Box::new(DestinationWriteFacts::not_applicable()),
        }
    }
}

pub fn run() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Load(args) => match run_load(args) {
            Ok(report) => {
                print_summary(&report);
                ExitCode::from(report.process_exit_code)
            }
            Err(error) => {
                eprintln!("data-spark: {error}");
                ExitCode::from(1)
            }
        },
    }
}

fn run_load(args: LoadArgs) -> Result<LoadReport, Box<dyn std::error::Error>> {
    let started_unix_ms = unix_ms();
    let load_id = Uuid::new_v4().to_string();
    let artifact_root = args
        .output_dir
        .unwrap_or_else(|| PathBuf::from(".data-spark").join("runs"));
    let artifact_dir = artifact_root.join(&load_id);
    fs::create_dir_all(&artifact_dir)?;

    let execution = execute_load_definition(&args.definition);

    // The rejected-records artifact is written before the report that names
    // it, from whichever side of the outcome carries rejections (ADR-0036).
    let rejected = match &execution {
        Ok(details) => &details.rejected,
        Err(failure) => &failure.rejected,
    };
    if !rejected.is_empty() {
        fs::write(
            artifact_dir.join(rejection::REJECTED_RECORDS_FILENAME),
            rejection::artifact_jsonl(rejected),
        )?;
    }

    let finished_unix_ms = unix_ms();
    let timings = Timings {
        started_unix_ms,
        finished_unix_ms,
        duration_ms: finished_unix_ms.saturating_sub(started_unix_ms),
    };
    let report = match execution {
        Ok(details) => {
            LoadReport::from_success(load_id, path_string(&artifact_dir), timings, details)
        }
        Err(failure) => {
            LoadReport::from_failure(load_id, path_string(&artifact_dir), timings, failure)
        }
    };

    fs::write(
        artifact_dir.join(LOAD_REPORT_FILENAME),
        serde_json::to_vec_pretty(&report)?,
    )?;

    Ok(report)
}

// Called once per load, so the size of the Err variant (which carries the
// definition context the report echoes back) is irrelevant.
#[allow(clippy::result_large_err)]
fn execute_load_definition(definition_path: &Path) -> Result<ExecutionDetails, ReportableFailure> {
    let definition_text = fs::read_to_string(definition_path).map_err(|error| {
        ReportableFailure::without_context(
            "load_definition_read_failed",
            format!("failed to read load definition: {error}"),
        )
    })?;

    let definition = serde_yaml::from_str::<LoadDefinition>(&definition_text).map_err(|error| {
        ReportableFailure::without_context(
            "invalid_load_definition_yaml",
            format!("failed to parse load definition: {error}"),
        )
    })?;

    let source_summary = definition
        .source
        .as_ref()
        .map(to_json)
        .unwrap_or_else(|| json!({}));
    let destination_summary = definition
        .destination
        .as_ref()
        .map(to_json)
        .unwrap_or_else(|| json!({}));
    let dataset = definition.dataset.clone();
    let load_mode = definition
        .load_mode
        .clone()
        .unwrap_or_else(|| "full_refresh".to_string());

    match definition.version {
        Some(SUPPORTED_LOAD_DEFINITION_VERSION) => {}
        Some(version) => {
            return Err(ReportableFailure {
                source_summary,
                destination_summary,
                dataset,
                load_mode,
                schema_decision: not_evaluated_schema_decision(),
                source_rows: 0,
                written_records: 0,
                rejected: Vec::new(),
                destination_write: DestinationWriteFacts::not_applicable(),
                code: "unsupported_load_definition_version",
                message: format!("unsupported load definition version: {version}"),
            })
        }
        None => {
            return Err(ReportableFailure {
                source_summary,
                destination_summary,
                dataset,
                load_mode,
                schema_decision: not_evaluated_schema_decision(),
                source_rows: 0,
                written_records: 0,
                rejected: Vec::new(),
                destination_write: DestinationWriteFacts::not_applicable(),
                code: "missing_load_definition_version",
                message: "load definition version is required".to_string(),
            })
        }
    }

    execute_supported_load(&definition).map_err(|execution_failure| ReportableFailure {
        source_summary,
        destination_summary,
        dataset,
        load_mode,
        schema_decision: execution_failure
            .schema_decision
            .map(|decision| *decision)
            .unwrap_or_else(not_evaluated_schema_decision),
        source_rows: execution_failure.source_rows.unwrap_or(0),
        written_records: execution_failure.written_records,
        rejected: execution_failure.rejected,
        destination_write: *execution_failure.destination_write,
        code: execution_failure.failure.code,
        message: execution_failure.failure.message,
    })
}

fn execute_supported_load(
    definition: &LoadDefinition,
) -> Result<ExecutionDetails, ExecutionFailure> {
    let source = definition.source.as_ref().ok_or_else(|| LoadFailure {
        code: "missing_source",
        message: "load definition source is required".to_string(),
    })?;
    let destination = definition.destination.as_ref().ok_or_else(|| LoadFailure {
        code: "missing_destination",
        message: "load definition destination is required".to_string(),
    })?;
    let load_mode = definition
        .load_mode
        .clone()
        .unwrap_or_else(|| "full_refresh".to_string());

    // Validate the load mode, both connectors, and the schema directive before
    // any source or destination I/O so an unsupported definition fails without
    // reading the source or touching the destination (ADR-0019), preserving the
    // error precedence: load mode -> source connector -> destination connector
    // -> schema directive (config, then pinned schema file) -> read -> write.
    // The source format is validated inside read() so its precedence stays
    // after the destination-connector check for a doubly-invalid definition.
    let mode = LoadMode::parse(&load_mode)?;
    let source_port = source_connector(source)?;
    let destination_port = destination_connector(destination, definition.dataset.as_deref())?;
    destination_port.validate_mode(mode)?;
    let directive = resolve_schema_directive(definition.schema.as_ref())?;

    let SourceRead {
        batch,
        schema_decision,
        pinned_schema_write,
        source_bytes,
        rejected,
    } = source_port.read(&directive)?;

    let rejected_count = rejected.len() as u64;
    let row_count = batch.num_rows() as u64;
    let source_rows = row_count + rejected_count;

    // The reject threshold gates the load while it is still side-effect free
    // (ADR-0036): more rejected records than the definition tolerates
    // (`reject_threshold`, default 0 per ADR-0020) is a validation failure,
    // which must leave the pin unpersisted and the destination untouched
    // (ADR-0019).
    let reject_threshold = definition.reject_threshold.unwrap_or(0);
    if rejected_count > reject_threshold {
        return Err(ExecutionFailure {
            failure: LoadFailure {
                code: "reject_threshold_exceeded",
                message: format!(
                    "rejected {rejected_count} of {source_rows} records, \
                     exceeding the reject threshold of {reject_threshold}"
                ),
            },
            schema_decision: Some(Box::new(schema_decision)),
            source_rows: Some(source_rows),
            written_records: 0,
            rejected,
            destination_write: Box::new(DestinationWriteFacts::not_applicable()),
        });
    }

    // Persist the produced or extended pin before the destination write: the
    // pin records the schema decision of this load's source, which stays valid
    // even if the write then fails, and a retry converges on the same pin.
    // Failures past this point happened after the schema decision and the
    // rejections were established, so they carry both for the report.
    let write_result = (|| {
        if let Some(pinned_schema_write) = &pinned_schema_write {
            persist_pinned_schema(pinned_schema_write)?;
        }
        destination_port.write(&batch, mode)
    })();
    let DestinationWrite {
        bytes_written,
        facts,
    } = match write_result {
        Ok(write) => write,
        Err(write_failure) => {
            return Err(ExecutionFailure {
                failure: write_failure.failure,
                schema_decision: Some(Box::new(schema_decision)),
                source_rows: Some(source_rows),
                written_records: write_failure.written_records,
                rejected,
                destination_write: Box::new(write_failure.facts),
            })
        }
    };

    Ok(ExecutionDetails {
        source_summary: to_json(source),
        destination_summary: to_json(destination),
        dataset: definition.dataset.clone(),
        load_mode,
        schema_decision,
        row_counts: RowCounts {
            source: source_rows,
            written: row_count,
            rejected: rejected_count,
        },
        byte_counts: ByteCounts {
            source: Some(source_bytes),
            destination: bytes_written,
        },
        rejected,
        destination_write: facts,
        execution: json!({
            "record_format": "arrow_record_batch",
            "batch_count": 1
        }),
    })
}

/// Resolves the definition's `schema` block into the schema directive the
/// source materializes under: no block means inference, a named pinned schema
/// file that does not exist yet means this load persists the inferred schema as
/// the new pin (ADR-0033), and an existing file is parsed and validated against
/// (ADR-0034).
fn resolve_schema_directive(
    schema_config: Option<&SchemaConfig>,
) -> Result<schema::SchemaDirective, LoadFailure> {
    let Some(schema_config) = schema_config else {
        return Ok(schema::SchemaDirective::Inferred);
    };
    let pinned_path = schema_config
        .pinned_path
        .as_ref()
        .filter(|pinned_path| !pinned_path.as_os_str().is_empty())
        .ok_or_else(|| LoadFailure {
            code: "invalid_schema_config",
            message: "schema.pinned_path is required when a load definition has a schema block"
                .to_string(),
        })?;
    let drift_policy = match schema_config.drift_policy.as_deref() {
        None | Some("fail") => schema::DriftPolicy::Fail,
        Some("allow_additive_nullable") => schema::DriftPolicy::AllowAdditiveNullable,
        Some(other) => {
            return Err(LoadFailure {
                code: "unsupported_drift_policy",
                message: format!("unsupported drift policy: {other}"),
            })
        }
    };

    match fs::read_to_string(pinned_path) {
        Ok(pin_text) => Ok(schema::SchemaDirective::Pinned {
            pinned_path: path_string(pinned_path),
            pin: schema::PinnedSchema::from_yaml(&pin_text)?,
            drift_policy,
        }),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            Ok(schema::SchemaDirective::PinInferred {
                pinned_path: path_string(pinned_path),
            })
        }
        Err(error) => Err(LoadFailure {
            code: "pinned_schema_read_failed",
            message: format!(
                "failed to read pinned schema {}: {error}",
                pinned_path.display()
            ),
        }),
    }
}

/// Writes the pinned schema file a load produced or extended, creating parent
/// directories as needed. The write carries the path it belongs to, so no
/// second lookup into the definition is needed.
fn persist_pinned_schema(write: &schema::PinnedSchemaWrite) -> Result<(), LoadFailure> {
    let pinned_path = Path::new(&write.pinned_path);
    let yaml = &write.yaml;
    let write_failure = |error: io::Error| LoadFailure {
        code: "pinned_schema_write_failed",
        message: format!(
            "failed to persist pinned schema {}: {error}",
            pinned_path.display()
        ),
    };
    if let Some(parent) = pinned_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).map_err(write_failure)?;
    }
    fs::write(pinned_path, yaml).map_err(write_failure)
}

fn to_json<T: Serialize>(value: &T) -> Value {
    serde_json::to_value(value).unwrap_or_else(|_| json!({}))
}

fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis()
}

fn print_summary(report: &LoadReport) {
    let report_path = Path::new(&report.artifact_dir).join(LOAD_REPORT_FILENAME);
    println!("Data Spark load {}", report.load_id);
    println!("Status: {}", report.exit_status);
    println!("Load mode: {}", report.load_mode);
    println!("Source: {}", summary_text(&report.source_summary));
    println!("Destination: {}", summary_text(&report.destination_summary));
    println!("Records read: {}", report.row_counts.source);
    println!("Records written: {}", report.row_counts.written);
    println!("Records rejected: {}", report.row_counts.rejected);
    println!("Artifact directory: {}", report.artifact_dir);
    println!("Load report: {}", path_string(&report_path));
    if let Some(artifact) = &report.rejected_records.artifact {
        println!("Rejected records artifact: {artifact}");
    }
    if let Some(error) = &report.error_summary {
        println!("Error: {}", error.message);
    }
}

fn summary_text(summary: &Value) -> String {
    match summary {
        Value::Object(map) if map.is_empty() => "not specified".to_string(),
        Value::Object(map) => map
            .iter()
            .map(|(key, value)| match value {
                Value::String(text) => format!("{key}={text}"),
                other => format!("{key}={other}"),
            })
            .collect::<Vec<_>>()
            .join(", "),
        other => other.to_string(),
    }
}

fn path_string(path: &Path) -> String {
    path.display().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timings() -> Timings {
        Timings {
            started_unix_ms: 100,
            finished_unix_ms: 250,
            duration_ms: 150,
        }
    }

    fn rejected_record(line: u64) -> rejection::RejectedRecord {
        rejection::RejectedRecord {
            line,
            code: rejection::MALFORMED_CSV_RECORD,
            field: None,
            message: "expected 2 fields, found 3".to_string(),
            record: Value::Null,
        }
    }

    #[test]
    fn from_success_reports_the_success_posture_around_the_execution_facts() {
        let details = ExecutionDetails {
            source_summary: json!({ "connector": "local_file" }),
            destination_summary: json!({ "connector": "parquet" }),
            dataset: Some("customers".to_string()),
            load_mode: "full_refresh".to_string(),
            schema_decision: json!({ "mode": "inferred" }),
            row_counts: RowCounts {
                source: 3,
                written: 2,
                rejected: 1,
            },
            byte_counts: ByteCounts {
                source: Some(64),
                destination: Some(128),
            },
            rejected: vec![rejected_record(3)],
            destination_write: DestinationWriteFacts::best_effort("replace_directory"),
            execution: json!({ "record_format": "arrow_record_batch", "batch_count": 1 }),
        };

        let report = LoadReport::from_success(
            "load-under-test".to_string(),
            "artifacts/load-under-test".to_string(),
            timings(),
            details,
        );

        assert_eq!(report.report_version, LOAD_REPORT_VERSION);
        assert_eq!(report.load_id, "load-under-test");
        assert_eq!(report.artifact_dir, "artifacts/load-under-test");
        assert_eq!(report.source_summary, json!({ "connector": "local_file" }));
        assert_eq!(
            report.destination_summary,
            json!({ "connector": "parquet" })
        );
        assert_eq!(report.dataset, Some("customers".to_string()));
        assert_eq!(report.load_mode, "full_refresh");
        assert_eq!(report.schema_decision, json!({ "mode": "inferred" }));
        assert_eq!(report.row_counts.source, 3);
        assert_eq!(report.row_counts.written, 2);
        assert_eq!(report.row_counts.rejected, 1);
        assert_eq!(report.byte_counts.source, Some(64));
        assert_eq!(report.byte_counts.destination, Some(128));
        // A completing load with rejections still names their artifact.
        assert_eq!(report.rejected_records.count, 1);
        assert_eq!(
            report.rejected_records.artifact,
            Some("artifacts/load-under-test/rejected-records.jsonl".to_string())
        );
        assert_eq!(
            report.destination_write,
            json!({ "atomicity": "best_effort", "strategy": "replace_directory" })
        );
        assert_eq!(
            report.execution,
            json!({ "record_format": "arrow_record_batch", "batch_count": 1 })
        );
        assert_eq!(report.timings.started_unix_ms, 100);
        assert_eq!(report.timings.finished_unix_ms, 250);
        assert_eq!(report.timings.duration_ms, 150);
        assert_eq!(report.exit_status, "succeeded");
        assert_eq!(report.process_exit_code, 0);
        assert!(report.error_summary.is_none());
    }

    #[test]
    fn from_success_reports_no_artifact_for_a_load_without_rejections() {
        let details = ExecutionDetails {
            source_summary: json!({}),
            destination_summary: json!({}),
            dataset: None,
            load_mode: "full_refresh".to_string(),
            schema_decision: json!({ "mode": "inferred" }),
            row_counts: RowCounts {
                source: 2,
                written: 2,
                rejected: 0,
            },
            byte_counts: ByteCounts {
                source: Some(64),
                destination: Some(128),
            },
            rejected: Vec::new(),
            destination_write: DestinationWriteFacts::not_applicable(),
            execution: json!({}),
        };

        let report = LoadReport::from_success(
            "load-under-test".to_string(),
            "artifacts/load-under-test".to_string(),
            timings(),
            details,
        );

        assert_eq!(report.rejected_records.count, 0);
        assert_eq!(report.rejected_records.artifact, None);
    }

    #[test]
    fn from_failure_reports_the_failure_posture_around_the_error() {
        let failure = ReportableFailure {
            source_summary: json!({ "connector": "local_file" }),
            destination_summary: json!({ "connector": "parquet" }),
            dataset: Some("customers".to_string()),
            load_mode: "full_refresh".to_string(),
            schema_decision: not_evaluated_schema_decision(),
            source_rows: 0,
            written_records: 0,
            rejected: Vec::new(),
            destination_write: DestinationWriteFacts::not_applicable(),
            code: "missing_source",
            message: "load definition source is required".to_string(),
        };

        let report = LoadReport::from_failure(
            "load-under-test".to_string(),
            "artifacts/load-under-test".to_string(),
            timings(),
            failure,
        );

        assert_eq!(report.report_version, LOAD_REPORT_VERSION);
        assert_eq!(report.load_id, "load-under-test");
        assert_eq!(report.artifact_dir, "artifacts/load-under-test");
        assert_eq!(report.source_summary, json!({ "connector": "local_file" }));
        assert_eq!(
            report.destination_summary,
            json!({ "connector": "parquet" })
        );
        assert_eq!(report.dataset, Some("customers".to_string()));
        assert_eq!(report.load_mode, "full_refresh");
        assert_eq!(report.schema_decision, json!({ "mode": "not_evaluated" }));
        assert_eq!(report.row_counts.source, 0);
        assert_eq!(report.row_counts.written, 0);
        assert_eq!(report.row_counts.rejected, 0);
        assert_eq!(report.byte_counts.source, None);
        assert_eq!(report.byte_counts.destination, None);
        assert_eq!(report.rejected_records.count, 0);
        assert_eq!(report.rejected_records.artifact, None);
        assert_eq!(
            report.destination_write,
            json!({ "atomicity": "not_applicable" })
        );
        assert_eq!(
            report.execution,
            json!({ "record_format": "not_started", "batch_count": 0 })
        );
        assert_eq!(report.timings.started_unix_ms, 100);
        assert_eq!(report.timings.finished_unix_ms, 250);
        assert_eq!(report.timings.duration_ms, 150);
        assert_eq!(report.exit_status, "failed");
        assert_eq!(report.process_exit_code, 1);
        let error = report
            .error_summary
            .expect("failed load report carries an error summary");
        assert_eq!(error.code, "missing_source");
        assert_eq!(error.message, "load definition source is required");
    }

    #[test]
    fn from_failure_reports_the_rejection_facts_the_load_had_established() {
        // A reject-threshold failure happens after the read completed: the
        // report carries the honest source count, the rejected count, and the
        // artifact the rejections were written to — with written pinned to 0.
        let failure = ReportableFailure {
            source_summary: json!({ "connector": "local_file" }),
            destination_summary: json!({ "connector": "parquet" }),
            dataset: Some("customers".to_string()),
            load_mode: "full_refresh".to_string(),
            schema_decision: json!({ "mode": "inferred" }),
            source_rows: 3,
            written_records: 0,
            rejected: vec![rejected_record(2), rejected_record(3)],
            destination_write: DestinationWriteFacts::not_applicable(),
            code: "reject_threshold_exceeded",
            message: "rejected 2 of 3 records, exceeding the reject threshold of 0".to_string(),
        };

        let report = LoadReport::from_failure(
            "load-under-test".to_string(),
            "artifacts/load-under-test".to_string(),
            timings(),
            failure,
        );

        assert_eq!(report.row_counts.source, 3);
        assert_eq!(report.row_counts.written, 0);
        assert_eq!(report.row_counts.rejected, 2);
        assert_eq!(report.rejected_records.count, 2);
        assert_eq!(
            report.rejected_records.artifact,
            Some("artifacts/load-under-test/rejected-records.jsonl".to_string())
        );
    }

    // ---- Reject threshold (ADR-0020, ADR-0036) ----

    fn threshold_definition(
        work: &tempfile::TempDir,
        reject_threshold: Option<u64>,
    ) -> LoadDefinition {
        // Line 3 has the wrong field count, so the load sees one parse
        // rejection out of two records.
        let source_path = work.path().join("customers.csv");
        fs::write(&source_path, "id\n1\n2,extra\n").expect("write csv");
        LoadDefinition {
            version: Some(1),
            source: Some(SourceDefinition {
                connector: "local_file".to_string(),
                path: source_path,
                format: Some("csv".to_string()),
            }),
            destination: Some(DestinationDefinition {
                connector: "parquet".to_string(),
                path: work.path().join("customers_dataset"),
            }),
            dataset: Some("customers".to_string()),
            load_mode: None,
            schema: None,
            reject_threshold,
        }
    }

    #[test]
    fn execute_supported_load_fails_when_rejections_exceed_the_default_threshold() {
        let work = tempfile::TempDir::new().expect("tempdir");
        let definition = threshold_definition(&work, None);

        let failure = execute_supported_load(&definition)
            .err()
            .expect("one rejection exceeds the default threshold of 0");

        assert_eq!(failure.failure.code, "reject_threshold_exceeded");
        assert_eq!(
            failure.failure.message,
            "rejected 1 of 2 records, exceeding the reject threshold of 0"
        );
        // The failure happened after the schema decision and the read, so the
        // report can echo both.
        assert_eq!(
            failure.schema_decision.expect("decision made")["mode"],
            "inferred"
        );
        assert_eq!(failure.source_rows, Some(2));
        assert_eq!(failure.rejected.len(), 1);
        // The threshold gate is side-effect free: no destination was written.
        assert!(!work.path().join("customers_dataset").exists());
    }

    #[test]
    fn execute_supported_load_completes_at_an_explicit_reject_threshold() {
        // One rejection at a threshold of exactly 1: at-or-below completes.
        let work = tempfile::TempDir::new().expect("tempdir");
        let definition = threshold_definition(&work, Some(1));

        let details =
            execute_supported_load(&definition).expect("at-or-below the threshold completes");

        assert_eq!(details.row_counts.source, 2);
        assert_eq!(details.row_counts.written, 1);
        assert_eq!(details.row_counts.rejected, 1);
        assert_eq!(details.rejected.len(), 1);
        assert!(work.path().join("customers_dataset").exists());
    }

    #[test]
    fn execute_supported_load_does_not_persist_the_pin_when_the_threshold_fails() {
        // A first pin-requesting load that fails its reject threshold is a
        // validation failure: it must leave no pinned schema behind
        // (ADR-0036), so a fixed source retries onto a clean bootstrap.
        let work = tempfile::TempDir::new().expect("tempdir");
        let pinned_path = work.path().join("customers.schema.yml");
        let mut definition = threshold_definition(&work, None);
        definition.schema = Some(SchemaConfig {
            pinned_path: Some(pinned_path.clone()),
            drift_policy: None,
        });

        let failure = execute_supported_load(&definition)
            .err()
            .expect("one rejection exceeds the default threshold of 0");

        assert_eq!(failure.failure.code, "reject_threshold_exceeded");
        assert!(
            !pinned_path.exists(),
            "a threshold-failed load must not persist the pin"
        );
    }

    #[test]
    fn load_definition_parses_the_reject_threshold() {
        let definition =
            serde_yaml::from_str::<LoadDefinition>("version: 1\nreject_threshold: 3\n")
                .expect("definition with a reject threshold parses");
        assert_eq!(definition.reject_threshold, Some(3));

        let definition = serde_yaml::from_str::<LoadDefinition>("version: 1\n")
            .expect("definition without a reject threshold parses");
        assert_eq!(definition.reject_threshold, None);

        // A negative threshold is not a count: the definition fails to parse,
        // surfacing as invalid_load_definition_yaml at the load boundary.
        assert!(
            serde_yaml::from_str::<LoadDefinition>("version: 1\nreject_threshold: -1\n").is_err()
        );
    }

    // ---- Schema directive resolution ----

    fn schema_config(pinned_path: Option<&str>, drift_policy: Option<&str>) -> SchemaConfig {
        SchemaConfig {
            pinned_path: pinned_path.map(PathBuf::from),
            drift_policy: drift_policy.map(str::to_string),
        }
    }

    #[test]
    fn resolve_schema_directive_defaults_to_inference_without_a_schema_block() {
        assert!(matches!(
            resolve_schema_directive(None),
            Ok(schema::SchemaDirective::Inferred)
        ));
    }

    #[test]
    fn resolve_schema_directive_requires_a_pinned_path_in_a_schema_block() {
        for config in [
            schema_config(None, None),
            schema_config(None, Some("fail")),
            schema_config(Some(""), None),
        ] {
            let error = resolve_schema_directive(Some(&config))
                .err()
                .expect("schema block without pinned_path rejected");
            assert_eq!(error.code, "invalid_schema_config");
            assert!(error.message.contains("schema.pinned_path is required"));
        }
    }

    #[test]
    fn resolve_schema_directive_rejects_unknown_drift_policies_before_reading_the_pin() {
        // The pin path does not exist: an unknown policy must fail instead of
        // silently bootstrapping.
        let config = schema_config(Some("/does/not/exist.schema.yml"), Some("relaxed"));
        let error = resolve_schema_directive(Some(&config))
            .err()
            .expect("unknown drift policy rejected");
        assert_eq!(error.code, "unsupported_drift_policy");
        assert_eq!(error.message, "unsupported drift policy: relaxed");
    }

    #[test]
    fn resolve_schema_directive_bootstraps_when_the_pin_file_is_absent() {
        let config = schema_config(Some("/does/not/exist/customers.schema.yml"), Some("fail"));
        match resolve_schema_directive(Some(&config)).expect("absent pin bootstraps") {
            schema::SchemaDirective::PinInferred { pinned_path } => {
                assert_eq!(pinned_path, "/does/not/exist/customers.schema.yml");
            }
            _ => panic!("expected the PinInferred directive"),
        }
    }

    #[test]
    fn resolve_schema_directive_surfaces_an_invalid_pinned_schema_file() {
        let work = tempfile::TempDir::new().expect("tempdir");
        let pinned_path = work.path().join("customers.schema.yml");
        fs::write(
            &pinned_path,
            "version: 7\nfields:\n- name: id\n  type: int64\n",
        )
        .expect("write pin");
        let config = schema_config(Some(pinned_path.to_str().expect("utf8 path")), None);

        let error = resolve_schema_directive(Some(&config))
            .err()
            .expect("invalid pin rejected");
        assert_eq!(error.code, "invalid_pinned_schema");
        assert!(error
            .message
            .contains("unsupported pinned schema version: 7"));
    }

    #[test]
    fn resolve_schema_directive_loads_an_existing_pin_with_the_declared_policy() {
        let work = tempfile::TempDir::new().expect("tempdir");
        let pinned_path = work.path().join("customers.schema.yml");
        fs::write(
            &pinned_path,
            "version: 1\nfields:\n- name: id\n  type: int64\n",
        )
        .expect("write pin");
        let config = schema_config(
            Some(pinned_path.to_str().expect("utf8 path")),
            Some("allow_additive_nullable"),
        );

        match resolve_schema_directive(Some(&config)).expect("existing pin loads") {
            schema::SchemaDirective::Pinned {
                pinned_path: reported_path,
                drift_policy: schema::DriftPolicy::AllowAdditiveNullable,
                ..
            } => assert_eq!(reported_path, pinned_path.display().to_string()),
            _ => panic!("expected the Pinned directive with the additive policy"),
        }
    }

    #[test]
    fn without_context_reports_empty_summaries_and_the_default_load_mode() {
        let failure = ReportableFailure::without_context(
            "load_definition_read_failed",
            "failed to read load definition: file is gone".to_string(),
        );

        assert_eq!(failure.source_summary, json!({}));
        assert_eq!(failure.destination_summary, json!({}));
        assert_eq!(failure.dataset, None);
        assert_eq!(failure.load_mode, "full_refresh");
        assert_eq!(failure.schema_decision, json!({ "mode": "not_evaluated" }));
        assert_eq!(failure.code, "load_definition_read_failed");
        assert_eq!(
            failure.message,
            "failed to read load definition: file is gone"
        );
    }
}
