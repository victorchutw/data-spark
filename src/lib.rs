use clap::{Args, Parser, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

mod connector;
mod schema;

use connector::{destination_connector, source_connector, DestinationWrite, LoadMode, SourceRead};

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
    load_mode: String,
    schema_decision: Value,
    row_counts: RowCounts,
    byte_counts: ByteCounts,
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
        LoadReport {
            report_version: LOAD_REPORT_VERSION,
            load_id,
            artifact_dir,
            source_summary: details.source_summary,
            destination_summary: details.destination_summary,
            load_mode: details.load_mode,
            schema_decision: details.schema_decision,
            row_counts: details.row_counts,
            byte_counts: details.byte_counts,
            destination_write: details.destination_write,
            execution: details.execution,
            timings,
            exit_status: "succeeded",
            process_exit_code: 0,
            error_summary: None,
        }
    }

    /// Assemble the report for a load that failed: the definition context is
    /// echoed back and every execution field takes its not-run posture.
    fn from_failure(
        load_id: String,
        artifact_dir: String,
        timings: Timings,
        failure: ReportableFailure,
    ) -> Self {
        LoadReport {
            report_version: LOAD_REPORT_VERSION,
            load_id,
            artifact_dir,
            source_summary: failure.source_summary,
            destination_summary: failure.destination_summary,
            load_mode: failure.load_mode,
            schema_decision: json!({ "mode": "not_evaluated" }),
            row_counts: RowCounts {
                source: 0,
                written: 0,
                rejected: 0,
            },
            byte_counts: ByteCounts {
                source: None,
                destination: None,
            },
            destination_write: json!({
                "atomicity": "not_applicable"
            }),
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
/// [`ReportableFailure`] instead.
struct ExecutionDetails {
    source_summary: Value,
    destination_summary: Value,
    load_mode: String,
    schema_decision: Value,
    row_counts: RowCounts,
    byte_counts: ByteCounts,
    destination_write: Value,
    execution: Value,
}

/// A failure joined with the load definition context (source, destination,
/// load mode) that a load report echoes back.
struct ReportableFailure {
    source_summary: Value,
    destination_summary: Value,
    load_mode: String,
    code: &'static str,
    message: String,
}

impl ReportableFailure {
    /// For failures raised before the load definition is parsed (read / YAML),
    /// when no definition context exists yet: empty summaries and the default
    /// load mode.
    fn without_context(code: &'static str, message: String) -> Self {
        ReportableFailure {
            source_summary: json!({}),
            destination_summary: json!({}),
            load_mode: "full_refresh".to_string(),
            code,
            message,
        }
    }
}

#[derive(Debug)]
struct LoadFailure {
    code: &'static str,
    message: String,
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
                load_mode,
                code: "unsupported_load_definition_version",
                message: format!("unsupported load definition version: {version}"),
            })
        }
        None => {
            return Err(ReportableFailure {
                source_summary,
                destination_summary,
                load_mode,
                code: "missing_load_definition_version",
                message: "load definition version is required".to_string(),
            })
        }
    }

    execute_supported_load(&definition).map_err(|failure| ReportableFailure {
        source_summary,
        destination_summary,
        load_mode,
        code: failure.code,
        message: failure.message,
    })
}

fn execute_supported_load(definition: &LoadDefinition) -> Result<ExecutionDetails, LoadFailure> {
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

    // Validate the load mode and both connectors before any I/O so an
    // unsupported definition fails without reading the source or touching the
    // destination (ADR-0019), preserving the current error precedence: load
    // mode -> source connector -> destination connector -> read -> write. The
    // source format is validated inside read() so its precedence stays after the
    // destination-connector check for a doubly-invalid definition.
    let mode = LoadMode::parse(&load_mode)?;
    let source_port = source_connector(source)?;
    let destination_port = destination_connector(destination)?;

    let SourceRead {
        batch,
        schema_decision,
        source_bytes,
    } = source_port.read()?;
    let DestinationWrite {
        bytes_written,
        atomicity,
        strategy,
    } = destination_port.write(&batch, mode)?;
    let row_count = batch.num_rows() as u64;

    Ok(ExecutionDetails {
        source_summary: to_json(source),
        destination_summary: to_json(destination),
        load_mode,
        schema_decision,
        row_counts: RowCounts {
            source: row_count,
            written: row_count,
            rejected: 0,
        },
        byte_counts: ByteCounts {
            source: Some(source_bytes),
            destination: Some(bytes_written),
        },
        destination_write: json!({
            "atomicity": atomicity,
            "strategy": strategy
        }),
        execution: json!({
            "record_format": "arrow_record_batch",
            "batch_count": 1
        }),
    })
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
    println!("Artifact directory: {}", report.artifact_dir);
    println!("Load report: {}", path_string(&report_path));
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

    #[test]
    fn from_success_reports_the_success_posture_around_the_execution_facts() {
        let details = ExecutionDetails {
            source_summary: json!({ "connector": "local_file" }),
            destination_summary: json!({ "connector": "parquet" }),
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
            destination_write: json!({
                "atomicity": "best_effort",
                "strategy": "replace_directory"
            }),
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
        assert_eq!(report.load_mode, "full_refresh");
        assert_eq!(report.schema_decision, json!({ "mode": "inferred" }));
        assert_eq!(report.row_counts.source, 2);
        assert_eq!(report.row_counts.written, 2);
        assert_eq!(report.row_counts.rejected, 0);
        assert_eq!(report.byte_counts.source, Some(64));
        assert_eq!(report.byte_counts.destination, Some(128));
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
    fn from_failure_reports_the_failure_posture_around_the_error() {
        let failure = ReportableFailure {
            source_summary: json!({ "connector": "local_file" }),
            destination_summary: json!({ "connector": "parquet" }),
            load_mode: "full_refresh".to_string(),
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
        assert_eq!(report.load_mode, "full_refresh");
        assert_eq!(report.schema_decision, json!({ "mode": "not_evaluated" }));
        assert_eq!(report.row_counts.source, 0);
        assert_eq!(report.row_counts.written, 0);
        assert_eq!(report.row_counts.rejected, 0);
        assert_eq!(report.byte_counts.source, None);
        assert_eq!(report.byte_counts.destination, None);
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
    fn without_context_reports_empty_summaries_and_the_default_load_mode() {
        let failure = ReportableFailure::without_context(
            "load_definition_read_failed",
            "failed to read load definition: file is gone".to_string(),
        );

        assert_eq!(failure.source_summary, json!({}));
        assert_eq!(failure.destination_summary, json!({}));
        assert_eq!(failure.load_mode, "full_refresh");
        assert_eq!(failure.code, "load_definition_read_failed");
        assert_eq!(
            failure.message,
            "failed to read load definition: file is gone"
        );
    }
}
