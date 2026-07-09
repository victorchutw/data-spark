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

#[derive(Clone, Serialize)]
struct RowCounts {
    source: u64,
    written: u64,
    rejected: u64,
}

#[derive(Clone, Serialize)]
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

#[derive(Clone, Serialize)]
struct ErrorSummary {
    code: &'static str,
    message: String,
}

struct LoadOutcome {
    load_id: String,
    artifact_dir: PathBuf,
    report_path: PathBuf,
    source_summary: Value,
    destination_summary: Value,
    load_mode: String,
    row_counts: RowCounts,
    exit_status: &'static str,
    process_exit_code: u8,
    error_summary: Option<ErrorSummary>,
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

struct ExecutionDetails {
    source_summary: Value,
    destination_summary: Value,
    load_mode: String,
    schema_decision: Value,
    row_counts: RowCounts,
    byte_counts: ByteCounts,
    destination_write: Value,
    execution: Value,
    error_summary: Option<ErrorSummary>,
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
            Ok(outcome) => {
                print_summary(&outcome);
                ExitCode::from(outcome.process_exit_code)
            }
            Err(error) => {
                eprintln!("data-spark: {error}");
                ExitCode::from(1)
            }
        },
    }
}

fn run_load(args: LoadArgs) -> Result<LoadOutcome, Box<dyn std::error::Error>> {
    let started_unix_ms = unix_ms();
    let load_id = Uuid::new_v4().to_string();
    let artifact_root = args
        .output_dir
        .unwrap_or_else(|| PathBuf::from(".data-spark").join("runs"));
    let artifact_dir = artifact_root.join(&load_id);
    fs::create_dir_all(&artifact_dir)?;

    let details = execute_load_definition(&args.definition);

    let (exit_status, process_exit_code) = if details.error_summary.is_some() {
        ("failed", 1)
    } else {
        ("succeeded", 0)
    };

    let finished_unix_ms = unix_ms();
    let timings = Timings {
        started_unix_ms,
        finished_unix_ms,
        duration_ms: finished_unix_ms.saturating_sub(started_unix_ms),
    };
    let report = LoadReport {
        report_version: LOAD_REPORT_VERSION,
        load_id: load_id.clone(),
        artifact_dir: path_string(&artifact_dir),
        source_summary: details.source_summary.clone(),
        destination_summary: details.destination_summary.clone(),
        load_mode: details.load_mode.clone(),
        schema_decision: details.schema_decision.clone(),
        row_counts: details.row_counts.clone(),
        byte_counts: details.byte_counts.clone(),
        destination_write: details.destination_write.clone(),
        execution: details.execution.clone(),
        timings,
        exit_status,
        process_exit_code,
        error_summary: details.error_summary.clone(),
    };

    let report_path = artifact_dir.join("load-report.json");
    fs::write(&report_path, serde_json::to_vec_pretty(&report)?)?;

    Ok(LoadOutcome {
        load_id,
        artifact_dir,
        report_path,
        source_summary: report.source_summary,
        destination_summary: report.destination_summary,
        load_mode: report.load_mode,
        row_counts: report.row_counts,
        exit_status,
        process_exit_code,
        error_summary: report.error_summary,
    })
}

fn execute_load_definition(definition_path: &Path) -> ExecutionDetails {
    let definition_text = match fs::read_to_string(definition_path) {
        Ok(definition_text) => definition_text,
        Err(error) => {
            return failed_details(
                json!({}),
                json!({}),
                "full_refresh".to_string(),
                "load_definition_read_failed",
                format!("failed to read load definition: {error}"),
            )
        }
    };

    let definition = match serde_yaml::from_str::<LoadDefinition>(&definition_text) {
        Ok(definition) => definition,
        Err(error) => {
            return failed_details(
                json!({}),
                json!({}),
                "full_refresh".to_string(),
                "invalid_load_definition_yaml",
                format!("failed to parse load definition: {error}"),
            )
        }
    };

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
            return failed_details(
                source_summary,
                destination_summary,
                load_mode,
                "unsupported_load_definition_version",
                format!("unsupported load definition version: {version}"),
            )
        }
        None => {
            return failed_details(
                source_summary,
                destination_summary,
                load_mode,
                "missing_load_definition_version",
                "load definition version is required".to_string(),
            )
        }
    }

    match execute_supported_load(&definition) {
        Ok(details) => details,
        Err(failure) => failed_details(
            source_summary,
            destination_summary,
            load_mode,
            failure.code,
            failure.message,
        ),
    }
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
        error_summary: None,
    })
}

fn failed_details(
    source_summary: Value,
    destination_summary: Value,
    load_mode: String,
    code: &'static str,
    message: String,
) -> ExecutionDetails {
    ExecutionDetails {
        source_summary,
        destination_summary,
        load_mode,
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
        error_summary: Some(ErrorSummary { code, message }),
    }
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

fn print_summary(outcome: &LoadOutcome) {
    println!("Data Spark load {}", outcome.load_id);
    println!("Status: {}", outcome.exit_status);
    println!("Load mode: {}", outcome.load_mode);
    println!("Source: {}", summary_text(&outcome.source_summary));
    println!(
        "Destination: {}",
        summary_text(&outcome.destination_summary)
    );
    println!("Records read: {}", outcome.row_counts.source);
    println!("Records written: {}", outcome.row_counts.written);
    println!("Artifact directory: {}", path_string(&outcome.artifact_dir));
    println!("Load report: {}", path_string(&outcome.report_path));
    if let Some(error) = &outcome.error_summary {
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
