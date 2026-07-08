use clap::{Args, Parser, Subcommand};
use serde::Serialize;
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

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
    timings: Timings,
    exit_status: &'static str,
    process_exit_code: u8,
    error_summary: Option<ErrorSummary>,
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

struct LoadOutcome {
    load_id: String,
    artifact_dir: PathBuf,
    report_path: PathBuf,
    source_summary: Value,
    destination_summary: Value,
    load_mode: String,
    exit_status: &'static str,
    process_exit_code: u8,
    error_summary: Option<ErrorSummary>,
}

fn main() -> ExitCode {
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

    let (source_summary, destination_summary, load_mode, error_summary) =
        match fs::read_to_string(&args.definition) {
            Ok(definition_text) => match serde_yaml::from_str::<Value>(&definition_text) {
                Ok(definition) => {
                    let source_summary = object_or_empty(definition.get("source"));
                    let destination_summary = object_or_empty(definition.get("destination"));
                    let load_mode = definition
                        .get("load_mode")
                        .and_then(Value::as_str)
                        .unwrap_or("full_refresh")
                        .to_string();
                    let error_summary = match definition.get("version").and_then(Value::as_u64) {
                        Some(SUPPORTED_LOAD_DEFINITION_VERSION) => None,
                        Some(version) => Some(ErrorSummary {
                            code: "unsupported_load_definition_version",
                            message: format!("unsupported load definition version: {version}"),
                        }),
                        None => Some(ErrorSummary {
                            code: "missing_load_definition_version",
                            message: "load definition version is required".to_string(),
                        }),
                    };
                    (
                        source_summary,
                        destination_summary,
                        load_mode,
                        error_summary,
                    )
                }
                Err(error) => (
                    json!({}),
                    json!({}),
                    "full_refresh".to_string(),
                    Some(ErrorSummary {
                        code: "invalid_load_definition_yaml",
                        message: format!("failed to parse load definition: {error}"),
                    }),
                ),
            },
            Err(error) => (
                json!({}),
                json!({}),
                "full_refresh".to_string(),
                Some(ErrorSummary {
                    code: "load_definition_read_failed",
                    message: format!("failed to read load definition: {error}"),
                }),
            ),
        };

    let (exit_status, process_exit_code) = if error_summary.is_some() {
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
        source_summary: source_summary.clone(),
        destination_summary: destination_summary.clone(),
        load_mode: load_mode.clone(),
        timings,
        exit_status,
        process_exit_code,
        error_summary,
    };

    let report_path = artifact_dir.join("load-report.json");
    fs::write(&report_path, serde_json::to_vec_pretty(&report)?)?;

    Ok(LoadOutcome {
        load_id,
        artifact_dir,
        report_path,
        source_summary,
        destination_summary,
        load_mode,
        exit_status,
        process_exit_code,
        error_summary: report.error_summary,
    })
}

fn object_or_empty(value: Option<&Value>) -> Value {
    value.cloned().unwrap_or_else(|| json!({}))
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
