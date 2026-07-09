use arrow_array::RecordBatch;
use clap::{Args, Parser, Subcommand};
use parquet::arrow::ArrowWriter;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

mod schema;

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

struct CsvRecords {
    field_names: Vec<String>,
    records: Vec<Vec<Option<String>>>,
    source_bytes: u64,
}

struct JsonlRecords {
    field_names: Vec<String>,
    objects: Vec<serde_json::Map<String, Value>>,
    source_bytes: u64,
}

struct DestinationStats {
    bytes_written: u64,
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

    let details = execute_load_definition(&args.definition, &load_id);

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

fn execute_load_definition(definition_path: &Path, load_id: &str) -> ExecutionDetails {
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

    match execute_supported_load(&definition, load_id) {
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

fn execute_supported_load(
    definition: &LoadDefinition,
    load_id: &str,
) -> Result<ExecutionDetails, LoadFailure> {
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

    if load_mode != "full_refresh" {
        return Err(LoadFailure {
            code: "unsupported_load_mode",
            message: format!("unsupported load mode: {load_mode}"),
        });
    }

    if source.connector != "local_file" {
        return Err(LoadFailure {
            code: "unsupported_source_connector",
            message: format!("unsupported source connector: {}", source.connector),
        });
    }

    if destination.connector != "parquet" {
        return Err(LoadFailure {
            code: "unsupported_destination_connector",
            message: format!(
                "unsupported destination connector: {}",
                destination.connector
            ),
        });
    }

    // Validate connectors and load rules before reading the source so an
    // unsupported destination fails without doing source I/O (ADR 0019).
    let (materialized, source_bytes) = match source_format(source).as_str() {
        "csv" => {
            let CsvRecords {
                field_names,
                records,
                source_bytes,
            } = read_local_csv(&source.path)?;
            (
                schema::from_text_columns(field_names, records)?,
                source_bytes,
            )
        }
        "jsonl" => {
            let JsonlRecords {
                field_names,
                objects,
                source_bytes,
            } = read_local_jsonl(&source.path)?;
            (
                schema::from_json_columns(field_names, objects)?,
                source_bytes,
            )
        }
        _ => {
            return Err(LoadFailure {
                code: "unsupported_source_format",
                message: "only local CSV and JSONL sources are supported by this load path"
                    .to_string(),
            })
        }
    };

    let destination_stats =
        write_parquet_full_refresh(&destination.path, &materialized.batch, load_id)?;
    let row_count = materialized.batch.num_rows() as u64;

    Ok(ExecutionDetails {
        source_summary: to_json(source),
        destination_summary: to_json(destination),
        load_mode,
        schema_decision: materialized.schema_decision,
        row_counts: RowCounts {
            source: row_count,
            written: row_count,
            rejected: 0,
        },
        byte_counts: ByteCounts {
            source: Some(source_bytes),
            destination: Some(destination_stats.bytes_written),
        },
        destination_write: json!({
            "atomicity": "best_effort",
            "strategy": "staging_then_replace"
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

fn read_local_csv(source_path: &Path) -> Result<CsvRecords, LoadFailure> {
    let file = File::open(source_path).map_err(|error| LoadFailure {
        code: "source_read_failed",
        message: format!(
            "failed to read CSV source {}: {error}",
            source_path.display()
        ),
    })?;
    let source_bytes = file
        .metadata()
        .map_err(|error| LoadFailure {
            code: "source_read_failed",
            message: format!(
                "failed to inspect CSV source {}: {error}",
                source_path.display()
            ),
        })?
        .len();

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(false)
        .from_reader(file);
    let field_names = reader
        .headers()
        .map_err(|error| malformed_csv(source_path, error))?
        .iter()
        .map(str::to_string)
        .collect::<Vec<_>>();

    if field_names.is_empty() {
        return Err(LoadFailure {
            code: "malformed_csv",
            message: format!(
                "CSV source {} must include at least one header field",
                source_path.display()
            ),
        });
    }

    let mut records = Vec::new();
    for record in reader.records() {
        let record = record.map_err(|error| malformed_csv(source_path, error))?;
        records.push(
            record
                .iter()
                .map(|value| {
                    if value.is_empty() {
                        None
                    } else {
                        Some(value.to_string())
                    }
                })
                .collect::<Vec<_>>(),
        );
    }

    Ok(CsvRecords {
        field_names,
        records,
        source_bytes,
    })
}

fn read_local_jsonl(source_path: &Path) -> Result<JsonlRecords, LoadFailure> {
    let file = File::open(source_path).map_err(|error| LoadFailure {
        code: "source_read_failed",
        message: format!(
            "failed to read JSONL source {}: {error}",
            source_path.display()
        ),
    })?;
    let source_bytes = file
        .metadata()
        .map_err(|error| LoadFailure {
            code: "source_read_failed",
            message: format!(
                "failed to inspect JSONL source {}: {error}",
                source_path.display()
            ),
        })?
        .len();

    // Read one record per line so peak memory tracks the parsed records rather
    // than an extra whole-file string copy, matching the CSV reader.
    let mut field_names: Vec<String> = Vec::new();
    let mut seen_fields: HashSet<String> = HashSet::new();
    let mut objects: Vec<serde_json::Map<String, Value>> = Vec::new();
    for (line_index, line) in BufReader::new(file).lines().enumerate() {
        let line = line.map_err(|error| LoadFailure {
            code: "source_read_failed",
            message: format!(
                "failed to read JSONL source {} line {}: {error}",
                source_path.display(),
                line_index + 1
            ),
        })?;
        if line.trim().is_empty() {
            continue;
        }
        let value = serde_json::from_str::<Value>(&line)
            .map_err(|error| malformed_jsonl(source_path, line_index + 1, error.to_string()))?;
        let object = match value {
            Value::Object(object) => object,
            _ => {
                return Err(malformed_jsonl(
                    source_path,
                    line_index + 1,
                    "each JSONL record must be a JSON object".to_string(),
                ))
            }
        };
        for key in object.keys() {
            if seen_fields.insert(key.clone()) {
                field_names.push(key.clone());
            }
        }
        objects.push(object);
    }

    if field_names.is_empty() {
        return Err(LoadFailure {
            code: "malformed_jsonl",
            message: format!(
                "JSONL source {} must include at least one record with fields",
                source_path.display()
            ),
        });
    }

    Ok(JsonlRecords {
        field_names,
        objects,
        source_bytes,
    })
}

fn write_parquet_full_refresh(
    destination_path: &Path,
    batch: &RecordBatch,
    load_id: &str,
) -> Result<DestinationStats, LoadFailure> {
    let parent = destination_path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|error| LoadFailure {
        code: "destination_write_failed",
        message: format!(
            "failed to prepare destination parent {}: {error}",
            parent.display()
        ),
    })?;

    let destination_name = destination_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("dataset");
    let staging_path = parent.join(format!(".{destination_name}.data-spark-staging-{load_id}"));
    remove_path_if_exists(&staging_path).map_err(|error| LoadFailure {
        code: "destination_write_failed",
        message: format!(
            "failed to clear staging destination {}: {error}",
            staging_path.display()
        ),
    })?;
    fs::create_dir_all(&staging_path).map_err(|error| LoadFailure {
        code: "destination_write_failed",
        message: format!(
            "failed to create staging destination {}: {error}",
            staging_path.display()
        ),
    })?;

    let parquet_file_path = staging_path.join("part-00000.parquet");
    let file = File::create(&parquet_file_path).map_err(|error| LoadFailure {
        code: "destination_write_failed",
        message: format!(
            "failed to create Parquet file {}: {error}",
            parquet_file_path.display()
        ),
    })?;
    let mut writer =
        ArrowWriter::try_new(file, batch.schema(), None).map_err(|error| LoadFailure {
            code: "destination_write_failed",
            message: format!("failed to initialize Parquet writer: {error}"),
        })?;
    writer.write(batch).map_err(|error| LoadFailure {
        code: "destination_write_failed",
        message: format!("failed to write Parquet records: {error}"),
    })?;
    writer.close().map_err(|error| LoadFailure {
        code: "destination_write_failed",
        message: format!("failed to close Parquet writer: {error}"),
    })?;

    remove_path_if_exists(destination_path).map_err(|error| LoadFailure {
        code: "destination_write_failed",
        message: format!(
            "failed to replace existing destination {}: {error}",
            destination_path.display()
        ),
    })?;
    fs::rename(&staging_path, destination_path).map_err(|error| LoadFailure {
        code: "destination_write_failed",
        message: format!(
            "failed to commit Parquet destination {}: {error}",
            destination_path.display()
        ),
    })?;

    let bytes_written = directory_bytes(destination_path).map_err(|error| LoadFailure {
        code: "destination_write_failed",
        message: format!(
            "failed to inspect Parquet destination {}: {error}",
            destination_path.display()
        ),
    })?;
    Ok(DestinationStats { bytes_written })
}

fn malformed_csv(source_path: &Path, error: csv::Error) -> LoadFailure {
    LoadFailure {
        code: "malformed_csv",
        message: format!("malformed CSV syntax in {}: {error}", source_path.display()),
    }
}

fn malformed_jsonl(source_path: &Path, line_number: usize, detail: String) -> LoadFailure {
    LoadFailure {
        code: "malformed_jsonl",
        message: format!(
            "malformed JSONL record in {} line {line_number}: {detail}",
            source_path.display()
        ),
    }
}

fn source_format(source: &SourceDefinition) -> String {
    source.format.clone().unwrap_or_else(|| {
        source
            .path
            .extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or("")
            .to_string()
    })
}

fn to_json<T: Serialize>(value: &T) -> Value {
    serde_json::to_value(value).unwrap_or_else(|_| json!({}))
}

fn remove_path_if_exists(path: &Path) -> io::Result<()> {
    if !path.exists() {
        return Ok(());
    }

    if path.is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    }
}

fn directory_bytes(path: &Path) -> io::Result<u64> {
    let mut total = 0;
    for entry in fs::read_dir(path)? {
        let path = entry?.path();
        let metadata = fs::metadata(&path)?;
        if metadata.is_dir() {
            total += directory_bytes(&path)?;
        } else {
            total += metadata.len();
        }
    }
    Ok(total)
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
