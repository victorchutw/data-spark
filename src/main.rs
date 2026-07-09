use arrow_array::builder::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use clap::{Args, Parser, Subcommand};
use parquet::arrow::ArrowWriter;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
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

struct SourceRecords {
    field_names: Vec<String>,
    records: Vec<Vec<Option<String>>>,
    inferred_types: Vec<InferredType>,
    source_bytes: u64,
}

struct SourceBatch {
    batch: RecordBatch,
    source_bytes: u64,
}

struct DestinationStats {
    bytes_written: u64,
}

struct LoadFailure {
    code: &'static str,
    message: String,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum InferredType {
    Null,
    Boolean,
    Int64,
    Float64,
    Utf8,
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

    let records = match source_format(source).as_str() {
        "csv" => read_local_csv(&source.path)?,
        "jsonl" => read_local_jsonl(&source.path)?,
        _ => {
            return Err(LoadFailure {
                code: "unsupported_source_format",
                message: "only local CSV and JSONL sources are supported by this load path"
                    .to_string(),
            })
        }
    };

    if destination.connector != "parquet" {
        return Err(LoadFailure {
            code: "unsupported_destination_connector",
            message: format!(
                "unsupported destination connector: {}",
                destination.connector
            ),
        });
    }

    let source_batch = records_to_batch(records)?;
    let destination_stats =
        write_parquet_full_refresh(&destination.path, &source_batch.batch, load_id)?;
    let row_count = source_batch.batch.num_rows() as u64;

    Ok(ExecutionDetails {
        source_summary: to_json(source),
        destination_summary: to_json(destination),
        load_mode,
        schema_decision: inferred_schema_decision(source_batch.batch.schema().as_ref()),
        row_counts: RowCounts {
            source: row_count,
            written: row_count,
            rejected: 0,
        },
        byte_counts: ByteCounts {
            source: Some(source_batch.source_bytes),
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

fn read_local_csv(source_path: &Path) -> Result<SourceRecords, LoadFailure> {
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

    // CSV fields arrive untyped, so their types are inferred from the text values.
    let inferred_types = infer_types(field_names.len(), &records, infer_text_type);

    Ok(SourceRecords {
        field_names,
        records,
        inferred_types,
        source_bytes,
    })
}

fn read_local_jsonl(source_path: &Path) -> Result<SourceRecords, LoadFailure> {
    let source_text = fs::read_to_string(source_path).map_err(|error| LoadFailure {
        code: "source_read_failed",
        message: format!(
            "failed to read JSONL source {}: {error}",
            source_path.display()
        ),
    })?;
    let source_bytes = source_text.len() as u64;

    let mut field_names: Vec<String> = Vec::new();
    let mut seen_fields: HashSet<String> = HashSet::new();
    let mut objects: Vec<serde_json::Map<String, Value>> = Vec::new();
    for (line_index, line) in source_text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let value = serde_json::from_str::<Value>(line)
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

    // JSON values carry their own type, so a field's type is inferred from the
    // observed JSON kinds rather than by re-parsing stringified values. This keeps
    // JSON strings like "01234" as text instead of retyping them as numbers.
    let mut inferred_types = vec![InferredType::Null; field_names.len()];
    for object in &objects {
        for (column_index, field_name) in field_names.iter().enumerate() {
            if let Some(value) = object.get(field_name) {
                inferred_types[column_index] =
                    inferred_types[column_index].merge(infer_json_type(value));
            }
        }
    }
    let inferred_types = inferred_types
        .into_iter()
        .map(default_null_to_text)
        .collect::<Vec<_>>();

    let records = objects
        .iter()
        .map(|object| {
            field_names
                .iter()
                .map(|field_name| json_scalar_to_string(object.get(field_name)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    Ok(SourceRecords {
        field_names,
        records,
        inferred_types,
        source_bytes,
    })
}

fn json_scalar_to_string(value: Option<&Value>) -> Option<String> {
    match value {
        None | Some(Value::Null) => None,
        Some(Value::String(text)) => Some(text.clone()),
        Some(Value::Bool(flag)) => Some(flag.to_string()),
        Some(Value::Number(number)) => Some(number.to_string()),
        Some(other) => Some(other.to_string()),
    }
}

fn records_to_batch(source_records: SourceRecords) -> Result<SourceBatch, LoadFailure> {
    let SourceRecords {
        field_names,
        records,
        inferred_types,
        source_bytes,
    } = source_records;

    let schema = Arc::new(Schema::new(
        field_names
            .iter()
            .zip(inferred_types.iter())
            .map(|(name, inferred_type)| Field::new(name, inferred_type.data_type(), true))
            .collect::<Vec<_>>(),
    ));
    let columns = inferred_types
        .iter()
        .enumerate()
        .map(|(column_index, inferred_type)| build_array(*inferred_type, &records, column_index))
        .collect::<Result<Vec<_>, _>>()?;
    let batch = RecordBatch::try_new(schema, columns).map_err(|error| LoadFailure {
        code: "record_batch_creation_failed",
        message: format!("failed to create Arrow record batch: {error}"),
    })?;

    Ok(SourceBatch {
        batch,
        source_bytes,
    })
}

fn infer_types(
    field_count: usize,
    records: &[Vec<Option<String>>],
    observe: fn(&str) -> InferredType,
) -> Vec<InferredType> {
    let mut inferred_types = vec![InferredType::Null; field_count];
    for record in records {
        for (column_index, value) in record.iter().enumerate() {
            if let Some(value) = value {
                inferred_types[column_index] = inferred_types[column_index].merge(observe(value));
            }
        }
    }

    inferred_types
        .into_iter()
        .map(default_null_to_text)
        .collect()
}

fn default_null_to_text(inferred_type: InferredType) -> InferredType {
    if inferred_type == InferredType::Null {
        InferredType::Utf8
    } else {
        inferred_type
    }
}

fn build_array(
    inferred_type: InferredType,
    records: &[Vec<Option<String>>],
    column_index: usize,
) -> Result<ArrayRef, LoadFailure> {
    match inferred_type {
        InferredType::Null | InferredType::Utf8 => {
            let mut builder = StringBuilder::new();
            for record in records {
                match &record[column_index] {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        InferredType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for record in records {
                match &record[column_index] {
                    Some(value) => builder.append_value(
                        parse_bool(value)
                            .ok_or_else(|| coercion_failure(column_index, value, "boolean"))?,
                    ),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        InferredType::Int64 => {
            let mut builder = Int64Builder::new();
            for record in records {
                match &record[column_index] {
                    Some(value) => builder.append_value(
                        value
                            .parse::<i64>()
                            .map_err(|_| coercion_failure(column_index, value, "int64"))?,
                    ),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        InferredType::Float64 => {
            let mut builder = Float64Builder::new();
            for record in records {
                match &record[column_index] {
                    Some(value) => builder.append_value(
                        value
                            .parse::<f64>()
                            .map_err(|_| coercion_failure(column_index, value, "float64"))?,
                    ),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
    }
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

fn coercion_failure(column_index: usize, value: &str, data_type: &str) -> LoadFailure {
    LoadFailure {
        code: "schema_coercion_failed",
        message: format!(
            "failed to coerce column {} value {:?} to {data_type}",
            column_index + 1,
            value
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

fn inferred_schema_decision(schema: &Schema) -> Value {
    json!({
        "mode": "inferred",
        "fields": schema
            .fields()
            .iter()
            .map(|field| {
                json!({
                    "name": field.name(),
                    "type": data_type_name(field.data_type()),
                    "nullable": field.is_nullable()
                })
            })
            .collect::<Vec<_>>()
    })
}

fn data_type_name(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Boolean => "boolean",
        DataType::Int64 => "int64",
        DataType::Float64 => "float64",
        DataType::Utf8 => "utf8",
        _ => "unsupported",
    }
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

impl InferredType {
    /// Widens two observed types to the narrowest type that can hold both.
    ///
    /// `Null` is the identity (an absent or null value constrains nothing),
    /// integers widen to floats when mixed, and any other disagreement falls
    /// back to text. Both source readers share this lattice so CSV and JSONL
    /// produce schemas the same way.
    fn merge(self, other: Self) -> Self {
        match (self, other) {
            (InferredType::Null, other) => other,
            (current, InferredType::Null) => current,
            (current, other) if current == other => current,
            (InferredType::Int64, InferredType::Float64)
            | (InferredType::Float64, InferredType::Int64) => InferredType::Float64,
            _ => InferredType::Utf8,
        }
    }

    fn data_type(self) -> DataType {
        match self {
            InferredType::Null | InferredType::Utf8 => DataType::Utf8,
            InferredType::Boolean => DataType::Boolean,
            InferredType::Int64 => DataType::Int64,
            InferredType::Float64 => DataType::Float64,
        }
    }
}

/// Observes the type carried by a text value, as CSV fields have no other type
/// information than how they parse.
fn infer_text_type(value: &str) -> InferredType {
    if parse_bool(value).is_some() {
        InferredType::Boolean
    } else if value.parse::<i64>().is_ok() {
        InferredType::Int64
    } else if value.parse::<f64>().is_ok() {
        InferredType::Float64
    } else {
        InferredType::Utf8
    }
}

/// Observes the type a JSON value already declares, so JSON strings stay text
/// even when they look numeric and nested values degrade to text.
fn infer_json_type(value: &Value) -> InferredType {
    match value {
        Value::Null => InferredType::Null,
        Value::Bool(_) => InferredType::Boolean,
        Value::Number(number) => {
            if number.is_i64() {
                InferredType::Int64
            } else {
                InferredType::Float64
            }
        }
        Value::String(_) | Value::Array(_) | Value::Object(_) => InferredType::Utf8,
    }
}

fn parse_bool(value: &str) -> Option<bool> {
    match value {
        "true" | "TRUE" | "True" => Some(true),
        "false" | "FALSE" | "False" => Some(false),
        _ => None,
    }
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
