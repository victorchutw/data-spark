use clap::{Args, Parser, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::ffi::OsString;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

mod connector;
mod dispatch;
mod rejection;
mod retry;
mod schema;

use connector::{
    destination_connector, resolved_source_format, source_connector, DestinationWriteFacts,
    LoadMode, SourceRead,
};
use dispatch::WritePhaseFailure;

const LOAD_REPORT_VERSION: u8 = 1;
/// The version of the binary itself (`CARGO_PKG_VERSION`), printed by
/// `--version` and echoed in every load report as the top-level
/// `binary_version` provenance field (ADR-0055).
const BINARY_VERSION: &str = env!("CARGO_PKG_VERSION");
const LOAD_REPORT_FILENAME: &str = "load-report.json";
const SUPPORTED_LOAD_DEFINITION_VERSION: u64 = 1;
/// The default chunk bound of `execution.chunk_rows` (ADR-0046): loads
/// materialize `RecordBatch` chunks of at most this many surviving records.
const DEFAULT_CHUNK_ROWS: u64 = 65536;

#[derive(Parser)]
#[command(name = "data-spark")]
#[command(version)]
#[command(about = "Portable data movement CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a load from a YAML load definition
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
    binary_version: &'static str,
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
        let rejected_records = RejectedRecordFacts::facts(details.rejected_count, &artifact_dir);
        LoadReport {
            report_version: LOAD_REPORT_VERSION,
            binary_version: BINARY_VERSION,
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
    /// (and their artifact) when the failure happened, and — once the load
    /// entered the write phase — the execution posture, which reports the
    /// chunks the destination had committed (ADR-0047) and the retry story
    /// (ADR-0050). The `not_started` posture states a retry story only when
    /// attempts were actually recorded, so every never-retried failure
    /// report keeps its established shape.
    fn from_failure(
        load_id: String,
        artifact_dir: String,
        timings: Timings,
        failure: ReportableFailure,
    ) -> Self {
        let rejected_records = RejectedRecordFacts::facts(failure.rejected_count, &artifact_dir);
        let execution = match failure.committed_execution {
            None => match failure.retry {
                None => json!({
                    "record_format": "not_started",
                    "batch_count": 0
                }),
                Some(retry) => json!({
                    "record_format": "not_started",
                    "batch_count": 0,
                    "retry": *retry
                }),
            },
            Some(committed) => json!({
                "record_format": "arrow_record_batch",
                "batch_count": committed.committed_chunks,
                "chunk_rows": committed.chunk_rows,
                "parallelism": committed.parallelism,
                "connector_parallelism_limit": committed.connector_parallelism_limit,
                "retry": *committed.retry
            }),
        };
        LoadReport {
            report_version: LOAD_REPORT_VERSION,
            binary_version: BINARY_VERSION,
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
                rejected: failure.rejected_count,
            },
            byte_counts: ByteCounts {
                source: None,
                destination: None,
            },
            rejected_records,
            destination_write: failure.destination_write.report_value(),
            execution,
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

/// The versioned YAML load definition contract (ADR-0010, ADR-0026). The
/// contract is exactly the keys declared here and in the nested blocks:
/// parsing rejects unknown keys recursively (ADR-0037), so a misspelled or
/// deferred capability key fails the load instead of being silently ignored.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LoadDefinition {
    version: Option<u64>,
    source: Option<SourceDefinition>,
    destination: Option<DestinationDefinition>,
    dataset: Option<String>,
    load_mode: Option<String>,
    /// The structural transform applied before schema pinning, validation,
    /// and destination writing: flatten mapping, field selection, then
    /// rename mapping (ADR-0039, ADR-0040, ADR-0041).
    transform: Option<schema::TransformConfig>,
    schema: Option<SchemaConfig>,
    artifacts: Option<ArtifactsConfig>,
    /// The execution-behavior settings of the load (ADR-0046): the chunk
    /// bound, the parallelism window (ADR-0052), and the retry policy
    /// (ADR-0049).
    execution: Option<ExecutionConfig>,
    /// The number of rejected records this load tolerates before failing.
    /// Defaults to `0`: any rejected record fails the load unless the
    /// definition explicitly allows more (ADR-0020).
    reject_threshold: Option<u64>,
}

/// The `execution` block of a load definition (ADR-0046): how the load
/// executes, as opposed to what it moves. `chunk_rows` bounds each
/// materialized `RecordBatch` chunk to that many surviving records; absent —
/// the block or the key — it defaults to 65536. `parallelism` bounds the
/// load's in-flight window of concurrent chunk writes (ADR-0052): absent,
/// the effective parallelism is the connector's declared limit for the
/// load's mode; present, it is `min(configured, limit)`, and `1` is the
/// explicit serial form. Peak memory scales as parallelism × `chunk_rows`
/// with no cross-field validation — the product is the user's to own.
/// Zero, negative, and non-integer values fail YAML parsing
/// (`NonZeroU64`), so every invalid value is a definition failure under
/// the existing code and no new execution failure code exists. Unknown
/// keys are rejected recursively (ADR-0037). `retry` holds the load's
/// retry policy (ADR-0049).
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ExecutionConfig {
    chunk_rows: Option<std::num::NonZeroU64>,
    parallelism: Option<std::num::NonZeroU64>,
    retry: Option<RetryConfig>,
}

/// The `execution.retry` block of a load definition (ADR-0049): the total
/// attempts each retry unit is allowed — including the first, nonzero so
/// `max_attempts: 1` is the disable form and `0` fails YAML parsing like an
/// invalid `chunk_rows` — and the fixed exponential backoff bounds in
/// milliseconds. All keys are optional and `retry: {}` equals an absent
/// block: absent knobs default to 3/200/5000. Unknown keys are rejected
/// recursively (ADR-0037).
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RetryConfig {
    max_attempts: Option<std::num::NonZeroU64>,
    initial_delay_ms: Option<u64>,
    max_delay_ms: Option<u64>,
}

/// The `artifacts` block of a load definition: the root under which this load's
/// unique artifact directory is created (ADR-0015).
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ArtifactsConfig {
    dir: Option<PathBuf>,
}

/// The `schema` block of a load definition: the path of the pinned schema file
/// the load reuses (ADR-0033), the drift policy that decides whether a load
/// may continue when schema drift is detected (ADR-0007), and the per-field
/// overrides applied to whatever the load infers (ADR-0038). The block is
/// valid with `pinned_path`, `overrides`, or both; `drift_policy` still
/// requires `pinned_path`.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SchemaConfig {
    pinned_path: Option<PathBuf>,
    drift_policy: Option<String>,
    overrides: Option<Vec<schema::OverrideEntry>>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SourceDefinition {
    connector: String,
    path: PathBuf,
    format: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DestinationDefinition {
    connector: String,
    path: PathBuf,
}

/// The execution facts of a load that succeeded. Failures travel as
/// [`ReportableFailure`] instead. Rejected records were already streamed to
/// their artifact during the read (ADR-0045), so only their count travels.
struct ExecutionDetails {
    source_summary: Value,
    destination_summary: Value,
    dataset: Option<String>,
    load_mode: String,
    schema_decision: Value,
    row_counts: RowCounts,
    byte_counts: ByteCounts,
    rejected_count: u64,
    destination_write: DestinationWriteFacts,
    execution: Value,
}

/// A failure joined with the load definition context (source, destination,
/// dataset, load mode) that a load report echoes back, plus the facts the
/// load had already established when it failed: the schema decision if one
/// had been made (`not_evaluated` otherwise), the source records counted
/// once the read completed (`0` otherwise), the count of records rejected
/// before failure — their artifact was already streamed — and, once the
/// load entered the write phase, the committed execution posture.
struct ReportableFailure {
    source_summary: Value,
    destination_summary: Value,
    dataset: Option<String>,
    load_mode: String,
    schema_decision: Value,
    source_rows: u64,
    written_records: u64,
    rejected_count: u64,
    committed_execution: Option<CommittedExecution>,
    /// The `not_started` posture's conditional retry story (ADR-0050); see
    /// [`ExecutionFailure`].
    retry: Option<Box<Value>>,
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
            rejected_count: 0,
            committed_execution: None,
            retry: None,
            destination_write: DestinationWriteFacts::not_applicable(),
            code,
            message,
        }
    }
}

/// The execution posture of a failure that happened after the load entered
/// the write phase (ADR-0047): the chunks the destination had committed —
/// `0` for a full refresh before its terminal commit, the prefix for
/// append — and the effective chunk bound the report echoes, joined by the
/// effective parallelism beside its connector limit (ADR-0053) and the
/// load's retry story (ADR-0050) — the policy echo plus the attempts array,
/// always present in this posture even when nothing was retried. Failures
/// before the write phase carry `None` and keep the `not_started` posture.
#[derive(Debug)]
struct CommittedExecution {
    committed_chunks: u64,
    chunk_rows: u64,
    parallelism: u64,
    connector_parallelism_limit: u64,
    // Boxed so the failure types carrying this posture stay small enough to
    // return by value.
    retry: Box<Value>,
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
/// the source records counted once the read completed, the count of records
/// rejected before the failure — already streamed to their artifact — and,
/// once the load entered the write phase, the committed execution posture.
/// Failures raised before or without those facts lift via `From` with none
/// of them.
#[derive(Debug)]
struct ExecutionFailure {
    failure: LoadFailure,
    // Boxed so the Err variant stays small enough to return by value.
    schema_decision: Option<Box<Value>>,
    source_rows: Option<u64>,
    written_records: u64,
    rejected_count: u64,
    // Boxed like the schema decision: the posture carries four counters and
    // the retry story, too wide to ride the Err variant inline.
    committed_execution: Option<Box<CommittedExecution>>,
    /// The retry story of a failure that kept the pre-write `not_started`
    /// posture: present exactly when wrapped `begin` attempts were recorded
    /// (ADR-0050's conditional presence rule) — provably never in the
    /// shipped connector matrix, where no failure is transient. In-session
    /// failures carry theirs on [`CommittedExecution`] instead. Boxed like
    /// the schema decision.
    retry: Option<Box<Value>>,
    destination_write: Box<DestinationWriteFacts>,
}

impl ExecutionFailure {
    /// Applies the flat reject threshold to source failures that happened before
    /// a schema decision but already established rejected-record facts. This
    /// keeps malformed records under ADR-0036 even when none survive to provide
    /// an inferable schema.
    fn apply_pending_reject_threshold(mut self, reject_threshold: u64) -> Self {
        if self.schema_decision.is_none() && self.rejected_count > reject_threshold {
            let source_rows = self.source_rows.unwrap_or(self.rejected_count);
            self.failure =
                reject_threshold_failure(self.rejected_count, source_rows, reject_threshold);
            self.source_rows = Some(source_rows);
        }
        self
    }

    /// Attaches the rejection count already streamed when the failure
    /// happened, so the report and its artifact facts stay honest.
    fn with_rejected_count(mut self, rejected_count: u64) -> Self {
        self.rejected_count = rejected_count;
        self
    }
}

impl From<LoadFailure> for ExecutionFailure {
    fn from(failure: LoadFailure) -> Self {
        ExecutionFailure {
            failure,
            schema_decision: None,
            source_rows: None,
            written_records: 0,
            rejected_count: 0,
            committed_execution: None,
            retry: None,
            destination_write: Box::new(DestinationWriteFacts::not_applicable()),
        }
    }
}

pub fn run() -> ExitCode {
    run_from(std::env::args_os())
}

/// Runs the CLI over explicit arguments. Public for the in-process harness
/// the internal library target exists for (ADR-0028) — the bounded-memory
/// proof must execute the full pipeline under the test's global allocator,
/// which a child process would escape.
pub fn run_from<ArgsIter, Arg>(args: ArgsIter) -> ExitCode
where
    ArgsIter: IntoIterator<Item = Arg>,
    Arg: Into<OsString> + Clone,
{
    let cli = Cli::parse_from(args);
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
    let definition = read_load_definition(&args.definition);
    // The one-off CLI redirect overrides the repeatable definition setting;
    // otherwise loads use the repository-wide default (ADR-0015).
    let artifact_root = args
        .output_dir
        .or_else(|| {
            definition
                .as_ref()
                .ok()
                .and_then(|definition| definition.artifacts.as_ref())
                .and_then(|artifacts| artifacts.dir.clone())
        })
        .unwrap_or_else(|| PathBuf::from(".data-spark").join("runs"));
    let artifact_dir = artifact_root.join(&load_id);
    fs::create_dir_all(&artifact_dir)?;

    // The rejected-records artifact streams during the read (ADR-0045), so
    // it exists before the report that names it (ADR-0036).
    let mut sink = rejection::RejectionSink::new(&artifact_dir);
    let execution = match definition {
        Ok(definition) => execute_load_definition(definition, &mut sink),
        Err(failure) => Err(failure),
    };
    // An artifact write failure aborts without a report, exactly like the
    // pre-streaming whole-artifact write did.
    if let Some(error) = sink.take_io_error() {
        return Err(Box::new(error));
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
fn read_load_definition(definition_path: &Path) -> Result<LoadDefinition, ReportableFailure> {
    let definition_text = fs::read_to_string(definition_path).map_err(|error| {
        ReportableFailure::without_context(
            "load_definition_read_failed",
            format!("failed to read load definition: {error}"),
        )
    })?;

    serde_yaml::from_str::<LoadDefinition>(&definition_text).map_err(|error| {
        ReportableFailure::without_context(
            "invalid_load_definition_yaml",
            format!("failed to parse load definition: {error}"),
        )
    })
}

// Called once per load, so the size of the Err variant (which carries the
// definition context the report echoes back) is irrelevant.
#[allow(clippy::result_large_err)]
fn execute_load_definition(
    definition: LoadDefinition,
    sink: &mut rejection::RejectionSink,
) -> Result<ExecutionDetails, ReportableFailure> {
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
                rejected_count: 0,
                committed_execution: None,
                retry: None,
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
                rejected_count: 0,
                committed_execution: None,
                retry: None,
                destination_write: DestinationWriteFacts::not_applicable(),
                code: "missing_load_definition_version",
                message: "load definition version is required".to_string(),
            })
        }
    }

    execute_supported_load(&definition, sink).map_err(|execution_failure| ReportableFailure {
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
        rejected_count: execution_failure.rejected_count,
        committed_execution: execution_failure
            .committed_execution
            .map(|committed| *committed),
        retry: execution_failure.retry,
        destination_write: *execution_failure.destination_write,
        code: execution_failure.failure.code,
        message: execution_failure.failure.message,
    })
}

fn reject_threshold_failure(
    rejected_count: u64,
    source_rows: u64,
    reject_threshold: u64,
) -> LoadFailure {
    LoadFailure {
        code: "reject_threshold_exceeded",
        message: format!(
            "rejected {rejected_count} of {source_rows} records, \
             exceeding the reject threshold of {reject_threshold}"
        ),
    }
}

fn execute_supported_load(
    definition: &LoadDefinition,
    sink: &mut rejection::RejectionSink,
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

    // Validate the load mode, both connectors, the schema directive, and the
    // execution config before any source or destination I/O so an unsupported
    // definition fails without reading the source or touching the destination
    // (ADR-0019), preserving the error precedence: load mode -> source
    // connector -> destination connector -> schema directive (config, then
    // pinned schema file) -> execution config -> read -> write. The source
    // format is validated inside read() so its precedence stays after the
    // destination-connector check for a doubly-invalid definition.
    let mode = LoadMode::parse(&load_mode)?;
    let source_port = source_connector(source)?;
    let destination_port = destination_connector(destination, definition.dataset.as_deref())?;
    destination_port.validate_mode(mode)?;
    let directive = resolve_schema_directive(
        definition.schema.as_ref(),
        definition.transform.as_ref(),
        &resolved_source_format(&source.path, source.format.as_deref()),
    )?;
    let chunk_rows = resolve_chunk_rows(definition.execution.as_ref());
    let retry_policy = resolve_retry_policy(definition.execution.as_ref());
    let connector_parallelism_limit = destination_port.parallelism_limit(mode);
    let parallelism =
        resolve_parallelism(definition.execution.as_ref(), connector_parallelism_limit);

    let reject_threshold = definition.reject_threshold.unwrap_or(0);
    let SourceRead {
        schema_decision,
        pinned_schema_write,
        source_bytes,
        source_rows,
        rejected_count,
        chunks,
    } = source_port
        .read(&directive, chunk_rows as usize, sink)
        .map_err(|failure| failure.apply_pending_reject_threshold(reject_threshold))?;
    let row_count = source_rows - rejected_count;

    // The reject threshold gates the load while it is still side-effect free
    // (ADR-0036): more rejected records than the definition tolerates
    // (`reject_threshold`, default 0 per ADR-0020) is a validation failure,
    // which must leave the pin unpersisted and the destination untouched
    // (ADR-0019). Pass 1 evaluates rejections over the full input before any
    // chunk is written (ADR-0045), so a breach anywhere in the source stops
    // the load here, before any destination write can happen.
    if rejected_count > reject_threshold {
        return Err(resolved_pre_write_failure(
            reject_threshold_failure(rejected_count, source_rows, reject_threshold),
            schema_decision,
            source_rows,
            rejected_count,
        ));
    }

    // Persist the produced or extended pin after the threshold gate and
    // before the first destination write: the pin records the schema decision
    // of this load's source, which stays valid even if the write then fails,
    // and a retry converges on the same pin. Failures past this point
    // happened after the schema decision and the rejections were established,
    // so they carry both for the report.
    if let Some(pinned_schema_write) = &pinned_schema_write {
        if let Err(failure) = persist_pinned_schema(pinned_schema_write) {
            return Err(resolved_pre_write_failure(
                failure,
                schema_decision,
                source_rows,
                rejected_count,
            ));
        }
    }

    let sleeper = retry::ThreadSleeper;
    let mut retry_attempts = Vec::new();
    let outcome = match dispatch::run_write_phase(
        chunks,
        destination_port.as_ref(),
        mode,
        parallelism,
        &retry_policy,
        &sleeper,
        &mut retry_attempts,
    ) {
        Ok(outcome) => outcome,
        // A failure opening the session keeps the pre-write posture: no
        // record batch was ever exchanged, so the report stays at
        // `not_started` exactly as it did before sessions existed
        // (ADR-0047), stating a retry story only when `begin` attempts were
        // actually recorded (ADR-0050) — never in the shipped connector
        // matrix, where no failure is transient.
        Err(WritePhaseFailure::BeforeSession(write_failure)) => {
            return Err(ExecutionFailure {
                failure: write_failure.failure,
                schema_decision: Some(Box::new(schema_decision)),
                source_rows: Some(source_rows),
                written_records: write_failure.written_records,
                rejected_count,
                committed_execution: None,
                retry: retry::report_when_attempted(&retry_policy, &retry_attempts),
                destination_write: Box::new(write_failure.facts),
            })
        }
        Err(WritePhaseFailure::InSession(write_failure)) => {
            return Err(ExecutionFailure {
                failure: write_failure.failure,
                schema_decision: Some(Box::new(schema_decision)),
                source_rows: Some(source_rows),
                written_records: write_failure.written_records,
                rejected_count,
                committed_execution: Some(Box::new(CommittedExecution {
                    committed_chunks: write_failure.committed_chunks,
                    chunk_rows,
                    parallelism: parallelism.get(),
                    connector_parallelism_limit: connector_parallelism_limit.get(),
                    retry: Box::new(retry::report_value(&retry_policy, &retry_attempts)),
                })),
                retry: None,
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
            destination: outcome.bytes_written,
        },
        rejected_count,
        destination_write: outcome.facts,
        execution: json!({
            "record_format": "arrow_record_batch",
            "batch_count": outcome.chunk_count,
            "chunk_rows": chunk_rows,
            "parallelism": parallelism.get(),
            "connector_parallelism_limit": connector_parallelism_limit.get(),
            "retry": retry::report_value(&retry_policy, &retry_attempts)
        }),
    })
}

/// Resolves the effective chunk bound from the definition's `execution`
/// block (ADR-0046): the declared `chunk_rows`, or the default wherever the
/// block or the key is absent. Never a failure — every invalid bound was
/// already rejected at YAML parse time.
fn resolve_chunk_rows(execution: Option<&ExecutionConfig>) -> u64 {
    execution
        .and_then(|execution| execution.chunk_rows)
        .map(std::num::NonZeroU64::get)
        .unwrap_or(DEFAULT_CHUNK_ROWS)
}

/// Resolves the effective retry policy from the definition's `execution.retry`
/// block (ADR-0049): each declared knob, with 3/200/5000 wherever the block
/// or a key is absent. Never a failure — a zero or non-integer
/// `max_attempts` was already rejected at YAML parse time.
fn resolve_retry_policy(execution: Option<&ExecutionConfig>) -> retry::RetryPolicy {
    let defaults = retry::RetryPolicy::default();
    let Some(config) = execution.and_then(|execution| execution.retry.as_ref()) else {
        return defaults;
    };
    retry::RetryPolicy {
        max_attempts: config.max_attempts.unwrap_or(defaults.max_attempts),
        initial_delay_ms: config.initial_delay_ms.unwrap_or(defaults.initial_delay_ms),
        max_delay_ms: config.max_delay_ms.unwrap_or(defaults.max_delay_ms),
    }
}

/// Resolves the effective load parallelism from the definition's
/// `execution.parallelism` and the destination's declared Connector
/// Parallelism Limit for the load's mode (ADR-0052): absent, the limit —
/// the conservative connector-specific default of ADR-0023 — and present,
/// `min(configured, limit)`, so the limit is a hard cap the configuration
/// can never exceed. Never a failure — every invalid value was already
/// rejected at YAML parse time.
fn resolve_parallelism(
    execution: Option<&ExecutionConfig>,
    connector_parallelism_limit: std::num::NonZeroU64,
) -> std::num::NonZeroU64 {
    execution
        .and_then(|execution| execution.parallelism)
        .map_or(connector_parallelism_limit, |configured| {
            configured.min(connector_parallelism_limit)
        })
}

/// An execution failure raised after the read resolved the load but before
/// the destination session opened — the threshold gate, the pin write, the
/// session open itself: the schema decision, counts, and already-streamed
/// rejections travel, and the execution posture stays pre-write.
fn resolved_pre_write_failure(
    failure: LoadFailure,
    schema_decision: Value,
    source_rows: u64,
    rejected_count: u64,
) -> ExecutionFailure {
    ExecutionFailure {
        failure,
        schema_decision: Some(Box::new(schema_decision)),
        source_rows: Some(source_rows),
        written_records: 0,
        rejected_count,
        committed_execution: None,
        retry: None,
        destination_write: Box::new(DestinationWriteFacts::not_applicable()),
    }
}

/// Resolves the definition's `transform` and `schema` blocks into the schema
/// directive the source materializes under: no `schema` block means
/// inference, a named pinned schema file that does not exist yet means this
/// load persists the inferred schema as the new pin (ADR-0033), an existing
/// file is parsed and validated against (ADR-0034), and the validated
/// `transform` (ADR-0039) and `overrides` (ADR-0038) travel on every
/// directive to reshape and rewrite whatever the load infers. A block with
/// `overrides` but no `pinned_path` is a plain inference directive with
/// overrides. The transform validates first — it precedes overrides and
/// pinning in the meaning order — and, like override validation, before any
/// pinned schema file is read, so a broken definition never silently
/// bootstraps a pin (ADR-0040). `source_format` is the resolved source
/// format, which transform validation checks against `transform.flatten`'s
/// JSONL requirement (ADR-0041) — a config-time failure, before the pin or
/// any file is read.
fn resolve_schema_directive(
    schema_config: Option<&SchemaConfig>,
    transform_config: Option<&schema::TransformConfig>,
    source_format: &str,
) -> Result<schema::SchemaDirective, LoadFailure> {
    let transform = match transform_config {
        None => schema::SchemaTransform::none(),
        Some(transform_config) => {
            schema::SchemaTransform::from_config(transform_config, source_format)?
        }
    };
    let Some(schema_config) = schema_config else {
        return Ok(schema::SchemaDirective::Inferred {
            transform,
            overrides: schema::SchemaOverrides::none(),
        });
    };
    let invalid_schema_config = |message: String| LoadFailure {
        code: "invalid_schema_config",
        message,
    };

    // A written-but-empty setting is noise, not an absent one: fail loud
    // instead of silently reinterpreting the block (ADR-0037's spirit).
    if schema_config
        .pinned_path
        .as_ref()
        .is_some_and(|pinned_path| pinned_path.as_os_str().is_empty())
    {
        return Err(invalid_schema_config(
            "schema.pinned_path must not be empty".to_string(),
        ));
    }
    if schema_config
        .overrides
        .as_ref()
        .is_some_and(|overrides| overrides.is_empty())
    {
        return Err(invalid_schema_config(
            "schema.overrides must declare at least one override".to_string(),
        ));
    }
    if schema_config.pinned_path.is_none() {
        if schema_config.drift_policy.is_some() {
            return Err(invalid_schema_config(
                "schema.drift_policy requires schema.pinned_path".to_string(),
            ));
        }
        if schema_config.overrides.is_none() {
            return Err(invalid_schema_config(
                "a schema block must set schema.pinned_path or schema.overrides".to_string(),
            ));
        }
    }
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
    let overrides = match &schema_config.overrides {
        None => schema::SchemaOverrides::none(),
        Some(entries) => schema::SchemaOverrides::from_entries(entries)?,
    };

    let Some(pinned_path) = schema_config.pinned_path.as_ref() else {
        return Ok(schema::SchemaDirective::Inferred {
            transform,
            overrides,
        });
    };
    match fs::read_to_string(pinned_path) {
        Ok(pin_text) => Ok(schema::SchemaDirective::Pinned {
            pinned_path: path_string(pinned_path),
            pin: schema::PinnedSchema::from_yaml(&pin_text)?,
            drift_policy,
            transform,
            overrides,
        }),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            Ok(schema::SchemaDirective::PinInferred {
                pinned_path: path_string(pinned_path),
                transform,
                overrides,
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
            rejected_count: 1,
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
        assert_eq!(report.binary_version, env!("CARGO_PKG_VERSION"));
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
            rejected_count: 0,
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
            rejected_count: 0,
            committed_execution: None,
            retry: None,
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
        assert_eq!(report.binary_version, env!("CARGO_PKG_VERSION"));
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
            rejected_count: 2,
            committed_execution: None,
            retry: None,
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
            transform: None,
            schema: None,
            artifacts: None,
            execution: None,
            reject_threshold,
        }
    }

    #[test]
    fn execute_supported_load_fails_when_rejections_exceed_the_default_threshold() {
        let work = tempfile::TempDir::new().expect("tempdir");
        let definition = threshold_definition(&work, None);

        let mut sink = rejection::RejectionSink::new(work.path());
        let failure = execute_supported_load(&definition, &mut sink)
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
        assert_eq!(failure.rejected_count, 1);
        // The rejection was streamed to its artifact before the gate fired.
        assert!(sink.take_io_error().is_none());
        assert!(work
            .path()
            .join(rejection::REJECTED_RECORDS_FILENAME)
            .exists());
        // The threshold gate is side-effect free: no destination was written.
        assert!(!work.path().join("customers_dataset").exists());
    }

    #[test]
    fn execute_supported_load_completes_at_an_explicit_reject_threshold() {
        // One rejection at a threshold of exactly 1: at-or-below completes.
        let work = tempfile::TempDir::new().expect("tempdir");
        let definition = threshold_definition(&work, Some(1));

        let mut sink = rejection::RejectionSink::new(work.path());
        let details = execute_supported_load(&definition, &mut sink)
            .expect("at-or-below the threshold completes");

        assert_eq!(details.row_counts.source, 2);
        assert_eq!(details.row_counts.written, 1);
        assert_eq!(details.row_counts.rejected, 1);
        assert_eq!(details.rejected_count, 1);
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
            overrides: None,
        });

        let mut sink = rejection::RejectionSink::new(work.path());
        let failure = execute_supported_load(&definition, &mut sink)
            .err()
            .expect("one rejection exceeds the default threshold of 0");

        assert_eq!(failure.failure.code, "reject_threshold_exceeded");
        assert!(
            !pinned_path.exists(),
            "a threshold-failed load must not persist the pin"
        );
    }

    #[test]
    fn from_failure_reports_the_committed_write_phase_posture() {
        // A failure after the load entered the write phase reports the
        // chunked execution posture: the committed chunk count, the
        // effective chunk bound (ADR-0047), the effective parallelism
        // beside its connector limit (ADR-0053), and the retry story — the
        // policy echo with its attempts array, empty when nothing was
        // retried (ADR-0050).
        let retry_story = json!({
            "max_attempts": 3,
            "initial_delay_ms": 200,
            "max_delay_ms": 5000,
            "attempts": []
        });
        let failure = ReportableFailure {
            source_summary: json!({ "connector": "local_file" }),
            destination_summary: json!({ "connector": "parquet" }),
            dataset: Some("customers".to_string()),
            load_mode: "append".to_string(),
            schema_decision: json!({ "mode": "inferred" }),
            source_rows: 5,
            written_records: 2,
            rejected_count: 0,
            committed_execution: Some(CommittedExecution {
                committed_chunks: 2,
                chunk_rows: 1,
                parallelism: 1,
                connector_parallelism_limit: 1,
                retry: Box::new(retry_story.clone()),
            }),
            retry: None,
            destination_write: DestinationWriteFacts::best_effort("staged_part_append"),
            code: "source_changed_during_load",
            message: "source changed during the load".to_string(),
        };

        let report = LoadReport::from_failure(
            "load-under-test".to_string(),
            "artifacts/load-under-test".to_string(),
            timings(),
            failure,
        );

        assert_eq!(
            report.execution,
            json!({
                "record_format": "arrow_record_batch",
                "batch_count": 2,
                "chunk_rows": 1,
                "parallelism": 1,
                "connector_parallelism_limit": 1,
                "retry": retry_story
            })
        );
        assert_eq!(report.row_counts.written, 2);
    }

    #[test]
    fn from_failure_states_a_not_started_retry_story_only_when_attempts_exist() {
        // The `not_started` posture carries `retry` exactly when the load
        // recorded attempts — a future connector's exhausted transient
        // `begin` — and keeps its established two-field shape otherwise
        // (ADR-0050): every never-retried failure report stays
        // byte-identical to today's.
        let retry_story = json!({
            "max_attempts": 3,
            "initial_delay_ms": 200,
            "max_delay_ms": 5000,
            "attempts": [{
                "operation": "begin",
                "attempt": 1,
                "error": {
                    "code": "destination_write_failed",
                    "message": "connection shortage"
                }
            }]
        });
        let failure = ReportableFailure {
            source_summary: json!({}),
            destination_summary: json!({}),
            dataset: None,
            load_mode: "append".to_string(),
            schema_decision: json!({ "mode": "inferred" }),
            source_rows: 1,
            written_records: 0,
            rejected_count: 0,
            committed_execution: None,
            retry: Some(Box::new(retry_story.clone())),
            destination_write: DestinationWriteFacts::not_applicable(),
            code: "destination_write_failed",
            message: "connection shortage".to_string(),
        };

        let report = LoadReport::from_failure(
            "load-under-test".to_string(),
            "artifacts/load-under-test".to_string(),
            timings(),
            failure,
        );

        assert_eq!(
            report.execution,
            json!({
                "record_format": "not_started",
                "batch_count": 0,
                "retry": retry_story
            })
        );
    }

    // ---- Source-mutation guard across the write phase (ADR-0045, ADR-0047) ----

    /// Reads a CSV source through the streaming port for a write-phase test:
    /// the resolved chunk stream plus the sink directory keeping the
    /// artifact.
    fn read_csv_chunks(
        work: &tempfile::TempDir,
        source_path: &Path,
        chunk_rows: usize,
    ) -> connector::SourceRead {
        let source_port = source_connector(&SourceDefinition {
            connector: "local_file".to_string(),
            path: source_path.to_path_buf(),
            format: Some("csv".to_string()),
        })
        .expect("source connector");
        let mut sink = rejection::RejectionSink::new(work.path());
        let read = source_port
            .read(&schema::SchemaDirective::inferred(), chunk_rows, &mut sink)
            .expect("read source");
        assert!(sink.take_io_error().is_none());
        read
    }

    fn parquet_destination(work: &tempfile::TempDir) -> Box<dyn connector::Destination> {
        destination_connector(
            &DestinationDefinition {
                connector: "parquet".to_string(),
                path: work.path().join("customers_dataset"),
            },
            None,
        )
        .expect("parquet connector")
    }

    fn duckdb_destination(work: &tempfile::TempDir) -> Box<dyn connector::Destination> {
        destination_connector(
            &DestinationDefinition {
                connector: "duckdb".to_string(),
                path: work.path().join("customers.duckdb"),
            },
            Some("customers"),
        )
        .expect("duckdb connector")
    }

    fn seed_destination(work: &tempfile::TempDir, destination: &dyn connector::Destination) {
        let seed_path = work.path().join("seed.csv");
        fs::write(&seed_path, "id,name\n7,Seed\n8,Kept\n").expect("write seed csv");
        let read = read_csv_chunks(work, &seed_path, usize::MAX);
        write_phase_without_retries(read.chunks, destination, LoadMode::FullRefresh)
            .expect("seed destination");
    }

    /// Drives the write phase under the default retry policy for tests that
    /// exercise no transient failure, asserting afterward that the engine
    /// stayed idle: no sleep, no recorded attempt.
    fn write_phase_without_retries(
        chunks: connector::SourceChunks,
        destination: &dyn connector::Destination,
        mode: LoadMode,
    ) -> Result<dispatch::WritePhaseOutcome, WritePhaseFailure> {
        let sleeper = retry::RecordingSleeper::new();
        let mut retry_attempts = Vec::new();
        let outcome = dispatch::run_write_phase(
            chunks,
            destination,
            mode,
            std::num::NonZeroU64::MIN,
            &retry::RetryPolicy::default(),
            &sleeper,
            &mut retry_attempts,
        );
        assert!(
            sleeper.slept_ms().is_empty(),
            "no local failure is transient, so the engine never sleeps"
        );
        assert!(
            retry_attempts.is_empty(),
            "no local failure is transient, so no attempt is recorded"
        );
        outcome
    }

    fn duckdb_rows(work: &tempfile::TempDir) -> usize {
        let connection =
            duckdb::Connection::open(work.path().join("customers.duckdb")).expect("open duckdb");
        connection
            .query_row("SELECT count(*) FROM customers", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("count rows") as usize
    }

    #[test]
    fn full_refresh_source_change_leaves_both_destinations_untouched() {
        // A bytes-only mutation after the read: every per-record outcome
        // still matches, so only the byte hash catches it — before the
        // terminal commit, leaving the seeded dataset intact (ADR-0045).
        for connector_name in ["parquet", "duckdb"] {
            let work = tempfile::TempDir::new().expect("tempdir");
            let destination: Box<dyn connector::Destination> = match connector_name {
                "parquet" => parquet_destination(&work),
                _ => duckdb_destination(&work),
            };
            seed_destination(&work, destination.as_ref());

            let source_path = work.path().join("customers.csv");
            fs::write(&source_path, "id,name\n1,Ada\n2,Grace\n3,Cara\n").expect("write csv");
            let read = read_csv_chunks(&work, &source_path, 1);
            fs::write(&source_path, "id,name\n1,Bob\n2,Grace\n3,Cara\n").expect("mutate csv");

            let Err(failure) = write_phase_without_retries(
                read.chunks,
                destination.as_ref(),
                LoadMode::FullRefresh,
            ) else {
                panic!("source change fails the load")
            };
            let WritePhaseFailure::InSession(failure) = failure else {
                panic!("the mutation guard fires inside the open session")
            };
            assert_eq!(failure.failure.code, "source_changed_during_load");
            assert_eq!(failure.committed_chunks, 0, "{connector_name}");
            assert_eq!(failure.written_records, 0, "{connector_name}");
            assert_eq!(
                failure.facts.report_value(),
                json!({ "atomicity": "not_applicable" }),
                "{connector_name}"
            );

            // The seeded destination survives untouched.
            match connector_name {
                "parquet" => {
                    let parts = fs::read_dir(work.path().join("customers_dataset"))
                        .expect("destination directory")
                        .filter(|entry| {
                            entry.as_ref().expect("entry").path().extension()
                                == Some("parquet".as_ref())
                        })
                        .count();
                    assert_eq!(parts, 1, "the seed part alone remains");
                }
                _ => assert_eq!(duckdb_rows(&work), 2),
            }
        }
    }

    #[test]
    fn append_source_change_keeps_exactly_the_committed_chunk_prefix() {
        // Record 3 turns semantically divergent after the read: with a chunk
        // bound of 1, chunks 1 and 2 commit before the divergence surfaces,
        // and the failure reports that prefix honestly (ADR-0047).
        for connector_name in ["parquet", "duckdb"] {
            let work = tempfile::TempDir::new().expect("tempdir");
            let destination: Box<dyn connector::Destination> = match connector_name {
                "parquet" => parquet_destination(&work),
                _ => duckdb_destination(&work),
            };
            seed_destination(&work, destination.as_ref());

            let source_path = work.path().join("customers.csv");
            fs::write(&source_path, "id,name\n1,Ada\n2,Grace\n3,Cara\n").expect("write csv");
            let read = read_csv_chunks(&work, &source_path, 1);
            fs::write(&source_path, "id,name\n1,Ada\n2,Grace\n3,Cara,extra\n").expect("mutate csv");

            let Err(failure) =
                write_phase_without_retries(read.chunks, destination.as_ref(), LoadMode::Append)
            else {
                panic!("source change fails the load")
            };
            let WritePhaseFailure::InSession(failure) = failure else {
                panic!("the mutation guard fires inside the open session")
            };
            assert_eq!(failure.failure.code, "source_changed_during_load");
            assert_eq!(failure.committed_chunks, 2, "{connector_name}");
            assert_eq!(failure.written_records, 2, "{connector_name}");
            let expected_strategy = match connector_name {
                "parquet" => "staged_part_append",
                _ => "insert",
            };
            assert_eq!(
                failure.facts.report_value(),
                json!({ "atomicity": "best_effort", "strategy": expected_strategy }),
                "{connector_name}"
            );

            match connector_name {
                "parquet" => {
                    let parts = fs::read_dir(work.path().join("customers_dataset"))
                        .expect("destination directory")
                        .filter(|entry| {
                            entry.as_ref().expect("entry").path().extension()
                                == Some("parquet".as_ref())
                        })
                        .count();
                    assert_eq!(parts, 3, "the seed part plus one part per committed chunk");
                }
                _ => assert_eq!(
                    duckdb_rows(&work),
                    4,
                    "the seed rows plus the committed prefix"
                ),
            }
        }
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

    #[test]
    fn load_definition_parses_the_retry_block() {
        // All keys optional: a full block, a partial block, and `retry: {}`
        // all parse; the zero and non-integer forms fail YAML parsing, so
        // every invalid knob surfaces as invalid_load_definition_yaml at the
        // load boundary (ADR-0049).
        let definition = serde_yaml::from_str::<LoadDefinition>(
            "version: 1\n\
             execution:\n\
             \x20 retry:\n\
             \x20   max_attempts: 5\n\
             \x20   initial_delay_ms: 50\n\
             \x20   max_delay_ms: 900\n",
        )
        .expect("definition with a full retry block parses");
        let retry_config = definition
            .execution
            .expect("execution block")
            .retry
            .expect("retry block");
        assert_eq!(
            retry_config.max_attempts.map(std::num::NonZeroU64::get),
            Some(5)
        );
        assert_eq!(retry_config.initial_delay_ms, Some(50));
        assert_eq!(retry_config.max_delay_ms, Some(900));

        let definition =
            serde_yaml::from_str::<LoadDefinition>("version: 1\nexecution:\n  retry: {}\n")
                .expect("an empty retry block parses");
        let retry_config = definition
            .execution
            .expect("execution block")
            .retry
            .expect("retry block");
        assert!(retry_config.max_attempts.is_none());
        assert!(retry_config.initial_delay_ms.is_none());
        assert!(retry_config.max_delay_ms.is_none());

        for yaml in [
            "version: 1\nexecution:\n  retry:\n    max_attempts: 0\n",
            "version: 1\nexecution:\n  retry:\n    max_attempts: 2.5\n",
            "version: 1\nexecution:\n  retry:\n    initial_delay_ms: -1\n",
        ] {
            assert!(
                serde_yaml::from_str::<LoadDefinition>(yaml).is_err(),
                "invalid retry knob accepted: {yaml:?}"
            );
        }
    }

    #[test]
    fn resolve_retry_policy_defaults_and_echoes_declared_knobs() {
        // Absent block, absent key, and `retry: {}` are all the defaults;
        // declared knobs win individually, so a partial block keeps the
        // remaining defaults (ADR-0049).
        for yaml in [
            "version: 1\n",
            "version: 1\nexecution: {}\n",
            "version: 1\nexecution:\n  retry: {}\n",
        ] {
            let definition =
                serde_yaml::from_str::<LoadDefinition>(yaml).expect("definition parses");
            let policy = resolve_retry_policy(definition.execution.as_ref());
            assert_eq!(policy.max_attempts.get(), 3, "for {yaml:?}");
            assert_eq!(policy.initial_delay_ms, 200, "for {yaml:?}");
            assert_eq!(policy.max_delay_ms, 5000, "for {yaml:?}");
        }

        let definition = serde_yaml::from_str::<LoadDefinition>(
            "version: 1\nexecution:\n  retry:\n    max_attempts: 5\n",
        )
        .expect("definition parses");
        let policy = resolve_retry_policy(definition.execution.as_ref());
        assert_eq!(policy.max_attempts.get(), 5);
        assert_eq!(policy.initial_delay_ms, 200);
        assert_eq!(policy.max_delay_ms, 5000);
    }

    #[test]
    fn load_definition_parses_the_parallelism_key() {
        // `execution.parallelism` is one optional nonzero scalar beside
        // `chunk_rows` and `retry` (ADR-0052): zero, negative, and
        // non-integer forms fail YAML parsing, surfacing as
        // invalid_load_definition_yaml at the load boundary.
        let definition =
            serde_yaml::from_str::<LoadDefinition>("version: 1\nexecution:\n  parallelism: 4\n")
                .expect("definition with parallelism parses");
        assert_eq!(
            definition
                .execution
                .expect("execution block")
                .parallelism
                .map(std::num::NonZeroU64::get),
            Some(4)
        );

        let definition = serde_yaml::from_str::<LoadDefinition>("version: 1\nexecution: {}\n")
            .expect("definition without parallelism parses");
        assert!(definition
            .execution
            .expect("execution block")
            .parallelism
            .is_none());

        for yaml in [
            "version: 1\nexecution:\n  parallelism: 0\n",
            "version: 1\nexecution:\n  parallelism: -2\n",
            "version: 1\nexecution:\n  parallelism: 1.5\n",
        ] {
            assert!(
                serde_yaml::from_str::<LoadDefinition>(yaml).is_err(),
                "invalid parallelism accepted: {yaml:?}"
            );
        }
    }

    #[test]
    fn resolve_parallelism_defaults_to_the_connector_limit_and_clamps_configured_values() {
        // Absent, the effective parallelism is the connector's declared
        // limit for the load's mode; present, it is min(configured, limit)
        // — the limit is a hard cap, never exceeded (ADR-0052).
        let nonzero = |value: u64| std::num::NonZeroU64::new(value).expect("nonzero");
        let execution = |yaml: &str| {
            serde_yaml::from_str::<LoadDefinition>(yaml)
                .expect("definition parses")
                .execution
        };

        for (yaml, limit, expected) in [
            ("version: 1\n", 1, 1),
            ("version: 1\nexecution: {}\n", 4, 4),
            ("version: 1\nexecution:\n  parallelism: 8\n", 2, 2),
            ("version: 1\nexecution:\n  parallelism: 1\n", 4, 1),
            ("version: 1\nexecution:\n  parallelism: 3\n", 3, 3),
        ] {
            assert_eq!(
                resolve_parallelism(execution(yaml).as_ref(), nonzero(limit)),
                nonzero(expected),
                "for {yaml:?} under limit {limit}"
            );
        }
    }

    #[test]
    fn shipped_destinations_declare_the_serial_parallelism_limit_for_every_mode() {
        // The provably serial shipped matrix (ADR-0051): both shipped
        // destinations hold the default limit of 1 for every load mode, so
        // any configured parallelism clamps to 1 and the windowed engine is
        // unreachable outside test connectors.
        let work = tempfile::TempDir::new().expect("tempdir");
        for destination in [parquet_destination(&work), duckdb_destination(&work)] {
            for mode in [LoadMode::FullRefresh, LoadMode::Append] {
                assert_eq!(destination.parallelism_limit(mode).get(), 1);
            }
        }
    }

    #[test]
    fn load_definition_rejects_unknown_keys_in_every_block() {
        // Strict contract (ADR-0037): a key the contract does not declare is a
        // parse failure naming the key, never a silently ignored no-op.
        for (yaml, unknown_field) in [
            ("version: 1\nparallelism: 4\n", "parallelism"),
            (
                "version: 1\nsource:\n  connector: local_file\n  path: a.csv\n  select: [id]\n",
                "select",
            ),
            (
                "version: 1\ndestination:\n  connector: parquet\n  path: out\n  rename: y\n",
                "rename",
            ),
            (
                "version: 1\ntransform:\n  select: [id]\n  drop: [note]\n",
                "drop",
            ),
            (
                "version: 1\nschema:\n  pinned_path: p.yml\n  checksum: abc123\n",
                "checksum",
            ),
            (
                "version: 1\nschema:\n  overrides:\n  - name: id\n    coerce: true\n",
                "coerce",
            ),
            (
                "version: 1\nartifacts:\n  dir: runs\n  retention_days: 7\n",
                "retention_days",
            ),
            (
                "version: 1\nexecution:\n  retry:\n    jitter: true\n",
                "jitter",
            ),
        ] {
            let error = serde_yaml::from_str::<LoadDefinition>(yaml)
                .err()
                .unwrap_or_else(|| panic!("definition {yaml:?} accepted"));
            let message = error.to_string();
            assert!(
                message.contains(&format!("unknown field `{unknown_field}`")),
                "message {message:?} misses the rejected field {unknown_field:?}"
            );
        }
    }

    // ---- Schema directive resolution ----

    fn schema_config(pinned_path: Option<&str>, drift_policy: Option<&str>) -> SchemaConfig {
        SchemaConfig {
            pinned_path: pinned_path.map(PathBuf::from),
            drift_policy: drift_policy.map(str::to_string),
            overrides: None,
        }
    }

    fn overrides_yaml(overrides: &str) -> Option<Vec<schema::OverrideEntry>> {
        Some(serde_yaml::from_str(overrides).expect("test overrides parse"))
    }

    #[test]
    fn resolve_schema_directive_defaults_to_inference_without_a_schema_block() {
        assert!(matches!(
            resolve_schema_directive(None, None, "csv"),
            Ok(schema::SchemaDirective::Inferred { .. })
        ));
    }

    #[test]
    fn resolve_schema_directive_rejects_underspecified_schema_blocks() {
        // The block is valid with pinned_path, overrides, or both (ADR-0038);
        // drift_policy still requires pinned_path, and a written-but-empty
        // setting is noise, not an absent one.
        let mut empty_overrides = schema_config(Some("p.yml"), None);
        empty_overrides.overrides = Some(Vec::new());
        for (config, expected_message_part) in [
            (
                schema_config(None, None),
                "a schema block must set schema.pinned_path or schema.overrides",
            ),
            (
                schema_config(None, Some("fail")),
                "schema.drift_policy requires schema.pinned_path",
            ),
            (
                schema_config(Some(""), None),
                "schema.pinned_path must not be empty",
            ),
            (
                empty_overrides,
                "schema.overrides must declare at least one override",
            ),
        ] {
            let error = resolve_schema_directive(Some(&config), None, "csv")
                .err()
                .expect("underspecified schema block rejected");
            assert_eq!(error.code, "invalid_schema_config");
            assert!(
                error.message.contains(expected_message_part),
                "message {:?} misses {expected_message_part:?}",
                error.message
            );
        }
    }

    #[test]
    fn resolve_schema_directive_accepts_overrides_without_a_pinned_path() {
        // Standalone overrides are an inference directive with overrides
        // (ADR-0038): no pin is read, bootstrapped, or required.
        let mut config = schema_config(None, None);
        config.overrides = overrides_yaml("- name: id\n  type: int64\n");

        match resolve_schema_directive(Some(&config), None, "csv")
            .expect("standalone overrides resolve")
        {
            schema::SchemaDirective::Inferred { overrides, .. } => assert!(!overrides.is_empty()),
            _ => panic!("expected the Inferred directive"),
        }
    }

    #[test]
    fn resolve_schema_directive_carries_overrides_onto_pin_directives() {
        let work = tempfile::TempDir::new().expect("tempdir");
        let pinned_path = work.path().join("customers.schema.yml");
        let mut config = schema_config(Some(pinned_path.to_str().expect("utf8 path")), None);
        config.overrides = overrides_yaml("- name: id\n  nullable: false\n");

        // Absent pin file: the bootstrap directive carries the overrides.
        match resolve_schema_directive(Some(&config), None, "csv").expect("absent pin bootstraps") {
            schema::SchemaDirective::PinInferred { overrides, .. } => {
                assert!(!overrides.is_empty())
            }
            _ => panic!("expected the PinInferred directive"),
        }

        // Existing pin file: the pinned directive carries the overrides.
        fs::write(
            &pinned_path,
            "version: 1\nfields:\n- name: id\n  type: int64\n",
        )
        .expect("write pin");
        match resolve_schema_directive(Some(&config), None, "csv").expect("existing pin loads") {
            schema::SchemaDirective::Pinned { overrides, .. } => assert!(!overrides.is_empty()),
            _ => panic!("expected the Pinned directive"),
        }
    }

    #[test]
    fn resolve_schema_directive_rejects_invalid_overrides_before_reading_the_pin() {
        // The pin path does not exist: override validation must fail instead
        // of silently bootstrapping. Duplicate names and a no-op entry are
        // config errors; a type outside the vocabulary mirrors
        // unsupported_drift_policy.
        for (overrides, expected_code, expected_message_part) in [
            (
                "- name: id\n  type: int64\n- name: id\n  nullable: false\n",
                "invalid_schema_config",
                "schema override for field \"id\" is declared more than once",
            ),
            (
                "- name: id\n",
                "invalid_schema_config",
                "schema override for field \"id\" must set at least one of type or nullable",
            ),
            (
                "- name: id\n  type: date\n",
                "unsupported_override_type",
                "unsupported schema override type for field \"id\": date",
            ),
        ] {
            let mut config = schema_config(Some("/does/not/exist.schema.yml"), None);
            config.overrides = overrides_yaml(overrides);
            let error = resolve_schema_directive(Some(&config), None, "csv")
                .err()
                .expect("invalid overrides rejected");
            assert_eq!(error.code, expected_code, "code for {overrides:?}");
            assert!(
                error.message.contains(expected_message_part),
                "message {:?} misses {expected_message_part:?}",
                error.message
            );
        }
    }

    fn transform_yaml(transform: &str) -> schema::TransformConfig {
        serde_yaml::from_str(transform).expect("test transform parses")
    }

    #[test]
    fn load_definition_parses_the_transform_block() {
        // The block parses with select, rename, or both; the strict contract
        // rejects nothing here — an empty or inconsistent block is directive
        // resolution's job, surfacing as invalid_transform_config.
        for yaml in [
            "version: 1\ntransform:\n  select: [id, total]\n",
            "version: 1\ntransform:\n  rename:\n    id: customer_id\n",
            "version: 1\ntransform:\n  select: [id]\n  rename:\n    id: customer_id\n",
            "version: 1\ntransform:\n  flatten:\n    customer.name: customer_name\n",
            "version: 1\ntransform:\n  flatten:\n    customer.name: customer_name\n  select: [customer_name]\n",
            "version: 1\ntransform: {}\n",
        ] {
            let definition = serde_yaml::from_str::<LoadDefinition>(yaml)
                .unwrap_or_else(|error| panic!("definition {yaml:?} failed to parse: {error}"));
            assert!(definition.transform.is_some(), "transform for {yaml:?}");
        }
    }

    #[test]
    fn load_definition_rejects_duplicate_rename_keys_at_parse_time() {
        // A duplicate rename key is a YAML parse failure — surfacing as
        // invalid_load_definition_yaml at the load boundary — never a silent
        // last-entry-wins.
        let error = serde_yaml::from_str::<LoadDefinition>(
            "version: 1\ntransform:\n  rename:\n    id: customer_id\n    id: account_id\n",
        )
        .expect_err("duplicate rename keys rejected");
        assert!(
            error
                .to_string()
                .contains("duplicate transform.rename key \"id\""),
            "message {error:?} misses the duplicate key"
        );
    }

    #[test]
    fn load_definition_rejects_duplicate_flatten_keys_at_parse_time() {
        // A duplicate flatten path is a YAML parse failure, exactly like a
        // duplicate rename key (ADR-0041).
        let error = serde_yaml::from_str::<LoadDefinition>(
            "version: 1\ntransform:\n  flatten:\n    customer.name: a\n    customer.name: b\n",
        )
        .expect_err("duplicate flatten keys rejected");
        assert!(
            error
                .to_string()
                .contains("duplicate transform.flatten key \"customer.name\""),
            "message {error:?} misses the duplicate key"
        );
    }

    #[test]
    fn resolve_schema_directive_rejects_invalid_transform_configs_before_reading_the_pin() {
        // The transform config failure matrix (ADR-0039), one case each. The
        // pin path does not exist: transform validation must fail instead of
        // silently bootstrapping, matching overrides.
        for (transform, expected_message_part) in [
            (
                "{}",
                "a transform block must set transform.flatten, transform.select, or transform.rename",
            ),
            (
                "select: []",
                "transform.select must name at least one field",
            ),
            (
                "select: [id, id]",
                "transform.select names field \"id\" more than once",
            ),
            ("rename: {}", "transform.rename must map at least one field"),
            (
                "select: [a, b]\nrename: {a: x, b: x}",
                "transform.rename maps more than one field to \"x\"",
            ),
            (
                "select: [a]\nrename: {b: c}",
                "transform.rename key \"b\" is not in transform.select",
            ),
            (
                "select: [a, b]\nrename: {a: b}",
                "transform.select and transform.rename map more than one field \
                 to the dataset name \"b\"",
            ),
            (
                "rename: {id: id}",
                "transform.rename maps field \"id\" to itself",
            ),
            (
                "rename: {id: \"\"}",
                "transform.rename target for field \"id\" must not be empty",
            ),
            (
                "rename: {id: \"   \"}",
                "transform.rename target for field \"id\" must not be empty",
            ),
            ("flatten: {}", "transform.flatten must map at least one path"),
            (
                "flatten: {customer: name}",
                "transform.flatten path \"customer\" must have at least two dot-separated segments",
            ),
            (
                "flatten: {customer..name: contact}",
                "transform.flatten path \"customer..name\" must not contain empty segments",
            ),
            (
                "flatten: {.name: contact}",
                "transform.flatten path \".name\" must not contain empty segments",
            ),
            (
                "flatten: {customer.name: \"   \"}",
                "transform.flatten output name for path \"customer.name\" must not be empty",
            ),
            (
                "flatten: {customer.name: contact, customer.email: contact}",
                "transform.flatten maps more than one path to \"contact\"",
            ),
            (
                "flatten: {customer.name: contact}\nrename: {id: contact}",
                "transform.flatten and transform.rename map more than one field \
                 to the dataset name \"contact\"",
            ),
            (
                "flatten: {customer.name: contact}\nselect: [id]",
                "transform.flatten output \"contact\" is not in transform.select",
            ),
            (
                "flatten: {customer.name: contact}\nselect: [id, contact]\nrename: {id: contact}",
                "transform.select and transform.rename map more than one field \
                 to the dataset name \"contact\"",
            ),
        ] {
            let config = schema_config(Some("/does/not/exist.schema.yml"), None);
            let error = resolve_schema_directive(Some(&config), Some(&transform_yaml(transform)), "jsonl")
                .err()
                .expect("invalid transform rejected");
            assert_eq!(
                error.code, "invalid_transform_config",
                "code for {transform:?}"
            );
            assert!(
                error.message.contains(expected_message_part),
                "message {:?} misses {expected_message_part:?}",
                error.message
            );
        }
    }

    #[test]
    fn resolve_schema_directive_rejects_flatten_on_a_csv_source() {
        // transform.flatten requires a JSONL source: CSV cells hold no
        // addressable structure, so the declaration is a config-time failure
        // before the pin or any file is read (ADR-0041). The same block
        // resolves under the JSONL format.
        let flatten = transform_yaml("flatten: {customer.name: contact}");
        let error = resolve_schema_directive(None, Some(&flatten), "csv")
            .err()
            .expect("flatten on csv rejected");
        assert_eq!(error.code, "invalid_transform_config");
        assert_eq!(
            error.message,
            "transform.flatten requires a JSONL source format; \
             the resolved source format is csv"
        );

        resolve_schema_directive(None, Some(&flatten), "jsonl").expect("flatten on jsonl resolves");
    }

    #[test]
    fn resolve_schema_directive_validates_the_transform_before_the_schema_block() {
        // Both blocks are broken: the transform precedes overrides and
        // pinning in the meaning order, so its config failure wins.
        let mut config = schema_config(None, None);
        config.overrides = Some(Vec::new());
        let error =
            resolve_schema_directive(Some(&config), Some(&transform_yaml("select: []")), "jsonl")
                .err()
                .expect("invalid transform rejected");
        assert_eq!(error.code, "invalid_transform_config");
    }

    #[test]
    fn resolve_schema_directive_carries_the_transform_onto_every_directive() {
        let work = tempfile::TempDir::new().expect("tempdir");
        let pinned_path = work.path().join("customers.schema.yml");
        let transform = transform_yaml("select: [id]");

        // No schema block: a plain inference directive with the transform.
        match resolve_schema_directive(None, Some(&transform), "jsonl")
            .expect("transform-only resolves")
        {
            schema::SchemaDirective::Inferred { transform, .. } => assert!(!transform.is_empty()),
            _ => panic!("expected the Inferred directive"),
        }

        // Absent pin file: the bootstrap directive carries the transform.
        let config = schema_config(Some(pinned_path.to_str().expect("utf8 path")), None);
        match resolve_schema_directive(Some(&config), Some(&transform), "jsonl")
            .expect("absent pin bootstraps")
        {
            schema::SchemaDirective::PinInferred { transform, .. } => {
                assert!(!transform.is_empty())
            }
            _ => panic!("expected the PinInferred directive"),
        }

        // Existing pin file: the pinned directive carries the transform.
        fs::write(
            &pinned_path,
            "version: 1\nfields:\n- name: id\n  type: int64\n",
        )
        .expect("write pin");
        match resolve_schema_directive(Some(&config), Some(&transform), "jsonl")
            .expect("existing pin loads")
        {
            schema::SchemaDirective::Pinned { transform, .. } => assert!(!transform.is_empty()),
            _ => panic!("expected the Pinned directive"),
        }
    }

    #[test]
    fn resolve_schema_directive_rejects_unknown_drift_policies_before_reading_the_pin() {
        // The pin path does not exist: an unknown policy must fail instead of
        // silently bootstrapping.
        let config = schema_config(Some("/does/not/exist.schema.yml"), Some("relaxed"));
        let error = resolve_schema_directive(Some(&config), None, "csv")
            .err()
            .expect("unknown drift policy rejected");
        assert_eq!(error.code, "unsupported_drift_policy");
        assert_eq!(error.message, "unsupported drift policy: relaxed");
    }

    #[test]
    fn resolve_schema_directive_bootstraps_when_the_pin_file_is_absent() {
        let config = schema_config(Some("/does/not/exist/customers.schema.yml"), Some("fail"));
        match resolve_schema_directive(Some(&config), None, "csv").expect("absent pin bootstraps") {
            schema::SchemaDirective::PinInferred { pinned_path, .. } => {
                assert_eq!(pinned_path, "/does/not/exist/customers.schema.yml");
            }
            _ => panic!("expected the PinInferred directive"),
        }
    }

    #[test]
    fn load_definition_parses_schema_overrides() {
        // Entries may set type, nullable, or both; name alone still parses —
        // rejecting a no-op entry is directive resolution's job, so the error
        // is a schema config failure rather than a YAML parse failure.
        let definition = serde_yaml::from_str::<LoadDefinition>(
            "version: 1\n\
             schema:\n\
             \x20 overrides:\n\
             \x20 - name: customer_id\n\
             \x20   type: utf8\n\
             \x20 - name: email\n\
             \x20   nullable: false\n\
             \x20 - name: total\n",
        )
        .expect("definition with overrides parses");
        let overrides = definition
            .schema
            .expect("schema block")
            .overrides
            .expect("overrides");
        assert_eq!(overrides.len(), 3);

        // An entry without a name is not part of the contract.
        assert!(serde_yaml::from_str::<LoadDefinition>(
            "version: 1\nschema:\n  overrides:\n  - type: utf8\n"
        )
        .is_err());
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

        let error = resolve_schema_directive(Some(&config), None, "csv")
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

        match resolve_schema_directive(Some(&config), None, "csv").expect("existing pin loads") {
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
