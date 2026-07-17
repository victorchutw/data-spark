//! Source and Destination connector ports: the single home for the "where data
//! comes from and where it goes" story of a load.
//!
//! Every connector in the ADR-bound matrix (local files, S3, PostgreSQL, ... as
//! sources; Parquet, DuckDB, BigQuery, ... as destinations) plugs in behind one
//! of two trait-object ports — [`Source`] and [`Destination`] — reached only
//! through the pure [`source_connector`] / [`destination_connector`] factories.
//! A source owns reading and materialization under the load's schema directive
//! ([`SourceRead`]); a destination owns writing and reports its own write facts
//! ([`DestinationWrite`]) so the orchestrator never branches on connector
//! identity. The load mode is parsed
//! once into [`LoadMode`] at the write-dispatch boundary. `local_file`,
//! `parquet`, and `duckdb` are the first connectors; everything else here is
//! private.

use crate::rejection::{self, RejectedRecord};
use crate::schema::{self, SchemaDirective};
use crate::{DestinationDefinition, ExecutionFailure, LoadFailure, SourceDefinition};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use duckdb::vtab::arrow::{arrow_recordbatch_to_query_params, ArrowVTab};
use duckdb::Connection;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use serde_json::Value;
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader};
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// What a [`Source`] hands back: the materialized Arrow batch of the surviving
/// records, the `schema_decision` shape the report echoes, the pinned schema
/// file write the orchestrator performs when the load produces or extends a
/// pin, the source bytes the source measured, and the rejected records — parse
/// rejections from the reader merged with per-record validation rejections
/// from materialization (ADR-0035). Recombines [`schema::Materialized`] with
/// the source-owned facts so `schema.rs` stays types-only.
pub(crate) struct SourceRead {
    pub(crate) batch: RecordBatch,
    pub(crate) schema_decision: Value,
    pub(crate) pinned_schema_write: Option<schema::PinnedSchemaWrite>,
    pub(crate) source_bytes: u64,
    pub(crate) rejected: Vec<RejectedRecord>,
}

impl SourceRead {
    /// Recombines a materialized batch with the source bytes the source measured
    /// and the reader's parse rejections into one read result, keeping
    /// `schema.rs` types-only.
    fn from_materialized(
        materialized: schema::Materialized,
        source_bytes: u64,
        parse_rejected: Vec<RejectedRecord>,
    ) -> Self {
        let mut rejected = parse_rejected;
        rejected.extend(materialized.rejected);
        SourceRead {
            batch: materialized.batch,
            schema_decision: materialized.schema_decision,
            pinned_schema_write: materialized.pinned_schema_write,
            source_bytes,
            rejected,
        }
    }
}

/// The destination-owned write facts serialized into the Load Report
/// (ADR-0021). Keeping it typed until the report boundary prevents success and
/// failure paths from constructing subtly different JSON shapes.
#[derive(Clone, Copy, Debug)]
pub(crate) struct DestinationWriteFacts {
    atomicity: &'static str,
    strategy: Option<&'static str>,
}

impl DestinationWriteFacts {
    pub(crate) fn not_applicable() -> Self {
        DestinationWriteFacts {
            atomicity: "not_applicable",
            strategy: None,
        }
    }

    pub(crate) fn atomic(strategy: &'static str) -> Self {
        DestinationWriteFacts {
            atomicity: "atomic",
            strategy: Some(strategy),
        }
    }

    pub(crate) fn best_effort(strategy: &'static str) -> Self {
        DestinationWriteFacts {
            atomicity: "best_effort",
            strategy: Some(strategy),
        }
    }

    pub(crate) fn report_value(self) -> Value {
        match self.strategy {
            Some(strategy) => serde_json::json!({
                "atomicity": self.atomicity,
                "strategy": strategy
            }),
            None => serde_json::json!({
                "atomicity": self.atomicity
            }),
        }
    }
}

/// What a [`Destination`] hands back: the bytes it wrote plus the write facts
/// it owns. `bytes_written` is `None` when the destination has no honest byte
/// count to report: a database table, unlike a file directory, has no measurable
/// on-disk extent of its own (ADR-0030).
pub(crate) struct DestinationWrite {
    pub(crate) bytes_written: Option<u64>,
    pub(crate) facts: DestinationWriteFacts,
}

/// A destination failure plus the write facts the report can state honestly.
/// Failures before a destination write begins use `not_applicable`; a connector
/// that crosses a best-effort write boundary attaches its strategy so operators
/// know the failed load may have changed the destination (ADR-0021).
#[derive(Debug)]
pub(crate) struct DestinationWriteFailure {
    pub(crate) failure: LoadFailure,
    pub(crate) facts: DestinationWriteFacts,
    pub(crate) written_records: u64,
}

impl DestinationWriteFailure {
    fn atomic(failure: LoadFailure, strategy: &'static str, written_records: u64) -> Self {
        DestinationWriteFailure {
            failure,
            facts: DestinationWriteFacts::atomic(strategy),
            written_records,
        }
    }

    fn best_effort(failure: LoadFailure, strategy: &'static str, written_records: u64) -> Self {
        DestinationWriteFailure {
            failure,
            facts: DestinationWriteFacts::best_effort(strategy),
            written_records,
        }
    }
}

impl From<LoadFailure> for DestinationWriteFailure {
    fn from(failure: LoadFailure) -> Self {
        DestinationWriteFailure {
            failure,
            facts: DestinationWriteFacts::not_applicable(),
            written_records: 0,
        }
    }
}

/// The rule that decides how a load changes the destination dataset. Parsed once
/// from the raw load-definition string at the write-dispatch boundary; the
/// report still carries the raw string. Full refresh and append exist today;
/// merge follows them (ADR-0008).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoadMode {
    FullRefresh,
    Append,
}

impl LoadMode {
    /// Parses the load mode, the single validation point for the raw string.
    pub(crate) fn parse(load_mode: &str) -> Result<Self, LoadFailure> {
        match load_mode {
            "full_refresh" => Ok(LoadMode::FullRefresh),
            "append" => Ok(LoadMode::Append),
            other => Err(LoadFailure {
                code: "unsupported_load_mode",
                message: format!("unsupported load mode: {other}"),
            }),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            LoadMode::FullRefresh => "full_refresh",
            LoadMode::Append => "append",
        }
    }
}

const FULL_REFRESH_AND_APPEND_LOAD_MODES: &[LoadMode] = &[LoadMode::FullRefresh, LoadMode::Append];

/// A named capability for reading records from a source. Reads and materializes
/// under the load's schema directive behind one narrow call so the orchestrator
/// never sees connector internals. Only reads make schema decisions, so only
/// this port's failures can carry one ([`ExecutionFailure`]).
pub(crate) trait Source {
    fn read(&self, directive: &SchemaDirective) -> Result<SourceRead, ExecutionFailure>;
}

/// A named capability for writing records to a destination. Declares the load
/// modes it supports, owns how each commits, and reports its own write facts.
pub(crate) trait Destination {
    fn supported_load_modes(&self) -> &'static [LoadMode];

    fn validate_mode(&self, mode: LoadMode) -> Result<(), LoadFailure> {
        if self.supported_load_modes().contains(&mode) {
            Ok(())
        } else {
            Err(LoadFailure {
                code: "unsupported_load_mode",
                message: format!("destination does not support load mode: {}", mode.as_str()),
            })
        }
    }

    fn write(
        &self,
        batch: &RecordBatch,
        mode: LoadMode,
    ) -> Result<DestinationWrite, DestinationWriteFailure>;
}

/// Resolves a source definition to its connector. Pure: validates the connector
/// name only, doing no I/O, so an unsupported connector fails before any source
/// read (ADR-0019). Format validation is deferred to [`Source::read`] to
/// preserve error precedence for a doubly-invalid definition.
pub(crate) fn source_connector(
    definition: &SourceDefinition,
) -> Result<Box<dyn Source>, LoadFailure> {
    match definition.connector.as_str() {
        "local_file" => Ok(Box::new(LocalFileSource {
            path: definition.path.clone(),
            format: definition.format.clone(),
        })),
        other => Err(LoadFailure {
            code: "unsupported_source_connector",
            message: format!("unsupported source connector: {other}"),
        }),
    }
}

/// Resolves a destination definition to its connector. Pure: validates the
/// connector name plus the addressing the connector needs — for `duckdb`, a
/// present `dataset` naming the destination table (ADR-0030) — doing no I/O,
/// so an unsupported or incomplete definition fails before any destination
/// write (ADR-0019).
pub(crate) fn destination_connector(
    definition: &DestinationDefinition,
    dataset: Option<&str>,
) -> Result<Box<dyn Destination>, LoadFailure> {
    match definition.connector.as_str() {
        "parquet" => Ok(Box::new(ParquetDestination {
            path: definition.path.clone(),
        })),
        "duckdb" => {
            // An empty dataset is no address at all, so it fails here as
            // missing rather than at write time; identifier content beyond
            // presence is deliberately left to DuckDB (no allowlist, ADR-0030).
            let dataset = dataset
                .filter(|dataset| !dataset.is_empty())
                .ok_or_else(|| LoadFailure {
                    code: "missing_dataset",
                    message: "load definition dataset is required for a duckdb destination"
                        .to_string(),
                })?;
            Ok(Box::new(DuckDbDestination {
                path: definition.path.clone(),
                dataset: dataset.to_string(),
            }))
        }
        other => Err(LoadFailure {
            code: "unsupported_destination_connector",
            message: format!("unsupported destination connector: {other}"),
        }),
    }
}

/// Reads local CSV and JSONL files. The `format` is resolved from the definition
/// or the path extension and validated before the file is opened, so an
/// unsupported format fails without source I/O.
struct LocalFileSource {
    path: PathBuf,
    format: Option<String>,
}

impl Source for LocalFileSource {
    fn read(&self, directive: &SchemaDirective) -> Result<SourceRead, ExecutionFailure> {
        // Validate the resolved format before opening the file so an unsupported
        // format fails without source I/O (ADR-0019), and so its precedence
        // stays after the destination-connector check for a doubly-invalid
        // definition (the check lives here, not in the factory).
        match self.resolved_format().as_str() {
            "csv" => {
                let CsvRecords {
                    field_names,
                    records,
                    rejected,
                    source_bytes,
                } = read_local_csv(&self.path)?;
                merge_read_result(
                    schema::from_text_columns(directive, field_names, records),
                    source_bytes,
                    rejected,
                )
            }
            "jsonl" => {
                let JsonlRecords {
                    field_names,
                    records,
                    rejected,
                    source_bytes,
                } = read_local_jsonl(&self.path)?;
                // No record ever parsed with fields, so no schema can be
                // inferred or validated: the load fails, with the parse
                // rejections travelling on the failure so their artifact is
                // still written. This is asymmetric with CSV on purpose — a
                // CSV header declares the source's fields even when every
                // record is rejected, while JSONL fields exist only in
                // records.
                if field_names.is_empty() {
                    return Err(ExecutionFailure {
                        failure: LoadFailure {
                            code: "malformed_jsonl",
                            message: format!(
                                "JSONL source {} must include at least one record with fields",
                                self.path.display()
                            ),
                        },
                        schema_decision: None,
                        source_rows: Some(records.len() as u64 + rejected.len() as u64),
                        written_records: 0,
                        rejected,
                        destination_write: Box::new(DestinationWriteFacts::not_applicable()),
                    });
                }
                merge_read_result(
                    schema::from_json_columns(directive, field_names, records),
                    source_bytes,
                    rejected,
                )
            }
            _ => Err(LoadFailure {
                code: "unsupported_source_format",
                message: "only local CSV and JSONL sources are supported by this load path"
                    .to_string(),
            }
            .into()),
        }
    }
}

impl LocalFileSource {
    /// Resolves the source format from the explicit `format` or, failing that,
    /// the path extension.
    fn resolved_format(&self) -> String {
        self.format.clone().unwrap_or_else(|| {
            self.path
                .extension()
                .and_then(|extension| extension.to_str())
                .unwrap_or("")
                .to_string()
        })
    }
}

/// Writes an Arrow batch to a local Parquet directory. Full refresh stages the
/// dataset and replaces the destination; append stages one new part before
/// adding it to the existing directory. Both report best-effort atomicity
/// (ADR-0021).
struct ParquetDestination {
    path: PathBuf,
}

impl Destination for ParquetDestination {
    fn supported_load_modes(&self) -> &'static [LoadMode] {
        FULL_REFRESH_AND_APPEND_LOAD_MODES
    }

    fn write(
        &self,
        batch: &RecordBatch,
        mode: LoadMode,
    ) -> Result<DestinationWrite, DestinationWriteFailure> {
        match mode {
            LoadMode::FullRefresh => self.write_full_refresh(batch),
            LoadMode::Append => self.write_append(batch),
        }
    }
}

impl ParquetDestination {
    fn write_full_refresh(
        &self,
        batch: &RecordBatch,
    ) -> Result<DestinationWrite, DestinationWriteFailure> {
        let destination_path = self.path.as_path();
        let parent = destination_path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(|error| LoadFailure {
            code: "destination_write_failed",
            message: format!(
                "failed to prepare destination parent {}: {error}",
                parent.display()
            ),
        })?;

        // A per-write token keeps the staging directory unique so concurrent
        // loads to one destination cannot collide before the atomic rename.
        let staging_token = Uuid::new_v4();
        let destination_name = destination_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("dataset");
        let staging_path = parent.join(format!(
            ".{destination_name}.data-spark-staging-{staging_token}"
        ));
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
        let bytes_written = write_parquet_batch(&parquet_file_path, batch)?;

        remove_path_if_exists(destination_path).map_err(|error| {
            DestinationWriteFailure::best_effort(
                LoadFailure {
                    code: "destination_write_failed",
                    message: format!(
                        "failed to replace existing destination {}: {error}",
                        destination_path.display()
                    ),
                },
                "staging_then_replace",
                0,
            )
        })?;
        fs::rename(&staging_path, destination_path).map_err(|error| {
            DestinationWriteFailure::best_effort(
                LoadFailure {
                    code: "destination_write_failed",
                    message: format!(
                        "failed to commit Parquet destination {}: {error}",
                        destination_path.display()
                    ),
                },
                "staging_then_replace",
                0,
            )
        })?;

        Ok(DestinationWrite {
            bytes_written: Some(bytes_written),
            facts: DestinationWriteFacts::best_effort("staging_then_replace"),
        })
    }

    fn write_append(
        &self,
        batch: &RecordBatch,
    ) -> Result<DestinationWrite, DestinationWriteFailure> {
        let destination_path = self.path.as_path();
        fs::create_dir_all(destination_path).map_err(|error| LoadFailure {
            code: "destination_write_failed",
            message: format!(
                "failed to prepare Parquet destination {}: {error}",
                destination_path.display()
            ),
        })?;
        let append_batch = prepare_parquet_append_batch(destination_path, batch)?;

        // A complete Parquet file is staged under a non-data extension and then
        // renamed into the dataset. Readers never observe a partially written
        // part, while the dataset-level append remains best-effort (ADR-0021).
        let part_token = Uuid::new_v4();
        let staging_path =
            destination_path.join(format!(".part-{part_token}.data-spark-staging.tmp"));
        let parquet_file_path = destination_path.join(format!("part-{part_token}.parquet"));
        let bytes_written = match write_parquet_batch(&staging_path, &append_batch) {
            Ok(bytes_written) => bytes_written,
            Err(failure) => {
                let _ = remove_path_if_exists(&staging_path);
                return Err(failure.into());
            }
        };
        if let Err(error) = fs::rename(&staging_path, &parquet_file_path) {
            let _ = remove_path_if_exists(&staging_path);
            return Err(DestinationWriteFailure::best_effort(
                LoadFailure {
                    code: "destination_write_failed",
                    message: format!(
                        "failed to commit Parquet part {}: {error}",
                        parquet_file_path.display()
                    ),
                },
                "staged_part_append",
                0,
            ));
        }

        Ok(DestinationWrite {
            bytes_written: Some(bytes_written),
            facts: DestinationWriteFacts::best_effort("staged_part_append"),
        })
    }
}

fn prepare_parquet_append_batch(
    destination_path: &Path,
    batch: &RecordBatch,
) -> Result<RecordBatch, LoadFailure> {
    let Some(destination_schema) = existing_parquet_schema(destination_path)? else {
        return Ok(batch.clone());
    };
    align_batch_to_destination_schema(destination_path, batch, destination_schema)
}

fn existing_parquet_schema(destination_path: &Path) -> Result<Option<SchemaRef>, LoadFailure> {
    let entries = fs::read_dir(destination_path).map_err(|error| LoadFailure {
        code: "destination_write_failed",
        message: format!(
            "failed to inspect Parquet destination {}: {error}",
            destination_path.display()
        ),
    })?;
    let mut parquet_paths = entries
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| LoadFailure {
            code: "destination_write_failed",
            message: format!(
                "failed to inspect Parquet destination {}: {error}",
                destination_path.display()
            ),
        })?;
    parquet_paths.retain(|path| {
        path.extension().and_then(|extension| extension.to_str()) == Some("parquet")
    });
    parquet_paths.sort();

    let mut destination_schema: Option<SchemaRef> = None;
    for parquet_path in parquet_paths {
        let file = File::open(&parquet_path).map_err(|error| LoadFailure {
            code: "destination_write_failed",
            message: format!(
                "failed to open existing Parquet file {}: {error}",
                parquet_path.display()
            ),
        })?;
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| LoadFailure {
                code: "destination_write_failed",
                message: format!(
                    "failed to read existing Parquet schema {}: {error}",
                    parquet_path.display()
                ),
            })?;
        let schema = builder.schema().clone();
        if let Some(expected) = &destination_schema {
            if expected.as_ref() != schema.as_ref() {
                return Err(LoadFailure {
                    code: "destination_write_failed",
                    message: format!(
                        "Parquet destination {} contains inconsistent schemas",
                        destination_path.display()
                    ),
                });
            }
        } else {
            destination_schema = Some(schema);
        }
    }
    Ok(destination_schema)
}

fn align_batch_to_destination_schema(
    destination_path: &Path,
    batch: &RecordBatch,
    destination_schema: SchemaRef,
) -> Result<RecordBatch, LoadFailure> {
    let source_schema = batch.schema();
    if source_schema.fields().len() != destination_schema.fields().len() {
        return Err(parquet_schema_mismatch(destination_path));
    }

    let mut columns = Vec::with_capacity(destination_schema.fields().len());
    for destination_field in destination_schema.fields() {
        let source_index = source_schema
            .index_of(destination_field.name())
            .map_err(|_| parquet_schema_mismatch(destination_path))?;
        let source_field = source_schema.field(source_index);
        let column = batch.column(source_index);
        if source_field.data_type() != destination_field.data_type()
            || (!destination_field.is_nullable() && column.null_count() > 0)
        {
            return Err(parquet_schema_mismatch(destination_path));
        }
        columns.push(column.clone());
    }

    RecordBatch::try_new(destination_schema, columns).map_err(|error| LoadFailure {
        code: "destination_write_failed",
        message: format!(
            "failed to align append records with Parquet destination {}: {error}",
            destination_path.display()
        ),
    })
}

fn parquet_schema_mismatch(destination_path: &Path) -> LoadFailure {
    LoadFailure {
        code: "destination_write_failed",
        message: format!(
            "append schema does not match Parquet destination {}",
            destination_path.display()
        ),
    }
}

fn write_parquet_batch(path: &Path, batch: &RecordBatch) -> Result<u64, LoadFailure> {
    let file = File::create(path).map_err(|error| LoadFailure {
        code: "destination_write_failed",
        message: format!("failed to create Parquet file {}: {error}", path.display()),
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
    writer.finish().map_err(|error| LoadFailure {
        code: "destination_write_failed",
        message: format!("failed to finish Parquet writer: {error}"),
    })?;
    Ok(writer.bytes_written() as u64)
}

/// Writes an Arrow batch into a table of a local DuckDB database file named by
/// the destination path, with the table named by the load definition's
/// `dataset`. Full refresh replaces the table in one
/// `CREATE OR REPLACE TABLE "<dataset>" AS SELECT * FROM arrow(?, ?)` statement;
/// append reads the existing table's Arrow schema, aligns matching fields by
/// name and exact type, then inserts through the same Arrow table function
/// without replacing existing records. Arrow-to-DuckDB type mapping stays
/// delegated to DuckDB (ADR-0030).
struct DuckDbDestination {
    path: PathBuf,
    dataset: String,
}

impl Destination for DuckDbDestination {
    fn supported_load_modes(&self) -> &'static [LoadMode] {
        FULL_REFRESH_AND_APPEND_LOAD_MODES
    }

    fn write(
        &self,
        batch: &RecordBatch,
        mode: LoadMode,
    ) -> Result<DestinationWrite, DestinationWriteFailure> {
        match mode {
            LoadMode::FullRefresh => self.write_full_refresh(batch),
            LoadMode::Append => self.write_append(batch),
        }
    }
}

impl DuckDbDestination {
    fn write_full_refresh(
        &self,
        batch: &RecordBatch,
    ) -> Result<DestinationWrite, DestinationWriteFailure> {
        let connection = self.open_arrow_connection()?;

        // The one statement auto-commits, so the replace needs no explicit
        // BEGIN / COMMIT until a multi-statement load mode does (ADR-0030). The
        // table identifier is always double-quote-escaped (`"` doubled) rather
        // than restricted to a character allowlist.
        let statement = format!(
            "CREATE OR REPLACE TABLE {} AS SELECT * FROM arrow(?, ?)",
            self.quoted_dataset()
        );
        connection
            .execute(&statement, arrow_recordbatch_to_query_params(batch.clone()))
            .map_err(|error| {
                DestinationWriteFailure::atomic(
                    LoadFailure {
                        code: "destination_write_failed",
                        message: format!(
                            "failed to replace DuckDB table {} in {}: {error}",
                            self.dataset,
                            self.path.display()
                        ),
                    },
                    "transactional_replace",
                    0,
                )
            })?;
        connection.close().map_err(|(_, error)| {
            DestinationWriteFailure::atomic(
                LoadFailure {
                    code: "destination_write_failed",
                    message: format!(
                        "failed to close DuckDB database {}: {error}",
                        self.path.display()
                    ),
                },
                "transactional_replace",
                batch.num_rows() as u64,
            )
        })?;

        Ok(DestinationWrite {
            bytes_written: None,
            facts: DestinationWriteFacts::atomic("transactional_replace"),
        })
    }

    fn write_append(
        &self,
        batch: &RecordBatch,
    ) -> Result<DestinationWrite, DestinationWriteFailure> {
        let connection = self.open_arrow_connection()?;
        // Inspect and align before crossing the append write boundary: DuckDB's
        // INSERT BY NAME would otherwise coerce lossy types or fill omissions
        // with nulls instead of enforcing the destination dataset schema.
        let append_batch = self.prepare_append_batch(&connection, batch)?;

        let statement = format!(
            "INSERT INTO {} BY NAME SELECT * FROM arrow(?, ?)",
            self.quoted_dataset()
        );
        connection
            .execute(
                &statement,
                arrow_recordbatch_to_query_params(append_batch.clone()),
            )
            .map_err(|error| {
                DestinationWriteFailure::best_effort(
                    LoadFailure {
                        code: "destination_write_failed",
                        message: format!(
                            "failed to append to DuckDB table {} in {}: {error}",
                            self.dataset,
                            self.path.display()
                        ),
                    },
                    "insert",
                    0,
                )
            })?;
        connection.close().map_err(|(_, error)| {
            DestinationWriteFailure::best_effort(
                LoadFailure {
                    code: "destination_write_failed",
                    message: format!(
                        "failed to close DuckDB database {}: {error}",
                        self.path.display()
                    ),
                },
                "insert",
                append_batch.num_rows() as u64,
            )
        })?;

        Ok(DestinationWrite {
            bytes_written: None,
            facts: DestinationWriteFacts::best_effort("insert"),
        })
    }

    fn prepare_append_batch(
        &self,
        connection: &Connection,
        batch: &RecordBatch,
    ) -> Result<RecordBatch, LoadFailure> {
        let statement = format!("SELECT * FROM {} LIMIT 0", self.quoted_dataset());
        let mut statement = connection
            .prepare(&statement)
            .map_err(|error| LoadFailure {
                code: "destination_write_failed",
                message: format!(
                    "failed to inspect DuckDB table {} in {} before append: {error}",
                    self.dataset,
                    self.path.display()
                ),
            })?;
        let destination_schema = statement
            .query_arrow([])
            .map_err(|error| LoadFailure {
                code: "destination_write_failed",
                message: format!(
                    "failed to inspect DuckDB table {} in {} before append: {error}",
                    self.dataset,
                    self.path.display()
                ),
            })?
            .get_schema();

        align_batch_to_duckdb_schema(self, batch, destination_schema)
    }

    fn open_arrow_connection(&self) -> Result<Connection, LoadFailure> {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(|error| LoadFailure {
            code: "destination_write_failed",
            message: format!(
                "failed to prepare destination parent {}: {error}",
                parent.display()
            ),
        })?;

        let connection = Connection::open(&self.path).map_err(|error| LoadFailure {
            code: "destination_write_failed",
            message: format!(
                "failed to open DuckDB database {}: {error}",
                self.path.display()
            ),
        })?;
        connection
            .register_table_function::<ArrowVTab>("arrow")
            .map_err(|error| LoadFailure {
                code: "destination_write_failed",
                message: format!("failed to register the DuckDB Arrow table function: {error}"),
            })?;
        Ok(connection)
    }

    fn quoted_dataset(&self) -> String {
        format!("\"{}\"", self.dataset.replace('"', "\"\""))
    }
}

fn align_batch_to_duckdb_schema(
    destination: &DuckDbDestination,
    batch: &RecordBatch,
    destination_schema: SchemaRef,
) -> Result<RecordBatch, LoadFailure> {
    let source_schema = batch.schema();
    if source_schema.fields().len() != destination_schema.fields().len() {
        return Err(duckdb_schema_mismatch(destination));
    }

    let mut columns = Vec::with_capacity(destination_schema.fields().len());
    for destination_field in destination_schema.fields() {
        let source_index = source_schema
            .index_of(destination_field.name())
            .map_err(|_| duckdb_schema_mismatch(destination))?;
        let source_field = source_schema.field(source_index);
        let column = batch.column(source_index);
        if source_field.data_type() != destination_field.data_type()
            || (!destination_field.is_nullable() && column.null_count() > 0)
        {
            return Err(duckdb_schema_mismatch(destination));
        }
        columns.push(column.clone());
    }

    RecordBatch::try_new(destination_schema, columns).map_err(|error| LoadFailure {
        code: "destination_write_failed",
        message: format!(
            "failed to align append records with DuckDB destination {} in {}: {error}",
            destination.dataset,
            destination.path.display()
        ),
    })
}

fn duckdb_schema_mismatch(destination: &DuckDbDestination) -> LoadFailure {
    LoadFailure {
        code: "destination_write_failed",
        message: format!(
            "append schema does not match DuckDB destination {} in {}",
            destination.dataset,
            destination.path.display()
        ),
    }
}

/// Joins a materialization outcome with the source-owned facts: the byte
/// count and the reader's parse rejections, which merge with the
/// materialization's validation rejections on success and travel with the
/// failure otherwise, so the rejected-records artifact is written either way.
fn merge_read_result(
    result: Result<schema::Materialized, ExecutionFailure>,
    source_bytes: u64,
    parse_rejected: Vec<RejectedRecord>,
) -> Result<SourceRead, ExecutionFailure> {
    match result {
        Ok(materialized) => Ok(SourceRead::from_materialized(
            materialized,
            source_bytes,
            parse_rejected,
        )),
        Err(failure) => {
            let mut rejected = parse_rejected;
            rejected.extend(failure.rejected);
            Err(ExecutionFailure {
                rejected,
                ..failure
            })
        }
    }
}

/// CSV cells arrive untyped as text; `schema::from_text_columns` infers their
/// types. Records that fail to parse as a record of the header's fields are
/// rejected, not the load. `source_bytes` is measured separately from the
/// values.
struct CsvRecords {
    field_names: Vec<String>,
    records: Vec<schema::TextRecord>,
    rejected: Vec<RejectedRecord>,
    source_bytes: u64,
}

/// JSONL cells arrive as parsed [`Value`]s carrying their own type;
/// `schema::from_json_columns` reads types from them directly. Lines that are
/// not a JSON object are rejected, not the load.
struct JsonlRecords {
    field_names: Vec<String>,
    records: Vec<schema::JsonRecord>,
    rejected: Vec<RejectedRecord>,
    source_bytes: u64,
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

    // Flexible parsing so a record with the wrong field count arrives as a
    // record and is rejected here with a clear message, instead of failing
    // the whole read (ADR-0036).
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
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
    let mut rejected = Vec::new();
    for record in reader.records() {
        match record {
            // The reader keeps yielding records after a per-record parse
            // error, so one unreadable record rejects only itself.
            Err(error) => {
                let line = error
                    .position()
                    .map(|position| position.line())
                    .unwrap_or(0);
                rejected.push(RejectedRecord {
                    line,
                    code: rejection::MALFORMED_CSV_RECORD,
                    field: None,
                    source_field: None,
                    message: error.to_string(),
                    record: Value::Null,
                });
            }
            Ok(record) => {
                let line = record
                    .position()
                    .map(|position| position.line())
                    .unwrap_or(0);
                if record.len() != field_names.len() {
                    // A wrong-length record cannot map onto the header, so
                    // its cells are recovered as an array.
                    rejected.push(RejectedRecord {
                        line,
                        code: rejection::MALFORMED_CSV_RECORD,
                        field: None,
                        source_field: None,
                        message: format!(
                            "expected {} fields, found {}",
                            field_names.len(),
                            record.len()
                        ),
                        record: Value::Array(
                            record
                                .iter()
                                .map(|value| Value::String(value.to_string()))
                                .collect(),
                        ),
                    });
                    continue;
                }
                records.push(schema::TextRecord {
                    line,
                    cells: record
                        .iter()
                        .map(|value| {
                            if value.is_empty() {
                                None
                            } else {
                                Some(value.to_string())
                            }
                        })
                        .collect::<Vec<_>>(),
                });
            }
        }
    }

    Ok(CsvRecords {
        field_names,
        records,
        rejected,
        source_bytes,
    })
}

fn read_local_jsonl(source_path: &Path) -> Result<JsonlRecords, ExecutionFailure> {
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
    // than an extra whole-file string copy, matching the CSV reader. Reading
    // bytes first lets invalid UTF-8 reject only its record instead of erasing
    // the source facts established by earlier lines.
    let mut field_names: Vec<String> = Vec::new();
    let mut seen_fields: HashSet<String> = HashSet::new();
    let mut records: Vec<schema::JsonRecord> = Vec::new();
    let mut rejected = Vec::new();
    let mut reader = BufReader::new(file);
    let mut line_bytes = Vec::new();
    let mut line_number = 0_u64;
    loop {
        line_bytes.clear();
        match reader.read_until(b'\n', &mut line_bytes) {
            Ok(0) => break,
            Ok(_) => line_number += 1,
            Err(error) => {
                return Err(ExecutionFailure {
                    failure: LoadFailure {
                        code: "source_read_failed",
                        message: format!(
                            "failed to read JSONL source {} after line {line_number}: {error}",
                            source_path.display(),
                        ),
                    },
                    schema_decision: None,
                    source_rows: Some(records.len() as u64 + rejected.len() as u64),
                    written_records: 0,
                    rejected,
                    destination_write: Box::new(DestinationWriteFacts::not_applicable()),
                });
            }
        }
        if line_bytes.last() == Some(&b'\n') {
            line_bytes.pop();
        }
        if line_bytes.last() == Some(&b'\r') {
            line_bytes.pop();
        }
        let line = match std::str::from_utf8(&line_bytes) {
            Ok(line) => line,
            Err(error) => {
                rejected.push(RejectedRecord {
                    line: line_number,
                    code: rejection::MALFORMED_JSONL_RECORD,
                    field: None,
                    source_field: None,
                    message: error.to_string(),
                    record: Value::String(String::from_utf8_lossy(&line_bytes).into_owned()),
                });
                continue;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        // A line that is not a JSON object rejects that record, not the load;
        // the raw line is recovered for troubleshooting.
        let object = match serde_json::from_str::<Value>(line) {
            Ok(Value::Object(object)) => object,
            Ok(_) => {
                rejected.push(RejectedRecord {
                    line: line_number,
                    code: rejection::MALFORMED_JSONL_RECORD,
                    field: None,
                    source_field: None,
                    message: "each JSONL record must be a JSON object".to_string(),
                    record: Value::String(line.to_string()),
                });
                continue;
            }
            Err(error) => {
                rejected.push(RejectedRecord {
                    line: line_number,
                    code: rejection::MALFORMED_JSONL_RECORD,
                    field: None,
                    source_field: None,
                    message: error.to_string(),
                    record: Value::String(line.to_string()),
                });
                continue;
            }
        };
        for key in object.keys() {
            if seen_fields.insert(key.clone()) {
                field_names.push(key.clone());
            }
        }
        records.push(schema::JsonRecord {
            line: line_number,
            object,
        });
    }

    Ok(JsonlRecords {
        field_names,
        records,
        rejected,
        source_bytes,
    })
}

fn malformed_csv(source_path: &Path, error: csv::Error) -> LoadFailure {
    LoadFailure {
        code: "malformed_csv",
        message: format!("malformed CSV syntax in {}: {error}", source_path.display()),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
    use arrow_schema::DataType;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tempfile::TempDir;

    // ---- Pure validation (no I/O): pins the four connector/mode/format codes ----

    #[test]
    fn source_connector_accepts_local_file_and_rejects_unknown() {
        assert!(source_connector(&source_definition("local_file", "data.csv", None)).is_ok());

        let error = source_connector(&source_definition("s3", "data.csv", None))
            .err()
            .expect("unknown source connector rejected");
        assert_eq!(error.code, "unsupported_source_connector");
        assert_eq!(error.message, "unsupported source connector: s3");
    }

    #[test]
    fn destination_connector_accepts_parquet_and_rejects_unknown() {
        assert!(destination_connector(&destination_definition("parquet", "out"), None).is_ok());

        let error = destination_connector(&destination_definition("bigquery", "out"), None)
            .err()
            .expect("unknown destination connector rejected");
        assert_eq!(error.code, "unsupported_destination_connector");
        assert_eq!(error.message, "unsupported destination connector: bigquery");
    }

    #[test]
    fn destination_connector_requires_a_dataset_for_duckdb() {
        assert!(destination_connector(
            &destination_definition("duckdb", "customers.duckdb"),
            Some("customers")
        )
        .is_ok());

        let error =
            destination_connector(&destination_definition("duckdb", "customers.duckdb"), None)
                .err()
                .expect("duckdb destination without a dataset rejected");
        assert_eq!(error.code, "missing_dataset");
        assert_eq!(
            error.message,
            "load definition dataset is required for a duckdb destination"
        );

        let error = destination_connector(
            &destination_definition("duckdb", "customers.duckdb"),
            Some(""),
        )
        .err()
        .expect("duckdb destination with an empty dataset rejected");
        assert_eq!(error.code, "missing_dataset");
    }

    #[test]
    fn local_file_source_rejects_unknown_format_before_touching_the_file() {
        // A non-existent path proves the format check precedes source I/O: an
        // unknown format must fail without ever opening the file.
        let source = LocalFileSource {
            path: PathBuf::from("/does/not/exist.xml"),
            format: Some("xml".to_string()),
        };
        let error = source
            .read(&SchemaDirective::inferred())
            .err()
            .expect("unknown format rejected");
        assert_eq!(error.failure.code, "unsupported_source_format");
    }

    #[test]
    fn load_mode_parse_accepts_full_refresh_and_append_and_rejects_unknown() {
        assert!(matches!(
            LoadMode::parse("full_refresh"),
            Ok(LoadMode::FullRefresh)
        ));
        assert!(matches!(LoadMode::parse("append"), Ok(LoadMode::Append)));

        let error = LoadMode::parse("merge")
            .err()
            .expect("unknown load mode rejected");
        assert_eq!(error.code, "unsupported_load_mode");
        assert_eq!(error.message, "unsupported load mode: merge");
    }

    // ---- Round-trips (temp dir): source read and destination write ----

    #[test]
    fn local_file_source_reads_csv_into_a_typed_batch() {
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.csv");
        fs::write(&source_path, "id,name,total\n1,Ada,42.50\n2,Grace,7.25\n").expect("write csv");

        let source = LocalFileSource {
            path: source_path,
            format: Some("csv".to_string()),
        };
        let read = source.read(&SchemaDirective::inferred()).expect("read csv");

        assert_eq!(read.batch.num_rows(), 2);
        assert_eq!(
            schema_types(&read.batch),
            vec![DataType::Int64, DataType::Utf8, DataType::Float64]
        );
        assert_eq!(ints(&read.batch, 0).value(0), 1);
        assert_eq!(strings(&read.batch, 1).value(1), "Grace");
        assert_eq!(floats(&read.batch, 2).value(0), 42.50);
        assert!(read.source_bytes > 0);
        assert_eq!(read.schema_decision["mode"], "inferred");
    }

    #[test]
    fn local_file_source_reads_jsonl_into_a_typed_batch() {
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.jsonl");
        fs::write(
            &source_path,
            "{\"id\": 1, \"name\": \"Ada\", \"active\": true}\n\
             {\"id\": 2, \"name\": \"Grace\", \"active\": false}\n",
        )
        .expect("write jsonl");

        let source = LocalFileSource {
            path: source_path,
            format: Some("jsonl".to_string()),
        };
        let read = source
            .read(&SchemaDirective::inferred())
            .expect("read jsonl");

        assert_eq!(read.batch.num_rows(), 2);
        assert_eq!(
            schema_types(&read.batch),
            vec![DataType::Int64, DataType::Utf8, DataType::Boolean]
        );
        assert_eq!(ints(&read.batch, 0).value(1), 2);
        assert_eq!(strings(&read.batch, 1).value(0), "Ada");
        assert!(bools(&read.batch, 2).value(0));
        assert!(!bools(&read.batch, 2).value(1));
        assert!(read.source_bytes > 0);
        assert_eq!(read.schema_decision["mode"], "inferred");
    }

    #[test]
    fn local_file_source_rejects_csv_records_with_the_wrong_field_count_and_reads_on() {
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.csv");
        fs::write(
            &source_path,
            "id,name\n1,Ada\n2,Grace,extra-field\n3,Cara\n",
        )
        .expect("write csv");

        let source = LocalFileSource {
            path: source_path,
            format: Some("csv".to_string()),
        };
        let read = source
            .read(&SchemaDirective::inferred())
            .expect("a bad record rejects the record, not the load");

        assert_eq!(read.batch.num_rows(), 2);
        assert_eq!(ints(&read.batch, 0).value(0), 1);
        assert_eq!(ints(&read.batch, 0).value(1), 3);

        assert_eq!(read.rejected.len(), 1);
        let rejected = &read.rejected[0];
        assert_eq!(rejected.line, 3);
        assert_eq!(rejected.code, "malformed_csv_record");
        assert_eq!(rejected.field, None);
        assert_eq!(rejected.message, "expected 2 fields, found 3");
        // A wrong-length record cannot map onto the header, so its cells are
        // recovered as an array.
        assert_eq!(
            rejected.record,
            serde_json::json!(["2", "Grace", "extra-field"])
        );
    }

    #[test]
    fn local_file_source_rejects_csv_records_that_fail_to_parse_and_reads_on() {
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.csv");
        // Record 2 carries invalid UTF-8: the reader rejects that record and
        // keeps reading.
        fs::write(&source_path, b"id,name\n1,Ada\n2,\xFF\n3,Cara\n").expect("write csv");

        let source = LocalFileSource {
            path: source_path,
            format: Some("csv".to_string()),
        };
        let read = source
            .read(&SchemaDirective::inferred())
            .expect("an unparseable record rejects the record, not the load");

        assert_eq!(read.batch.num_rows(), 2);
        assert_eq!(ints(&read.batch, 0).value(1), 3);
        assert_eq!(read.rejected.len(), 1);
        let rejected = &read.rejected[0];
        assert_eq!(rejected.line, 3);
        assert_eq!(rejected.code, "malformed_csv_record");
        assert!(
            rejected.message.to_lowercase().contains("utf-8"),
            "message {:?} names the parse problem",
            rejected.message
        );
        // Nothing could be recovered from the record.
        assert_eq!(rejected.record, Value::Null);
    }

    #[test]
    fn local_file_source_rejects_malformed_jsonl_lines_and_reads_on() {
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.jsonl");
        // Line 2 is truncated JSON, line 3 is valid JSON but not an object,
        // line 4 is blank (skipped, not a record), line 5 is valid.
        fs::write(
            &source_path,
            "{\"id\": 1}\n{\"id\": 2, \n[1, 2]\n\n{\"id\": 4}\n",
        )
        .expect("write jsonl");

        let source = LocalFileSource {
            path: source_path,
            format: Some("jsonl".to_string()),
        };
        let read = source
            .read(&SchemaDirective::inferred())
            .expect("bad lines reject their records, not the load");

        assert_eq!(read.batch.num_rows(), 2);
        assert_eq!(ints(&read.batch, 0).value(0), 1);
        assert_eq!(ints(&read.batch, 0).value(1), 4);

        assert_eq!(read.rejected.len(), 2);
        assert_eq!(read.rejected[0].line, 2);
        assert_eq!(read.rejected[0].code, "malformed_jsonl_record");
        assert_eq!(read.rejected[0].field, None);
        assert!(!read.rejected[0].message.is_empty());
        // The raw line is recovered for troubleshooting.
        assert_eq!(read.rejected[0].record, serde_json::json!("{\"id\": 2, "));
        assert_eq!(read.rejected[1].line, 3);
        assert_eq!(read.rejected[1].code, "malformed_jsonl_record");
        assert_eq!(
            read.rejected[1].message,
            "each JSONL record must be a JSON object"
        );
        assert_eq!(read.rejected[1].record, serde_json::json!("[1, 2]"));
    }

    #[test]
    fn local_file_source_merges_parse_and_validation_rejections() {
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.csv");
        // Line 3 has the wrong field count (parse rejection); line 4 misfits
        // the pinned int64 (validation rejection); lines 2 survives.
        fs::write(&source_path, "id\n1\n2,extra\nabc\n").expect("write csv");

        let source = LocalFileSource {
            path: source_path,
            format: Some("csv".to_string()),
        };
        let directive = SchemaDirective::Pinned {
            pinned_path: "customers.schema.yml".to_string(),
            pin: schema::PinnedSchema::from_yaml(
                "version: 1\nfields:\n- name: id\n  type: int64\n",
            )
            .expect("test pin parses"),
            drift_policy: schema::DriftPolicy::Fail,
            transform: schema::SchemaTransform::none(),
            overrides: schema::SchemaOverrides::none(),
        };
        let read = source
            .read(&directive)
            .expect("rejections do not fail the read");

        assert_eq!(read.batch.num_rows(), 1);
        assert_eq!(ints(&read.batch, 0).value(0), 1);
        assert_eq!(read.rejected.len(), 2);
        let lines_and_codes = read
            .rejected
            .iter()
            .map(|rejected| (rejected.line, rejected.code))
            .collect::<Vec<_>>();
        assert!(lines_and_codes.contains(&(3, "malformed_csv_record")));
        assert!(lines_and_codes.contains(&(4, "type_coercion_failed")));
    }

    #[test]
    fn local_file_source_fails_jsonl_with_no_parseable_records_but_reports_the_rejections() {
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.jsonl");
        fs::write(&source_path, "not json\n[1]\n").expect("write jsonl");

        let source = LocalFileSource {
            path: source_path,
            format: Some("jsonl".to_string()),
        };
        let error = source
            .read(&SchemaDirective::inferred())
            .err()
            .expect("a source with no parseable records fails the load");

        // No record ever parsed, so no schema can be inferred: the load fails,
        // but the source count and parse rejections still travel with the
        // failure so the orchestrator can report them and write their artifact.
        assert_eq!(error.failure.code, "malformed_jsonl");
        assert!(error
            .failure
            .message
            .contains("must include at least one record with fields"));
        assert_eq!(error.source_rows, Some(2));
        assert_eq!(error.rejected.len(), 2);
        assert_eq!(error.rejected[0].line, 1);
        assert_eq!(error.rejected[1].line, 2);
    }

    #[test]
    fn local_file_source_counts_empty_jsonl_objects_when_no_fields_can_be_inferred() {
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.jsonl");
        fs::write(&source_path, "{}\nnot json\n").expect("write jsonl");

        let source = LocalFileSource {
            path: source_path,
            format: Some("jsonl".to_string()),
        };
        let error = source
            .read(&SchemaDirective::inferred())
            .err()
            .expect("a source with no fields fails the load");

        // The empty object parsed as a source record even though it declared no
        // fields, so it must remain part of the known source count alongside the
        // rejected line.
        assert_eq!(error.failure.code, "malformed_jsonl");
        assert_eq!(error.source_rows, Some(2));
        assert_eq!(error.rejected.len(), 1);
        assert_eq!(error.rejected[0].line, 2);
    }

    #[test]
    fn parquet_destination_writes_a_readable_batch_and_reports_write_facts() {
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.csv");
        fs::write(&source_path, "id,name\n1,Ada\n2,Grace\n").expect("write csv");
        let batch = LocalFileSource {
            path: source_path,
            format: Some("csv".to_string()),
        }
        .read(&SchemaDirective::inferred())
        .expect("read csv")
        .batch;

        let destination_path = work.path().join("customers_dataset");
        let destination = ParquetDestination {
            path: destination_path.clone(),
        };
        let written = destination
            .write(&batch, LoadMode::FullRefresh)
            .expect("write parquet");

        assert!(
            written
                .bytes_written
                .expect("parquet measures written bytes")
                > 0
        );
        assert_eq!(
            written.facts.report_value(),
            serde_json::json!({
                "atomicity": "best_effort",
                "strategy": "staging_then_replace"
            })
        );

        let read_back = read_single_parquet_batch(&destination_path);
        assert_eq!(read_back.num_rows(), 2);
        assert_eq!(ints(&read_back, 0).value(0), 1);
        assert_eq!(strings(&read_back, 1).value(1), "Grace");
    }

    #[test]
    fn duckdb_destination_writes_a_readable_table_and_reports_write_facts() {
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.csv");
        fs::write(&source_path, "id,name\n1,Ada\n2,Grace\n").expect("write csv");
        let batch = LocalFileSource {
            path: source_path,
            format: Some("csv".to_string()),
        }
        .read(&SchemaDirective::inferred())
        .expect("read csv")
        .batch;

        // The database file sits under a not-yet-existing parent to pin that
        // the destination prepares the parent directory like Parquet does.
        let database_path = work.path().join("warehouse").join("customers.duckdb");
        let destination = DuckDbDestination {
            path: database_path.clone(),
            dataset: "customers".to_string(),
        };
        let written = destination
            .write(&batch, LoadMode::FullRefresh)
            .expect("write duckdb");

        assert_eq!(written.bytes_written, None);
        assert_eq!(
            written.facts.report_value(),
            serde_json::json!({
                "atomicity": "atomic",
                "strategy": "transactional_replace"
            })
        );

        let read_back = read_single_duckdb_batch(&database_path, "customers");
        assert_eq!(read_back.num_rows(), 2);
        assert_eq!(ints(&read_back, 0).value(0), 1);
        assert_eq!(strings(&read_back, 1).value(1), "Grace");
    }

    #[test]
    fn duckdb_destination_full_refresh_replaces_the_existing_table() {
        let work = TempDir::new().expect("tempdir");
        let database_path = work.path().join("customers.duckdb");
        let destination = DuckDbDestination {
            path: database_path.clone(),
            dataset: "customers".to_string(),
        };

        let first_path = work.path().join("first.csv");
        fs::write(&first_path, "id,name\n1,Ada\n2,Grace\n").expect("write first csv");
        let second_path = work.path().join("second.csv");
        fs::write(&second_path, "id,name\n3,Katherine\n").expect("write second csv");
        for source_path in [first_path, second_path] {
            let batch = LocalFileSource {
                path: source_path,
                format: Some("csv".to_string()),
            }
            .read(&SchemaDirective::inferred())
            .expect("read csv")
            .batch;
            destination
                .write(&batch, LoadMode::FullRefresh)
                .expect("write duckdb");
        }

        let read_back = read_single_duckdb_batch(&database_path, "customers");
        assert_eq!(read_back.num_rows(), 1);
        assert_eq!(ints(&read_back, 0).value(0), 3);
        assert_eq!(strings(&read_back, 1).value(0), "Katherine");
    }

    // ---- Test helpers ----

    fn source_definition(connector: &str, path: &str, format: Option<&str>) -> SourceDefinition {
        SourceDefinition {
            connector: connector.to_string(),
            path: PathBuf::from(path),
            format: format.map(str::to_string),
        }
    }

    fn destination_definition(connector: &str, path: &str) -> DestinationDefinition {
        DestinationDefinition {
            connector: connector.to_string(),
            path: PathBuf::from(path),
        }
    }

    fn read_single_duckdb_batch(database_path: &Path, dataset: &str) -> RecordBatch {
        let connection = Connection::open(database_path).expect("open duckdb database");
        let mut statement = connection
            .prepare(&format!(
                "SELECT * FROM \"{}\" ORDER BY 1",
                dataset.replace('"', "\"\"")
            ))
            .expect("prepare duckdb read-back");
        let batches = statement
            .query_arrow([])
            .expect("query duckdb table")
            .collect::<Vec<_>>();
        assert_eq!(batches.len(), 1, "one duckdb batch expected");
        batches.into_iter().next().expect("one duckdb batch")
    }

    fn read_single_parquet_batch(destination_path: &Path) -> RecordBatch {
        let parquet_path = fs::read_dir(destination_path)
            .expect("destination directory")
            .map(|entry| entry.expect("destination entry").path())
            .find(|path| path.extension().and_then(|ext| ext.to_str()) == Some("parquet"))
            .expect("one parquet data file");
        let file = File::open(parquet_path).expect("open parquet file");
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("parquet reader")
            .build()
            .expect("build parquet reader");
        reader
            .next()
            .expect("one parquet batch")
            .expect("read parquet batch")
    }

    fn schema_types(batch: &RecordBatch) -> Vec<DataType> {
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect()
    }

    fn strings(batch: &RecordBatch, index: usize) -> &StringArray {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 column")
    }

    fn ints(batch: &RecordBatch, index: usize) -> &Int64Array {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 column")
    }

    fn floats(batch: &RecordBatch, index: usize) -> &Float64Array {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float64 column")
    }

    fn bools(batch: &RecordBatch, index: usize) -> &BooleanArray {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean column")
    }
}
