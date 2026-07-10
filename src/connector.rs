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

use crate::schema::{self, SchemaDirective};
use crate::{DestinationDefinition, ExecutionFailure, LoadFailure, SourceDefinition};
use arrow_array::RecordBatch;
use duckdb::vtab::arrow::{arrow_recordbatch_to_query_params, ArrowVTab};
use duckdb::Connection;
use parquet::arrow::ArrowWriter;
use serde_json::Value;
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader};
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// What a [`Source`] hands back: the materialized Arrow batch, the
/// `schema_decision` shape the report echoes, the pinned schema YAML the
/// orchestrator persists when the load produces or extends a pin, and the
/// source bytes the source measured. Recombines [`schema::Materialized`] with
/// the byte count so `schema.rs` stays types-only.
pub(crate) struct SourceRead {
    pub(crate) batch: RecordBatch,
    pub(crate) schema_decision: Value,
    pub(crate) pinned_schema_yaml: Option<String>,
    pub(crate) source_bytes: u64,
}

impl SourceRead {
    /// Recombines a materialized batch with the source bytes the source measured
    /// into one read result, keeping `schema.rs` types-only.
    fn from_materialized(materialized: schema::Materialized, source_bytes: u64) -> Self {
        SourceRead {
            batch: materialized.batch,
            schema_decision: materialized.schema_decision,
            pinned_schema_yaml: materialized.pinned_schema_yaml,
            source_bytes,
        }
    }
}

/// What a [`Destination`] hands back: the bytes it wrote plus the write facts it
/// owns. `atomicity` / `strategy` are reported by the destination itself
/// (ADR-0021) rather than hard-coded by the orchestrator, so a future
/// destination reports its own without an orchestrator branch. `bytes_written`
/// is `None` when the destination has no honest byte count to report: a
/// database table, unlike a file directory, has no measurable on-disk extent of
/// its own (ADR-0030).
pub(crate) struct DestinationWrite {
    pub(crate) bytes_written: Option<u64>,
    pub(crate) atomicity: &'static str,
    pub(crate) strategy: &'static str,
}

/// The rule that decides how a load changes the destination dataset. Parsed once
/// from the raw load-definition string at the write-dispatch boundary; the
/// report still carries the raw string. Only `full_refresh` exists today —
/// append / merge (ADR-0008) widen this enum.
pub(crate) enum LoadMode {
    FullRefresh,
}

impl LoadMode {
    /// Parses the load mode, the single validation point for the raw string.
    pub(crate) fn parse(load_mode: &str) -> Result<Self, LoadFailure> {
        match load_mode {
            "full_refresh" => Ok(LoadMode::FullRefresh),
            other => Err(LoadFailure {
                code: "unsupported_load_mode",
                message: format!("unsupported load mode: {other}"),
            }),
        }
    }
}

/// A named capability for reading records from a source. Reads and materializes
/// under the load's schema directive behind one narrow call so the orchestrator
/// never sees connector internals. Only reads make schema decisions, so only
/// this port's failures can carry one ([`ExecutionFailure`]).
pub(crate) trait Source {
    fn read(&self, directive: &SchemaDirective) -> Result<SourceRead, ExecutionFailure>;
}

/// A named capability for writing records to a destination. Owns how it commits
/// the write and reports its own write facts.
pub(crate) trait Destination {
    fn write(&self, batch: &RecordBatch, mode: LoadMode) -> Result<DestinationWrite, LoadFailure>;
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
                    source_bytes,
                } = read_local_csv(&self.path)?;
                let materialized = schema::from_text_columns(directive, field_names, records)?;
                Ok(SourceRead::from_materialized(materialized, source_bytes))
            }
            "jsonl" => {
                let JsonlRecords {
                    field_names,
                    objects,
                    source_bytes,
                } = read_local_jsonl(&self.path)?;
                let materialized = schema::from_json_columns(directive, field_names, objects)?;
                Ok(SourceRead::from_materialized(materialized, source_bytes))
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
/// write and replaces the destination in one rename, reporting best-effort
/// atomicity (ADR-0021).
struct ParquetDestination {
    path: PathBuf,
}

impl Destination for ParquetDestination {
    fn write(&self, batch: &RecordBatch, mode: LoadMode) -> Result<DestinationWrite, LoadFailure> {
        match mode {
            LoadMode::FullRefresh => self.write_full_refresh(batch),
        }
    }
}

impl ParquetDestination {
    fn write_full_refresh(&self, batch: &RecordBatch) -> Result<DestinationWrite, LoadFailure> {
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
        Ok(DestinationWrite {
            bytes_written: Some(bytes_written),
            atomicity: "best_effort",
            strategy: "staging_then_replace",
        })
    }
}

/// Writes an Arrow batch into a table of a local DuckDB database file named by
/// the destination path, with the table named by the load definition's
/// `dataset`. Full refresh replaces the table in one
/// `CREATE OR REPLACE TABLE "<dataset>" AS SELECT * FROM arrow(?, ?)` statement
/// against the registered Arrow table function, so Arrow-to-DuckDB type mapping
/// is delegated to DuckDB and the single statement auto-commits as an Atomic
/// Commit: the new table becomes visible completely or the old table is left
/// untouched (ADR-0030).
struct DuckDbDestination {
    path: PathBuf,
    dataset: String,
}

impl Destination for DuckDbDestination {
    fn write(&self, batch: &RecordBatch, mode: LoadMode) -> Result<DestinationWrite, LoadFailure> {
        match mode {
            LoadMode::FullRefresh => self.write_full_refresh(batch),
        }
    }
}

impl DuckDbDestination {
    fn write_full_refresh(&self, batch: &RecordBatch) -> Result<DestinationWrite, LoadFailure> {
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

        // The one statement auto-commits, so the replace needs no explicit
        // BEGIN / COMMIT until a multi-statement load mode does (ADR-0030). The
        // table identifier is always double-quote-escaped (`"` doubled) rather
        // than restricted to a character allowlist.
        let statement = format!(
            "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM arrow(?, ?)",
            self.dataset.replace('"', "\"\"")
        );
        connection
            .execute(&statement, arrow_recordbatch_to_query_params(batch.clone()))
            .map_err(|error| LoadFailure {
                code: "destination_write_failed",
                message: format!(
                    "failed to replace DuckDB table {} in {}: {error}",
                    self.dataset,
                    self.path.display()
                ),
            })?;
        connection.close().map_err(|(_, error)| LoadFailure {
            code: "destination_write_failed",
            message: format!(
                "failed to close DuckDB database {}: {error}",
                self.path.display()
            ),
        })?;

        Ok(DestinationWrite {
            bytes_written: None,
            atomicity: "atomic",
            strategy: "transactional_replace",
        })
    }
}

/// CSV cells arrive untyped as text; `schema::from_text_columns` infers their
/// types. `source_bytes` is measured separately from the values.
struct CsvRecords {
    field_names: Vec<String>,
    records: Vec<Vec<Option<String>>>,
    source_bytes: u64,
}

/// JSONL cells arrive as parsed [`Value`]s carrying their own type;
/// `schema::from_json_columns` reads types from them directly.
struct JsonlRecords {
    field_names: Vec<String>,
    objects: Vec<serde_json::Map<String, Value>>,
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
            .read(&SchemaDirective::Inferred)
            .err()
            .expect("unknown format rejected");
        assert_eq!(error.failure.code, "unsupported_source_format");
    }

    #[test]
    fn load_mode_parse_accepts_full_refresh_and_rejects_unknown() {
        assert!(matches!(
            LoadMode::parse("full_refresh"),
            Ok(LoadMode::FullRefresh)
        ));

        let error = LoadMode::parse("append")
            .err()
            .expect("unknown load mode rejected");
        assert_eq!(error.code, "unsupported_load_mode");
        assert_eq!(error.message, "unsupported load mode: append");
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
        let read = source.read(&SchemaDirective::Inferred).expect("read csv");

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
        let read = source.read(&SchemaDirective::Inferred).expect("read jsonl");

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
    fn parquet_destination_writes_a_readable_batch_and_reports_write_facts() {
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.csv");
        fs::write(&source_path, "id,name\n1,Ada\n2,Grace\n").expect("write csv");
        let batch = LocalFileSource {
            path: source_path,
            format: Some("csv".to_string()),
        }
        .read(&SchemaDirective::Inferred)
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
        assert_eq!(written.atomicity, "best_effort");
        assert_eq!(written.strategy, "staging_then_replace");

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
        .read(&SchemaDirective::Inferred)
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
        assert_eq!(written.atomicity, "atomic");
        assert_eq!(written.strategy, "transactional_replace");

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
            .read(&SchemaDirective::Inferred)
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
