//! Source and Destination connector ports: the single home for the "where data
//! comes from and where it goes" story of a load.
//!
//! Every connector in the ADR-bound matrix (local files, S3, PostgreSQL, ... as
//! sources; Parquet, DuckDB, BigQuery, ... as destinations) plugs in behind one
//! of two trait-object ports — [`Source`] and [`Destination`] — reached only
//! through the pure [`source_connector`] / [`destination_connector`] factories.
//! A source resolves the whole-input facts of a load — schema decision,
//! rejection outcome, source bytes — and hands back a lazy iterator of
//! bounded [`RecordBatch`] chunks ([`SourceRead`], ADR-0046); `local_file`
//! resolves by streaming the source twice (ADR-0045), guarded against the
//! file changing between the passes (`source_changed_during_load`). A
//! destination opens a mode-scoped write session ([`Destination::begin`])
//! whose writer takes one chunk at a time and commits per ADR-0047 — full
//! refresh at one terminal commit, append per chunk — reporting its own
//! write facts ([`DestinationWrite`]) so the orchestrator never branches on
//! connector identity. The load mode is parsed once into [`LoadMode`] at the
//! session boundary. `local_file`, `parquet`, and `duckdb` are the first
//! connectors, and `sqlserver` enters as Definition-phase surface — offline
//! block validation with every load mode still declined (ADR-0060);
//! everything else here is private.

use crate::rejection::{RejectedRecord, RejectionSink};
use crate::rejection::{MALFORMED_CSV_RECORD, MALFORMED_JSONL_RECORD};
use crate::schema::{self, SchemaDirective};
use crate::{
    DestinationDefinition, ExecutionFailure, LoadFailure, SourceDefinition,
    SqlServerDestinationDefinition,
};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use duckdb::vtab::arrow::{arrow_recordbatch_to_query_params, ArrowVTab};
use duckdb::Connection;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use serde_json::Value;
use std::cell::Cell;
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Mutex;
use uuid::Uuid;

/// What a [`Source`] hands back once pass 1 resolved the load (ADR-0045,
/// ADR-0046): the `schema_decision` shape the report echoes, the pinned
/// schema file write the orchestrator performs when the load produces or
/// extends a pin, the source bytes and full-input record counts the source
/// measured, and the lazy chunk iterator pass 2 materializes through — no
/// records are held. Rejections were already streamed to the caller's
/// [`RejectionSink`].
pub(crate) struct SourceRead {
    pub(crate) schema_decision: Value,
    pub(crate) pinned_schema_write: Option<schema::PinnedSchemaWrite>,
    pub(crate) source_bytes: u64,
    pub(crate) source_rows: u64,
    pub(crate) rejected_count: u64,
    pub(crate) chunks: SourceChunks,
}

/// The lazy pass-2 chunk stream of a resolved load: each item is one
/// materialized chunk of at most `chunk_rows` surviving records, in source
/// order, against the fixed pass-1 plan. A load whose surviving record run
/// is empty still yields one empty chunk, so an empty dataset materializes
/// its schema. The iterator re-opens and re-reads the source on first pull;
/// divergence from pass 1 — semantic or byte-wise — surfaces as a
/// `source_changed_during_load` item (ADR-0045).
pub(crate) type SourceChunks = Box<dyn Iterator<Item = Result<RecordBatch, LoadFailure>>>;

/// The destination-owned write facts serialized into the Load Report
/// (ADR-0021). Keeping it typed until the report boundary prevents success and
/// failure paths from constructing subtly different JSON shapes.
#[derive(Clone, Copy, Debug)]
pub(crate) struct DestinationWriteFacts {
    atomicity: &'static str,
    strategy: Option<&'static str>,
    /// A committed merge session's record partition (ADR-0059); `None`
    /// everywhere else, including every failure path — a terminal-commit
    /// failure committed nothing to count.
    merge: Option<MergeWriteCounts>,
}

/// How a committed merge partitioned its surviving records (ADR-0059):
/// `updated` counts the records whose key tuple matched an existing
/// destination record — replaced whole — and `inserted` the unmatched
/// remainder, so `updated + inserted` always equals the written record
/// count.
#[derive(Clone, Copy, Debug)]
struct MergeWriteCounts {
    updated: u64,
    inserted: u64,
}

impl DestinationWriteFacts {
    pub(crate) fn not_applicable() -> Self {
        DestinationWriteFacts {
            atomicity: "not_applicable",
            strategy: None,
            merge: None,
        }
    }

    pub(crate) fn atomic(strategy: &'static str) -> Self {
        DestinationWriteFacts {
            atomicity: "atomic",
            strategy: Some(strategy),
            merge: None,
        }
    }

    pub(crate) fn best_effort(strategy: &'static str) -> Self {
        DestinationWriteFacts {
            atomicity: "best_effort",
            strategy: Some(strategy),
            merge: None,
        }
    }

    fn with_merge_counts(self, updated: u64, inserted: u64) -> Self {
        DestinationWriteFacts {
            merge: Some(MergeWriteCounts { updated, inserted }),
            ..self
        }
    }

    pub(crate) fn report_value(self) -> Value {
        let mut facts = serde_json::Map::new();
        facts.insert("atomicity".to_string(), Value::from(self.atomicity));
        if let Some(strategy) = self.strategy {
            facts.insert("strategy".to_string(), Value::from(strategy));
        }
        if let Some(merge) = self.merge {
            facts.insert(
                "merge".to_string(),
                serde_json::json!({
                    "updated": merge.updated,
                    "inserted": merge.inserted
                }),
            );
        }
        Value::Object(facts)
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

/// The originator-owned retry classification of a destination write failure
/// (ADR-0048): only the connector that raised a failure may classify it, and
/// a failure may be transient only when the failed attempt provably committed
/// nothing to the destination and the session can accept a re-attempt of
/// the same retry unit — any uncertainty keeps it terminal. Every
/// construction path defaults to terminal; marking a failure transient
/// takes an explicit `Transience::Transient` at the raising site.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Transience {
    /// Safe for the retry engine to re-attempt with the same input.
    // No shipped connector classifies any failure transient (ADR-0048's
    // zero-local-transient invariant): the variant is constructed only by
    // the scripted test connectors, so the retry engine is provably idle in
    // the current matrix. Drop the attribute with the first connector that
    // marks a failure transient.
    #[cfg_attr(not(test), allow(dead_code))]
    Transient,
    /// Never engine-retried: terminal by classification or by uncertainty.
    Terminal,
}

/// A destination failure plus the write facts the report can state honestly.
/// Failures before any destination-visible change use `not_applicable`; a
/// connector that crosses a best-effort write boundary attaches its strategy
/// so operators know the failed load may have changed the destination
/// (ADR-0021). `committed_chunks` and `written_records` count what the
/// destination had already committed when the failure happened — `0` for a
/// full refresh before its terminal commit, the chunk prefix for append
/// (ADR-0047). `transience` is the originator's retry classification
/// (ADR-0048), terminal on every existing path.
#[derive(Debug)]
pub(crate) struct DestinationWriteFailure {
    pub(crate) failure: LoadFailure,
    pub(crate) facts: DestinationWriteFacts,
    pub(crate) written_records: u64,
    pub(crate) committed_chunks: u64,
    pub(crate) transience: Transience,
}

impl DestinationWriteFailure {
    fn atomic(failure: LoadFailure, strategy: &'static str, written_records: u64) -> Self {
        DestinationWriteFailure {
            failure,
            facts: DestinationWriteFacts::atomic(strategy),
            written_records,
            committed_chunks: 0,
            transience: Transience::Terminal,
        }
    }

    fn best_effort(failure: LoadFailure, strategy: &'static str, written_records: u64) -> Self {
        DestinationWriteFailure {
            failure,
            facts: DestinationWriteFacts::best_effort(strategy),
            written_records,
            committed_chunks: 0,
            transience: Transience::Terminal,
        }
    }
}

impl From<LoadFailure> for DestinationWriteFailure {
    fn from(failure: LoadFailure) -> Self {
        DestinationWriteFailure {
            failure,
            facts: DestinationWriteFacts::not_applicable(),
            written_records: 0,
            committed_chunks: 0,
            transience: Transience::Terminal,
        }
    }
}

/// The rule that decides how a load changes the destination dataset. Parsed once
/// from the raw load-definition string at the write-dispatch boundary; the
/// report still carries the raw string. Full refresh, append, and merge — the
/// complete v1 mode set of ADR-0008 — exist today; merge lands on DuckDB
/// only, and every other destination declines it through its declared mode
/// support (ADR-0057).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoadMode {
    FullRefresh,
    Append,
    Merge,
}

impl LoadMode {
    /// Parses the load mode, the single validation point for the raw string:
    /// `unsupported_load_mode` means exactly "this mode string is unknown" —
    /// a known mode a destination declines fails as
    /// `unsupported_load_mode_for_destination` instead (ADR-0057).
    pub(crate) fn parse(load_mode: &str) -> Result<Self, LoadFailure> {
        match load_mode {
            "full_refresh" => Ok(LoadMode::FullRefresh),
            "append" => Ok(LoadMode::Append),
            "merge" => Ok(LoadMode::Merge),
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
            LoadMode::Merge => "merge",
        }
    }
}

const FULL_REFRESH_AND_APPEND_LOAD_MODES: &[LoadMode] = &[LoadMode::FullRefresh, LoadMode::Append];

const ALL_LOAD_MODES: &[LoadMode] = &[LoadMode::FullRefresh, LoadMode::Append, LoadMode::Merge];

/// A named capability for reading records from a source. Resolves the load
/// under its schema directive — completing every whole-input decision before
/// any chunk exists — and returns the resolved facts plus the lazy chunk
/// iterator (ADR-0046) behind one narrow call, so the orchestrator never
/// sees connector internals; how a source resolves (two passes for
/// `local_file`, ADR-0045) is implementation strategy, not port contract.
/// Rejections stream to the caller's sink as they are found. Only reads make
/// schema decisions, so only this port's failures can carry one
/// ([`ExecutionFailure`]).
pub(crate) trait Source {
    fn read(
        &self,
        directive: &SchemaDirective,
        merge_keys: &schema::MergeKeys,
        chunk_rows: usize,
        sink: &mut RejectionSink,
    ) -> Result<SourceRead, ExecutionFailure>;
}

/// A named capability for writing records to a destination. Declares the
/// load modes it supports, opens one write session per load
/// ([`Destination::begin`]), owns how that session commits (ADR-0047), and
/// reports its own write facts.
pub(crate) trait Destination {
    /// The connector name the definition selects this destination by, so a
    /// declined load mode can name the destination that declined it.
    fn connector_name(&self) -> &'static str;

    fn supported_load_modes(&self) -> &'static [LoadMode];

    /// The Connector Parallelism Limit of one load mode (ADR-0051): the
    /// hard cap on how many `write_chunk` operations the dispatcher may
    /// run concurrently within a load in that mode, and the effective
    /// parallelism when the definition configures none. Declaring a limit
    /// above 1 carries a declaration obligation mirroring the transience
    /// rule of ADR-0048: only when concurrent `write_chunk` completions
    /// cannot break the mode's documented commit semantics. The default is
    /// the always-safe serial 1, and no shipped connector overrides it —
    /// the shipped matrix is provably serial.
    fn parallelism_limit(&self, _mode: LoadMode) -> NonZeroU64 {
        NonZeroU64::MIN
    }

    /// Declines a known load mode this destination does not support, before
    /// any I/O. Distinct from `unsupported_load_mode`, which names an
    /// unknown mode string: a declined mode names the destination and what
    /// it does support, so narrowing the matrix stays a per-destination
    /// declaration (ADR-0057).
    fn validate_mode(&self, mode: LoadMode) -> Result<(), LoadFailure> {
        if self.supported_load_modes().contains(&mode) {
            return Ok(());
        }
        let supported = self
            .supported_load_modes()
            .iter()
            .map(|supported| supported.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        // A destination whose whole mode list is still empty deserves a
        // readable spelling, not an empty parenthetical.
        let supported_clause = if supported.is_empty() {
            "no load modes are supported for this destination yet".to_string()
        } else {
            format!("supported load modes: {supported}")
        };
        Err(LoadFailure {
            code: "unsupported_load_mode_for_destination",
            message: format!(
                "{} destination does not support load mode: {} ({supported_clause})",
                self.connector_name(),
                mode.as_str()
            ),
        })
    }

    /// Opens the write session for one load in the given mode. The session
    /// does whatever destination-side preparation the mode needs — staging
    /// directories, connections, destination-schema inspection — so a
    /// preparation failure surfaces before any chunk is written.
    fn begin(&self, mode: LoadMode) -> Result<Box<dyn DestinationWriter>, DestinationWriteFailure>;
}

/// One destination write session (ADR-0046): chunks are written through
/// [`DestinationWriter::write_chunk`], and the session ends in exactly one
/// of [`DestinationWriter::commit`] — returning the write facts — or
/// [`DestinationWriter::abandon`], when the load fails mid-stream. The
/// commit boundary is mode-owned (ADR-0047): a full-refresh session makes
/// nothing visible before `commit`, while an append session commits every
/// chunk inside `write_chunk`. `write_chunk` takes `&self` and the trait is
/// `Sync` because the dispatcher may run up to the mode's declared
/// parallelism limit of writes concurrently from its worker threads
/// (ADR-0051); each writer owns whatever interior synchronization its
/// declared limit needs — a plain lock suffices at limit 1. `commit` and
/// `abandon` consume the writer on the dispatching thread after every write
/// returned. Dropping an uncommitted writer cleans up its staging state.
pub(crate) trait DestinationWriter: Sync {
    fn write_chunk(&self, batch: &RecordBatch) -> Result<(), DestinationWriteFailure>;

    fn commit(self: Box<Self>) -> Result<DestinationWrite, DestinationWriteFailure>;

    /// Ends the session without committing further work and reports what the
    /// destination had already committed, so a source-side failure
    /// mid-stream still yields honest destination facts.
    fn abandon(self: Box<Self>) -> AbandonedWrite;
}

/// The destination-owned state of an abandoned write session: the chunks and
/// records already committed — the append prefix that stays visible, or
/// nothing for an uncommitted full refresh — and the write facts that state
/// them honestly (`not_applicable` when the destination was never changed).
pub(crate) struct AbandonedWrite {
    pub(crate) committed_chunks: u64,
    pub(crate) written_records: u64,
    pub(crate) facts: DestinationWriteFacts,
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

/// Resolves a destination definition to its connector. Validates the
/// connector name plus everything the connector can check offline — for
/// `duckdb`, a present `dataset` naming the destination table (ADR-0030);
/// for `sqlserver`, the whole block, the dot-free `dataset`, and the
/// credential reference resolving in the process environment (ADR-0060) —
/// doing no source or destination I/O, so an unsupported or incomplete
/// definition fails before any destination write (ADR-0019). `merge_keys`
/// are the definition's already-validated merge keys — empty for every
/// non-merge load — which a merge session upserts on (ADR-0059);
/// destinations that decline merge ignore them.
pub(crate) fn destination_connector(
    definition: &DestinationDefinition,
    dataset: Option<&str>,
    merge_keys: &[String],
) -> Result<Box<dyn Destination>, LoadFailure> {
    match definition {
        DestinationDefinition::File(definition) => match definition.connector.as_str() {
            "parquet" => Ok(Box::new(ParquetDestination {
                path: definition.path.clone(),
            })),
            "duckdb" => {
                // An empty dataset is no address at all, so it fails here as
                // missing rather than at write time; identifier content beyond
                // presence is deliberately left to DuckDB (no allowlist,
                // ADR-0030).
                let dataset = dataset
                    .filter(|dataset| !dataset.is_empty())
                    .ok_or_else(|| LoadFailure {
                        code: "missing_dataset",
                        message: "load definition dataset is required for a duckdb destination"
                            .to_string(),
                    })?;
                Ok(Box::new(DuckDbDestination {
                    table: DuckDbTable {
                        path: definition.path.clone(),
                        dataset: dataset.to_string(),
                    },
                    merge_keys: merge_keys.to_vec(),
                }))
            }
            other => Err(LoadFailure {
                code: "unsupported_destination_connector",
                message: format!("unsupported destination connector: {other}"),
            }),
        },
        DestinationDefinition::SqlServer(definition) => {
            let config = SqlServerConfig::from_definition(definition, dataset)?;
            resolve_credential_reference(&config.password_env)?;
            Ok(Box::new(SqlServerDestination { config }))
        }
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
    fn read(
        &self,
        directive: &SchemaDirective,
        merge_keys: &schema::MergeKeys,
        chunk_rows: usize,
        sink: &mut RejectionSink,
    ) -> Result<SourceRead, ExecutionFailure> {
        // Validate the resolved format before opening the file so an unsupported
        // format fails without source I/O (ADR-0019), and so its precedence
        // stays after the destination-connector check for a doubly-invalid
        // definition (the check lives here, not in the factory).
        match self.resolved_format().as_str() {
            "csv" => self.read_csv(directive, merge_keys, chunk_rows, sink),
            "jsonl" => self.read_jsonl(directive, merge_keys, chunk_rows, sink),
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
        resolved_source_format(&self.path, self.format.as_deref())
    }

    /// Pass 1 over a CSV source (ADR-0045): streams every record once through
    /// the schema observer, holding only bounded state — the header, the
    /// observed-type lattices, and the streaming rejection outputs — and
    /// resolves the whole-input schema decision at end of input. Parse and
    /// per-record validation rejections stream to the sink in source-line
    /// order as they are found; a shape-verdict failure streams no
    /// validation rejections at all, because the header already decides the
    /// verdict before any record is judged.
    fn read_csv(
        &self,
        directive: &SchemaDirective,
        merge_keys: &schema::MergeKeys,
        chunk_rows: usize,
        sink: &mut RejectionSink,
    ) -> Result<SourceRead, ExecutionFailure> {
        let (file, source_bytes) = open_source_file(&self.path, "CSV")?;
        let hash = HashState::new();
        let mut reader = csv_source_reader(HashingReader::new(file, hash.clone()));
        let field_names = read_csv_header(&mut reader, &self.path)?;

        let mut observer = schema::TextObserver::new(directive, merge_keys, &field_names);
        let mut parsed_records = 0_u64;
        let mut parse_rejected = 0_u64;
        for record in reader.records() {
            match csv_item(record, field_names.len()) {
                CsvItem::Rejected(rejection) => {
                    parse_rejected += 1;
                    sink.record(&rejection);
                }
                CsvItem::Record(text_record) => {
                    parsed_records += 1;
                    if let Some(rejection) = observer.observe(&text_record) {
                        sink.record(&rejection);
                    }
                }
            }
        }
        let source_rows = parsed_records + parse_rejected;

        let resolution = observer
            .finish()
            .map_err(|failure| failure.with_rejected_count(sink.count()))?;
        let schema::Resolution {
            plan,
            checks,
            decision,
            pinned_schema_write,
        } = resolution;
        let chunks = CsvChunks {
            path: self.path.clone(),
            plan,
            checks,
            field_names,
            chunk_rows,
            expectations: PassOneExpectations {
                rejected_lines: sink.rejected_lines().iter().copied().collect(),
                records: source_rows,
                hash: hash.value(),
            },
            reading: None,
            phase: ChunkPhase::NotStarted,
        };
        Ok(SourceRead {
            schema_decision: decision,
            pinned_schema_write,
            source_bytes,
            source_rows,
            rejected_count: sink.count(),
            chunks: Box::new(chunks),
        })
    }

    /// Pass 1 over a JSONL source (ADR-0045): streams every line once,
    /// growing the key union and the observed-type lattices, and judges each
    /// record against the directive-derived checks. Parse rejections stream
    /// to the sink as found; validation rejections cannot be confirmed until
    /// the end-of-input shape verdict, so they buffer through the
    /// line-ordered spill and merge in — or are discarded — with the
    /// verdict.
    fn read_jsonl(
        &self,
        directive: &SchemaDirective,
        merge_keys: &schema::MergeKeys,
        chunk_rows: usize,
        sink: &mut RejectionSink,
    ) -> Result<SourceRead, ExecutionFailure> {
        let (file, source_bytes) = open_source_file(&self.path, "JSONL")?;
        let hash = HashState::new();
        let mut reader = BufReader::new(HashingReader::new(file, hash.clone()));

        let mut observer = schema::JsonObserver::new(directive, merge_keys);
        let mut spill = sink.spill();
        let mut field_names: Vec<String> = Vec::new();
        let mut seen_fields: HashSet<String> = HashSet::new();
        let mut parsed_records = 0_u64;
        let mut parse_rejected = 0_u64;
        let mut line_bytes = Vec::new();
        let mut line_number = 0_u64;
        loop {
            line_bytes.clear();
            match reader.read_until(b'\n', &mut line_bytes) {
                Ok(0) => break,
                Ok(_) => line_number += 1,
                Err(error) => {
                    spill.discard(sink);
                    return Err(ExecutionFailure {
                        failure: LoadFailure {
                            code: "source_read_failed",
                            message: format!(
                                "failed to read JSONL source {} after line {line_number}: {error}",
                                self.path.display(),
                            ),
                        },
                        schema_decision: None,
                        source_rows: Some(parsed_records + parse_rejected),
                        written_records: 0,
                        rejected_count: sink.count(),
                        committed_execution: None,
                        retry: None,
                        destination_write: Box::new(DestinationWriteFacts::not_applicable()),
                    });
                }
            }
            match jsonl_line_record(&line_bytes, line_number) {
                JsonlLine::Blank => {}
                JsonlLine::Rejected(rejection) => {
                    parse_rejected += 1;
                    sink.record(&rejection);
                }
                JsonlLine::Record(record) => {
                    parsed_records += 1;
                    for key in record.object.keys() {
                        if seen_fields.insert(key.clone()) {
                            field_names.push(key.clone());
                        }
                    }
                    match observer.observe(&record) {
                        schema::JsonOutcome::Survived => {}
                        schema::JsonOutcome::Rejected => spill.record(record.line, &record.object),
                    }
                }
            }
        }
        let source_rows = parsed_records + parse_rejected;

        // No record ever parsed with fields, so no schema can be inferred or
        // validated: the load fails, with the parse rejections already
        // streamed so their artifact is still written. This is asymmetric
        // with CSV on purpose — a CSV header declares the source's fields
        // even when every record is rejected, while JSONL fields exist only
        // in records.
        if field_names.is_empty() {
            spill.discard(sink);
            return Err(ExecutionFailure {
                failure: LoadFailure {
                    code: "malformed_jsonl",
                    message: format!(
                        "JSONL source {} must include at least one record with fields",
                        self.path.display()
                    ),
                },
                schema_decision: None,
                source_rows: Some(source_rows),
                written_records: 0,
                rejected_count: sink.count(),
                committed_execution: None,
                retry: None,
                destination_write: Box::new(DestinationWriteFacts::not_applicable()),
            });
        }

        let resolution = match observer.finish(&field_names) {
            Ok(resolution) => resolution,
            Err(failure) => {
                // The verdict failed before validation could count: the
                // spilled validation rejections are discarded, so the
                // artifact carries only parse rejections.
                spill.discard(sink);
                return Err(failure.with_rejected_count(sink.count()));
            }
        };
        sink.merge_spill(spill, |line, object| {
            resolution.rejection_for(&schema::JsonRecord { line, object })
        });

        let schema::Resolution {
            plan,
            checks,
            decision,
            pinned_schema_write,
        } = resolution;
        let chunks = JsonlChunks {
            path: self.path.clone(),
            plan,
            checks,
            resolved_shape: seen_fields,
            chunk_rows,
            expectations: PassOneExpectations {
                rejected_lines: sink.rejected_lines().iter().copied().collect(),
                records: source_rows,
                hash: hash.value(),
            },
            reading: None,
            phase: ChunkPhase::NotStarted,
        };
        Ok(SourceRead {
            schema_decision: decision,
            pinned_schema_write,
            source_bytes,
            source_rows,
            rejected_count: sink.count(),
            chunks: Box::new(chunks),
        })
    }
}

/// Resolves a definition's source format — the explicit `format` or, failing
/// that, the path extension. Pure: no I/O, so config-time checks that hinge
/// on the format (`transform.flatten` requires a JSONL source, ADR-0041) run
/// before any file is touched, while format *validation* stays inside
/// [`Source::read`] to preserve error precedence.
pub(crate) fn resolved_source_format(path: &Path, format: Option<&str>) -> String {
    format.map(str::to_string).unwrap_or_else(|| {
        path.extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or("")
            .to_string()
    })
}

/// Writes Arrow chunks to a local Parquet directory. A full-refresh session
/// stages every chunk as one part file and replaces the destination in a
/// single terminal rename (ADR-0047); an append session stages and renames
/// one complete part per chunk, committing per chunk. Both report
/// best-effort atomicity (ADR-0021).
struct ParquetDestination {
    path: PathBuf,
}

impl Destination for ParquetDestination {
    fn connector_name(&self) -> &'static str {
        "parquet"
    }

    fn supported_load_modes(&self) -> &'static [LoadMode] {
        FULL_REFRESH_AND_APPEND_LOAD_MODES
    }

    fn begin(&self, mode: LoadMode) -> Result<Box<dyn DestinationWriter>, DestinationWriteFailure> {
        match mode {
            LoadMode::FullRefresh => Ok(Box::new(ParquetFullRefreshWriter::begin(
                self.path.clone(),
            )?)),
            LoadMode::Append => Ok(Box::new(ParquetAppendWriter::begin(self.path.clone())?)),
            LoadMode::Merge => {
                // Parquet declines merge (ADR-0057); the orchestrator's
                // validate_mode call fails the load before begin, and a
                // directly-driven session reports the same declination.
                self.validate_mode(mode)?;
                unreachable!("parquet validate_mode declines merge");
            }
        }
    }
}

/// The Parquet full-refresh session: every chunk lands as one part file in a
/// unique staging directory, and the single terminal commit replaces the
/// destination with one rename — nothing is destination-visible before it
/// (ADR-0047). Dropped uncommitted, the writer removes its staging
/// directory.
struct ParquetFullRefreshWriter {
    destination_path: PathBuf,
    staging_path: PathBuf,
    // Interior lock because `write_chunk` takes `&self` (the session
    // contract is `Sync`); at this connector's declared parallelism limit
    // of 1 the lock is never contended.
    progress: Mutex<ParquetFullRefreshProgress>,
    committed: bool,
}

struct ParquetFullRefreshProgress {
    parts: u64,
    bytes_written: u64,
}

impl ParquetFullRefreshWriter {
    fn begin(destination_path: PathBuf) -> Result<Self, DestinationWriteFailure> {
        let parent = destination_path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(|error| LoadFailure {
            code: "destination_write_failed",
            message: format!(
                "failed to prepare destination parent {}: {error}",
                parent.display()
            ),
        })?;

        // A per-load token keeps the staging directory unique so concurrent
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

        Ok(ParquetFullRefreshWriter {
            destination_path,
            staging_path,
            progress: Mutex::new(ParquetFullRefreshProgress {
                parts: 0,
                bytes_written: 0,
            }),
            committed: false,
        })
    }
}

impl DestinationWriter for ParquetFullRefreshWriter {
    fn write_chunk(&self, batch: &RecordBatch) -> Result<(), DestinationWriteFailure> {
        let mut progress = self.progress.lock().expect("writer progress lock");
        let parquet_file_path = self
            .staging_path
            .join(format!("part-{:05}.parquet", progress.parts));
        let bytes_written = write_parquet_batch(&parquet_file_path, batch)?;
        progress.parts += 1;
        progress.bytes_written += bytes_written;
        Ok(())
    }

    fn commit(mut self: Box<Self>) -> Result<DestinationWrite, DestinationWriteFailure> {
        remove_path_if_exists(&self.destination_path).map_err(|error| {
            DestinationWriteFailure::best_effort(
                LoadFailure {
                    code: "destination_write_failed",
                    message: format!(
                        "failed to replace existing destination {}: {error}",
                        self.destination_path.display()
                    ),
                },
                "staging_then_replace",
                0,
            )
        })?;
        fs::rename(&self.staging_path, &self.destination_path).map_err(|error| {
            DestinationWriteFailure::best_effort(
                LoadFailure {
                    code: "destination_write_failed",
                    message: format!(
                        "failed to commit Parquet destination {}: {error}",
                        self.destination_path.display()
                    ),
                },
                "staging_then_replace",
                0,
            )
        })?;
        self.committed = true;

        let bytes_written = self
            .progress
            .lock()
            .expect("writer progress lock")
            .bytes_written;
        Ok(DestinationWrite {
            bytes_written: Some(bytes_written),
            facts: DestinationWriteFacts::best_effort("staging_then_replace"),
        })
    }

    fn abandon(self: Box<Self>) -> AbandonedWrite {
        // Nothing was destination-visible before the terminal commit; Drop
        // removes the staging directory.
        AbandonedWrite {
            committed_chunks: 0,
            written_records: 0,
            facts: DestinationWriteFacts::not_applicable(),
        }
    }
}

impl Drop for ParquetFullRefreshWriter {
    fn drop(&mut self) {
        if !self.committed {
            let _ = remove_path_if_exists(&self.staging_path);
        }
    }
}

/// The Parquet append session: each chunk is staged under a non-data
/// extension and renamed into the dataset as one complete part — the
/// per-chunk commit of ADR-0047, so readers never observe a partially
/// written part while a failure keeps exactly the committed chunk prefix.
/// Chunks align against the destination schema read once at session start:
/// all chunks of a load share one plan, so the pre-existing dataset is the
/// only schema that can disagree.
struct ParquetAppendWriter {
    destination_path: PathBuf,
    destination_schema: Option<SchemaRef>,
    // Interior lock because `write_chunk` takes `&self` (the session
    // contract is `Sync`); at this connector's declared parallelism limit
    // of 1 the lock is never contended.
    progress: Mutex<ParquetAppendProgress>,
}

struct ParquetAppendProgress {
    committed_chunks: u64,
    written_records: u64,
    bytes_written: u64,
}

impl ParquetAppendProgress {
    /// Attaches the committed-prefix facts to a chunk failure: once a chunk
    /// has committed, the destination has changed, so even a pre-boundary
    /// failure must report the append strategy rather than `not_applicable`.
    fn chunk_failure(&self, failure: LoadFailure) -> DestinationWriteFailure {
        if self.committed_chunks > 0 {
            DestinationWriteFailure {
                failure,
                facts: DestinationWriteFacts::best_effort("staged_part_append"),
                written_records: self.written_records,
                committed_chunks: self.committed_chunks,
                transience: Transience::Terminal,
            }
        } else {
            failure.into()
        }
    }
}

impl ParquetAppendWriter {
    fn begin(destination_path: PathBuf) -> Result<Self, DestinationWriteFailure> {
        fs::create_dir_all(&destination_path).map_err(|error| LoadFailure {
            code: "destination_write_failed",
            message: format!(
                "failed to prepare Parquet destination {}: {error}",
                destination_path.display()
            ),
        })?;
        let destination_schema = existing_parquet_schema(&destination_path)?;
        Ok(ParquetAppendWriter {
            destination_path,
            destination_schema,
            progress: Mutex::new(ParquetAppendProgress {
                committed_chunks: 0,
                written_records: 0,
                bytes_written: 0,
            }),
        })
    }
}

impl DestinationWriter for ParquetAppendWriter {
    fn write_chunk(&self, batch: &RecordBatch) -> Result<(), DestinationWriteFailure> {
        let mut progress = self.progress.lock().expect("writer progress lock");
        let append_batch = match &self.destination_schema {
            None => batch.clone(),
            Some(destination_schema) => align_batch_to_destination_schema(
                &self.destination_path,
                batch,
                destination_schema.clone(),
            )
            .map_err(|failure| progress.chunk_failure(failure))?,
        };

        let part_token = Uuid::new_v4();
        let staging_path = self
            .destination_path
            .join(format!(".part-{part_token}.data-spark-staging.tmp"));
        let parquet_file_path = self
            .destination_path
            .join(format!("part-{part_token}.parquet"));
        let bytes_written = match write_parquet_batch(&staging_path, &append_batch) {
            Ok(bytes_written) => bytes_written,
            Err(failure) => {
                let _ = remove_path_if_exists(&staging_path);
                return Err(progress.chunk_failure(failure));
            }
        };
        if let Err(error) = fs::rename(&staging_path, &parquet_file_path) {
            let _ = remove_path_if_exists(&staging_path);
            return Err(DestinationWriteFailure {
                failure: LoadFailure {
                    code: "destination_write_failed",
                    message: format!(
                        "failed to commit Parquet part {}: {error}",
                        parquet_file_path.display()
                    ),
                },
                facts: DestinationWriteFacts::best_effort("staged_part_append"),
                written_records: progress.written_records,
                committed_chunks: progress.committed_chunks,
                transience: Transience::Terminal,
            });
        }
        progress.committed_chunks += 1;
        progress.written_records += append_batch.num_rows() as u64;
        progress.bytes_written += bytes_written;
        Ok(())
    }

    fn commit(self: Box<Self>) -> Result<DestinationWrite, DestinationWriteFailure> {
        // Every chunk already committed inside write_chunk; the terminal
        // commit only reports the session's facts.
        let bytes_written = self
            .progress
            .lock()
            .expect("writer progress lock")
            .bytes_written;
        Ok(DestinationWrite {
            bytes_written: Some(bytes_written),
            facts: DestinationWriteFacts::best_effort("staged_part_append"),
        })
    }

    fn abandon(self: Box<Self>) -> AbandonedWrite {
        let progress = self.progress.lock().expect("writer progress lock");
        append_abandoned(
            "staged_part_append",
            progress.committed_chunks,
            progress.written_records,
        )
    }
}

/// The abandonment state shared by the per-chunk-commit (append) sessions:
/// once any chunk committed the destination changed, so the facts state the
/// mode's strategy; a session that committed nothing stays `not_applicable`.
fn append_abandoned(
    strategy: &'static str,
    committed_chunks: u64,
    written_records: u64,
) -> AbandonedWrite {
    AbandonedWrite {
        facts: if committed_chunks > 0 {
            DestinationWriteFacts::best_effort(strategy)
        } else {
            DestinationWriteFacts::not_applicable()
        },
        committed_chunks,
        written_records,
    }
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

/// Writes Arrow chunks into a table of a local DuckDB database file named by
/// the destination path, with the table named by the load definition's
/// `dataset`. A full-refresh session wraps the replace in one explicit
/// transaction — `BEGIN`, `CREATE OR REPLACE TABLE "<dataset>" AS SELECT *
/// FROM arrow(?, ?)` for the first chunk, one `INSERT` per remaining chunk,
/// `COMMIT` — the multi-statement moment ADR-0030 anticipated, so the whole
/// load stays one Atomic Commit (ADR-0047). An append session reads the
/// existing table's Arrow schema once, aligns each chunk by name and exact
/// type, and runs one auto-committed `INSERT ... BY NAME` per chunk.
/// Arrow-to-DuckDB type mapping stays delegated to DuckDB (ADR-0030).
struct DuckDbDestination {
    table: DuckDbTable,
    // The definition's validated merge keys — empty for non-merge loads,
    // non-empty whenever a merge session begins (the definition boundary
    // rejects a merge load without keys).
    merge_keys: Vec<String>,
}

/// The table a DuckDB session addresses: the database file plus the dataset
/// name, with the identifier always double-quote-escaped (`"` doubled)
/// rather than restricted to a character allowlist (ADR-0030).
#[derive(Clone)]
struct DuckDbTable {
    path: PathBuf,
    dataset: String,
}

/// Double-quote-escapes one identifier (`"` doubled) for a DuckDB
/// statement, the ADR-0030 escaping rule shared by every identifier a
/// session spells — dataset names, stage names, and column names alike.
fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

impl DuckDbTable {
    fn quoted_dataset(&self) -> String {
        quote_identifier(&self.dataset)
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

    /// Probes the destination table's Arrow schema with a `LIMIT 0` select —
    /// the shared session-start step of the modes that write into an
    /// existing table (append and merge): the table must exist, and the
    /// probed schema is what every chunk aligns against. `operation` names
    /// the mode in the failure message ("before append" / "before merge").
    fn inspect_schema(
        &self,
        connection: &Connection,
        operation: &'static str,
    ) -> Result<SchemaRef, LoadFailure> {
        let statement = format!("SELECT * FROM {} LIMIT 0", self.quoted_dataset());
        let inspect_failure = |error: duckdb::Error| LoadFailure {
            code: "destination_write_failed",
            message: format!(
                "failed to inspect DuckDB table {} in {} before {operation}: {error}",
                self.dataset,
                self.path.display()
            ),
        };
        let mut statement = connection.prepare(&statement).map_err(inspect_failure)?;
        Ok(statement
            .query_arrow([])
            .map_err(inspect_failure)?
            .get_schema())
    }

    fn close_failure(&self, error: duckdb::Error) -> LoadFailure {
        LoadFailure {
            code: "destination_write_failed",
            message: format!(
                "failed to close DuckDB database {}: {error}",
                self.path.display()
            ),
        }
    }
}

impl Destination for DuckDbDestination {
    fn connector_name(&self) -> &'static str {
        "duckdb"
    }

    fn supported_load_modes(&self) -> &'static [LoadMode] {
        ALL_LOAD_MODES
    }

    fn begin(&self, mode: LoadMode) -> Result<Box<dyn DestinationWriter>, DestinationWriteFailure> {
        match mode {
            LoadMode::FullRefresh => Ok(Box::new(DuckDbFullRefreshWriter::begin(
                self.table.clone(),
            )?)),
            LoadMode::Append => Ok(Box::new(DuckDbAppendWriter::begin(self.table.clone())?)),
            LoadMode::Merge => Ok(Box::new(DuckDbMergeWriter::begin(
                self.table.clone(),
                self.merge_keys.clone(),
            )?)),
        }
    }
}

/// The DuckDB full-refresh session: an explicit transaction holds the
/// `CREATE OR REPLACE` and every following chunk `INSERT`, and the terminal
/// `COMMIT` is the single commit boundary — any earlier failure rolls back
/// and leaves the destination untouched (ADR-0047), preserving the Atomic
/// Commit posture of ADR-0030 across chunks. Dropping the uncommitted
/// writer drops the connection, which rolls the open transaction back.
struct DuckDbFullRefreshWriter {
    table: DuckDbTable,
    // Interior lock because `write_chunk` takes `&self` (the session
    // contract is `Sync`); at this connector's declared parallelism limit
    // of 1 the lock is never contended.
    session: Mutex<DuckDbFullRefreshSession>,
}

struct DuckDbFullRefreshSession {
    connection: Option<Connection>,
    // Chunks written inside the still-open transaction: nothing is committed
    // before COMMIT, which is why abandon reports zero regardless.
    written_chunks: u64,
    written_records: u64,
}

impl DuckDbFullRefreshWriter {
    fn begin(table: DuckDbTable) -> Result<Self, DestinationWriteFailure> {
        let connection = table.open_arrow_connection()?;
        connection
            .execute_batch("BEGIN")
            .map_err(|error| LoadFailure {
                code: "destination_write_failed",
                message: format!(
                    "failed to begin the DuckDB replace transaction in {}: {error}",
                    table.path.display()
                ),
            })?;
        Ok(DuckDbFullRefreshWriter {
            table,
            session: Mutex::new(DuckDbFullRefreshSession {
                connection: Some(connection),
                written_chunks: 0,
                written_records: 0,
            }),
        })
    }

    fn replace_failure(&self, error: duckdb::Error) -> DestinationWriteFailure {
        DestinationWriteFailure::atomic(
            LoadFailure {
                code: "destination_write_failed",
                message: format!(
                    "failed to replace DuckDB table {} in {}: {error}",
                    self.table.dataset,
                    self.table.path.display()
                ),
            },
            "transactional_replace",
            0,
        )
    }
}

impl DestinationWriter for DuckDbFullRefreshWriter {
    fn write_chunk(&self, batch: &RecordBatch) -> Result<(), DestinationWriteFailure> {
        let mut session = self.session.lock().expect("writer session lock");
        // The first chunk replaces the table; the remaining chunks extend it
        // inside the same transaction, so every statement shares the replace
        // wording and the atomic posture.
        let statement = if session.written_chunks == 0 {
            format!(
                "CREATE OR REPLACE TABLE {} AS SELECT * FROM arrow(?, ?)",
                self.table.quoted_dataset()
            )
        } else {
            format!(
                "INSERT INTO {} SELECT * FROM arrow(?, ?)",
                self.table.quoted_dataset()
            )
        };
        session
            .connection
            .as_ref()
            .expect("session connection open")
            .execute(&statement, arrow_recordbatch_to_query_params(batch.clone()))
            .map_err(|error| self.replace_failure(error))?;
        session.written_chunks += 1;
        session.written_records += batch.num_rows() as u64;
        Ok(())
    }

    fn commit(self: Box<Self>) -> Result<DestinationWrite, DestinationWriteFailure> {
        let (connection, written_chunks, written_records) = {
            let mut session = self.session.lock().expect("writer session lock");
            (
                session.connection.take().expect("session connection open"),
                session.written_chunks,
                session.written_records,
            )
        };
        connection.execute_batch("COMMIT").map_err(|error| {
            DestinationWriteFailure::atomic(
                LoadFailure {
                    code: "destination_write_failed",
                    message: format!(
                        "failed to commit the DuckDB replace of table {} in {}: {error}",
                        self.table.dataset,
                        self.table.path.display()
                    ),
                },
                "transactional_replace",
                0,
            )
        })?;
        connection.close().map_err(|(_, error)| {
            // The replace had already committed when the close failed, so
            // the failure reports the written records and chunks honestly.
            DestinationWriteFailure {
                failure: self.table.close_failure(error),
                facts: DestinationWriteFacts::atomic("transactional_replace"),
                written_records,
                committed_chunks: written_chunks,
                transience: Transience::Terminal,
            }
        })?;

        Ok(DestinationWrite {
            bytes_written: None,
            facts: DestinationWriteFacts::atomic("transactional_replace"),
        })
    }

    fn abandon(self: Box<Self>) -> AbandonedWrite {
        // Dropping the connection rolls the open transaction back: nothing
        // was committed.
        AbandonedWrite {
            committed_chunks: 0,
            written_records: 0,
            facts: DestinationWriteFacts::not_applicable(),
        }
    }
}

/// The DuckDB append session: the destination table's Arrow schema is
/// inspected once at session start, each chunk aligns against it before
/// crossing the write boundary — DuckDB's `INSERT ... BY NAME` would
/// otherwise coerce lossy types or fill omissions with nulls — and one
/// auto-committed `INSERT` commits each chunk (ADR-0047), so a failure
/// keeps exactly the committed chunk prefix.
struct DuckDbAppendWriter {
    table: DuckDbTable,
    destination_schema: SchemaRef,
    // Interior lock because `write_chunk` takes `&self` (the session
    // contract is `Sync`); at this connector's declared parallelism limit
    // of 1 the lock is never contended.
    session: Mutex<DuckDbAppendSession>,
}

struct DuckDbAppendSession {
    connection: Option<Connection>,
    committed_chunks: u64,
    written_records: u64,
}

impl DuckDbAppendSession {
    /// Attaches the committed-prefix facts to a chunk failure; see
    /// [`ParquetAppendProgress::chunk_failure`].
    fn chunk_failure(&self, failure: LoadFailure) -> DestinationWriteFailure {
        if self.committed_chunks > 0 {
            DestinationWriteFailure {
                failure,
                facts: DestinationWriteFacts::best_effort("insert"),
                written_records: self.written_records,
                committed_chunks: self.committed_chunks,
                transience: Transience::Terminal,
            }
        } else {
            failure.into()
        }
    }
}

impl DuckDbAppendWriter {
    fn begin(table: DuckDbTable) -> Result<Self, DestinationWriteFailure> {
        let connection = table.open_arrow_connection()?;
        let destination_schema = table.inspect_schema(&connection, "append")?;
        Ok(DuckDbAppendWriter {
            table,
            destination_schema,
            session: Mutex::new(DuckDbAppendSession {
                connection: Some(connection),
                committed_chunks: 0,
                written_records: 0,
            }),
        })
    }
}

impl DestinationWriter for DuckDbAppendWriter {
    fn write_chunk(&self, batch: &RecordBatch) -> Result<(), DestinationWriteFailure> {
        let mut session = self.session.lock().expect("writer session lock");
        let append_batch = align_batch_to_duckdb_schema(
            &self.table,
            batch,
            self.destination_schema.clone(),
            "append",
        )
        .map_err(|failure| session.chunk_failure(failure))?;

        let statement = format!(
            "INSERT INTO {} BY NAME SELECT * FROM arrow(?, ?)",
            self.table.quoted_dataset()
        );
        session
            .connection
            .as_ref()
            .expect("session connection open")
            .execute(
                &statement,
                arrow_recordbatch_to_query_params(append_batch.clone()),
            )
            .map_err(|error| DestinationWriteFailure {
                failure: LoadFailure {
                    code: "destination_write_failed",
                    message: format!(
                        "failed to append to DuckDB table {} in {}: {error}",
                        self.table.dataset,
                        self.table.path.display()
                    ),
                },
                facts: DestinationWriteFacts::best_effort("insert"),
                written_records: session.written_records,
                committed_chunks: session.committed_chunks,
                transience: Transience::Terminal,
            })?;
        session.committed_chunks += 1;
        session.written_records += append_batch.num_rows() as u64;
        Ok(())
    }

    fn commit(self: Box<Self>) -> Result<DestinationWrite, DestinationWriteFailure> {
        let (connection, committed_chunks, written_records) = {
            let mut session = self.session.lock().expect("writer session lock");
            (
                session.connection.take().expect("session connection open"),
                session.committed_chunks,
                session.written_records,
            )
        };
        connection
            .close()
            .map_err(|(_, error)| DestinationWriteFailure {
                failure: self.table.close_failure(error),
                facts: DestinationWriteFacts::best_effort("insert"),
                written_records,
                committed_chunks,
                transience: Transience::Terminal,
            })?;

        Ok(DestinationWrite {
            bytes_written: None,
            facts: DestinationWriteFacts::best_effort("insert"),
        })
    }

    fn abandon(self: Box<Self>) -> AbandonedWrite {
        let session = self.session.lock().expect("writer session lock");
        append_abandoned("insert", session.committed_chunks, session.written_records)
    }
}

/// The DuckDB merge session (ADR-0059): the destination table must already
/// exist — its schema is probed at session start exactly like append, so
/// bootstrap stays "first load `full_refresh`, then switch to `merge`" —
/// then one explicit transaction holds a session-unique temporary staging
/// table, every chunk aligns against the destination schema and lands in
/// the stage, and `commit` runs the duplicate-key gate (ADR-0058), the
/// matched-record count, and one `MERGE INTO` before the single terminal
/// `COMMIT` (the full-refresh commit family of ADR-0047). Nothing is
/// destination-visible before that commit; any earlier failure drops the
/// connection, which rolls the open transaction back and discards the
/// temporary stage.
struct DuckDbMergeWriter {
    table: DuckDbTable,
    merge_keys: Vec<String>,
    destination_schema: SchemaRef,
    /// The session-unique temporary stage name. References qualify it as
    /// `temp.main.<stage>` so no persistent table can capture them, and the
    /// UUID suffix keeps the unqualified `MERGE INTO` target from ever
    /// resolving to the stage by name.
    stage: String,
    // Interior lock because `write_chunk` takes `&self` (the session
    // contract is `Sync`); at this connector's declared parallelism limit
    // of 1 the lock is never contended.
    session: Mutex<DuckDbMergeSession>,
}

struct DuckDbMergeSession {
    connection: Option<Connection>,
    // Chunks staged inside the still-open transaction: nothing is committed
    // before COMMIT, which is why abandon reports zero regardless.
    staged_chunks: u64,
    staged_records: u64,
}

impl DuckDbMergeWriter {
    fn begin(table: DuckDbTable, merge_keys: Vec<String>) -> Result<Self, DestinationWriteFailure> {
        // The definition boundary rejects a merge load without keys, so a
        // merge session always has at least one.
        assert!(!merge_keys.is_empty(), "merge session without merge keys");
        let connection = table.open_arrow_connection()?;
        let destination_schema = table.inspect_schema(&connection, "merge")?;
        connection
            .execute_batch("BEGIN")
            .map_err(|error| LoadFailure {
                code: "destination_write_failed",
                message: format!(
                    "failed to begin the DuckDB merge transaction in {}: {error}",
                    table.path.display()
                ),
            })?;
        let stage = format!("data_spark_merge_stage_{}", Uuid::new_v4().simple());
        connection
            .execute_batch(&format!(
                "CREATE TEMPORARY TABLE {} AS SELECT * FROM {} LIMIT 0",
                quote_identifier(&stage),
                table.quoted_dataset()
            ))
            .map_err(|error| LoadFailure {
                code: "destination_write_failed",
                message: format!(
                    "failed to create the DuckDB merge stage for table {} in {}: {error}",
                    table.dataset,
                    table.path.display()
                ),
            })?;
        Ok(DuckDbMergeWriter {
            table,
            merge_keys,
            destination_schema,
            stage,
            session: Mutex::new(DuckDbMergeSession {
                connection: Some(connection),
                staged_chunks: 0,
                staged_records: 0,
            }),
        })
    }

    /// A failure inside the still-open merge transaction: atomic facts,
    /// nothing written — dropping the connection rolls everything back.
    fn transactional_failure(
        &self,
        code: &'static str,
        message: String,
    ) -> DestinationWriteFailure {
        DestinationWriteFailure::atomic(LoadFailure { code, message }, "transactional_merge", 0)
    }

    fn write_failure(&self, action: &str, error: duckdb::Error) -> DestinationWriteFailure {
        self.transactional_failure(
            "destination_write_failed",
            format!(
                "failed to {action} DuckDB table {} in {}: {error}",
                self.table.dataset,
                self.table.path.display()
            ),
        )
    }

    /// The qualified stage reference every statement uses: the `temp`
    /// catalog is per connection, so the reference can never capture a
    /// persistent table.
    fn stage_reference(&self) -> String {
        format!("temp.main.{}", quote_identifier(&self.stage))
    }

    /// The `ON` equality over the merge keys: null keys were rejected before
    /// the write phase, so SQL equality is total over the staged records.
    fn key_equality(&self) -> String {
        self.merge_keys
            .iter()
            .map(|key| {
                format!(
                    "merge_target.{key} = merge_source.{key}",
                    key = quote_identifier(key)
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ")
    }

    /// Counts the staged records that share their merge key tuple with
    /// another staged record — the ADR-0058 gate, run destination-side
    /// because a distinct-key set is not bounded pass-1 state (ADR-0045).
    /// Zero or at least two, never one, so the failure message can speak of
    /// records in the plural.
    fn colliding_records_sql(&self) -> String {
        let partition = self
            .merge_keys
            .iter()
            .map(|key| quote_identifier(key))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "SELECT count(*) FROM (SELECT count(*) OVER (PARTITION BY {partition}) \
             AS key_tuple_records FROM {stage}) AS keyed WHERE key_tuple_records > 1",
            stage = self.stage_reference()
        )
    }

    /// Counts the staged records whose key tuple matches an existing
    /// destination record — the report's `updated`; the remainder of the
    /// stage is `inserted`, so `updated + inserted` is the staged record
    /// count by construction (ADR-0059).
    fn matched_records_sql(&self) -> String {
        format!(
            "SELECT count(*) FROM {stage} AS merge_source WHERE EXISTS \
             (SELECT 1 FROM {target} AS merge_target WHERE {equality})",
            stage = self.stage_reference(),
            target = self.table.quoted_dataset(),
            equality = self.key_equality()
        )
    }

    /// The single `MERGE INTO` of the session: matched destination records
    /// are replaced whole — every non-key column takes the staged value —
    /// and unmatched staged records insert. With every column a key there is
    /// nothing to update, so the matched clause is omitted and matched
    /// records are left as their own replacement.
    fn merge_sql(&self) -> String {
        let columns = self
            .destination_schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        let non_key_updates = columns
            .iter()
            .filter(|column| !self.merge_keys.iter().any(|key| key == *column))
            .map(|column| {
                format!(
                    "{column} = merge_source.{column}",
                    column = quote_identifier(column)
                )
            })
            .collect::<Vec<_>>();
        let matched_clause = if non_key_updates.is_empty() {
            String::new()
        } else {
            format!(
                " WHEN MATCHED THEN UPDATE SET {}",
                non_key_updates.join(", ")
            )
        };
        let insert_columns = columns
            .iter()
            .map(|column| quote_identifier(column))
            .collect::<Vec<_>>()
            .join(", ");
        let insert_values = columns
            .iter()
            .map(|column| format!("merge_source.{}", quote_identifier(column)))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "MERGE INTO {target} AS merge_target USING {stage} AS merge_source \
             ON {equality}{matched_clause} \
             WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})",
            target = self.table.quoted_dataset(),
            stage = self.stage_reference(),
            equality = self.key_equality()
        )
    }
}

impl DestinationWriter for DuckDbMergeWriter {
    fn write_chunk(&self, batch: &RecordBatch) -> Result<(), DestinationWriteFailure> {
        let mut session = self.session.lock().expect("writer session lock");
        // Chunk alignment inherits the append rule verbatim (ADR-0059): 1:1
        // name match, exact types, non-nullable destination columns admit
        // no nulls — additive drift against an unmigrated table fails here
        // exactly like append.
        let merge_batch = align_batch_to_duckdb_schema(
            &self.table,
            batch,
            self.destination_schema.clone(),
            "merge",
        )
        .map_err(|failure| DestinationWriteFailure::atomic(failure, "transactional_merge", 0))?;

        let statement = format!(
            "INSERT INTO {} BY NAME SELECT * FROM arrow(?, ?)",
            self.stage_reference()
        );
        session
            .connection
            .as_ref()
            .expect("session connection open")
            .execute(
                &statement,
                arrow_recordbatch_to_query_params(merge_batch.clone()),
            )
            .map_err(|error| self.write_failure("stage merge records for", error))?;
        session.staged_chunks += 1;
        session.staged_records += merge_batch.num_rows() as u64;
        Ok(())
    }

    fn commit(self: Box<Self>) -> Result<DestinationWrite, DestinationWriteFailure> {
        let (connection, staged_chunks, staged_records) = {
            let mut session = self.session.lock().expect("writer session lock");
            (
                session.connection.take().expect("session connection open"),
                session.staged_chunks,
                session.staged_records,
            )
        };

        // The duplicate-key gate (ADR-0058): surviving records must map to
        // distinct key tuples, or the merge's meaning would depend on an
        // arbitrary winner. Failing here returns before COMMIT, so dropping
        // the connection rolls the whole transaction back.
        let colliding: i64 = connection
            .query_row(&self.colliding_records_sql(), [], |row| row.get(0))
            .map_err(|error| self.write_failure("check merge key uniqueness for", error))?;
        if colliding > 0 {
            return Err(self.transactional_failure(
                "duplicate_merge_keys",
                format!(
                    "{colliding} surviving records share a merge key tuple with another \
                     surviving record for DuckDB table {} in {}",
                    self.table.dataset,
                    self.table.path.display()
                ),
            ));
        }

        // Counted inside the same transaction snapshot the MERGE runs under,
        // so matched-then-replaced and unmatched-then-inserted partition the
        // stage exactly.
        let matched: i64 = connection
            .query_row(&self.matched_records_sql(), [], |row| row.get(0))
            .map_err(|error| self.write_failure("count matched merge records for", error))?;
        let updated = u64::try_from(matched).expect("non-negative matched count");
        let inserted = staged_records - updated;

        connection
            .execute_batch(&self.merge_sql())
            .map_err(|error| self.write_failure("merge into", error))?;
        connection
            .execute_batch("COMMIT")
            .map_err(|error| self.write_failure("commit the merge of", error))?;
        connection.close().map_err(|(_, error)| {
            // The merge had already committed when the close failed, so the
            // failure reports the written records and chunks honestly.
            DestinationWriteFailure {
                failure: self.table.close_failure(error),
                facts: DestinationWriteFacts::atomic("transactional_merge"),
                written_records: staged_records,
                committed_chunks: staged_chunks,
                transience: Transience::Terminal,
            }
        })?;

        Ok(DestinationWrite {
            bytes_written: None,
            facts: DestinationWriteFacts::atomic("transactional_merge")
                .with_merge_counts(updated, inserted),
        })
    }

    fn abandon(self: Box<Self>) -> AbandonedWrite {
        // Dropping the connection rolls the open transaction back: nothing
        // was committed, and the temporary stage vanishes with it.
        AbandonedWrite {
            committed_chunks: 0,
            written_records: 0,
            facts: DestinationWriteFacts::not_applicable(),
        }
    }
}

fn align_batch_to_duckdb_schema(
    table: &DuckDbTable,
    batch: &RecordBatch,
    destination_schema: SchemaRef,
    operation: &'static str,
) -> Result<RecordBatch, LoadFailure> {
    let source_schema = batch.schema();
    if source_schema.fields().len() != destination_schema.fields().len() {
        return Err(duckdb_schema_mismatch(table, operation));
    }

    let mut columns = Vec::with_capacity(destination_schema.fields().len());
    for destination_field in destination_schema.fields() {
        let source_index = source_schema
            .index_of(destination_field.name())
            .map_err(|_| duckdb_schema_mismatch(table, operation))?;
        let source_field = source_schema.field(source_index);
        let column = batch.column(source_index);
        if source_field.data_type() != destination_field.data_type()
            || (!destination_field.is_nullable() && column.null_count() > 0)
        {
            return Err(duckdb_schema_mismatch(table, operation));
        }
        columns.push(column.clone());
    }

    RecordBatch::try_new(destination_schema, columns).map_err(|error| LoadFailure {
        code: "destination_write_failed",
        message: format!(
            "failed to align {operation} records with DuckDB destination {} in {}: {error}",
            table.dataset,
            table.path.display()
        ),
    })
}

fn duckdb_schema_mismatch(table: &DuckDbTable, operation: &'static str) -> LoadFailure {
    LoadFailure {
        code: "destination_write_failed",
        message: format!(
            "{operation} schema does not match DuckDB destination {} in {}",
            table.dataset,
            table.path.display()
        ),
    }
}

/// The resolved `sqlserver` destination address (ADR-0060): every key of the
/// block validated and defaulted, plus the dataset naming the destination
/// table. Resolution is offline — the Definition phase never opens a
/// connection — so connectivity, authentication, and TLS failures stay
/// write-phase facts for the slices that write.
// The write slices (ADR-0064) consume the resolved address; until the first
// sqlserver session exists, only `password_env` is read outside tests, and
// the remaining fields would trip dead_code in non-test builds. Drop the
// attribute with the first sqlserver write session.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct SqlServerConfig {
    host: String,
    port: u16,
    database: String,
    schema: String,
    user: String,
    password_env: String,
    encryption: SqlServerEncryption,
    trust_server_certificate: bool,
    accept_datetime_rounding: bool,
    dataset: String,
}

/// The two encryption postures of ADR-0060: `required` — the secure default —
/// and `optional`, reproducing the proven SSMS `Encrypt=Optional` posture for
/// servers that decline required encryption. No fully-unencrypted state
/// exists: it would send login credentials effectively in the clear.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SqlServerEncryption {
    Required,
    Optional,
}

impl SqlServerConfig {
    /// Validates and resolves the `sqlserver` destination block plus the
    /// load's `dataset` (ADR-0060), entirely offline. Each required key must
    /// be present and non-empty, `encryption` must be `required` (the
    /// default) or `optional`, and `schema` — when written — must be one
    /// non-empty dot-free identifier (`invalid_destination_config`); absent
    /// optional keys take their defaults — port 1433, schema `dbo`,
    /// encryption required, both boolean knobs false. The dataset must be
    /// present and non-empty (`missing_dataset`) and dot-free: qualification
    /// is physical addressing and lives in `schema`, so a dotted name fails
    /// `invalid_dataset_name` pointing there.
    fn from_definition(
        definition: &SqlServerDestinationDefinition,
        dataset: Option<&str>,
    ) -> Result<SqlServerConfig, LoadFailure> {
        debug_assert_eq!(
            definition.connector, "sqlserver",
            "deserialization dispatches the sqlserver shape on its connector name"
        );
        let invalid_destination_config = |message: String| LoadFailure {
            code: "invalid_destination_config",
            message,
        };
        let required = |key: &'static str, value: &Option<String>| {
            value
                .as_deref()
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    invalid_destination_config(format!(
                        "destination.{key} is required for a sqlserver destination \
                         and must not be empty"
                    ))
                })
        };
        let host = required("host", &definition.host)?;
        let database = required("database", &definition.database)?;
        let user = required("user", &definition.user)?;
        let password_env = required("password_env", &definition.password_env)?;
        let encryption = match definition.encryption.as_deref() {
            None | Some("required") => SqlServerEncryption::Required,
            Some("optional") => SqlServerEncryption::Optional,
            Some(other) => {
                return Err(invalid_destination_config(format!(
                    "destination.encryption must be \"required\" or \"optional\", got: {other}"
                )))
            }
        };
        let schema = match definition.schema.as_deref() {
            None => "dbo".to_string(),
            Some("") => {
                return Err(invalid_destination_config(
                    "destination.schema must not be empty for a sqlserver destination".to_string(),
                ))
            }
            Some(schema) if schema.contains('.') => {
                return Err(invalid_destination_config(format!(
                    "destination.schema must be a single identifier without a dot, \
                     got: {schema}"
                )))
            }
            Some(schema) => schema.to_string(),
        };
        let dataset = dataset
            .filter(|dataset| !dataset.is_empty())
            .ok_or_else(|| LoadFailure {
                code: "missing_dataset",
                message: "load definition dataset is required for a sqlserver destination"
                    .to_string(),
            })?;
        if dataset.contains('.') {
            return Err(LoadFailure {
                code: "invalid_dataset_name",
                message: format!(
                    "load definition dataset {dataset:?} must not contain a dot for a \
                     sqlserver destination: schema qualification lives in destination.schema"
                ),
            });
        }
        Ok(SqlServerConfig {
            host,
            port: definition
                .port
                .map(std::num::NonZeroU16::get)
                .unwrap_or(1433),
            database,
            schema,
            user,
            password_env,
            encryption,
            trust_server_certificate: definition.trust_server_certificate.unwrap_or(false),
            accept_datetime_rounding: definition.accept_datetime_rounding.unwrap_or(false),
            dataset: dataset.to_string(),
        })
    }
}

/// Resolves the credential reference in the Definition phase (ADR-0060): the
/// environment variable `password_env` names must be set and non-empty at
/// load time. Only resolvability is checked — the value stays in the
/// environment, never in any struct — and it can never reach a report,
/// because no contract key can carry it (ADR-0061).
fn resolve_credential_reference(password_env: &str) -> Result<(), LoadFailure> {
    match std::env::var_os(password_env) {
        None => Err(LoadFailure {
            code: "unresolved_credential_reference",
            message: format!(
                "environment variable {password_env} named by destination.password_env \
                 is not set"
            ),
        }),
        Some(value) if value.is_empty() => Err(LoadFailure {
            code: "unresolved_credential_reference",
            message: format!(
                "environment variable {password_env} named by destination.password_env \
                 is empty"
            ),
        }),
        Some(_) => Ok(()),
    }
}

/// The SQL Server destination (ADR-0060): in this slice, pure
/// Definition-phase surface — the block is validated offline and echoed, and
/// no load mode is supported yet. The write strategies land mode by mode
/// (ADR-0064), widening [`Destination::supported_load_modes`] as they do, so
/// until then every load declines through [`Destination::validate_mode`]
/// before any connection could exist.
struct SqlServerDestination {
    // Held for the write slices (ADR-0064); no shipped path reads it yet.
    #[allow(dead_code)]
    config: SqlServerConfig,
}

const NO_LOAD_MODES: &[LoadMode] = &[];

impl Destination for SqlServerDestination {
    fn connector_name(&self) -> &'static str {
        "sqlserver"
    }

    fn supported_load_modes(&self) -> &'static [LoadMode] {
        NO_LOAD_MODES
    }

    fn begin(&self, mode: LoadMode) -> Result<Box<dyn DestinationWriter>, DestinationWriteFailure> {
        // Every mode is declined until the first write slice widens the
        // list: the orchestrator's validate_mode call fails the load before
        // begin, and a directly-driven session reports the same declination.
        self.validate_mode(mode)?;
        unreachable!("sqlserver validate_mode declines every load mode");
    }
}

/// Opens a source file and measures its bytes, the shared head of both
/// passes.
fn open_source_file(source_path: &Path, format_label: &str) -> Result<(File, u64), LoadFailure> {
    let file = File::open(source_path).map_err(|error| LoadFailure {
        code: "source_read_failed",
        message: format!(
            "failed to read {format_label} source {}: {error}",
            source_path.display()
        ),
    })?;
    let source_bytes = file
        .metadata()
        .map_err(|error| LoadFailure {
            code: "source_read_failed",
            message: format!(
                "failed to inspect {format_label} source {}: {error}",
                source_path.display()
            ),
        })?
        .len();
    Ok((file, source_bytes))
}

/// The CSV reader configuration both passes share. Flexible parsing so a
/// record with the wrong field count arrives as a record and is rejected
/// with a clear message, instead of failing the whole read (ADR-0036).
fn csv_source_reader<R: Read>(reader: R) -> csv::Reader<R> {
    csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(reader)
}

fn read_csv_header<R: Read>(
    reader: &mut csv::Reader<R>,
    source_path: &Path,
) -> Result<Vec<String>, LoadFailure> {
    let field_names = reader
        .headers()
        .map_err(|error| LoadFailure {
            code: "malformed_csv",
            message: format!("malformed CSV syntax in {}: {error}", source_path.display()),
        })?
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
    Ok(field_names)
}

/// One CSV reader item as both passes classify it: a positioned record, or
/// the parse rejection it earns — the reader keeps yielding records after a
/// per-record parse error, so one unreadable record rejects only itself,
/// and a wrong-length record cannot map onto the header, so its cells are
/// recovered as an array.
enum CsvItem {
    Record(schema::TextRecord),
    Rejected(RejectedRecord),
}

fn csv_item(record: Result<csv::StringRecord, csv::Error>, field_count: usize) -> CsvItem {
    match record {
        Err(error) => {
            let line = error
                .position()
                .map(|position| position.line())
                .unwrap_or(0);
            CsvItem::Rejected(RejectedRecord {
                line,
                code: MALFORMED_CSV_RECORD,
                field: None,
                source_field: None,
                message: error.to_string(),
                record: Value::Null,
            })
        }
        Ok(record) => {
            let line = record
                .position()
                .map(|position| position.line())
                .unwrap_or(0);
            if record.len() != field_count {
                return CsvItem::Rejected(RejectedRecord {
                    line,
                    code: MALFORMED_CSV_RECORD,
                    field: None,
                    source_field: None,
                    message: format!("expected {field_count} fields, found {}", record.len()),
                    record: Value::Array(
                        record
                            .iter()
                            .map(|value| Value::String(value.to_string()))
                            .collect(),
                    ),
                });
            }
            CsvItem::Record(schema::TextRecord {
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
            })
        }
    }
}

/// One JSONL line as both passes classify it: skipped blank space, a
/// positioned record, or the parse rejection it earns — a line that is not a
/// JSON object rejects that record, not the load, with the raw line
/// recovered for troubleshooting. Lines arrive as bytes so invalid UTF-8
/// rejects only its record.
enum JsonlLine {
    Blank,
    Record(schema::JsonRecord),
    Rejected(RejectedRecord),
}

fn jsonl_line_record(line_bytes: &[u8], line_number: u64) -> JsonlLine {
    let mut line_bytes = line_bytes;
    if line_bytes.last() == Some(&b'\n') {
        line_bytes = &line_bytes[..line_bytes.len() - 1];
    }
    if line_bytes.last() == Some(&b'\r') {
        line_bytes = &line_bytes[..line_bytes.len() - 1];
    }
    let line = match std::str::from_utf8(line_bytes) {
        Ok(line) => line,
        Err(error) => {
            return JsonlLine::Rejected(RejectedRecord {
                line: line_number,
                code: MALFORMED_JSONL_RECORD,
                field: None,
                source_field: None,
                message: error.to_string(),
                record: Value::String(String::from_utf8_lossy(line_bytes).into_owned()),
            })
        }
    };
    if line.trim().is_empty() {
        return JsonlLine::Blank;
    }
    match serde_json::from_str::<Value>(line) {
        Ok(Value::Object(object)) => JsonlLine::Record(schema::JsonRecord {
            line: line_number,
            object,
        }),
        Ok(_) => JsonlLine::Rejected(RejectedRecord {
            line: line_number,
            code: MALFORMED_JSONL_RECORD,
            field: None,
            source_field: None,
            message: "each JSONL record must be a JSON object".to_string(),
            record: Value::String(line.to_string()),
        }),
        Err(error) => JsonlLine::Rejected(RejectedRecord {
            line: line_number,
            code: MALFORMED_JSONL_RECORD,
            field: None,
            source_field: None,
            message: error.to_string(),
            record: Value::String(line.to_string()),
        }),
    }
}

/// The FNV-1a state both passes fold the source bytes through, shared
/// between the reader wrapper and the end-of-pass comparison. The hash is a
/// same-process mutation guard, not a cryptographic digest (ADR-0045).
#[derive(Clone)]
struct HashState(Rc<Cell<u64>>);

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

impl HashState {
    fn new() -> Self {
        HashState(Rc::new(Cell::new(FNV_OFFSET_BASIS)))
    }

    fn update(&self, bytes: &[u8]) {
        let mut hash = self.0.get();
        for byte in bytes {
            hash = (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME);
        }
        self.0.set(hash);
    }

    fn value(&self) -> u64 {
        self.0.get()
    }
}

/// Folds every byte a pass reads into the hash state, incidentally to the
/// existing read — no second read of the source happens for the guard.
struct HashingReader<R> {
    inner: R,
    state: HashState,
}

impl<R> HashingReader<R> {
    fn new(inner: R, state: HashState) -> Self {
        HashingReader { inner, state }
    }
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.state.update(&buf[..read]);
        Ok(read)
    }
}

/// Where a pass-2 chunk stream stands: the file is opened lazily on the
/// first pull, the final chunk is followed by one end-of-input divergence
/// check, and any failure is terminal.
enum ChunkPhase {
    NotStarted,
    Streaming,
    FinalCheck,
    Done,
}

fn source_changed(path: &Path, detail: String) -> LoadFailure {
    LoadFailure {
        code: "source_changed_during_load",
        message: format!(
            "source {} changed during the load: {detail}",
            path.display()
        ),
    }
}

/// The per-record cross-check of pass 2 against pass 1 (ADR-0045): a record
/// whose pass-2 outcome matches pass 1 either skips (rejected in both) or
/// survives into the chunk; a differing outcome is semantic divergence.
enum RecordDisposition {
    Survives,
    Skips,
}

/// What pass 1 established and pass 2 must reproduce (ADR-0045): the
/// rejected records' source lines, the full-input record count, and the
/// byte hash — the whole divergence contract of a chunk stream, shared by
/// both formats.
struct PassOneExpectations {
    rejected_lines: HashSet<u64>,
    records: u64,
    hash: u64,
}

impl PassOneExpectations {
    fn cross_check(
        &self,
        path: &Path,
        line: u64,
        rejected_now: bool,
    ) -> Result<RecordDisposition, LoadFailure> {
        match (rejected_now, self.rejected_lines.contains(&line)) {
            (true, true) => Ok(RecordDisposition::Skips),
            (false, false) => Ok(RecordDisposition::Survives),
            _ => Err(source_changed(
                path,
                format!("the record at line {line} no longer matches its first-pass outcome"),
            )),
        }
    }

    /// The end-of-input divergence checks, run on the pull after the final
    /// chunk: the record count first — it names the change — then the byte
    /// hash, which catches everything else.
    fn verify_end_of_input(&self, path: &Path, seen: u64, hash: u64) -> Result<(), LoadFailure> {
        if seen != self.records {
            return Err(source_changed(
                path,
                format!("it now has {seen} records instead of {}", self.records),
            ));
        }
        if hash != self.hash {
            return Err(source_changed(
                path,
                "its bytes no longer match the first pass".to_string(),
            ));
        }
        Ok(())
    }
}

/// The pass-2 chunk stream over a CSV source: re-reads the file against the
/// fixed plan, re-runs the same per-record checks only to skip the records
/// pass 1 rejected, and yields chunks of at most `chunk_rows` surviving
/// records — deterministically, since survivors fill chunks in source order.
/// Divergence from pass 1 fails the stream as `source_changed_during_load`:
/// semantically per record (header or outcome mismatch) and at end of input
/// (record count, then the byte hash — checked on the pull after the final
/// chunk, so a full refresh sees it before its terminal commit and an
/// append at end of stream, ADR-0045).
struct CsvChunks {
    path: PathBuf,
    plan: schema::ChunkPlan,
    checks: Vec<schema::FieldCheck>,
    field_names: Vec<String>,
    chunk_rows: usize,
    expectations: PassOneExpectations,
    reading: Option<CsvReading>,
    phase: ChunkPhase,
}

struct CsvReading {
    records: csv::StringRecordsIntoIter<HashingReader<File>>,
    hash: HashState,
    seen: u64,
    yielded_any: bool,
}

impl CsvChunks {
    fn open(&mut self) -> Result<(), LoadFailure> {
        let (file, _) = open_source_file(&self.path, "CSV")?;
        let hash = HashState::new();
        let mut reader = csv_source_reader(HashingReader::new(file, hash.clone()));
        let field_names = read_csv_header(&mut reader, &self.path).map_err(|_| {
            source_changed(&self.path, "its CSV header no longer parses".to_string())
        })?;
        if field_names != self.field_names {
            return Err(source_changed(
                &self.path,
                "its CSV header no longer matches the resolved schema".to_string(),
            ));
        }
        self.reading = Some(CsvReading {
            records: reader.into_records(),
            hash,
            seen: 0,
            yielded_any: false,
        });
        Ok(())
    }

    fn next_chunk(&mut self) -> Result<Option<RecordBatch>, LoadFailure> {
        loop {
            match self.phase {
                ChunkPhase::Done => return Ok(None),
                ChunkPhase::NotStarted => {
                    self.open()?;
                    self.phase = ChunkPhase::Streaming;
                }
                ChunkPhase::FinalCheck => {
                    self.phase = ChunkPhase::Done;
                    let reading = self.reading.take().expect("final check follows streaming");
                    self.expectations.verify_end_of_input(
                        &self.path,
                        reading.seen,
                        reading.hash.value(),
                    )?;
                    return Ok(None);
                }
                ChunkPhase::Streaming => {
                    let reading = self.reading.as_mut().expect("streaming follows open");
                    let mut survivors: Vec<schema::TextRecord> = Vec::new();
                    loop {
                        let Some(record) = reading.records.next() else {
                            // End of input: the final (or single empty)
                            // chunk goes out before the end-of-input checks.
                            self.phase = ChunkPhase::FinalCheck;
                            if survivors.is_empty() && reading.yielded_any {
                                break;
                            }
                            reading.yielded_any = true;
                            return self.plan.build_text_chunk(&survivors).map(Some);
                        };
                        reading.seen += 1;
                        match csv_item(record, self.field_names.len()) {
                            CsvItem::Rejected(rejection) => {
                                self.expectations
                                    .cross_check(&self.path, rejection.line, true)?;
                            }
                            CsvItem::Record(text_record) => {
                                let rejected_now = schema::validate_text_record(
                                    &text_record,
                                    &self.checks,
                                    &self.field_names,
                                )
                                .is_some();
                                match self.expectations.cross_check(
                                    &self.path,
                                    text_record.line,
                                    rejected_now,
                                )? {
                                    RecordDisposition::Skips => {}
                                    RecordDisposition::Survives => survivors.push(text_record),
                                }
                            }
                        }
                        if survivors.len() == self.chunk_rows {
                            reading.yielded_any = true;
                            return self.plan.build_text_chunk(&survivors).map(Some);
                        }
                    }
                }
            }
        }
    }
}

impl Iterator for CsvChunks {
    type Item = Result<RecordBatch, LoadFailure>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_chunk() {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => None,
            Err(failure) => {
                self.phase = ChunkPhase::Done;
                Some(Err(failure))
            }
        }
    }
}

/// The pass-2 chunk stream over a JSONL source; see [`CsvChunks`]. The
/// JSONL-specific semantic guard: a record carrying a key outside the
/// resolved shape — the pass-1 key union — is divergence, since the resolved
/// plan could not have observed it.
struct JsonlChunks {
    path: PathBuf,
    plan: schema::ChunkPlan,
    checks: Vec<schema::FieldCheck>,
    resolved_shape: HashSet<String>,
    chunk_rows: usize,
    expectations: PassOneExpectations,
    reading: Option<JsonlReading>,
    phase: ChunkPhase,
}

struct JsonlReading {
    reader: BufReader<HashingReader<File>>,
    hash: HashState,
    line_number: u64,
    seen: u64,
    yielded_any: bool,
}

impl JsonlChunks {
    fn open(&mut self) -> Result<(), LoadFailure> {
        let (file, _) = open_source_file(&self.path, "JSONL")?;
        let hash = HashState::new();
        self.reading = Some(JsonlReading {
            reader: BufReader::new(HashingReader::new(file, hash.clone())),
            hash,
            line_number: 0,
            seen: 0,
            yielded_any: false,
        });
        Ok(())
    }

    fn next_chunk(&mut self) -> Result<Option<RecordBatch>, LoadFailure> {
        loop {
            match self.phase {
                ChunkPhase::Done => return Ok(None),
                ChunkPhase::NotStarted => {
                    self.open()?;
                    self.phase = ChunkPhase::Streaming;
                }
                ChunkPhase::FinalCheck => {
                    self.phase = ChunkPhase::Done;
                    let reading = self.reading.take().expect("final check follows streaming");
                    self.expectations.verify_end_of_input(
                        &self.path,
                        reading.seen,
                        reading.hash.value(),
                    )?;
                    return Ok(None);
                }
                ChunkPhase::Streaming => {
                    let reading = self.reading.as_mut().expect("streaming follows open");
                    let mut survivors: Vec<schema::JsonRecord> = Vec::new();
                    let mut line_bytes = Vec::new();
                    loop {
                        line_bytes.clear();
                        match reading.reader.read_until(b'\n', &mut line_bytes) {
                            Ok(0) => {
                                self.phase = ChunkPhase::FinalCheck;
                                if survivors.is_empty() && reading.yielded_any {
                                    break;
                                }
                                reading.yielded_any = true;
                                return self.plan.build_json_chunk(&survivors).map(Some);
                            }
                            Ok(_) => reading.line_number += 1,
                            Err(error) => {
                                return Err(LoadFailure {
                                    code: "source_read_failed",
                                    message: format!(
                                        "failed to read JSONL source {} after line {}: {error}",
                                        self.path.display(),
                                        reading.line_number,
                                    ),
                                });
                            }
                        }
                        match jsonl_line_record(&line_bytes, reading.line_number) {
                            JsonlLine::Blank => {}
                            JsonlLine::Rejected(rejection) => {
                                reading.seen += 1;
                                self.expectations
                                    .cross_check(&self.path, rejection.line, true)?;
                            }
                            JsonlLine::Record(record) => {
                                reading.seen += 1;
                                for key in record.object.keys() {
                                    if !self.resolved_shape.contains(key) {
                                        return Err(source_changed(
                                            &self.path,
                                            format!(
                                                "the record at line {} carries field {key:?} \
                                                 outside the resolved schema shape",
                                                record.line
                                            ),
                                        ));
                                    }
                                }
                                let rejected_now =
                                    schema::json_record_violates(&record, &self.checks);
                                match self.expectations.cross_check(
                                    &self.path,
                                    record.line,
                                    rejected_now,
                                )? {
                                    RecordDisposition::Skips => {}
                                    RecordDisposition::Survives => survivors.push(record),
                                }
                            }
                        }
                        if survivors.len() == self.chunk_rows {
                            reading.yielded_any = true;
                            return self.plan.build_json_chunk(&survivors).map(Some);
                        }
                    }
                }
            }
        }
    }
}

impl Iterator for JsonlChunks {
    type Item = Result<RecordBatch, LoadFailure>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_chunk() {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => None,
            Err(failure) => {
                self.phase = ChunkPhase::Done;
                Some(Err(failure))
            }
        }
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

    // ---- Streaming-port test harness ----

    /// A whole source read through the streaming port in one call: one chunk
    /// holds every survivor, and the rejections come back parsed from the
    /// streamed artifact, line-ordered exactly as the artifact contract
    /// states them.
    struct WholeRead {
        batch: RecordBatch,
        schema_decision: Value,
        source_bytes: u64,
        source_rows: u64,
        rejected_count: u64,
        rejected: Vec<Value>,
    }

    fn read_whole(source: &LocalFileSource, directive: &SchemaDirective) -> WholeRead {
        read_whole_with_merge_keys(source, &schema::MergeKeys::none(), directive)
    }

    fn read_whole_with_merge_keys(
        source: &LocalFileSource,
        merge_keys: &schema::MergeKeys,
        directive: &SchemaDirective,
    ) -> WholeRead {
        let work = TempDir::new().expect("artifact tempdir");
        let mut sink = RejectionSink::new(work.path());
        let read = source
            .read(directive, merge_keys, usize::MAX, &mut sink)
            .expect("read source");
        let batches = read
            .chunks
            .collect::<Result<Vec<_>, _>>()
            .expect("collect chunks");
        assert_eq!(batches.len(), 1, "whole read yields one chunk");
        assert!(sink.take_io_error().is_none());
        WholeRead {
            batch: batches.into_iter().next().expect("one chunk"),
            schema_decision: read.schema_decision,
            source_bytes: read.source_bytes,
            source_rows: read.source_rows,
            rejected_count: read.rejected_count,
            rejected: artifact_rejections(work.path()),
        }
    }

    fn read_whole_failure(
        source: &LocalFileSource,
        directive: &SchemaDirective,
    ) -> (ExecutionFailure, Vec<Value>) {
        read_whole_failure_with_merge_keys(source, &schema::MergeKeys::none(), directive)
    }

    fn read_whole_failure_with_merge_keys(
        source: &LocalFileSource,
        merge_keys: &schema::MergeKeys,
        directive: &SchemaDirective,
    ) -> (ExecutionFailure, Vec<Value>) {
        let work = TempDir::new().expect("artifact tempdir");
        let mut sink = RejectionSink::new(work.path());
        let failure = source
            .read(directive, merge_keys, usize::MAX, &mut sink)
            .err()
            .expect("read fails");
        assert!(sink.take_io_error().is_none());
        (failure, artifact_rejections(work.path()))
    }

    fn artifact_rejections(artifact_dir: &Path) -> Vec<Value> {
        let path = artifact_dir.join(crate::rejection::REJECTED_RECORDS_FILENAME);
        if !path.exists() {
            return Vec::new();
        }
        fs::read_to_string(path)
            .expect("rejected-records artifact")
            .lines()
            .map(|line| serde_json::from_str(line).expect("artifact line is json"))
            .collect()
    }

    /// Writes one batch through a full session — begin, one chunk, commit —
    /// the single-chunk write the pre-session port performed in one call.
    fn write_single(
        destination: &dyn Destination,
        batch: &RecordBatch,
        mode: LoadMode,
    ) -> DestinationWrite {
        let writer = destination.begin(mode).expect("begin write session");
        writer.write_chunk(batch).expect("write chunk");
        writer.commit().expect("commit write session")
    }

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
        assert!(
            destination_connector(&destination_definition("parquet", "out"), None, &[]).is_ok()
        );

        let error = destination_connector(&destination_definition("bigquery", "out"), None, &[])
            .err()
            .expect("unknown destination connector rejected");
        assert_eq!(error.code, "unsupported_destination_connector");
        assert_eq!(error.message, "unsupported destination connector: bigquery");
    }

    #[test]
    fn destination_connector_requires_a_dataset_for_duckdb() {
        assert!(destination_connector(
            &destination_definition("duckdb", "customers.duckdb"),
            Some("customers"),
            &[]
        )
        .is_ok());

        let error = destination_connector(
            &destination_definition("duckdb", "customers.duckdb"),
            None,
            &[],
        )
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
            &[],
        )
        .err()
        .expect("duckdb destination with an empty dataset rejected");
        assert_eq!(error.code, "missing_dataset");
    }

    // ---- SQL Server Definition-phase validation (ADR-0060, ADR-0061) ----

    #[test]
    fn sql_server_config_defaults_absent_optional_keys() {
        let config = SqlServerConfig::from_definition(&sqlserver_definition(), Some("customers"))
            .expect("minimal sqlserver block resolves");

        assert_eq!(config.host, "db.example.internal");
        assert_eq!(config.port, 1433);
        assert_eq!(config.database, "analytics");
        assert_eq!(config.schema, "dbo");
        assert_eq!(config.user, "loader");
        assert_eq!(config.password_env, "MSSQL_PASSWORD");
        assert_eq!(config.encryption, SqlServerEncryption::Required);
        assert!(!config.trust_server_certificate);
        assert!(!config.accept_datetime_rounding);
        assert_eq!(config.dataset, "customers");
    }

    #[test]
    fn sql_server_config_keeps_explicit_values() {
        let mut definition = sqlserver_definition();
        definition.port = Some(std::num::NonZeroU16::new(14330).expect("nonzero port"));
        definition.schema = Some("sales".to_string());
        definition.encryption = Some("optional".to_string());
        definition.trust_server_certificate = Some(true);
        definition.accept_datetime_rounding = Some(true);

        let config = SqlServerConfig::from_definition(&definition, Some("customers"))
            .expect("explicit sqlserver block resolves");

        assert_eq!(config.port, 14330);
        assert_eq!(config.schema, "sales");
        assert_eq!(config.encryption, SqlServerEncryption::Optional);
        assert!(config.trust_server_certificate);
        assert!(config.accept_datetime_rounding);
    }

    /// Overwrites one required key of a sqlserver block under test.
    type RequiredKeySetter = fn(&mut SqlServerDestinationDefinition, Option<String>);

    #[test]
    fn sql_server_config_requires_each_required_key_present_and_non_empty() {
        let setters: [(&str, RequiredKeySetter); 4] = [
            ("host", |definition, value| definition.host = value),
            ("database", |definition, value| definition.database = value),
            ("user", |definition, value| definition.user = value),
            ("password_env", |definition, value| {
                definition.password_env = value
            }),
        ];
        for (key, set) in setters {
            for value in [None, Some(String::new())] {
                let case = if value.is_none() { "missing" } else { "empty" };
                let mut definition = sqlserver_definition();
                set(&mut definition, value);
                let error = SqlServerConfig::from_definition(&definition, Some("customers"))
                    .err()
                    .unwrap_or_else(|| panic!("{case} {key} rejected"));
                assert_eq!(error.code, "invalid_destination_config", "code for {key}");
                assert!(
                    error.message.contains(&format!("destination.{key}")),
                    "message names destination.{key}: {}",
                    error.message
                );
            }
        }
    }

    #[test]
    fn sql_server_config_accepts_exactly_the_two_encryption_postures() {
        for (value, expected) in [
            ("required", SqlServerEncryption::Required),
            ("optional", SqlServerEncryption::Optional),
        ] {
            let mut definition = sqlserver_definition();
            definition.encryption = Some(value.to_string());
            let config = SqlServerConfig::from_definition(&definition, Some("customers"))
                .expect("valid encryption resolves");
            assert_eq!(config.encryption, expected, "encryption {value}");
        }

        let mut definition = sqlserver_definition();
        definition.encryption = Some("off".to_string());
        let error = SqlServerConfig::from_definition(&definition, Some("customers"))
            .err()
            .expect("unknown encryption value rejected");
        assert_eq!(error.code, "invalid_destination_config");
        assert!(
            error.message.contains("destination.encryption"),
            "message names destination.encryption: {}",
            error.message
        );
    }

    #[test]
    fn sql_server_config_rejects_an_empty_or_dotted_schema() {
        for schema in ["", "dbo.sales"] {
            let mut definition = sqlserver_definition();
            definition.schema = Some(schema.to_string());
            let error = SqlServerConfig::from_definition(&definition, Some("customers"))
                .err()
                .expect("broken schema rejected");
            assert_eq!(
                error.code, "invalid_destination_config",
                "schema {schema:?}"
            );
            assert!(
                error.message.contains("destination.schema"),
                "message names destination.schema: {}",
                error.message
            );
        }
    }

    #[test]
    fn sql_server_config_requires_a_dataset_like_duckdb() {
        for dataset in [None, Some("")] {
            let error = SqlServerConfig::from_definition(&sqlserver_definition(), dataset)
                .err()
                .expect("absent dataset rejected");
            assert_eq!(error.code, "missing_dataset");
            assert_eq!(
                error.message,
                "load definition dataset is required for a sqlserver destination"
            );
        }
    }

    #[test]
    fn sql_server_config_rejects_a_dotted_dataset_pointing_at_schema() {
        // Qualification is physical addressing and lives in destination.schema
        // (ADR-0060), so the failure must send the author there.
        let error =
            SqlServerConfig::from_definition(&sqlserver_definition(), Some("dbo.customers"))
                .err()
                .expect("dotted dataset rejected");
        assert_eq!(error.code, "invalid_dataset_name");
        assert!(
            error.message.contains("destination.schema"),
            "message points at destination.schema: {}",
            error.message
        );
    }

    #[test]
    fn sql_server_destination_declines_every_load_mode_with_a_readable_message() {
        let config = SqlServerConfig::from_definition(&sqlserver_definition(), Some("customers"))
            .expect("valid sqlserver block resolves");
        let destination = SqlServerDestination { config };

        assert!(destination.supported_load_modes().is_empty());
        for mode in [LoadMode::FullRefresh, LoadMode::Append, LoadMode::Merge] {
            let error = destination
                .validate_mode(mode)
                .expect_err("every mode declined");
            assert_eq!(error.code, "unsupported_load_mode_for_destination");
            assert_eq!(
                error.message,
                format!(
                    "sqlserver destination does not support load mode: {} \
                     (no load modes are supported for this destination yet)",
                    mode.as_str()
                )
            );
        }
    }

    #[test]
    fn destination_connector_fails_when_the_sqlserver_credential_reference_is_unset() {
        // A name no environment defines, so the test never mutates the
        // process environment; the set and empty postures live in the CLI
        // contract tests, which control a child process's environment.
        let mut definition = sqlserver_definition();
        definition.password_env = Some("DATA_SPARK_TEST_UNSET_CREDENTIAL_132".to_string());
        let error = destination_connector(
            &DestinationDefinition::SqlServer(definition),
            Some("customers"),
            &[],
        )
        .err()
        .expect("unset credential reference rejected");
        assert_eq!(error.code, "unresolved_credential_reference");
        assert!(
            error
                .message
                .contains("DATA_SPARK_TEST_UNSET_CREDENTIAL_132"),
            "message names the unresolved variable: {}",
            error.message
        );
    }

    #[test]
    fn local_file_source_rejects_unknown_format_before_touching_the_file() {
        // A non-existent path proves the format check precedes source I/O: an
        // unknown format must fail without ever opening the file.
        let source = LocalFileSource {
            path: PathBuf::from("/does/not/exist.xml"),
            format: Some("xml".to_string()),
        };
        let (error, rejected) = read_whole_failure(&source, &SchemaDirective::inferred());
        assert_eq!(error.failure.code, "unsupported_source_format");
        assert!(rejected.is_empty());
    }

    #[test]
    fn load_mode_parse_accepts_the_v1_mode_set_and_rejects_unknown() {
        assert!(matches!(
            LoadMode::parse("full_refresh"),
            Ok(LoadMode::FullRefresh)
        ));
        assert!(matches!(LoadMode::parse("append"), Ok(LoadMode::Append)));
        assert!(matches!(LoadMode::parse("merge"), Ok(LoadMode::Merge)));

        let error = LoadMode::parse("upsert")
            .err()
            .expect("unknown load mode rejected");
        assert_eq!(error.code, "unsupported_load_mode");
        assert_eq!(error.message, "unsupported load mode: upsert");
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
        let read = read_whole(&source, &SchemaDirective::inferred());

        assert_eq!(read.batch.num_rows(), 2);
        assert_eq!(
            schema_types(&read.batch),
            vec![DataType::Int64, DataType::Utf8, DataType::Float64]
        );
        assert_eq!(ints(&read.batch, 0).value(0), 1);
        assert_eq!(strings(&read.batch, 1).value(1), "Grace");
        assert_eq!(floats(&read.batch, 2).value(0), 42.50);
        assert!(read.source_bytes > 0);
        assert_eq!(read.source_rows, 2);
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
        let read = read_whole(&source, &SchemaDirective::inferred());

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
        assert_eq!(read.source_rows, 2);
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
        let read = read_whole(&source, &SchemaDirective::inferred());

        assert_eq!(read.batch.num_rows(), 2);
        assert_eq!(ints(&read.batch, 0).value(0), 1);
        assert_eq!(ints(&read.batch, 0).value(1), 3);

        assert_eq!(read.rejected_count, 1);
        assert_eq!(read.rejected.len(), 1);
        let rejected = &read.rejected[0];
        assert_eq!(rejected["line"], 3);
        assert_eq!(rejected["code"], "malformed_csv_record");
        assert!(rejected["field"].is_null());
        assert_eq!(rejected["message"], "expected 2 fields, found 3");
        // A wrong-length record cannot map onto the header, so its cells are
        // recovered as an array.
        assert_eq!(
            rejected["record"],
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
        let read = read_whole(&source, &SchemaDirective::inferred());

        assert_eq!(read.batch.num_rows(), 2);
        assert_eq!(ints(&read.batch, 0).value(1), 3);
        assert_eq!(read.rejected.len(), 1);
        let rejected = &read.rejected[0];
        assert_eq!(rejected["line"], 3);
        assert_eq!(rejected["code"], "malformed_csv_record");
        let message = rejected["message"].as_str().expect("rejection message");
        assert!(
            message.to_lowercase().contains("utf-8"),
            "message {message:?} names the parse problem"
        );
        // Nothing could be recovered from the record.
        assert!(rejected["record"].is_null());
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
        let read = read_whole(&source, &SchemaDirective::inferred());

        assert_eq!(read.batch.num_rows(), 2);
        assert_eq!(ints(&read.batch, 0).value(0), 1);
        assert_eq!(ints(&read.batch, 0).value(1), 4);

        assert_eq!(read.rejected.len(), 2);
        assert_eq!(read.rejected[0]["line"], 2);
        assert_eq!(read.rejected[0]["code"], "malformed_jsonl_record");
        assert!(read.rejected[0]["field"].is_null());
        assert!(!read.rejected[0]["message"]
            .as_str()
            .expect("rejection message")
            .is_empty());
        // The raw line is recovered for troubleshooting.
        assert_eq!(
            read.rejected[0]["record"],
            serde_json::json!("{\"id\": 2, ")
        );
        assert_eq!(read.rejected[1]["line"], 3);
        assert_eq!(read.rejected[1]["code"], "malformed_jsonl_record");
        assert_eq!(
            read.rejected[1]["message"],
            "each JSONL record must be a JSON object"
        );
        assert_eq!(read.rejected[1]["record"], serde_json::json!("[1, 2]"));
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
        let read = read_whole(&source, &directive);

        assert_eq!(read.batch.num_rows(), 1);
        assert_eq!(ints(&read.batch, 0).value(0), 1);
        assert_eq!(read.rejected_count, 2);
        let lines_and_codes = read
            .rejected
            .iter()
            .map(|rejected| {
                (
                    rejected["line"].as_u64().expect("rejection line"),
                    rejected["code"]
                        .as_str()
                        .expect("rejection code")
                        .to_string(),
                )
            })
            .collect::<Vec<_>>();
        assert!(lines_and_codes.contains(&(3, "malformed_csv_record".to_string())));
        assert!(lines_and_codes.contains(&(4, "type_coercion_failed".to_string())));
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
        let (error, rejected) = read_whole_failure(&source, &SchemaDirective::inferred());

        // No record ever parsed, so no schema can be inferred: the load
        // fails, but the source count travels with the failure and the parse
        // rejections were already streamed to their artifact.
        assert_eq!(error.failure.code, "malformed_jsonl");
        assert!(error
            .failure
            .message
            .contains("must include at least one record with fields"));
        assert_eq!(error.source_rows, Some(2));
        assert_eq!(error.rejected_count, 2);
        assert_eq!(rejected.len(), 2);
        assert_eq!(rejected[0]["line"], 1);
        assert_eq!(rejected[1]["line"], 2);
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
        let (error, rejected) = read_whole_failure(&source, &SchemaDirective::inferred());

        // The empty object parsed as a source record even though it declared no
        // fields, so it must remain part of the known source count alongside the
        // rejected line.
        assert_eq!(error.failure.code, "malformed_jsonl");
        assert_eq!(error.source_rows, Some(2));
        assert_eq!(error.rejected_count, 1);
        assert_eq!(rejected.len(), 1);
        assert_eq!(rejected[0]["line"], 2);
    }

    #[test]
    fn parquet_destination_writes_a_readable_batch_and_reports_write_facts() {
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.csv");
        fs::write(&source_path, "id,name\n1,Ada\n2,Grace\n").expect("write csv");
        let batch = read_whole(
            &LocalFileSource {
                path: source_path,
                format: Some("csv".to_string()),
            },
            &SchemaDirective::inferred(),
        )
        .batch;

        let destination_path = work.path().join("customers_dataset");
        let destination = ParquetDestination {
            path: destination_path.clone(),
        };
        let written = write_single(&destination, &batch, LoadMode::FullRefresh);

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
        let batch = read_whole(
            &LocalFileSource {
                path: source_path,
                format: Some("csv".to_string()),
            },
            &SchemaDirective::inferred(),
        )
        .batch;

        // The database file sits under a not-yet-existing parent to pin that
        // the destination prepares the parent directory like Parquet does.
        let database_path = work.path().join("warehouse").join("customers.duckdb");
        let destination = DuckDbDestination {
            table: DuckDbTable {
                path: database_path.clone(),
                dataset: "customers".to_string(),
            },
            merge_keys: Vec::new(),
        };
        let written = write_single(&destination, &batch, LoadMode::FullRefresh);

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
            table: DuckDbTable {
                path: database_path.clone(),
                dataset: "customers".to_string(),
            },
            merge_keys: Vec::new(),
        };

        let first_path = work.path().join("first.csv");
        fs::write(&first_path, "id,name\n1,Ada\n2,Grace\n").expect("write first csv");
        let second_path = work.path().join("second.csv");
        fs::write(&second_path, "id,name\n3,Katherine\n").expect("write second csv");
        for source_path in [first_path, second_path] {
            let batch = read_whole(
                &LocalFileSource {
                    path: source_path,
                    format: Some("csv".to_string()),
                },
                &SchemaDirective::inferred(),
            )
            .batch;
            write_single(&destination, &batch, LoadMode::FullRefresh);
        }

        let read_back = read_single_duckdb_batch(&database_path, "customers");
        assert_eq!(read_back.num_rows(), 1);
        assert_eq!(ints(&read_back, 0).value(0), 3);
        assert_eq!(strings(&read_back, 1).value(0), "Katherine");
    }

    // ---- Chunked pass-2 streams (ADR-0045) ----

    fn csv_source(path: &Path) -> LocalFileSource {
        LocalFileSource {
            path: path.to_path_buf(),
            format: Some("csv".to_string()),
        }
    }

    fn jsonl_source(path: &Path) -> LocalFileSource {
        LocalFileSource {
            path: path.to_path_buf(),
            format: Some("jsonl".to_string()),
        }
    }

    fn read_chunked(
        source: &LocalFileSource,
        directive: &SchemaDirective,
        chunk_rows: usize,
        sink_dir: &Path,
    ) -> SourceRead {
        let mut sink = RejectionSink::new(sink_dir);
        let read = source
            .read(directive, &schema::MergeKeys::none(), chunk_rows, &mut sink)
            .expect("read source");
        assert!(sink.take_io_error().is_none());
        read
    }

    #[test]
    fn csv_chunks_split_survivors_deterministically_by_chunk_rows() {
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.csv");
        fs::write(&source_path, "id\n1\n2\n3\n4\n5\n6\n7\n").expect("write csv");

        // 7 survivors at a bound of 3 split 3/3/1 — and split the same way
        // again on a second read of the same bytes (ADR-0045's deterministic
        // chunk boundary).
        for _ in 0..2 {
            let read = read_chunked(
                &csv_source(&source_path),
                &SchemaDirective::inferred(),
                3,
                work.path(),
            );
            let row_counts = read
                .chunks
                .collect::<Result<Vec<_>, _>>()
                .expect("collect chunks")
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>();
            assert_eq!(row_counts, vec![3, 3, 1]);
        }
    }

    #[test]
    fn chunk_stream_yields_one_empty_chunk_for_a_survivor_free_load() {
        // A header-only CSV still materializes its schema: exactly one
        // zero-row chunk, so an empty dataset lands with the right columns.
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.csv");
        fs::write(&source_path, "id,name\n").expect("write csv");

        let read = read_chunked(
            &csv_source(&source_path),
            &SchemaDirective::inferred(),
            4,
            work.path(),
        );
        let batches = read
            .chunks
            .collect::<Result<Vec<_>, _>>()
            .expect("collect chunks");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 0);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[test]
    fn csv_chunks_skip_the_records_pass_one_rejected() {
        // Line 3 parse-rejects in pass 1; pass 2 re-runs the same checks and
        // skips it, so the two survivors share one chunk of two.
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.csv");
        fs::write(&source_path, "id\n1\n2,extra\n3\n").expect("write csv");

        let read = read_chunked(
            &csv_source(&source_path),
            &SchemaDirective::inferred(),
            2,
            work.path(),
        );
        assert_eq!(read.rejected_count, 1);
        let batches = read
            .chunks
            .collect::<Result<Vec<_>, _>>()
            .expect("collect chunks");
        assert_eq!(batches.len(), 1);
        assert_eq!(ints(&batches[0], 0).value(0), 1);
        assert_eq!(ints(&batches[0], 0).value(1), 3);
    }

    fn collect_until_failure(chunks: SourceChunks) -> (Vec<RecordBatch>, Option<LoadFailure>) {
        let mut batches = Vec::new();
        for chunk in chunks {
            match chunk {
                Ok(batch) => batches.push(batch),
                Err(failure) => return (batches, Some(failure)),
            }
        }
        (batches, None)
    }

    #[test]
    fn csv_chunks_fail_on_a_bytes_only_change_at_end_of_stream() {
        // The mutation preserves every per-record outcome — same width, same
        // types — so only the byte hash can catch it, on the pull after the
        // final chunk.
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.csv");
        fs::write(&source_path, "id,name\n1,Ada\n2,Grace\n").expect("write csv");

        let read = read_chunked(
            &csv_source(&source_path),
            &SchemaDirective::inferred(),
            1,
            work.path(),
        );
        fs::write(&source_path, "id,name\n1,Bob\n2,Grace\n").expect("mutate csv");

        let (batches, failure) = collect_until_failure(read.chunks);
        assert_eq!(batches.len(), 2, "all chunks yield before the end check");
        let failure = failure.expect("bytes-only divergence detected");
        assert_eq!(failure.code, "source_changed_during_load");
        assert!(
            failure.message.contains("bytes no longer match"),
            "message {:?} names the byte guard",
            failure.message
        );
    }

    #[test]
    fn csv_chunks_fail_semantically_when_a_record_outcome_diverges() {
        // Record 3 turns wrong-width after pass 1: its pass-2 outcome
        // (rejected) no longer matches pass 1 (survived), which the
        // semantic guard catches at that record.
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.csv");
        fs::write(&source_path, "id\n1\n2\n3\n").expect("write csv");

        let read = read_chunked(
            &csv_source(&source_path),
            &SchemaDirective::inferred(),
            1,
            work.path(),
        );
        fs::write(&source_path, "id\n1\n2\n3,extra\n").expect("mutate csv");

        let (batches, failure) = collect_until_failure(read.chunks);
        assert_eq!(batches.len(), 2, "the chunks before the divergence yield");
        let failure = failure.expect("semantic divergence detected");
        assert_eq!(failure.code, "source_changed_during_load");
        assert!(
            failure.message.contains("line 4"),
            "message {:?} names the diverging record",
            failure.message
        );
    }

    #[test]
    fn csv_chunks_fail_when_the_record_count_changes() {
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.csv");
        fs::write(&source_path, "id\n1\n2\n").expect("write csv");

        let read = read_chunked(
            &csv_source(&source_path),
            &SchemaDirective::inferred(),
            usize::MAX,
            work.path(),
        );
        fs::write(&source_path, "id\n1\n2\n3\n").expect("append a record");

        let (_, failure) = collect_until_failure(read.chunks);
        let failure = failure.expect("record-count divergence detected");
        assert_eq!(failure.code, "source_changed_during_load");
        assert!(
            failure.message.contains("3 records instead of 2"),
            "message {:?} names the count divergence",
            failure.message
        );
    }

    #[test]
    fn jsonl_chunks_fail_when_a_record_carries_a_key_outside_the_resolved_shape() {
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.jsonl");
        fs::write(&source_path, "{\"id\": 1}\n{\"id\": 2}\n").expect("write jsonl");

        let read = read_chunked(
            &jsonl_source(&source_path),
            &SchemaDirective::inferred(),
            1,
            work.path(),
        );
        fs::write(&source_path, "{\"id\": 1}\n{\"id\": 2, \"note\": \"x\"}\n")
            .expect("mutate jsonl");

        let (batches, failure) = collect_until_failure(read.chunks);
        assert_eq!(batches.len(), 1);
        let failure = failure.expect("shape divergence detected");
        assert_eq!(failure.code, "source_changed_during_load");
        assert!(
            failure.message.contains("\"note\""),
            "message {:?} names the new field",
            failure.message
        );
    }

    // ---- Destination write sessions (ADR-0046, ADR-0047) ----

    fn two_row_batch(work: &TempDir, name: &str) -> RecordBatch {
        let source_path = work.path().join(name);
        fs::write(&source_path, "id,name\n1,Ada\n2,Grace\n").expect("write csv");
        read_whole(&csv_source(&source_path), &SchemaDirective::inferred()).batch
    }

    fn one_row_batch(work: &TempDir, name: &str) -> RecordBatch {
        let source_path = work.path().join(name);
        fs::write(&source_path, "id,name\n3,Katherine\n").expect("write csv");
        read_whole(&csv_source(&source_path), &SchemaDirective::inferred()).batch
    }

    #[test]
    fn parquet_full_refresh_session_stages_chunks_and_commits_in_one_rename() {
        let work = TempDir::new().expect("tempdir");
        let destination_path = work.path().join("customers_dataset");
        let destination = ParquetDestination {
            path: destination_path.clone(),
        };
        let first = two_row_batch(&work, "first.csv");
        let second = one_row_batch(&work, "second.csv");

        let writer = destination
            .begin(LoadMode::FullRefresh)
            .expect("begin session");
        writer.write_chunk(&first).expect("write first chunk");
        writer.write_chunk(&second).expect("write second chunk");
        assert!(
            !destination_path.exists(),
            "nothing is destination-visible before the terminal commit"
        );
        let written = writer.commit().expect("commit session");

        assert!(written.bytes_written.expect("parquet measures bytes") > 0);
        assert_eq!(
            written.facts.report_value(),
            serde_json::json!({
                "atomicity": "best_effort",
                "strategy": "staging_then_replace"
            })
        );
        let mut part_rows = Vec::new();
        for entry in fs::read_dir(&destination_path).expect("destination directory") {
            let path = entry.expect("entry").path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
                let mut reader =
                    ParquetRecordBatchReaderBuilder::try_new(File::open(path).expect("open part"))
                        .expect("parquet reader")
                        .build()
                        .expect("build reader");
                part_rows.push(
                    reader
                        .next()
                        .expect("one batch")
                        .expect("read batch")
                        .num_rows(),
                );
            }
        }
        part_rows.sort();
        assert_eq!(part_rows, vec![1, 2], "one part per chunk");
    }

    #[test]
    fn parquet_full_refresh_drop_without_commit_cleans_staging_and_keeps_destination() {
        let work = TempDir::new().expect("tempdir");
        let destination_path = work.path().join("customers_dataset");
        let destination = ParquetDestination {
            path: destination_path.clone(),
        };
        let batch = two_row_batch(&work, "seed.csv");
        write_single(&destination, &batch, LoadMode::FullRefresh);

        let writer = destination
            .begin(LoadMode::FullRefresh)
            .expect("begin session");
        writer.write_chunk(&batch).expect("write chunk");
        let abandoned = writer.abandon();
        assert_eq!(abandoned.committed_chunks, 0);
        assert_eq!(abandoned.written_records, 0);
        assert_eq!(
            abandoned.facts.report_value(),
            serde_json::json!({ "atomicity": "not_applicable" })
        );

        // The staging directory is gone and the previous dataset survives.
        let leftovers = fs::read_dir(work.path())
            .expect("work dir")
            .map(|entry| {
                entry
                    .expect("entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .filter(|name| name.contains("data-spark-staging"))
            .collect::<Vec<_>>();
        assert_eq!(leftovers, Vec::<String>::new());
        let read_back = read_single_parquet_batch(&destination_path);
        assert_eq!(read_back.num_rows(), 2);
    }

    #[test]
    fn duckdb_full_refresh_session_replaces_only_at_terminal_commit() {
        let work = TempDir::new().expect("tempdir");
        let database_path = work.path().join("customers.duckdb");
        let destination = DuckDbDestination {
            table: DuckDbTable {
                path: database_path.clone(),
                dataset: "customers".to_string(),
            },
            merge_keys: Vec::new(),
        };
        let seed = two_row_batch(&work, "seed.csv");
        write_single(&destination, &seed, LoadMode::FullRefresh);

        // An abandoned replace rolls back: the seeded rows survive.
        let replacement = one_row_batch(&work, "replacement.csv");
        let writer = destination
            .begin(LoadMode::FullRefresh)
            .expect("begin session");
        writer.write_chunk(&replacement).expect("write chunk");
        let abandoned = writer.abandon();
        assert_eq!(abandoned.committed_chunks, 0);
        let read_back = read_single_duckdb_batch(&database_path, "customers");
        assert_eq!(read_back.num_rows(), 2);

        // A committed multi-chunk replace lands every chunk in one table.
        let writer = destination
            .begin(LoadMode::FullRefresh)
            .expect("begin session");
        writer.write_chunk(&replacement).expect("write first chunk");
        writer.write_chunk(&seed).expect("write second chunk");
        let written = writer.commit().expect("commit session");
        assert_eq!(
            written.facts.report_value(),
            serde_json::json!({
                "atomicity": "atomic",
                "strategy": "transactional_replace"
            })
        );
        let read_back = read_single_duckdb_batch(&database_path, "customers");
        assert_eq!(read_back.num_rows(), 3);
    }

    #[test]
    fn duckdb_append_abandon_keeps_the_committed_chunk_prefix() {
        let work = TempDir::new().expect("tempdir");
        let database_path = work.path().join("customers.duckdb");
        let destination = DuckDbDestination {
            table: DuckDbTable {
                path: database_path.clone(),
                dataset: "customers".to_string(),
            },
            merge_keys: Vec::new(),
        };
        let seed = two_row_batch(&work, "seed.csv");
        write_single(&destination, &seed, LoadMode::FullRefresh);

        let appended = one_row_batch(&work, "appended.csv");
        let writer = destination.begin(LoadMode::Append).expect("begin session");
        writer.write_chunk(&appended).expect("write chunk");
        let abandoned = writer.abandon();

        // The chunk had already committed (per-chunk commit), so abandonment
        // reports it and the table keeps it.
        assert_eq!(abandoned.committed_chunks, 1);
        assert_eq!(abandoned.written_records, 1);
        assert_eq!(
            abandoned.facts.report_value(),
            serde_json::json!({ "atomicity": "best_effort", "strategy": "insert" })
        );
        let read_back = read_single_duckdb_batch(&database_path, "customers");
        assert_eq!(read_back.num_rows(), 3);
    }

    // ---- Merge sessions (ADR-0057, ADR-0058, ADR-0059) ----

    /// Seeds a DuckDB table for the merge tests, which never auto-create.
    fn seed_duckdb_table(database_path: &Path, statements: &str) {
        let connection = Connection::open(database_path).expect("open DuckDB destination");
        connection
            .execute_batch(statements)
            .expect("seed DuckDB destination");
    }

    fn csv_batch(work: &TempDir, name: &str, content: &str) -> RecordBatch {
        let source_path = work.path().join(name);
        fs::write(&source_path, content).expect("write csv");
        read_whole(
            &LocalFileSource {
                path: source_path,
                format: Some("csv".to_string()),
            },
            &SchemaDirective::inferred(),
        )
        .batch
    }

    fn merge_destination(database_path: &Path, keys: &[&str]) -> DuckDbDestination {
        DuckDbDestination {
            table: DuckDbTable {
                path: database_path.to_path_buf(),
                dataset: "customers".to_string(),
            },
            merge_keys: keys.iter().map(|key| key.to_string()).collect(),
        }
    }

    #[test]
    fn parquet_destination_declines_merge_before_any_session() {
        let destination = ParquetDestination {
            path: PathBuf::from("customers_dataset"),
        };

        let error = destination
            .validate_mode(LoadMode::Merge)
            .expect_err("parquet declines merge");
        assert_eq!(error.code, "unsupported_load_mode_for_destination");
        assert_eq!(
            error.message,
            "parquet destination does not support load mode: merge \
             (supported load modes: full_refresh, append)"
        );
    }

    #[test]
    fn duckdb_destination_supports_every_v1_load_mode() {
        let destination = merge_destination(Path::new("customers.duckdb"), &["id"]);
        for mode in [LoadMode::FullRefresh, LoadMode::Append, LoadMode::Merge] {
            assert!(destination.validate_mode(mode).is_ok());
        }
    }

    #[test]
    fn duckdb_merge_session_updates_matching_keys_and_inserts_new_ones() {
        let work = TempDir::new().expect("tempdir");
        let database_path = work.path().join("customers.duckdb");
        seed_duckdb_table(
            &database_path,
            "CREATE TABLE customers (id BIGINT, name VARCHAR); \
             INSERT INTO customers VALUES (1, 'Ada'), (2, 'Grace')",
        );
        let batch = csv_batch(&work, "day-2.csv", "id,name\n2,Grace Hopper\n3,Katherine\n");

        let destination = merge_destination(&database_path, &["id"]);
        let written = write_single(&destination, &batch, LoadMode::Merge);

        assert_eq!(written.bytes_written, None);
        assert_eq!(
            written.facts.report_value(),
            serde_json::json!({
                "atomicity": "atomic",
                "strategy": "transactional_merge",
                "merge": { "updated": 1, "inserted": 1 }
            })
        );
        let read_back = read_single_duckdb_batch(&database_path, "customers");
        assert_eq!(read_back.num_rows(), 3);
        assert_eq!(ints(&read_back, 0).values(), &[1, 2, 3]);
        assert_eq!(strings(&read_back, 1).value(0), "Ada");
        assert_eq!(strings(&read_back, 1).value(1), "Grace Hopper");
        assert_eq!(strings(&read_back, 1).value(2), "Katherine");
    }

    #[test]
    fn duckdb_merge_with_every_column_a_key_still_counts_matches() {
        // With no non-key column there is nothing to update, so the merge
        // runs without a matched clause; matched records still count as
        // updated, because their replacement equals themselves.
        let work = TempDir::new().expect("tempdir");
        let database_path = work.path().join("customers.duckdb");
        seed_duckdb_table(
            &database_path,
            "CREATE TABLE customers (id BIGINT, name VARCHAR); \
             INSERT INTO customers VALUES (1, 'Ada')",
        );
        let batch = csv_batch(&work, "pairs.csv", "id,name\n1,Ada\n2,Grace\n");

        let destination = merge_destination(&database_path, &["id", "name"]);
        let written = write_single(&destination, &batch, LoadMode::Merge);

        assert_eq!(
            written.facts.report_value()["merge"],
            serde_json::json!({ "updated": 1, "inserted": 1 })
        );
        let read_back = read_single_duckdb_batch(&database_path, "customers");
        assert_eq!(read_back.num_rows(), 2);
    }

    #[test]
    fn duckdb_merge_session_fails_on_duplicate_merge_keys_and_commits_nothing() {
        let work = TempDir::new().expect("tempdir");
        let database_path = work.path().join("customers.duckdb");
        seed_duckdb_table(
            &database_path,
            "CREATE TABLE customers (id BIGINT, name VARCHAR); \
             INSERT INTO customers VALUES (1, 'Ada')",
        );
        let batch = csv_batch(&work, "dupes.csv", "id,name\n5,First\n5,Second\n");

        let destination = merge_destination(&database_path, &["id"]);
        let writer = destination.begin(LoadMode::Merge).expect("begin session");
        writer.write_chunk(&batch).expect("stage chunk");
        let failure = writer.commit().err().expect("duplicate keys fail");

        assert_eq!(failure.failure.code, "duplicate_merge_keys");
        assert_eq!(
            failure.failure.message,
            format!(
                "2 surviving records share a merge key tuple with another surviving \
                 record for DuckDB table customers in {}",
                database_path.display()
            )
        );
        assert_eq!(failure.written_records, 0);
        assert_eq!(failure.committed_chunks, 0);
        assert_eq!(
            failure.facts.report_value(),
            serde_json::json!({
                "atomicity": "atomic",
                "strategy": "transactional_merge"
            })
        );
        // The rollback left the destination exactly as seeded.
        let read_back = read_single_duckdb_batch(&database_path, "customers");
        assert_eq!(read_back.num_rows(), 1);
        assert_eq!(strings(&read_back, 1).value(0), "Ada");
    }

    #[test]
    fn duckdb_merge_session_requires_an_existing_table() {
        let work = TempDir::new().expect("tempdir");
        let database_path = work.path().join("customers.duckdb");

        let destination = merge_destination(&database_path, &["id"]);
        let failure = destination
            .begin(LoadMode::Merge)
            .err()
            .expect("merge into a missing table fails at begin");

        assert_eq!(failure.failure.code, "destination_write_failed");
        assert!(
            failure.failure.message.contains("before merge"),
            "message names the merge probe: {}",
            failure.failure.message
        );
        assert_eq!(
            failure.facts.report_value(),
            serde_json::json!({ "atomicity": "not_applicable" })
        );
    }

    #[test]
    fn duckdb_merge_abandon_commits_nothing() {
        let work = TempDir::new().expect("tempdir");
        let database_path = work.path().join("customers.duckdb");
        seed_duckdb_table(
            &database_path,
            "CREATE TABLE customers (id BIGINT, name VARCHAR); \
             INSERT INTO customers VALUES (1, 'Ada')",
        );
        let batch = csv_batch(&work, "abandoned.csv", "id,name\n2,Grace\n");

        let destination = merge_destination(&database_path, &["id"]);
        let writer = destination.begin(LoadMode::Merge).expect("begin session");
        writer.write_chunk(&batch).expect("stage chunk");
        let abandoned = writer.abandon();

        assert_eq!(abandoned.committed_chunks, 0);
        assert_eq!(abandoned.written_records, 0);
        let read_back = read_single_duckdb_batch(&database_path, "customers");
        assert_eq!(read_back.num_rows(), 1);
    }

    #[test]
    fn csv_merge_key_nulls_reject_records_and_unknown_keys_fail_the_read() {
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.csv");
        fs::write(&source_path, "id,name\n1,Ada\n,Grace\n").expect("write csv");
        let source = LocalFileSource {
            path: source_path,
            format: Some("csv".to_string()),
        };

        // The empty cell in the key field is the pipeline's existing null:
        // the record rejects as `null_merge_key` and the survivor still
        // shapes the batch.
        let read = read_whole_with_merge_keys(
            &source,
            &schema::MergeKeys::from_names(vec!["id".to_string()]),
            &SchemaDirective::inferred(),
        );
        assert_eq!(read.source_rows, 2);
        assert_eq!(read.rejected_count, 1);
        assert_eq!(read.batch.num_rows(), 1);
        assert_eq!(read.rejected[0]["code"], "null_merge_key");
        assert_eq!(read.rejected[0]["field"], "id");
        assert_eq!(read.rejected[0]["message"], "merge key \"id\" is null");

        // A key naming no dataset field fails the read side-effect-free.
        let (failure, rejected) = read_whole_failure_with_merge_keys(
            &source,
            &schema::MergeKeys::from_names(vec!["customer_id".to_string()]),
            &SchemaDirective::inferred(),
        );
        assert_eq!(failure.failure.code, "unknown_merge_key_field");
        assert_eq!(
            failure.failure.message,
            "merge keys name fields absent from the resolved dataset schema: customer_id"
        );
        assert!(rejected.is_empty());
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
        DestinationDefinition::File(crate::FileDestinationDefinition {
            connector: connector.to_string(),
            path: PathBuf::from(path),
        })
    }

    /// A complete minimal sqlserver block: every required key present, every
    /// optional key absent, so tests mutate exactly the key they exercise.
    fn sqlserver_definition() -> SqlServerDestinationDefinition {
        SqlServerDestinationDefinition {
            connector: "sqlserver".to_string(),
            host: Some("db.example.internal".to_string()),
            port: None,
            database: Some("analytics".to_string()),
            schema: None,
            user: Some("loader".to_string()),
            password_env: Some("MSSQL_PASSWORD".to_string()),
            encryption: None,
            trust_server_certificate: None,
            accept_datetime_rounding: None,
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
