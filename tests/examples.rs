//! Runnable examples, executed.
//!
//! Every directory under `examples/` is a self-contained runnable example: a
//! fixture, one or more load definitions, and a `README.md` stating what the
//! example demonstrates. This test is what keeps them runnable — each example is
//! copied into a temp directory, loaded with the real binary, and checked
//! against the load report facts its README states, down to the write atomicity
//! and strategy the prose names. The two negative examples are checked the same
//! way: their documented failure is the expected outcome.

use assert_cmd::Command;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value;
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// Where an example's loads write, so the test can count what landed.
enum Destination {
    DuckDb {
        path: &'static str,
        dataset: &'static str,
    },
    Parquet {
        path: &'static str,
    },
}

/// The report's `row_counts`, named at the call site as the report names them.
struct RowCounts {
    source: u64,
    written: u64,
    rejected: u64,
}

/// The report's `destination_write`: the write atomicity and the strategy that
/// reached it, which a load that never got to its destination has neither of —
/// plus, for a committed merge, the `updated`/`inserted` partition its README
/// states.
struct DestinationWrite {
    atomicity: &'static str,
    strategy: Option<&'static str>,
    merge: Option<(u64, u64)>,
}

impl DestinationWrite {
    fn atomic(strategy: &'static str) -> Self {
        Self {
            atomicity: "atomic",
            strategy: Some(strategy),
            merge: None,
        }
    }

    fn merged(updated: u64, inserted: u64) -> Self {
        Self {
            atomicity: "atomic",
            strategy: Some("transactional_merge"),
            merge: Some((updated, inserted)),
        }
    }

    fn best_effort(strategy: &'static str) -> Self {
        Self {
            atomicity: "best_effort",
            strategy: Some(strategy),
            merge: None,
        }
    }

    fn not_applicable() -> Self {
        Self {
            atomicity: "not_applicable",
            strategy: None,
            merge: None,
        }
    }
}

/// The report's `execution`: how the load ran, for the examples whose README
/// documents it.
struct Execution {
    batch_count: u64,
    chunk_rows: u64,
    parallelism: u64,
    connector_parallelism_limit: u64,
    retry: Retry,
}

/// The report's `execution.retry`: the policy echo, as the report names it.
struct Retry {
    max_attempts: u64,
    initial_delay_ms: u64,
    max_delay_ms: u64,
}

struct Example {
    /// The directory name under `examples/`.
    name: &'static str,
    destination: Destination,
    /// The documented loads, in the order the README tells a reader to run them.
    loads: Vec<Load>,
}

/// The load report facts one documented load must produce.
struct Load {
    definition: &'static str,
    /// `error_summary.code`; `None` for a load the README documents as
    /// succeeding.
    failure_code: Option<&'static str>,
    row_counts: RowCounts,
    write: Option<DestinationWrite>,
    schema_mode: &'static str,
    drift_status: &'static str,
    /// `schema_decision.fields` as `(name, type)` pairs in output order; empty
    /// where the example is not about the resolved shape.
    fields: &'static [(&'static str, &'static str)],
    /// The field names the drift outcome names: the fields added to the pin when
    /// `drift_status` is `additive_fields_added`, and the added fields the
    /// policy refused when it is `failed_on_drift`.
    drift_fields: &'static [&'static str],
    /// The rejection codes `rejected-records.jsonl` carries, sorted.
    rejection_codes: &'static [&'static str],
    /// Records the destination dataset holds once this load has finished.
    destination_records: u64,
    /// Part files a Parquet dataset directory holds afterwards, for the examples
    /// whose README counts them.
    destination_parts: Option<u64>,
    /// `execution`, where the README states how the load ran.
    execution: Option<Execution>,
}

impl Load {
    fn succeeds(definition: &'static str) -> Self {
        Self {
            definition,
            failure_code: None,
            row_counts: RowCounts {
                source: 0,
                written: 0,
                rejected: 0,
            },
            write: None,
            schema_mode: "inferred",
            drift_status: "not_applicable",
            fields: &[],
            drift_fields: &[],
            rejection_codes: &[],
            destination_records: 0,
            destination_parts: None,
            execution: None,
        }
    }

    fn fails(definition: &'static str, failure_code: &'static str) -> Self {
        Self {
            failure_code: Some(failure_code),
            ..Self::succeeds(definition)
        }
    }

    fn row_counts(mut self, row_counts: RowCounts) -> Self {
        self.row_counts = row_counts;
        self
    }

    fn write(mut self, write: DestinationWrite) -> Self {
        self.write = Some(write);
        self
    }

    fn schema(mut self, mode: &'static str, drift_status: &'static str) -> Self {
        self.schema_mode = mode;
        self.drift_status = drift_status;
        self
    }

    fn fields(mut self, fields: &'static [(&'static str, &'static str)]) -> Self {
        self.fields = fields;
        self
    }

    fn drift_fields(mut self, drift_fields: &'static [&'static str]) -> Self {
        self.drift_fields = drift_fields;
        self
    }

    fn rejection_codes(mut self, rejection_codes: &'static [&'static str]) -> Self {
        self.rejection_codes = rejection_codes;
        self
    }

    fn destination_records(mut self, destination_records: u64) -> Self {
        self.destination_records = destination_records;
        self
    }

    fn destination_parts(mut self, destination_parts: u64) -> Self {
        self.destination_parts = Some(destination_parts);
        self
    }

    fn execution(mut self, execution: Execution) -> Self {
        self.execution = Some(execution);
        self
    }
}

/// Every example and the outcome each of its documented loads must produce. A
/// directory added under `examples/` without an entry here fails
/// `every_example_directory_is_declared_and_self_contained`.
fn examples() -> Vec<Example> {
    vec![
        Example {
            name: "csv-to-duckdb-full-refresh",
            destination: Destination::DuckDb {
                path: "customers.duckdb",
                dataset: "customers",
            },
            loads: vec![Load::succeeds("customers-load.yml")
                .row_counts(RowCounts {
                    source: 3,
                    written: 3,
                    rejected: 0,
                })
                .write(DestinationWrite::atomic("transactional_replace"))
                .fields(&[
                    ("customer_id", "int64"),
                    ("name", "utf8"),
                    ("signup_date", "utf8"),
                    ("total_spend", "float64"),
                ])
                .destination_records(3)],
        },
        Example {
            name: "csv-to-parquet-append",
            destination: Destination::Parquet {
                path: "events-dataset",
            },
            loads: vec![
                Load::succeeds("load-day-1.yml")
                    .row_counts(RowCounts {
                        source: 3,
                        written: 3,
                        rejected: 0,
                    })
                    .write(DestinationWrite::best_effort("staged_part_append"))
                    .destination_records(3),
                // Append adds day 2 without changing day 1's records.
                Load::succeeds("load-day-2.yml")
                    .row_counts(RowCounts {
                        source: 2,
                        written: 2,
                        rejected: 0,
                    })
                    .write(DestinationWrite::best_effort("staged_part_append"))
                    .destination_records(5),
            ],
        },
        Example {
            name: "jsonl-to-duckdb-append",
            destination: Destination::DuckDb {
                path: "analytics.duckdb",
                dataset: "orders",
            },
            loads: vec![
                // A DuckDB append writes into a table that exists, so the first
                // load is the full refresh that creates it.
                Load::succeeds("load-day-1.yml")
                    .row_counts(RowCounts {
                        source: 3,
                        written: 3,
                        rejected: 0,
                    })
                    .write(DestinationWrite::atomic("transactional_replace"))
                    .destination_records(3),
                Load::succeeds("load-day-2.yml")
                    .row_counts(RowCounts {
                        source: 2,
                        written: 2,
                        rejected: 0,
                    })
                    .write(DestinationWrite::best_effort("insert"))
                    .destination_records(5),
            ],
        },
        Example {
            name: "csv-to-duckdb-merge",
            destination: Destination::DuckDb {
                path: "crm.duckdb",
                dataset: "customers",
            },
            loads: vec![
                // A merge writes into a table that exists, so the first load
                // is the full refresh that creates it.
                Load::succeeds("load-day-1.yml")
                    .row_counts(RowCounts {
                        source: 3,
                        written: 3,
                        rejected: 0,
                    })
                    .write(DestinationWrite::atomic("transactional_replace"))
                    .destination_records(3),
                // Day 2 updates customer 2 whole and inserts customer 4;
                // customers 1 and 3 stay — merge never deletes.
                Load::succeeds("load-day-2.yml")
                    .row_counts(RowCounts {
                        source: 2,
                        written: 2,
                        rejected: 0,
                    })
                    .write(DestinationWrite::merged(1, 1))
                    .destination_records(4),
            ],
        },
        Example {
            name: "jsonl-to-parquet-full-refresh",
            destination: Destination::Parquet {
                path: "inventory-dataset",
            },
            loads: vec![
                Load::succeeds("load.yml")
                    .row_counts(RowCounts {
                        source: 3,
                        written: 3,
                        rejected: 0,
                    })
                    .write(DestinationWrite::best_effort("staging_then_replace"))
                    .destination_records(3),
                // Full refresh replaces the dataset, so loading the same
                // definition again leaves three records, not six.
                Load::succeeds("load.yml")
                    .row_counts(RowCounts {
                        source: 3,
                        written: 3,
                        rejected: 0,
                    })
                    .write(DestinationWrite::best_effort("staging_then_replace"))
                    .destination_records(3),
            ],
        },
        Example {
            name: "pinned-schema-additive-drift",
            destination: Destination::Parquet {
                path: "shipments-dataset",
            },
            loads: vec![
                // The first load bootstraps the pin from its own inference.
                Load::succeeds("load-day-1.yml")
                    .row_counts(RowCounts {
                        source: 3,
                        written: 3,
                        rejected: 0,
                    })
                    .write(DestinationWrite::best_effort("staging_then_replace"))
                    .fields(&[
                        ("shipment_id", "int64"),
                        ("city", "utf8"),
                        ("weight_kg", "float64"),
                    ])
                    .destination_records(3),
                // Day 2 carries one added nullable field, which the policy
                // admits and the pin is extended to carry.
                Load::succeeds("load-day-2.yml")
                    .row_counts(RowCounts {
                        source: 2,
                        written: 2,
                        rejected: 0,
                    })
                    .write(DestinationWrite::best_effort("staging_then_replace"))
                    .schema("pinned", "additive_fields_added")
                    .fields(&[
                        ("shipment_id", "int64"),
                        ("city", "utf8"),
                        ("weight_kg", "float64"),
                        ("carrier", "utf8"),
                    ])
                    .drift_fields(&["carrier"])
                    .destination_records(2),
            ],
        },
        Example {
            name: "pinned-schema-fail-on-drift",
            destination: Destination::DuckDb {
                path: "billing.duckdb",
                dataset: "invoices",
            },
            loads: vec![
                Load::succeeds("load-day-1.yml")
                    .row_counts(RowCounts {
                        source: 2,
                        written: 2,
                        rejected: 0,
                    })
                    .write(DestinationWrite::atomic("transactional_replace"))
                    .destination_records(2),
                // The added field the additive example admits fails here, and it
                // fails before the destination is touched: no write was
                // attempted, no record counts were reached, and the table still
                // holds day 1.
                Load::fails("load-day-2.yml", "schema_drift")
                    .row_counts(RowCounts {
                        source: 0,
                        written: 0,
                        rejected: 0,
                    })
                    .write(DestinationWrite::not_applicable())
                    .schema("pinned", "failed_on_drift")
                    .drift_fields(&["currency"])
                    .destination_records(2),
            ],
        },
        Example {
            name: "structural-transform",
            destination: Destination::DuckDb {
                path: "analytics.duckdb",
                dataset: "orders",
            },
            loads: vec![Load::succeeds("load.yml")
                .row_counts(RowCounts {
                    source: 3,
                    written: 3,
                    rejected: 0,
                })
                .write(DestinationWrite::atomic("transactional_replace"))
                .fields(&[
                    ("order_id", "int64"),
                    ("customer_id", "int64"),
                    ("customer_name", "utf8"),
                    ("amount", "float64"),
                    ("placed_on", "utf8"),
                ])
                .destination_records(3)],
        },
        Example {
            name: "declared-types",
            destination: Destination::DuckDb {
                path: "payments.duckdb",
                dataset: "payments",
            },
            loads: vec![Load::succeeds("load.yml")
                .row_counts(RowCounts {
                    source: 3,
                    written: 3,
                    rejected: 0,
                })
                .write(DestinationWrite::atomic("transactional_replace"))
                .fields(&[
                    ("payment_id", "int64"),
                    ("paid_at", "timestamp"),
                    ("recorded_at", "timestamptz"),
                    ("amount", "decimal(12,2)"),
                ])
                .destination_records(3)],
        },
        Example {
            name: "rejected-records",
            destination: Destination::Parquet {
                path: "measurements-dataset",
            },
            loads: vec![Load::succeeds("load.yml")
                .row_counts(RowCounts {
                    source: 4,
                    written: 2,
                    rejected: 2,
                })
                .write(DestinationWrite::best_effort("staging_then_replace"))
                .fields(&[
                    ("station", "utf8"),
                    ("reading", "float64"),
                    ("taken_on", "utf8"),
                ])
                .rejection_codes(&["missing_required_field", "type_coercion_failed"])
                .destination_records(2)],
        },
        Example {
            name: "chunked-execution",
            destination: Destination::Parquet {
                path: "readings-dataset",
            },
            // Five records under a chunk bound of two commit as three chunks,
            // and an append commits one part file per chunk.
            loads: vec![Load::succeeds("load.yml")
                .row_counts(RowCounts {
                    source: 5,
                    written: 5,
                    rejected: 0,
                })
                .write(DestinationWrite::best_effort("staged_part_append"))
                .execution(Execution {
                    batch_count: 3,
                    chunk_rows: 2,
                    // The definition asks for 4 and the connector's limit for
                    // this mode is 1, so the clamp is what ran.
                    parallelism: 1,
                    connector_parallelism_limit: 1,
                    retry: Retry {
                        max_attempts: 5,
                        initial_delay_ms: 100,
                        max_delay_ms: 1000,
                    },
                })
                .destination_records(5)
                .destination_parts(3)],
        },
    ]
}

#[test]
fn every_example_loads_as_its_readme_documents() {
    for example in examples() {
        run_example(&example);
    }
}

/// The directory listing is the source of truth for what exists; this test keeps
/// the table above and the READMEs honest about it.
#[test]
fn every_example_directory_is_declared_and_self_contained() {
    let examples = examples();

    let mut directories = fs::read_dir(examples_dir())
        .expect("examples directory")
        .map(|entry| entry.expect("examples entry").path())
        .filter(|path| path.is_dir())
        .map(|path| file_name(&path))
        .collect::<Vec<_>>();
    directories.sort();
    let mut declared = examples
        .iter()
        .map(|example| example.name.to_string())
        .collect::<Vec<_>>();
    declared.sort();
    assert_eq!(
        directories, declared,
        "every examples/ directory must be declared in this test, and every \
         declared example must exist"
    );

    assert!(
        examples_dir().join("README.md").is_file(),
        "examples/README.md indexes the examples"
    );

    for example in &examples {
        let example_dir = examples_dir().join(example.name);
        let readme_path = example_dir.join("README.md");
        let readme = fs::read_to_string(&readme_path)
            .unwrap_or_else(|_| panic!("{}: README.md states what it demonstrates", example.name));

        let mut definitions = example
            .loads
            .iter()
            .map(|load| load.definition.to_string())
            .collect::<Vec<_>>();
        definitions.sort();
        definitions.dedup();
        let mut yaml_files = example_files(&example_dir)
            .iter()
            .filter(|path| {
                matches!(
                    path.extension().and_then(|extension| extension.to_str()),
                    Some("yml") | Some("yaml")
                )
            })
            .map(|path| file_name(path))
            .collect::<Vec<_>>();
        yaml_files.sort();
        assert_eq!(
            yaml_files, definitions,
            "{}: every load definition in the directory must be loaded by this \
             test, and every declared load must name a definition that exists",
            example.name
        );

        for definition in &definitions {
            assert!(
                readme.contains(definition.as_str()),
                "{}: README.md must name the {definition} it documents",
                example.name
            );
        }
    }
}

/// The quickstart is the front page's promise, so it is the example the test
/// suite loads, byte for byte.
#[test]
fn the_readme_quickstart_is_the_csv_to_duckdb_example() {
    let readme = fs::read_to_string(repository_root().join("README.md")).expect("README.md");
    let example_dir = examples_dir().join("csv-to-duckdb-full-refresh");

    assert!(
        readme.contains("examples/csv-to-duckdb-full-refresh"),
        "the quickstart must point at the example that proves it"
    );
    for file in ["customers.csv", "customers-load.yml"] {
        let example_file =
            fs::read_to_string(example_dir.join(file)).unwrap_or_else(|_| panic!("example {file}"));
        assert_eq!(
            heredoc_body(&readme, file),
            example_file,
            "the README quickstart must write the same {file} as the example holds"
        );
    }
}

fn run_example(example: &Example) {
    let source_dir = examples_dir().join(example.name);
    let tracked_files = directory_snapshot(&source_dir);

    // Examples are loaded from a copy, so a destination, an artifact directory,
    // and a bootstrapped pinned schema file all land outside the repository
    // tree.
    let work = TempDir::new().expect("tempdir");
    let example_dir = work.path().join(example.name);
    copy_directory(&source_dir, &example_dir);

    for (index, load) in example.loads.iter().enumerate() {
        let context = format!("{}: load {} ({})", example.name, index + 1, load.definition);
        let artifacts_dir = work.path().join(format!("artifacts-{}", index + 1));
        let assert = Command::cargo_bin("data-spark")
            .expect("binary")
            .current_dir(&example_dir)
            .arg("load")
            .arg("--output-dir")
            .arg(&artifacts_dir)
            .arg(load.definition)
            .assert();
        let assert = match load.failure_code {
            None => assert.success(),
            // A failed load exits 1, so assert the code the contract names
            // rather than any nonzero one: the process and the report's
            // `process_exit_code` have to keep agreeing.
            Some(_) => assert.failure().code(1),
        };
        let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
        let report = read_single_report(&artifacts_dir, &context);

        assert_report(&context, &stdout, &report, load);
        assert_rejected_records(&context, &example_dir, &report, load);
        assert_eq!(
            destination_records(&example_dir, &example.destination),
            load.destination_records,
            "{context}: records the destination dataset holds afterwards"
        );
        if let Some(parts) = load.destination_parts {
            assert_eq!(
                destination_parts(&example_dir, &example.destination),
                Some(parts),
                "{context}: part files the dataset directory holds afterwards"
            );
        }
    }

    assert_eq!(
        directory_snapshot(&source_dir),
        tracked_files,
        "{}: loading an example must leave the repository tree untouched",
        example.name
    );
}

fn assert_report(context: &str, stdout: &str, report: &Value, load: &Load) {
    assert_eq!(report["report_version"], 1, "{context}");
    assert_eq!(
        report["binary_version"],
        env!("CARGO_PKG_VERSION"),
        "{context}: the report names the binary version that wrote it"
    );
    match load.failure_code {
        None => {
            assert_eq!(report["exit_status"], "succeeded", "{context}");
            assert_eq!(report["process_exit_code"], 0, "{context}");
            assert!(
                report["error_summary"].is_null(),
                "{context}: a succeeding load carries no error summary"
            );
            assert!(stdout.contains("Status: succeeded"), "{context}");
        }
        Some(code) => {
            assert_eq!(report["exit_status"], "failed", "{context}");
            assert_eq!(report["process_exit_code"], 1, "{context}");
            assert_eq!(
                report["error_summary"]["code"], code,
                "{context}: the documented failure code"
            );
            assert!(stdout.contains("Status: failed"), "{context}");
        }
    }

    assert_eq!(
        report["row_counts"]["source"], load.row_counts.source,
        "{context}: records read"
    );
    assert_eq!(
        report["row_counts"]["written"], load.row_counts.written,
        "{context}: records written"
    );
    assert_eq!(
        report["row_counts"]["rejected"], load.row_counts.rejected,
        "{context}: records rejected"
    );

    if let Some(write) = &load.write {
        assert_eq!(
            report["destination_write"]["atomicity"], write.atomicity,
            "{context}: the write atomicity the README names"
        );
        match write.strategy {
            Some(strategy) => assert_eq!(
                report["destination_write"]["strategy"], strategy,
                "{context}: the write strategy the README names"
            ),
            None => assert!(
                report["destination_write"]["strategy"].is_null(),
                "{context}: a load that reached no destination write has no strategy"
            ),
        }
        match write.merge {
            Some((updated, inserted)) => assert_eq!(
                report["destination_write"]["merge"],
                serde_json::json!({ "updated": updated, "inserted": inserted }),
                "{context}: the merge partition the README names"
            ),
            // Truly absent, not `null` by another name: indexing would
            // conflate the two, and the contract promises absence.
            None => assert!(
                report["destination_write"].get("merge").is_none(),
                "{context}: only a committed merge reports merge counts"
            ),
        }
    }

    let schema_decision = &report["schema_decision"];
    assert_eq!(
        schema_decision["mode"], load.schema_mode,
        "{context}: how the schema was decided"
    );
    assert_eq!(
        schema_decision["drift_status"], load.drift_status,
        "{context}: the drift outcome"
    );
    if !load.fields.is_empty() {
        assert_eq!(
            field_pairs(&schema_decision["fields"], context),
            load.fields
                .iter()
                .map(|(name, field_type)| (name.to_string(), field_type.to_string()))
                .collect::<Vec<_>>(),
            "{context}: the dataset schema in output order"
        );
    }
    if let Some(execution) = &load.execution {
        let reported = &report["execution"];
        assert_eq!(
            reported["record_format"], "arrow_record_batch",
            "{context}: a load that reached the write phase exchanged record batches"
        );
        assert_eq!(
            reported["batch_count"], execution.batch_count,
            "{context}: the chunks the destination committed"
        );
        assert_eq!(
            reported["chunk_rows"], execution.chunk_rows,
            "{context}: the effective chunk bound"
        );
        assert_eq!(
            reported["parallelism"], execution.parallelism,
            "{context}: the effective load parallelism"
        );
        assert_eq!(
            reported["connector_parallelism_limit"], execution.connector_parallelism_limit,
            "{context}: the connector's parallelism limit for this load mode"
        );
        let retry = &reported["retry"];
        assert_eq!(
            retry["max_attempts"], execution.retry.max_attempts,
            "{context}: the attempts the retry policy allows per retry unit"
        );
        assert_eq!(
            retry["initial_delay_ms"], execution.retry.initial_delay_ms,
            "{context}: the retry policy's first backoff"
        );
        assert_eq!(
            retry["max_delay_ms"], execution.retry.max_delay_ms,
            "{context}: the retry policy's backoff ceiling"
        );
        // No shipped connector classifies a failure transient, so nothing is
        // ever retried.
        assert_eq!(
            retry["attempts"].as_array().map(Vec::len),
            Some(0),
            "{context}: no attempt was retried"
        );
    }

    if !load.drift_fields.is_empty() {
        let named = match load.drift_status {
            "additive_fields_added" => field_pairs(&schema_decision["added_fields"], context)
                .into_iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>(),
            "failed_on_drift" => schema_decision["drift"]["added_fields"]
                .as_array()
                .unwrap_or_else(|| panic!("{context}: drift detail names the added fields"))
                .iter()
                .map(|name| name.as_str().expect("drift field name").to_string())
                .collect::<Vec<_>>(),
            other => panic!("{context}: {other} names no drift fields"),
        };
        assert_eq!(
            named,
            load.drift_fields
                .iter()
                .map(|name| name.to_string())
                .collect::<Vec<_>>(),
            "{context}: the fields the drift outcome names"
        );
    }
}

fn assert_rejected_records(context: &str, example_dir: &Path, report: &Value, load: &Load) {
    assert_eq!(
        report["rejected_records"]["count"], load.row_counts.rejected,
        "{context}: the rejected-record count mirrors row_counts.rejected"
    );

    let artifact = &report["rejected_records"]["artifact"];
    if load.row_counts.rejected == 0 {
        assert!(
            artifact.is_null(),
            "{context}: no rejected records means no artifact"
        );
        return;
    }

    // The report states the path as the load resolved it, so joining covers both
    // the relative default and the absolute path `--output-dir` produces here.
    let artifact_path = example_dir.join(
        artifact
            .as_str()
            .unwrap_or_else(|| panic!("{context}: the rejected-records artifact path")),
    );
    let mut codes = fs::read_to_string(&artifact_path)
        .unwrap_or_else(|_| panic!("{context}: read {}", artifact_path.display()))
        .lines()
        .map(|line| {
            let rejection = serde_json::from_str::<Value>(line).expect("artifact line is json");
            // Every rejected record can be traced back to the source line it
            // came from, with the content the load could recover.
            assert!(
                rejection["line"].as_u64().is_some_and(|line| line > 0),
                "{context}: a rejected record states its source line number"
            );
            assert!(
                rejection["message"]
                    .as_str()
                    .is_some_and(|message| !message.is_empty()),
                "{context}: a rejected record states why it was rejected"
            );
            assert!(
                rejection["record"].is_object(),
                "{context}: a rejected record carries the record content"
            );
            rejection["code"]
                .as_str()
                .expect("rejection code")
                .to_string()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        codes.len() as u64,
        load.row_counts.rejected,
        "{context}: one artifact line per rejected record"
    );
    if !load.rejection_codes.is_empty() {
        codes.sort();
        assert_eq!(
            codes,
            load.rejection_codes
                .iter()
                .map(|code| code.to_string())
                .collect::<Vec<_>>(),
            "{context}: the documented rejection codes"
        );
    }
}

fn field_pairs(fields: &Value, context: &str) -> Vec<(String, String)> {
    fields
        .as_array()
        .unwrap_or_else(|| panic!("{context}: the report states the fields"))
        .iter()
        .map(|field| {
            (
                field["name"].as_str().expect("field name").to_string(),
                field["type"].as_str().expect("field type").to_string(),
            )
        })
        .collect()
}

fn destination_records(example_dir: &Path, destination: &Destination) -> u64 {
    match destination {
        Destination::DuckDb { path, dataset } => duckdb_records(&example_dir.join(path), dataset),
        Destination::Parquet { path } => parquet_records(&example_dir.join(path)),
    }
}

/// The part files the destination holds, where the destination is made of part
/// files at all — a DuckDB table has none to count.
fn destination_parts(example_dir: &Path, destination: &Destination) -> Option<u64> {
    match destination {
        Destination::DuckDb { .. } => None,
        Destination::Parquet { path } => {
            Some(parquet_part_files(&example_dir.join(path)).len() as u64)
        }
    }
}

fn duckdb_records(database_path: &Path, dataset: &str) -> u64 {
    let connection = duckdb::Connection::open(database_path).expect("open duckdb database");
    let mut statement = connection
        .prepare(&format!(
            "SELECT count(*) FROM \"{}\"",
            dataset.replace('"', "\"\"")
        ))
        .expect("prepare duckdb count");
    let counts = statement
        .query_map([], |row| row.get::<_, i64>(0))
        .expect("query duckdb count")
        .collect::<Result<Vec<_>, _>>()
        .expect("read duckdb count");
    assert_eq!(counts.len(), 1, "one count row");
    counts[0] as u64
}

fn parquet_records(destination_path: &Path) -> u64 {
    parquet_part_files(destination_path)
        .iter()
        .map(|path| {
            let file = File::open(path).expect("open parquet file");
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("parquet reader");
            builder.metadata().file_metadata().num_rows() as u64
        })
        .sum()
}

/// The part files a Parquet dataset directory holds, in name order.
fn parquet_part_files(destination_path: &Path) -> Vec<PathBuf> {
    let mut files = fs::read_dir(destination_path)
        .expect("parquet destination directory")
        .map(|entry| entry.expect("destination entry").path())
        .filter(|path| path.extension().and_then(|extension| extension.to_str()) == Some("parquet"))
        .collect::<Vec<_>>();
    files.sort();
    for path in &files {
        assert!(
            file_name(path).starts_with("part-"),
            "records land as part-*.parquet files: {}",
            path.display()
        );
    }
    files
}

fn read_single_report(artifacts_dir: &Path, context: &str) -> Value {
    let artifact_dirs = fs::read_dir(artifacts_dir)
        .expect("artifact root")
        .map(|entry| entry.expect("artifact entry").path())
        .collect::<Vec<_>>();
    assert_eq!(
        artifact_dirs.len(),
        1,
        "{context}: a load writes one artifact directory"
    );
    let report_path = artifact_dirs[0].join("load-report.json");
    serde_json::from_slice(&fs::read(&report_path).expect("load report")).expect("json report")
}

/// The files an example is made of. An example directory holds nothing else, so
/// a load's leftovers have to be cleaned up before this test can pass.
fn example_files(directory: &Path) -> Vec<PathBuf> {
    let mut files = fs::read_dir(directory)
        .expect("example directory")
        .map(|entry| entry.expect("example entry").path())
        .collect::<Vec<_>>();
    files.sort();
    for path in &files {
        assert!(
            path.is_file(),
            "an example holds only its own files, so a load's leftovers have to \
             be cleaned up: {}",
            path.display()
        );
    }
    files
}

/// The example's files and their contents, so the test can prove a load changed
/// none of them.
fn directory_snapshot(directory: &Path) -> Vec<(String, String)> {
    example_files(directory)
        .iter()
        .map(|path| {
            (
                file_name(path),
                fs::read_to_string(path).expect("example file is text"),
            )
        })
        .collect()
}

fn copy_directory(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).expect("create example copy");
    for path in example_files(source) {
        fs::copy(&path, destination.join(file_name(&path))).expect("copy example file");
    }
}

/// The body a `cat > <file> <<'EOF'` block in the README writes.
fn heredoc_body(readme: &str, file: &str) -> String {
    let opening = format!("cat > {file} <<'EOF'\n");
    let start = readme
        .find(&opening)
        .unwrap_or_else(|| panic!("the README writes {file} with a heredoc"))
        + opening.len();
    let body = &readme[start..];
    let end = body
        .find("\nEOF\n")
        .unwrap_or_else(|| panic!("the {file} heredoc in the README is terminated"));
    body[..=end].to_string()
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .expect("file name")
        .to_string()
}

fn examples_dir() -> PathBuf {
    repository_root().join("examples")
}

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}
