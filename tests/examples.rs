//! Runnable examples, executed.
//!
//! Every directory under `examples/` is a self-contained runnable example: a
//! fixture, one or more load definitions, and a `README.md` stating what the
//! example demonstrates. This test is what keeps them runnable — each example
//! is copied into a temp directory, run against the real binary, and checked
//! against the load report facts its README documents. The two negative
//! examples are checked the same way: their documented failure is the expected
//! outcome.

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

struct Example {
    /// The directory name under `examples/`.
    name: &'static str,
    destination: Destination,
    /// The documented runs, in the order the README tells a reader to run them.
    runs: Vec<Run>,
}

/// The load report facts one documented run must produce.
struct Run {
    definition: &'static str,
    /// `error_summary.code`; `None` for a run the README documents as succeeding.
    failure_code: Option<&'static str>,
    source_records: u64,
    written_records: u64,
    rejected_records: u64,
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
    /// Records the destination dataset holds once this run has finished.
    destination_records: u64,
}

impl Run {
    fn succeeds(definition: &'static str) -> Self {
        Self {
            definition,
            failure_code: None,
            source_records: 0,
            written_records: 0,
            rejected_records: 0,
            schema_mode: "inferred",
            drift_status: "not_applicable",
            fields: &[],
            drift_fields: &[],
            rejection_codes: &[],
            destination_records: 0,
        }
    }

    fn fails(definition: &'static str, failure_code: &'static str) -> Self {
        Self {
            failure_code: Some(failure_code),
            ..Self::succeeds(definition)
        }
    }

    fn records(mut self, source: u64, written: u64, rejected: u64) -> Self {
        self.source_records = source;
        self.written_records = written;
        self.rejected_records = rejected;
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
}

/// Every example and the outcome each of its documented runs must produce.
/// A directory added under `examples/` without an entry here fails
/// `every_example_directory_is_declared_and_self_contained`.
fn examples() -> Vec<Example> {
    vec![
        Example {
            name: "csv-to-duckdb-full-refresh",
            destination: Destination::DuckDb {
                path: "customers.duckdb",
                dataset: "customers",
            },
            runs: vec![Run::succeeds("customers-load.yml")
                .records(3, 3, 0)
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
            runs: vec![
                Run::succeeds("load-day-1.yml")
                    .records(3, 3, 0)
                    .destination_records(3),
                // Append adds day 2 without touching day 1's records.
                Run::succeeds("load-day-2.yml")
                    .records(2, 2, 0)
                    .destination_records(5),
            ],
        },
        Example {
            name: "jsonl-to-duckdb-append",
            destination: Destination::DuckDb {
                path: "analytics.duckdb",
                dataset: "orders",
            },
            runs: vec![
                Run::succeeds("load-day-1.yml")
                    .records(3, 3, 0)
                    .destination_records(3),
                Run::succeeds("load-day-2.yml")
                    .records(2, 2, 0)
                    .destination_records(5),
            ],
        },
        Example {
            name: "jsonl-to-parquet-full-refresh",
            destination: Destination::Parquet {
                path: "inventory-dataset",
            },
            runs: vec![
                Run::succeeds("load.yml")
                    .records(3, 3, 0)
                    .destination_records(3),
                // Full refresh replaces the dataset, so a second run of the
                // same definition leaves three records, not six.
                Run::succeeds("load.yml")
                    .records(3, 3, 0)
                    .destination_records(3),
            ],
        },
        Example {
            name: "pinned-schema-additive-drift",
            destination: Destination::Parquet {
                path: "shipments-dataset",
            },
            runs: vec![
                // The first load bootstraps the pin from its own inference.
                Run::succeeds("load-day-1.yml")
                    .records(3, 3, 0)
                    .fields(&[
                        ("shipment_id", "int64"),
                        ("city", "utf8"),
                        ("weight_kg", "float64"),
                    ])
                    .destination_records(3),
                // Day 2 carries one added nullable field, which the policy
                // admits and the pin is extended to carry.
                Run::succeeds("load-day-2.yml")
                    .records(2, 2, 0)
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
            runs: vec![
                Run::succeeds("load-day-1.yml")
                    .records(2, 2, 0)
                    .destination_records(2),
                // The same added field the additive example admits fails here,
                // before the destination is touched: it still holds day 1.
                Run::fails("load-day-2.yml", "schema_drift")
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
            runs: vec![Run::succeeds("load.yml")
                .records(3, 3, 0)
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
            runs: vec![Run::succeeds("load.yml")
                .records(3, 3, 0)
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
            runs: vec![Run::succeeds("load.yml")
                .records(4, 2, 2)
                .fields(&[
                    ("station", "utf8"),
                    ("reading", "float64"),
                    ("taken_on", "utf8"),
                ])
                .rejection_codes(&["missing_required_field", "type_coercion_failed"])
                .destination_records(2)],
        },
    ]
}

#[test]
fn every_example_runs_as_its_readme_documents() {
    for example in examples() {
        run_example(&example);
    }
}

/// The directory listing is the source of truth for what exists; this test
/// keeps the table above and the READMEs honest about it.
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
            .runs
            .iter()
            .map(|run| run.definition.to_string())
            .collect::<Vec<_>>();
        definitions.sort();
        definitions.dedup();
        let mut yaml_files = fs::read_dir(&example_dir)
            .expect("example directory")
            .map(|entry| entry.expect("example entry").path())
            .filter(|path| {
                matches!(
                    path.extension().and_then(|extension| extension.to_str()),
                    Some("yml") | Some("yaml")
                )
            })
            .map(|path| file_name(&path))
            .collect::<Vec<_>>();
        yaml_files.sort();
        assert_eq!(
            yaml_files, definitions,
            "{}: every load definition in the directory must be run by this \
             test, and every run must name a definition that exists",
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
/// suite runs, byte for byte.
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

    // Examples run against a copy, so a load's destination, artifacts, and
    // pinned schema file land outside the repository tree.
    let work = TempDir::new().expect("tempdir");
    let example_dir = work.path().join(example.name);
    copy_directory(&source_dir, &example_dir);

    for (index, run) in example.runs.iter().enumerate() {
        let context = format!("{}: run {} ({})", example.name, index + 1, run.definition);
        let artifacts_dir = work.path().join(format!("artifacts-{}", index + 1));
        let assert = Command::cargo_bin("data-spark")
            .expect("binary")
            .current_dir(&example_dir)
            .arg("load")
            .arg("--output-dir")
            .arg(&artifacts_dir)
            .arg(run.definition)
            .assert();
        let assert = match run.failure_code {
            None => assert.success(),
            Some(_) => assert.failure(),
        };
        let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
        let report = read_single_report(&artifacts_dir, &context);

        assert_report(&context, &stdout, &report, run);
        assert_rejected_records(&context, &example_dir, &report, run);
        assert_eq!(
            destination_records(&example_dir, &example.destination),
            run.destination_records,
            "{context}: records the destination dataset holds after this run"
        );
    }

    assert_eq!(
        directory_snapshot(&source_dir),
        tracked_files,
        "{}: running an example must leave the repository tree untouched",
        example.name
    );
}

fn assert_report(context: &str, stdout: &str, report: &Value, run: &Run) {
    assert_eq!(report["report_version"], 1, "{context}");
    match run.failure_code {
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
        report["row_counts"]["source"], run.source_records,
        "{context}: records read"
    );
    assert_eq!(
        report["row_counts"]["written"], run.written_records,
        "{context}: records written"
    );
    assert_eq!(
        report["row_counts"]["rejected"], run.rejected_records,
        "{context}: records rejected"
    );

    let schema_decision = &report["schema_decision"];
    assert_eq!(
        schema_decision["mode"], run.schema_mode,
        "{context}: how the schema was decided"
    );
    assert_eq!(
        schema_decision["drift_status"], run.drift_status,
        "{context}: the drift outcome"
    );
    if !run.fields.is_empty() {
        assert_eq!(
            field_pairs(&schema_decision["fields"], context),
            run.fields
                .iter()
                .map(|(name, field_type)| (name.to_string(), field_type.to_string()))
                .collect::<Vec<_>>(),
            "{context}: the dataset schema in output order"
        );
    }
    if !run.drift_fields.is_empty() {
        let named = match run.drift_status {
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
            run.drift_fields
                .iter()
                .map(|name| name.to_string())
                .collect::<Vec<_>>(),
            "{context}: the fields the drift outcome names"
        );
    }
}

fn assert_rejected_records(context: &str, example_dir: &Path, report: &Value, run: &Run) {
    assert_eq!(
        report["rejected_records"]["count"], run.rejected_records,
        "{context}: the rejected-record count mirrors row_counts.rejected"
    );

    let artifact = &report["rejected_records"]["artifact"];
    if run.rejected_records == 0 {
        assert!(
            artifact.is_null(),
            "{context}: no rejected records means no artifact"
        );
        return;
    }

    let artifact_path = example_dir.join(
        artifact
            .as_str()
            .unwrap_or_else(|| panic!("{context}: the rejected-records artifact path")),
    );
    let mut codes = fs::read_to_string(&artifact_path)
        .unwrap_or_else(|_| panic!("{context}: read {}", artifact_path.display()))
        .lines()
        .map(|line| {
            serde_json::from_str::<Value>(line).expect("artifact line is json")["code"]
                .as_str()
                .expect("rejection code")
                .to_string()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        codes.len() as u64,
        run.rejected_records,
        "{context}: one artifact line per rejected record"
    );
    if !run.rejection_codes.is_empty() {
        codes.sort();
        assert_eq!(
            codes,
            run.rejection_codes
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
    let mut files = fs::read_dir(destination_path)
        .expect("parquet destination directory")
        .map(|entry| entry.expect("destination entry").path())
        .filter(|path| path.extension().and_then(|extension| extension.to_str()) == Some("parquet"))
        .collect::<Vec<_>>();
    files.sort();
    files
        .iter()
        .map(|path| {
            let file = File::open(path).expect("open parquet file");
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("parquet reader");
            builder.metadata().file_metadata().num_rows() as u64
        })
        .sum()
}

fn read_single_report(artifacts_dir: &Path, context: &str) -> Value {
    let run_dirs = fs::read_dir(artifacts_dir)
        .expect("artifact root")
        .map(|entry| entry.expect("artifact entry").path())
        .collect::<Vec<_>>();
    assert_eq!(
        run_dirs.len(),
        1,
        "{context}: a load writes one artifact directory"
    );
    let report_path = run_dirs[0].join("load-report.json");
    serde_json::from_slice(&fs::read(&report_path).expect("load report")).expect("json report")
}

/// The example directory's files and their contents, so the test can prove a
/// run changed none of them.
fn directory_snapshot(directory: &Path) -> Vec<(String, String)> {
    let mut files = fs::read_dir(directory)
        .expect("example directory")
        .map(|entry| entry.expect("example entry").path())
        .map(|path| {
            assert!(
                path.is_file(),
                "an example holds only its own files, so a load's leftovers have \
                 to be cleaned up: {}",
                path.display()
            );
            (
                file_name(&path),
                fs::read_to_string(&path).expect("example file is text"),
            )
        })
        .collect::<Vec<_>>();
    files.sort();
    files
}

fn copy_directory(source: &Path, destination: &Path) {
    fs::create_dir_all(destination).expect("create example copy");
    for (name, contents) in directory_snapshot(source) {
        fs::write(destination.join(name), contents).expect("copy example file");
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
