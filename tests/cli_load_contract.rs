use arrow_array::{
    Array, BooleanArray, Decimal128Array, Float64Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use assert_cmd::Command;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::LogicalType;
use serde_json::Value;
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

#[test]
fn version_flags_print_the_package_version_and_exit_zero() {
    for flag in ["--version", "-V"] {
        let assert = Command::cargo_bin("data-spark")
            .expect("binary")
            .arg(flag)
            .assert()
            .success();
        let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
        assert_eq!(
            stdout.trim(),
            format!("data-spark {}", env!("CARGO_PKG_VERSION")),
            "{flag} names the binary and the version it was built from"
        );
    }
}

#[test]
fn help_describes_the_load_subcommand() {
    let assert = Command::cargo_bin("data-spark")
        .expect("binary")
        .arg("--help")
        .assert()
        .success();
    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let load_line = stdout
        .lines()
        .find(|line| line.trim_start().starts_with("load"))
        .expect("--help lists the load subcommand");
    assert!(
        load_line.contains("Run a load from a YAML load definition"),
        "the load subcommand carries a description, got: {load_line:?}"
    );
}

#[test]
fn local_csv_full_refresh_writes_readable_parquet_directory_report_and_summary() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    fs::write(
        &source_path,
        "customer_id,name,total\n1,Ada,42.50\n2,Grace,7.25\n",
    )
    .expect("write source csv");

    let destination_path = work.path().join("customers_dataset");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: parquet
  path: {}
dataset: customers
load_mode: full_refresh
"#,
            source_path.display(),
            destination_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    let assert = Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .success();

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let (report_path, report) =
        read_single_report(&artifacts_dir, "csv load writes one artifact directory");
    let parquet_path = single_parquet_file(&destination_path);
    let batch = read_single_parquet_batch(&parquet_path);

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>(),
        vec!["customer_id", "name", "total"]
    );

    let customer_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("customer_id is int64");
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name is string");
    let totals = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("total is float64");

    assert_eq!(customer_ids.value(0), 1);
    assert_eq!(customer_ids.value(1), 2);
    assert_eq!(names.value(0), "Ada");
    assert_eq!(names.value(1), "Grace");
    assert_eq!(totals.value(0), 42.50);
    assert_eq!(totals.value(1), 7.25);

    assert_eq!(report["report_version"], 1);
    assert_eq!(
        report["binary_version"],
        env!("CARGO_PKG_VERSION"),
        "the report names the binary version that wrote it"
    );
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(report["process_exit_code"], 0);
    assert_eq!(report["dataset"], "customers");
    assert_eq!(report["load_mode"], "full_refresh");
    assert_eq!(report["schema_decision"]["mode"], "inferred");
    assert_eq!(
        report["schema_decision"]["drift_status"], "not_applicable",
        "an inferred load has no pinned schema to drift from"
    );
    assert_eq!(report["row_counts"]["source"], 2);
    assert_eq!(report["row_counts"]["written"], 2);
    assert!(
        report["byte_counts"]["source"]
            .as_u64()
            .expect("source byte count")
            > 0
    );
    assert!(
        report["byte_counts"]["destination"]
            .as_u64()
            .expect("destination byte count")
            > 0
    );
    assert_eq!(report["destination_write"]["atomicity"], "best_effort");
    assert_eq!(report["execution"]["record_format"], "arrow_record_batch");
    assert_eq!(report["execution"]["batch_count"], 1);
    assert_eq!(
        report["execution"]["chunk_rows"], 65536,
        "the effective chunk bound is echoed, defaulting to 65536 (ADR-0046)"
    );
    assert!(report["error_summary"].is_null());
    assert!(report_path.ends_with("load-report.json"));

    assert!(stdout.contains("Status: succeeded"));
    assert!(stdout.contains("Records read: 2"));
    assert!(stdout.contains("Records written: 2"));
    assert!(stdout.contains(report_path.to_str().expect("report path")));
}

#[test]
fn local_csv_append_preserves_existing_parquet_records_and_reports_the_load() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    let destination_path = work.path().join("customers_dataset");
    let definition_path = work.path().join("load.yml");

    fs::write(&source_path, "customer_id,name\n1,Ada\n2,Grace\n").expect("write first csv");
    write_load_definition(
        &definition_path,
        &source_path,
        "csv",
        "parquet",
        &destination_path,
        "full_refresh",
        None,
    );
    let first = run_cli_load(
        work.path(),
        &work.path().join("artifacts-first"),
        &definition_path,
        true,
    );

    fs::write(&source_path, "name,customer_id\nKatherine,3\n").expect("write append csv");
    write_load_definition(
        &definition_path,
        &source_path,
        "csv",
        "parquet",
        &destination_path,
        "append",
        None,
    );
    let append = run_cli_load(
        work.path(),
        &work.path().join("artifacts-append"),
        &definition_path,
        true,
    );

    assert_eq!(
        id_name_records(&read_parquet_batches(&destination_path)),
        vec![
            (1, "Ada".to_string()),
            (2, "Grace".to_string()),
            (3, "Katherine".to_string()),
        ]
    );
    assert_successful_append(&append, "staged_part_append");
    assert_ne!(first.report["load_id"], append.report["load_id"]);
    assert!(!append.report["load_id"]
        .as_str()
        .expect("append load id")
        .is_empty());
    assert_eq!(
        PathBuf::from(
            append.report["artifact_dir"]
                .as_str()
                .expect("artifact directory")
        ),
        append.report_path.parent().expect("report parent")
    );
}

#[test]
fn local_csv_full_refresh_writes_readable_duckdb_table_report_and_summary() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    fs::write(
        &source_path,
        "customer_id,name,total\n1,Ada,42.50\n2,Grace,7.25\n",
    )
    .expect("write source csv");

    let database_path = work.path().join("customers.duckdb");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: duckdb
  path: {}
dataset: customers
load_mode: full_refresh
"#,
            source_path.display(),
            database_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    let assert = Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .success();

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let (report_path, report) =
        read_single_report(&artifacts_dir, "duckdb load writes one artifact directory");
    let batch = read_single_duckdb_batch(&database_path, "customers");

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>(),
        vec!["customer_id", "name", "total"]
    );

    let customer_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("customer_id is int64");
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name is string");
    let totals = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("total is float64");

    assert_eq!(customer_ids.value(0), 1);
    assert_eq!(customer_ids.value(1), 2);
    assert_eq!(names.value(0), "Ada");
    assert_eq!(names.value(1), "Grace");
    assert_eq!(totals.value(0), 42.50);
    assert_eq!(totals.value(1), 7.25);

    assert_eq!(report["report_version"], 1);
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(report["process_exit_code"], 0);
    assert_eq!(report["dataset"], "customers");
    assert_eq!(report["load_mode"], "full_refresh");
    assert_eq!(report["schema_decision"]["mode"], "inferred");
    assert_eq!(report["row_counts"]["source"], 2);
    assert_eq!(report["row_counts"]["written"], 2);
    assert!(
        report["byte_counts"]["source"]
            .as_u64()
            .expect("source byte count")
            > 0
    );
    assert!(
        report["byte_counts"]["destination"].is_null(),
        "a duckdb destination reports no destination byte count"
    );
    assert_eq!(report["destination_write"]["atomicity"], "atomic");
    assert_eq!(
        report["destination_write"]["strategy"],
        "transactional_replace"
    );
    assert_eq!(report["execution"]["record_format"], "arrow_record_batch");
    assert_eq!(report["execution"]["batch_count"], 1);
    assert_eq!(
        report["execution"]["chunk_rows"], 65536,
        "the effective chunk bound is echoed, defaulting to 65536 (ADR-0046)"
    );
    assert!(report["error_summary"].is_null());
    assert!(report_path.ends_with("load-report.json"));

    assert!(stdout.contains("Status: succeeded"));
    assert!(stdout.contains("Records read: 2"));
    assert!(stdout.contains("Records written: 2"));
    assert!(stdout.contains(report_path.to_str().expect("report path")));
}

#[test]
fn local_csv_append_preserves_existing_duckdb_records_and_reports_the_load() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    let database_path = work.path().join("customers.duckdb");
    let definition_path = work.path().join("load.yml");

    fs::write(&source_path, "customer_id,name\n1,Ada\n2,Grace\n").expect("write first csv");
    write_load_definition(
        &definition_path,
        &source_path,
        "csv",
        "duckdb",
        &database_path,
        "full_refresh",
        None,
    );
    run_cli_load(
        work.path(),
        &work.path().join("artifacts-first"),
        &definition_path,
        true,
    );

    // Reordered source fields still append by name rather than silently swapping
    // same-typed values by position.
    fs::write(&source_path, "name,customer_id\nKatherine,3\n").expect("write append csv");
    write_load_definition(
        &definition_path,
        &source_path,
        "csv",
        "duckdb",
        &database_path,
        "append",
        None,
    );
    let append = run_cli_load(
        work.path(),
        &work.path().join("artifacts-append"),
        &definition_path,
        true,
    );

    let batch = read_single_duckdb_batch(&database_path, "customers");
    let customer_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("customer_id is int64");
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name is string");
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(customer_ids.values(), &[1, 2, 3]);
    assert_eq!(names.value(0), "Ada");
    assert_eq!(names.value(1), "Grace");
    assert_eq!(names.value(2), "Katherine");
    assert!(append.report["byte_counts"]["destination"].is_null());
    assert_successful_append(&append, "insert");
}

#[test]
fn local_jsonl_full_refresh_writes_readable_parquet_directory_report_and_summary() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.jsonl");
    fs::write(
        &source_path,
        "{\"customer_id\": 1, \"name\": \"Ada\", \"total\": 42.50, \"active\": true}\n\
         {\"customer_id\": 2, \"name\": \"Grace\", \"total\": 7.25, \"active\": false}\n",
    )
    .expect("write source jsonl");

    let destination_path = work.path().join("customers_dataset");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: jsonl
destination:
  connector: parquet
  path: {}
dataset: customers
load_mode: full_refresh
"#,
            source_path.display(),
            destination_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    let assert = Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .success();

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let (report_path, report) =
        read_single_report(&artifacts_dir, "jsonl load writes one artifact directory");
    let parquet_path = single_parquet_file(&destination_path);
    let batch = read_single_parquet_batch(&parquet_path);

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>(),
        vec!["customer_id", "name", "total", "active"]
    );

    let customer_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("customer_id is int64");
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name is string");
    let totals = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("total is float64");
    let actives = batch
        .column(3)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("active is boolean");

    assert_eq!(customer_ids.value(0), 1);
    assert_eq!(customer_ids.value(1), 2);
    assert_eq!(names.value(0), "Ada");
    assert_eq!(names.value(1), "Grace");
    assert_eq!(totals.value(0), 42.50);
    assert_eq!(totals.value(1), 7.25);
    assert!(actives.value(0));
    assert!(!actives.value(1));

    assert_eq!(report["report_version"], 1);
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(report["process_exit_code"], 0);
    assert_eq!(report["load_mode"], "full_refresh");
    assert_eq!(report["schema_decision"]["mode"], "inferred");
    assert_eq!(report["row_counts"]["source"], 2);
    assert_eq!(report["row_counts"]["written"], 2);
    assert!(
        report["byte_counts"]["source"]
            .as_u64()
            .expect("source byte count")
            > 0
    );
    assert!(
        report["byte_counts"]["destination"]
            .as_u64()
            .expect("destination byte count")
            > 0
    );
    assert_eq!(report["destination_write"]["atomicity"], "best_effort");
    assert_eq!(report["execution"]["record_format"], "arrow_record_batch");
    assert_eq!(report["execution"]["batch_count"], 1);
    assert_eq!(
        report["execution"]["chunk_rows"], 65536,
        "the effective chunk bound is echoed, defaulting to 65536 (ADR-0046)"
    );
    assert!(report["error_summary"].is_null());
    assert!(report_path.ends_with("load-report.json"));

    assert!(stdout.contains("Status: succeeded"));
    assert!(stdout.contains("Records read: 2"));
    assert!(stdout.contains("Records written: 2"));
    assert!(stdout.contains(report_path.to_str().expect("report path")));
}

#[test]
fn local_jsonl_append_preserves_existing_parquet_records_and_reports_the_load() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.jsonl");
    let destination_path = work.path().join("customers_dataset");
    let definition_path = work.path().join("load.yml");

    fs::write(
        &source_path,
        "{\"customer_id\": 1, \"name\": \"Ada\", \"active\": true}\n\
         {\"customer_id\": 2, \"name\": \"Grace\", \"active\": false}\n",
    )
    .expect("write first jsonl");
    write_load_definition(
        &definition_path,
        &source_path,
        "jsonl",
        "parquet",
        &destination_path,
        "full_refresh",
        None,
    );
    run_cli_load(
        work.path(),
        &work.path().join("artifacts-first"),
        &definition_path,
        true,
    );

    fs::write(
        &source_path,
        "{\"name\": \"Katherine\", \"active\": true, \"customer_id\": 3}\n",
    )
    .expect("write append jsonl");
    write_load_definition(
        &definition_path,
        &source_path,
        "jsonl",
        "parquet",
        &destination_path,
        "append",
        None,
    );
    let append = run_cli_load(
        work.path(),
        &work.path().join("artifacts-append"),
        &definition_path,
        true,
    );

    assert_eq!(
        id_name_records(&read_parquet_batches(&destination_path)),
        vec![
            (1, "Ada".to_string()),
            (2, "Grace".to_string()),
            (3, "Katherine".to_string()),
        ]
    );
    assert_successful_append(&append, "staged_part_append");
}

#[test]
fn local_jsonl_full_refresh_writes_readable_duckdb_table_report_and_summary() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.jsonl");
    fs::write(
        &source_path,
        "{\"customer_id\": 1, \"name\": \"Ada\", \"total\": 42.50, \"active\": true}\n\
         {\"customer_id\": 2, \"name\": \"Grace\", \"total\": 7.25, \"active\": false}\n",
    )
    .expect("write source jsonl");

    let database_path = work.path().join("customers.duckdb");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: jsonl
destination:
  connector: duckdb
  path: {}
dataset: customers
load_mode: full_refresh
"#,
            source_path.display(),
            database_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    let assert = Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .success();

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let (report_path, report) = read_single_report(
        &artifacts_dir,
        "jsonl duckdb load writes one artifact directory",
    );
    let batch = read_single_duckdb_batch(&database_path, "customers");

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name())
            .collect::<Vec<_>>(),
        vec!["customer_id", "name", "total", "active"]
    );

    let customer_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("customer_id is int64");
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name is string");
    let totals = batch
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("total is float64");
    let actives = batch
        .column(3)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("active is boolean");

    assert_eq!(customer_ids.value(0), 1);
    assert_eq!(customer_ids.value(1), 2);
    assert_eq!(names.value(0), "Ada");
    assert_eq!(names.value(1), "Grace");
    assert_eq!(totals.value(0), 42.50);
    assert_eq!(totals.value(1), 7.25);
    assert!(actives.value(0));
    assert!(!actives.value(1));

    assert_eq!(report["report_version"], 1);
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(report["process_exit_code"], 0);
    assert_eq!(report["dataset"], "customers");
    assert_eq!(report["load_mode"], "full_refresh");
    assert_eq!(report["schema_decision"]["mode"], "inferred");
    assert_eq!(report["row_counts"]["source"], 2);
    assert_eq!(report["row_counts"]["written"], 2);
    assert!(
        report["byte_counts"]["source"]
            .as_u64()
            .expect("source byte count")
            > 0
    );
    assert!(
        report["byte_counts"]["destination"].is_null(),
        "a duckdb destination reports no destination byte count"
    );
    assert_eq!(report["destination_write"]["atomicity"], "atomic");
    assert_eq!(
        report["destination_write"]["strategy"],
        "transactional_replace"
    );
    assert_eq!(report["execution"]["record_format"], "arrow_record_batch");
    assert_eq!(report["execution"]["batch_count"], 1);
    assert_eq!(
        report["execution"]["chunk_rows"], 65536,
        "the effective chunk bound is echoed, defaulting to 65536 (ADR-0046)"
    );
    assert!(report["error_summary"].is_null());
    assert!(report_path.ends_with("load-report.json"));

    assert!(stdout.contains("Status: succeeded"));
    assert!(stdout.contains("Records read: 2"));
    assert!(stdout.contains("Records written: 2"));
    assert!(stdout.contains(report_path.to_str().expect("report path")));
}

#[test]
fn local_jsonl_append_preserves_existing_duckdb_records_and_reports_the_load() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.jsonl");
    let database_path = work.path().join("customers.duckdb");
    let definition_path = work.path().join("load.yml");

    fs::write(
        &source_path,
        "{\"customer_id\": 1, \"name\": \"Ada\", \"active\": true}\n\
         {\"customer_id\": 2, \"name\": \"Grace\", \"active\": false}\n",
    )
    .expect("write first jsonl");
    write_load_definition(
        &definition_path,
        &source_path,
        "jsonl",
        "duckdb",
        &database_path,
        "full_refresh",
        None,
    );
    run_cli_load(
        work.path(),
        &work.path().join("artifacts-first"),
        &definition_path,
        true,
    );

    fs::write(
        &source_path,
        "{\"name\": \"Katherine\", \"active\": true, \"customer_id\": 3}\n",
    )
    .expect("write append jsonl");
    write_load_definition(
        &definition_path,
        &source_path,
        "jsonl",
        "duckdb",
        &database_path,
        "append",
        None,
    );
    let append = run_cli_load(
        work.path(),
        &work.path().join("artifacts-append"),
        &definition_path,
        true,
    );

    let batch = read_single_duckdb_batch(&database_path, "customers");
    let customer_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("customer_id is int64");
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name is string");
    let actives = batch
        .column(2)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("active is boolean");
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(customer_ids.values(), &[1, 2, 3]);
    assert_eq!(names.value(0), "Ada");
    assert_eq!(names.value(1), "Grace");
    assert_eq!(names.value(2), "Katherine");
    assert!(actives.value(0));
    assert!(!actives.value(1));
    assert!(actives.value(2));
    assert_successful_append(&append, "insert");
}

#[test]
fn local_csv_keeps_zero_padded_numeric_text_as_text() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("accounts.csv");
    // `zip` values are zero-padded numeric text: per ADR-0032 they must stay
    // text, not be retyped as int64 (which would drop the leading zeros).
    fs::write(&source_path, "zip,balance\n00501,10\n02134,5\n").expect("write source csv");

    let destination_path = work.path().join("accounts_dataset");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: parquet
  path: {}
dataset: accounts
load_mode: full_refresh
"#,
            source_path.display(),
            destination_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .success();

    let (_, report) = read_single_report(
        &artifacts_dir,
        "zero-padded csv load writes one artifact directory",
    );
    assert_eq!(report["schema_decision"]["fields"][0]["name"], "zip");
    assert_eq!(report["schema_decision"]["fields"][0]["type"], "utf8");
    assert_eq!(report["schema_decision"]["fields"][1]["name"], "balance");
    assert_eq!(report["schema_decision"]["fields"][1]["type"], "int64");

    let parquet_path = single_parquet_file(&destination_path);
    let batch = read_single_parquet_batch(&parquet_path);

    let zips = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("zip stays string");
    let balances = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("balance is int64");

    assert_eq!(zips.value(0), "00501");
    assert_eq!(zips.value(1), "02134");
    assert_eq!(balances.value(0), 10);
    assert_eq!(balances.value(1), 5);
}

#[test]
fn local_jsonl_infers_field_types_from_json_values_not_stringified_text() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("accounts.jsonl");
    // `zip` is a JSON string of digits: it must stay text, not be retyped as a
    // number (which would drop the leading zero). `balance` is a JSON number.
    fs::write(
        &source_path,
        "{\"zip\": \"01234\", \"balance\": 10}\n{\"zip\": \"00987\", \"balance\": 5}\n",
    )
    .expect("write source jsonl");

    let destination_path = work.path().join("accounts_dataset");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: jsonl
destination:
  connector: parquet
  path: {}
dataset: accounts
load_mode: full_refresh
"#,
            source_path.display(),
            destination_path.display()
        ),
    )
    .expect("write load definition");

    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(work.path().join("artifacts"))
        .arg(&definition_path)
        .assert()
        .success();

    let parquet_path = single_parquet_file(&destination_path);
    let batch = read_single_parquet_batch(&parquet_path);

    let zips = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("zip stays string");
    let balances = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("balance is int64");

    assert_eq!(zips.value(0), "01234");
    assert_eq!(zips.value(1), "00987");
    assert_eq!(balances.value(0), 10);
    assert_eq!(balances.value(1), 5);
}

#[test]
fn malformed_local_jsonl_rejects_the_record_and_fails_the_default_threshold() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.jsonl");
    fs::write(
        &source_path,
        "{\"customer_id\": 1, \"name\": \"Ada\"}\n{\"customer_id\": 2, \"name\": \n",
    )
    .expect("write malformed source jsonl");

    let destination_path = work.path().join("customers_dataset");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: jsonl
destination:
  connector: parquet
  path: {}
dataset: customers
load_mode: full_refresh
"#,
            source_path.display(),
            destination_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    let assert = Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .failure();

    assert!(
        !destination_path.exists(),
        "a load failing its reject threshold must not touch the destination"
    );

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let (report_path, report) = read_single_report(
        &artifacts_dir,
        "failed jsonl load writes one artifact directory",
    );

    // The malformed line became a rejected record; the default reject
    // threshold of 0 then failed the load (ADR-0020).
    assert_eq!(report["report_version"], 1);
    assert_eq!(report["exit_status"], "failed");
    assert_eq!(report["process_exit_code"], 1);
    assert_eq!(report["row_counts"]["source"], 2);
    assert_eq!(report["row_counts"]["written"], 0);
    assert_eq!(report["row_counts"]["rejected"], 1);
    assert_eq!(report["rejected_records"]["count"], 1);
    assert_eq!(report["destination_write"]["atomicity"], "not_applicable");
    assert_eq!(report["error_summary"]["code"], "reject_threshold_exceeded");
    assert_eq!(
        report["error_summary"]["message"],
        "rejected 1 of 2 records, exceeding the reject threshold of 0"
    );

    // The rejected-record artifact holds the source context a troubleshooter
    // needs: the line, the parse problem, and the raw line text.
    let artifact_path = PathBuf::from(
        report["rejected_records"]["artifact"]
            .as_str()
            .expect("artifact path"),
    );
    let rejected_lines = read_rejected_records(&artifact_path);
    assert_eq!(rejected_lines.len(), 1);
    assert_eq!(rejected_lines[0]["line"], 2);
    assert_eq!(rejected_lines[0]["code"], "malformed_jsonl_record");
    assert_eq!(rejected_lines[0]["field"], Value::Null);
    assert!(!rejected_lines[0]["message"]
        .as_str()
        .expect("rejection message")
        .is_empty());
    assert_eq!(
        rejected_lines[0]["record"],
        "{\"customer_id\": 2, \"name\": "
    );

    assert!(stdout.contains("Status: failed"));
    assert!(stdout.contains("Records rejected: 1"));
    assert!(stdout.contains(artifact_path.to_str().expect("artifact path")));
    assert!(stdout.contains("exceeding the reject threshold of 0"));
    assert!(stdout.contains(report_path.to_str().expect("report path")));
}

#[test]
fn all_invalid_jsonl_rejections_fail_the_default_threshold_before_destination_writing() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.jsonl");
    fs::write(&source_path, "not json\n[1]\n").expect("write invalid source jsonl");

    let destination_path = work.path().join("customers_dataset");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: jsonl
destination:
  connector: parquet
  path: {}
dataset: customers
load_mode: full_refresh
"#,
            source_path.display(),
            destination_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    let assert = Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .failure();

    assert!(
        !destination_path.exists(),
        "an unreadable JSONL source must fail before destination writing"
    );

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let (report_path, report) = read_single_report(
        &artifacts_dir,
        "all-invalid jsonl load writes one artifact directory",
    );

    assert_eq!(report["report_version"], 1);
    assert_eq!(report["exit_status"], "failed");
    assert_eq!(report["process_exit_code"], 1);
    assert_eq!(report["error_summary"]["code"], "reject_threshold_exceeded");
    assert_eq!(
        report["error_summary"]["message"],
        "rejected 2 of 2 records, exceeding the reject threshold of 0"
    );
    assert_eq!(report["row_counts"]["source"], 2);
    assert_eq!(report["row_counts"]["written"], 0);
    assert_eq!(report["row_counts"]["rejected"], 2);
    assert_eq!(report["destination_write"]["atomicity"], "not_applicable");

    let artifact_path = PathBuf::from(
        report["rejected_records"]["artifact"]
            .as_str()
            .expect("rejected artifact path"),
    );
    let rejected = read_rejected_records(&artifact_path);
    assert_eq!(rejected.len(), 2);
    assert_eq!(rejected[0]["line"], 1);
    assert_eq!(rejected[0]["code"], "malformed_jsonl_record");
    assert_eq!(rejected[1]["line"], 2);
    assert_eq!(rejected[1]["code"], "malformed_jsonl_record");

    assert!(stdout.contains("Status: failed"));
    assert!(stdout.contains("Records read: 2"));
    assert!(stdout.contains("Records written: 0"));
    assert!(stdout.contains("Records rejected: 2"));
    assert!(stdout.contains("exceeding the reject threshold of 0"));
    assert!(stdout.contains(report_path.to_str().expect("report path")));
    assert!(stdout.contains(artifact_path.to_str().expect("artifact path")));
}

#[test]
fn invalid_utf8_jsonl_line_is_rejected_without_losing_prior_source_facts() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.jsonl");
    fs::write(
        &source_path,
        b"{\"customer_id\":1}\nnot json\n{\"customer_id\":\"\xFF\"}\n",
    )
    .expect("write malformed source jsonl");

    let destination_path = work.path().join("customers_dataset");
    let definition_path = work.path().join("load.yml");
    write_load_definition(
        &definition_path,
        &source_path,
        "jsonl",
        "parquet",
        &destination_path,
        "full_refresh",
        None,
    );

    let artifacts_dir = work.path().join("artifacts");
    let result = run_cli_load(work.path(), &artifacts_dir, &definition_path, false);
    let report = &result.report;

    assert!(
        !destination_path.exists(),
        "malformed JSONL records must fail the default threshold before writing"
    );
    assert_eq!(report["report_version"], 1);
    assert_eq!(report["exit_status"], "failed");
    assert_eq!(report["process_exit_code"], 1);
    assert_eq!(report["error_summary"]["code"], "reject_threshold_exceeded");
    assert_eq!(
        report["error_summary"]["message"],
        "rejected 2 of 3 records, exceeding the reject threshold of 0"
    );
    assert_eq!(report["row_counts"]["source"], 3);
    assert_eq!(report["row_counts"]["written"], 0);
    assert_eq!(report["row_counts"]["rejected"], 2);
    assert_eq!(report["destination_write"]["atomicity"], "not_applicable");

    let artifact_path = PathBuf::from(
        report["rejected_records"]["artifact"]
            .as_str()
            .expect("rejected artifact path"),
    );
    let rejected = read_rejected_records(&artifact_path);
    assert_eq!(rejected.len(), 2);
    assert_eq!(rejected[0]["line"], 2);
    assert_eq!(rejected[0]["code"], "malformed_jsonl_record");
    assert_eq!(rejected[1]["line"], 3);
    assert_eq!(rejected[1]["code"], "malformed_jsonl_record");

    assert!(result.stdout.contains("Status: failed"));
    assert!(result.stdout.contains("Records read: 3"));
    assert!(result.stdout.contains("Records written: 0"));
    assert!(result.stdout.contains("Records rejected: 2"));
    assert!(result
        .stdout
        .contains(result.report_path.to_str().expect("report path")));
    assert!(result
        .stdout
        .contains(artifact_path.to_str().expect("artifact path")));
}

#[test]
fn missing_local_source_fails_before_destination_writing_with_a_report_and_summary() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("missing-customers.csv");
    let destination_path = work.path().join("customers_dataset");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: parquet
  path: {}
dataset: customers
load_mode: full_refresh
"#,
            source_path.display(),
            destination_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    let assert = Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .failure();

    assert!(
        !destination_path.exists(),
        "a missing source must fail before destination writing"
    );

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let (report_path, report) = read_single_report(
        &artifacts_dir,
        "missing-source load writes one artifact directory",
    );

    assert_eq!(report["report_version"], 1);
    assert_eq!(
        report["binary_version"],
        env!("CARGO_PKG_VERSION"),
        "a failure report names the binary version that wrote it"
    );
    assert_eq!(report["exit_status"], "failed");
    assert_eq!(report["process_exit_code"], 1);
    assert_eq!(report["error_summary"]["code"], "source_read_failed");
    assert!(report["error_summary"]["message"]
        .as_str()
        .expect("error message")
        .contains("failed to read CSV source"));
    assert_eq!(report["row_counts"]["source"], 0);
    assert_eq!(report["row_counts"]["written"], 0);
    assert_eq!(report["row_counts"]["rejected"], 0);
    assert!(report["rejected_records"]["artifact"].is_null());
    assert_eq!(report["destination_write"]["atomicity"], "not_applicable");

    assert!(stdout.contains("Status: failed"));
    assert!(stdout.contains("Records read: 0"));
    assert!(stdout.contains("Records written: 0"));
    assert!(stdout.contains("Records rejected: 0"));
    assert!(stdout.contains("failed to read CSV source"));
    assert!(stdout.contains(report_path.to_str().expect("report path")));
}

#[test]
fn empty_csv_fails_explicitly_before_destination_writing() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    fs::write(&source_path, "").expect("write empty source csv");

    let destination_path = work.path().join("customers_dataset");
    let definition_path = work.path().join("load.yml");
    write_load_definition(
        &definition_path,
        &source_path,
        "csv",
        "parquet",
        &destination_path,
        "full_refresh",
        None,
    );

    let artifacts_dir = work.path().join("artifacts");
    let result = run_cli_load(work.path(), &artifacts_dir, &definition_path, false);
    let report = &result.report;

    assert!(!destination_path.exists());
    assert_eq!(report["report_version"], 1);
    assert_eq!(report["exit_status"], "failed");
    assert_eq!(report["process_exit_code"], 1);
    assert_eq!(report["error_summary"]["code"], "malformed_csv");
    assert!(report["error_summary"]["message"]
        .as_str()
        .expect("error message")
        .contains("must include at least one header field"));
    assert_eq!(report["row_counts"]["source"], 0);
    assert_eq!(report["row_counts"]["written"], 0);
    assert_eq!(report["row_counts"]["rejected"], 0);
    assert!(report["rejected_records"]["artifact"].is_null());
    assert_eq!(report["destination_write"]["atomicity"], "not_applicable");
    assert!(result.stdout.contains("Status: failed"));
    assert!(result.stdout.contains("Records read: 0"));
    assert!(result.stdout.contains("Records written: 0"));
    assert!(result.stdout.contains("Records rejected: 0"));
    assert!(result
        .stdout
        .contains(result.report_path.to_str().expect("report path")));
}

#[test]
fn empty_jsonl_fails_explicitly_before_destination_writing() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.jsonl");
    fs::write(&source_path, "\n  \n").expect("write blank source jsonl");

    let destination_path = work.path().join("customers_dataset");
    let definition_path = work.path().join("load.yml");
    write_load_definition(
        &definition_path,
        &source_path,
        "jsonl",
        "parquet",
        &destination_path,
        "full_refresh",
        None,
    );

    let artifacts_dir = work.path().join("artifacts");
    let result = run_cli_load(work.path(), &artifacts_dir, &definition_path, false);
    let report = &result.report;

    assert!(!destination_path.exists());
    assert_eq!(report["report_version"], 1);
    assert_eq!(report["exit_status"], "failed");
    assert_eq!(report["process_exit_code"], 1);
    assert_eq!(report["error_summary"]["code"], "malformed_jsonl");
    assert!(report["error_summary"]["message"]
        .as_str()
        .expect("error message")
        .contains("must include at least one record with fields"));
    assert_eq!(report["row_counts"]["source"], 0);
    assert_eq!(report["row_counts"]["written"], 0);
    assert_eq!(report["row_counts"]["rejected"], 0);
    assert!(report["rejected_records"]["artifact"].is_null());
    assert_eq!(report["destination_write"]["atomicity"], "not_applicable");
    assert!(result.stdout.contains("Status: failed"));
    assert!(result.stdout.contains("Records read: 0"));
    assert!(result.stdout.contains("Records written: 0"));
    assert!(result.stdout.contains("Records rejected: 0"));
    assert!(result
        .stdout
        .contains(result.report_path.to_str().expect("report path")));
}

#[test]
fn header_only_csv_completes_as_an_empty_load() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    fs::write(&source_path, "customer_id,name\n").expect("write header-only source csv");

    let destination_path = work.path().join("customers_dataset");
    let definition_path = work.path().join("load.yml");
    write_load_definition(
        &definition_path,
        &source_path,
        "csv",
        "parquet",
        &destination_path,
        "full_refresh",
        None,
    );

    let artifacts_dir = work.path().join("artifacts");
    let result = run_cli_load(work.path(), &artifacts_dir, &definition_path, true);
    let report = &result.report;

    assert_eq!(report["report_version"], 1);
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(report["process_exit_code"], 0);
    assert_eq!(report["schema_decision"]["mode"], "inferred");
    assert_eq!(report["row_counts"]["source"], 0);
    assert_eq!(report["row_counts"]["written"], 0);
    assert_eq!(report["row_counts"]["rejected"], 0);
    assert!(report["rejected_records"]["artifact"].is_null());
    assert_eq!(report["destination_write"]["atomicity"], "best_effort");
    assert!(report["error_summary"].is_null());

    let files = parquet_files(&destination_path);
    assert_eq!(
        files.len(),
        1,
        "the empty dataset still has one Parquet file"
    );
    let rows = read_parquet_batches(&destination_path)
        .iter()
        .map(|batch| batch.num_rows())
        .sum::<usize>();
    assert_eq!(rows, 0, "an external Parquet reader sees no records");

    assert!(result.stdout.contains("Status: succeeded"));
    assert!(result.stdout.contains("Records read: 0"));
    assert!(result.stdout.contains("Records written: 0"));
    assert!(result.stdout.contains("Records rejected: 0"));
    assert!(result
        .stdout
        .contains(result.report_path.to_str().expect("report path")));
}

#[test]
fn all_rejected_parseable_jsonl_completes_as_an_empty_load() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("events.jsonl");
    // Every record parses as a JSON object but rejects under the override,
    // so the source still offers field names with no survivor to observe.
    fs::write(
        &source_path,
        "{\"station\": null, \"reading\": 1.5}\n{\"station\": null, \"reading\": 2.5}\n",
    )
    .expect("write all-rejected source jsonl");

    let destination_path = work.path().join("events_dataset");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: jsonl
destination:
  connector: parquet
  path: {}
load_mode: full_refresh
schema:
  overrides:
    - name: station
      nullable: false
reject_threshold: 10
"#,
            source_path.display(),
            destination_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    let result = run_cli_load(work.path(), &artifacts_dir, &definition_path, true);
    let report = &result.report;

    // ADR-0056: only an all-unparseable JSONL source fails (`malformed_jsonl`);
    // records that parse but reject resolve the schema from their own field
    // names, and a tolerated all-rejected load completes as an empty one.
    assert_eq!(report["report_version"], 1);
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(report["process_exit_code"], 0);
    assert_eq!(report["schema_decision"]["mode"], "inferred");
    let fields: Vec<(&str, &str)> = report["schema_decision"]["fields"]
        .as_array()
        .expect("schema fields")
        .iter()
        .map(|field| {
            (
                field["name"].as_str().expect("field name"),
                field["type"].as_str().expect("field type"),
            )
        })
        .collect();
    // No surviving record shaped the columns, so both infer as utf8.
    assert_eq!(fields, [("station", "utf8"), ("reading", "utf8")]);
    assert_eq!(report["row_counts"]["source"], 2);
    assert_eq!(report["row_counts"]["written"], 0);
    assert_eq!(report["row_counts"]["rejected"], 2);
    assert_eq!(report["rejected_records"]["count"], 2);
    assert_eq!(report["destination_write"]["atomicity"], "best_effort");
    assert!(report["error_summary"].is_null());

    let artifact_path = PathBuf::from(
        report["rejected_records"]["artifact"]
            .as_str()
            .expect("artifact path"),
    );
    let rejected_lines = read_rejected_records(&artifact_path);
    assert_eq!(rejected_lines.len(), 2);
    assert_eq!(rejected_lines[0]["line"], 1);
    assert_eq!(rejected_lines[0]["code"], "missing_required_field");
    assert_eq!(rejected_lines[1]["line"], 2);
    assert_eq!(rejected_lines[1]["code"], "missing_required_field");

    let files = parquet_files(&destination_path);
    assert_eq!(
        files.len(),
        1,
        "the empty dataset still has one Parquet file"
    );
    let rows = read_parquet_batches(&destination_path)
        .iter()
        .map(|batch| batch.num_rows())
        .sum::<usize>();
    assert_eq!(rows, 0, "an external Parquet reader sees no records");

    assert!(result.stdout.contains("Status: succeeded"));
    assert!(result.stdout.contains("Records read: 2"));
    assert!(result.stdout.contains("Records written: 0"));
    assert!(result.stdout.contains("Records rejected: 2"));
}

#[test]
fn malformed_csv_header_fails_with_a_report_and_summary_before_destination_writing() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    fs::write(&source_path, b"customer_id,\xFF\n1,Ada\n").expect("write malformed source csv");

    let destination_path = work.path().join("customers_dataset");
    let definition_path = work.path().join("load.yml");
    write_load_definition(
        &definition_path,
        &source_path,
        "csv",
        "parquet",
        &destination_path,
        "full_refresh",
        None,
    );

    let artifacts_dir = work.path().join("artifacts");
    let result = run_cli_load(work.path(), &artifacts_dir, &definition_path, false);
    let report = &result.report;

    assert!(
        !destination_path.exists(),
        "a malformed CSV header must fail before destination writing"
    );
    assert_eq!(report["report_version"], 1);
    assert_eq!(report["exit_status"], "failed");
    assert_eq!(report["process_exit_code"], 1);
    assert_eq!(report["error_summary"]["code"], "malformed_csv");
    assert!(report["error_summary"]["message"]
        .as_str()
        .expect("error message")
        .contains("malformed CSV syntax"));
    assert_eq!(report["row_counts"]["source"], 0);
    assert_eq!(report["row_counts"]["written"], 0);
    assert_eq!(report["row_counts"]["rejected"], 0);
    assert!(report["rejected_records"]["artifact"].is_null());
    assert_eq!(report["destination_write"]["atomicity"], "not_applicable");
    assert!(result.stdout.contains("Status: failed"));
    assert!(result.stdout.contains("malformed CSV syntax"));
    assert!(result
        .stdout
        .contains(result.report_path.to_str().expect("report path")));
}

#[test]
fn malformed_local_csv_rejects_the_record_and_fails_the_default_threshold() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    fs::write(
        &source_path,
        "customer_id,name\n1,Ada\n2,Grace,extra-field\n",
    )
    .expect("write malformed source csv");

    let destination_path = work.path().join("customers_dataset");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: parquet
  path: {}
dataset: customers
load_mode: full_refresh
"#,
            source_path.display(),
            destination_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    let assert = Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .failure();

    assert!(
        !destination_path.exists(),
        "a load failing its reject threshold must not touch the destination"
    );

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let (report_path, report) = read_single_report(
        &artifacts_dir,
        "failed csv load writes one artifact directory",
    );

    assert_eq!(report["report_version"], 1);
    assert_eq!(report["exit_status"], "failed");
    assert_eq!(report["process_exit_code"], 1);
    assert_eq!(report["row_counts"]["source"], 2);
    assert_eq!(report["row_counts"]["written"], 0);
    assert_eq!(report["row_counts"]["rejected"], 1);
    assert_eq!(report["rejected_records"]["count"], 1);
    assert_eq!(report["destination_write"]["atomicity"], "not_applicable");
    assert_eq!(report["error_summary"]["code"], "reject_threshold_exceeded");
    assert_eq!(
        report["error_summary"]["message"],
        "rejected 1 of 2 records, exceeding the reject threshold of 0"
    );

    // The wrong-length record is recovered as an array of its cells, with the
    // line number and the field-count mismatch spelled out.
    let artifact_path = PathBuf::from(
        report["rejected_records"]["artifact"]
            .as_str()
            .expect("artifact path"),
    );
    let rejected_lines = read_rejected_records(&artifact_path);
    assert_eq!(rejected_lines.len(), 1);
    assert_eq!(rejected_lines[0]["line"], 3);
    assert_eq!(rejected_lines[0]["code"], "malformed_csv_record");
    assert_eq!(rejected_lines[0]["message"], "expected 2 fields, found 3");
    assert_eq!(
        rejected_lines[0]["record"],
        serde_json::json!(["2", "Grace", "extra-field"])
    );

    assert!(stdout.contains("Status: failed"));
    assert!(stdout.contains("Records rejected: 1"));
    assert!(stdout.contains("exceeding the reject threshold of 0"));
    assert!(stdout.contains(report_path.to_str().expect("report path")));
}

#[test]
fn local_csv_full_refresh_replaces_existing_parquet_destination() {
    let work = TempDir::new().expect("tempdir");
    let destination_path = work.path().join("customers_dataset");
    let source_path = work.path().join("customers.csv");
    let definition_path = work.path().join("load.yml");

    fs::write(
        &source_path,
        "customer_id,name,total\n1,Ada,42.50\n2,Grace,7.25\n",
    )
    .expect("write first source csv");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: parquet
  path: {}
dataset: customers
load_mode: full_refresh
"#,
            source_path.display(),
            destination_path.display()
        ),
    )
    .expect("write load definition");

    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(work.path().join("artifacts-first"))
        .arg(&definition_path)
        .assert()
        .success();

    fs::write(&source_path, "customer_id,name,total\n3,Katherine,100.00\n")
        .expect("write replacement source csv");

    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(work.path().join("artifacts-second"))
        .arg(&definition_path)
        .assert()
        .success();

    let parquet_path = single_parquet_file(&destination_path);
    let batch = read_single_parquet_batch(&parquet_path);
    let customer_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("customer_id is int64");
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name is string");

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(customer_ids.value(0), 3);
    assert_eq!(names.value(0), "Katherine");
}

#[test]
fn v1_load_definition_writes_report_and_human_summary() {
    let work = TempDir::new().expect("tempdir");
    fs::write(
        work.path().join("customers.csv"),
        "customer_id,name\n1,Ada\n",
    )
    .expect("write source csv");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        r#"
version: 1
source:
  connector: local_file
  path: customers.csv
  format: csv
destination:
  connector: parquet
  path: customers.parquet
dataset: customers
load_mode: full_refresh
"#,
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    let assert = Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .success();

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let (report_path, report) =
        read_single_report(&artifacts_dir, "one artifact directory per load");

    let load_id = report["load_id"].as_str().expect("load id");
    assert!(!load_id.is_empty());
    assert_eq!(report["report_version"], 1);
    assert_eq!(report["source_summary"]["connector"], "local_file");
    assert_eq!(report["source_summary"]["path"], "customers.csv");
    assert_eq!(report["destination_summary"]["connector"], "parquet");
    assert_eq!(report["destination_summary"]["path"], "customers.parquet");
    assert_eq!(report["load_mode"], "full_refresh");
    assert_eq!(report["exit_status"], "succeeded");
    assert!(report["timings"]["duration_ms"].is_number());
    assert!(report["error_summary"].is_null());

    assert!(stdout.contains("Data Spark load"));
    assert!(stdout.contains(load_id));
    assert!(stdout.contains("succeeded"));
    assert!(stdout.contains("full_refresh"));
    assert!(report_path.ends_with("load-report.json"));
    assert!(stdout.contains(report_path.to_str().expect("report path")));
}

#[test]
fn missing_version_fails_with_report_before_destination_work() {
    let work = TempDir::new().expect("tempdir");
    let destination_path = work.path().join("should-not-exist.parquet");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
source:
  connector: local_file
  path: missing-source.csv
  format: csv
destination:
  connector: parquet
  path: {}
dataset: customers
load_mode: full_refresh
"#,
            destination_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    let assert = Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .failure();

    assert!(
        !destination_path.exists(),
        "load definition contract failures must happen before destination writing"
    );

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let (report_path, report) = read_single_report(
        &artifacts_dir,
        "failed load still has one artifact directory",
    );

    assert_eq!(report["report_version"], 1);
    assert!(!report["load_id"].as_str().expect("load id").is_empty());
    assert_eq!(report["exit_status"], "failed");
    assert_eq!(report["process_exit_code"], 1);
    assert_eq!(
        report["error_summary"]["code"],
        "missing_load_definition_version"
    );
    assert!(report["error_summary"]["message"]
        .as_str()
        .expect("error message")
        .contains("version is required"));

    assert!(stdout.contains("Data Spark load"));
    assert!(stdout.contains("failed"));
    assert!(stdout.contains("version is required"));
    assert!(stdout.contains(report_path.to_str().expect("report path")));
}

#[test]
fn unsupported_version_fails_with_report_before_source_or_destination_work() {
    let work = TempDir::new().expect("tempdir");
    let destination_path = work.path().join("should-not-exist.parquet");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 2
source:
  connector: local_file
  path: missing-source.csv
  format: csv
destination:
  connector: parquet
  path: {}
dataset: customers
load_mode: full_refresh
"#,
            destination_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    let assert = Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .failure();

    assert!(
        !destination_path.exists(),
        "unsupported versions must fail before destination writing"
    );

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let (report_path, report) = read_single_report(
        &artifacts_dir,
        "failed load still has one artifact directory",
    );

    assert_eq!(report["report_version"], 1);
    assert_eq!(report["exit_status"], "failed");
    assert_eq!(report["process_exit_code"], 1);
    assert_eq!(
        report["error_summary"]["code"],
        "unsupported_load_definition_version"
    );
    assert!(report["error_summary"]["message"]
        .as_str()
        .expect("error message")
        .contains("unsupported load definition version: 2"));

    assert!(stdout.contains("failed"));
    assert!(stdout.contains("unsupported load definition version: 2"));
    assert!(stdout.contains(report_path.to_str().expect("report path")));
}

#[test]
fn malformed_load_definition_fails_with_report_and_summary() {
    let work = TempDir::new().expect("tempdir");
    let definition_path = work.path().join("load.yml");
    fs::write(&definition_path, "version: [\n").expect("write malformed load definition");

    let artifacts_dir = work.path().join("artifacts");
    let assert = Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .failure();

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let (report_path, report) = read_single_report(
        &artifacts_dir,
        "failed parse still has one artifact directory",
    );

    assert_eq!(report["report_version"], 1);
    assert_eq!(report["exit_status"], "failed");
    assert_eq!(report["process_exit_code"], 1);
    assert_eq!(
        report["error_summary"]["code"],
        "invalid_load_definition_yaml"
    );
    assert!(report["error_summary"]["message"]
        .as_str()
        .expect("error message")
        .contains("failed to parse load definition"));

    assert!(stdout.contains("Data Spark load"));
    assert!(stdout.contains("failed"));
    assert!(stdout.contains("failed to parse load definition"));
    assert!(stdout.contains(report_path.to_str().expect("report path")));
}

#[test]
fn unknown_load_definition_key_fails_with_report_before_source_or_destination_work() {
    // Strict contract (ADR-0037): the source file is deliberately missing and
    // the destination must never appear, so the unknown-key failure provably
    // happens before source reading and destination writing.
    let work = TempDir::new().expect("tempdir");
    let destination_path = work.path().join("should-not-exist.parquet");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
parallelism: 4
source:
  connector: local_file
  path: missing-source.csv
  format: csv
destination:
  connector: parquet
  path: {}
dataset: customers
load_mode: full_refresh
"#,
            destination_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    let assert = Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .failure();

    assert!(
        !destination_path.exists(),
        "unknown definition keys must fail before destination writing"
    );

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let (report_path, report) = read_single_report(
        &artifacts_dir,
        "failed strict parse still has one artifact directory",
    );

    assert_eq!(report["report_version"], 1);
    assert_eq!(report["exit_status"], "failed");
    assert_eq!(report["process_exit_code"], 1);
    assert_eq!(
        report["error_summary"]["code"],
        "invalid_load_definition_yaml"
    );
    let message = report["error_summary"]["message"]
        .as_str()
        .expect("error message");
    assert!(message.contains("failed to parse load definition"));
    assert!(
        message.contains("unknown field `parallelism`"),
        "error text must identify the rejected field: {message}"
    );

    assert!(stdout.contains("Data Spark load"));
    assert!(stdout.contains("failed"));
    assert!(stdout.contains("unknown field `parallelism`"));
    assert!(stdout.contains(report_path.to_str().expect("report path")));
}

#[test]
fn unknown_keys_in_nested_load_definition_blocks_fail_before_side_effects() {
    // Strict contract (ADR-0037) inside each nested block: source,
    // destination, schema, and artifacts. The missing source file and the
    // never-created destination prove the failure boundary for each case.
    let work = TempDir::new().expect("tempdir");
    let destination_path = work.path().join("should-not-exist.parquet");
    let cases = [
        (
            "select",
            r#"
version: 1
source:
  connector: local_file
  path: missing-source.csv
  format: csv
  select: [customer_id]
destination:
  connector: parquet
  path: {destination}
"#,
        ),
        (
            "rename",
            r#"
version: 1
source:
  connector: local_file
  path: missing-source.csv
  format: csv
destination:
  connector: parquet
  path: {destination}
  rename: customers
"#,
        ),
        (
            "checksum",
            r#"
version: 1
source:
  connector: local_file
  path: missing-source.csv
  format: csv
destination:
  connector: parquet
  path: {destination}
schema:
  pinned_path: customers.schema.yml
  checksum: abc123
"#,
        ),
        (
            "retention_days",
            r#"
version: 1
source:
  connector: local_file
  path: missing-source.csv
  format: csv
destination:
  connector: parquet
  path: {destination}
artifacts:
  dir: definition-artifacts
  retention_days: 7
"#,
        ),
    ];

    for (unknown_field, template) in cases {
        let definition_text =
            template.replace("{destination}", &destination_path.display().to_string());
        let definition_path = work.path().join(format!("load-{unknown_field}.yml"));
        fs::write(&definition_path, definition_text).expect("write load definition");
        let artifacts_dir = work.path().join(format!("artifacts-{unknown_field}"));

        Command::cargo_bin("data-spark")
            .expect("binary")
            .current_dir(work.path())
            .arg("load")
            .arg("--output-dir")
            .arg(&artifacts_dir)
            .arg(&definition_path)
            .assert()
            .failure();

        assert!(
            !destination_path.exists(),
            "unknown {unknown_field} key must fail before destination writing"
        );
        let (_, report) = read_single_report(
            &artifacts_dir,
            "failed strict parse still has one artifact directory",
        );
        assert_eq!(report["report_version"], 1);
        assert_eq!(report["exit_status"], "failed");
        assert_eq!(report["process_exit_code"], 1);
        assert_eq!(
            report["error_summary"]["code"], "invalid_load_definition_yaml",
            "code for unknown {unknown_field}"
        );
        let message = report["error_summary"]["message"]
            .as_str()
            .expect("error message");
        assert!(
            message.contains(&format!("unknown field `{unknown_field}`")),
            "error text must identify {unknown_field}: {message}"
        );
    }
}

#[test]
fn unknown_pinned_schema_key_fails_before_source_or_destination_work() {
    // Strict pin contract (ADR-0037): the pin parse failure fires while the
    // source file is missing and before the destination exists, proving the
    // load stopped at the schema directive (ADR-0019 ordering).
    let work = TempDir::new().expect("tempdir");
    let destination_path = work.path().join("should-not-exist.parquet");
    let pinned_path = work.path().join("customers.schema.yml");
    fs::write(
        &pinned_path,
        "version: 1\nowner: bi-team\nfields:\n- name: customer_id\n  type: int64\n",
    )
    .expect("write pinned schema");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: missing-source.csv
  format: csv
destination:
  connector: parquet
  path: {}
dataset: customers
load_mode: full_refresh
schema:
  pinned_path: {}
"#,
            destination_path.display(),
            pinned_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    let assert = Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .failure();

    assert!(
        !destination_path.exists(),
        "an invalid pinned schema must fail before destination writing"
    );

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let (report_path, report) = read_single_report(
        &artifacts_dir,
        "failed pin parse still has one artifact directory",
    );

    assert_eq!(report["report_version"], 1);
    assert_eq!(report["exit_status"], "failed");
    assert_eq!(report["process_exit_code"], 1);
    assert_eq!(report["error_summary"]["code"], "invalid_pinned_schema");
    let message = report["error_summary"]["message"]
        .as_str()
        .expect("error message");
    assert!(message.contains("failed to parse pinned schema"));
    assert!(
        message.contains("unknown field `owner`"),
        "error text must identify the rejected field: {message}"
    );

    assert!(stdout.contains("failed"));
    assert!(stdout.contains("unknown field `owner`"));
    assert!(stdout.contains(report_path.to_str().expect("report path")));
}

#[test]
fn load_without_output_dir_uses_default_data_spark_runs_directory() {
    let work = TempDir::new().expect("tempdir");
    fs::write(
        work.path().join("customers.csv"),
        "customer_id,name\n1,Ada\n",
    )
    .expect("write source csv");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        r#"
version: 1
source:
  connector: local_file
  path: customers.csv
  format: csv
destination:
  connector: parquet
  path: customers_dataset
dataset: customers
load_mode: full_refresh
"#,
    )
    .expect("write load definition");

    let assert = Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg(&definition_path)
        .assert()
        .success();

    let default_runs_dir = work.path().join(".data-spark").join("runs");
    let (report_path, report) = read_single_report(
        &default_runs_dir,
        "default runs directory has one load artifact",
    );
    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");

    assert_eq!(report["report_version"], 1);
    assert_eq!(report["exit_status"], "succeeded");
    assert!(report["artifact_dir"]
        .as_str()
        .expect("artifact dir")
        .contains(".data-spark/runs"));
    assert!(report_path.ends_with("load-report.json"));
    assert!(stdout.contains(".data-spark/runs"));
    assert!(stdout.contains("load-report.json"));
}

#[test]
fn load_definition_artifact_dir_redirects_report_and_rejected_records() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    fs::write(
        &source_path,
        "customer_id,name\n1,Ada\n2,Grace,extra-field\n",
    )
    .expect("write source csv");

    let destination_path = work.path().join("customers_dataset");
    let artifacts_dir = work.path().join("definition-artifacts");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: parquet
  path: {}
dataset: customers
load_mode: full_refresh
reject_threshold: 1
artifacts:
  dir: {}
"#,
            source_path.display(),
            destination_path.display(),
            artifacts_dir.display()
        ),
    )
    .expect("write load definition");

    let assert = Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg(&definition_path)
        .assert()
        .success();

    assert!(
        !work.path().join(".data-spark").join("runs").exists(),
        "a definition artifact directory must replace the default root"
    );

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let (report_path, report) = read_single_report(
        &artifacts_dir,
        "definition artifact root has one load directory",
    );

    assert_eq!(report["report_version"], 1);
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(report["process_exit_code"], 0);
    assert_eq!(report["row_counts"]["source"], 2);
    assert_eq!(report["row_counts"]["written"], 1);
    assert_eq!(report["row_counts"]["rejected"], 1);
    assert_eq!(
        PathBuf::from(report["artifact_dir"].as_str().expect("artifact directory")),
        report_path.parent().expect("report parent")
    );

    let rejected_path = PathBuf::from(
        report["rejected_records"]["artifact"]
            .as_str()
            .expect("rejected artifact path"),
    );
    assert_eq!(rejected_path.parent(), report_path.parent());
    let rejected = read_rejected_records(&rejected_path);
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0]["code"], "malformed_csv_record");

    let batch = read_single_parquet_batch(&single_parquet_file(&destination_path));
    assert_eq!(batch.num_rows(), 1);
    assert!(stdout.contains(report_path.to_str().expect("report path")));
    assert!(stdout.contains(rejected_path.to_str().expect("rejected artifact path")));
}

#[test]
fn cli_output_dir_overrides_the_load_definition_artifact_dir() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    fs::write(&source_path, "customer_id,name\n1,Ada\n").expect("write source csv");

    let destination_path = work.path().join("customers_dataset");
    let definition_artifacts = work.path().join("definition-artifacts");
    let cli_artifacts = work.path().join("cli-artifacts");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: parquet
  path: {}
dataset: customers
load_mode: full_refresh
artifacts:
  dir: {}
"#,
            source_path.display(),
            destination_path.display(),
            definition_artifacts.display()
        ),
    )
    .expect("write load definition");

    let assert = Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&cli_artifacts)
        .arg(&definition_path)
        .assert()
        .success();

    assert!(
        !definition_artifacts.exists(),
        "the one-off CLI redirect must override the repeatable definition root"
    );
    assert!(!work.path().join(".data-spark").join("runs").exists());

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let (report_path, report) =
        read_single_report(&cli_artifacts, "CLI artifact root has one load directory");
    assert_eq!(report["report_version"], 1);
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(
        PathBuf::from(report["artifact_dir"].as_str().expect("artifact directory")),
        report_path.parent().expect("report parent")
    );
    assert!(stdout.contains(report_path.to_str().expect("report path")));
}

#[test]
fn local_csv_pinned_schema_bootstraps_then_reuses_the_pin_for_parquet() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    fs::write(
        &source_path,
        "customer_id,name,total\n1,Ada,42.50\n2,Grace,7.25\n",
    )
    .expect("write source csv");

    let destination_path = work.path().join("customers_dataset");
    let pinned_path = work.path().join("customers.schema.yml");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: parquet
  path: {}
dataset: customers
load_mode: full_refresh
schema:
  pinned_path: {}
"#,
            source_path.display(),
            destination_path.display(),
            pinned_path.display()
        ),
    )
    .expect("write load definition");

    // First load: no pinned schema file exists yet, so the inferred schema is
    // persisted as the new pin.
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(work.path().join("artifacts-first"))
        .arg(&definition_path)
        .assert()
        .success();

    assert_eq!(
        fs::read_to_string(&pinned_path).expect("pinned schema file persisted"),
        "version: 1\n\
         fields:\n\
         - name: customer_id\n\
         \x20 type: int64\n\
         \x20 nullable: true\n\
         - name: name\n\
         \x20 type: utf8\n\
         \x20 nullable: true\n\
         - name: total\n\
         \x20 type: float64\n\
         \x20 nullable: true\n"
    );
    let (_, first_report) = read_single_report(
        &work.path().join("artifacts-first"),
        "first pinned load writes one artifact directory",
    );
    assert_eq!(
        first_report["schema_decision"],
        serde_json::json!({
            "mode": "inferred",
            "fields": [
                {"name": "customer_id", "type": "int64", "nullable": true},
                {"name": "name", "type": "utf8", "nullable": true},
                {"name": "total", "type": "float64", "nullable": true}
            ],
            "drift_status": "not_applicable",
            "pinned_schema_path": pinned_path.display().to_string(),
            "pinned_schema_persisted": true
        })
    );

    // Repeat load with matching records: validated against the pin.
    fs::write(&source_path, "customer_id,name,total\n3,Katherine,100.00\n")
        .expect("write replacement source csv");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(work.path().join("artifacts-second"))
        .arg(&definition_path)
        .assert()
        .success();

    let (_, second_report) = read_single_report(
        &work.path().join("artifacts-second"),
        "second pinned load writes one artifact directory",
    );
    assert_eq!(
        second_report["schema_decision"],
        serde_json::json!({
            "mode": "pinned",
            "fields": [
                {"name": "customer_id", "type": "int64", "nullable": true},
                {"name": "name", "type": "utf8", "nullable": true},
                {"name": "total", "type": "float64", "nullable": true}
            ],
            "drift_status": "none",
            "pinned_schema_path": pinned_path.display().to_string()
        })
    );
    assert_eq!(second_report["exit_status"], "succeeded");

    let parquet_path = single_parquet_file(&destination_path);
    let batch = read_single_parquet_batch(&parquet_path);
    assert_eq!(batch.num_rows(), 1);
    let customer_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("customer_id is int64");
    assert_eq!(customer_ids.value(0), 3);
}

#[test]
fn local_jsonl_pinned_schema_bootstraps_then_reuses_the_pin_for_parquet() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.jsonl");
    fs::write(
        &source_path,
        "{\"customer_id\": 1, \"name\": \"Ada\", \"total\": 42.50, \"active\": true}\n",
    )
    .expect("write source jsonl");

    let destination_path = work.path().join("customers_dataset");
    let pinned_path = work.path().join("customers.schema.yml");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: jsonl
destination:
  connector: parquet
  path: {}
dataset: customers
load_mode: full_refresh
schema:
  pinned_path: {}
"#,
            source_path.display(),
            destination_path.display(),
            pinned_path.display()
        ),
    )
    .expect("write load definition");

    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(work.path().join("artifacts-first"))
        .arg(&definition_path)
        .assert()
        .success();

    assert_eq!(
        fs::read_to_string(&pinned_path).expect("pinned schema file persisted"),
        "version: 1\n\
         fields:\n\
         - name: customer_id\n\
         \x20 type: int64\n\
         \x20 nullable: true\n\
         - name: name\n\
         \x20 type: utf8\n\
         \x20 nullable: true\n\
         - name: total\n\
         \x20 type: float64\n\
         \x20 nullable: true\n\
         - name: active\n\
         \x20 type: boolean\n\
         \x20 nullable: true\n"
    );

    fs::write(
        &source_path,
        "{\"customer_id\": 3, \"name\": \"Katherine\", \"total\": 100.00, \"active\": false}\n",
    )
    .expect("write replacement source jsonl");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(work.path().join("artifacts-second"))
        .arg(&definition_path)
        .assert()
        .success();

    let (_, second_report) = read_single_report(
        &work.path().join("artifacts-second"),
        "second pinned jsonl load writes one artifact directory",
    );
    assert_eq!(second_report["schema_decision"]["mode"], "pinned");
    assert_eq!(second_report["schema_decision"]["drift_status"], "none");

    let parquet_path = single_parquet_file(&destination_path);
    let batch = read_single_parquet_batch(&parquet_path);
    assert_eq!(batch.num_rows(), 1);
    let customer_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("customer_id is int64");
    assert_eq!(customer_ids.value(0), 3);
}

#[test]
fn local_csv_pinned_schema_bootstraps_then_reuses_the_pin_for_duckdb() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    fs::write(
        &source_path,
        "customer_id,name,total\n1,Ada,42.50\n2,Grace,7.25\n",
    )
    .expect("write source csv");

    let database_path = work.path().join("customers.duckdb");
    let pinned_path = work.path().join("customers.schema.yml");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: duckdb
  path: {}
dataset: customers
load_mode: full_refresh
schema:
  pinned_path: {}
"#,
            source_path.display(),
            database_path.display(),
            pinned_path.display()
        ),
    )
    .expect("write load definition");

    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(work.path().join("artifacts-first"))
        .arg(&definition_path)
        .assert()
        .success();

    assert!(
        pinned_path.exists(),
        "first load persists the pinned schema file"
    );

    fs::write(&source_path, "customer_id,name,total\n3,Katherine,100.00\n")
        .expect("write replacement source csv");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(work.path().join("artifacts-second"))
        .arg(&definition_path)
        .assert()
        .success();

    let (_, second_report) = read_single_report(
        &work.path().join("artifacts-second"),
        "second pinned duckdb load writes one artifact directory",
    );
    assert_eq!(second_report["schema_decision"]["mode"], "pinned");
    assert_eq!(second_report["schema_decision"]["drift_status"], "none");

    let batch = read_single_duckdb_batch(&database_path, "customers");
    assert_eq!(batch.num_rows(), 1);
    let customer_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("customer_id is int64");
    assert_eq!(customer_ids.value(0), 3);
}

#[test]
fn local_jsonl_pinned_schema_bootstraps_then_reuses_the_pin_for_duckdb() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.jsonl");
    fs::write(
        &source_path,
        "{\"customer_id\": 1, \"name\": \"Ada\", \"total\": 42.50, \"active\": true}\n",
    )
    .expect("write source jsonl");

    let database_path = work.path().join("customers.duckdb");
    let pinned_path = work.path().join("customers.schema.yml");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: jsonl
destination:
  connector: duckdb
  path: {}
dataset: customers
load_mode: full_refresh
schema:
  pinned_path: {}
"#,
            source_path.display(),
            database_path.display(),
            pinned_path.display()
        ),
    )
    .expect("write load definition");

    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(work.path().join("artifacts-first"))
        .arg(&definition_path)
        .assert()
        .success();

    assert!(
        pinned_path.exists(),
        "first load persists the pinned schema file"
    );

    fs::write(
        &source_path,
        "{\"customer_id\": 3, \"name\": \"Katherine\", \"total\": 100.00, \"active\": false}\n",
    )
    .expect("write replacement source jsonl");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(work.path().join("artifacts-second"))
        .arg(&definition_path)
        .assert()
        .success();

    let (_, second_report) = read_single_report(
        &work.path().join("artifacts-second"),
        "second pinned jsonl duckdb load writes one artifact directory",
    );
    assert_eq!(second_report["schema_decision"]["mode"], "pinned");
    assert_eq!(second_report["schema_decision"]["drift_status"], "none");

    let batch = read_single_duckdb_batch(&database_path, "customers");
    assert_eq!(batch.num_rows(), 1);
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name is string");
    assert_eq!(names.value(0), "Katherine");
}

#[test]
fn schema_drift_fails_fast_by_default_before_destination_writing() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    fs::write(&source_path, "customer_id,name\n1,Ada\n").expect("write source csv");

    let destination_path = work.path().join("customers_dataset");
    let pinned_path = work.path().join("customers.schema.yml");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: parquet
  path: {}
dataset: customers
load_mode: full_refresh
schema:
  pinned_path: {}
"#,
            source_path.display(),
            destination_path.display(),
            pinned_path.display()
        ),
    )
    .expect("write load definition");

    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(work.path().join("artifacts-first"))
        .arg(&definition_path)
        .assert()
        .success();

    // The source grows a `vip` column; the default drift policy fails the load.
    fs::write(&source_path, "customer_id,name,vip\n3,Katherine,true\n")
        .expect("write drifted source csv");
    let assert = Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(work.path().join("artifacts-second"))
        .arg(&definition_path)
        .assert()
        .failure();

    // Fail fast before destination writing: the destination still holds the
    // first load's records.
    let parquet_path = single_parquet_file(&destination_path);
    let batch = read_single_parquet_batch(&parquet_path);
    assert_eq!(batch.num_rows(), 1);
    let customer_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("customer_id is int64");
    assert_eq!(customer_ids.value(0), 1);

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let (_, report) = read_single_report(
        &work.path().join("artifacts-second"),
        "drift-failed load writes one artifact directory",
    );
    assert_eq!(report["exit_status"], "failed");
    assert_eq!(report["process_exit_code"], 1);
    assert_eq!(report["row_counts"]["source"], 0);
    assert_eq!(report["row_counts"]["written"], 0);
    assert_eq!(report["destination_write"]["atomicity"], "not_applicable");
    assert_eq!(report["error_summary"]["code"], "schema_drift");
    assert!(report["error_summary"]["message"]
        .as_str()
        .expect("error message")
        .contains("added fields: vip"));
    assert_eq!(report["schema_decision"]["mode"], "pinned");
    assert_eq!(report["schema_decision"]["drift_status"], "failed_on_drift");
    assert_eq!(
        report["schema_decision"]["drift"]["added_fields"],
        serde_json::json!(["vip"])
    );
    assert_eq!(
        report["schema_decision"]["pinned_schema_path"],
        pinned_path.display().to_string()
    );

    assert!(stdout.contains("Status: failed"));
    assert!(stdout.contains("schema drift against pinned schema"));
}

#[test]
fn additive_nullable_drift_continues_and_extends_the_pin_when_explicitly_allowed() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.jsonl");
    fs::write(
        &source_path,
        "{\"customer_id\": 1, \"name\": \"Ada\"}\n{\"customer_id\": 2, \"name\": \"Grace\"}\n",
    )
    .expect("write source jsonl");

    let database_path = work.path().join("customers.duckdb");
    let pinned_path = work.path().join("customers.schema.yml");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: jsonl
destination:
  connector: duckdb
  path: {}
dataset: customers
load_mode: full_refresh
schema:
  pinned_path: {}
  drift_policy: allow_additive_nullable
"#,
            source_path.display(),
            database_path.display(),
            pinned_path.display()
        ),
    )
    .expect("write load definition");

    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(work.path().join("artifacts-first"))
        .arg(&definition_path)
        .assert()
        .success();

    // The source grows a `vip` field that one record leaves absent; the
    // explicit additive policy lets the load continue.
    fs::write(
        &source_path,
        "{\"customer_id\": 3, \"name\": \"Katherine\", \"vip\": true}\n\
         {\"customer_id\": 4, \"name\": \"Lin\"}\n",
    )
    .expect("write additive source jsonl");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(work.path().join("artifacts-second"))
        .arg(&definition_path)
        .assert()
        .success();

    let (_, report) = read_single_report(
        &work.path().join("artifacts-second"),
        "additive load writes one artifact directory",
    );
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(report["schema_decision"]["mode"], "pinned");
    assert_eq!(
        report["schema_decision"]["drift_status"],
        "additive_fields_added"
    );
    assert_eq!(
        report["schema_decision"]["added_fields"],
        serde_json::json!([{"name": "vip", "type": "boolean", "nullable": true}])
    );
    assert_eq!(report["schema_decision"]["pinned_schema_persisted"], true);

    // The pin now carries the added field.
    assert_eq!(
        fs::read_to_string(&pinned_path).expect("pinned schema file"),
        "version: 1\n\
         fields:\n\
         - name: customer_id\n\
         \x20 type: int64\n\
         \x20 nullable: true\n\
         - name: name\n\
         \x20 type: utf8\n\
         \x20 nullable: true\n\
         - name: vip\n\
         \x20 type: boolean\n\
         \x20 nullable: true\n"
    );

    let batch = read_single_duckdb_batch(&database_path, "customers");
    assert_eq!(batch.num_rows(), 2);
    let vips = batch
        .column(2)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("vip is boolean");
    assert!(vips.value(0), "customer 3 is vip");
    assert!(vips.is_null(1), "absent vip lands as null");
}

#[test]
fn type_misfits_under_a_pin_reject_records_and_respect_the_write_boundary() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    fs::write(&source_path, "customer_id,total\n1,10.5\n").expect("write source csv");

    let database_path = work.path().join("customers.duckdb");
    let pinned_path = work.path().join("customers.schema.yml");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: duckdb
  path: {}
dataset: customers
load_mode: full_refresh
schema:
  pinned_path: {}
  drift_policy: allow_additive_nullable
"#,
            source_path.display(),
            database_path.display(),
            pinned_path.display()
        ),
    )
    .expect("write load definition");

    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(work.path().join("artifacts-first"))
        .arg(&definition_path)
        .assert()
        .success();

    // `customer_id` value "abc" misfits the pinned int64: the record is
    // rejected (ADR-0035) and the default reject threshold of 0 fails the
    // load. The source shape still matches the pin, so this is not drift.
    fs::write(&source_path, "customer_id,total\nabc,7\n").expect("write misfit source csv");
    let assert = Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(work.path().join("artifacts-second"))
        .arg(&definition_path)
        .assert()
        .failure();

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let (_, report) = read_single_report(
        &work.path().join("artifacts-second"),
        "type-misfit load writes one artifact directory",
    );
    assert_eq!(report["error_summary"]["code"], "reject_threshold_exceeded");
    assert_eq!(
        report["error_summary"]["message"],
        "rejected 1 of 1 records, exceeding the reject threshold of 0"
    );
    assert_eq!(report["schema_decision"]["mode"], "pinned");
    assert_eq!(report["schema_decision"]["drift_status"], "none");
    assert_eq!(report["row_counts"]["rejected"], 1);
    assert!(stdout.contains("Status: failed"));

    let artifact_path = PathBuf::from(
        report["rejected_records"]["artifact"]
            .as_str()
            .expect("artifact path"),
    );
    let rejected_lines = read_rejected_records(&artifact_path);
    assert_eq!(rejected_lines.len(), 1);
    assert_eq!(rejected_lines[0]["line"], 2);
    assert_eq!(rejected_lines[0]["code"], "type_coercion_failed");
    assert_eq!(rejected_lines[0]["field"], "customer_id");
    assert_eq!(
        rejected_lines[0]["message"],
        "value \"abc\" does not fit pinned type int64 for field \"customer_id\""
    );
    assert_eq!(
        rejected_lines[0]["record"],
        serde_json::json!({"customer_id": "abc", "total": "7"})
    );

    // Write boundary (ADR-0019): the threshold failed the load before any
    // destination write, so the destination still holds the first load's
    // records.
    let batch = read_single_duckdb_batch(&database_path, "customers");
    assert_eq!(batch.num_rows(), 1);
    let customer_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("customer_id is int64");
    assert_eq!(customer_ids.value(0), 1);
}

#[test]
fn csv_overrides_narrow_an_inferred_text_column_and_reject_dirty_records_for_duckdb() {
    // The ADR-0038 core scenario: `customer_id` infers utf8 only because of
    // the dirty "n/a" value, and a standalone override (no pinned_path)
    // corrects the column to its true int64 type while the dirty record is
    // rejected under the threshold.
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    fs::write(&source_path, "customer_id,name\n1,Ada\nn/a,Grace\n3,Lin\n")
        .expect("write source csv");

    let database_path = work.path().join("customers.duckdb");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: duckdb
  path: {}
dataset: customers
load_mode: full_refresh
schema:
  overrides:
  - name: customer_id
    type: int64
reject_threshold: 1
"#,
            source_path.display(),
            database_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .success();

    let (_, report) = read_single_report(
        &artifacts_dir,
        "override load writes one artifact directory",
    );
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(report["row_counts"]["source"], 3);
    assert_eq!(report["row_counts"]["written"], 2);
    assert_eq!(report["row_counts"]["rejected"], 1);
    assert_eq!(
        report["schema_decision"],
        serde_json::json!({
            "mode": "inferred",
            "fields": [
                {"name": "customer_id", "type": "int64", "nullable": true},
                {"name": "name", "type": "utf8", "nullable": true}
            ],
            "drift_status": "not_applicable",
            "overrides": [
                {"name": "customer_id", "type": "int64"}
            ]
        })
    );

    let artifact_path = PathBuf::from(
        report["rejected_records"]["artifact"]
            .as_str()
            .expect("artifact path"),
    );
    let rejected_lines = read_rejected_records(&artifact_path);
    assert_eq!(rejected_lines.len(), 1);
    assert_eq!(rejected_lines[0]["line"], 3);
    assert_eq!(rejected_lines[0]["code"], "type_coercion_failed");
    assert_eq!(rejected_lines[0]["field"], "customer_id");
    assert_eq!(
        rejected_lines[0]["message"],
        "value \"n/a\" does not fit overridden type int64 for field \"customer_id\""
    );

    // DuckDB reads the overridden column as BIGINT, not the inferred VARCHAR.
    let batch = read_single_duckdb_batch(&database_path, "customers");
    assert_eq!(batch.num_rows(), 2);
    let customer_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("customer_id is int64");
    assert_eq!(customer_ids.value(0), 1);
    assert_eq!(customer_ids.value(1), 3);
}

#[test]
fn jsonl_non_nullable_override_rejects_null_and_absent_records_for_parquet() {
    // A `nullable: false` override makes the field required with pinned-field
    // per-record semantics (ADR-0038): a JSON null and an omitted field both
    // reject their record, and the Parquet schema shows the field as
    // required.
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.jsonl");
    fs::write(
        &source_path,
        "{\"customer_id\": 1, \"email\": \"ada@example.com\"}\n\
         {\"customer_id\": 2, \"email\": null}\n\
         {\"customer_id\": 3}\n",
    )
    .expect("write source jsonl");

    let destination_path = work.path().join("customers_dataset");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: jsonl
destination:
  connector: parquet
  path: {}
dataset: customers
load_mode: full_refresh
schema:
  overrides:
  - name: email
    nullable: false
reject_threshold: 2
"#,
            source_path.display(),
            destination_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .success();

    let (_, report) = read_single_report(
        &artifacts_dir,
        "override load writes one artifact directory",
    );
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(report["row_counts"]["written"], 1);
    assert_eq!(report["row_counts"]["rejected"], 2);
    assert_eq!(
        report["schema_decision"]["fields"][1],
        serde_json::json!({"name": "email", "type": "utf8", "nullable": false})
    );
    assert_eq!(
        report["schema_decision"]["overrides"],
        serde_json::json!([{"name": "email", "nullable": false}])
    );

    let artifact_path = PathBuf::from(
        report["rejected_records"]["artifact"]
            .as_str()
            .expect("artifact path"),
    );
    let rejected_lines = read_rejected_records(&artifact_path);
    assert_eq!(rejected_lines.len(), 2);
    for (rejected, line, record) in [
        (
            &rejected_lines[0],
            2,
            serde_json::json!({"customer_id": 2, "email": null}),
        ),
        (&rejected_lines[1], 3, serde_json::json!({"customer_id": 3})),
    ] {
        assert_eq!(rejected["line"], line);
        assert_eq!(rejected["code"], "missing_required_field");
        assert_eq!(rejected["field"], "email");
        assert_eq!(rejected["message"], "required field \"email\" is null");
        assert_eq!(rejected["record"], record);
    }

    // The Parquet schema records the overridden field as required.
    let batch = read_single_parquet_batch(&single_parquet_file(&destination_path));
    assert_eq!(batch.num_rows(), 1);
    assert!(!batch.schema().field(1).is_nullable());
    let emails = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("email is utf8");
    assert_eq!(emails.value(0), "ada@example.com");
}

#[test]
fn csv_override_bootstrap_persists_the_overridden_pin_then_reuses_it_for_parquet() {
    // The bootstrap load persists the *overridden* schema as the pin
    // (ADR-0038): `customer_id` would infer int64, but the override keeps it
    // text, so the pin and the destination both record utf8 — and a second
    // run of the same definition finds the override consistent with the pin.
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    fs::write(&source_path, "customer_id,name\n1,Ada\n2,Grace\n").expect("write source csv");

    let destination_path = work.path().join("customers_dataset");
    let pinned_path = work.path().join("customers.schema.yml");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: parquet
  path: {}
dataset: customers
load_mode: full_refresh
schema:
  pinned_path: {}
  overrides:
  - name: customer_id
    type: utf8
"#,
            source_path.display(),
            destination_path.display(),
            pinned_path.display()
        ),
    )
    .expect("write load definition");

    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(work.path().join("artifacts-first"))
        .arg(&definition_path)
        .assert()
        .success();

    assert_eq!(
        fs::read_to_string(&pinned_path).expect("pinned schema file persisted"),
        "version: 1\n\
         fields:\n\
         - name: customer_id\n\
         \x20 type: utf8\n\
         \x20 nullable: true\n\
         - name: name\n\
         \x20 type: utf8\n\
         \x20 nullable: true\n"
    );
    let (_, first_report) = read_single_report(
        &work.path().join("artifacts-first"),
        "bootstrap load writes one artifact directory",
    );
    assert_eq!(first_report["schema_decision"]["mode"], "inferred");
    assert_eq!(
        first_report["schema_decision"]["pinned_schema_persisted"],
        true
    );
    assert_eq!(
        first_report["schema_decision"]["fields"][0],
        serde_json::json!({"name": "customer_id", "type": "utf8", "nullable": true})
    );

    // Repeat load: the pin now governs `customer_id`, and the override agrees
    // with it.
    fs::write(&source_path, "customer_id,name\n3,Katherine\n")
        .expect("write replacement source csv");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(work.path().join("artifacts-second"))
        .arg(&definition_path)
        .assert()
        .success();

    let (_, second_report) = read_single_report(
        &work.path().join("artifacts-second"),
        "pinned load writes one artifact directory",
    );
    assert_eq!(second_report["schema_decision"]["mode"], "pinned");
    assert_eq!(second_report["schema_decision"]["drift_status"], "none");
    assert_eq!(
        second_report["schema_decision"]["overrides"],
        serde_json::json!([{"name": "customer_id", "type": "utf8"}])
    );

    // The destination holds the overridden text column, not the inferred
    // int64.
    let batch = read_single_parquet_batch(&single_parquet_file(&destination_path));
    assert_eq!(batch.num_rows(), 1);
    let customer_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("customer_id is utf8");
    assert_eq!(customer_ids.value(0), "3");
}

#[test]
fn jsonl_override_conflicting_with_the_pin_fails_for_duckdb() {
    // A field an existing pin governs takes nothing from an override, but the
    // override must agree with it: the bootstrapped pin declares
    // `customer_id` nullable, so a later `nullable: false` override is an
    // override conflict (ADR-0038) — the load fails before touching the
    // destination, and the report carries the conflict detail.
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.jsonl");
    fs::write(&source_path, "{\"customer_id\": 1, \"name\": \"Ada\"}\n")
        .expect("write source jsonl");

    let database_path = work.path().join("customers.duckdb");
    let pinned_path = work.path().join("customers.schema.yml");
    let definition_path = work.path().join("load.yml");
    let definition = |overrides_block: &str| {
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: jsonl
destination:
  connector: duckdb
  path: {}
dataset: customers
load_mode: full_refresh
schema:
  pinned_path: {}
{overrides_block}"#,
            source_path.display(),
            database_path.display(),
            pinned_path.display()
        )
    };
    fs::write(&definition_path, definition("")).expect("write load definition");

    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(work.path().join("artifacts-first"))
        .arg(&definition_path)
        .assert()
        .success();

    // The definition now overrides a pinned field's nullability without
    // editing the pin.
    fs::write(
        &definition_path,
        definition("  overrides:\n  - name: customer_id\n    nullable: false\n"),
    )
    .expect("write conflicting load definition");
    let assert = Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(work.path().join("artifacts-second"))
        .arg(&definition_path)
        .assert()
        .failure();

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let (_, report) = read_single_report(
        &work.path().join("artifacts-second"),
        "conflict-failed load writes one artifact directory",
    );
    assert_eq!(report["exit_status"], "failed");
    assert_eq!(report["error_summary"]["code"], "schema_override_conflict");
    assert_eq!(
        report["error_summary"]["message"],
        format!(
            "schema override for field \"customer_id\" contradicts pinned schema {}: \
             pinned nullable true, override nullable false",
            pinned_path.display()
        )
    );
    assert_eq!(report["schema_decision"]["mode"], "pinned");
    assert_eq!(
        report["schema_decision"]["drift_status"], "not_applicable",
        "the load failed before any drift comparison"
    );
    assert_eq!(
        report["schema_decision"]["conflict"],
        serde_json::json!({
            "field": "customer_id",
            "pinned": {"type": "int64", "nullable": true},
            "override": {"nullable": false}
        })
    );
    assert_eq!(
        report["schema_decision"]["overrides"],
        serde_json::json!([{"name": "customer_id", "nullable": false}])
    );
    assert!(stdout.contains("contradicts pinned schema"));

    // Write boundary (ADR-0019): the destination still holds the first
    // load's record.
    let batch = read_single_duckdb_batch(&database_path, "customers");
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn unknown_override_fields_fail_before_missing_field_drift() {
    // The override names a pinned field the source no longer carries: both an
    // unknown override name and missing-field drift are present, and the load
    // fails as unknown_override_field — the override check runs as soon as
    // the observed names are known, before any pin comparison (ADR-0038).
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    fs::write(&source_path, "customer_id\n1\n").expect("write source csv");

    let pinned_path = work.path().join("customers.schema.yml");
    fs::write(
        &pinned_path,
        "version: 1\n\
         fields:\n\
         - name: customer_id\n\
         \x20 type: int64\n\
         - name: name\n\
         \x20 type: utf8\n",
    )
    .expect("write pinned schema");

    let destination_path = work.path().join("customers_dataset");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: parquet
  path: {}
dataset: customers
load_mode: full_refresh
schema:
  pinned_path: {}
  overrides:
  - name: name
    nullable: false
"#,
            source_path.display(),
            destination_path.display(),
            pinned_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .failure();

    assert!(
        !destination_path.exists(),
        "an unknown override field must fail before destination writing"
    );
    let (_, report) = read_single_report(
        &artifacts_dir,
        "override-failed load writes one artifact directory",
    );
    assert_eq!(report["error_summary"]["code"], "unknown_override_field");
    assert_eq!(
        report["error_summary"]["message"],
        "schema overrides name fields absent from the observed source shape: name"
    );
    assert_eq!(report["schema_decision"]["mode"], "pinned");
    assert_eq!(report["schema_decision"]["drift_status"], "not_applicable");
    assert_eq!(
        report["schema_decision"]["overrides"],
        serde_json::json!([{"name": "name", "nullable": false}])
    );
}

#[test]
fn invalid_override_configs_fail_before_source_or_destination_work() {
    // The override failure taxonomy (ADR-0038), one format each: every case
    // is definition validation, so the missing source file and the
    // never-created destination prove the failure boundary (ADR-0019).
    let work = TempDir::new().expect("tempdir");
    let destination_path = work.path().join("should-not-exist");
    let cases = [
        (
            "unsupported-type",
            "csv",
            "  - name: customer_id\n    type: date\n",
            "unsupported_override_type",
            "unsupported schema override type for field \"customer_id\": date",
        ),
        (
            "duplicate-names",
            "jsonl",
            "  - name: customer_id\n    type: int64\n  - name: customer_id\n    nullable: false\n",
            "invalid_schema_config",
            "schema override for field \"customer_id\" is declared more than once",
        ),
        (
            "no-op-entry",
            "csv",
            "  - name: customer_id\n",
            "invalid_schema_config",
            "schema override for field \"customer_id\" must set at least one of type or nullable",
        ),
        (
            "unknown-entry-key",
            "jsonl",
            "  - name: customer_id\n    coerce: true\n",
            "invalid_load_definition_yaml",
            "unknown field `coerce`",
        ),
    ];

    for (label, format, overrides_block, expected_code, expected_message_part) in cases {
        let definition_path = work.path().join(format!("load-{label}.yml"));
        fs::write(
            &definition_path,
            format!(
                "version: 1\n\
                 source:\n\
                 \x20 connector: local_file\n\
                 \x20 path: missing-source.{format}\n\
                 \x20 format: {format}\n\
                 destination:\n\
                 \x20 connector: parquet\n\
                 \x20 path: {}\n\
                 schema:\n\
                 \x20 overrides:\n{overrides_block}",
                destination_path.display()
            ),
        )
        .expect("write load definition");
        let artifacts_dir = work.path().join(format!("artifacts-{label}"));

        Command::cargo_bin("data-spark")
            .expect("binary")
            .current_dir(work.path())
            .arg("load")
            .arg("--output-dir")
            .arg(&artifacts_dir)
            .arg(&definition_path)
            .assert()
            .failure();

        assert!(
            !destination_path.exists(),
            "case {label} must fail before destination writing"
        );
        let (_, report) = read_single_report(
            &artifacts_dir,
            "failed override config still has one artifact directory",
        );
        assert_eq!(
            report["error_summary"]["code"], expected_code,
            "code for case {label}"
        );
        let message = report["error_summary"]["message"]
            .as_str()
            .expect("error message");
        assert!(
            message.contains(expected_message_part),
            "case {label} message {message:?} misses {expected_message_part:?}"
        );
    }
}

#[test]
fn additive_override_shapes_the_added_field_and_the_rewritten_pin_for_parquet() {
    // Under the additive policy an override naming the added field is
    // explicit intent (ADR-0038): it beats the policy's nullable default, the
    // required field rejects the record that omits it, and the rewritten pin
    // records the overridden properties.
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.jsonl");
    fs::write(
        &source_path,
        "{\"customer_id\": 1, \"vip\": \"gold\"}\n{\"customer_id\": 2}\n",
    )
    .expect("write source jsonl");

    let pinned_path = work.path().join("customers.schema.yml");
    fs::write(
        &pinned_path,
        "version: 1\n\
         fields:\n\
         - name: customer_id\n\
         \x20 type: int64\n\
         \x20 nullable: true\n",
    )
    .expect("write pinned schema");

    let destination_path = work.path().join("customers_dataset");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: jsonl
destination:
  connector: parquet
  path: {}
dataset: customers
load_mode: full_refresh
schema:
  pinned_path: {}
  drift_policy: allow_additive_nullable
  overrides:
  - name: vip
    type: utf8
    nullable: false
reject_threshold: 1
"#,
            source_path.display(),
            destination_path.display(),
            pinned_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .success();

    let (_, report) = read_single_report(
        &artifacts_dir,
        "additive override load writes one artifact directory",
    );
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(report["row_counts"]["written"], 1);
    assert_eq!(report["row_counts"]["rejected"], 1);
    assert_eq!(
        report["schema_decision"]["drift_status"],
        "additive_fields_added"
    );
    assert_eq!(
        report["schema_decision"]["added_fields"],
        serde_json::json!([{"name": "vip", "type": "utf8", "nullable": false}])
    );
    assert_eq!(
        report["schema_decision"]["overrides"],
        serde_json::json!([{"name": "vip", "type": "utf8", "nullable": false}])
    );

    let artifact_path = PathBuf::from(
        report["rejected_records"]["artifact"]
            .as_str()
            .expect("artifact path"),
    );
    let rejected_lines = read_rejected_records(&artifact_path);
    assert_eq!(rejected_lines.len(), 1);
    assert_eq!(rejected_lines[0]["line"], 2);
    assert_eq!(rejected_lines[0]["code"], "missing_required_field");
    assert_eq!(rejected_lines[0]["field"], "vip");

    // The rewritten pin records the overridden properties, so later loads
    // hold the field to them.
    assert_eq!(
        fs::read_to_string(&pinned_path).expect("pinned schema file"),
        "version: 1\n\
         fields:\n\
         - name: customer_id\n\
         \x20 type: int64\n\
         \x20 nullable: true\n\
         - name: vip\n\
         \x20 type: utf8\n\
         \x20 nullable: false\n"
    );

    // The destination materializes the overridden field: required utf8.
    let batch = read_single_parquet_batch(&single_parquet_file(&destination_path));
    assert_eq!(batch.num_rows(), 1);
    assert!(!batch.schema().field(1).is_nullable());
    let vips = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("vip is utf8");
    assert_eq!(vips.value(0), "gold");
}

#[test]
fn csv_transform_selects_and_renames_columns_for_duckdb() {
    // The happy transform path (ADR-0039): select fixes the dataset order
    // (total before id), rename maps id → customer_id, the unselected
    // `region` column never reaches the destination, and the report echoes
    // the transform as written.
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    fs::write(
        &source_path,
        "id,region,total\n1,north,42.5\n2,south,7.25\n",
    )
    .expect("write source csv");

    let database_path = work.path().join("customers.duckdb");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: duckdb
  path: {}
dataset: customers
load_mode: full_refresh
transform:
  select: [total, id]
  rename:
    id: customer_id
"#,
            source_path.display(),
            database_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .success();

    let (_, report) = read_single_report(
        &artifacts_dir,
        "transform load writes one artifact directory",
    );
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(report["row_counts"]["source"], 2);
    assert_eq!(report["row_counts"]["written"], 2);
    assert_eq!(report["row_counts"]["rejected"], 0);
    assert_eq!(
        report["schema_decision"],
        serde_json::json!({
            "mode": "inferred",
            "fields": [
                {"name": "total", "type": "float64", "nullable": true},
                {"name": "customer_id", "type": "int64", "nullable": true}
            ],
            "drift_status": "not_applicable",
            "transform": {
                "select": ["total", "id"],
                "rename": {"id": "customer_id"}
            }
        })
    );

    // The DuckDB table carries the dataset names in select order.
    let batch = read_single_duckdb_batch(&database_path, "customers");
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>(),
        vec!["total", "customer_id"]
    );
    assert_eq!(batch.num_rows(), 2);
    let totals = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("total is float64");
    let customer_ids = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("customer_id is int64");
    assert_eq!(totals.value(0), 7.25);
    assert_eq!(customer_ids.value(0), 2);
    assert_eq!(totals.value(1), 42.5);
    assert_eq!(customer_ids.value(1), 1);
}

#[test]
fn jsonl_transform_selects_and_renames_columns_for_parquet() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("orders.jsonl");
    fs::write(
        &source_path,
        "{\"id\": 1, \"note\": \"a\", \"total\": 10}\n\
         {\"id\": 2, \"note\": \"b\", \"total\": 42.5}\n",
    )
    .expect("write source jsonl");

    let destination_path = work.path().join("orders_dataset");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: jsonl
destination:
  connector: parquet
  path: {}
dataset: orders
load_mode: full_refresh
transform:
  select: [id, total]
  rename:
    total: amount
"#,
            source_path.display(),
            destination_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .success();

    let (_, report) = read_single_report(
        &artifacts_dir,
        "transform load writes one artifact directory",
    );
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(
        report["schema_decision"],
        serde_json::json!({
            "mode": "inferred",
            "fields": [
                {"name": "id", "type": "int64", "nullable": true},
                {"name": "amount", "type": "float64", "nullable": true}
            ],
            "drift_status": "not_applicable",
            "transform": {
                "select": ["id", "total"],
                "rename": {"total": "amount"}
            }
        })
    );

    // The Parquet schema carries the dataset names in select order, and the
    // renamed column reads its values from the source field.
    let batch = read_single_parquet_batch(&single_parquet_file(&destination_path));
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>(),
        vec!["id", "amount"]
    );
    assert_eq!(batch.num_rows(), 2);
    let amounts = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("amount is float64");
    assert_eq!(amounts.value(0), 10.0);
    assert_eq!(amounts.value(1), 42.5);
}

#[test]
fn invalid_transform_configs_fail_before_source_or_destination_work() {
    // A rename key outside the select list is a config-time failure
    // (ADR-0039): no I/O has happened, so the schema decision stays
    // not-evaluated, the missing source file is never read, and the
    // destination is never created.
    let work = TempDir::new().expect("tempdir");
    let database_path = work.path().join("should-not-exist.duckdb");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: missing-source.csv
  format: csv
destination:
  connector: duckdb
  path: {}
dataset: customers
load_mode: full_refresh
transform:
  select: [id]
  rename:
    total: amount
"#,
            database_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .failure();

    assert!(
        !database_path.exists(),
        "an invalid transform config must fail before destination writing"
    );
    let (_, report) = read_single_report(
        &artifacts_dir,
        "failed transform config still has one artifact directory",
    );
    assert_eq!(report["error_summary"]["code"], "invalid_transform_config");
    assert_eq!(
        report["error_summary"]["message"],
        "transform.rename key \"total\" is not in transform.select"
    );
    assert_eq!(
        report["schema_decision"],
        serde_json::json!({ "mode": "not_evaluated" })
    );
}

#[test]
fn unknown_transform_fields_fail_before_destination_writing() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    fs::write(&source_path, "id,name\n1,Ada\n").expect("write source csv");

    let database_path = work.path().join("should-not-exist.duckdb");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: duckdb
  path: {}
dataset: customers
load_mode: full_refresh
transform:
  select: [id, vip]
"#,
            source_path.display(),
            database_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .failure();

    assert!(
        !database_path.exists(),
        "an unknown transform field must fail before destination writing"
    );
    let (_, report) = read_single_report(
        &artifacts_dir,
        "transform-failed load writes one artifact directory",
    );
    assert_eq!(report["error_summary"]["code"], "unknown_transform_field");
    assert_eq!(
        report["error_summary"]["message"],
        "transform selects, renames, or flattens fields absent from the observed source shape: vip"
    );
    assert_eq!(
        report["schema_decision"],
        serde_json::json!({
            "mode": "inferred",
            "drift_status": "not_applicable",
            "transform": {
                "select": ["id", "vip"]
            }
        })
    );
}

#[test]
fn transform_name_collisions_fail_before_destination_writing() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    fs::write(&source_path, "id,legacy_id\n1,2\n").expect("write source csv");

    let database_path = work.path().join("should-not-exist.duckdb");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: duckdb
  path: {}
dataset: customers
load_mode: full_refresh
transform:
  rename:
    legacy_id: id
"#,
            source_path.display(),
            database_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .failure();

    assert!(
        !database_path.exists(),
        "a transform name collision must fail before destination writing"
    );
    let (_, report) = read_single_report(
        &artifacts_dir,
        "transform-failed load writes one artifact directory",
    );
    assert_eq!(report["error_summary"]["code"], "transform_name_collision");
    assert_eq!(
        report["error_summary"]["message"],
        "transform rename collides on dataset field \"id\": \
         source fields id, legacy_id map to the same name"
    );
    assert_eq!(
        report["schema_decision"],
        serde_json::json!({
            "mode": "inferred",
            "drift_status": "not_applicable",
            "transform": {
                "rename": {"legacy_id": "id"}
            }
        })
    );
}

#[test]
fn transform_pin_bootstrap_records_dataset_names_and_shields_unselected_drift() {
    // The bootstrap pin records the transformed dataset shape — dataset
    // names, in select order (ADR-0040) — and a later load whose source
    // gains an unselected field reports no drift even under the default
    // fail policy.
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    fs::write(&source_path, "id,note,total\n1,a,42.5\n").expect("write source csv");

    let destination_path = work.path().join("customers_dataset");
    let pinned_path = work.path().join("customers.schema.yml");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: parquet
  path: {}
dataset: customers
load_mode: full_refresh
transform:
  select: [total, id]
  rename:
    id: customer_id
schema:
  pinned_path: {}
"#,
            source_path.display(),
            destination_path.display(),
            pinned_path.display()
        ),
    )
    .expect("write load definition");

    let bootstrap_artifacts = work.path().join("artifacts-bootstrap");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&bootstrap_artifacts)
        .arg(&definition_path)
        .assert()
        .success();

    let (_, report) = read_single_report(
        &bootstrap_artifacts,
        "bootstrap load writes one artifact directory",
    );
    assert_eq!(report["schema_decision"]["mode"], "inferred");
    assert_eq!(report["schema_decision"]["pinned_schema_persisted"], true);
    assert_eq!(
        fs::read_to_string(&pinned_path).expect("bootstrapped pin"),
        "version: 1\n\
         fields:\n\
         - name: total\n\
         \x20 type: float64\n\
         \x20 nullable: true\n\
         - name: customer_id\n\
         \x20 type: int64\n\
         \x20 nullable: true\n"
    );

    // The source gains an unselected `extra` field: invisible to drift.
    fs::write(&source_path, "id,note,total,extra\n2,b,7.25,x\n").expect("rewrite source csv");
    let reuse_artifacts = work.path().join("artifacts-reuse");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&reuse_artifacts)
        .arg(&definition_path)
        .assert()
        .success();

    let (_, report) = read_single_report(
        &reuse_artifacts,
        "pin-reusing load writes one artifact directory",
    );
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(report["schema_decision"]["mode"], "pinned");
    assert_eq!(report["schema_decision"]["drift_status"], "none");
    assert_eq!(
        report["schema_decision"]["transform"],
        serde_json::json!({
            "select": ["total", "id"],
            "rename": {"id": "customer_id"}
        })
    );
}

#[test]
fn rejected_records_on_renamed_fields_carry_dataset_and_source_names() {
    // A coercion failure on a renamed field names the dataset field while
    // pointing back at the source field, and the recovered record keeps the
    // source names (ADR-0039). The override speaks the dataset namespace.
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    fs::write(&source_path, "id,note\n1,a\nn/a,b\n2,c\n").expect("write source csv");

    let destination_path = work.path().join("customers_dataset");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: parquet
  path: {}
dataset: customers
load_mode: full_refresh
transform:
  rename:
    id: customer_id
schema:
  overrides:
  - name: customer_id
    type: int64
reject_threshold: 1
"#,
            source_path.display(),
            destination_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .success();

    let (_, report) = read_single_report(
        &artifacts_dir,
        "transform load writes one artifact directory",
    );
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(report["row_counts"]["written"], 2);
    assert_eq!(report["row_counts"]["rejected"], 1);

    let artifact_path = PathBuf::from(
        report["rejected_records"]["artifact"]
            .as_str()
            .expect("artifact path"),
    );
    let rejected_lines = read_rejected_records(&artifact_path);
    assert_eq!(rejected_lines.len(), 1);
    assert_eq!(rejected_lines[0]["line"], 3);
    assert_eq!(rejected_lines[0]["code"], "type_coercion_failed");
    assert_eq!(rejected_lines[0]["field"], "customer_id");
    assert_eq!(rejected_lines[0]["source_field"], "id");
    assert_eq!(
        rejected_lines[0]["message"],
        "value \"n/a\" does not fit overridden type int64 for field \"customer_id\""
    );
    assert_eq!(
        rejected_lines[0]["record"],
        serde_json::json!({"id": "n/a", "note": "b"})
    );

    // The destination sees the renamed columns and only the surviving records.
    let batch = read_single_parquet_batch(&single_parquet_file(&destination_path));
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>(),
        vec!["customer_id", "note"]
    );
    let customer_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("customer_id is int64");
    assert_eq!(customer_ids.value(0), 1);
    assert_eq!(customer_ids.value(1), 2);
}

#[test]
fn jsonl_flatten_combines_with_select_and_rename_for_duckdb() {
    // The flatten happy path (ADR-0041): flatten evaluates first, its
    // outputs are ordinary fields to select (placing them anywhere) and
    // rename, and the destination carries the dataset names in select order
    // with correctly typed extracted values. The report echoes the flatten
    // mapping as written.
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("orders.jsonl");
    fs::write(
        &source_path,
        "{\"id\": 1, \"customer\": {\"name\": \"Ada\", \"age\": 36}}\n\
         {\"id\": 2, \"customer\": {\"name\": \"Bo\", \"age\": 52}}\n",
    )
    .expect("write source jsonl");

    let database_path = work.path().join("orders.duckdb");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: jsonl
destination:
  connector: duckdb
  path: {}
dataset: orders
load_mode: full_refresh
transform:
  flatten:
    customer.name: customer_name
    customer.age: customer_age
  select: [id, customer_name, customer_age]
  rename:
    customer_name: contact
"#,
            source_path.display(),
            database_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .success();

    let (_, report) =
        read_single_report(&artifacts_dir, "flatten load writes one artifact directory");
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(report["row_counts"]["source"], 2);
    assert_eq!(report["row_counts"]["written"], 2);
    assert_eq!(report["row_counts"]["rejected"], 0);
    assert_eq!(
        report["schema_decision"],
        serde_json::json!({
            "mode": "inferred",
            "fields": [
                {"name": "id", "type": "int64", "nullable": true},
                {"name": "contact", "type": "utf8", "nullable": true},
                {"name": "customer_age", "type": "int64", "nullable": true}
            ],
            "drift_status": "not_applicable",
            "transform": {
                "flatten": {
                    "customer.name": "customer_name",
                    "customer.age": "customer_age"
                },
                "select": ["id", "customer_name", "customer_age"],
                "rename": {"customer_name": "contact"}
            }
        })
    );

    let batch = read_single_duckdb_batch(&database_path, "orders");
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>(),
        vec!["id", "contact", "customer_age"]
    );
    assert_eq!(batch.num_rows(), 2);
    let contacts = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("contact is utf8");
    let ages = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("customer_age is int64");
    assert_eq!(contacts.value(0), "Ada");
    assert_eq!(ages.value(0), 36);
    assert_eq!(contacts.value(1), "Bo");
    assert_eq!(ages.value(1), 52);
}

#[test]
fn jsonl_flatten_combines_with_select_and_rename_for_parquet() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("orders.jsonl");
    fs::write(
        &source_path,
        "{\"id\": 1, \"customer\": {\"name\": \"Ada\", \"age\": 36}}\n\
         {\"id\": 2, \"customer\": {\"name\": \"Bo\", \"age\": 52}}\n",
    )
    .expect("write source jsonl");

    let destination_path = work.path().join("orders_dataset");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: jsonl
destination:
  connector: parquet
  path: {}
dataset: orders
load_mode: full_refresh
transform:
  flatten:
    customer.name: customer_name
  select: [customer_name, id]
  rename:
    customer_name: contact
"#,
            source_path.display(),
            destination_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .success();

    let (_, report) =
        read_single_report(&artifacts_dir, "flatten load writes one artifact directory");
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(
        report["schema_decision"]["transform"],
        serde_json::json!({
            "flatten": {"customer.name": "customer_name"},
            "select": ["customer_name", "id"],
            "rename": {"customer_name": "contact"}
        })
    );

    // The Parquet schema carries the dataset names in select order — the
    // flatten output placed first — and the extracted values.
    let batch = read_single_parquet_batch(&single_parquet_file(&destination_path));
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>(),
        vec!["contact", "id"]
    );
    let contacts = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("contact is utf8");
    assert_eq!(contacts.value(0), "Ada");
    assert_eq!(contacts.value(1), "Bo");
}

#[test]
fn jsonl_flatten_yields_null_for_missing_null_and_scalar_steps() {
    // The extraction table of ADR-0041 within one load: a missing leaf key,
    // a null parent, and a scalar intermediate segment all yield null in the
    // flatten column, while the parent column keeps its JSON text.
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("orders.jsonl");
    fs::write(
        &source_path,
        "{\"id\": 1, \"customer\": {\"name\": \"Ada\"}}\n\
         {\"id\": 2, \"customer\": {\"vip\": true}}\n\
         {\"id\": 3, \"customer\": null}\n\
         {\"id\": 4, \"customer\": \"opaque\"}\n",
    )
    .expect("write source jsonl");

    let database_path = work.path().join("orders.duckdb");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: jsonl
destination:
  connector: duckdb
  path: {}
dataset: orders
load_mode: full_refresh
transform:
  flatten:
    customer.name: customer_name
"#,
            source_path.display(),
            database_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .success();

    let (_, report) = read_single_report(
        &artifacts_dir,
        "flatten null-semantics load writes one artifact directory",
    );
    // Extraction is total: nothing rejects, every record lands.
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(report["row_counts"]["written"], 4);
    assert_eq!(report["row_counts"]["rejected"], 0);

    let batch = read_single_duckdb_batch(&database_path, "orders");
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>(),
        vec!["id", "customer", "customer_name"]
    );
    let parents = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("customer is utf8");
    let names = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("customer_name is utf8");
    assert_eq!(names.value(0), "Ada");
    assert!(names.is_null(1), "missing leaf key extracts to null");
    assert!(names.is_null(2), "null parent extracts to null");
    assert!(names.is_null(3), "scalar intermediate extracts to null");
    assert_eq!(parents.value(0), "{\"name\":\"Ada\"}");
    assert_eq!(parents.value(1), "{\"vip\":true}");
    assert!(parents.is_null(2), "null parent stays null");
    assert_eq!(parents.value(3), "opaque");
}

#[test]
fn jsonl_flatten_structured_leaf_stays_json_text_and_deeper_path_coexists() {
    // An object leaf materializes its compact JSON text, and a deeper path
    // on the same parent coexists as its own typed column (ADR-0041).
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("orders.jsonl");
    fs::write(
        &source_path,
        "{\"id\": 1, \"customer\": {\"address\": {\"city\": \"Taipei\", \"zip\": \"100\"}}}\n",
    )
    .expect("write source jsonl");

    let destination_path = work.path().join("orders_dataset");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: jsonl
destination:
  connector: parquet
  path: {}
dataset: orders
load_mode: full_refresh
transform:
  flatten:
    customer.address: address
    customer.address.city: city
"#,
            source_path.display(),
            destination_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .success();

    let batch = read_single_parquet_batch(&single_parquet_file(&destination_path));
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect::<Vec<_>>(),
        vec!["id", "customer", "address", "city"]
    );
    let addresses = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("address is utf8");
    let cities = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("city is utf8");
    assert_eq!(addresses.value(0), "{\"city\":\"Taipei\",\"zip\":\"100\"}");
    assert_eq!(cities.value(0), "Taipei");
}

#[test]
fn invalid_flatten_configs_fail_before_source_or_destination_work() {
    // The flatten config failure matrix (ADR-0041), one case each: every
    // case is definition validation, so the schema decision stays
    // not-evaluated, the missing source file is never read, and the
    // destination is never created (ADR-0019).
    let work = TempDir::new().expect("tempdir");
    let destination_path = work.path().join("should-not-exist");
    let cases = [
        (
            "single-segment-path",
            "jsonl",
            "transform:\n  flatten:\n    customer: contact\n",
            "transform.flatten path \"customer\" must have at least two dot-separated segments",
        ),
        (
            "duplicate-output-names",
            "jsonl",
            "transform:\n  flatten:\n    customer.name: contact\n    customer.email: contact\n",
            "transform.flatten maps more than one path to \"contact\"",
        ),
        (
            "collides-with-rename-target",
            "jsonl",
            "transform:\n  flatten:\n    customer.name: contact\n  rename:\n    id: contact\n",
            "transform.flatten and transform.rename map more than one field \
             to the dataset name \"contact\"",
        ),
        (
            "absent-from-select",
            "jsonl",
            "transform:\n  flatten:\n    customer.name: contact\n  select: [id]\n",
            "transform.flatten output \"contact\" is not in transform.select",
        ),
        (
            "csv-source",
            "csv",
            "transform:\n  flatten:\n    customer.name: contact\n",
            "transform.flatten requires a JSONL source format; \
             the resolved source format is csv",
        ),
    ];

    for (label, format, transform_block, expected_message) in cases {
        let definition_path = work.path().join(format!("load-{label}.yml"));
        fs::write(
            &definition_path,
            format!(
                "version: 1\n\
                 source:\n\
                 \x20 connector: local_file\n\
                 \x20 path: missing-source.{format}\n\
                 \x20 format: {format}\n\
                 destination:\n\
                 \x20 connector: parquet\n\
                 \x20 path: {}\n\
                 {transform_block}",
                destination_path.display()
            ),
        )
        .expect("write load definition");
        let artifacts_dir = work.path().join(format!("artifacts-{label}"));

        Command::cargo_bin("data-spark")
            .expect("binary")
            .current_dir(work.path())
            .arg("load")
            .arg("--output-dir")
            .arg(&artifacts_dir)
            .arg(&definition_path)
            .assert()
            .failure();

        assert!(
            !destination_path.exists(),
            "case {label} must fail before destination writing"
        );
        let (_, report) = read_single_report(
            &artifacts_dir,
            "failed flatten config still has one artifact directory",
        );
        assert_eq!(
            report["error_summary"]["code"], "invalid_transform_config",
            "code for case {label}"
        );
        assert_eq!(
            report["error_summary"]["message"], expected_message,
            "message for case {label}"
        );
        assert_eq!(
            report["schema_decision"],
            serde_json::json!({ "mode": "not_evaluated" }),
            "schema decision for case {label}"
        );
    }
}

#[test]
fn unknown_flatten_first_segments_fail_before_destination_writing() {
    // A flatten path whose first segment names no batch-wide observed field
    // is an unknown transform field, reported as the user wrote the full
    // path; a deeper segment absent from records is a null value instead.
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("orders.jsonl");
    fs::write(&source_path, "{\"id\": 1}\n").expect("write source jsonl");

    let database_path = work.path().join("should-not-exist.duckdb");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: jsonl
destination:
  connector: duckdb
  path: {}
dataset: orders
load_mode: full_refresh
transform:
  flatten:
    customer.name: contact
"#,
            source_path.display(),
            database_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .failure();

    assert!(
        !database_path.exists(),
        "an unknown flatten path must fail before destination writing"
    );
    let (_, report) = read_single_report(
        &artifacts_dir,
        "flatten-failed load writes one artifact directory",
    );
    assert_eq!(report["error_summary"]["code"], "unknown_transform_field");
    assert_eq!(
        report["error_summary"]["message"],
        "transform selects, renames, or flattens fields absent from the observed source shape: customer.name"
    );
    assert_eq!(
        report["schema_decision"],
        serde_json::json!({
            "mode": "inferred",
            "drift_status": "not_applicable",
            "transform": {
                "flatten": {"customer.name": "contact"}
            }
        })
    );
}

#[test]
fn flatten_outputs_shadowing_observed_fields_fail_before_destination_writing() {
    // A flatten output may never shadow an observed source field — even one
    // the select list would drop (ADR-0041).
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("orders.jsonl");
    fs::write(
        &source_path,
        "{\"id\": 1, \"customer\": {\"name\": \"Ada\"}}\n",
    )
    .expect("write source jsonl");

    for (label, transform_block) in [
        (
            "shadows-observed",
            "transform:\n  flatten:\n    customer.name: id\n",
        ),
        (
            "shadows-select-dropped",
            "transform:\n  flatten:\n    customer.name: id\n  select: [id]\n",
        ),
    ] {
        let database_path = work.path().join(format!("no-{label}.duckdb"));
        let definition_path = work.path().join(format!("load-{label}.yml"));
        fs::write(
            &definition_path,
            format!(
                "version: 1\n\
                 source:\n\
                 \x20 connector: local_file\n\
                 \x20 path: {}\n\
                 \x20 format: jsonl\n\
                 destination:\n\
                 \x20 connector: duckdb\n\
                 \x20 path: {}\n\
                 dataset: orders\n\
                 load_mode: full_refresh\n\
                 {transform_block}",
                source_path.display(),
                database_path.display()
            ),
        )
        .expect("write load definition");
        let artifacts_dir = work.path().join(format!("artifacts-{label}"));

        Command::cargo_bin("data-spark")
            .expect("binary")
            .current_dir(work.path())
            .arg("load")
            .arg("--output-dir")
            .arg(&artifacts_dir)
            .arg(&definition_path)
            .assert()
            .failure();

        assert!(
            !database_path.exists(),
            "case {label} must fail before destination writing"
        );
        let (_, report) = read_single_report(
            &artifacts_dir,
            "flatten-collision load writes one artifact directory",
        );
        assert_eq!(
            report["error_summary"]["code"], "transform_name_collision",
            "code for case {label}"
        );
        assert_eq!(
            report["error_summary"]["message"],
            "transform flatten collides on dataset field \"id\": \
             source path customer.name shadows an observed source field",
            "message for case {label}"
        );
        assert_eq!(
            report["schema_decision"]["mode"], "inferred",
            "posture for case {label}"
        );
        assert_eq!(
            report["schema_decision"]["drift_status"], "not_applicable",
            "drift for case {label}"
        );
    }
}

#[test]
fn flatten_pin_bootstrap_records_outputs_then_a_removed_entry_drifts() {
    // The bootstrap pin records flatten outputs like any dataset field — by
    // output name, in dataset order (ADR-0033, ADR-0041) — so removing the
    // flatten entry later leaves the pinned field missing: schema drift.
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("orders.jsonl");
    fs::write(
        &source_path,
        "{\"id\": 1, \"customer\": {\"name\": \"Ada\"}}\n",
    )
    .expect("write source jsonl");
    let destination_path = work.path().join("orders_dataset");
    let pinned_path = work.path().join("orders.schema.yml");

    let definition_path = work.path().join("load.yml");
    let definition = |transform_block: &str| {
        format!(
            "version: 1\n\
             source:\n\
             \x20 connector: local_file\n\
             \x20 path: {}\n\
             \x20 format: jsonl\n\
             destination:\n\
             \x20 connector: parquet\n\
             \x20 path: {}\n\
             dataset: orders\n\
             load_mode: full_refresh\n\
             {transform_block}\
             schema:\n\
             \x20 pinned_path: {}\n",
            source_path.display(),
            destination_path.display(),
            pinned_path.display()
        )
    };
    fs::write(
        &definition_path,
        definition("transform:\n  flatten:\n    customer.name: customer_name\n"),
    )
    .expect("write load definition");

    let bootstrap_artifacts = work.path().join("artifacts-bootstrap");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&bootstrap_artifacts)
        .arg(&definition_path)
        .assert()
        .success();

    // The persisted pin records the flatten output in dataset order.
    assert_eq!(
        fs::read_to_string(&pinned_path).expect("pin persisted"),
        "version: 1\n\
         fields:\n\
         - name: id\n\
         \x20 type: int64\n\
         \x20 nullable: true\n\
         - name: customer\n\
         \x20 type: utf8\n\
         \x20 nullable: true\n\
         - name: customer_name\n\
         \x20 type: utf8\n\
         \x20 nullable: true\n"
    );
    let (_, report) = read_single_report(
        &bootstrap_artifacts,
        "bootstrap load writes one artifact directory",
    );
    assert_eq!(report["schema_decision"]["pinned_schema_persisted"], true);

    // Dropping the flatten entry leaves the pinned output missing.
    fs::write(&definition_path, definition("")).expect("rewrite load definition");
    let drift_artifacts = work.path().join("artifacts-drift");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&drift_artifacts)
        .arg(&definition_path)
        .assert()
        .failure();

    let (_, report) =
        read_single_report(&drift_artifacts, "drift load writes one artifact directory");
    assert_eq!(report["error_summary"]["code"], "schema_drift");
    assert_eq!(report["schema_decision"]["drift_status"], "failed_on_drift");
    assert_eq!(
        report["schema_decision"]["drift"]["missing_fields"],
        serde_json::json!(["customer_name"])
    );
}

#[test]
fn adding_a_flatten_entry_under_a_pin_follows_the_drift_policy() {
    // A flatten entry added after the pin was recorded is ordinary additive
    // drift: the default policy fails, allow_additive_nullable proceeds and
    // extends the pin with the output (ADR-0034, ADR-0041).
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("orders.jsonl");
    fs::write(
        &source_path,
        "{\"id\": 1, \"customer\": {\"name\": \"Ada\"}}\n",
    )
    .expect("write source jsonl");
    let destination_path = work.path().join("orders_dataset");
    let pinned_path = work.path().join("orders.schema.yml");

    let definition_path = work.path().join("load.yml");
    let definition = |transform_block: &str, drift_policy: &str| {
        format!(
            "version: 1\n\
             source:\n\
             \x20 connector: local_file\n\
             \x20 path: {}\n\
             \x20 format: jsonl\n\
             destination:\n\
             \x20 connector: parquet\n\
             \x20 path: {}\n\
             dataset: orders\n\
             load_mode: full_refresh\n\
             {transform_block}\
             schema:\n\
             \x20 pinned_path: {}\n\
             {drift_policy}",
            source_path.display(),
            destination_path.display(),
            pinned_path.display()
        )
    };

    // Bootstrap the pin without any flatten declaration.
    fs::write(&definition_path, definition("", "")).expect("write load definition");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(work.path().join("artifacts-bootstrap"))
        .arg(&definition_path)
        .assert()
        .success();

    // Declaring the flatten under the default fail policy is drift.
    let flatten_block = "transform:\n  flatten:\n    customer.name: customer_name\n";
    fs::write(&definition_path, definition(flatten_block, "")).expect("rewrite load definition");
    let fail_artifacts = work.path().join("artifacts-fail");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&fail_artifacts)
        .arg(&definition_path)
        .assert()
        .failure();
    let (_, report) = read_single_report(
        &fail_artifacts,
        "failed drift load writes one artifact directory",
    );
    assert_eq!(report["error_summary"]["code"], "schema_drift");
    assert_eq!(
        report["schema_decision"]["drift"]["added_fields"],
        serde_json::json!(["customer_name"])
    );

    // The additive policy admits the output and extends the pin.
    fs::write(
        &definition_path,
        definition(
            flatten_block,
            "\x20 drift_policy: allow_additive_nullable\n",
        ),
    )
    .expect("rewrite load definition");
    let additive_artifacts = work.path().join("artifacts-additive");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&additive_artifacts)
        .arg(&definition_path)
        .assert()
        .success();
    let (_, report) = read_single_report(
        &additive_artifacts,
        "additive drift load writes one artifact directory",
    );
    assert_eq!(
        report["schema_decision"]["drift_status"],
        "additive_fields_added"
    );
    assert_eq!(
        report["schema_decision"]["added_fields"],
        serde_json::json!([
            {"name": "customer_name", "type": "utf8", "nullable": true}
        ])
    );
    assert!(
        fs::read_to_string(&pinned_path)
            .expect("extended pin")
            .contains("- name: customer_name\n"),
        "the extended pin records the flatten output"
    );
}

#[test]
fn pinned_flatten_misfits_reject_records_with_the_declared_path() {
    // Strictness composes through the pin (ADR-0035): a pinned flatten
    // output whose extracted value misfits produces the existing per-record
    // rejection, whose artifact line names the output and carries the
    // declared source path as source_field.
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("orders.jsonl");
    fs::write(&source_path, "{\"id\": 1, \"customer\": {\"code\": 7}}\n")
        .expect("write source jsonl");
    let destination_path = work.path().join("orders_dataset");
    let pinned_path = work.path().join("orders.schema.yml");

    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: jsonl
destination:
  connector: parquet
  path: {}
dataset: orders
load_mode: full_refresh
transform:
  flatten:
    customer.code: customer_code
schema:
  pinned_path: {}
"#,
            source_path.display(),
            destination_path.display(),
            pinned_path.display()
        ),
    )
    .expect("write load definition");

    // Bootstrap pins customer_code as int64 from the observed batch.
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(work.path().join("artifacts-bootstrap"))
        .arg(&definition_path)
        .assert()
        .success();
    assert!(
        fs::read_to_string(&pinned_path)
            .expect("pin persisted")
            .contains("- name: customer_code\n\x20 type: int64\n"),
        "the bootstrap pins the extracted int64"
    );

    // The next batch extracts a string: the record rejects per ADR-0035 and
    // the default threshold fails the load before any destination change.
    fs::write(
        &source_path,
        "{\"id\": 2, \"customer\": {\"code\": \"n/a\"}}\n",
    )
    .expect("rewrite source jsonl");
    let misfit_artifacts = work.path().join("artifacts-misfit");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&misfit_artifacts)
        .arg(&definition_path)
        .assert()
        .failure();

    let (report_path, report) = read_single_report(
        &misfit_artifacts,
        "misfit load writes one artifact directory",
    );
    assert_eq!(report["error_summary"]["code"], "reject_threshold_exceeded");
    assert_eq!(report["rejected_records"]["count"], 1);
    let artifact_dir = report_path.parent().expect("artifact directory");
    let rejected_lines = read_rejected_records(&artifact_dir.join("rejected-records.jsonl"));
    assert_eq!(rejected_lines.len(), 1);
    assert_eq!(rejected_lines[0]["code"], "type_coercion_failed");
    assert_eq!(rejected_lines[0]["field"], "customer_code");
    assert_eq!(rejected_lines[0]["source_field"], "customer.code");
    assert_eq!(
        rejected_lines[0]["record"],
        serde_json::json!({"id": 2, "customer": {"code": "n/a"}})
    );
}

#[test]
fn overrides_shape_flatten_outputs_like_any_dataset_field() {
    // schema.overrides names dataset fields (ADR-0038, ADR-0040): naming a
    // flatten output overrides its inferred type, and naming a dropped or
    // unknown field keeps failing as unknown_override_field.
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("orders.jsonl");
    fs::write(&source_path, "{\"id\": 1, \"customer\": {\"code\": 7}}\n")
        .expect("write source jsonl");
    let destination_path = work.path().join("orders_dataset");

    let definition_path = work.path().join("load.yml");
    let definition = |overridden_field: &str| {
        format!(
            "version: 1\n\
             source:\n\
             \x20 connector: local_file\n\
             \x20 path: {}\n\
             \x20 format: jsonl\n\
             destination:\n\
             \x20 connector: parquet\n\
             \x20 path: {}\n\
             dataset: orders\n\
             load_mode: full_refresh\n\
             transform:\n\
             \x20 flatten:\n\
             \x20\x20\x20 customer.code: customer_code\n\
             \x20 select: [id, customer_code]\n\
             schema:\n\
             \x20 overrides:\n\
             \x20\x20\x20 - name: {overridden_field}\n\
             \x20\x20\x20\x20\x20 type: utf8\n",
            source_path.display(),
            destination_path.display()
        )
    };

    // Overriding the flatten output rewrites its inferred int64 to utf8.
    fs::write(&definition_path, definition("customer_code")).expect("write load definition");
    let override_artifacts = work.path().join("artifacts-override");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&override_artifacts)
        .arg(&definition_path)
        .assert()
        .success();
    let (_, report) = read_single_report(
        &override_artifacts,
        "override load writes one artifact directory",
    );
    assert_eq!(
        report["schema_decision"]["fields"],
        serde_json::json!([
            {"name": "id", "type": "int64", "nullable": true},
            {"name": "customer_code", "type": "utf8", "nullable": true}
        ])
    );
    let batch = read_single_parquet_batch(&single_parquet_file(&destination_path));
    let codes = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("customer_code is utf8");
    assert_eq!(codes.value(0), "7");

    // Naming the select-dropped parent stays an unknown override field.
    fs::write(&definition_path, definition("customer")).expect("rewrite load definition");
    let unknown_artifacts = work.path().join("artifacts-unknown");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&unknown_artifacts)
        .arg(&definition_path)
        .assert()
        .failure();
    let (_, report) = read_single_report(
        &unknown_artifacts,
        "unknown-override load writes one artifact directory",
    );
    assert_eq!(report["error_summary"]["code"], "unknown_override_field");
    assert_eq!(
        report["error_summary"]["message"],
        "schema overrides name fields absent from the observed source shape: customer"
    );
}

#[test]
fn parquet_append_with_an_incompatible_schema_fails_without_changing_the_destination() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    let destination_path = work.path().join("customers_dataset");
    let definition_path = work.path().join("load.yml");

    fs::write(&source_path, "customer_id,name\n1,Ada\n").expect("write first csv");
    write_load_definition(
        &definition_path,
        &source_path,
        "csv",
        "parquet",
        &destination_path,
        "full_refresh",
        None,
    );
    run_cli_load(
        work.path(),
        &work.path().join("artifacts-first"),
        &definition_path,
        true,
    );

    fs::write(&source_path, "customer_id,total\n2,7\n").expect("write incompatible csv");
    write_load_definition(
        &definition_path,
        &source_path,
        "csv",
        "parquet",
        &destination_path,
        "append",
        None,
    );
    let append = run_cli_load(
        work.path(),
        &work.path().join("artifacts-append"),
        &definition_path,
        false,
    );
    let report = &append.report;

    assert_eq!(report["load_mode"], "append");
    assert_eq!(report["exit_status"], "failed");
    assert_eq!(report["error_summary"]["code"], "destination_write_failed");
    assert_eq!(report["row_counts"]["source"], 1);
    assert_eq!(report["row_counts"]["written"], 0);
    assert_eq!(report["row_counts"]["rejected"], 0);
    assert_eq!(report["destination_write"]["atomicity"], "not_applicable");
    assert_eq!(
        id_name_records(&read_parquet_batches(&destination_path)),
        vec![(1, "Ada".to_string())]
    );
    assert!(append.stdout.contains("Status: failed"));
    assert!(append.stdout.contains("Load mode: append"));
    assert!(append
        .stdout
        .contains(append.report_path.to_str().expect("report path")));
}

#[test]
fn append_validation_failure_leaves_the_destination_unchanged_and_reports_no_write() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    let database_path = work.path().join("customers.duckdb");
    let pinned_path = work.path().join("customers.schema.yml");
    let definition_path = work.path().join("load.yml");

    fs::write(&source_path, "customer_id,total\n1,10.5\n").expect("write first csv");
    write_load_definition(
        &definition_path,
        &source_path,
        "csv",
        "duckdb",
        &database_path,
        "full_refresh",
        Some(&pinned_path),
    );
    run_cli_load(
        work.path(),
        &work.path().join("artifacts-first"),
        &definition_path,
        true,
    );

    fs::write(&source_path, "customer_id,total\nabc,7\n").expect("write misfit csv");
    write_load_definition(
        &definition_path,
        &source_path,
        "csv",
        "duckdb",
        &database_path,
        "append",
        Some(&pinned_path),
    );
    let append = run_cli_load(
        work.path(),
        &work.path().join("artifacts-append"),
        &definition_path,
        false,
    );
    let report = &append.report;

    assert_eq!(report["load_mode"], "append");
    assert_eq!(report["exit_status"], "failed");
    assert_eq!(report["process_exit_code"], 1);
    assert_eq!(report["error_summary"]["code"], "reject_threshold_exceeded");
    assert_eq!(report["schema_decision"]["mode"], "pinned");
    assert_eq!(report["schema_decision"]["drift_status"], "none");
    assert_eq!(report["row_counts"]["source"], 1);
    assert_eq!(report["row_counts"]["written"], 0);
    assert_eq!(report["row_counts"]["rejected"], 1);
    assert_eq!(report["rejected_records"]["count"], 1);
    assert_eq!(report["destination_write"]["atomicity"], "not_applicable");

    let artifact_path = PathBuf::from(
        report["rejected_records"]["artifact"]
            .as_str()
            .expect("rejected-record artifact"),
    );
    let rejected = read_rejected_records(&artifact_path);
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0]["code"], "type_coercion_failed");
    assert_eq!(rejected[0]["field"], "customer_id");

    let batch = read_single_duckdb_batch(&database_path, "customers");
    assert_eq!(batch.num_rows(), 1);
    let customer_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("customer_id is int64");
    assert_eq!(customer_ids.value(0), 1);

    assert!(append.stdout.contains("Status: failed"));
    assert!(append.stdout.contains("Load mode: append"));
    assert!(append.stdout.contains("Records read: 1"));
    assert!(append.stdout.contains("Records written: 0"));
    assert!(append.stdout.contains("Records rejected: 1"));
    assert!(append
        .stdout
        .contains(append.report_path.to_str().expect("report path")));
}

#[test]
fn duckdb_append_rejects_lossy_type_coercion_before_writing() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    let database_path = work.path().join("customers.duckdb");
    let definition_path = work.path().join("load.yml");

    {
        let connection = duckdb::Connection::open(&database_path).expect("open DuckDB destination");
        connection
            .execute_batch("CREATE TABLE customers (id BIGINT); INSERT INTO customers VALUES (1)")
            .expect("seed DuckDB destination");
    }

    fs::write(&source_path, "id\n2.9\n").expect("write lossy append csv");
    write_load_definition(
        &definition_path,
        &source_path,
        "csv",
        "duckdb",
        &database_path,
        "append",
        None,
    );
    let append = run_cli_load(
        work.path(),
        &work.path().join("artifacts-append"),
        &definition_path,
        false,
    );
    let report = &append.report;

    assert_eq!(report["load_mode"], "append");
    assert_eq!(report["exit_status"], "failed");
    assert_eq!(report["process_exit_code"], 1);
    assert_eq!(report["error_summary"]["code"], "destination_write_failed");
    assert!(report["error_summary"]["message"]
        .as_str()
        .expect("error message")
        .contains("append schema does not match DuckDB destination"));
    assert_eq!(report["schema_decision"]["mode"], "inferred");
    assert_eq!(report["row_counts"]["source"], 1);
    assert_eq!(report["row_counts"]["written"], 0);
    assert_eq!(report["row_counts"]["rejected"], 0);
    assert_eq!(report["rejected_records"]["count"], 0);
    assert!(report["rejected_records"]["artifact"].is_null());
    assert_eq!(report["destination_write"]["atomicity"], "not_applicable");
    assert!(report["destination_write"]["strategy"].is_null());
    assert!(!report["load_id"].as_str().expect("load id").is_empty());
    assert_eq!(
        PathBuf::from(report["artifact_dir"].as_str().expect("artifact directory")),
        append.report_path.parent().expect("report parent")
    );

    let batch = read_single_duckdb_batch(&database_path, "customers");
    assert_eq!(batch.num_rows(), 1);
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id is int64");
    assert_eq!(ids.value(0), 1);

    assert!(append.stdout.contains("Status: failed"));
    assert!(append.stdout.contains("Records read: 1"));
    assert!(append.stdout.contains("Records written: 0"));
    assert!(append.stdout.contains("Records rejected: 0"));
    assert!(append
        .stdout
        .contains(append.report_path.to_str().expect("report path")));
}

#[test]
fn duckdb_append_rejects_missing_destination_field_before_writing() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    let database_path = work.path().join("customers.duckdb");
    let definition_path = work.path().join("load.yml");

    {
        let connection = duckdb::Connection::open(&database_path).expect("open DuckDB destination");
        connection
            .execute_batch(
                "CREATE TABLE customers (id BIGINT, name VARCHAR); \
                 INSERT INTO customers VALUES (1, 'Ada')",
            )
            .expect("seed DuckDB destination");
    }

    fs::write(&source_path, "id\n2\n").expect("write missing-field append csv");
    write_load_definition(
        &definition_path,
        &source_path,
        "csv",
        "duckdb",
        &database_path,
        "append",
        None,
    );
    let append = run_cli_load(
        work.path(),
        &work.path().join("artifacts-append"),
        &definition_path,
        false,
    );
    let report = &append.report;

    assert_eq!(report["load_mode"], "append");
    assert_eq!(report["exit_status"], "failed");
    assert_eq!(report["process_exit_code"], 1);
    assert_eq!(report["error_summary"]["code"], "destination_write_failed");
    assert!(report["error_summary"]["message"]
        .as_str()
        .expect("error message")
        .contains("append schema does not match DuckDB destination"));
    assert_eq!(report["schema_decision"]["mode"], "inferred");
    assert_eq!(report["row_counts"]["source"], 1);
    assert_eq!(report["row_counts"]["written"], 0);
    assert_eq!(report["row_counts"]["rejected"], 0);
    assert_eq!(report["rejected_records"]["count"], 0);
    assert!(report["rejected_records"]["artifact"].is_null());
    assert_eq!(report["destination_write"]["atomicity"], "not_applicable");
    assert!(report["destination_write"]["strategy"].is_null());
    assert!(!report["load_id"].as_str().expect("load id").is_empty());
    assert_eq!(
        PathBuf::from(report["artifact_dir"].as_str().expect("artifact directory")),
        append.report_path.parent().expect("report parent")
    );

    let batch = read_single_duckdb_batch(&database_path, "customers");
    assert_eq!(batch.num_rows(), 1);
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id is int64");
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name is string");
    assert_eq!(ids.value(0), 1);
    assert_eq!(names.value(0), "Ada");

    assert!(append.stdout.contains("Status: failed"));
    assert!(append.stdout.contains("Records read: 1"));
    assert!(append.stdout.contains("Records written: 0"));
    assert!(append.stdout.contains("Records rejected: 0"));
    assert!(append
        .stdout
        .contains(append.report_path.to_str().expect("report path")));
}

#[test]
fn append_destination_write_failure_reports_best_effort_and_preserves_existing_records() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    let database_path = work.path().join("customers.duckdb");
    let definition_path = work.path().join("load.yml");

    {
        let connection = duckdb::Connection::open(&database_path).expect("open DuckDB destination");
        connection
            .execute_batch(
                "CREATE TABLE customers (customer_id BIGINT PRIMARY KEY, name VARCHAR); \
                 INSERT INTO customers VALUES (1, 'Ada')",
            )
            .expect("seed constrained DuckDB destination");
    }

    // The source schema matches the destination, but the duplicate primary key
    // makes the insert itself fail after the best-effort write boundary.
    fs::write(&source_path, "customer_id,name\n1,Grace\n").expect("write duplicate-key append csv");
    write_load_definition(
        &definition_path,
        &source_path,
        "csv",
        "duckdb",
        &database_path,
        "append",
        None,
    );
    let append = run_cli_load(
        work.path(),
        &work.path().join("artifacts-append"),
        &definition_path,
        false,
    );
    let report = &append.report;

    assert_eq!(report["load_mode"], "append");
    assert_eq!(report["exit_status"], "failed");
    assert_eq!(report["process_exit_code"], 1);
    assert_eq!(report["error_summary"]["code"], "destination_write_failed");
    assert_eq!(report["schema_decision"]["mode"], "inferred");
    assert_eq!(report["row_counts"]["source"], 1);
    assert_eq!(report["row_counts"]["written"], 0);
    assert_eq!(report["row_counts"]["rejected"], 0);
    assert_eq!(report["rejected_records"]["count"], 0);
    assert!(report["rejected_records"]["artifact"].is_null());
    assert_eq!(report["destination_write"]["atomicity"], "best_effort");
    assert_eq!(report["destination_write"]["strategy"], "insert");

    let batch = read_single_duckdb_batch(&database_path, "customers");
    assert_eq!(batch.num_rows(), 1);
    let customer_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("customer_id is int64");
    assert_eq!(customer_ids.value(0), 1);

    assert!(!report["load_id"].as_str().expect("load id").is_empty());
    assert_eq!(
        PathBuf::from(report["artifact_dir"].as_str().expect("artifact directory")),
        append.report_path.parent().expect("report parent")
    );
    assert!(append.stdout.contains("Status: failed"));
    assert!(append.stdout.contains("Load mode: append"));
    assert!(append.stdout.contains("Records read: 1"));
    assert!(append.stdout.contains("Records written: 0"));
    assert!(append
        .stdout
        .contains(append.report_path.to_str().expect("report path")));
}

#[test]
fn a_configured_reject_threshold_lets_a_load_complete_with_rejected_records() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    // Line 3 has the wrong field count; the two other records are loadable.
    fs::write(
        &source_path,
        "customer_id,name\n1,Ada\n2,Grace,extra-field\n3,Cara\n",
    )
    .expect("write source csv");

    let destination_path = work.path().join("customers_dataset");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: parquet
  path: {}
dataset: customers
load_mode: full_refresh
reject_threshold: 1
"#,
            source_path.display(),
            destination_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    let assert = Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .success();

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let (_, report) = read_single_report(
        &artifacts_dir,
        "within-threshold load writes one artifact directory",
    );

    // One rejection at a threshold of exactly 1: at-or-below completes,
    // writing the surviving records under the configured load rules
    // (ADR-0020), with the destination's write facts reported honestly.
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(report["process_exit_code"], 0);
    assert_eq!(report["row_counts"]["source"], 3);
    assert_eq!(report["row_counts"]["written"], 2);
    assert_eq!(report["row_counts"]["rejected"], 1);
    assert_eq!(report["rejected_records"]["count"], 1);
    assert_eq!(report["destination_write"]["atomicity"], "best_effort");
    assert!(report["error_summary"].is_null());

    let artifact_path = PathBuf::from(
        report["rejected_records"]["artifact"]
            .as_str()
            .expect("artifact path"),
    );
    let rejected_lines = read_rejected_records(&artifact_path);
    assert_eq!(rejected_lines.len(), 1);
    assert_eq!(rejected_lines[0]["line"], 3);
    assert_eq!(rejected_lines[0]["code"], "malformed_csv_record");

    // Only the surviving records reached the destination.
    let parquet_path = single_parquet_file(&destination_path);
    let batch = read_single_parquet_batch(&parquet_path);
    assert_eq!(batch.num_rows(), 2);
    let customer_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("customer_id is int64");
    assert_eq!(customer_ids.value(0), 1);
    assert_eq!(customer_ids.value(1), 3);

    assert!(stdout.contains("Status: succeeded"));
    assert!(stdout.contains("Records read: 3"));
    assert!(stdout.contains("Records written: 2"));
    assert!(stdout.contains("Records rejected: 1"));
    assert!(stdout.contains(artifact_path.to_str().expect("artifact path")));
}

#[test]
fn a_destination_write_failure_still_reports_the_accepted_rejections() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    fs::write(
        &source_path,
        "customer_id,name\n1,Ada\n2,Grace,extra-field\n",
    )
    .expect("write source csv");

    // The destination's parent path is an existing file, so the destination
    // write fails only after the within-threshold rejection was accepted and
    // its artifact written.
    let blocker_path = work.path().join("blocker");
    fs::write(&blocker_path, "not a directory").expect("write blocker file");
    let destination_path = blocker_path.join("customers_dataset");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: parquet
  path: {}
dataset: customers
load_mode: full_refresh
reject_threshold: 1
"#,
            source_path.display(),
            destination_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    let assert = Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .failure();

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let (report_path, report) = read_single_report(
        &artifacts_dir,
        "write-failed load writes one artifact directory",
    );

    // The write failure happened after the read and the threshold decision:
    // the report stays honest about what was established — the schema
    // decision, the source count, and the rejections with their artifact —
    // while claiming nothing about the write.
    assert_eq!(report["report_version"], 1);
    assert_eq!(report["exit_status"], "failed");
    assert_eq!(report["process_exit_code"], 1);
    assert_eq!(report["error_summary"]["code"], "destination_write_failed");
    assert!(!report["error_summary"]["message"]
        .as_str()
        .expect("error message")
        .is_empty());
    assert_eq!(report["schema_decision"]["mode"], "inferred");
    assert_eq!(report["row_counts"]["source"], 2);
    assert_eq!(report["row_counts"]["written"], 0);
    assert_eq!(report["row_counts"]["rejected"], 1);
    assert_eq!(report["rejected_records"]["count"], 1);
    assert_eq!(report["destination_write"]["atomicity"], "not_applicable");
    // The destination session never opened (the parent path blocks it), so
    // the execution posture stays pre-write (ADR-0047).
    assert_eq!(report["execution"]["record_format"], "not_started");
    assert_eq!(report["execution"]["batch_count"], 0);

    let artifact_path = PathBuf::from(
        report["rejected_records"]["artifact"]
            .as_str()
            .expect("artifact path"),
    );
    let rejected_lines = read_rejected_records(&artifact_path);
    assert_eq!(rejected_lines.len(), 1);
    assert_eq!(rejected_lines[0]["code"], "malformed_csv_record");

    assert!(stdout.contains("Status: failed"));
    assert!(stdout.contains("Records read: 2"));
    assert!(stdout.contains("Records written: 0"));
    assert!(stdout.contains("Records rejected: 1"));
    assert!(stdout.contains(report_path.to_str().expect("report path")));
    assert!(stdout.contains(artifact_path.to_str().expect("artifact path")));
}

#[test]
fn missing_required_fields_reject_records_under_a_hand_tightened_pin() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.jsonl");
    // One record carries `name`, one nulls it, one omits it entirely: under a
    // `nullable: false` pin the null and the omission are required-field
    // violations (ADR-0035).
    fs::write(
        &source_path,
        "{\"customer_id\": 1, \"name\": \"Ada\"}\n\
         {\"customer_id\": 2, \"name\": null}\n\
         {\"customer_id\": 3}\n",
    )
    .expect("write source jsonl");

    // The pin is hand-tightened (ADR-0033: hand edits stay possible):
    // bootstrap infers nullable fields only, so `nullable: false` enters by
    // editing the pinned schema file.
    let pinned_path = work.path().join("customers.schema.yml");
    fs::write(
        &pinned_path,
        "version: 1\n\
         fields:\n\
         - name: customer_id\n\
         \x20 type: int64\n\
         - name: name\n\
         \x20 type: utf8\n\
         \x20 nullable: false\n",
    )
    .expect("write pinned schema");

    let database_path = work.path().join("customers.duckdb");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: jsonl
destination:
  connector: duckdb
  path: {}
dataset: customers
load_mode: full_refresh
schema:
  pinned_path: {}
reject_threshold: 2
"#,
            source_path.display(),
            database_path.display(),
            pinned_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .success();

    let (_, report) = read_single_report(
        &artifacts_dir,
        "required-field load writes one artifact directory",
    );
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(report["row_counts"]["source"], 3);
    assert_eq!(report["row_counts"]["written"], 1);
    assert_eq!(report["row_counts"]["rejected"], 2);
    // The schema decision echoes the required field.
    assert_eq!(
        report["schema_decision"]["fields"][1],
        serde_json::json!({"name": "name", "type": "utf8", "nullable": false})
    );

    let artifact_path = PathBuf::from(
        report["rejected_records"]["artifact"]
            .as_str()
            .expect("artifact path"),
    );
    let rejected_lines = read_rejected_records(&artifact_path);
    assert_eq!(rejected_lines.len(), 2);
    for (rejected, line, record) in [
        (
            &rejected_lines[0],
            2,
            serde_json::json!({"customer_id": 2, "name": null}),
        ),
        (&rejected_lines[1], 3, serde_json::json!({"customer_id": 3})),
    ] {
        assert_eq!(rejected["line"], line);
        assert_eq!(rejected["code"], "missing_required_field");
        assert_eq!(rejected["field"], "name");
        assert_eq!(rejected["message"], "required field \"name\" is null");
        assert_eq!(rejected["record"], record);
    }

    // Only the record satisfying the required field reached the destination.
    let batch = read_single_duckdb_batch(&database_path, "customers");
    assert_eq!(batch.num_rows(), 1);
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name is string");
    assert_eq!(names.value(0), "Ada");
}

// 2026-07-14T17:00:00Z in epoch microseconds, computed independently with
// `date -u`. The declared-type tests spell this one instant many ways.
const DECLARED_INSTANT_MICROS: i64 = 1_784_048_400_000_000;

#[test]
fn csv_declared_types_land_in_duckdb_with_matching_column_types() {
    // The ADR-0042 tracer for the DuckDB leg: declared overrides materialize
    // Arrow Timestamp(us, None) / Timestamp(us, UTC) / Decimal128, and DuckDB
    // stores them as TIMESTAMP / TIMESTAMPTZ / DECIMAL(10,2). The two
    // settled_at spellings name the same instant from different offsets, so
    // they must store equal values (ADR-0043).
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("orders.csv");
    fs::write(
        &source_path,
        "order_id,created_at,settled_at,amount\n\
         1,2026-07-14 17:00:00,2026-07-14T17:00:00Z,1.2\n\
         2,2026-07-14T17:00:00.5,2026-07-15T01:00:00+08:00,007.50\n",
    )
    .expect("write source csv");

    let database_path = work.path().join("orders.duckdb");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: duckdb
  path: {}
dataset: orders
load_mode: full_refresh
schema:
  overrides:
  - name: created_at
    type: timestamp
  - name: settled_at
    type: timestamptz
  - name: amount
    type: decimal(10,2)
"#,
            source_path.display(),
            database_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .success();

    let (_, report) = read_single_report(&artifacts_dir, "load writes one artifact directory");
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(report["row_counts"]["written"], 2);
    // The report prints the declared names exactly as declared, and no
    // contract version changed.
    assert_eq!(report["report_version"], 1);
    assert_eq!(
        report["schema_decision"]["fields"],
        serde_json::json!([
            {"name": "order_id", "type": "int64", "nullable": true},
            {"name": "created_at", "type": "timestamp", "nullable": true},
            {"name": "settled_at", "type": "timestamptz", "nullable": true},
            {"name": "amount", "type": "decimal(10,2)", "nullable": true}
        ])
    );

    // DuckDB's own catalog states the destination column types.
    assert_eq!(
        duckdb_column_types(&database_path, "orders"),
        vec![
            ("order_id".to_string(), "BIGINT".to_string()),
            ("created_at".to_string(), "TIMESTAMP".to_string()),
            (
                "settled_at".to_string(),
                "TIMESTAMP WITH TIME ZONE".to_string()
            ),
            ("amount".to_string(), "DECIMAL(10,2)".to_string()),
        ]
    );

    let batch = read_single_duckdb_batch(&database_path, "orders");
    assert_eq!(batch.num_rows(), 2);
    let created = batch
        .column(1)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("created_at reads back as microsecond timestamps");
    assert_eq!(created.value(0), DECLARED_INSTANT_MICROS);
    assert_eq!(created.value(1), DECLARED_INSTANT_MICROS + 500_000);
    let settled = batch
        .column(2)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("settled_at reads back as microsecond timestamps");
    // Different offset spellings of one instant store equal instants.
    assert_eq!(settled.value(0), DECLARED_INSTANT_MICROS);
    assert_eq!(settled.value(1), DECLARED_INSTANT_MICROS);
    let amounts = batch
        .column(3)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("amount reads back as decimal128");
    assert_eq!(amounts.value(0), 120); // "1.2" landed as 1.20
    assert_eq!(amounts.value(1), 750); // "007.50" landed as 7.50
}

#[test]
fn jsonl_declared_types_land_in_parquet_with_matching_logical_types() {
    // The Parquet leg of the ADR-0042 tracer: the file's own logical types
    // carry the declared semantics — TIMESTAMP(MICROS) split by
    // isAdjustedToUTC (ADR-0043) and DECIMAL(10,2) (ADR-0044) — and a JSON
    // integer rescales into the declared decimal.
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("orders.jsonl");
    fs::write(
        &source_path,
        "{\"created_at\": \"2026-07-14T17:00:00\", \"settled_at\": \"2026-07-15T01:00:00+08:00\", \"amount\": \"1.2\"}\n\
         {\"created_at\": null, \"settled_at\": \"2026-07-14T17:00:00Z\", \"amount\": 42}\n",
    )
    .expect("write source jsonl");

    let destination_path = work.path().join("orders_dataset");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: jsonl
destination:
  connector: parquet
  path: {}
dataset: orders
load_mode: full_refresh
schema:
  overrides:
  - name: created_at
    type: timestamp
  - name: settled_at
    type: timestamptz
  - name: amount
    type: decimal(10,2)
"#,
            source_path.display(),
            destination_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .success();

    let (_, report) = read_single_report(&artifacts_dir, "load writes one artifact directory");
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(report["row_counts"]["written"], 2);

    let parquet_path = single_parquet_file(&destination_path);
    let batch = read_single_parquet_batch(&parquet_path);
    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>(),
        vec![
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            DataType::Decimal128(10, 2),
        ]
    );

    // The Parquet file's own logical types carry the declared semantics.
    let file = File::open(&parquet_path).expect("open parquet file");
    let metadata = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("parquet reader")
        .metadata()
        .clone();
    let columns = metadata.file_metadata().schema_descr().columns();
    assert!(
        matches!(
            columns[0].logical_type_ref(),
            Some(LogicalType::Timestamp {
                is_adjusted_to_u_t_c: false,
                unit: parquet::basic::TimeUnit::MICROS,
            })
        ),
        "created_at logical type: {:?}",
        columns[0].logical_type_ref()
    );
    assert!(
        matches!(
            columns[1].logical_type_ref(),
            Some(LogicalType::Timestamp {
                is_adjusted_to_u_t_c: true,
                unit: parquet::basic::TimeUnit::MICROS,
            })
        ),
        "settled_at logical type: {:?}",
        columns[1].logical_type_ref()
    );
    assert!(
        matches!(
            columns[2].logical_type_ref(),
            Some(LogicalType::Decimal {
                precision: 10,
                scale: 2,
            })
        ),
        "amount logical type: {:?}",
        columns[2].logical_type_ref()
    );

    let created = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("created_at reads back as microsecond timestamps");
    assert_eq!(created.value(0), DECLARED_INSTANT_MICROS);
    assert!(created.is_null(1));
    let settled = batch
        .column(1)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("settled_at reads back as microsecond timestamps");
    // Both spellings of the one instant store equal instants.
    assert_eq!(settled.value(0), DECLARED_INSTANT_MICROS);
    assert_eq!(settled.value(1), DECLARED_INSTANT_MICROS);
    let amounts = batch
        .column(2)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("amount reads back as decimal128");
    assert_eq!(amounts.value(0), 120); // "1.2" landed as 1.20
    assert_eq!(amounts.value(1), 4_200); // the JSON integer 42 rescaled
}

#[test]
fn csv_declared_type_misfits_reject_records_naming_the_cause_within_the_threshold() {
    // The ADR-0043/0044 rejection menu end to end: each misfit becomes one
    // type_coercion_failed rejected record whose artifact line names the
    // concrete cause, and the load completes because the definition
    // tolerates them (reject_threshold, ADR-0020/0036).
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("orders.csv");
    fs::write(
        &source_path,
        "created_at,amount\n\
         2026-07-14 17:00:00,1.2\n\
         2026-07-14T17:00:00Z,1\n\
         2026-07-14,1\n\
         1784048400,1\n\
         2026-07-14 17:00:00.1234567,1\n\
         2026-07-14 17:00:00,1.234\n\
         2026-07-14 17:00:00,100000000.00\n\
         2026-07-14 17:00:00,1e3\n\
         2026-07-14 17:00:00,\"1,000\"\n",
    )
    .expect("write source csv");

    let destination_path = work.path().join("orders_dataset");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: parquet
  path: {}
dataset: orders
load_mode: full_refresh
reject_threshold: 8
schema:
  overrides:
  - name: created_at
    type: timestamp
  - name: amount
    type: decimal(10,2)
"#,
            source_path.display(),
            destination_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .success();

    let (_, report) = read_single_report(&artifacts_dir, "load writes one artifact directory");
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(report["row_counts"]["source"], 9);
    assert_eq!(report["row_counts"]["written"], 1);
    assert_eq!(report["row_counts"]["rejected"], 8);

    let artifact_path = PathBuf::from(
        report["rejected_records"]["artifact"]
            .as_str()
            .expect("artifact path"),
    );
    let rejected_lines = read_rejected_records(&artifact_path);
    assert_eq!(rejected_lines.len(), 8);
    for (rejected, line, field, cause_part) in [
        (&rejected_lines[0], 3, "created_at", "carries a UTC offset"),
        (&rejected_lines[1], 4, "created_at", "date-only text"),
        (
            &rejected_lines[2],
            5,
            "created_at",
            "epoch numbers are not accepted",
        ),
        (&rejected_lines[3], 6, "created_at", "more than 6 digits"),
        (&rejected_lines[4], 7, "amount", "more than scale 2"),
        (&rejected_lines[5], 8, "amount", "overflows decimal(10,2)"),
        (&rejected_lines[6], 9, "amount", "exponent notation"),
        (&rejected_lines[7], 10, "amount", "thousands separators"),
    ] {
        assert_eq!(rejected["line"], line, "artifact line for {cause_part:?}");
        assert_eq!(rejected["code"], "type_coercion_failed");
        assert_eq!(rejected["field"], field);
        let message = rejected["message"].as_str().expect("artifact message");
        assert!(
            message.contains(cause_part),
            "message {message:?} misses the cause {cause_part:?}"
        );
    }

    // Only the clean record reached the destination.
    let batch = read_single_parquet_batch(&single_parquet_file(&destination_path));
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn jsonl_declared_type_misfits_reject_json_shapes_naming_the_cause() {
    // The JSON-shape half of the rejection menu (ADR-0043/0044): epoch
    // numbers under a timestamp, an offset-less instant, a JSON float whose
    // digits were already lost, and an over-precision integer each reject
    // per record with the cause named.
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("orders.jsonl");
    fs::write(
        &source_path,
        "{\"settled_at\": \"2026-07-14T17:00:00Z\", \"amount\": \"1.2\"}\n\
         {\"settled_at\": 1784048400, \"amount\": \"1.2\"}\n\
         {\"settled_at\": \"2026-07-14T17:00:00\", \"amount\": \"1.2\"}\n\
         {\"settled_at\": \"2026-07-14T17:00:00Z\", \"amount\": 1.2}\n\
         {\"settled_at\": \"2026-07-14T17:00:00Z\", \"amount\": 100000000}\n",
    )
    .expect("write source jsonl");

    let database_path = work.path().join("orders.duckdb");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: jsonl
destination:
  connector: duckdb
  path: {}
dataset: orders
load_mode: full_refresh
reject_threshold: 4
schema:
  overrides:
  - name: settled_at
    type: timestamptz
  - name: amount
    type: decimal(10,2)
"#,
            source_path.display(),
            database_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_dir = work.path().join("artifacts");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .success();

    let (_, report) = read_single_report(&artifacts_dir, "load writes one artifact directory");
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(report["row_counts"]["written"], 1);
    assert_eq!(report["row_counts"]["rejected"], 4);

    let artifact_path = PathBuf::from(
        report["rejected_records"]["artifact"]
            .as_str()
            .expect("artifact path"),
    );
    let rejected_lines = read_rejected_records(&artifact_path);
    assert_eq!(rejected_lines.len(), 4);
    for (rejected, line, field, cause_part) in [
        (
            &rejected_lines[0],
            2,
            "settled_at",
            "epoch numbers are not accepted",
        ),
        (
            &rejected_lines[1],
            3,
            "settled_at",
            "missing its mandatory UTC offset",
        ),
        (
            &rejected_lines[2],
            4,
            "amount",
            "JSON floats do not fit a declared decimal",
        ),
        (&rejected_lines[3], 5, "amount", "overflows decimal(10,2)"),
    ] {
        assert_eq!(rejected["line"], line, "artifact line for {cause_part:?}");
        assert_eq!(rejected["code"], "type_coercion_failed");
        assert_eq!(rejected["field"], field);
        let message = rejected["message"].as_str().expect("artifact message");
        assert!(
            message.contains(cause_part),
            "message {message:?} misses the cause {cause_part:?}"
        );
    }

    // The JSONL leg of the destination matrix: the surviving record lands in
    // DuckDB under the declared column types.
    assert_eq!(
        duckdb_column_types(&database_path, "orders"),
        vec![
            (
                "settled_at".to_string(),
                "TIMESTAMP WITH TIME ZONE".to_string()
            ),
            ("amount".to_string(), "DECIMAL(10,2)".to_string()),
        ]
    );
    let batch = read_single_duckdb_batch(&database_path, "orders");
    assert_eq!(batch.num_rows(), 1);
    let settled = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("settled_at reads back as microsecond timestamps");
    assert_eq!(settled.value(0), DECLARED_INSTANT_MICROS);
    let amounts = batch
        .column(1)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("amount reads back as decimal128");
    assert_eq!(amounts.value(0), 120);
}

#[test]
fn declared_type_pin_bootstraps_and_an_omitted_override_fails_as_drift() {
    // ADR-0042 pinning: the bootstrap pin prints the declared names exactly
    // as declared under the unchanged pin contract version, and a later load
    // omitting the overrides fails as drift naming pinned vs effective type
    // with the missing-override hint — before touching the destination.
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("orders.csv");
    fs::write(
        &source_path,
        "created_at,amount,note\n2026-07-14 17:00:00,1.2,a\n",
    )
    .expect("write source csv");

    let destination_path = work.path().join("orders_dataset");
    let pinned_path = work.path().join("orders.schema.yml");
    let declared_definition = work.path().join("load.yml");
    fs::write(
        &declared_definition,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: parquet
  path: {}
dataset: orders
load_mode: full_refresh
schema:
  pinned_path: {}
  overrides:
  - name: created_at
    type: timestamp
  - name: amount
    type: decimal(10,2)
"#,
            source_path.display(),
            destination_path.display(),
            pinned_path.display()
        ),
    )
    .expect("write load definition");

    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(work.path().join("artifacts-first"))
        .arg(&declared_definition)
        .assert()
        .success();

    assert_eq!(
        fs::read_to_string(&pinned_path).expect("pinned schema file persisted"),
        "version: 1\n\
         fields:\n\
         - name: created_at\n\
         \x20 type: timestamp\n\
         \x20 nullable: true\n\
         - name: amount\n\
         \x20 type: decimal(10,2)\n\
         \x20 nullable: true\n\
         - name: note\n\
         \x20 type: utf8\n\
         \x20 nullable: true\n"
    );
    let (_, first_report) = read_single_report(
        &work.path().join("artifacts-first"),
        "bootstrap load writes one artifact directory",
    );
    assert_eq!(
        first_report["schema_decision"]["pinned_schema_persisted"],
        true
    );
    // The CSV leg of the destination matrix: the bootstrap load's Parquet
    // dataset materializes the declared column types.
    let first_batch = read_single_parquet_batch(&single_parquet_file(&destination_path));
    assert_eq!(
        first_batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>(),
        vec![
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Decimal128(10, 2),
            DataType::Utf8,
        ]
    );

    // The same source under the same pin, but the definition no longer
    // declares the types: the pin alone cannot resurrect them.
    let undeclared_definition = work.path().join("load-undeclared.yml");
    fs::write(
        &undeclared_definition,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: parquet
  path: {}
dataset: orders
load_mode: full_refresh
schema:
  pinned_path: {}
"#,
            source_path.display(),
            destination_path.display(),
            pinned_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_second = work.path().join("artifacts-second");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_second)
        .arg(&undeclared_definition)
        .assert()
        .failure();

    let (_, second_report) = read_single_report(
        &artifacts_second,
        "drift-failed load writes one artifact directory",
    );
    assert_eq!(second_report["exit_status"], "failed");
    assert_eq!(second_report["error_summary"]["code"], "schema_drift");
    let message = second_report["error_summary"]["message"]
        .as_str()
        .expect("error message");
    assert!(
        message.contains("\"created_at\" is pinned as timestamp but its effective type is utf8"),
        "message {message:?} misses the created_at drift"
    );
    assert!(
        message.contains("\"amount\" is pinned as decimal(10,2) but its effective type is float64"),
        "message {message:?} misses the amount drift"
    );
    assert!(
        message.contains("override may be missing"),
        "message {message:?} misses the hint"
    );
    assert_eq!(
        second_report["schema_decision"]["drift_status"],
        "failed_on_drift"
    );
    assert_eq!(
        second_report["schema_decision"]["drift"]["undeclared_fields"],
        serde_json::json!([
            {"name": "created_at", "pinned_type": "timestamp", "effective_type": "utf8"},
            {"name": "amount", "pinned_type": "decimal(10,2)", "effective_type": "float64"}
        ])
    );
    // The drift failure preceded destination work: the first load's dataset
    // is untouched.
    let batch = read_single_parquet_batch(&single_parquet_file(&destination_path));
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn declared_type_conflicts_and_malformed_declarations_fail_before_any_write() {
    // ADR-0042/0044 declaration failures are load failures, never per-record
    // rejections: a parameter change against the pin is a contradiction
    // under either drift policy, a malformed declaration fails validation,
    // and a pin file carrying a malformed type string fails its contract —
    // all before the destination exists.
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("orders.csv");
    fs::write(&source_path, "amount\n1.2\n").expect("write source csv");
    let pinned_path = work.path().join("orders.schema.yml");
    fs::write(
        &pinned_path,
        "version: 1\nfields:\n- name: amount\n  type: decimal(10,2)\n",
    )
    .expect("write pinned schema");

    let destination_path = work.path().join("orders_dataset");
    let definition = |schema_block: &str, load_name: &str| {
        let definition_path = work.path().join(load_name);
        fs::write(
            &definition_path,
            format!(
                r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: parquet
  path: {}
dataset: orders
load_mode: full_refresh
schema:
{schema_block}
"#,
                source_path.display(),
                destination_path.display()
            ),
        )
        .expect("write load definition");
        definition_path
    };

    // A precision change against the pin contradicts it under both policies.
    for (index, drift_policy) in ["fail", "allow_additive_nullable"].iter().enumerate() {
        let definition_path = definition(
            &format!(
                "  pinned_path: {}\n\
                 \x20 drift_policy: {drift_policy}\n\
                 \x20 overrides:\n\
                 \x20 - name: amount\n\
                 \x20   type: decimal(12,2)",
                pinned_path.display()
            ),
            &format!("load-conflict-{index}.yml"),
        );
        let artifacts_dir = work.path().join(format!("artifacts-conflict-{index}"));
        Command::cargo_bin("data-spark")
            .expect("binary")
            .current_dir(work.path())
            .arg("load")
            .arg("--output-dir")
            .arg(&artifacts_dir)
            .arg(&definition_path)
            .assert()
            .failure();
        let (_, report) = read_single_report(
            &artifacts_dir,
            "conflict load writes one artifact directory",
        );
        assert_eq!(
            report["error_summary"]["code"], "schema_override_conflict",
            "under drift_policy {drift_policy}"
        );
        let message = report["error_summary"]["message"]
            .as_str()
            .expect("error message");
        assert!(
            message.contains("pinned type decimal(10,2), override type decimal(12,2)"),
            "message {message:?} misses the parameter contradiction"
        );
    }

    // Malformed declarations fail override validation before any read.
    for (index, malformed_type) in ["decimal", "decimal(39,2)", "decimal(10,11)"]
        .iter()
        .enumerate()
    {
        let definition_path = definition(
            &format!(
                "  overrides:\n\
                 \x20 - name: amount\n\
                 \x20   type: {malformed_type}"
            ),
            &format!("load-malformed-{index}.yml"),
        );
        let artifacts_dir = work.path().join(format!("artifacts-malformed-{index}"));
        Command::cargo_bin("data-spark")
            .expect("binary")
            .current_dir(work.path())
            .arg("load")
            .arg("--output-dir")
            .arg(&artifacts_dir)
            .arg(&definition_path)
            .assert()
            .failure();
        let (_, report) = read_single_report(
            &artifacts_dir,
            "malformed-declaration load writes one artifact directory",
        );
        assert_eq!(report["error_summary"]["code"], "unsupported_override_type");
        let message = report["error_summary"]["message"]
            .as_str()
            .expect("error message");
        assert!(
            message.contains(*malformed_type),
            "message {message:?} misses the malformed type {malformed_type:?}"
        );
    }

    // A hand-edited pin carrying a malformed type string fails its contract.
    fs::write(
        &pinned_path,
        "version: 1\nfields:\n- name: amount\n  type: \"decimal(10, 2)\"\n",
    )
    .expect("write malformed pinned schema");
    let definition_path = definition(
        &format!("  pinned_path: {}", pinned_path.display()),
        "load-malformed-pin.yml",
    );
    let artifacts_dir = work.path().join("artifacts-malformed-pin");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_dir)
        .arg(&definition_path)
        .assert()
        .failure();
    let (_, report) = read_single_report(
        &artifacts_dir,
        "malformed-pin load writes one artifact directory",
    );
    assert_eq!(report["error_summary"]["code"], "invalid_pinned_schema");
    assert!(
        report["error_summary"]["message"]
            .as_str()
            .expect("error message")
            .contains("unsupported pinned schema field type: decimal(10, 2)"),
        "message misses the malformed pin type"
    );

    // None of the failures created the destination.
    assert!(
        !destination_path.exists(),
        "declaration failures must precede destination writes"
    );
}

#[test]
fn an_added_declared_type_field_is_additive_drift_and_rewrites_the_pin() {
    // ADR-0042 meets ADR-0034's additive policy: a newly added nullable
    // declared-type field is legal additive drift — the override shapes it,
    // the rewritten pin records the declared name, and DuckDB stores the
    // new column as TIMESTAMPTZ.
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("orders.csv");
    fs::write(&source_path, "order_id\n1\n").expect("write source csv");

    let database_path = work.path().join("orders.duckdb");
    let pinned_path = work.path().join("orders.schema.yml");
    let first_definition = work.path().join("load-first.yml");
    fs::write(
        &first_definition,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: duckdb
  path: {}
dataset: orders
load_mode: full_refresh
schema:
  pinned_path: {}
"#,
            source_path.display(),
            database_path.display(),
            pinned_path.display()
        ),
    )
    .expect("write load definition");

    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(work.path().join("artifacts-first"))
        .arg(&first_definition)
        .assert()
        .success();

    // The source gains settled_at, declared timestamptz by the second load.
    fs::write(
        &source_path,
        "order_id,settled_at\n2,2026-07-15T01:00:00+08:00\n",
    )
    .expect("write extended source csv");
    let second_definition = work.path().join("load-second.yml");
    fs::write(
        &second_definition,
        format!(
            r#"
version: 1
source:
  connector: local_file
  path: {}
  format: csv
destination:
  connector: duckdb
  path: {}
dataset: orders
load_mode: full_refresh
schema:
  pinned_path: {}
  drift_policy: allow_additive_nullable
  overrides:
  - name: settled_at
    type: timestamptz
"#,
            source_path.display(),
            database_path.display(),
            pinned_path.display()
        ),
    )
    .expect("write load definition");

    let artifacts_second = work.path().join("artifacts-second");
    Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work.path())
        .arg("load")
        .arg("--output-dir")
        .arg(&artifacts_second)
        .arg(&second_definition)
        .assert()
        .success();

    let (_, report) = read_single_report(
        &artifacts_second,
        "additive load writes one artifact directory",
    );
    assert_eq!(
        report["schema_decision"]["drift_status"],
        "additive_fields_added"
    );
    assert_eq!(
        report["schema_decision"]["added_fields"],
        serde_json::json!([
            {"name": "settled_at", "type": "timestamptz", "nullable": true}
        ])
    );
    assert_eq!(
        fs::read_to_string(&pinned_path).expect("rewritten pin"),
        "version: 1\n\
         fields:\n\
         - name: order_id\n\
         \x20 type: int64\n\
         \x20 nullable: true\n\
         - name: settled_at\n\
         \x20 type: timestamptz\n\
         \x20 nullable: true\n"
    );
    assert_eq!(
        duckdb_column_types(&database_path, "orders"),
        vec![
            ("order_id".to_string(), "BIGINT".to_string()),
            (
                "settled_at".to_string(),
                "TIMESTAMP WITH TIME ZONE".to_string()
            ),
        ]
    );
    let batch = read_single_duckdb_batch(&database_path, "orders");
    let settled = batch
        .column(1)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("settled_at reads back as microsecond timestamps");
    assert_eq!(settled.value(0), DECLARED_INSTANT_MICROS);
}

struct CliLoadResult {
    stdout: String,
    report_path: PathBuf,
    report: Value,
}

// ---- Chunked execution (issue #52: ADR-0045, ADR-0046, ADR-0047) ----

/// Writes a load definition with an `execution.chunk_rows` block and
/// optional extra blocks appended verbatim.
fn write_chunked_definition(
    definition_path: &Path,
    source_path: &Path,
    destination_connector: &str,
    destination_path: &Path,
    load_mode: &str,
    chunk_rows: u64,
    extra_blocks: &str,
) {
    let source_format = source_path
        .extension()
        .and_then(|extension| extension.to_str())
        .expect("source path carries its format extension");
    fs::write(
        definition_path,
        format!(
            "version: 1\n\
             source:\n\
             \x20 connector: local_file\n\
             \x20 path: {}\n\
             \x20 format: {source_format}\n\
             destination:\n\
             \x20 connector: {destination_connector}\n\
             \x20 path: {}\n\
             dataset: customers\n\
             load_mode: {load_mode}\n\
             execution:\n\
             \x20 chunk_rows: {chunk_rows}\n\
             {extra_blocks}",
            source_path.display(),
            destination_path.display(),
        ),
    )
    .expect("write load definition");
}

#[test]
fn multi_chunk_full_refresh_matches_single_chunk_content_for_both_connectors() {
    // 5 records at a chunk bound of 2 execute as 3 chunks. Full refresh
    // keeps its single terminal commit (ADR-0047): Parquet lands one part
    // per chunk behind one rename, DuckDB lands one table holding every
    // chunk, and both report the committed chunk count with the echoed
    // bound.
    for destination_connector in ["parquet", "duckdb"] {
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.csv");
        fs::write(
            &source_path,
            "customer_id,name\n1,Ada\n2,Grace\n3,Cara\n4,Dana\n5,Elle\n",
        )
        .expect("write source csv");
        let destination_path = match destination_connector {
            "parquet" => work.path().join("customers_dataset"),
            _ => work.path().join("customers.duckdb"),
        };
        let definition_path = work.path().join("load.yml");
        write_chunked_definition(
            &definition_path,
            &source_path,
            destination_connector,
            &destination_path,
            "full_refresh",
            2,
            "",
        );

        let load = run_cli_load(
            work.path(),
            &work.path().join("artifacts"),
            &definition_path,
            true,
        );
        let report = &load.report;

        assert_eq!(
            report["exit_status"], "succeeded",
            "{destination_connector}"
        );
        assert_eq!(report["row_counts"]["source"], 5);
        assert_eq!(report["row_counts"]["written"], 5);
        assert_eq!(report["execution"]["record_format"], "arrow_record_batch");
        assert_eq!(
            report["execution"]["batch_count"], 3,
            "batch_count counts committed chunks: ceil(5 / 2)"
        );
        assert_eq!(report["execution"]["chunk_rows"], 2);

        match destination_connector {
            "parquet" => {
                assert_eq!(
                    report["destination_write"]["strategy"],
                    "staging_then_replace"
                );
                let records = id_name_records(&read_parquet_batches(&destination_path));
                assert_eq!(records.len(), 5);
                assert_eq!(records[0], (1, "Ada".to_string()));
                assert_eq!(records[4], (5, "Elle".to_string()));
                assert_eq!(
                    parquet_files(&destination_path).len(),
                    3,
                    "one part per chunk"
                );
            }
            _ => {
                assert_eq!(
                    report["destination_write"]["strategy"],
                    "transactional_replace"
                );
                let batch = read_single_duckdb_batch(&destination_path, "customers");
                assert_eq!(batch.num_rows(), 5);
            }
        }
    }
}

#[test]
fn multi_chunk_append_commits_one_chunk_at_a_time_for_both_connectors() {
    // Append commits per chunk (ADR-0047): 5 records at a bound of 2 land as
    // 3 committed chunks on top of the seeded dataset.
    for destination_connector in ["parquet", "duckdb"] {
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.csv");
        let destination_path = match destination_connector {
            "parquet" => work.path().join("customers_dataset"),
            _ => work.path().join("customers.duckdb"),
        };
        let definition_path = work.path().join("load.yml");

        fs::write(&source_path, "customer_id,name\n1,Ada\n").expect("write seed csv");
        write_load_definition(
            &definition_path,
            &source_path,
            "csv",
            destination_connector,
            &destination_path,
            "full_refresh",
            None,
        );
        run_cli_load(
            work.path(),
            &work.path().join("artifacts-seed"),
            &definition_path,
            true,
        );

        fs::write(
            &source_path,
            "customer_id,name\n2,Grace\n3,Cara\n4,Dana\n5,Elle\n6,Faye\n",
        )
        .expect("write append csv");
        write_chunked_definition(
            &definition_path,
            &source_path,
            destination_connector,
            &destination_path,
            "append",
            2,
            "",
        );
        let append = run_cli_load(
            work.path(),
            &work.path().join("artifacts-append"),
            &definition_path,
            true,
        );
        let report = &append.report;

        assert_eq!(
            report["exit_status"], "succeeded",
            "{destination_connector}"
        );
        assert_eq!(report["row_counts"]["written"], 5);
        assert_eq!(report["execution"]["batch_count"], 3);
        assert_eq!(report["execution"]["chunk_rows"], 2);

        match destination_connector {
            "parquet" => {
                assert_eq!(
                    report["destination_write"]["strategy"],
                    "staged_part_append"
                );
                let records = id_name_records(&read_parquet_batches(&destination_path));
                assert_eq!(records.len(), 6, "the seed record plus five appended");
                assert_eq!(
                    parquet_files(&destination_path).len(),
                    4,
                    "the seed part plus one committed part per append chunk"
                );
            }
            _ => {
                assert_eq!(report["destination_write"]["strategy"], "insert");
                let batch = read_single_duckdb_batch(&destination_path, "customers");
                assert_eq!(batch.num_rows(), 6);
            }
        }
    }
}

#[test]
fn an_empty_execution_block_defaults_the_chunk_bound() {
    // `execution: {}` declares nothing, so the bound defaults to 65536
    // exactly as an absent block does (ADR-0046) — no new failure code
    // exists for the execution block.
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    fs::write(&source_path, "customer_id\n1\n").expect("write source csv");
    let destination_path = work.path().join("customers_dataset");
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        format!(
            "version: 1\n\
             source:\n\
             \x20 connector: local_file\n\
             \x20 path: {}\n\
             \x20 format: csv\n\
             destination:\n\
             \x20 connector: parquet\n\
             \x20 path: {}\n\
             dataset: customers\n\
             load_mode: full_refresh\n\
             execution: {{}}\n",
            source_path.display(),
            destination_path.display(),
        ),
    )
    .expect("write load definition");

    let load = run_cli_load(
        work.path(),
        &work.path().join("artifacts"),
        &definition_path,
        true,
    );
    assert_eq!(load.report["exit_status"], "succeeded");
    assert_eq!(load.report["execution"]["chunk_rows"], 65536);
}

#[test]
fn invalid_execution_blocks_fail_before_source_or_destination_work() {
    // The execution block is part of the strict contract: a zero, negative,
    // or non-integer bound fails YAML parsing (`chunk_rows` is a nonzero
    // integer), and an unknown key is rejected recursively (ADR-0037) — all
    // before any source read or destination write, and all under the
    // existing definition failure code.
    for (execution_block, expected_code, expected_message_part) in [
        (
            "execution:\n  chunk_rows: 0\n",
            "invalid_load_definition_yaml",
            "expected a nonzero u64",
        ),
        (
            "execution:\n  workers: 4\n",
            "invalid_load_definition_yaml",
            "unknown field `workers`",
        ),
        (
            "execution:\n  chunk_rows: -1\n",
            "invalid_load_definition_yaml",
            "expected a nonzero u64",
        ),
    ] {
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.csv");
        fs::write(&source_path, "customer_id\n1\n").expect("write source csv");
        let destination_path = work.path().join("customers_dataset");
        let definition_path = work.path().join("load.yml");
        fs::write(
            &definition_path,
            format!(
                "version: 1\n\
                 source:\n\
                 \x20 connector: local_file\n\
                 \x20 path: {}\n\
                 \x20 format: csv\n\
                 destination:\n\
                 \x20 connector: parquet\n\
                 \x20 path: {}\n\
                 dataset: customers\n\
                 load_mode: full_refresh\n\
                 {execution_block}",
                source_path.display(),
                destination_path.display(),
            ),
        )
        .expect("write load definition");

        let load = run_cli_load(
            work.path(),
            &work.path().join("artifacts"),
            &definition_path,
            false,
        );
        let report = &load.report;

        assert_eq!(
            report["error_summary"]["code"], expected_code,
            "{execution_block:?}"
        );
        let message = report["error_summary"]["message"]
            .as_str()
            .expect("error message");
        assert!(
            message.contains(expected_message_part),
            "message {message:?} misses {expected_message_part:?}"
        );
        assert_eq!(report["execution"]["record_format"], "not_started");
        assert_eq!(report["execution"]["batch_count"], 0);
        assert!(
            !destination_path.exists(),
            "an invalid execution block must not reach the destination"
        );
    }
}

#[test]
fn a_late_reject_threshold_breach_leaves_destination_and_pin_untouched() {
    // The rejection sits in the last record: pass 1 evaluates the threshold
    // over the full input before any write (ADR-0045), so even at a chunk
    // bound of 1 — where earlier records would already have filled chunks —
    // the breach fails the load with today's code and wording, no
    // destination content, and no persisted pin.
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    fs::write(&source_path, "customer_id\n1\n2\n3\n4,extra\n").expect("write source csv");
    let destination_path = work.path().join("customers_dataset");
    let pinned_path = work.path().join("customers.schema.yml");
    let definition_path = work.path().join("load.yml");
    write_chunked_definition(
        &definition_path,
        &source_path,
        "parquet",
        &destination_path,
        "full_refresh",
        1,
        &format!("schema:\n  pinned_path: {}\n", pinned_path.display()),
    );

    let load = run_cli_load(
        work.path(),
        &work.path().join("artifacts"),
        &definition_path,
        false,
    );
    let report = &load.report;

    assert_eq!(report["error_summary"]["code"], "reject_threshold_exceeded");
    assert_eq!(
        report["error_summary"]["message"],
        "rejected 1 of 4 records, exceeding the reject threshold of 0"
    );
    assert_eq!(report["row_counts"]["source"], 4);
    assert_eq!(report["row_counts"]["written"], 0);
    assert_eq!(report["row_counts"]["rejected"], 1);
    assert_eq!(report["execution"]["record_format"], "not_started");
    assert_eq!(report["execution"]["batch_count"], 0);
    assert!(
        !destination_path.exists(),
        "a threshold breach anywhere in the source precedes any destination write"
    );
    assert!(
        !pinned_path.exists(),
        "a threshold-failed load must not persist the pin"
    );
}

#[test]
fn multi_chunk_pinned_loads_resolve_schema_against_the_whole_input() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.jsonl");
    let destination_path = work.path().join("customers_dataset");
    let pinned_path = work.path().join("customers.schema.yml");
    let definition_path = work.path().join("load.yml");

    // Bootstrap at a chunk bound of 1: `total` reads int64 in the first
    // record and only widens to float64 in the last — the pin must record
    // the whole-input observation, not the first chunk's.
    fs::write(
        &source_path,
        "{\"id\": 1, \"total\": 1}\n{\"id\": 2, \"total\": 2}\n{\"id\": 3, \"total\": 2.5}\n",
    )
    .expect("write source jsonl");
    write_chunked_definition(
        &definition_path,
        &source_path,
        "parquet",
        &destination_path,
        "full_refresh",
        1,
        &format!("schema:\n  pinned_path: {}\n", pinned_path.display()),
    );
    let bootstrap = run_cli_load(
        work.path(),
        &work.path().join("artifacts-bootstrap"),
        &definition_path,
        true,
    );
    assert_eq!(bootstrap.report["execution"]["batch_count"], 3);
    assert_eq!(
        bootstrap.report["schema_decision"]["fields"][1],
        serde_json::json!({"name": "total", "type": "float64", "nullable": true})
    );
    let pin_text = fs::read_to_string(&pinned_path).expect("bootstrapped pin");
    assert!(
        pin_text.contains("type: float64"),
        "pin {pin_text:?} records the whole-input widening"
    );

    // Additive drift under the pin: `note` exists only in the final record,
    // so the whole-input union must admit it as one added field — and a
    // pinned field carried by every record is never missing-field drift just
    // because single-record chunks lack it.
    fs::write(
        &source_path,
        "{\"id\": 1, \"total\": 1}\n{\"id\": 2, \"total\": 2}\n\
         {\"id\": 3, \"total\": 2.5, \"note\": \"vip\"}\n",
    )
    .expect("write drifted jsonl");
    write_chunked_definition(
        &definition_path,
        &source_path,
        "parquet",
        &destination_path,
        "full_refresh",
        1,
        &format!(
            "schema:\n  pinned_path: {}\n  drift_policy: allow_additive_nullable\n",
            pinned_path.display()
        ),
    );
    let drifted = run_cli_load(
        work.path(),
        &work.path().join("artifacts-drift"),
        &definition_path,
        true,
    );
    assert_eq!(
        drifted.report["schema_decision"]["drift_status"],
        "additive_fields_added"
    );
    assert_eq!(
        drifted.report["schema_decision"]["added_fields"],
        serde_json::json!([{"name": "note", "type": "utf8", "nullable": true}])
    );
    let extended_pin = fs::read_to_string(&pinned_path).expect("extended pin");
    assert!(
        extended_pin.contains("name: note"),
        "pin {extended_pin:?} carries the added field"
    );
    let records = read_parquet_batches(&destination_path);
    let total_rows: usize = records.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

#[test]
fn a_drift_failed_multi_chunk_load_keeps_only_parse_rejections_in_the_artifact() {
    // The pin names a field absent from every record — missing-field drift
    // judged against the whole input — while the source interleaves a parse
    // rejection and a would-be validation rejection. The drift failure must
    // leave an artifact carrying only the parse rejection (ADR-0045's
    // spill-discard).
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.jsonl");
    fs::write(&source_path, "{\"id\": 1}\n[1]\n{\"id\": \"abc\"}\n").expect("write source jsonl");
    let pinned_path = work.path().join("customers.schema.yml");
    fs::write(
        &pinned_path,
        "version: 1\n\
         fields:\n\
         - name: id\n\
         \x20 type: int64\n\
         - name: missing\n\
         \x20 type: utf8\n",
    )
    .expect("write pinned schema");
    let destination_path = work.path().join("customers_dataset");
    let definition_path = work.path().join("load.yml");
    write_chunked_definition(
        &definition_path,
        &source_path,
        "parquet",
        &destination_path,
        "full_refresh",
        1,
        &format!(
            "schema:\n  pinned_path: {}\nreject_threshold: 5\n",
            pinned_path.display()
        ),
    );

    let load = run_cli_load(
        work.path(),
        &work.path().join("artifacts"),
        &definition_path,
        false,
    );
    let report = &load.report;

    assert_eq!(report["error_summary"]["code"], "schema_drift");
    assert_eq!(report["schema_decision"]["drift_status"], "failed_on_drift");
    assert_eq!(report["rejected_records"]["count"], 1);
    let artifact_path = PathBuf::from(
        report["rejected_records"]["artifact"]
            .as_str()
            .expect("artifact path"),
    );
    let rejected_lines = read_rejected_records(&artifact_path);
    assert_eq!(
        rejected_lines.len(),
        1,
        "a shape-drift failure's artifact carries only parse rejections"
    );
    assert_eq!(rejected_lines[0]["line"], 2);
    assert_eq!(rejected_lines[0]["code"], "malformed_jsonl_record");
    assert!(!destination_path.exists());
}

#[test]
fn multi_chunk_artifacts_interleave_parse_and_validation_rejections_byte_identically() {
    // CSV under a pin: the parse rejection on line 3 and the validation
    // rejection on line 4 interleave in source-line order, and every line
    // matches the exact artifact rendering.
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    fs::write(&source_path, "id\n1\n2,extra\nabc\n5\n").expect("write source csv");
    let pinned_path = work.path().join("customers.schema.yml");
    fs::write(
        &pinned_path,
        "version: 1\nfields:\n- name: id\n  type: int64\n",
    )
    .expect("write pinned schema");
    let destination_path = work.path().join("customers_dataset");
    let definition_path = work.path().join("load.yml");
    write_chunked_definition(
        &definition_path,
        &source_path,
        "parquet",
        &destination_path,
        "full_refresh",
        1,
        &format!(
            "schema:\n  pinned_path: {}\nreject_threshold: 2\n",
            pinned_path.display()
        ),
    );

    let load = run_cli_load(
        work.path(),
        &work.path().join("artifacts"),
        &definition_path,
        true,
    );
    let artifact_path = PathBuf::from(
        load.report["rejected_records"]["artifact"]
            .as_str()
            .expect("artifact path"),
    );
    let artifact = fs::read_to_string(&artifact_path).expect("artifact");
    let expected = format!(
        "{}\n{}\n",
        serde_json::json!({
            "line": 3,
            "code": "malformed_csv_record",
            "field": null,
            "source_field": null,
            "message": "expected 1 fields, found 2",
            "record": ["2", "extra"]
        }),
        serde_json::json!({
            "line": 4,
            "code": "type_coercion_failed",
            "field": "id",
            "source_field": null,
            "message": "value \"abc\" does not fit pinned type int64 for field \"id\"",
            "record": {"id": "abc"}
        })
    );
    assert_eq!(artifact, expected);

    // JSONL under an override: the parse rejection streams while the
    // validation rejection spills through the end-of-input merge — the
    // final artifact still reads in source-line order, byte-identically.
    let jsonl_path = work.path().join("customers.jsonl");
    fs::write(
        &jsonl_path,
        "{\"id\": 1, \"name\": \"Ada\"}\n[1]\n{\"id\": 3, \"name\": null}\n{\"id\": 4, \"name\": \"Cara\"}\n",
    )
    .expect("write source jsonl");
    let jsonl_definition_path = work.path().join("load-jsonl.yml");
    write_chunked_definition(
        &jsonl_definition_path,
        &jsonl_path,
        "parquet",
        &work.path().join("customers_jsonl_dataset"),
        "full_refresh",
        1,
        "schema:\n  overrides:\n  - name: name\n    nullable: false\nreject_threshold: 2\n",
    );

    let jsonl_load = run_cli_load(
        work.path(),
        &work.path().join("artifacts-jsonl"),
        &jsonl_definition_path,
        true,
    );
    let jsonl_artifact_path = PathBuf::from(
        jsonl_load.report["rejected_records"]["artifact"]
            .as_str()
            .expect("artifact path"),
    );
    let jsonl_artifact = fs::read_to_string(&jsonl_artifact_path).expect("artifact");
    let jsonl_expected = format!(
        "{}\n{}\n",
        serde_json::json!({
            "line": 2,
            "code": "malformed_jsonl_record",
            "field": null,
            "source_field": null,
            "message": "each JSONL record must be a JSON object",
            "record": "[1]"
        }),
        serde_json::json!({
            "line": 3,
            "code": "missing_required_field",
            "field": "name",
            "source_field": null,
            "message": "required field \"name\" is null",
            "record": {"id": 3, "name": null}
        })
    );
    assert_eq!(jsonl_artifact, jsonl_expected);
}

#[test]
fn multi_chunk_artifacts_stay_byte_identical_for_inferred_csv_and_pinned_jsonl() {
    // The remaining two directive cells of the artifact matrix: CSV under an
    // inference-driven override, and JSONL under a pin — each interleaving a
    // parse and a validation rejection across single-record chunks.
    let work = TempDir::new().expect("tempdir");

    // CSV, inferred directive with an override: the override supplies the
    // per-record check inference alone would not impose.
    let source_path = work.path().join("customers.csv");
    fs::write(&source_path, "id\n1\n2,extra\nabc\n5\n").expect("write source csv");
    let definition_path = work.path().join("load.yml");
    write_chunked_definition(
        &definition_path,
        &source_path,
        "parquet",
        &work.path().join("customers_dataset"),
        "full_refresh",
        1,
        "schema:\n  overrides:\n  - name: id\n    type: int64\nreject_threshold: 2\n",
    );

    let load = run_cli_load(
        work.path(),
        &work.path().join("artifacts"),
        &definition_path,
        true,
    );
    let artifact_path = PathBuf::from(
        load.report["rejected_records"]["artifact"]
            .as_str()
            .expect("artifact path"),
    );
    let artifact = fs::read_to_string(&artifact_path).expect("artifact");
    let expected = format!(
        "{}\n{}\n",
        serde_json::json!({
            "line": 3,
            "code": "malformed_csv_record",
            "field": null,
            "source_field": null,
            "message": "expected 1 fields, found 2",
            "record": ["2", "extra"]
        }),
        serde_json::json!({
            "line": 4,
            "code": "type_coercion_failed",
            "field": "id",
            "source_field": null,
            "message": "value \"abc\" does not fit overridden type int64 for field \"id\"",
            "record": {"id": "abc"}
        })
    );
    assert_eq!(artifact, expected);

    // JSONL under a pin: the validation rejection spills until the shape
    // verdict passes and merges back in line order.
    let jsonl_path = work.path().join("customers.jsonl");
    fs::write(
        &jsonl_path,
        "{\"id\": 1}\n[1]\n{\"id\": \"abc\"}\n{\"id\": 4}\n",
    )
    .expect("write source jsonl");
    let pinned_path = work.path().join("customers.schema.yml");
    fs::write(
        &pinned_path,
        "version: 1\nfields:\n- name: id\n  type: int64\n",
    )
    .expect("write pinned schema");
    let jsonl_definition_path = work.path().join("load-jsonl.yml");
    write_chunked_definition(
        &jsonl_definition_path,
        &jsonl_path,
        "parquet",
        &work.path().join("customers_jsonl_dataset"),
        "full_refresh",
        1,
        &format!(
            "schema:\n  pinned_path: {}\nreject_threshold: 2\n",
            pinned_path.display()
        ),
    );

    let jsonl_load = run_cli_load(
        work.path(),
        &work.path().join("artifacts-jsonl"),
        &jsonl_definition_path,
        true,
    );
    let jsonl_artifact_path = PathBuf::from(
        jsonl_load.report["rejected_records"]["artifact"]
            .as_str()
            .expect("artifact path"),
    );
    let jsonl_artifact = fs::read_to_string(&jsonl_artifact_path).expect("artifact");
    let jsonl_expected = format!(
        "{}\n{}\n",
        serde_json::json!({
            "line": 2,
            "code": "malformed_jsonl_record",
            "field": null,
            "source_field": null,
            "message": "each JSONL record must be a JSON object",
            "record": "[1]"
        }),
        serde_json::json!({
            "line": 3,
            "code": "type_coercion_failed",
            "field": "id",
            "source_field": null,
            "message": "value \"abc\" does not fit pinned type int64 for field \"id\"",
            "record": {"id": "abc"}
        })
    );
    assert_eq!(jsonl_artifact, jsonl_expected);
}

#[test]
fn chunk_split_is_deterministic_across_identical_runs() {
    // The same source bytes and the same bound must produce the same
    // per-chunk row counts — the future retry and parallelism unit — made
    // observable by append's one-part-per-chunk commits.
    let mut observed_splits = Vec::new();
    for _ in 0..2 {
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.csv");
        fs::write(
            &source_path,
            "customer_id,name\n1,Ada\n2,Grace\n3,Cara\n4,Dana\n5,Elle\n6,Faye\n7,Gwen\n8,Hope\n",
        )
        .expect("write source csv");
        let destination_path = work.path().join("customers_dataset");
        let definition_path = work.path().join("load.yml");
        write_chunked_definition(
            &definition_path,
            &source_path,
            "parquet",
            &destination_path,
            "append",
            3,
            "",
        );

        let load = run_cli_load(
            work.path(),
            &work.path().join("artifacts"),
            &definition_path,
            true,
        );
        assert_eq!(load.report["execution"]["batch_count"], 3);

        let mut part_rows = parquet_files(&destination_path)
            .iter()
            .map(|path| {
                read_parquet_file_batches(path)
                    .iter()
                    .map(|batch| batch.num_rows())
                    .sum::<usize>()
            })
            .collect::<Vec<_>>();
        part_rows.sort();
        assert_eq!(part_rows, vec![2, 3, 3], "chunks fill in source order");
        observed_splits.push(part_rows);
    }
    assert_eq!(observed_splits[0], observed_splits[1]);
}

// ---- Retry policy and attempt metadata (issue #51: ADR-0048, ADR-0049, ADR-0050) ----

/// The `execution.retry` echo of an all-defaults load: 3/200/5000 around the
/// always-present empty attempts array (ADR-0050).
fn default_retry_echo() -> Value {
    serde_json::json!({
        "max_attempts": 3,
        "initial_delay_ms": 200,
        "max_delay_ms": 5000,
        "attempts": []
    })
}

/// Writes a load definition whose `execution` block is appended verbatim —
/// empty for an absent block — so retry knobs vary per test.
fn write_retry_definition(
    definition_path: &Path,
    source_path: &Path,
    destination_connector: &str,
    destination_path: &Path,
    load_mode: &str,
    execution_block: &str,
) {
    fs::write(
        definition_path,
        format!(
            "version: 1\n\
             source:\n\
             \x20 connector: local_file\n\
             \x20 path: {}\n\
             \x20 format: csv\n\
             destination:\n\
             \x20 connector: {destination_connector}\n\
             \x20 path: {}\n\
             dataset: customers\n\
             load_mode: {load_mode}\n\
             {execution_block}",
            source_path.display(),
            destination_path.display(),
        ),
    )
    .expect("write load definition");
}

#[test]
fn success_reports_state_the_default_retry_policy_across_the_connector_matrix() {
    // Every write-phase report carries `execution.retry` (ADR-0050): the
    // effective policy echo — the 3/200/5000 defaults here — around an
    // always-present empty attempts array, across all four connector × mode
    // cells. No shipped failure is classified transient (ADR-0048), so the
    // attempts array is empty everywhere and the engine stays provably idle.
    for (destination_connector, load_mode) in [
        ("parquet", "full_refresh"),
        ("parquet", "append"),
        ("duckdb", "full_refresh"),
        ("duckdb", "append"),
    ] {
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.csv");
        fs::write(&source_path, "customer_id,name\n1,Ada\n").expect("write source csv");
        let destination_path = match destination_connector {
            "parquet" => work.path().join("customers_dataset"),
            _ => work.path().join("customers.duckdb"),
        };
        // A DuckDB append needs the destination table to exist.
        if destination_connector == "duckdb" && load_mode == "append" {
            let connection =
                duckdb::Connection::open(&destination_path).expect("open DuckDB destination");
            connection
                .execute_batch("CREATE TABLE customers (customer_id BIGINT, name VARCHAR)")
                .expect("seed DuckDB destination");
        }
        let definition_path = work.path().join("load.yml");
        write_retry_definition(
            &definition_path,
            &source_path,
            destination_connector,
            &destination_path,
            load_mode,
            "",
        );

        let load = run_cli_load(
            work.path(),
            &work.path().join("artifacts"),
            &definition_path,
            true,
        );
        let report = &load.report;

        assert_eq!(
            report["exit_status"], "succeeded",
            "{destination_connector}/{load_mode}"
        );
        assert_eq!(report["report_version"], 1);
        assert_eq!(report["execution"]["record_format"], "arrow_record_batch");
        assert_eq!(
            report["execution"]["retry"],
            default_retry_echo(),
            "{destination_connector}/{load_mode}"
        );
    }
}

#[test]
fn declared_retry_knobs_echo_faithfully_and_partial_blocks_keep_the_defaults() {
    // The report echoes the effective policy (ADR-0049): declared knobs win
    // individually, `retry: {}` equals an absent block, and `max_attempts: 1`
    // — the disable form — is a legal declaration echoed like any other.
    for (execution_block, expected_max_attempts, expected_initial, expected_max_delay) in [
        (
            "execution:\n\
             \x20 retry:\n\
             \x20   max_attempts: 5\n\
             \x20   initial_delay_ms: 50\n\
             \x20   max_delay_ms: 900\n",
            5,
            50,
            900,
        ),
        ("execution:\n  retry: {}\n", 3, 200, 5000),
        ("execution:\n  retry:\n    max_attempts: 1\n", 1, 200, 5000),
    ] {
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.csv");
        fs::write(&source_path, "customer_id\n1\n").expect("write source csv");
        let destination_path = work.path().join("customers_dataset");
        let definition_path = work.path().join("load.yml");
        write_retry_definition(
            &definition_path,
            &source_path,
            "parquet",
            &destination_path,
            "full_refresh",
            execution_block,
        );

        let load = run_cli_load(
            work.path(),
            &work.path().join("artifacts"),
            &definition_path,
            true,
        );

        assert_eq!(
            load.report["execution"]["retry"],
            serde_json::json!({
                "max_attempts": expected_max_attempts,
                "initial_delay_ms": expected_initial,
                "max_delay_ms": expected_max_delay,
                "attempts": []
            }),
            "{execution_block:?}"
        );
    }
}

#[test]
fn invalid_retry_blocks_fail_before_source_or_destination_work() {
    // The retry block is part of the strict contract (ADR-0049): a zero or
    // non-integer `max_attempts`, a negative delay, and an unknown key all
    // fail YAML parsing under the existing definition failure code, before
    // any source read or destination write — and the failed report keeps
    // the exact two-field `not_started` posture, with no retry object.
    for (execution_block, expected_message_part) in [
        (
            "execution:\n  retry:\n    max_attempts: 0\n",
            "expected a nonzero u64",
        ),
        (
            "execution:\n  retry:\n    max_attempts: 2.5\n",
            "max_attempts",
        ),
        (
            "execution:\n  retry:\n    initial_delay_ms: -1\n",
            "initial_delay_ms",
        ),
        (
            "execution:\n  retry:\n    jitter: true\n",
            "unknown field `jitter`",
        ),
    ] {
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.csv");
        fs::write(&source_path, "customer_id\n1\n").expect("write source csv");
        let destination_path = work.path().join("customers_dataset");
        let definition_path = work.path().join("load.yml");
        write_retry_definition(
            &definition_path,
            &source_path,
            "parquet",
            &destination_path,
            "full_refresh",
            execution_block,
        );

        let load = run_cli_load(
            work.path(),
            &work.path().join("artifacts"),
            &definition_path,
            false,
        );
        let report = &load.report;

        assert_eq!(
            report["error_summary"]["code"], "invalid_load_definition_yaml",
            "{execution_block:?}"
        );
        let message = report["error_summary"]["message"]
            .as_str()
            .expect("error message");
        assert!(
            message.contains(expected_message_part),
            "message {message:?} misses {expected_message_part:?}"
        );
        assert_eq!(
            report["execution"],
            serde_json::json!({
                "record_format": "not_started",
                "batch_count": 0
            }),
            "a never-retried failure report carries no retry object"
        );
        assert!(
            !destination_path.exists(),
            "an invalid retry block must not reach the destination"
        );
    }
}

#[test]
fn an_in_session_append_mismatch_carries_the_retry_object_with_zero_attempts() {
    // A schema-mismatched DuckDB append fails inside the open session: the
    // write-phase posture always carries `execution.retry` (ADR-0050), and
    // the local mismatch is terminal (ADR-0048), so the load retried
    // nothing and the attempts array is empty.
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    let database_path = work.path().join("customers.duckdb");
    let definition_path = work.path().join("load.yml");

    {
        let connection = duckdb::Connection::open(&database_path).expect("open DuckDB destination");
        connection
            .execute_batch("CREATE TABLE customers (id BIGINT); INSERT INTO customers VALUES (1)")
            .expect("seed DuckDB destination");
    }

    fs::write(&source_path, "id\n2.9\n").expect("write lossy append csv");
    write_retry_definition(
        &definition_path,
        &source_path,
        "duckdb",
        &database_path,
        "append",
        "",
    );

    let load = run_cli_load(
        work.path(),
        &work.path().join("artifacts"),
        &definition_path,
        false,
    );
    let report = &load.report;

    assert_eq!(report["error_summary"]["code"], "destination_write_failed");
    assert_eq!(
        report["execution"],
        serde_json::json!({
            "record_format": "arrow_record_batch",
            "batch_count": 0,
            "chunk_rows": 65536,
            "parallelism": 1,
            "connector_parallelism_limit": 1,
            "retry": default_retry_echo()
        }),
        "the in-session posture carries the full execution echo, \
         parallelism facts included (ADR-0053)"
    );
}

#[test]
fn a_failed_begin_keeps_the_exact_not_started_posture_without_a_retry_object() {
    // A DuckDB append against a missing table fails opening the session:
    // the report keeps the pre-write posture exactly as before — two fields,
    // no retry object — because a never-retried `not_started` failure tells
    // no retry story (ADR-0050's conditional presence rule).
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    let database_path = work.path().join("customers.duckdb");
    let definition_path = work.path().join("load.yml");
    fs::write(&source_path, "customer_id\n1\n").expect("write source csv");
    write_retry_definition(
        &definition_path,
        &source_path,
        "duckdb",
        &database_path,
        "append",
        "",
    );

    let load = run_cli_load(
        work.path(),
        &work.path().join("artifacts"),
        &definition_path,
        false,
    );
    let report = &load.report;

    assert_eq!(report["error_summary"]["code"], "destination_write_failed");
    assert!(report["error_summary"]["message"]
        .as_str()
        .expect("error message")
        .contains("before append"));
    assert_eq!(
        report["execution"],
        serde_json::json!({
            "record_format": "not_started",
            "batch_count": 0
        })
    );
    assert_eq!(report["destination_write"]["atomicity"], "not_applicable");
}

#[test]
fn configured_parallelism_clamps_to_the_serial_connector_limit_across_the_matrix() {
    // Both shipped destinations declare a Connector Parallelism Limit of 1
    // for every mode (ADR-0051), so any configured `parallelism` yields
    // effective parallelism 1 — the min of the two (ADR-0052) — and the
    // report echoes both facts (ADR-0053) around otherwise unchanged
    // execution content, with `report_version` still 1.
    for (destination_connector, load_mode) in [
        ("parquet", "full_refresh"),
        ("parquet", "append"),
        ("duckdb", "full_refresh"),
        ("duckdb", "append"),
    ] {
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.csv");
        fs::write(&source_path, "customer_id,name\n1,Ada\n").expect("write source csv");
        let destination_path = match destination_connector {
            "parquet" => work.path().join("customers_dataset"),
            _ => work.path().join("customers.duckdb"),
        };
        // A DuckDB append needs the destination table to exist.
        if destination_connector == "duckdb" && load_mode == "append" {
            let connection =
                duckdb::Connection::open(&destination_path).expect("open DuckDB destination");
            connection
                .execute_batch("CREATE TABLE customers (customer_id BIGINT, name VARCHAR)")
                .expect("seed DuckDB destination");
        }
        let definition_path = work.path().join("load.yml");
        write_retry_definition(
            &definition_path,
            &source_path,
            destination_connector,
            &destination_path,
            load_mode,
            "execution:\n  parallelism: 8\n",
        );

        let load = run_cli_load(
            work.path(),
            &work.path().join("artifacts"),
            &definition_path,
            true,
        );
        let report = &load.report;

        assert_eq!(
            report["exit_status"], "succeeded",
            "{destination_connector}/{load_mode}"
        );
        assert_eq!(report["report_version"], 1);
        assert_eq!(
            report["execution"],
            serde_json::json!({
                "record_format": "arrow_record_batch",
                "batch_count": 1,
                "chunk_rows": 65536,
                "parallelism": 1,
                "connector_parallelism_limit": 1,
                "retry": default_retry_echo()
            }),
            "{destination_connector}/{load_mode}"
        );
    }
}

#[test]
fn parallelism_echoes_the_serial_default_and_the_explicit_serial_form_alike() {
    // Absent — the block or the key — the effective parallelism is the
    // connector's declared limit (ADR-0052), and `parallelism: 1` is the
    // explicit serial form: every variant echoes 1/1 in the report.
    for execution_block in ["", "execution: {}\n", "execution:\n  parallelism: 1\n"] {
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.csv");
        fs::write(&source_path, "customer_id\n1\n").expect("write source csv");
        let destination_path = work.path().join("customers_dataset");
        let definition_path = work.path().join("load.yml");
        write_retry_definition(
            &definition_path,
            &source_path,
            "parquet",
            &destination_path,
            "full_refresh",
            execution_block,
        );

        let load = run_cli_load(
            work.path(),
            &work.path().join("artifacts"),
            &definition_path,
            true,
        );

        assert_eq!(
            load.report["execution"]["parallelism"], 1,
            "{execution_block:?}"
        );
        assert_eq!(
            load.report["execution"]["connector_parallelism_limit"], 1,
            "{execution_block:?}"
        );
    }
}

#[test]
fn invalid_parallelism_values_fail_before_source_or_destination_work() {
    // `execution.parallelism` is part of the strict contract (ADR-0052):
    // zero, negative, and non-integer values fail YAML parsing under the
    // existing definition failure code — no new failure code exists — and
    // an unknown key beside a valid `parallelism` is still rejected
    // recursively (ADR-0037), all before any source read or destination
    // write.
    for (execution_block, expected_message_part) in [
        ("execution:\n  parallelism: 0\n", "expected a nonzero u64"),
        ("execution:\n  parallelism: -2\n", "expected a nonzero u64"),
        ("execution:\n  parallelism: 1.5\n", "parallelism"),
        (
            "execution:\n  parallelism: 2\n  workers: 4\n",
            "unknown field `workers`",
        ),
    ] {
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.csv");
        fs::write(&source_path, "customer_id\n1\n").expect("write source csv");
        let destination_path = work.path().join("customers_dataset");
        let definition_path = work.path().join("load.yml");
        write_retry_definition(
            &definition_path,
            &source_path,
            "parquet",
            &destination_path,
            "full_refresh",
            execution_block,
        );

        let load = run_cli_load(
            work.path(),
            &work.path().join("artifacts"),
            &definition_path,
            false,
        );
        let report = &load.report;

        assert_eq!(
            report["error_summary"]["code"], "invalid_load_definition_yaml",
            "{execution_block:?}"
        );
        let message = report["error_summary"]["message"]
            .as_str()
            .expect("error message");
        assert!(
            message.contains(expected_message_part),
            "message {message:?} misses {expected_message_part:?}"
        );
        assert_eq!(
            report["execution"],
            serde_json::json!({
                "record_format": "not_started",
                "batch_count": 0
            }),
            "an invalid parallelism keeps the exact not_started posture"
        );
        assert!(
            !destination_path.exists(),
            "an invalid parallelism value must not reach the destination"
        );
    }
}

// ---- Merge loads (ADR-0057, ADR-0058, ADR-0059) ----

/// Writes a merge-shaped load definition: the standard blocks plus a raw
/// trailing block for `merge:`, `transform:`, or `reject_threshold:` lines.
fn write_merge_definition(
    definition_path: &Path,
    source_path: &Path,
    source_format: &str,
    destination_connector: &str,
    destination_path: &Path,
    load_mode: &str,
    extra_blocks: &str,
) {
    fs::write(
        definition_path,
        format!(
            "version: 1\n\
             source:\n\
             \x20 connector: local_file\n\
             \x20 path: {}\n\
             \x20 format: {source_format}\n\
             destination:\n\
             \x20 connector: {destination_connector}\n\
             \x20 path: {}\n\
             dataset: customers\n\
             load_mode: {load_mode}\n\
             {extra_blocks}",
            source_path.display(),
            destination_path.display(),
        ),
    )
    .expect("write load definition");
}

#[test]
fn local_csv_merge_updates_matching_records_and_inserts_new_ones_into_duckdb() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    let database_path = work.path().join("customers.duckdb");
    let definition_path = work.path().join("load.yml");

    // Day 1 bootstraps the destination table: merge never auto-creates.
    fs::write(
        &source_path,
        "customer_id,name,tier\n1,Ada,gold\n2,Grace,silver\n",
    )
    .expect("write bootstrap csv");
    write_merge_definition(
        &definition_path,
        &source_path,
        "csv",
        "duckdb",
        &database_path,
        "full_refresh",
        "",
    );
    let bootstrap = run_cli_load(
        work.path(),
        &work.path().join("artifacts-day-1"),
        &definition_path,
        true,
    );
    assert!(
        bootstrap.report["merge"].is_null(),
        "a non-merge load echoes a null merge block"
    );

    // Day 2 merges one matching key (replaced whole: every non-key column
    // takes the source value) and one new key.
    fs::write(
        &source_path,
        "customer_id,name,tier\n2,Grace Hopper,gold\n3,Katherine,bronze\n",
    )
    .expect("write merge csv");
    write_merge_definition(
        &definition_path,
        &source_path,
        "csv",
        "duckdb",
        &database_path,
        "merge",
        "merge:\n  keys: [customer_id]\n",
    );
    let merge = run_cli_load(
        work.path(),
        &work.path().join("artifacts-day-2"),
        &definition_path,
        true,
    );
    let report = &merge.report;

    assert_eq!(report["report_version"], 1);
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(report["process_exit_code"], 0);
    assert_eq!(report["load_mode"], "merge");
    assert_eq!(
        report["merge"],
        serde_json::json!({ "keys": ["customer_id"] })
    );
    assert_eq!(report["row_counts"]["source"], 2);
    assert_eq!(report["row_counts"]["written"], 2);
    assert_eq!(report["row_counts"]["rejected"], 0);
    assert_eq!(
        report["destination_write"],
        serde_json::json!({
            "atomicity": "atomic",
            "strategy": "transactional_merge",
            "merge": { "updated": 1, "inserted": 1 }
        })
    );
    let updated = report["destination_write"]["merge"]["updated"]
        .as_u64()
        .expect("updated count");
    let inserted = report["destination_write"]["merge"]["inserted"]
        .as_u64()
        .expect("inserted count");
    assert_eq!(
        updated + inserted,
        report["row_counts"]["written"].as_u64().expect("written"),
        "updated + inserted == row_counts.written"
    );
    assert!(report["byte_counts"]["destination"].is_null());
    assert!(merge.stdout.contains("Load mode: merge"));

    // The matched record was replaced whole, the new key inserted, and the
    // untouched record kept every value.
    let batch = read_single_duckdb_batch(&database_path, "customers");
    assert_eq!(batch.num_rows(), 3);
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("customer_id is int64");
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name is string");
    let tiers = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("tier is string");
    assert_eq!(ids.values(), &[1, 2, 3]);
    assert_eq!(names.value(0), "Ada");
    assert_eq!(tiers.value(0), "gold");
    assert_eq!(names.value(1), "Grace Hopper");
    assert_eq!(tiers.value(1), "gold");
    assert_eq!(names.value(2), "Katherine");
    assert_eq!(tiers.value(2), "bronze");
}

#[test]
fn local_jsonl_merge_matches_on_renamed_and_flattened_multi_field_keys() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.jsonl");
    let database_path = work.path().join("customers.duckdb");
    let definition_path = work.path().join("load.yml");
    let transform_block = "transform:\n\
         \x20 flatten:\n\
         \x20   region.code: region_code\n\
         \x20 select: [id, region_code, name]\n\
         \x20 rename:\n\
         \x20   id: customer_id\n";

    fs::write(
        &source_path,
        concat!(
            "{\"id\": 1, \"region\": {\"code\": \"eu\"}, \"name\": \"Ada\"}\n",
            "{\"id\": 2, \"region\": {\"code\": \"us\"}, \"name\": \"Grace\"}\n",
        ),
    )
    .expect("write bootstrap jsonl");
    write_merge_definition(
        &definition_path,
        &source_path,
        "jsonl",
        "duckdb",
        &database_path,
        "full_refresh",
        transform_block,
    );
    run_cli_load(
        work.path(),
        &work.path().join("artifacts-day-1"),
        &definition_path,
        true,
    );

    // The key tuple is a rename target plus a flatten output: (2, us)
    // matches an existing record while (2, eu) is a new tuple, so the same
    // customer_id both updates and inserts under the two-field key.
    fs::write(
        &source_path,
        concat!(
            "{\"id\": 2, \"region\": {\"code\": \"us\"}, \"name\": \"Grace Hopper\"}\n",
            "{\"id\": 2, \"region\": {\"code\": \"eu\"}, \"name\": \"Grace Brewster\"}\n",
        ),
    )
    .expect("write merge jsonl");
    write_merge_definition(
        &definition_path,
        &source_path,
        "jsonl",
        "duckdb",
        &database_path,
        "merge",
        &format!("{transform_block}merge:\n  keys: [customer_id, region_code]\n"),
    );
    let merge = run_cli_load(
        work.path(),
        &work.path().join("artifacts-day-2"),
        &definition_path,
        true,
    );
    let report = &merge.report;

    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(
        report["merge"],
        serde_json::json!({ "keys": ["customer_id", "region_code"] })
    );
    assert_eq!(
        report["destination_write"]["merge"],
        serde_json::json!({ "updated": 1, "inserted": 1 })
    );
    assert_eq!(report["row_counts"]["written"], 2);

    let batch = read_single_duckdb_batch(&database_path, "customers");
    assert_eq!(batch.num_rows(), 3);
    let mut records = Vec::new();
    for index in 0..batch.num_rows() {
        records.push((
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("customer_id is int64")
                .value(index),
            batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("region_code is string")
                .value(index)
                .to_string(),
            batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("name is string")
                .value(index)
                .to_string(),
        ));
    }
    records.sort();
    assert_eq!(
        records,
        vec![
            (1, "eu".to_string(), "Ada".to_string()),
            (2, "eu".to_string(), "Grace Brewster".to_string()),
            (2, "us".to_string(), "Grace Hopper".to_string()),
        ]
    );
}

#[test]
fn merge_key_nulls_reject_records_against_the_reject_threshold() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    let database_path = work.path().join("customers.duckdb");
    let definition_path = work.path().join("load.yml");

    fs::write(&source_path, "customer_id,name\n1,Ada\n").expect("write bootstrap csv");
    write_merge_definition(
        &definition_path,
        &source_path,
        "csv",
        "duckdb",
        &database_path,
        "full_refresh",
        "",
    );
    run_cli_load(
        work.path(),
        &work.path().join("artifacts-day-1"),
        &definition_path,
        true,
    );

    // A CSV empty cell is the pipeline's existing null: under the default
    // threshold of 0 the single null-key record fails the whole load before
    // any destination write.
    fs::write(&source_path, "customer_id,name\n2,Grace\n,Katherine\n").expect("write null-key csv");
    write_merge_definition(
        &definition_path,
        &source_path,
        "csv",
        "duckdb",
        &database_path,
        "merge",
        "merge:\n  keys: [customer_id]\n",
    );
    let failed = run_cli_load(
        work.path(),
        &work.path().join("artifacts-default"),
        &definition_path,
        false,
    );
    let report = &failed.report;
    assert_eq!(report["error_summary"]["code"], "reject_threshold_exceeded");
    assert_eq!(report["row_counts"]["rejected"], 1);
    assert_eq!(report["row_counts"]["written"], 0);
    assert_eq!(report["destination_write"]["atomicity"], "not_applicable");
    let rejections = read_rejected_records(&PathBuf::from(
        report["rejected_records"]["artifact"]
            .as_str()
            .expect("rejected artifact path"),
    ));
    assert_eq!(rejections.len(), 1);
    assert_eq!(rejections[0]["code"], "null_merge_key");
    assert_eq!(rejections[0]["field"], "customer_id");
    assert_eq!(
        rejections[0]["message"],
        "merge key \"customer_id\" is null"
    );
    let batch = read_single_duckdb_batch(&database_path, "customers");
    assert_eq!(batch.num_rows(), 1, "the failed load wrote nothing");

    // An explicit threshold admits the rejection: the survivors merge and
    // the counts exclude the rejected record.
    write_merge_definition(
        &definition_path,
        &source_path,
        "csv",
        "duckdb",
        &database_path,
        "merge",
        "merge:\n  keys: [customer_id]\nreject_threshold: 1\n",
    );
    let merged = run_cli_load(
        work.path(),
        &work.path().join("artifacts-threshold"),
        &definition_path,
        true,
    );
    assert_eq!(merged.report["row_counts"]["rejected"], 1);
    assert_eq!(merged.report["row_counts"]["written"], 1);
    assert_eq!(
        merged.report["destination_write"]["merge"],
        serde_json::json!({ "updated": 0, "inserted": 1 })
    );
    let batch = read_single_duckdb_batch(&database_path, "customers");
    assert_eq!(batch.num_rows(), 2);
}

#[test]
fn jsonl_absent_key_fields_reject_as_null_merge_keys() {
    // A JSONL record with the key field absent — or explicitly null — is
    // the existing null definition, so it rejects as `null_merge_key`
    // exactly like a null value.
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.jsonl");
    let database_path = work.path().join("customers.duckdb");
    let definition_path = work.path().join("load.yml");

    fs::write(&source_path, "{\"customer_id\": 1, \"name\": \"Ada\"}\n")
        .expect("write bootstrap jsonl");
    write_merge_definition(
        &definition_path,
        &source_path,
        "jsonl",
        "duckdb",
        &database_path,
        "full_refresh",
        "",
    );
    run_cli_load(
        work.path(),
        &work.path().join("artifacts-day-1"),
        &definition_path,
        true,
    );

    fs::write(
        &source_path,
        concat!(
            "{\"customer_id\": 2, \"name\": \"Grace\"}\n",
            "{\"name\": \"Absent\"}\n",
            "{\"customer_id\": null, \"name\": \"Null\"}\n",
        ),
    )
    .expect("write merge jsonl");
    write_merge_definition(
        &definition_path,
        &source_path,
        "jsonl",
        "duckdb",
        &database_path,
        "merge",
        "merge:\n  keys: [customer_id]\nreject_threshold: 2\n",
    );
    let merged = run_cli_load(
        work.path(),
        &work.path().join("artifacts-day-2"),
        &definition_path,
        true,
    );

    assert_eq!(merged.report["row_counts"]["rejected"], 2);
    assert_eq!(merged.report["row_counts"]["written"], 1);
    let rejections = read_rejected_records(&PathBuf::from(
        merged.report["rejected_records"]["artifact"]
            .as_str()
            .expect("rejected artifact path"),
    ));
    assert_eq!(rejections.len(), 2);
    for rejection in &rejections {
        assert_eq!(rejection["code"], "null_merge_key");
    }
    let batch = read_single_duckdb_batch(&database_path, "customers");
    assert_eq!(batch.num_rows(), 2);
}

#[test]
fn merge_cross_validation_failures_keep_the_pure_pre_write_posture() {
    for (load_mode, merge_block, expected_code, expected_message) in [
        (
            "merge",
            "",
            "missing_merge_keys",
            "load definition merge.keys is required when load_mode is merge",
        ),
        (
            "append",
            "merge:\n  keys: [customer_id]\n",
            "invalid_merge_config",
            "a merge block requires load_mode: merge",
        ),
        (
            "merge",
            "merge:\n  keys: []\n",
            "invalid_merge_config",
            "merge.keys must name at least one dataset field",
        ),
        (
            "merge",
            "merge:\n  keys: [customer_id, customer_id]\n",
            "invalid_merge_config",
            "merge.keys names field \"customer_id\" more than once",
        ),
    ] {
        let work = TempDir::new().expect("tempdir");
        let source_path = work.path().join("customers.csv");
        let database_path = work.path().join("customers.duckdb");
        let definition_path = work.path().join("load.yml");
        fs::write(&source_path, "customer_id,name\n1,Ada\n").expect("write source csv");
        write_merge_definition(
            &definition_path,
            &source_path,
            "csv",
            "duckdb",
            &database_path,
            load_mode,
            merge_block,
        );

        let load = run_cli_load(
            work.path(),
            &work.path().join("artifacts"),
            &definition_path,
            false,
        );
        let report = &load.report;

        assert_eq!(
            report["error_summary"]["code"], expected_code,
            "{merge_block:?}"
        );
        assert_eq!(
            report["error_summary"]["message"], expected_message,
            "{merge_block:?}"
        );
        assert_eq!(report["exit_status"], "failed", "{merge_block:?}");
        assert_eq!(report["process_exit_code"], 1, "{merge_block:?}");
        assert_eq!(
            report["schema_decision"],
            serde_json::json!({ "mode": "not_evaluated" }),
            "{merge_block:?}"
        );
        assert_eq!(
            report["execution"],
            serde_json::json!({
                "record_format": "not_started",
                "batch_count": 0
            }),
            "{merge_block:?}"
        );
        assert_eq!(report["row_counts"]["source"], 0, "{merge_block:?}");
        assert_eq!(report["row_counts"]["written"], 0, "{merge_block:?}");
        assert_eq!(report["row_counts"]["rejected"], 0, "{merge_block:?}");
        assert_eq!(
            report["destination_write"]["atomicity"], "not_applicable",
            "{merge_block:?}"
        );
        assert!(
            !database_path.exists(),
            "a config failure must not touch the destination: {merge_block:?}"
        );
    }
}

#[test]
fn merge_echoes_the_declared_block_even_when_cross_validation_rejects_it() {
    // The report echoes the merge block like it echoes any load mode string:
    // as declared, even when the declaration is the failure.
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    let definition_path = work.path().join("load.yml");
    fs::write(&source_path, "customer_id\n1\n").expect("write source csv");
    write_merge_definition(
        &definition_path,
        &source_path,
        "csv",
        "duckdb",
        &work.path().join("customers.duckdb"),
        "append",
        "merge:\n  keys: [customer_id]\n",
    );

    let load = run_cli_load(
        work.path(),
        &work.path().join("artifacts"),
        &definition_path,
        false,
    );

    assert_eq!(load.report["error_summary"]["code"], "invalid_merge_config");
    assert_eq!(
        load.report["merge"],
        serde_json::json!({ "keys": ["customer_id"] })
    );
}

#[test]
fn parquet_declines_merge_and_unknown_modes_stay_unsupported_load_mode() {
    // A parquet merge fails before any I/O: the missing source proves no
    // source read happened, and the failure names the destination and its
    // supported modes (`unsupported_load_mode_for_destination`).
    let work = TempDir::new().expect("tempdir");
    let definition_path = work.path().join("load.yml");
    let destination_path = work.path().join("customers_dataset");
    write_merge_definition(
        &definition_path,
        &work.path().join("missing.csv"),
        "csv",
        "parquet",
        &destination_path,
        "merge",
        "merge:\n  keys: [customer_id]\n",
    );
    let declined = run_cli_load(
        work.path(),
        &work.path().join("artifacts-parquet"),
        &definition_path,
        false,
    );
    assert_eq!(
        declined.report["error_summary"]["code"],
        "unsupported_load_mode_for_destination"
    );
    assert_eq!(
        declined.report["error_summary"]["message"],
        "parquet destination does not support load mode: merge \
         (supported load modes: full_refresh, append)"
    );
    assert_eq!(
        declined.report["schema_decision"],
        serde_json::json!({ "mode": "not_evaluated" })
    );
    assert!(!destination_path.exists());

    // `unsupported_load_mode` now means exactly "the mode string is
    // unknown".
    write_merge_definition(
        &definition_path,
        &work.path().join("missing.csv"),
        "csv",
        "parquet",
        &destination_path,
        "sideload",
        "",
    );
    let unknown = run_cli_load(
        work.path(),
        &work.path().join("artifacts-unknown"),
        &definition_path,
        false,
    );
    assert_eq!(
        unknown.report["error_summary"]["code"],
        "unsupported_load_mode"
    );
    assert_eq!(
        unknown.report["error_summary"]["message"],
        "unsupported load mode: sideload"
    );
}

#[test]
fn unknown_merge_key_fields_fail_before_any_destination_write() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    let database_path = work.path().join("customers.duckdb");
    let definition_path = work.path().join("load.yml");

    fs::write(&source_path, "customer_id,name\n1,Ada\n").expect("write bootstrap csv");
    write_merge_definition(
        &definition_path,
        &source_path,
        "csv",
        "duckdb",
        &database_path,
        "full_refresh",
        "",
    );
    run_cli_load(
        work.path(),
        &work.path().join("artifacts-day-1"),
        &definition_path,
        true,
    );

    // A key naming a renamed-away source name is not a dataset field: keys
    // speak dataset names, after the transform.
    write_merge_definition(
        &definition_path,
        &source_path,
        "csv",
        "duckdb",
        &database_path,
        "merge",
        "transform:\n  rename:\n    customer_id: id\nmerge:\n  keys: [customer_id]\n",
    );
    let failed = run_cli_load(
        work.path(),
        &work.path().join("artifacts-unknown-key"),
        &definition_path,
        false,
    );
    let report = &failed.report;
    assert_eq!(report["error_summary"]["code"], "unknown_merge_key_field");
    assert_eq!(
        report["error_summary"]["message"],
        "merge keys name fields absent from the resolved dataset schema: customer_id"
    );
    assert_eq!(report["schema_decision"]["mode"], "inferred");
    assert_eq!(report["row_counts"]["source"], 0);
    assert_eq!(report["row_counts"]["rejected"], 0);
    assert_eq!(report["destination_write"]["atomicity"], "not_applicable");
    assert_eq!(
        report["execution"],
        serde_json::json!({
            "record_format": "not_started",
            "batch_count": 0
        })
    );
    let batch = read_single_duckdb_batch(&database_path, "customers");
    assert_eq!(batch.num_rows(), 1, "the destination stayed untouched");

    // For JSONL the dataset shape is the union of the batch's record keys, so
    // a batch-wide-absent key field is an unknown field — never a batch of
    // null-key rejections.
    let jsonl_path = work.path().join("customers.jsonl");
    fs::write(&jsonl_path, "{\"name\": \"Ada\"}\n").expect("write jsonl");
    write_merge_definition(
        &definition_path,
        &jsonl_path,
        "jsonl",
        "duckdb",
        &database_path,
        "merge",
        "merge:\n  keys: [customer_id]\n",
    );
    let absent = run_cli_load(
        work.path(),
        &work.path().join("artifacts-absent-key"),
        &definition_path,
        false,
    );
    assert_eq!(
        absent.report["error_summary"]["code"],
        "unknown_merge_key_field"
    );
    assert_eq!(absent.report["row_counts"]["rejected"], 0);
    assert!(absent.report["rejected_records"]["artifact"].is_null());
}

#[test]
fn merge_into_a_missing_duckdb_table_keeps_the_not_started_posture() {
    // Merge never auto-creates its destination table: a missing table fails
    // opening the session, mirroring the append probe — same code, "before
    // merge" wording, and the exact pre-write posture.
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    let database_path = work.path().join("customers.duckdb");
    let definition_path = work.path().join("load.yml");
    fs::write(&source_path, "customer_id\n1\n").expect("write source csv");
    write_merge_definition(
        &definition_path,
        &source_path,
        "csv",
        "duckdb",
        &database_path,
        "merge",
        "merge:\n  keys: [customer_id]\n",
    );

    let load = run_cli_load(
        work.path(),
        &work.path().join("artifacts"),
        &definition_path,
        false,
    );
    let report = &load.report;

    assert_eq!(report["error_summary"]["code"], "destination_write_failed");
    assert!(report["error_summary"]["message"]
        .as_str()
        .expect("error message")
        .contains("before merge"));
    assert_eq!(
        report["execution"],
        serde_json::json!({
            "record_format": "not_started",
            "batch_count": 0
        })
    );
    assert_eq!(report["destination_write"]["atomicity"], "not_applicable");
}

#[test]
fn a_zero_survivor_merge_commits_a_no_op_and_exits_zero() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("readings.csv");
    let database_path = work.path().join("readings.duckdb");
    let definition_path = work.path().join("load.yml");

    // Text-typed columns keep the header-only day aligned with the table.
    fs::write(&source_path, "station,city\nS-1,Berlin\n").expect("write bootstrap csv");
    fs::write(
        &definition_path,
        format!(
            "version: 1\n\
             source:\n\
             \x20 connector: local_file\n\
             \x20 path: {}\n\
             \x20 format: csv\n\
             destination:\n\
             \x20 connector: duckdb\n\
             \x20 path: {}\n\
             dataset: readings\n\
             load_mode: full_refresh\n",
            source_path.display(),
            database_path.display(),
        ),
    )
    .expect("write bootstrap definition");
    run_cli_load(
        work.path(),
        &work.path().join("artifacts-day-1"),
        &definition_path,
        true,
    );

    // A header-only day has zero survivors: the stage stays empty, the merge
    // is a no-op, and the terminal commit still completes the load.
    fs::write(&source_path, "station,city\n").expect("write empty csv");
    fs::write(
        &definition_path,
        format!(
            "version: 1\n\
             source:\n\
             \x20 connector: local_file\n\
             \x20 path: {}\n\
             \x20 format: csv\n\
             destination:\n\
             \x20 connector: duckdb\n\
             \x20 path: {}\n\
             dataset: readings\n\
             load_mode: merge\n\
             merge:\n\
             \x20 keys: [station]\n",
            source_path.display(),
            database_path.display(),
        ),
    )
    .expect("write merge definition");
    let merge = run_cli_load(
        work.path(),
        &work.path().join("artifacts-day-2"),
        &definition_path,
        true,
    );

    assert_eq!(merge.report["exit_status"], "succeeded");
    assert_eq!(merge.report["row_counts"]["source"], 0);
    assert_eq!(merge.report["row_counts"]["written"], 0);
    assert_eq!(
        merge.report["destination_write"],
        serde_json::json!({
            "atomicity": "atomic",
            "strategy": "transactional_merge",
            "merge": { "updated": 0, "inserted": 0 }
        })
    );
    let batch = read_single_duckdb_batch(&database_path, "readings");
    assert_eq!(batch.num_rows(), 1, "the destination is unchanged");
}

#[test]
fn a_multi_chunk_merge_stages_every_chunk_and_commits_once() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    let database_path = work.path().join("customers.duckdb");
    let definition_path = work.path().join("load.yml");

    fs::write(&source_path, "customer_id,name\n1,Ada\n2,Grace\n").expect("write bootstrap csv");
    write_merge_definition(
        &definition_path,
        &source_path,
        "csv",
        "duckdb",
        &database_path,
        "full_refresh",
        "",
    );
    run_cli_load(
        work.path(),
        &work.path().join("artifacts-day-1"),
        &definition_path,
        true,
    );

    // Three records under a chunk bound of one stage as three chunks inside
    // the same transaction and commit together at the terminal commit.
    fs::write(
        &source_path,
        "customer_id,name\n2,Grace Hopper\n3,Katherine\n4,Edsger\n",
    )
    .expect("write merge csv");
    write_merge_definition(
        &definition_path,
        &source_path,
        "csv",
        "duckdb",
        &database_path,
        "merge",
        "merge:\n  keys: [customer_id]\nexecution:\n  chunk_rows: 1\n",
    );
    let merge = run_cli_load(
        work.path(),
        &work.path().join("artifacts-day-2"),
        &definition_path,
        true,
    );

    assert_eq!(merge.report["execution"]["batch_count"], 3);
    assert_eq!(merge.report["execution"]["chunk_rows"], 1);
    assert_eq!(
        merge.report["destination_write"]["merge"],
        serde_json::json!({ "updated": 1, "inserted": 2 })
    );
    assert_eq!(merge.report["row_counts"]["written"], 3);
    let batch = read_single_duckdb_batch(&database_path, "customers");
    assert_eq!(batch.num_rows(), 4);
}

#[test]
fn duplicate_merge_keys_fail_the_load_and_leave_the_destination_byte_identical() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    let database_path = work.path().join("customers.duckdb");
    let definition_path = work.path().join("load.yml");

    fs::write(&source_path, "customer_id,name\n1,Ada\n").expect("write bootstrap csv");
    write_merge_definition(
        &definition_path,
        &source_path,
        "csv",
        "duckdb",
        &database_path,
        "full_refresh",
        "",
    );
    run_cli_load(
        work.path(),
        &work.path().join("artifacts-day-1"),
        &definition_path,
        true,
    );
    let destination_before = fs::read(&database_path).expect("read destination bytes");

    // Two surviving records share one key tuple: never first/last-wins —
    // the load fails and the transaction rolls back before the real table
    // was touched.
    fs::write(
        &source_path,
        "customer_id,name\n7,First\n7,Second\n8,Other\n",
    )
    .expect("write duplicate-key csv");
    write_merge_definition(
        &definition_path,
        &source_path,
        "csv",
        "duckdb",
        &database_path,
        "merge",
        "merge:\n  keys: [customer_id]\n",
    );
    let failed = run_cli_load(
        work.path(),
        &work.path().join("artifacts-day-2"),
        &definition_path,
        false,
    );
    let report = &failed.report;

    assert_eq!(report["error_summary"]["code"], "duplicate_merge_keys");
    assert_eq!(
        report["error_summary"]["message"],
        format!(
            "2 surviving records share a merge key tuple with another surviving \
             record for DuckDB table customers in {}",
            database_path.display()
        )
    );
    assert_eq!(report["exit_status"], "failed");
    assert_eq!(report["row_counts"]["written"], 0);
    assert_eq!(report["destination_write"]["atomicity"], "atomic");
    assert_eq!(
        report["destination_write"]["strategy"],
        "transactional_merge"
    );
    assert!(
        report["destination_write"]["merge"].is_null(),
        "a failed merge reports no merge counts"
    );
    assert_eq!(report["execution"]["record_format"], "arrow_record_batch");
    assert_eq!(report["execution"]["batch_count"], 0);

    let destination_after = fs::read(&database_path).expect("read destination bytes");
    assert_eq!(
        destination_before, destination_after,
        "the rolled-back merge left the destination byte-identical"
    );
}

#[test]
fn a_mid_load_merge_failure_leaves_the_destination_byte_identical() {
    let work = TempDir::new().expect("tempdir");
    let source_path = work.path().join("customers.csv");
    let database_path = work.path().join("customers.duckdb");
    let definition_path = work.path().join("load.yml");

    fs::write(&source_path, "customer_id,name\n1,Ada\n").expect("write bootstrap csv");
    write_merge_definition(
        &definition_path,
        &source_path,
        "csv",
        "duckdb",
        &database_path,
        "full_refresh",
        "",
    );
    run_cli_load(
        work.path(),
        &work.path().join("artifacts-day-1"),
        &definition_path,
        true,
    );
    let destination_before = fs::read(&database_path).expect("read destination bytes");

    // An added source column against the unmigrated table fails chunk
    // alignment exactly like append — but under merge's terminal commit the
    // failure commits nothing.
    fs::write(&source_path, "customer_id,name,tier\n2,Grace,gold\n").expect("write drifted csv");
    write_merge_definition(
        &definition_path,
        &source_path,
        "csv",
        "duckdb",
        &database_path,
        "merge",
        "merge:\n  keys: [customer_id]\n",
    );
    let failed = run_cli_load(
        work.path(),
        &work.path().join("artifacts-day-2"),
        &definition_path,
        false,
    );
    let report = &failed.report;

    assert_eq!(report["error_summary"]["code"], "destination_write_failed");
    assert!(report["error_summary"]["message"]
        .as_str()
        .expect("error message")
        .contains("merge schema does not match DuckDB destination"));
    assert_eq!(report["row_counts"]["written"], 0);
    assert_eq!(report["destination_write"]["atomicity"], "atomic");
    assert_eq!(
        report["destination_write"]["strategy"],
        "transactional_merge"
    );

    let destination_after = fs::read(&database_path).expect("read destination bytes");
    assert_eq!(
        destination_before, destination_after,
        "the failed merge left the destination byte-identical"
    );
}

fn write_load_definition(
    definition_path: &Path,
    source_path: &Path,
    source_format: &str,
    destination_connector: &str,
    destination_path: &Path,
    load_mode: &str,
    pinned_path: Option<&Path>,
) {
    let schema_block = pinned_path
        .map(|path| format!("schema:\n  pinned_path: {}\n", path.display()))
        .unwrap_or_default();
    fs::write(
        definition_path,
        format!(
            "version: 1\n\
             source:\n\
             \x20 connector: local_file\n\
             \x20 path: {}\n\
             \x20 format: {source_format}\n\
             destination:\n\
             \x20 connector: {destination_connector}\n\
             \x20 path: {}\n\
             dataset: customers\n\
             load_mode: {load_mode}\n\
             {schema_block}",
            source_path.display(),
            destination_path.display(),
        ),
    )
    .expect("write load definition");
}

fn run_cli_load(
    work_dir: &Path,
    artifacts_dir: &Path,
    definition_path: &Path,
    expect_success: bool,
) -> CliLoadResult {
    let assert = Command::cargo_bin("data-spark")
        .expect("binary")
        .current_dir(work_dir)
        .arg("load")
        .arg("--output-dir")
        .arg(artifacts_dir)
        .arg(definition_path)
        .assert();
    let assert = if expect_success {
        assert.success()
    } else {
        assert.failure()
    };
    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let (report_path, report) =
        read_single_report(artifacts_dir, "load writes one artifact directory");
    CliLoadResult {
        stdout,
        report_path,
        report,
    }
}

fn assert_successful_append(result: &CliLoadResult, expected_strategy: &str) {
    let report = &result.report;
    assert_eq!(report["report_version"], 1);
    assert_eq!(report["load_mode"], "append");
    assert_eq!(report["exit_status"], "succeeded");
    assert_eq!(report["process_exit_code"], 0);
    assert_eq!(report["row_counts"]["source"], 1);
    assert_eq!(report["row_counts"]["written"], 1);
    assert_eq!(report["row_counts"]["rejected"], 0);
    assert_eq!(report["rejected_records"]["count"], 0);
    assert!(report["rejected_records"]["artifact"].is_null());
    assert_eq!(report["destination_write"]["atomicity"], "best_effort");
    assert_eq!(report["destination_write"]["strategy"], expected_strategy);
    assert!(report["error_summary"].is_null());
    assert!(result.stdout.contains("Status: succeeded"));
    assert!(result.stdout.contains("Records read: 1"));
    assert!(result.stdout.contains("Records written: 1"));
    assert!(result.stdout.contains("Records rejected: 0"));
    assert!(result
        .stdout
        .contains(result.report_path.to_str().expect("report path")));
}

fn parquet_files(destination_path: &Path) -> Vec<PathBuf> {
    let mut files = fs::read_dir(destination_path)
        .expect("parquet destination directory")
        .map(|entry| entry.expect("destination entry").path())
        .filter(|path| path.extension().and_then(|extension| extension.to_str()) == Some("parquet"))
        .collect::<Vec<_>>();
    files.sort();
    files
}

fn read_parquet_file_batches(parquet_path: &Path) -> Vec<arrow_array::RecordBatch> {
    let file = File::open(parquet_path).expect("open parquet file");
    ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("parquet reader")
        .build()
        .expect("build parquet reader")
        .map(|batch| batch.expect("read parquet batch"))
        .collect()
}

fn read_parquet_batches(destination_path: &Path) -> Vec<arrow_array::RecordBatch> {
    parquet_files(destination_path)
        .into_iter()
        .flat_map(|path| read_parquet_file_batches(&path))
        .collect()
}

fn id_name_records(batches: &[arrow_array::RecordBatch]) -> Vec<(i64, String)> {
    let mut records = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("customer_id is int64");
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name is string");
        for index in 0..batch.num_rows() {
            records.push((ids.value(index), names.value(index).to_string()));
        }
    }
    records.sort_by_key(|(id, _)| *id);
    records
}

fn read_single_report(artifacts_dir: &Path, artifact_count_message: &str) -> (PathBuf, Value) {
    let run_dirs = fs::read_dir(artifacts_dir)
        .expect("artifact root")
        .collect::<Result<Vec<_>, _>>()
        .expect("artifact entries");
    assert_eq!(run_dirs.len(), 1, "{artifact_count_message}");

    let report_path = run_dirs[0].path().join("load-report.json");
    let report =
        serde_json::from_slice(&fs::read(&report_path).expect("load report")).expect("json report");
    (report_path, report)
}

fn read_rejected_records(artifact_path: &Path) -> Vec<Value> {
    fs::read_to_string(artifact_path)
        .expect("rejected-records artifact")
        .lines()
        .map(|line| serde_json::from_str(line).expect("artifact line is json"))
        .collect()
}

fn single_parquet_file(destination_path: &Path) -> PathBuf {
    let files = parquet_files(destination_path);
    assert_eq!(files.len(), 1, "one parquet data file");
    files[0].clone()
}

/// The `(column_name, data_type)` rows DuckDB's own catalog states for a
/// table, in column order — the destination-side truth about stored types.
fn duckdb_column_types(database_path: &Path, dataset: &str) -> Vec<(String, String)> {
    let connection = duckdb::Connection::open(database_path).expect("open duckdb database");
    let mut statement = connection
        .prepare(
            "SELECT column_name, data_type FROM information_schema.columns \
             WHERE table_name = ? ORDER BY ordinal_position",
        )
        .expect("prepare duckdb catalog query");
    let column_types = statement
        .query_map([dataset], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .expect("query duckdb catalog")
        .collect::<Result<Vec<_>, _>>()
        .expect("read duckdb catalog rows");
    column_types
}

fn read_single_duckdb_batch(database_path: &Path, dataset: &str) -> arrow_array::RecordBatch {
    let connection = duckdb::Connection::open(database_path).expect("open duckdb database");
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

fn read_single_parquet_batch(parquet_path: &Path) -> arrow_array::RecordBatch {
    let mut batches = read_parquet_file_batches(parquet_path);
    assert_eq!(batches.len(), 1, "one parquet batch expected");
    batches.pop().expect("one parquet batch")
}
