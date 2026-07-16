use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use assert_cmd::Command;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value;
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

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
            "overrides",
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
  overrides: {}
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

struct CliLoadResult {
    stdout: String,
    report_path: PathBuf,
    report: Value,
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
