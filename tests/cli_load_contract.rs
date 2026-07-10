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
fn malformed_local_jsonl_fails_with_report_before_destination_writing() {
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
        "malformed JSONL must fail before destination writing"
    );

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let (report_path, report) = read_single_report(
        &artifacts_dir,
        "failed jsonl load writes one artifact directory",
    );

    assert_eq!(report["report_version"], 1);
    assert_eq!(report["exit_status"], "failed");
    assert_eq!(report["process_exit_code"], 1);
    assert_eq!(report["row_counts"]["source"], 0);
    assert_eq!(report["row_counts"]["written"], 0);
    assert_eq!(report["destination_write"]["atomicity"], "not_applicable");
    assert_eq!(report["error_summary"]["code"], "malformed_jsonl");
    assert!(report["error_summary"]["message"]
        .as_str()
        .expect("error message")
        .contains("malformed JSONL"));

    assert!(stdout.contains("Status: failed"));
    assert!(stdout.contains("malformed JSONL"));
    assert!(stdout.contains(report_path.to_str().expect("report path")));
}

#[test]
fn malformed_local_csv_fails_with_report_before_destination_writing() {
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
        "malformed CSV must fail before destination writing"
    );

    let stdout = String::from_utf8(assert.get_output().stdout.clone()).expect("stdout utf8");
    let (report_path, report) = read_single_report(
        &artifacts_dir,
        "failed csv load writes one artifact directory",
    );

    assert_eq!(report["report_version"], 1);
    assert_eq!(report["exit_status"], "failed");
    assert_eq!(report["process_exit_code"], 1);
    assert_eq!(report["row_counts"]["source"], 0);
    assert_eq!(report["row_counts"]["written"], 0);
    assert_eq!(report["destination_write"]["atomicity"], "not_applicable");
    assert_eq!(report["error_summary"]["code"], "malformed_csv");
    assert!(report["error_summary"]["message"]
        .as_str()
        .expect("error message")
        .contains("malformed CSV syntax"));

    assert!(stdout.contains("Status: failed"));
    assert!(stdout.contains("malformed CSV syntax"));
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

fn single_parquet_file(destination_path: &Path) -> PathBuf {
    let parquet_files = fs::read_dir(destination_path)
        .expect("parquet destination directory")
        .map(|entry| entry.expect("destination entry").path())
        .filter(|path| path.extension().and_then(|extension| extension.to_str()) == Some("parquet"))
        .collect::<Vec<_>>();
    assert_eq!(parquet_files.len(), 1, "one parquet data file");
    parquet_files[0].clone()
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
    let file = File::open(parquet_path).expect("open parquet file");
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("parquet reader")
        .build()
        .expect("build parquet reader");
    let batch = reader
        .next()
        .expect("one parquet batch")
        .expect("read parquet batch");
    assert!(reader.next().is_none(), "one parquet batch expected");
    batch
}
