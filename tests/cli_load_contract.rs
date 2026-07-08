use assert_cmd::Command;
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

#[test]
fn v1_load_definition_writes_report_and_human_summary() {
    let work = TempDir::new().expect("tempdir");
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
    let definition_path = work.path().join("load.yml");
    fs::write(
        &definition_path,
        r#"
version: 1
source:
  connector: local_file
  path: customers.csv
destination:
  connector: duckdb
  path: warehouse.duckdb
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
