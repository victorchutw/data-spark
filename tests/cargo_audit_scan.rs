use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;

#[test]
fn scan_preserves_the_terminal_report_and_extracts_the_json_vulnerability_set() {
    let work = tempfile::tempdir().expect("create audit scan test directory");
    let bin = work.path().join("bin");
    fs::create_dir(&bin).expect("create stub bin directory");

    let cargo_path = bin.join("cargo");
    fs::write(
        &cargo_path,
        r#"#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$CARGO_CALLS"

if [ "$*" = "audit" ]; then
  printf '%s\n' 'native cargo-audit report'
  printf '%s\n' 'error: 2 vulnerabilities found!'
  exit 1
fi

if [ "$*" = "audit -n --json" ]; then
  printf '%s\n' '{"vulnerabilities":{"list":[{"advisory":{"id":"RUSTSEC-2026-0002"}},{"advisory":{"id":"RUSTSEC-2026-0001"}}]}}'
  exit 1
fi

exit 64
"#,
    )
    .expect("write cargo stub");
    let mut permissions = fs::metadata(&cargo_path)
        .expect("stat cargo stub")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&cargo_path, permissions).expect("make cargo stub executable");

    let report_path = work.path().join("audit-report.txt");
    let json_path = work.path().join("audit.json");
    let ids_path = work.path().join("vulnerability-ids.txt");
    let calls_path = work.path().join("cargo-calls.log");
    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").expect("PATH is set")
    );
    let output = Command::new("bash")
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join(".github/scripts/cargo-audit-scan.sh"))
        .arg(&report_path)
        .arg(&json_path)
        .arg(&ids_path)
        .env("PATH", path)
        .env("CARGO_CALLS", &calls_path)
        .output()
        .expect("run cargo-audit scan");

    assert!(
        output.status.success(),
        "scan failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read_to_string(calls_path).expect("read cargo calls"),
        "audit\naudit -n --json\n"
    );
    assert_eq!(
        fs::read_to_string(report_path).expect("read terminal report"),
        "native cargo-audit report\nerror: 2 vulnerabilities found!\n"
    );
    assert_eq!(
        fs::read_to_string(ids_path).expect("read vulnerability IDs"),
        "RUSTSEC-2026-0001\nRUSTSEC-2026-0002\n"
    );
}
