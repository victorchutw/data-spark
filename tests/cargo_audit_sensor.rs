use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tempfile::TempDir;

const SENSOR: &str = ".github/scripts/cargo-audit-sensor.sh";

struct SensorRun {
    _work: TempDir,
    output: Output,
    calls_path: PathBuf,
    body_path: PathBuf,
}

impl SensorRun {
    fn calls(&self) -> String {
        fs::read_to_string(&self.calls_path).unwrap_or_default()
    }

    fn body(&self) -> String {
        fs::read_to_string(&self.body_path).unwrap_or_default()
    }
}

fn run_sensor(current_ids: &str, issue_list_json: &str, issue_json: &str) -> SensorRun {
    let work = tempfile::tempdir().expect("create sensor test directory");
    let bin = work.path().join("bin");
    fs::create_dir(&bin).expect("create stub bin directory");

    let report_path = work.path().join("audit-report.txt");
    let ids_path = work.path().join("vulnerability-ids.txt");
    let calls_path = work.path().join("gh-mutations.log");
    let body_path = work.path().join("gh-body.md");
    fs::write(&report_path, "cargo-audit terminal report\n").expect("write audit report");
    fs::write(&ids_path, current_ids).expect("write vulnerability IDs");

    let gh_path = bin.join("gh");
    fs::write(
        &gh_path,
        r#"#!/usr/bin/env bash
set -euo pipefail

case "$1 $2" in
  "issue list")
    printf '%s\n' "$GH_ISSUE_LIST_JSON"
    ;;
  "issue view")
    printf '%s\n' "$GH_ISSUE_JSON"
    ;;
  "issue comment"|"issue create"|"issue edit")
    printf '%s\n' "$*" >> "$GH_MUTATIONS"
    while [ "$#" -gt 0 ]; do
      if [ "$1" = "--body-file" ]; then
        cp "$2" "$GH_BODY"
        break
      fi
      shift
    done
    ;;
  *)
    printf 'unexpected gh command: %s\n' "$*" >&2
    exit 64
    ;;
esac
"#,
    )
    .expect("write gh stub");
    let mut permissions = fs::metadata(&gh_path).expect("stat gh stub").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&gh_path, permissions).expect("make gh stub executable");

    let path = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").expect("PATH is set")
    );
    let output = Command::new("bash")
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join(SENSOR))
        .env("PATH", path)
        .env("AUDIT_REPORT_PATH", &report_path)
        .env("VULNERABILITY_IDS_PATH", &ids_path)
        .env(
            "ISSUE_TITLE",
            "cargo-audit sensor: new findings on Cargo.lock",
        )
        .env("RUN_URL", "https://github.test/actions/runs/123")
        .env("GH_REPO", "victorchutw/data-spark")
        .env("GH_ISSUE_LIST_JSON", issue_list_json)
        .env("GH_ISSUE_JSON", issue_json)
        .env("GH_MUTATIONS", &calls_path)
        .env("GH_BODY", &body_path)
        .output()
        .expect("run cargo-audit sensor");

    SensorRun {
        _work: work,
        output,
        calls_path,
        body_path,
    }
}

#[test]
fn matching_recorded_vulnerability_set_posts_no_comment() {
    let issue_json = r#"{
        "author": {"login": "app/github-actions"},
        "body": "<!-- cargo-audit-vulnerability-ids\nRUSTSEC-2026-0001\nRUSTSEC-2026-0002\n-->",
        "comments": []
    }"#;

    let run = run_sensor(
        "RUSTSEC-2026-0002\nRUSTSEC-2026-0001\n",
        r#"[{"number":42,"title":"cargo-audit sensor: new findings on Cargo.lock"}]"#,
        issue_json,
    );

    assert!(
        run.output.status.success(),
        "sensor failed: {}",
        String::from_utf8_lossy(&run.output.stderr)
    );
    assert_eq!(run.calls(), "");
}

#[test]
fn new_advisory_named_only_by_a_human_is_reported_with_the_full_change() {
    let issue_json = r#"{
        "author": {"login": "app/github-actions"},
        "body": "<!-- cargo-audit-vulnerability-ids\nRUSTSEC-2026-0001\n-->",
        "comments": [
            {
                "author": {"login": "maintainer"},
                "body": "Unlike RUSTSEC-2026-0002, the existing finding is understood."
            }
        ]
    }"#;

    let run = run_sensor(
        "RUSTSEC-2026-0002\nRUSTSEC-2026-0001\n",
        r#"[{"number":42,"title":"cargo-audit sensor: new findings on Cargo.lock"}]"#,
        issue_json,
    );

    assert!(
        run.output.status.success(),
        "sensor failed: {}",
        String::from_utf8_lossy(&run.output.stderr)
    );
    assert!(run.calls().starts_with("issue comment 42 --body-file "));
    assert!(!run.calls().contains("--label"));
    assert_eq!(
        run.body(),
        "The cargo-audit sensor found a changed vulnerability set.\n\n\
Run: https://github.test/actions/runs/123\n\n\
## New vulnerability IDs\n\n\
- RUSTSEC-2026-0002\n\n\
## Cleared vulnerability IDs\n\n\
- None\n\n\
```text\n\
cargo-audit terminal report\n\
```\n\n\
<!-- cargo-audit-vulnerability-ids\n\
RUSTSEC-2026-0001\n\
RUSTSEC-2026-0002\n\
-->\n"
    );
}

#[test]
fn fully_cleared_findings_are_reported_from_the_latest_workflow_record() {
    let issue_json = r#"{
        "author": {"login": "app/github-actions"},
        "body": "<!-- cargo-audit-vulnerability-ids\nRUSTSEC-2026-0001\n-->",
        "comments": [
            {
                "author": {"login": "github-actions"},
                "body": "<!-- cargo-audit-vulnerability-ids\nRUSTSEC-2026-0002\n-->"
            },
            {
                "author": {"login": "maintainer"},
                "body": "<!-- cargo-audit-vulnerability-ids\nRUSTSEC-2026-0003\n-->"
            }
        ]
    }"#;

    let run = run_sensor(
        "",
        r#"[{"number":42,"title":"cargo-audit sensor: new findings on Cargo.lock"}]"#,
        issue_json,
    );

    assert!(
        run.output.status.success(),
        "sensor failed: {}",
        String::from_utf8_lossy(&run.output.stderr)
    );
    assert!(run.calls().starts_with("issue comment 42 --body-file "));
    assert!(!run.calls().contains("issue edit"));
    assert!(!run.calls().contains("--label"));
    assert_eq!(
        run.body(),
        "The cargo-audit sensor reports that the findings have cleared.\n\n\
Run: https://github.test/actions/runs/123\n\n\
## New vulnerability IDs\n\n\
- None\n\n\
## Cleared vulnerability IDs\n\n\
- RUSTSEC-2026-0002\n\n\
```text\n\
cargo-audit terminal report\n\
```\n\n\
<!-- cargo-audit-vulnerability-ids\n\
-->\n"
    );
}

#[test]
fn findings_without_an_open_tracking_issue_open_the_original_issue_shape() {
    let run = run_sensor("RUSTSEC-2026-0002\nRUSTSEC-2026-0001\n", "[]", "{}");

    assert!(
        run.output.status.success(),
        "sensor failed: {}",
        String::from_utf8_lossy(&run.output.stderr)
    );
    assert!(run.calls().starts_with(
        "issue create --title cargo-audit sensor: new findings on Cargo.lock --label needs-triage --body-file "
    ));
    assert!(!run.calls().contains("issue comment"));
    assert_eq!(
        run.body(),
        "The weekly cargo-audit sensor reported findings on `Cargo.lock` beyond the accepted ignore list.\n\n\
Run: https://github.test/actions/runs/123\n\n\
```text\n\
cargo-audit terminal report\n\
```\n\n\
Triage per ADR-0069: analyze reachability, then either accept the advisory with a recorded analysis and an ignore-list entry, or move down the fallback ladder (crates.io upgrade, maintainer-fork pinned rev, vendoring).\n\n\
<!-- cargo-audit-vulnerability-ids\n\
RUSTSEC-2026-0001\n\
RUSTSEC-2026-0002\n\
-->\n"
    );
}
