#!/usr/bin/env bash
set -euo pipefail

: "${AUDIT_REPORT_PATH:?AUDIT_REPORT_PATH must name the terminal audit report}"
: "${VULNERABILITY_IDS_PATH:?VULNERABILITY_IDS_PATH must name the vulnerability ID list}"
: "${ISSUE_TITLE:?ISSUE_TITLE must be set}"
: "${RUN_URL:?RUN_URL must be set}"

# `gh issue view` exposes the GitHub Actions app through two GraphQL logins:
# issue authors carry the app prefix, while comment authors do not.
readonly WORKFLOW_ISSUE_ACTOR="app/github-actions"
readonly WORKFLOW_COMMENT_ACTOR="github-actions"

normalize_ids() {
  awk '/^RUSTSEC-[0-9]{4}-[0-9]{4}$/ { print }' | LC_ALL=C sort -u
}

extract_last_recorded_ids() {
  awk '
    $0 == "<!-- cargo-audit-vulnerability-ids" {
      in_block = 1
      block = ""
      next
    }
    in_block && $0 == "-->" {
      last = block
      in_block = 0
      next
    }
    in_block && $0 ~ /^RUSTSEC-[0-9]{4}-[0-9]{4}$/ {
      block = block $0 "\n"
      next
    }
    in_block {
      in_block = 0
    }
    END {
      printf "%s", last
    }
  '
}

render_native_report() {
  local report_path=$1

  echo '```text'
  cat "$report_path"
  echo '```'
}

render_recorded_ids() {
  local ids=$1

  echo '<!-- cargo-audit-vulnerability-ids'
  if [ -n "$ids" ]; then
    printf '%s\n' "$ids"
  fi
  echo '-->'
}

existing_issue_number=$(
  gh issue list --state open --limit 100 --json number,title \
    | jq -r --arg title "$ISSUE_TITLE" \
        '[.[] | select(.title == $title)] | first | .number // empty'
)
current_ids=$(normalize_ids < "$VULNERABILITY_IDS_PATH")

if [ -z "$existing_issue_number" ]; then
  if [ -z "$current_ids" ]; then
    echo "No vulnerabilities and no open tracking issue; no action taken."
    exit 0
  fi

  issue_body=$(mktemp)
  trap 'rm -f -- "$issue_body"' EXIT

  {
    echo 'The weekly cargo-audit sensor reported findings on `Cargo.lock` beyond the accepted ignore list.'
    echo
    echo "Run: $RUN_URL"
    echo
    render_native_report "$AUDIT_REPORT_PATH"
    echo
    echo "Triage per ADR-0069: analyze reachability, then either accept the advisory with a recorded analysis and an ignore-list entry, or move down the fallback ladder (crates.io upgrade, maintainer-fork pinned rev, vendoring)."
    echo
    render_recorded_ids "$current_ids"
  } > "$issue_body"

  gh issue create \
    --title "$ISSUE_TITLE" \
    --label needs-triage \
    --body-file "$issue_body"
  exit 0
fi

issue_json=$(gh issue view "$existing_issue_number" --json author,body,comments)
recorded_ids=$(
  jq -r \
    --arg issue_actor "$WORKFLOW_ISSUE_ACTOR" \
    --arg comment_actor "$WORKFLOW_COMMENT_ACTOR" '
    (if .author.login == $issue_actor then .body else empty end),
    (.comments[]? | select(.author.login == $comment_actor) | .body)
  ' <<< "$issue_json" \
    | extract_last_recorded_ids \
    | normalize_ids
)

if [ "$recorded_ids" = "$current_ids" ]; then
  echo "Vulnerability set unchanged for open issue #$existing_issue_number; no comment posted."
  exit 0
fi

new_ids=$(comm -13 \
  <(printf '%s\n' "$recorded_ids" | normalize_ids) \
  <(printf '%s\n' "$current_ids" | normalize_ids))
cleared_ids=$(comm -23 \
  <(printf '%s\n' "$recorded_ids" | normalize_ids) \
  <(printf '%s\n' "$current_ids" | normalize_ids))

render_id_list() {
  local ids=$1

  if [ -z "$ids" ]; then
    echo "- None"
  else
    sed 's/^/- /' <<< "$ids"
  fi
}

comment_body=$(mktemp)
trap 'rm -f -- "$comment_body"' EXIT

{
  if [ -z "$current_ids" ]; then
    echo "The cargo-audit sensor reports that the findings have cleared."
  else
    echo "The cargo-audit sensor found a changed vulnerability set."
  fi
  echo
  echo "Run: $RUN_URL"
  echo
  echo "## New vulnerability IDs"
  echo
  render_id_list "$new_ids"
  echo
  echo "## Cleared vulnerability IDs"
  echo
  render_id_list "$cleared_ids"
  echo
  render_native_report "$AUDIT_REPORT_PATH"
  echo
  render_recorded_ids "$current_ids"
} > "$comment_body"

gh issue comment "$existing_issue_number" --body-file "$comment_body"
