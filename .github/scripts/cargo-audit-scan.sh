#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: cargo-audit-scan.sh <terminal-report> <json-report> <vulnerability-ids>" >&2
  exit 64
fi

readonly terminal_report=$1
readonly json_report=$2
readonly vulnerability_ids=$3

terminal_exit=0
cargo audit 2>&1 | tee "$terminal_report" || terminal_exit=$?

json_exit=0
cargo audit -n --json > "$json_report" || json_exit=$?

jq -e '.vulnerabilities.list | type == "array"' "$json_report" > /dev/null
jq -r '.vulnerabilities.list[].advisory.id' "$json_report" \
  | LC_ALL=C sort -u > "$vulnerability_ids"

if [ -s "$vulnerability_ids" ]; then
  if [ "$terminal_exit" -eq 0 ] \
    || [ "$json_exit" -eq 0 ] \
    || ! grep -Eq 'error: [0-9]+ vulnerabilit' "$terminal_report"; then
    echo "cargo-audit terminal and JSON results disagree about vulnerability findings" >&2
    exit 1
  fi
  exit 0
fi

if [ "$terminal_exit" -ne 0 ]; then
  exit "$terminal_exit"
fi
if [ "$json_exit" -ne 0 ]; then
  exit "$json_exit"
fi
