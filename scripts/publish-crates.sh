#!/usr/bin/env bash
# Resolve the workspace dependency DAG before publishing any crate.
set -euo pipefail

usage() {
  cat <<'USAGE' >&2
Usage: publish-crates.sh --repo-root DIR [--wait-seconds N] [--plan-only]
USAGE
}

repo_root=""
wait_seconds=30
plan_only=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-root) repo_root="${2:?missing value for --repo-root}"; shift 2 ;;
    --wait-seconds) wait_seconds="${2:?missing value for --wait-seconds}"; shift 2 ;;
    --plan-only) plan_only=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage; exit 2 ;;
  esac
done

[[ -n "$repo_root" && -d "$repo_root" ]] || { printf '%s\n' '--repo-root must name a directory' >&2; exit 2; }
[[ "$wait_seconds" =~ ^[0-9]+$ ]] || { printf '%s\n' '--wait-seconds must be a non-negative integer' >&2; exit 2; }

tool_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
plan_file="$(mktemp "${TMPDIR:-/tmp}/chirps-publish-plan.XXXXXXXX")"
cleanup() { rm -f "$plan_file"; }
trap cleanup EXIT
python3 "$tool_root/scripts/release/resolve-publish-order.py" --repo-root "$repo_root" > "$plan_file"

cat "$plan_file"
if [[ "$plan_only" == true ]]; then
  exit 0
fi

current_layer=""
while IFS=$'\t' read -r layer crate; do
  if [[ -n "$current_layer" && "$layer" != "$current_layer" ]]; then
    echo "Waiting ${wait_seconds}s for the crates.io index after publish layer ${current_layer}"
    sleep "$wait_seconds"
  fi
  echo "Publishing ${crate} (dependency layer ${layer})"
  bash "$tool_root/scripts/publish-crate.sh" --repo-root "$repo_root" "$crate"
  current_layer="$layer"
done < "$plan_file"
