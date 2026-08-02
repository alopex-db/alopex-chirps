#!/usr/bin/env bash
# Runs the existing in-process QUIC fixture as a diagnostic baseline only.
# Its MockNetwork control plane means it is explicitly not release evidence.
set -euo pipefail
umask 077

usage() {
  cat <<'USAGE'
Usage: run-local-v0_5_2-baseline.sh --output DIR

Writes test.log, host-facts.txt, and result.env. The script uses a temporary
CARGO_TARGET_DIR and deletes it after recording the result. A failing 100 MB/s
assertion is preserved in the evidence and returned as a non-zero status.
USAGE
}

output=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --output) output="${2:?missing value for --output}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done
[[ -n "$output" ]] || { printf '%s\n' '--output is required' >&2; exit 2; }
for command in cargo git mktemp rm; do
  command -v "$command" >/dev/null || { printf 'required command not found: %s\n' "$command" >&2; exit 127; }
done
if [[ -e "$output" ]] && [[ -n "$(find "$output" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]]; then
  printf 'refusing to overwrite non-empty baseline directory: %s\n' "$output" >&2
  exit 2
fi
mkdir -p "$output"
target_dir="$(mktemp -d /tmp/chirps-v0_5_2-local-target.XXXXXX)"
cleanup() { rm -rf -- "$target_dir"; }
trap cleanup EXIT INT TERM

{
  printf 'schema_version=1\n'
  printf 'kind=chirps-file-transfer-local-loopback\n'
  printf 'scope=single-host-loopback; chunk=data QUIC; control=MockNetwork\n'
  printf 'source_sha=%s\n' "$(git rev-parse HEAD)"
  printf 'started_at_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >"$output/result.env"
{
  printf '%s\n' '# hostname'; hostname 2>&1 || true
  printf '%s\n' '# uname'; uname -a 2>&1 || true
  printf '%s\n' '# cpu'; lscpu 2>&1 || true
  printf '%s\n' '# memory'; free -h 2>&1 || true
} >"$output/host-facts.txt"

set +e
CARGO_TARGET_DIR="$target_dir" CHIRPS_FILE_TRANSFER_PERF_1GBPS=1 \
  cargo test --release -p alopex-chirps-file-transfer --test file_transfer \
  file_transfer_throughput_meets_v0_5_2_target -- --ignored --nocapture >"$output/test.log" 2>&1
test_status=$?
set -e

throughput="$(sed -n 's/.*end-to-end=\([0-9][0-9]*\) B\/s.*/\1/p' "$output/test.log" | tail -n 1 || true)"
payload="$(sed -n 's/.*payload=\([0-9][0-9]*\) B\/s.*/\1/p' "$output/test.log" | tail -n 1 || true)"
{
  printf 'exit_status=%s\n' "$test_status"
  printf 'end_to_end_bytes_per_second=%s\n' "$throughput"
  printf 'payload_bytes_per_second=%s\n' "$payload"
  printf 'release_evidence=false\n'
  printf 'completed_at_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >>"$output/result.env"

if [[ "$test_status" -ne 0 ]]; then
  exit "$test_status"
fi
