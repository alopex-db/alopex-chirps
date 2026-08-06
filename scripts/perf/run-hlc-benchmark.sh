#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf '%s\n' 'Usage: run-hlc-benchmark.sh --output FILE'
}

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
output=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --output) output="${2:?missing value for --output}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done
[[ -n "$output" ]] || { printf '%s\n' '--output is required' >&2; exit 2; }

target_dir="${CARGO_TARGET_DIR:-$repo_root/target}"
cargo bench --manifest-path "$repo_root/Cargo.toml" \
  -p alopex-chirps --features hlc --bench hlc_bench -- --noplot
python3 "$repo_root/scripts/perf/collect-hlc-evidence.py" \
  --repo-root "$repo_root" \
  --estimates "$target_dir/criterion/local_hlc_tick/new/estimates.json" \
  --output "$output"
