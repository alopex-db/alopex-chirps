#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 --version X.Y.Z --output-dir DIR --source-commit SHA" >&2
}

output_dir=""
source_commit=""
version=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --version) version="${2:?missing version}"; shift 2 ;;
    --output-dir) output_dir="${2:?missing output dir}"; shift 2 ;;
    --source-commit) source_commit="${2:?missing source commit}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) usage; exit 2 ;;
  esac
done
[[ -n "$version" && -n "$output_dir" && -n "$source_commit" ]] || { usage; exit 2; }

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_root="$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/chirps-v06-gate.XXXXXX")"
cleanup() { rm -rf "$tmp_root"; }
trap cleanup EXIT

python3 "$repo_root/scripts/release/bundle-v0.6-evidence.py" \
  --repo-root "$repo_root" \
  --catalog "$repo_root/docs/release/evidence/v${version}/required-evidence.json" \
  --version "$version" \
  --preflight

CARGO_TARGET_DIR="$tmp_root/target-tests" cargo test --locked -p alopex-chirps --all-features -- --test-threads=1

CARGO_TARGET_DIR="$tmp_root/target-harness" cargo run --locked -p chirps-deterministic-harness -- \
  run --scenario multi-raft-v0.6 --seed 0x0000000000000603 \
  --artifact "$tmp_root/multi-raft-fault.json" --minimize-on-failure
CARGO_TARGET_DIR="$tmp_root/target-replay" cargo run --locked -p chirps-deterministic-harness -- \
  replay --artifact "$tmp_root/multi-raft-fault.json"

registry="$tmp_root/alopex-core-registry.json"
bash "$repo_root/scripts/verify-registry-dependency.sh" \
  --output "$registry" --release-version "$version" --source-commit "$source_commit"

rm -rf "$output_dir"
python3 "$repo_root/scripts/release/bundle-v0.6-evidence.py" \
  --repo-root "$repo_root" \
  --catalog "$repo_root/docs/release/evidence/v${version}/required-evidence.json" \
  --version "$version" \
  --source-commit "$source_commit" \
  --registry-evidence "$registry" \
  --output-dir "$output_dir" \
  --target-command "cargo test --locked -p alopex-chirps --all-features -- --test-threads=1" \
  --target-command "cargo test --locked -p chirps-deterministic-harness" \
  --target-command "cargo build --locked -p alopex-chirps-raft-storage (isolated registry-only workspace)"
python3 "$repo_root/scripts/release/verify-evidence-manifest.py" \
  --manifest "$output_dir/manifest.json" \
  --requirements "$repo_root/docs/release/evidence/v${version}/required-evidence.json" \
  --schema "$repo_root/docs/release/evidence/v${version}/manifest.schema.json" \
  --version "$version" --source-commit "$source_commit"
