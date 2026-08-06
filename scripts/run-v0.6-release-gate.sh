#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: run-v0.6-release-gate.sh --output-dir DIR [--source-commit SHA]

Runs the v0.6-only local gate, produces registry build evidence, and assembles
the complete version-bound manifest. Missing required v0.6 evidence is fatal.
USAGE
}

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_dir=""
source_commit=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir) output_dir="${2:?missing value for --output-dir}"; shift 2 ;;
    --source-commit) source_commit="${2:?missing value for --source-commit}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done
[[ -n "$output_dir" ]] || { printf '%s\n' '--output-dir is required' >&2; exit 2; }
source_commit="${source_commit:-$(git -C "$repo_root" rev-parse HEAD)}"
catalog="$repo_root/docs/release/evidence/v0.6.0/required-evidence.json"

# Fail before expensive commands when another v0.6 requirement has not yet
# produced its real release evidence.
python3 "$repo_root/scripts/release/bundle-v0.6-evidence.py" \
  --repo-root "$repo_root" --catalog "$catalog" --preflight

scratch="$(mktemp -d "${TMPDIR:-/tmp}/chirps-v06-gate.XXXXXXXX")"
cleanup() { rm -rf "$scratch"; }
trap cleanup EXIT
registry_evidence="$scratch/alopex-core-registry.json"

"$repo_root/scripts/verify-registry-dependency.sh" \
  --source-commit "$source_commit" --output "$registry_evidence"
cargo test --locked --manifest-path "$repo_root/Cargo.toml" -p alopex-chirps --all-features
cargo test --locked --manifest-path "$repo_root/Cargo.toml" -p chirps-deterministic-harness

python3 "$repo_root/scripts/release/bundle-v0.6-evidence.py" \
  --repo-root "$repo_root" \
  --catalog "$catalog" \
  --source-commit "$source_commit" \
  --registry-evidence "$registry_evidence" \
  --output-dir "$output_dir"

python3 "$repo_root/scripts/release/verify-evidence-manifest.py" \
  --manifest "$output_dir/manifest.json" \
  --requirements "$catalog" \
  --schema "$repo_root/docs/release/evidence/v0.6.0/manifest.schema.json" \
  --version 0.6.0 \
  --source-commit "$source_commit"
