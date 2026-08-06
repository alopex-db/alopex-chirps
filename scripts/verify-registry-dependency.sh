#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: verify-registry-dependency.sh --output FILE [--source-commit SHA] [--offline]

Copies the registry-only fixture to an isolated temporary workspace, builds it
with Cargo.lock, and writes evidence only after the real build succeeds.
USAGE
}

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source_commit=""
output=""
offline=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --output) output="${2:?missing value for --output}"; shift 2 ;;
    --source-commit) source_commit="${2:?missing value for --source-commit}"; shift 2 ;;
    --offline) offline=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "$output" ]] || { printf '%s\n' '--output is required' >&2; exit 2; }
source_commit="${source_commit:-$(git -C "$repo_root" rev-parse HEAD)}"
fixture="$repo_root/scripts/fixtures/alopex-core-registry-check"
[[ -f "$fixture/Cargo.lock" ]] || {
  printf 'fixture lock is missing: %s\n' "$fixture/Cargo.lock" >&2
  exit 1
}

scratch="$(mktemp -d "${TMPDIR:-/tmp}/chirps-registry-check.XXXXXXXX")"
cleanup() { rm -rf "$scratch"; }
trap cleanup EXIT
mkdir -p "$scratch/crates/chirps-raft-storage"
cp "$fixture/Cargo.toml" "$fixture/Cargo.lock" "$scratch/"
cp "$repo_root/crates/chirps-raft-storage/Cargo.toml" "$scratch/crates/chirps-raft-storage/Cargo.toml"
cp -R "$repo_root/crates/chirps-raft-storage/src" "$scratch/crates/chirps-raft-storage/src"

cargo_args=(build --locked --manifest-path "$scratch/Cargo.toml" -p alopex-chirps-raft-storage)
if [[ "$offline" == true ]]; then
  cargo_args+=(--offline)
fi
cargo "${cargo_args[@]}"

python3 "$repo_root/scripts/release/verify-registry-dependency.py" \
  --root-manifest "$repo_root/crates/chirps-raft-storage/Cargo.toml" \
  --root-lock "$repo_root/Cargo.lock" \
  --fixture "$scratch" \
  --schema "$repo_root/docs/release/evidence/v0.6.0/registry-dependency.schema.json" \
  --source-commit "$source_commit" \
  --output "$output"
