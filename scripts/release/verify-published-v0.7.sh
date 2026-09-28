#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE' >&2
Usage: verify-published-v0.7.sh --evidence FILE --evidence-sha256 SHA256
       --candidate FILE --candidate-sha256 SHA256
       --bundle FILE --bundle-sha256 SHA256 --tag-object SHA1 [--schema FILE]
       verify-published-v0.7.sh --self-test

The verifier validates stored evidence, then reads the remote annotated tag,
all nine registry archives, OCI manifest, and exact GitHub release assets.
--tag-object pins the annotated tag object recorded by the approved tag job.
It performs no remote writes and never rebuilds or publishes an artifact.
USAGE
}

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
schema="$repo_root/docs/release/v0.7.0-evidence-schema.json"
evidence=""
candidate=""
bundle=""
evidence_sha256=""
candidate_sha256=""
bundle_sha256=""
tag_object=""
self_test=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --evidence) evidence="${2:?missing value for --evidence}"; shift 2 ;;
    --evidence-sha256) evidence_sha256="${2:?missing value for --evidence-sha256}"; shift 2 ;;
    --candidate) candidate="${2:?missing value for --candidate}"; shift 2 ;;
    --candidate-sha256) candidate_sha256="${2:?missing value for --candidate-sha256}"; shift 2 ;;
    --bundle) bundle="${2:?missing value for --bundle}"; shift 2 ;;
    --bundle-sha256) bundle_sha256="${2:?missing value for --bundle-sha256}"; shift 2 ;;
    --tag-object) tag_object="${2:?missing value for --tag-object}"; shift 2 ;;
    --schema) schema="${2:?missing value for --schema}"; shift 2 ;;
    --self-test) self_test=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage; exit 2 ;;
  esac
done

digest_file() {
  sha256sum -- "$1" | awk '{print $1}'
}

verify_digest() {
  local path="$1"
  local expected="$2"
  local label="$3"
  [[ "$expected" =~ ^[0-9a-f]{64}$ ]] || {
    printf '%s expected digest must be lowercase SHA-256\n' "$label" >&2
    return 1
  }
  [[ -f "$path" && ! -L "$path" ]] || {
    printf '%s must be a regular non-symlink file: %s\n' "$label" "$path" >&2
    return 1
  }
  local actual
  actual="$(digest_file "$path")"
  [[ "$actual" == "$expected" ]] || {
    printf '%s stored-byte digest mismatch\n' "$label" >&2
    return 1
  }
}

if [[ "$self_test" == true ]]; then
  [[ -z "$evidence" && -z "$candidate" && -z "$bundle" ]] || {
    printf '%s\n' '--self-test does not accept stored evidence paths' >&2
    exit 2
  }
  tmp_root="$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/chirps-v07-published.XXXXXX")"
  cleanup() { rm -rf -- "$tmp_root"; }
  trap cleanup EXIT
  fixture="$tmp_root/stored.bin"
  printf '%s' 'exact stored bytes' > "$fixture"
  fixture_sha256="$(digest_file "$fixture")"
  verify_digest "$fixture" "$fixture_sha256" fixture
  printf '%s' 'drift' >> "$fixture"
  if verify_digest "$fixture" "$fixture_sha256" byte-drift 2>/dev/null; then
    printf '%s\n' 'byte-drift negative fixture unexpectedly passed' >&2
    exit 1
  fi
  printf '%s\n' 'published verifier self-test passed: byte drift rejected'
  PYTHONDONTWRITEBYTECODE=1 python3 "$repo_root/scripts/release/test-verify-published-v0.7.py"
  exit 0
fi

[[ -n "$evidence" && -n "$candidate" && -n "$bundle" ]] || {
  printf '%s\n' 'published verification requires candidate, evidence, and bundle files' >&2
  exit 2
}
[[ -n "$evidence_sha256" && -n "$candidate_sha256" && -n "$bundle_sha256" ]] || {
  printf '%s\n' 'published verification requires all three expected SHA-256 values' >&2
  exit 2
}

verify_digest "$evidence" "$evidence_sha256" evidence
verify_digest "$candidate" "$candidate_sha256" candidate
verify_digest "$bundle" "$bundle_sha256" bundle
[[ "$tag_object" =~ ^[0-9a-f]{40}$ ]] || {
  printf '%s\n' '--tag-object must pin the expected annotated tag SHA-1' >&2
  exit 2
}

python3 - "$evidence" "$candidate" "$bundle" <<'PY'
import json
import sys
from pathlib import Path

evidence = Path(sys.argv[1]).resolve(strict=True)
supplied = {
    "candidate": Path(sys.argv[2]).resolve(strict=True),
    "bundle": Path(sys.argv[3]).resolve(strict=True),
}
index = json.loads(evidence.read_bytes())
for name, path in supplied.items():
    reference = index.get(name)
    if not isinstance(reference, dict) or not isinstance(reference.get("path"), str):
        raise SystemExit(f"evidence index has no {name} path reference")
    expected = (evidence.parent / reference["path"]).resolve(strict=True)
    if expected != path:
        raise SystemExit(f"supplied {name} is not the file referenced by evidence")
PY

"$repo_root/scripts/release/verify-v0.7-evidence.py" \
  --schema "$schema" "$evidence"

python3 "$repo_root/scripts/release/verify-published-v0.7.py" \
  --evidence "$evidence" --candidate "$candidate" --tag-object "$tag_object"

verify_digest "$evidence" "$evidence_sha256" evidence
verify_digest "$candidate" "$candidate_sha256" candidate
verify_digest "$bundle" "$bundle_sha256" bundle
printf '%s\n' 'published v0.7 remote artifacts match stored bytes'
