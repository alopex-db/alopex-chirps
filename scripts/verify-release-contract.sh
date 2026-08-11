#!/usr/bin/env bash
# Validates that a versioned release contract exists before publish.  The
# contract is reviewed evidence, not a substitute for the tests it references.
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: verify-release-contract.sh --version X.Y.Z [--require-ready]
       [--manifest FILE] [--source-commit SHA] [--repo-root DIR]

Checks docs/release/vX.Y.Z.md for traceability, exclusions, and approval
sections. --require-ready additionally rejects a non-READY release status and
unproven/TODO markers. If the version has a required-evidence catalog, a
version-bound manifest, target-version gate, exact required evidence set, and
artifact SHA-256 verification are required before --require-ready can succeed.
USAGE
}

tool_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repo_root="$tool_root"
version=""
require_ready=false
manifest=""
source_commit=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --version) version="${2:?missing value for --version}"; shift 2 ;;
    --require-ready) require_ready=true; shift ;;
    --manifest) manifest="${2:?missing value for --manifest}"; shift 2 ;;
    --source-commit) source_commit="${2:?missing value for --source-commit}"; shift 2 ;;
    --repo-root) repo_root="${2:?missing value for --repo-root}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || {
  printf '%s\n' '--version must be semantic version X.Y.Z' >&2
  exit 2
}
contract_rel="docs/release/v${version}.md"
contract="$repo_root/$contract_rel"
[[ -f "$contract" ]] || { printf 'missing release contract: %s\n' "$contract_rel" >&2; exit 1; }

for heading in '## 要件・検証対応表' '## 未証明・除外事項' '## 変更影響レビュー' '## 承認'; do
  grep -Fqx "$heading" "$contract" || { printf 'missing heading %s in %s\n' "$heading" "$contract" >&2; exit 1; }
done
awk '
  /^## 要件・検証対応表$/ { in_matrix = 1; next }
  /^## / { in_matrix = 0 }
  in_matrix && /^\|/ && $0 !~ /^\|[[:space:]-]+\|/ && $0 !~ /^\| 要件 \/ 失敗モード \|/ {
    found = 1
  }
  END { exit(found ? 0 : 1) }
' "$contract" || {
  printf 'missing acceptance matrix data row in %s\n' "$contract" >&2
  exit 1
}

if [[ "$require_ready" == true ]]; then
  grep -Fqx 'Release readiness: READY' "$contract" || {
    printf 'release contract is not READY: %s\n' "$contract" >&2
    exit 1
  }
  awk -F '|' '
    function trim(value) {
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      return value
    }
    function unresolved(value) {
      return value ~ /(BLOCKED|未証明|未検証|TODO|未記入|未取得|未確定)/
    }
    /^## 要件・検証対応表$/ { section = "matrix"; next }
    /^## 未証明・除外事項$/ { section = "exclusions"; next }
    /^## 承認$/ { section = "approvals"; next }
    /^## / { section = ""; next }
    section == "matrix" && /^\|/ && $0 !~ /^\|[[:space:]-]+\|/ && $0 !~ /^\| 要件 \/ 失敗モード \|/ {
      if (unresolved(trim($(NF - 1)))) {
        printf "unresolved acceptance status: %s\n", $0 > "/dev/stderr"
        invalid = 1
      }
    }
    section == "exclusions" && /^\|/ && $0 !~ /^\|[[:space:]-]+\|/ && $0 !~ /^\| 項目 \|/ {
      if (unresolved(trim($(NF - 1)))) {
        printf "release-blocking exclusion: %s\n", $0 > "/dev/stderr"
        invalid = 1
      }
    }
    section == "approvals" && /^- / && unresolved($0) {
      printf "unresolved approval: %s\n", $0 > "/dev/stderr"
      invalid = 1
    }
    END { exit(invalid ? 1 : 0) }
  ' "$contract" || {
    printf 'release contract retains an unresolved release-blocking marker: %s\n' "$contract" >&2
    exit 1
  }
fi

requirements="$repo_root/docs/release/evidence/v${version}/required-evidence.json"
if [[ -n "$manifest" ]]; then
  [[ -f "$requirements" ]] || {
    printf 'manifest supplied but release evidence catalog is missing: %s\n' "$requirements" >&2
    exit 1
  }
  source_commit="${source_commit:-$(git -C "$repo_root" rev-parse HEAD)}"
  python3 "$tool_root/scripts/release/verify-evidence-manifest.py" \
    --manifest "$manifest" \
    --requirements "$repo_root/docs/release/evidence/v${version}/required-evidence.json" \
    --schema "$repo_root/docs/release/evidence/v${version}/manifest.schema.json" \
    --version "$version" \
    --source-commit "$source_commit"
elif [[ "$require_ready" == true && -f "$requirements" ]]; then
  printf 'READY verification requires --manifest and the target-version gate for %s\n' "$version" >&2
  exit 1
fi

printf 'release contract validated: %s\n' "$contract_rel"
