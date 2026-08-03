#!/usr/bin/env bash
# Validates that a versioned release contract exists before publish.  The
# contract is reviewed evidence, not a substitute for the tests it references.
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: verify-release-contract.sh --version X.Y.Z [--require-ready]

Checks docs/release/vX.Y.Z.md for traceability, exclusions, and approval
sections. --require-ready additionally rejects a non-READY release status and
unproven/TODO markers.
USAGE
}

version=""
require_ready=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --version) version="${2:?missing value for --version}"; shift 2 ;;
    --require-ready) require_ready=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || {
  printf '%s\n' '--version must be semantic version X.Y.Z' >&2
  exit 2
}
contract="docs/release/v${version}.md"
[[ -f "$contract" ]] || { printf 'missing release contract: %s\n' "$contract" >&2; exit 1; }

for heading in '## 要件・検証対応表' '## 未証明・除外事項' '## 変更影響レビュー' '## 承認'; do
  rg -Fqx "$heading" "$contract" || { printf 'missing heading %s in %s\n' "$heading" "$contract" >&2; exit 1; }
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
  rg -Fqx 'Release readiness: READY' "$contract" || {
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

printf 'release contract validated: %s\n' "$contract"
