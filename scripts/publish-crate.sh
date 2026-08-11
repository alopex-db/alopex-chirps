#!/bin/bash
# publish-crate.sh
# Publish a crate and ignore only the registry's idempotent "already exists" error.
#
# Usage: ./scripts/publish-crate.sh --repo-root DIR <crate-name>

set -euo pipefail

repo_root=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-root) repo_root="${2:?missing value for --repo-root}"; shift 2 ;;
    -h|--help) echo "Usage: $0 --repo-root DIR <crate-name>"; exit 0 ;;
    --) shift; break ;;
    *) break ;;
  esac
done
[[ -n "$repo_root" && $# -eq 1 ]] || {
  echo "Usage: $0 --repo-root DIR <crate-name>" >&2
  exit 2
}

crate_name="$1"

set +e
publish_output="$(cargo publish --manifest-path "${repo_root}/Cargo.toml" -p "${crate_name}" 2>&1)"
publish_status=$?
set -e

printf '%s\n' "${publish_output}"

if [[ ${publish_status} -ne 0 ]]; then
  if echo "${publish_output}" | grep -qiE "already exists|already uploaded|already published|already present|already in the index"; then
    echo "crate ${crate_name} already published; skipping."
    exit 0
  fi
  exit "${publish_status}"
fi
