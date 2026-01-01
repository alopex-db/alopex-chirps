#!/bin/bash
# publish-crate.sh
# Publish a crate and ignore "already exists" errors.
#
# Usage: ./scripts/publish-crate.sh <crate-name>

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <crate-name>" >&2
  exit 2
fi

crate_name="$1"

set +e
publish_output="$(cargo publish -p "${crate_name}" 2>&1)"
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
