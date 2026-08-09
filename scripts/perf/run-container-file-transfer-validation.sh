#!/usr/bin/env bash
# Executes FileTransfer's lower-layer tests in a container built from the same
# immutable source SHA as the controlled product-performance image.
set -euo pipefail
umask 077

usage() {
  cat <<'USAGE'
Usage:
  run-container-file-transfer-validation.sh --output DIR [--image IMAGE]

Builds the Dockerfile's validation stage from committed source, then runs:
  1. alopex-chirps-file-transfer unit and integration tests,
  2. alopex-chirps-transport-quic integration tests,
  3. the FileTransfer Criterion component harness.

The output records logs, Criterion estimates, image/source identity, and a
hash manifest. It is local pre-binary evidence, not release evidence.
USAGE
}

output=""
image=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --output) output="${2:?missing value for --output}"; shift 2 ;;
    --image) image="${2:?missing value for --image}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "$output" ]] || { printf '%s\n' '--output is required' >&2; exit 2; }
if [[ -e "$output" ]] && [[ -n "$(find "$output" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]]; then
  printf 'refusing to overwrite non-empty output directory: %s\n' "$output" >&2
  exit 2
fi
for command in docker git tar mktemp python3 sha256sum; do
  command -v "$command" >/dev/null || { printf 'required command not found: %s\n' "$command" >&2; exit 127; }
done
[[ -z "$(git status --porcelain)" ]] || {
  printf '%s\n' 'refusing dirty source tree: commit the exact source before container validation' >&2
  exit 2
}

source_sha="$(git rev-parse HEAD)"
run_id="${source_sha:0:12}-$(date -u +%Y%m%dT%H%M%SZ)-$$"
container_name="chirps-ft-validation-${run_id}"
build_context="$(mktemp -d /tmp/chirps-ft-validation-context.XXXXXX)"
container_started=false

cleanup() {
  local status=$?
  if [[ "$container_started" == true ]]; then
    docker rm -f "$container_name" >/dev/null 2>&1 || true
  fi
  rm -rf -- "$build_context"
  exit "$status"
}
trap cleanup EXIT INT TERM

mkdir -p "$output/logs" "$output/evidence"
if [[ -z "$image" ]]; then
  image="chirps-ft-validation:${source_sha}"
  git archive --format=tar "$source_sha" | tar -x -C "$build_context"
  docker build \
    --file "$build_context/scripts/perf/container/Dockerfile" \
    --target validation \
    --build-arg "SOURCE_SHA=$source_sha" \
    --tag "$image" \
    "$build_context" >"$output/logs/image-build.log" 2>&1
fi

image_digest="$(docker image inspect --format '{{.Id}}' "$image")"
image_source_sha="$(docker image inspect --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' "$image")"
[[ "$image_source_sha" == "$source_sha" ]] || {
  printf 'validation image source label %s does not match checked-out SHA %s\n' "$image_source_sha" "$source_sha" >&2
  exit 2
}

docker run --detach --rm \
  --name "$container_name" \
  --network none \
  --memory 4g \
  --memory-swap 4g \
  "$image" sleep infinity >/dev/null
container_started=true

file_transfer_status=0
transport_status=0
criterion_status=0
if docker exec "$container_name" cargo test --locked -p alopex-chirps-file-transfer --tests \
  >"$output/logs/file-transfer-tests.log" 2>&1; then
  :
else
  file_transfer_status=$?
fi
if docker exec "$container_name" cargo test --locked -p alopex-chirps-transport-quic --tests \
  >"$output/logs/transport-quic-tests.log" 2>&1; then
  :
else
  transport_status=$?
fi
if [[ "$file_transfer_status" -eq 0 && "$transport_status" -eq 0 ]]; then
  if docker exec --env CARGO_TARGET_DIR=/src/target "$container_name" \
    cargo bench --locked -p alopex-chirps-file-transfer \
    --bench file_transfer_components >"$output/logs/criterion.log" 2>&1; then
    :
  else
    criterion_status=$?
  fi
else
  criterion_status=125
  printf '%s\n' 'Criterion skipped because a prerequisite test stage failed.' >"$output/logs/criterion.log"
fi

criterion_complete=false
if [[ "$criterion_status" -eq 0 ]]; then
  if docker exec "$container_name" test -d /src/target/criterion \
    && docker cp "$container_name:/src/target/criterion" "$output/criterion"; then
    if python3 scripts/perf/verify-file-transfer-criterion.py \
      --current "$output/criterion" \
      --output "$output/evidence/function-calibration.json" \
      >"$output/logs/criterion-verification.log" 2>&1; then
      criterion_complete=true
    else
      criterion_status=$?
    fi
  else
    criterion_status=2
    printf '%s\n' 'Criterion completed but /src/target/criterion could not be collected.' \
      >"$output/logs/criterion-verification.log"
  fi
fi

python3 - "$output" "$source_sha" "$image" "$image_digest" \
  "$file_transfer_status" "$transport_status" "$criterion_status" "$criterion_complete" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
file_transfer_status = int(sys.argv[5])
transport_status = int(sys.argv[6])
criterion_status = int(sys.argv[7])
criterion_complete = sys.argv[8] == "true"
passed = (
    file_transfer_status == 0
    and transport_status == 0
    and criterion_status == 0
    and criterion_complete
)
result = {
    "schema_version": 1,
    "evidence_class": "container-lower-layer-validation",
    "source_sha": sys.argv[2],
    "image": sys.argv[3],
    "image_digest": sys.argv[4],
    "network_mode": "none",
    "file_transfer_tests_exit_code": file_transfer_status,
    "transport_quic_tests_exit_code": transport_status,
    "criterion_exit_code": criterion_status,
    "criterion_operations_complete": criterion_complete,
    "criterion_regression_baseline_supplied": False,
    "passed": passed,
    "release_evidence": False,
}
(root / "evidence" / "result.json").write_text(
    json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
PY

(
  cd "$output"
  find . -type f ! -name manifest.sha256 -print0 | sort -z | xargs -0 sha256sum > manifest.sha256
)
printf 'container lower-layer evidence: %s/evidence\n' "$output"
[[ "$file_transfer_status" -eq 0 && "$transport_status" -eq 0 && "$criterion_status" -eq 0 && "$criterion_complete" == true ]]
