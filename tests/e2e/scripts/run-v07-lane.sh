#!/usr/bin/env bash
set -euo pipefail

if ! command -v rtk >/dev/null 2>&1; then
    rtk() {
        [[ "${1:-}" != "proxy" ]] || shift
        command "$@"
    }
fi

readonly EXPECTED_SOURCE_COMMIT="76dcccf24b27c61a9434a79b3f81f065c4d3a832"
readonly EXPECTED_SOURCE_TREE="c63cdecf5edb11650e7db85db3f7ad168a2103f7"
readonly EXPECTED_PRODUCTION_MANIFEST_SHA256="a18d6fb6bf0a0d3662176dcd0fde91f0e49989c53bc6cffffd21d60a19958ca0"
readonly EXPECTED_FAULT_MANIFEST_SHA256="1efa54339d9a07372419bc908885a1872456941d589ccecccda4baa65e87c7e1"
readonly EXPECTED_PRODUCTION_OUTPUT="/home/roomtv/works/alopex-db/release-artifacts/chirps-v0.7.0/server/production/iggy-server"
readonly EXPECTED_PRODUCTION_SHA256="254865eb345fd2c095513e95c449d183bef9801f472f6dd50310a2d2421e297c"
readonly EXPECTED_FAULT_OUTPUT="/home/roomtv/works/alopex-db/release-artifacts/chirps-v0.7.0/server/test/iggy-server"
readonly EXPECTED_FAULT_SHA256="2daa29a71b9d7ba4555ad84f2273c0f269744efe6091dd0dc8aa2fa647fb31b4"
readonly e2e_run_token="chirps-v07-e2e-${BASHPID}-${RANDOM}"

usage() {
    rtk echo "usage: $0 --lane production|fault (--target NAME|--materialized-all|--strict-all) [--evidence-dir DIR]" >&2
    exit 64
}

lane=""
mode=""
selected_target=""
evidence_dir=""
while (($#)); do
    case "$1" in
        --evidence-dir)
            (($# >= 2)) || usage
            [[ -z "$evidence_dir" ]] || usage
            evidence_dir="$2"
            shift 2
            ;;
        --lane)
            (($# >= 2)) || usage
            lane="$2"
            shift 2
            ;;
        --target)
            (($# >= 2)) || usage
            [[ -z "$mode" ]] || usage
            mode="target"
            selected_target="$2"
            shift 2
            ;;
        --materialized-all)
            [[ -z "$mode" ]] || usage
            mode="materialized"
            shift
            ;;
        --strict-all)
            [[ -z "$mode" ]] || usage
            mode="strict"
            shift
            ;;
        *) usage ;;
    esac
done
[[ "$lane" == "production" || "$lane" == "fault" ]] || usage
[[ -n "$mode" ]] || usage

readonly script_dir="$(cd "$(rtk dirname "${BASH_SOURCE[0]}")" && rtk pwd)"
readonly repository_root="$(cd "${script_dir}/../../.." && rtk pwd)"
readonly e2e_root="${repository_root}/tests/e2e"
readonly production_manifest="${repository_root}/server/iggy-compatible/manifest.toml"
readonly fault_manifest="${repository_root}/server/iggy-compatible/test-manifest.toml"
cd "${repository_root}"
: "${CHIRPS_SERVER_MANIFEST:?CHIRPS_SERVER_MANIFEST is required}"
: "${CHIRPS_ARTIFACT_KIND:?CHIRPS_ARTIFACT_KIND is required}"
: "${CHIRPS_REQUIRE_OUTPUT_DIGEST:?CHIRPS_REQUIRE_OUTPUT_DIGEST is required}"
: "${CHIRPS_LOCAL_CORPUS_ROOT:?CHIRPS_LOCAL_CORPUS_ROOT is required}"
[[ "${CHIRPS_REQUIRE_OUTPUT_DIGEST}" == "1" ]] || {
    rtk echo "CHIRPS_REQUIRE_OUTPUT_DIGEST must be 1" >&2
    exit 65
}
[[ -d "${CHIRPS_LOCAL_CORPUS_ROOT}" ]] || {
    rtk echo "CHIRPS_LOCAL_CORPUS_ROOT must be an existing directory" >&2
    exit 65
}

mapfile -t verified < <(rtk python3 - \
    "${repository_root}" \
    "${CHIRPS_SERVER_MANIFEST}" \
    "${lane}" \
    "${CHIRPS_ARTIFACT_KIND}" \
    "${production_manifest}" \
    "${fault_manifest}" \
    "${EXPECTED_SOURCE_COMMIT}" \
    "${EXPECTED_SOURCE_TREE}" \
    "${EXPECTED_PRODUCTION_MANIFEST_SHA256}" \
    "${EXPECTED_FAULT_MANIFEST_SHA256}" \
    "${EXPECTED_PRODUCTION_OUTPUT}" \
    "${EXPECTED_PRODUCTION_SHA256}" \
    "${EXPECTED_FAULT_OUTPUT}" \
    "${EXPECTED_FAULT_SHA256}" <<'PY'
import hashlib
import os
import pathlib
import stat
import sys
import tomllib

root = pathlib.Path(sys.argv[1])
provided_manifest = pathlib.Path(sys.argv[2])
lane = sys.argv[3]
requested_kind = sys.argv[4]
production_path = pathlib.Path(sys.argv[5])
fault_path = pathlib.Path(sys.argv[6])
expected_source_commit = sys.argv[7]
expected_source_tree = sys.argv[8]
production_manifest_sha256 = sys.argv[9]
fault_manifest_sha256 = sys.argv[10]
production_output = pathlib.Path(sys.argv[11])
production_output_sha256 = sys.argv[12]
fault_output = pathlib.Path(sys.argv[13])
fault_output_sha256 = sys.argv[14]

expected_manifest = production_path if lane == "production" else fault_path
expected_manifest_sha256 = (
    production_manifest_sha256 if lane == "production" else fault_manifest_sha256
)
expected_output = production_output if lane == "production" else fault_output
expected_output_sha256 = (
    production_output_sha256 if lane == "production" else fault_output_sha256
)

def require_canonical_regular(path, label):
    if not path.is_absolute() or path.resolve(strict=True) != path:
        raise SystemExit(f"{label} path is not canonical")
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise SystemExit(f"{label} must be a non-symlink regular file")

if provided_manifest != expected_manifest:
    raise SystemExit("runner manifest path is not the canonical lane manifest")
require_canonical_regular(expected_manifest, "artifact manifest")
manifest_bytes = expected_manifest.read_bytes()
if hashlib.sha256(manifest_bytes).hexdigest() != expected_manifest_sha256:
    raise SystemExit("artifact manifest digest disagrees with the attested trust anchor")
manifest = tomllib.loads(manifest_bytes.decode("utf-8"))

expected_kind = "production" if lane == "production" else "publish-disabled-test"
if requested_kind != expected_kind or manifest["artifact"]["kind"] != expected_kind:
    raise SystemExit("lane, requested kind, and manifest kind must agree")
if manifest.get("schema_version") != 1:
    raise SystemExit("unsupported artifact manifest schema")
if bool(manifest["artifact"]["publishable"]) != (lane == "production"):
    raise SystemExit("manifest publishability disagrees with lane")
if manifest["build"].get("default_features") is not False:
    raise SystemExit("compatible server must disable default features")
expected_features = ["mimalloc"] if lane == "production" else ["mimalloc", "chirps-test-failpoints"]
if manifest["build"].get("features") != expected_features:
    raise SystemExit("compatible server feature set is not exact")
if lane == "production" and "chirps-test-failpoints" not in manifest["build"].get("forbidden_features", []):
    raise SystemExit("production manifest does not forbid the fault feature")
if manifest["source"].get("commit") != expected_source_commit:
    raise SystemExit("source commit disagrees with the attested trust anchor")
if manifest["source"].get("tree") != expected_source_tree:
    raise SystemExit("source tree disagrees with the attested trust anchor")
if manifest["build"].get("runtime_build_sha") != expected_source_commit:
    raise SystemExit("runtime build SHA disagrees with source commit")

binary = pathlib.Path(manifest["artifact"]["output_path"])
if binary != expected_output:
    raise SystemExit("server artifact path disagrees with the attested trust anchor")
if manifest["artifact"]["output_sha256"] != expected_output_sha256:
    raise SystemExit("server artifact digest disagrees with the attested trust anchor")
require_canonical_regular(binary, "server artifact")
if not os.access(binary, os.X_OK):
    raise SystemExit("server artifact is not executable")
actual_digest = hashlib.sha256(binary.read_bytes()).hexdigest()
if actual_digest != expected_output_sha256:
    raise SystemExit("server artifact digest disagrees with manifest")

if lane == "fault":
    failpoints = manifest.get("failpoints", {})
    if failpoints.get("environment") != "IGGY_CHIRPS_FAILPOINT" or "response" not in failpoints.get("stages", []):
        raise SystemExit("fault manifest lacks the response failpoint contract")
    if manifest["verification"].get("production_manifest") != "server/iggy-compatible/manifest.toml":
        raise SystemExit("fault manifest production reference is not canonical")
    referenced_production = root / manifest["verification"]["production_manifest"]
    if referenced_production != production_path:
        raise SystemExit("fault manifest production reference escaped the canonical path")
    require_canonical_regular(production_path, "production manifest")
    production_bytes = production_path.read_bytes()
    if hashlib.sha256(production_bytes).hexdigest() != production_manifest_sha256:
        raise SystemExit("production manifest digest disagrees with the attested trust anchor")
    production = tomllib.loads(production_bytes.decode("utf-8"))
    if manifest["source"] != production["source"]:
        raise SystemExit("fault and production source identities differ")
    if production["source"].get("commit") != expected_source_commit:
        raise SystemExit("production source commit disagrees with the attested trust anchor")
    if production["source"].get("tree") != expected_source_tree:
        raise SystemExit("production source tree disagrees with the attested trust anchor")
    if pathlib.Path(production["artifact"]["output_path"]) != production_output:
        raise SystemExit("production output path disagrees with the attested trust anchor")
    if production["artifact"].get("output_sha256") != production_output_sha256:
        raise SystemExit("production output digest disagrees with the attested trust anchor")
    if manifest["artifact"]["output_path"] == production["artifact"]["output_path"]:
        raise SystemExit("fault and production outputs are not distinct")
    if manifest["artifact"]["output_sha256"] == production["artifact"]["output_sha256"]:
        raise SystemExit("fault and production digests are not distinct")

print(binary)
print(actual_digest)
print(manifest["source"]["commit"])
print(manifest["source"]["tree"])
PY
)
[[ "${#verified[@]}" == "4" ]] || {
    rtk echo "artifact verification did not return a complete identity" >&2
    exit 65
}
export CHIRPS_SERVER_BINARY="${verified[0]}"
export CHIRPS_SERVER_SHA256="${verified[1]}"
export CHIRPS_SERVER_SOURCE_COMMIT="${verified[2]}"
export CHIRPS_SERVER_SOURCE_TREE="${verified[3]}"
export CHIRPS_E2E_LANE="${lane}"

readonly -a production_targets=(
    durable_session durable_send durable_shutdown durable_creation durable_checkpoint
    durable_poll durable_delivery durable_compaction durable_diagnostics durable_observability
)
readonly -a fault_targets=(
    durable_session durable_send durable_diagnostics durable_metadata_recovery
)
readonly -a production_companions=(
    durable_server_faults durable_retention_window durable_owner durable_checkpoint_recovery
    durable_retention durable_redelivery durable_capacity durable_bootstrap_security
)
readonly -a fault_companions=(durable_server_faults)

if [[ "$lane" == "production" ]]; then
    targets=("${production_targets[@]}")
    companions=("${production_companions[@]}")
else
    targets=("${fault_targets[@]}")
    companions=("${fault_companions[@]}")
fi

contains() {
    local needle="$1"
    shift
    local value
    for value in "$@"; do
        [[ "$value" == "$needle" ]] && return 0
    done
    return 1
}

declared_test() {
    rtk python3 - "${e2e_root}/Cargo.toml" "$1" <<'PY'
import pathlib
import sys
import tomllib
with open(sys.argv[1], "rb") as handle:
    manifest = tomllib.load(handle)
name = sys.argv[2]
expected = f"tests/{name}.rs"
raise SystemExit(0 if any(item.get("name") == name and item.get("path") == expected for item in manifest.get("test", [])) else 1)
PY
}

validate_source_reachability() {
    local source stem owner
    shopt -s nullglob
    for source in "${e2e_root}"/tests/durable_*.rs; do
        stem="$(rtk basename "${source}" .rs)"
        if ! contains "$stem" "${production_targets[@]}" "${production_companions[@]}" "${fault_targets[@]}" "${fault_companions[@]}"; then
            rtk echo "unreachable v0.7 E2E source: ${source}" >&2
            return 1
        fi
        if contains "$stem" "${production_companions[@]}" "${fault_companions[@]}"; then
            case "$stem" in
                durable_server_faults) owner="durable_send" ;;
                durable_retention_window) owner="durable_shutdown" ;;
                durable_owner) owner="durable_creation" ;;
                durable_checkpoint_recovery) owner="durable_checkpoint" ;;
                durable_retention) owner="durable_poll" ;;
                durable_poll_concurrency) owner="durable_poll" ;;
                durable_replay) owner="durable_delivery" ;;
                durable_redelivery) owner="durable_delivery" ;;
                durable_recreation) owner="durable_compaction" ;;
                durable_capacity) owner="durable_compaction" ;;
                durable_bootstrap_security) owner="durable_diagnostics" ;;
                durable_diagnostic_faults) owner="durable_diagnostics" ;;
                durable_metadata_corruption) owner="durable_metadata_recovery" ;;
                *) return 1 ;;
            esac
            [[ -f "${e2e_root}/tests/${owner}.rs" ]] || {
                rtk echo "companion ${stem} has no materialized primary ${owner}" >&2
                return 1
            }
            rtk grep -Eq "^[[:space:]]*(pub[[:space:]]+)?mod[[:space:]]+${stem}[[:space:]]*;[[:space:]]*$" "${e2e_root}/tests/${owner}.rs" || {
                rtk echo "companion ${stem} is not imported by ${owner}" >&2
                return 1
            }
        fi
    done
}

owned_server_pids() {
    local environment pid
    shopt -s nullglob
    for environment in /proc/[0-9]*/environ; do
        [[ -r "$environment" ]] || continue
        if { rtk tr '\0' '\n' < "$environment"; } 2>/dev/null |
            rtk grep -Fxq "CHIRPS_E2E_RUN_TOKEN=${e2e_run_token}"; then
            pid="${environment#/proc/}"
            pid="${pid%/environ}"
            rtk echo "$pid"
        fi
    done
}

cleanup_owned_servers() {
    local pid iteration
    local -a pids=()
    mapfile -t pids < <(owned_server_pids)
    for pid in "${pids[@]}"; do
        rtk kill -TERM "$pid" 2>/dev/null || true
    done
    for iteration in {1..20}; do
        mapfile -t pids < <(owned_server_pids)
        ((${#pids[@]} == 0)) && return 0
        rtk sleep 0.05
    done
    for pid in "${pids[@]}"; do
        rtk kill -KILL "$pid" 2>/dev/null || true
    done
}

trap cleanup_owned_servers EXIT

run_target() {
    local target="$1"
    contains "$target" "${targets[@]}" || {
        rtk echo "target ${target} is unknown or belongs to the other lane" >&2
        return 1
    }
    [[ -f "${e2e_root}/tests/${target}.rs" ]] || {
        rtk echo "target source is not materialized: ${target}" >&2
        return 1
    }
    declared_test "$target" || {
        rtk echo "target is not explicitly declared in Cargo.toml: ${target}" >&2
        return 1
    }
    if [[ -n "$evidence_dir" ]]; then
        CHIRPS_E2E_RUN_TOKEN="${e2e_run_token}" rtk proxy python3 \
            "${repository_root}/scripts/release/v07_e2e_evidence.py" \
            --repo-root "$repository_root" --output "$evidence_dir" --lane "$lane" --target "$target"
    else
        CHIRPS_E2E_RUN_TOKEN="${e2e_run_token}" rtk cargo test --locked --manifest-path "${repository_root}/Cargo.toml" \
            -p chirps-e2e --test "$target" -- --ignored --nocapture --test-threads=1
    fi
}

validate_source_reachability
case "$mode" in
    target)
        run_target "$selected_target"
        ;;
    materialized)
        materialized_count=0
        for target in "${targets[@]}"; do
            [[ -f "${e2e_root}/tests/${target}.rs" ]] || continue
            ((materialized_count += 1))
            run_target "$target"
        done
        ((materialized_count > 0)) || {
            rtk echo "materialized lane has no primary targets" >&2
            exit 1
        }
        ;;
    strict)
        for target in "${targets[@]}"; do
            [[ -f "${e2e_root}/tests/${target}.rs" ]] || {
                rtk echo "strict lane is missing primary target: ${target}" >&2
                exit 1
            }
            declared_test "$target" || {
                rtk echo "strict lane has an undeclared primary target: ${target}" >&2
                exit 1
            }
        done
        for companion in "${companions[@]}"; do
            [[ -f "${e2e_root}/tests/${companion}.rs" ]] || {
                rtk echo "strict lane is missing companion source: ${companion}" >&2
                exit 1
            }
        done
        for target in "${targets[@]}"; do
            run_target "$target"
        done
        if [[ -n "$evidence_dir" ]]; then
            rtk proxy python3 "${repository_root}/scripts/release/v07_e2e_evidence.py" \
                --repo-root "$repository_root" --output "$evidence_dir" --lane "$lane" --seal-lane
        fi
        ;;
esac
