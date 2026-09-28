#!/usr/bin/env bash
set -euo pipefail

if ! command -v rtk >/dev/null 2>&1; then
    rtk() {
        [[ "${1:-}" != "proxy" ]] || shift
        command "$@"
    }
fi

readonly EXPECTED_BASELINE_COMMIT="f5350d999d883fd3ca9dd33b3dc2754ddb0df049"
readonly EXPECTED_SOURCE_COMMIT="336d20c53b4bba663c257bdc0271373cfc2f1864"
readonly EXPECTED_SOURCE_TREE="b2099c2dc404534429e210069990a10496d4fefd"
readonly EXPECTED_LOCK_SHA256="9b601087feed75db7cc6e3e5bbe185fbc1cd5ef9ea2d84dbda8b6a9deb40f6c8"
readonly EXPECTED_TOOLCHAIN_SHA256="c73ceece264a4826462f5e22926b8909955e5c98cd391733846540d4ed9e6f21"
readonly EXPECTED_RUSTC_COMMIT="4a4ef493e3a1488c6e321570238084b38948f6db"
readonly EXPECTED_CARGO_COMMIT="85eff7c80277b57f78b11e28d14154ab12fcf643"
readonly EXPECTED_RUNTIME_IMAGE="docker.io/library/debian@sha256:38a76d01668772e381ad2826d876627c89e7133e2f8a0f5d567306798b0f2a16"
readonly EXPECTED_DOCKERFILE_SHA256="04adcdf8441d27d97ec0074cb78ad424d0253ab48d80ac7fc76b311345bdad71"
readonly SOURCE_DATE_EPOCH="1790115008"
TASK_TARGET=""
readonly SOURCE_REPOSITORY="${IGGY_SOURCE_DIR:-/home/roomtv/works/alopex-db/iggy-worktrees/v0.7.0-compatible}"
readonly ARTIFACT_PATH="/home/roomtv/works/alopex-db/release-artifacts/chirps-v0.7.0/server/production/iggy-server"
readonly ARTIFACT_DIR="${ARTIFACT_PATH%/*}"
readonly LIBRARY_PATH_VALUE="/tmp/chirps-v07-libudev-link"
CARGO_BIN="$(cd "${SOURCE_REPOSITORY}" && rtk proxy rustup which cargo --toolchain 1.94.0)"
RUSTC_BIN="$(cd "${SOURCE_REPOSITORY}" && rtk proxy rustup which rustc --toolchain 1.94.0)"
CARGO_HOME_VALUE="$(cd "${SOURCE_REPOSITORY}" && rtk proxy rustup show home)"
readonly CARGO_BIN RUSTC_BIN CARGO_HOME_VALUE

ROOT="$(cd "$(rtk dirname "${BASH_SOURCE[0]}")/.." && rtk pwd)"
readonly ROOT
readonly MANIFEST="${ROOT}/server/iggy-compatible/manifest.toml"
readonly DOCKERFILE="${ROOT}/server/iggy-compatible/Dockerfile"
STAGING=""
OUTPUT_TMP=""

die() {
    rtk echo "build-compatible-iggy: $*" >&2
    exit 1
}

require_equal() {
    local label="$1" actual="$2" expected="$3"
    [[ "${actual}" == "${expected}" ]] || die "${label} mismatch: expected ${expected}, got ${actual}"
}

cleanup() {
    local status="$1" cleanup_failed=0
    trap - EXIT HUP INT TERM
    if [[ -n "${TASK_TARGET}" ]]; then
        if ! rtk env CARGO_HOME="${CARGO_HOME_VALUE}" RUSTC="${RUSTC_BIN}" \
            "${CARGO_BIN}" clean --manifest-path "${SOURCE_REPOSITORY}/Cargo.toml" \
                --target-dir "${TASK_TARGET}" >/dev/null; then
            rtk echo "build-compatible-iggy: cargo cleanup failed" >&2
            cleanup_failed=1
        fi
        if [[ -d "${TASK_TARGET}" ]] && ! rtk rmdir "${TASK_TARGET}"; then
            rtk echo "build-compatible-iggy: task target remains non-empty" >&2
            cleanup_failed=1
        fi
        if [[ -e "${TASK_TARGET}" ]]; then
            rtk echo "build-compatible-iggy: task target cleanup was incomplete" >&2
            cleanup_failed=1
        fi
        TASK_TARGET=""
    fi
    if [[ "${STAGING}" == /tmp/chirps-v07-task-5_17-source.* ]]; then
        if ! rtk rm -rf -- "${STAGING}"; then
            rtk echo "build-compatible-iggy: source staging removal failed" >&2
            cleanup_failed=1
        fi
    fi
    if [[ -n "${STAGING}" && -e "${STAGING}" ]]; then
        rtk echo "build-compatible-iggy: source staging cleanup was incomplete" >&2
        cleanup_failed=1
    fi
    if [[ -n "${OUTPUT_TMP}" ]]; then
        if ! rtk rm -f -- "${OUTPUT_TMP}" || [[ -e "${OUTPUT_TMP}" ]]; then
            rtk echo "build-compatible-iggy: output staging cleanup was incomplete" >&2
            cleanup_failed=1
        fi
    fi
    STAGING=""
    OUTPUT_TMP=""
    if [[ "${cleanup_failed}" -ne 0 && "${status}" -eq 0 ]]; then
        status=1
    fi
    return "${status}"
}

on_exit() {
    local status="$?" final_status
    if cleanup "${status}"; then
        final_status=0
    else
        final_status=$?
    fi
    exit "${final_status}"
}

verify_manifest() {
    rtk env \
        EXPECTED_BASELINE_COMMIT="${EXPECTED_BASELINE_COMMIT}" \
        EXPECTED_SOURCE_COMMIT="${EXPECTED_SOURCE_COMMIT}" \
        EXPECTED_SOURCE_TREE="${EXPECTED_SOURCE_TREE}" \
        EXPECTED_LOCK_SHA256="${EXPECTED_LOCK_SHA256}" \
        EXPECTED_TOOLCHAIN_SHA256="${EXPECTED_TOOLCHAIN_SHA256}" \
        EXPECTED_RUSTC_COMMIT="${EXPECTED_RUSTC_COMMIT}" \
        EXPECTED_CARGO_COMMIT="${EXPECTED_CARGO_COMMIT}" \
        EXPECTED_RUNTIME_IMAGE="${EXPECTED_RUNTIME_IMAGE}" \
        EXPECTED_DOCKERFILE_SHA256="${EXPECTED_DOCKERFILE_SHA256}" \
        EXPECTED_ARTIFACT_PATH="${ARTIFACT_PATH}" \
        EXPECTED_SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH}" \
        python3 - "${MANIFEST}" "${DOCKERFILE}" <<'PY'
import hashlib
import os
from pathlib import Path
import sys
import tomllib

manifest_path, dockerfile_path = map(Path, sys.argv[1:])
manifest = tomllib.loads(manifest_path.read_text())
expected = {
    ("schema_version",): 1,
    ("artifact", "kind"): "production",
    ("artifact", "publishable"): True,
    ("artifact", "filename"): "iggy-server",
    ("artifact", "output_path"): os.environ["EXPECTED_ARTIFACT_PATH"],
    ("source", "repository"): "https://github.com/apache/iggy.git",
    ("source", "baseline_commit"): os.environ["EXPECTED_BASELINE_COMMIT"],
    ("source", "commit"): os.environ["EXPECTED_SOURCE_COMMIT"],
    ("source", "tree"): os.environ["EXPECTED_SOURCE_TREE"],
    ("source", "cargo_lock_sha256"): os.environ["EXPECTED_LOCK_SHA256"],
    ("source", "clean_required"): True,
    ("toolchain", "channel"): "1.94.0",
    ("toolchain", "rustc_commit"): os.environ["EXPECTED_RUSTC_COMMIT"],
    ("toolchain", "cargo_commit"): os.environ["EXPECTED_CARGO_COMMIT"],
    ("toolchain", "manifest_sha256"): os.environ["EXPECTED_TOOLCHAIN_SHA256"],
    ("build", "package"): "server",
    ("build", "binary"): "iggy-server",
    ("build", "profile"): "release",
    ("build", "default_features"): False,
    ("build", "features"): ["mimalloc"],
    ("build", "forbidden_features"): ["chirps-test-failpoints"],
    ("build", "source_date_epoch"): int(os.environ["EXPECTED_SOURCE_DATE_EPOCH"]),
    ("build", "runtime_build_sha"): os.environ["EXPECTED_SOURCE_COMMIT"],
    ("container", "dockerfile"): "server/iggy-compatible/Dockerfile",
    ("container", "platform"): "linux/amd64",
    ("container", "runtime_image"): os.environ["EXPECTED_RUNTIME_IMAGE"],
    ("container", "runtime_snapshot"): "20260810T000000Z",
    ("verification", "forbidden_environment"): "IGGY_CHIRPS_FAILPOINT",
    ("verification", "forbidden_symbols"): ["IGGY_CHIRPS_FAILPOINT", "ChirpsFailpoint"],
    ("verification", "repeat_build"): "full-target-clean",
}
for keys, value in expected.items():
    actual = manifest
    for key in keys:
        actual = actual[key]
    if actual != value:
        raise SystemExit(f"manifest {'.'.join(keys)} mismatch: {actual!r}")

output_digest = manifest["artifact"]["output_sha256"]
if len(output_digest) != 64 or any(c not in "0123456789abcdef" for c in output_digest):
    raise SystemExit("manifest artifact.output_sha256 is not a SHA-256")
dockerfile_digest = hashlib.sha256(dockerfile_path.read_bytes()).hexdigest()
if dockerfile_digest != os.environ["EXPECTED_DOCKERFILE_SHA256"]:
    raise SystemExit("actual Dockerfile digest mismatch")
if manifest["container"]["dockerfile_sha256"] != os.environ["EXPECTED_DOCKERFILE_SHA256"]:
    raise SystemExit("manifest container.dockerfile_sha256 mismatch")
dockerfile = dockerfile_path.read_text()
from_lines = [
    line.strip()
    for line in dockerfile.splitlines()
    if line.strip() and not line.lstrip().startswith("#") and line.lstrip().upper().startswith("FROM ")
]
expected_from = f'FROM --platform=linux/amd64 {os.environ["EXPECTED_RUNTIME_IMAGE"]}'
if from_lines != [expected_from]:
    raise SystemExit(f"Dockerfile FROM mismatch: {from_lines!r}")
if f'org.opencontainers.image.revision="{os.environ["EXPECTED_SOURCE_COMMIT"]}"' not in dockerfile:
    raise SystemExit("Dockerfile does not fix the source revision label")
if f'org.alopex.chirps.server-sha256="{output_digest}"' not in dockerfile:
    raise SystemExit("Dockerfile does not fix the output digest label")
PY
}

manifest_output_sha256() {
    rtk python3 -c 'import sys,tomllib; print(tomllib.load(open(sys.argv[1], "rb"))["artifact"]["output_sha256"])' "${MANIFEST}"
}

verify_inputs() {
    [[ -z "${IGGY_CHIRPS_FAILPOINT+x}" ]] || die "IGGY_CHIRPS_FAILPOINT is forbidden for production artifacts"
    [[ -d "${SOURCE_REPOSITORY}/.git" || -f "${SOURCE_REPOSITORY}/.git" ]] || die "Iggy source is not a Git worktree"
    require_equal "source commit" "$(rtk git -C "${SOURCE_REPOSITORY}" rev-parse HEAD^{commit})" "${EXPECTED_SOURCE_COMMIT}"
    require_equal "source tree" "$(rtk git -C "${SOURCE_REPOSITORY}" rev-parse HEAD^{tree})" "${EXPECTED_SOURCE_TREE}"
    [[ -z "$(rtk proxy git -C "${SOURCE_REPOSITORY}" status --porcelain=v1 --untracked-files=all)" ]] || die "Iggy source worktree is dirty"
    require_equal "Cargo.lock digest" "$(rtk sha256sum "${SOURCE_REPOSITORY}/Cargo.lock" | rtk awk '{print $1}')" "${EXPECTED_LOCK_SHA256}"
    require_equal "toolchain manifest digest" "$(rtk sha256sum "${SOURCE_REPOSITORY}/rust-toolchain.toml" | rtk awk '{print $1}')" "${EXPECTED_TOOLCHAIN_SHA256}"
    ! rtk grep -Eq '(^|[^[:alnum:]_-])(latest|main|master)([^[:alnum:]_-]|$)' "${DOCKERFILE}" "${MANIFEST}" || die "mutable artifact input found"
    ! rtk grep -Eq 'COPY .*[*.]|COPY[[:space:]]+\.|ADD[[:space:]]' "${DOCKERFILE}" || die "Dockerfile may copy only the attested server binary"
    verify_manifest
}

build_once() {
    local source_dir="$1" digest_file="$2"
    (
        cd "${source_dir}"
        rtk env \
            CARGO_TARGET_DIR="${TASK_TARGET}" \
            CARGO_HOME="${CARGO_HOME_VALUE}" \
            LIBRARY_PATH="${LIBRARY_PATH_VALUE}" \
            SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH}" \
            VERGEN_GIT_SHA="${EXPECTED_SOURCE_COMMIT}" \
            CARGO_INCREMENTAL=0 \
            RUSTC="${RUSTC_BIN}" \
            RUSTFLAGS="--remap-path-prefix=${source_dir}=/usr/src/iggy -C link-arg=-Wl,--build-id=none" \
            "${CARGO_BIN}" build --locked --offline --release -p server --bin iggy-server \
                --no-default-features --features mimalloc
        rtk proxy env CARGO_TARGET_DIR="${TASK_TARGET}" CARGO_HOME="${CARGO_HOME_VALUE}" \
            RUSTC="${RUSTC_BIN}" \
            "${CARGO_BIN}" tree --locked --offline -p server -e features \
                --no-default-features --features mimalloc >"${TASK_TARGET}/features.txt"
    )
    ! rtk grep -q 'chirps-test-failpoints' "${TASK_TARGET}/features.txt" || die "test failpoint feature is enabled"
    for symbol in IGGY_CHIRPS_FAILPOINT ChirpsFailpoint; do
        ! rtk grep -aFq "${symbol}" "${TASK_TARGET}/release/iggy-server" || die "test failpoint symbol ${symbol} is present"
    done
    rtk grep -aFq "${EXPECTED_SOURCE_COMMIT}" "${TASK_TARGET}/release/iggy-server" || die "runtime build SHA is absent"
    rtk sha256sum "${TASK_TARGET}/release/iggy-server" | rtk awk '{print $1}' >"${digest_file}"
}

verify_toolchain() {
    local cargo_commit rustc_commit
    rustc_commit="$(rtk "${RUSTC_BIN}" -Vv | rtk sed -n 's/^commit-hash: //p')"
    cargo_commit="$(rtk "${CARGO_BIN}" -Vv | rtk sed -n 's/^commit-hash: //p')"
    require_equal "rustc commit" "${rustc_commit}" "${EXPECTED_RUSTC_COMMIT}"
    require_equal "cargo commit" "${cargo_commit}" "${EXPECTED_CARGO_COMMIT}"
}

install_artifact() {
    local source_binary="$1" digest="$2"
    [[ ! -L "${ARTIFACT_DIR}" ]] || die "artifact directory may not be a symlink"
    rtk mkdir -p "${ARTIFACT_DIR}"
    require_equal "artifact directory" "$(rtk realpath --canonicalize-existing "${ARTIFACT_DIR}")" "${ARTIFACT_DIR}"
    [[ ! -L "${ARTIFACT_PATH}" ]] || die "artifact output may not be a symlink"
    if [[ -e "${ARTIFACT_PATH}" ]]; then
        require_equal "existing artifact digest" "$(rtk sha256sum "${ARTIFACT_PATH}" | rtk awk '{print $1}')" "${digest}"
        return
    fi
    OUTPUT_TMP="$(rtk mktemp "${ARTIFACT_DIR}/.iggy-server.XXXXXX")"
    rtk cp --no-dereference "${source_binary}" "${OUTPUT_TMP}"
    rtk chmod 0755 "${OUTPUT_TMP}"
    require_equal "install candidate digest" "$(rtk sha256sum "${OUTPUT_TMP}" | rtk awk '{print $1}')" "${digest}"
    rtk mv -T "${OUTPUT_TMP}" "${ARTIFACT_PATH}"
    OUTPUT_TMP=""
}

verify_reproducible() {
    local first_digest second_digest expected_digest
    trap on_exit EXIT
    trap 'exit 129' HUP
    trap 'exit 130' INT
    trap 'exit 143' TERM
    TASK_TARGET="$(rtk mktemp -d /tmp/chirps-v07-task-5_17-target.XXXXXX)"
    STAGING="$(rtk mktemp -d /tmp/chirps-v07-task-5_17-source.XXXXXX)"
    rtk mkdir -p "${STAGING}/source"
    rtk proxy git -C "${SOURCE_REPOSITORY}" archive "${EXPECTED_SOURCE_COMMIT}" | rtk proxy tar -x -C "${STAGING}/source"

    build_once "${STAGING}/source" "${STAGING}/first.sha256"
    first_digest="$(<"${STAGING}/first.sha256")"
    rtk cp --no-dereference "${TASK_TARGET}/release/iggy-server" "${STAGING}/iggy-server.first"
    rtk env CARGO_HOME="${CARGO_HOME_VALUE}" RUSTC="${RUSTC_BIN}" \
        "${CARGO_BIN}" clean --manifest-path "${SOURCE_REPOSITORY}/Cargo.toml" \
            --target-dir "${TASK_TARGET}" >/dev/null
    [[ ! -e "${TASK_TARGET}" ]] || die "full target clean did not remove the first build"
    build_once "${STAGING}/source" "${STAGING}/second.sha256"
    second_digest="$(<"${STAGING}/second.sha256")"
    rtk cp --no-dereference "${TASK_TARGET}/release/iggy-server" "${STAGING}/iggy-server"

    require_equal "repeat output digest" "${second_digest}" "${first_digest}"
    rtk echo "repeat output digests: first=${first_digest} second=${second_digest}"
    expected_digest="$(manifest_output_sha256)"
    require_equal "manifest output digest" "${second_digest}" "${expected_digest}"
    require_equal "first saved output digest" "$(rtk sha256sum "${STAGING}/iggy-server.first" | rtk awk '{print $1}')" "${first_digest}"
    install_artifact "${STAGING}/iggy-server" "${second_digest}"
    cleanup 0
    rtk echo "production artifact verified: ${ARTIFACT_PATH} sha256:${second_digest}"
}

[[ "${1:-}" == "--verify-reproducible" && "$#" -eq 1 ]] || die "usage: $0 --verify-reproducible"
verify_inputs
verify_toolchain
verify_reproducible
