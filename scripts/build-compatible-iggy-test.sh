#!/usr/bin/env bash
set -euo pipefail

if ! command -v rtk >/dev/null 2>&1; then
    rtk() {
        [[ "${1:-}" != "proxy" ]] || shift
        command "$@"
    }
fi

readonly EXPECTED_SOURCE_COMMIT="336d20c53b4bba663c257bdc0271373cfc2f1864"
readonly EXPECTED_SOURCE_TREE="b2099c2dc404534429e210069990a10496d4fefd"
readonly EXPECTED_LOCK_SHA256="9b601087feed75db7cc6e3e5bbe185fbc1cd5ef9ea2d84dbda8b6a9deb40f6c8"
readonly EXPECTED_TOOLCHAIN_SHA256="c73ceece264a4826462f5e22926b8909955e5c98cd391733846540d4ed9e6f21"
readonly EXPECTED_RUSTC_COMMIT="4a4ef493e3a1488c6e321570238084b38948f6db"
readonly EXPECTED_CARGO_COMMIT="85eff7c80277b57f78b11e28d14154ab12fcf643"
readonly EXPECTED_DOCKERFILE_SHA256="73db335563519f1eb79dc7e2101804e21215615612a07e06cc4df71f200f884e"
readonly SOURCE_DATE_EPOCH="1790115008"
readonly SOURCE_REPOSITORY="${IGGY_SOURCE_DIR:-/home/roomtv/works/alopex-db/iggy-worktrees/v0.7.0-compatible}"
TASK_TARGET=""
readonly LIBRARY_PATH_VALUE="/tmp/chirps-v07-libudev-link"
readonly ARTIFACT_PATH="/home/roomtv/works/alopex-db/release-artifacts/chirps-v0.7.0/server/test/iggy-server"
readonly ARTIFACT_DIR="${ARTIFACT_PATH%/*}"
CARGO_BIN="$(cd "${SOURCE_REPOSITORY}" && rtk proxy rustup which cargo --toolchain 1.94.0)"
RUSTC_BIN="$(cd "${SOURCE_REPOSITORY}" && rtk proxy rustup which rustc --toolchain 1.94.0)"
CARGO_HOME_VALUE="$(cd "${SOURCE_REPOSITORY}" && rtk proxy rustup show home)"
readonly CARGO_BIN RUSTC_BIN CARGO_HOME_VALUE

ROOT="$(cd "$(rtk dirname "${BASH_SOURCE[0]}")/.." && rtk pwd)"
readonly ROOT
source "${ROOT}/scripts/release/owned-target.sh"
readonly MANIFEST="${ROOT}/server/iggy-compatible/test-manifest.toml"
readonly PRODUCTION_MANIFEST="${ROOT}/server/iggy-compatible/manifest.toml"
readonly DOCKERFILE="${ROOT}/server/iggy-compatible/Dockerfile.test"
MODE=""
STAGING=""
OUTPUT_TMP=""

die() {
    rtk echo "build-compatible-iggy-test: $*" >&2
    exit 1
}

require_equal() {
    local label="$1" actual="$2" expected="$3"
    [[ "${actual}" == "${expected}" ]] || die "${label} mismatch: expected ${expected}, got ${actual}"
}

cleanup() {
    local status="$1" cleanup_failed=0
    trap - EXIT HUP INT TERM
    if ! chirps_target_clean "${TASK_TARGET}" rtk env CARGO_HOME="${CARGO_HOME_VALUE}" RUSTC="${RUSTC_BIN}" \
        "${CARGO_BIN}" clean --manifest-path "${SOURCE_REPOSITORY}/Cargo.toml" \
            --target-dir "${TASK_TARGET}" >/dev/null 2>&1; then
        rtk echo "build-compatible-iggy-test: task target cleanup command failed" >&2
        cleanup_failed=1
    fi
    if [[ -e "${TASK_TARGET}" ]]; then
        rtk echo "build-compatible-iggy-test: task target cleanup was incomplete" >&2
        cleanup_failed=1
    fi
    if [[ -n "${STAGING}" ]]; then
        if [[ "${STAGING}" == /tmp/chirps-v07-task-5_18-source.* ]]; then
            if ! rtk rm -rf -- "${STAGING}"; then
                rtk echo "build-compatible-iggy-test: source staging cleanup command failed" >&2
                cleanup_failed=1
            fi
        else
            rtk echo "build-compatible-iggy-test: refusing unexpected source staging path: ${STAGING}" >&2
            cleanup_failed=1
        fi
        if [[ -e "${STAGING}" ]]; then
            rtk echo "build-compatible-iggy-test: source staging cleanup was incomplete" >&2
            cleanup_failed=1
        fi
    fi
    if [[ -n "${OUTPUT_TMP}" ]]; then
        if [[ "${OUTPUT_TMP}" == "${ARTIFACT_DIR}"/.iggy-server.* ]]; then
            if ! rtk rm -f -- "${OUTPUT_TMP}"; then
                rtk echo "build-compatible-iggy-test: artifact temporary cleanup command failed" >&2
                cleanup_failed=1
            fi
        else
            rtk echo "build-compatible-iggy-test: refusing unexpected artifact temporary path: ${OUTPUT_TMP}" >&2
            cleanup_failed=1
        fi
        if [[ -e "${OUTPUT_TMP}" ]]; then
            rtk echo "build-compatible-iggy-test: artifact temporary cleanup was incomplete" >&2
            cleanup_failed=1
        fi
    fi
    STAGING=""
    OUTPUT_TMP=""
    if (( status != 0 )); then
        return "${status}"
    fi
    (( cleanup_failed == 0 ))
}

verify_manifests() {
    rtk env MODE="${MODE}" EXPECTED_DOCKERFILE_SHA256="${EXPECTED_DOCKERFILE_SHA256}" \
        python3 - "${MANIFEST}" "${PRODUCTION_MANIFEST}" "${DOCKERFILE}" <<'PY'
import hashlib
import os
from pathlib import Path
import sys
import tomllib

test_path, production_path, dockerfile_path = map(Path, sys.argv[1:])
test = tomllib.loads(test_path.read_text())
production = tomllib.loads(production_path.read_text())
stages = [
    "message-open-sync", "index-open-sync", "append", "journal-flush",
    "message-sync", "index-sync", "response", "partial-write", "truncate",
    "checksum-failure", "unknown-version", "disk-full", "permission-denied",
    "read-only", "io-timeout", "snapshot-temp-write", "snapshot-file-sync",
    "snapshot-rename", "snapshot-directory-sync", "snapshot-install",
    "snapshot-apply",
]
expected = {
    ("schema_version",): 1,
    ("artifact", "kind"): "publish-disabled-test",
    ("artifact", "publishable"): False,
    ("artifact", "filename"): "iggy-server",
    ("artifact", "output_path"): "/home/roomtv/works/alopex-db/release-artifacts/chirps-v0.7.0/server/test/iggy-server",
    ("source", "repository"): "https://github.com/apache/iggy.git",
    ("source", "baseline_commit"): "f5350d999d883fd3ca9dd33b3dc2754ddb0df049",
    ("source", "commit"): "336d20c53b4bba663c257bdc0271373cfc2f1864",
    ("source", "tree"): "b2099c2dc404534429e210069990a10496d4fefd",
    ("source", "cargo_lock_sha256"): "9b601087feed75db7cc6e3e5bbe185fbc1cd5ef9ea2d84dbda8b6a9deb40f6c8",
    ("source", "clean_required"): True,
    ("toolchain", "channel"): "1.94.0",
    ("toolchain", "rustc_commit"): "4a4ef493e3a1488c6e321570238084b38948f6db",
    ("toolchain", "cargo_commit"): "85eff7c80277b57f78b11e28d14154ab12fcf643",
    ("toolchain", "manifest_sha256"): "c73ceece264a4826462f5e22926b8909955e5c98cd391733846540d4ed9e6f21",
    ("build", "package"): "server",
    ("build", "binary"): "iggy-server",
    ("build", "profile"): "release",
    ("build", "default_features"): False,
    ("build", "features"): ["mimalloc", "chirps-test-failpoints"],
    ("build", "source_date_epoch"): 1790115008,
    ("build", "runtime_build_sha"): "336d20c53b4bba663c257bdc0271373cfc2f1864",
    ("container", "dockerfile"): "server/iggy-compatible/Dockerfile.test",
    ("container", "platform"): "linux/amd64",
    ("container", "runtime_image"): "docker.io/library/debian@sha256:38a76d01668772e381ad2826d876627c89e7133e2f8a0f5d567306798b0f2a16",
    ("container", "runtime_snapshot"): "20260810T000000Z",
    ("failpoints", "environment"): "IGGY_CHIRPS_FAILPOINT",
    ("failpoints", "stages"): stages,
    ("verification", "production_manifest"): "server/iggy-compatible/manifest.toml",
    ("verification", "required_symbols"): ["IGGY_CHIRPS_FAILPOINT"],
    ("verification", "production_forbidden_symbols"): ["IGGY_CHIRPS_FAILPOINT", "ChirpsFailpoint"],
    ("verification", "distinct_production_output"): True,
}
for keys, wanted in expected.items():
    actual = test
    for key in keys:
        actual = actual[key]
    if actual != wanted:
        raise SystemExit(f"test manifest {'.'.join(keys)} mismatch: {actual!r}")

if production["artifact"]["kind"] != "production" or production["artifact"]["publishable"] is not True:
    raise SystemExit("production manifest kind/publishable mismatch")
for section in ("source", "toolchain"):
    if test[section] != production[section]:
        raise SystemExit(f"test and production {section} identities differ")
for key in (
    "package", "binary", "profile", "default_features", "source_date_epoch",
    "runtime_build_sha",
):
    if test["build"][key] != production["build"][key]:
        raise SystemExit(f"test and production build.{key} differ")
production_features = production["build"]["features"]
if production["build"].get("forbidden_features") != ["chirps-test-failpoints"]:
    raise SystemExit("production manifest does not forbid the test failpoint feature")
if test["build"]["features"] != [*production_features, "chirps-test-failpoints"]:
    raise SystemExit("test features must add only chirps-test-failpoints to production features")
for key in ("platform", "runtime_image", "runtime_snapshot"):
    if test["container"][key] != production["container"][key]:
        raise SystemExit(f"test and production container.{key} differ")

test_digest = test["artifact"]["output_sha256"]
production_digest = production["artifact"]["output_sha256"]
for label, digest in (("test", test_digest), ("production", production_digest)):
    if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        raise SystemExit(f"{label} output digest is not a SHA-256")
if test_digest == production_digest:
    raise SystemExit("test and production output digests must differ")
if os.environ["MODE"] == "--verify-stages" and test_digest == "0" * 64:
    raise SystemExit("test output digest has not been fixed")
if len(stages) != len(set(stages)):
    raise SystemExit("test failpoint allowlist contains duplicates")

dockerfile_digest = hashlib.sha256(dockerfile_path.read_bytes()).hexdigest()
if dockerfile_digest != os.environ["EXPECTED_DOCKERFILE_SHA256"]:
    raise SystemExit("actual test Dockerfile digest mismatch")
if test["container"]["dockerfile_sha256"] != os.environ["EXPECTED_DOCKERFILE_SHA256"]:
    raise SystemExit("test Dockerfile digest mismatch")
dockerfile = dockerfile_path.read_text()
from_lines = [
    line.strip()
    for line in dockerfile.splitlines()
    if line.strip() and not line.lstrip().startswith("#") and line.lstrip().upper().startswith("FROM ")
]
expected_from = f'FROM --platform=linux/amd64 {test["container"]["runtime_image"]}'
if from_lines != [expected_from]:
    raise SystemExit(f"test Dockerfile FROM mismatch: {from_lines!r}")
required_fragments = [
    test["container"]["runtime_image"],
    f'org.opencontainers.image.revision="{test["source"]["commit"]}"',
    'org.alopex.chirps.artifact-kind="publish-disabled-test"',
    'org.alopex.chirps.publishable="false"',
    'org.alopex.chirps.failpoints="enabled"',
    f'org.alopex.chirps.server-sha256="{test_digest}"',
]
for fragment in required_fragments:
    if fragment not in dockerfile:
        raise SystemExit(f"test Dockerfile is missing {fragment!r}")
PY
}

verify_registry() {
    rtk python3 - "${MANIFEST}" "${SOURCE_REPOSITORY}" <<'PY'
from pathlib import Path
import re
import sys
import tomllib

manifest = tomllib.load(open(sys.argv[1], "rb"))
source = Path(sys.argv[2])
registry_path = source / "core/common/src/chirps_failpoints.rs"
registry = registry_path.read_text()
all_match = re.search(r"pub const ALL:.*?=\s*\[(.*?)\];", registry, re.S)
if not all_match:
    raise SystemExit("failpoint registry ALL is missing")
variants = re.findall(r"Self::([A-Za-z0-9_]+)", all_match.group(1))
as_str = registry[registry.index("pub const fn as_str"):registry.index("pub fn parse")]
names = dict(re.findall(r'Self::([A-Za-z0-9_]+)\s*=>\s*"([^"]+)"', as_str))
try:
    registry_stages = [names[variant] for variant in variants]
except KeyError as error:
    raise SystemExit(f"failpoint registry lacks a name for {error.args[0]}") from error
if registry_stages != manifest["failpoints"]["stages"]:
    raise SystemExit("manifest stages differ from the ordered Iggy registry")
if len(variants) != 21 or len(set(variants)) != 21:
    raise SystemExit("Iggy registry is not the exact 21-stage allowlist")

gated = r'#\[cfg\(feature\s*=\s*"chirps-test-failpoints"\)\](?!\s*#\[(?:tokio::)?test\])\s*'
within_item = r'(?:(?!#\s*\[cfg).){0,3000}?'

def gated_path(*tokens):
    return gated + within_item + within_item.join(re.escape(token) for token in tokens)

runtime_owners = {
    "message-open-sync": (
        "core/common/src/types/segment_storage/messages_writer.rs",
        gated_path("crate::chirps_failpoints::check(", "ChirpsFailpoint::MessageOpenSync"),
        None,
    ),
    "index-open-sync": (
        "core/common/src/types/segment_storage/index_writer.rs",
        gated_path("crate::chirps_failpoints::check(", "ChirpsFailpoint::IndexOpenSync"),
        None,
    ),
    "append": (
        "core/common/src/types/segment_storage/messages_writer.rs",
        gated_path("crate::chirps_failpoints::check(", "ChirpsFailpoint::Append"),
        None,
    ),
    "journal-flush": (
        "core/server/src/shard/system/messages.rs",
        gated_path("iggy_common::chirps_failpoints::check(", "ChirpsFailpoint::JournalFlush"),
        None,
    ),
    "message-sync": (
        "core/common/src/types/segment_storage/messages_writer.rs",
        gated_path("crate::chirps_failpoints::check(", "ChirpsFailpoint::MessageSync"),
        None,
    ),
    "index-sync": (
        "core/common/src/types/segment_storage/index_writer.rs",
        gated_path("crate::chirps_failpoints::check(", "ChirpsFailpoint::IndexSync"),
        None,
    ),
    "response": (
        "core/server/src/chirps_extension/dispatch.rs",
        gated_path("iggy_common::chirps_failpoints::check(", "ChirpsFailpoint::Response"),
        None,
    ),
    "partial-write": (
        "core/common/src/types/segment_storage/messages_writer.rs",
        gated_path("chirps_failpoints::check(ChirpsFailpoint::PartialWrite)"),
        None,
    ),
    "truncate": (
        "core/server/src/chirps_extension/resource_store.rs",
        gated_path("chirps_failpoints::check(ChirpsFailpoint::Truncate)"),
        None,
    ),
    "checksum-failure": (
        "core/server/src/chirps_extension/resource_store.rs",
        gated_path("chirps_failpoints::check(ChirpsFailpoint::ChecksumFailure)"),
        None,
    ),
    "unknown-version": (
        "core/server/src/chirps_extension/resource_store.rs",
        gated_path("chirps_failpoints::check(ChirpsFailpoint::UnknownVersion)"),
        None,
    ),
}
for name, variant in (
    ("disk-full", "DiskFull"),
    ("permission-denied", "PermissionDenied"),
    ("read-only", "ReadOnly"),
    ("io-timeout", "IoTimeout"),
):
    runtime_owners[name] = (
        "core/common/src/types/segment_storage/messages_writer.rs",
        gated_path(f"ChirpsFailpoint::{variant}", "chirps_failpoints::check(stage)"),
        None,
    )
for name, stage, variant in (
    ("snapshot-temp-write", "TempWrite", "SnapshotTempWrite"),
    ("snapshot-file-sync", "FileSync", "SnapshotFileSync"),
    ("snapshot-rename", "Rename", "SnapshotRename"),
    ("snapshot-directory-sync", "DirectorySync", "SnapshotDirectorySync"),
    ("snapshot-install", "Install", "SnapshotInstall"),
    ("snapshot-apply", "Apply", "SnapshotApply"),
):
    runtime_owners[name] = (
        "core/server/src/chirps_extension/resource_snapshot.rs",
        gated_path("fn check_failpoint", f"SnapshotStage::{stage} => ChirpsFailpoint::{variant}"),
        f"check_failpoint(SnapshotStage::{stage})",
    )

if list(runtime_owners) != manifest["failpoints"]["stages"]:
    raise SystemExit("runtime owner map differs from the ordered failpoint manifest")
for stage, (relative_path, pattern, runtime_call) in runtime_owners.items():
    owner = source / relative_path
    code = re.sub(r"/\*.*?\*/", "", owner.read_text(), flags=re.S)
    code = re.sub(r"//[^\n]*", "", code)
    code = " ".join(code.split())
    if not re.search(pattern, code, re.S):
        raise SystemExit(f"failpoint stage {stage} lacks its gated runtime path in {relative_path}")
    if runtime_call is not None and runtime_call not in code:
        raise SystemExit(f"failpoint stage {stage} lacks its runtime invocation in {relative_path}")
PY
}

manifest_output_sha256() {
    rtk python3 -c 'import sys,tomllib; print(tomllib.load(open(sys.argv[1], "rb"))["artifact"]["output_sha256"])' "${MANIFEST}"
}

verify_task_target() {
    [[ -n "${TASK_TARGET}" ]] || {
        [[ "${MODE}" == "--self-check" ]] && return
        die "task target must be allocated before verification"
    }
    case "${TASK_TARGET}" in
        /tmp/chirps-v07-*-target.*)
            [[ ! -L "${TASK_TARGET}" ]] || die "task target may not be a symlink"
            ;;
        *)
            die "CARGO_TARGET_DIR must be an isolated /tmp/chirps-v07-*-target directory"
            ;;
    esac
}

production_output() {
    rtk python3 -c 'import sys,tomllib; print(tomllib.load(open(sys.argv[1], "rb"))["artifact"]["output_path"])' "${PRODUCTION_MANIFEST}"
}

production_output_sha256() {
    rtk python3 -c 'import sys,tomllib; print(tomllib.load(open(sys.argv[1], "rb"))["artifact"]["output_sha256"])' "${PRODUCTION_MANIFEST}"
}

verify_inputs() {
    verify_task_target
    [[ -z "${IGGY_CHIRPS_FAILPOINT+x}" ]] || die "IGGY_CHIRPS_FAILPOINT must not affect artifact construction"
    [[ -d "${SOURCE_REPOSITORY}/.git" || -f "${SOURCE_REPOSITORY}/.git" ]] || die "Iggy source is not a Git worktree"
    require_equal "source commit" "$(rtk git -C "${SOURCE_REPOSITORY}" rev-parse HEAD^{commit})" "${EXPECTED_SOURCE_COMMIT}"
    require_equal "source tree" "$(rtk git -C "${SOURCE_REPOSITORY}" rev-parse HEAD^{tree})" "${EXPECTED_SOURCE_TREE}"
    [[ -z "$(rtk proxy git -C "${SOURCE_REPOSITORY}" status --porcelain=v1 --untracked-files=all)" ]] || die "Iggy source worktree is dirty"
    require_equal "Cargo.lock digest" "$(rtk sha256sum "${SOURCE_REPOSITORY}/Cargo.lock" | rtk awk '{print $1}')" "${EXPECTED_LOCK_SHA256}"
    require_equal "toolchain manifest digest" "$(rtk sha256sum "${SOURCE_REPOSITORY}/rust-toolchain.toml" | rtk awk '{print $1}')" "${EXPECTED_TOOLCHAIN_SHA256}"
    ! rtk grep -Eq '(^|[^[:alnum:]_-])(latest|main|master)([^[:alnum:]_-]|$)' "${DOCKERFILE}" "${MANIFEST}" || die "mutable artifact input found"
    ! rtk grep -Eq 'COPY .*[*.]|COPY[[:space:]]+\.|ADD[[:space:]]' "${DOCKERFILE}" || die "Dockerfile may copy only the attested server binary"
    verify_manifests
    verify_registry
}

verify_toolchain() {
    local cargo_commit rustc_commit
    rustc_commit="$(rtk "${RUSTC_BIN}" -Vv | rtk sed -n 's/^commit-hash: //p')"
    cargo_commit="$(rtk "${CARGO_BIN}" -Vv | rtk sed -n 's/^commit-hash: //p')"
    require_equal "rustc commit" "${rustc_commit}" "${EXPECTED_RUSTC_COMMIT}"
    require_equal "cargo commit" "${cargo_commit}" "${EXPECTED_CARGO_COMMIT}"
}

build_test_binary() {
    local source_dir="$1"
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
                --no-default-features --features mimalloc,chirps-test-failpoints
        rtk proxy env CARGO_TARGET_DIR="${TASK_TARGET}" CARGO_HOME="${CARGO_HOME_VALUE}" \
            RUSTC="${RUSTC_BIN}" \
            "${CARGO_BIN}" tree --locked --offline -p server -e features -i iggy_common \
                --no-default-features --features mimalloc,chirps-test-failpoints >"${TASK_TARGET}/features.txt"
    )
    rtk grep -Fq 'iggy_common feature "chirps-test-failpoints"' "${TASK_TARGET}/features.txt" \
        || die "test failpoint feature is absent from iggy_common"
    rtk grep -Fq 'server feature "chirps-test-failpoints"' "${TASK_TARGET}/features.txt" \
        || die "test failpoint feature is absent from server"
    rtk python3 - "${MANIFEST}" "${TASK_TARGET}/release/iggy-server" <<'PY'
import sys
import tomllib

manifest = tomllib.load(open(sys.argv[1], "rb"))
binary = open(sys.argv[2], "rb").read()
needles = [manifest["failpoints"]["environment"], *manifest["failpoints"]["stages"]]
missing = [needle for needle in needles if needle.encode() not in binary]
if missing:
    raise SystemExit(f"test binary lacks failpoint controls: {', '.join(missing)}")
PY
}

verify_production_control() {
    local production_path production_digest symbol
    production_path="$(production_output)"
    production_digest="$(production_output_sha256)"
    [[ -f "${production_path}" && ! -L "${production_path}" ]] || die "production control artifact is missing or a symlink"
    require_equal "production output digest" "$(rtk sha256sum "${production_path}" | rtk awk '{print $1}')" "${production_digest}"
    for symbol in IGGY_CHIRPS_FAILPOINT ChirpsFailpoint; do
        ! rtk grep -aFq "${symbol}" "${production_path}" || die "production artifact contains ${symbol}"
    done
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

verify_stages() {
    local actual_digest expected_digest production_digest
    trap 'status=$?; set +e; cleanup "${status}"; exit $?' EXIT
    trap 'exit 129' HUP
    trap 'exit 130' INT
    trap 'exit 143' TERM
    STAGING="$(rtk mktemp -d /tmp/chirps-v07-task-5_18-source.XXXXXX)"
    rtk mkdir -p "${STAGING}/source"
    rtk proxy git -C "${SOURCE_REPOSITORY}" archive "${EXPECTED_SOURCE_COMMIT}" | rtk proxy tar -x -C "${STAGING}/source"
    chirps_target_clean "${TASK_TARGET}" rtk env CARGO_HOME="${CARGO_HOME_VALUE}" RUSTC="${RUSTC_BIN}" \
        "${CARGO_BIN}" clean --manifest-path "${SOURCE_REPOSITORY}/Cargo.toml" \
            --target-dir "${TASK_TARGET}" >/dev/null
    [[ ! -e "${TASK_TARGET}" ]] || die "pre-build target clean was incomplete"
    build_test_binary "${STAGING}/source"
    actual_digest="$(rtk sha256sum "${TASK_TARGET}/release/iggy-server" | rtk awk '{print $1}')"
    expected_digest="$(manifest_output_sha256)"
    production_digest="$(production_output_sha256)"
    require_equal "test output digest" "${actual_digest}" "${expected_digest}"
    [[ "${actual_digest}" != "${production_digest}" ]] || die "test and production output digests are identical"
    verify_production_control
    install_artifact "${TASK_TARGET}/release/iggy-server" "${actual_digest}"
    cleanup 0
    rtk echo "publish-disabled test artifact verified: ${ARTIFACT_PATH} sha256:${actual_digest}"
}

case "${1:-}" in
    --self-check)
        [[ "$#" -eq 1 ]] || die "usage: $0 --self-check"
        MODE="--self-check"
        ;;
    --verify-stages)
        [[ "$#" -eq 1 ]] || die "usage: $0 --verify-stages"
        MODE="--verify-stages"
        ;;
    --push|--publish)
        die "publication is forbidden for the test artifact"
        ;;
    *)
        die "usage: $0 --self-check | --verify-stages"
        ;;
esac

if [[ "${MODE}" == "--verify-stages" ]]; then
    TASK_TARGET="$(rtk mktemp -d /tmp/chirps-v07-task-5_18-target.XXXXXX)"
fi

verify_inputs
if [[ "${MODE}" == "--self-check" ]]; then
    rtk echo "publish-disabled test artifact inputs are statically valid"
    exit 0
fi
verify_toolchain
verify_stages
