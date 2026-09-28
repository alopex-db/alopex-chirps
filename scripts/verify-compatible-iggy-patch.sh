#!/usr/bin/env bash
set -euo pipefail

if ! command -v rtk >/dev/null 2>&1; then
    rtk() {
        [[ "${1:-}" != "proxy" ]] || shift
        command "$@"
    }
fi

readonly EXPECTED_BASELINE_COMMIT="f5350d999d883fd3ca9dd33b3dc2754ddb0df049"
readonly EXPECTED_SOURCE_COMMIT="76dcccf24b27c61a9434a79b3f81f065c4d3a832"
readonly EXPECTED_PARENT_COMMIT="f0a629bfd3ea7a56c563f6587e1d4ac4a6512640"
readonly EXPECTED_SOURCE_TREE="c63cdecf5edb11650e7db85db3f7ad168a2103f7"
readonly EXPECTED_BUNDLE_REF="refs/heads/chirps/v0.7.0-compatible"
readonly EXPECTED_BUNDLE_SHA256="ef3a0137841b12032b081c47c80a5cfa2b168d8605082132e4c3b1cf7e42d6c0"
readonly EXPECTED_DIFF_SHA256="f7379205b55e77f2bb0738745537057c3a4d02cda0c816bc9a0f54cca498467d"
readonly EXPECTED_LOCK_SHA256="0e4ac6717cfb6ba04894f734b8f56afc56e265925fdd805242b6e39d4d676b41"
readonly EXPECTED_TOOLCHAIN_SHA256="c73ceece264a4826462f5e22926b8909955e5c98cd391733846540d4ed9e6f21"
readonly EXPECTED_PRODUCTION_MANIFEST_SHA256="a18d6fb6bf0a0d3662176dcd0fde91f0e49989c53bc6cffffd21d60a19958ca0"
readonly EXPECTED_TEST_MANIFEST_SHA256="1efa54339d9a07372419bc908885a1872456941d589ccecccda4baa65e87c7e1"

ROOT="$(cd "$(rtk dirname "${BASH_SOURCE[0]}")/.." && rtk pwd)"
readonly ROOT
readonly EXPECTED_MANIFEST="${ROOT}/server/iggy-compatible/manifest.toml"
readonly EXPECTED_SERIES="${ROOT}/server/iggy-compatible/patches/series.toml"
IGGY_REPOSITORY="${IGGY_SOURCE_DIR:-/home/roomtv/works/alopex-db/iggy-worktrees/v0.7.0-compatible}"
MANIFEST=""
SERIES=""
STAGING=""
CLEAN_CHECKOUT=0
SOURCE_RECONSTRUCTION=0
REQUIRE_IGGY_CLEAN=0
ALLOW_CHIRPS_SCOPED_DIRTY=0
REQUIRE_BOTH_CLEAN=0

die() {
    rtk echo "verify-compatible-iggy-patch: $*" >&2
    exit 1
}

usage() {
    die "usage: $0 (--source-reconstruction --require-iggy-clean | --clean-checkout (--require-iggy-clean --allow-chirps-scoped-dirty | --require-both-clean)) --manifest PATH --series PATH"
}

require_equal() {
    local label="$1" actual="$2" expected="$3"
    [[ "${actual}" == "${expected}" ]] || die "${label} mismatch: expected ${expected}, got ${actual}"
}

cleanup() {
    local status="$1" cleanup_failed=0
    trap - EXIT HUP INT TERM
    if [[ -n "${STAGING}" ]]; then
        if [[ "${STAGING}" == /tmp/chirps-v07-task-5_19-source.* && ! -L "${STAGING}" ]]; then
            if ! rtk rm -rf -- "${STAGING}"; then
                rtk echo "verify-compatible-iggy-patch: reconstruction staging cleanup failed" >&2
                cleanup_failed=1
            fi
        else
            rtk echo "verify-compatible-iggy-patch: refusing unexpected staging path: ${STAGING}" >&2
            cleanup_failed=1
        fi
        if [[ -e "${STAGING}" || -L "${STAGING}" ]]; then
            rtk echo "verify-compatible-iggy-patch: reconstruction staging remains" >&2
            cleanup_failed=1
        fi
    fi
    STAGING=""
    if (( status != 0 )); then
        return "${status}"
    fi
    (( cleanup_failed == 0 ))
}

on_exit() {
    local status="$?" final_status
    set +e
    if cleanup "${status}"; then
        final_status=0
    else
        final_status=$?
    fi
    exit "${final_status}"
}

require_clean_repository() {
    local label="$1" repository="$2" status
    status="$(rtk proxy git -C "${repository}" status --porcelain=v1 --untracked-files=all)"
    [[ -z "${status}" ]] || die "${label} repository is dirty"
}

require_chirps_scoped_dirty() {
    local status line path
    status="$(rtk proxy git -C "${ROOT}" status --porcelain=v1 --untracked-files=all)"
    while IFS= read -r line; do
        [[ -n "${line}" ]] || continue
        [[ "${#line}" -ge 4 ]] || die "invalid Chirps status entry"
        path="${line:3}"
        case "${path}" in
            scripts/build-compatible-iggy.sh|\
            scripts/build-compatible-iggy-test.sh|\
            scripts/verify-compatible-iggy-patch.sh|\
            server/iggy-compatible/Dockerfile|\
            server/iggy-compatible/Dockerfile.test|\
            server/iggy-compatible/manifest.toml|\
            server/iggy-compatible/test-manifest.toml|\
            server/iggy-compatible/patches/series.toml)
                ;;
            *)
                die "Chirps dirty path is outside Tasks 5.17-5.19: ${path}"
                ;;
        esac
    done <<<"${status}"
}

while (( $# > 0 )); do
    case "$1" in
        --clean-checkout)
            (( CLEAN_CHECKOUT == 0 )) || usage
            CLEAN_CHECKOUT=1
            shift
            ;;
        --source-reconstruction)
            (( SOURCE_RECONSTRUCTION == 0 )) || usage
            SOURCE_RECONSTRUCTION=1
            shift
            ;;
        --require-iggy-clean)
            (( REQUIRE_IGGY_CLEAN == 0 )) || usage
            REQUIRE_IGGY_CLEAN=1
            shift
            ;;
        --allow-chirps-scoped-dirty)
            (( ALLOW_CHIRPS_SCOPED_DIRTY == 0 )) || usage
            ALLOW_CHIRPS_SCOPED_DIRTY=1
            shift
            ;;
        --require-both-clean)
            (( REQUIRE_BOTH_CLEAN == 0 )) || usage
            REQUIRE_BOTH_CLEAN=1
            shift
            ;;
        --manifest)
            [[ -z "${MANIFEST}" && $# -ge 2 ]] || usage
            MANIFEST="$2"
            shift 2
            ;;
        --series)
            [[ -z "${SERIES}" && $# -ge 2 ]] || usage
            SERIES="$2"
            shift 2
            ;;
        *)
            usage
            ;;
    esac
done

[[ -n "${MANIFEST}" && -n "${SERIES}" ]] || usage
if (( SOURCE_RECONSTRUCTION == 1 )); then
    (( CLEAN_CHECKOUT == 0 && REQUIRE_IGGY_CLEAN == 1 && ALLOW_CHIRPS_SCOPED_DIRTY == 0 && REQUIRE_BOTH_CLEAN == 0 )) || usage
else
    (( CLEAN_CHECKOUT == 1 )) || usage
    if (( REQUIRE_BOTH_CLEAN == 1 )); then
        (( REQUIRE_IGGY_CLEAN == 0 && ALLOW_CHIRPS_SCOPED_DIRTY == 0 )) || usage
    else
        (( REQUIRE_IGGY_CLEAN == 1 && ALLOW_CHIRPS_SCOPED_DIRTY == 1 )) || usage
    fi
fi

[[ -f "${MANIFEST}" && ! -L "${MANIFEST}" ]] || die "production manifest is missing or a symlink"
[[ -f "${SERIES}" && ! -L "${SERIES}" ]] || die "patch series is missing or a symlink"
MANIFEST="$(rtk realpath --canonicalize-existing "${MANIFEST}")"
SERIES="$(rtk realpath --canonicalize-existing "${SERIES}")"
require_equal "production manifest path" "${MANIFEST}" "${EXPECTED_MANIFEST}"
require_equal "patch series path" "${SERIES}" "${EXPECTED_SERIES}"

[[ -d "${IGGY_REPOSITORY}" && ! -L "${IGGY_REPOSITORY}" ]] || die "Iggy repository is missing or a symlink"
IGGY_REPOSITORY="$(rtk realpath --canonicalize-existing "${IGGY_REPOSITORY}")"
require_equal "Chirps repository root" \
    "$(rtk proxy git -C "${ROOT}" rev-parse --show-toplevel)" "${ROOT}"
require_equal "Iggy repository root" \
    "$(rtk proxy git -C "${IGGY_REPOSITORY}" rev-parse --show-toplevel)" "${IGGY_REPOSITORY}"
require_equal "Iggy source commit" \
    "$(rtk proxy git -C "${IGGY_REPOSITORY}" rev-parse HEAD^{commit})" "${EXPECTED_SOURCE_COMMIT}"
require_equal "Iggy source tree" \
    "$(rtk proxy git -C "${IGGY_REPOSITORY}" rev-parse HEAD^{tree})" "${EXPECTED_SOURCE_TREE}"

require_clean_repository "Iggy" "${IGGY_REPOSITORY}"
if (( REQUIRE_BOTH_CLEAN == 1 )); then
    require_clean_repository "Chirps" "${ROOT}"
elif (( SOURCE_RECONSTRUCTION == 0 )); then
    require_chirps_scoped_dirty
fi

trap on_exit EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
STAGING="$(rtk mktemp -d /tmp/chirps-v07-task-5_19-source.XXXXXX)"
readonly BUNDLE="${STAGING}/compatible.bundle"
readonly RECONSTRUCTED="${STAGING}/repository"

rtk env \
    EXPECTED_BASELINE_COMMIT="${EXPECTED_BASELINE_COMMIT}" \
    EXPECTED_SOURCE_COMMIT="${EXPECTED_SOURCE_COMMIT}" \
    EXPECTED_PARENT_COMMIT="${EXPECTED_PARENT_COMMIT}" \
    EXPECTED_SOURCE_TREE="${EXPECTED_SOURCE_TREE}" \
    EXPECTED_BUNDLE_REF="${EXPECTED_BUNDLE_REF}" \
    EXPECTED_BUNDLE_SHA256="${EXPECTED_BUNDLE_SHA256}" \
    EXPECTED_DIFF_SHA256="${EXPECTED_DIFF_SHA256}" \
    EXPECTED_LOCK_SHA256="${EXPECTED_LOCK_SHA256}" \
    EXPECTED_TOOLCHAIN_SHA256="${EXPECTED_TOOLCHAIN_SHA256}" \
    EXPECTED_PRODUCTION_MANIFEST_SHA256="${EXPECTED_PRODUCTION_MANIFEST_SHA256}" \
    EXPECTED_TEST_MANIFEST_SHA256="${EXPECTED_TEST_MANIFEST_SHA256}" \
    EXPECTED_PRODUCTION_MANIFEST_PATH="server/iggy-compatible/manifest.toml" \
    EXPECTED_TEST_MANIFEST_PATH="server/iggy-compatible/test-manifest.toml" \
    EXPECTED_SERIES_PATH="server/iggy-compatible/patches/series.toml" \
    python3 - "${MANIFEST}" "${SERIES}" "${ROOT}/server/iggy-compatible/test-manifest.toml" "${BUNDLE}" <<'PY'
import base64
import binascii
import hashlib
import os
from pathlib import Path
import re
import sys
import tomllib

production_path, series_path, test_path, bundle_path = map(Path, sys.argv[1:])
for path in (production_path, series_path, test_path):
    if not path.is_file() or path.is_symlink():
        raise SystemExit(f"required input is missing or a symlink: {path}")

production_bytes = production_path.read_bytes()
test_bytes = test_path.read_bytes()
if hashlib.sha256(production_bytes).hexdigest() != os.environ["EXPECTED_PRODUCTION_MANIFEST_SHA256"]:
    raise SystemExit("production manifest digest mismatch")
if hashlib.sha256(test_bytes).hexdigest() != os.environ["EXPECTED_TEST_MANIFEST_SHA256"]:
    raise SystemExit("test manifest digest mismatch")

production = tomllib.loads(production_bytes.decode())
test = tomllib.loads(test_bytes.decode())
document = tomllib.loads(series_path.read_text())
if set(document) != {"schema_version", "series"} or document["schema_version"] != 1:
    raise SystemExit("patch series schema mismatch")
series = document["series"]
expected_keys = {
    "repository", "baseline_commit", "commit", "parent_commit", "tree",
    "commit_count", "bundle_format", "bundle_encoding", "bundle_ref",
    "bundle_sha256", "complete_diff_sha256", "cargo_lock_sha256",
    "toolchain_manifest_sha256", "production_manifest",
    "production_manifest_sha256", "test_manifest", "test_manifest_sha256",
    "bundle_base64",
}
if set(series) != expected_keys:
    raise SystemExit("patch series keys mismatch")

expected_series = {
    "repository": "https://github.com/apache/iggy.git",
    "baseline_commit": os.environ["EXPECTED_BASELINE_COMMIT"],
    "commit": os.environ["EXPECTED_SOURCE_COMMIT"],
    "parent_commit": os.environ["EXPECTED_PARENT_COMMIT"],
    "tree": os.environ["EXPECTED_SOURCE_TREE"],
    "commit_count": 3,
    "bundle_format": "git-bundle-v2",
    "bundle_encoding": "base64",
    "bundle_ref": os.environ["EXPECTED_BUNDLE_REF"],
    "bundle_sha256": os.environ["EXPECTED_BUNDLE_SHA256"],
    "complete_diff_sha256": os.environ["EXPECTED_DIFF_SHA256"],
    "cargo_lock_sha256": os.environ["EXPECTED_LOCK_SHA256"],
    "toolchain_manifest_sha256": os.environ["EXPECTED_TOOLCHAIN_SHA256"],
    "production_manifest": os.environ["EXPECTED_PRODUCTION_MANIFEST_PATH"],
    "production_manifest_sha256": os.environ["EXPECTED_PRODUCTION_MANIFEST_SHA256"],
    "test_manifest": os.environ["EXPECTED_TEST_MANIFEST_PATH"],
    "test_manifest_sha256": os.environ["EXPECTED_TEST_MANIFEST_SHA256"],
}
for key, expected in expected_series.items():
    if series[key] != expected:
        raise SystemExit(f"patch series {key} mismatch")

expected_source = {
    "repository": "https://github.com/apache/iggy.git",
    "baseline_commit": os.environ["EXPECTED_BASELINE_COMMIT"],
    "commit": os.environ["EXPECTED_SOURCE_COMMIT"],
    "tree": os.environ["EXPECTED_SOURCE_TREE"],
    "cargo_lock_sha256": os.environ["EXPECTED_LOCK_SHA256"],
    "clean_required": True,
}
expected_toolchain = {
    "channel": "1.94.0",
    "rustc_commit": "4a4ef493e3a1488c6e321570238084b38948f6db",
    "cargo_commit": "85eff7c80277b57f78b11e28d14154ab12fcf643",
    "manifest_sha256": os.environ["EXPECTED_TOOLCHAIN_SHA256"],
}
for label, manifest in (("production", production), ("test", test)):
    if manifest.get("source") != expected_source:
        raise SystemExit(f"{label} manifest source identity mismatch")
    if manifest.get("toolchain") != expected_toolchain:
        raise SystemExit(f"{label} manifest toolchain identity mismatch")
    if manifest.get("build", {}).get("runtime_build_sha") != os.environ["EXPECTED_SOURCE_COMMIT"]:
        raise SystemExit(f"{label} manifest runtime build identity mismatch")

expected_patch_binding = {
    "path": os.environ["EXPECTED_SERIES_PATH"],
    "bundle_sha256": os.environ["EXPECTED_BUNDLE_SHA256"],
    "complete_diff_sha256": os.environ["EXPECTED_DIFF_SHA256"],
}
if production.get("patch_series") != expected_patch_binding:
    raise SystemExit("production manifest patch-series binding mismatch")
if "patch_series" in test:
    raise SystemExit("test manifest must consume the production patch-series binding")

encoded = series["bundle_base64"]
if not isinstance(encoded, str):
    raise SystemExit("bundle payload is not base64 text")
compact = "".join(encoded.split())
if not re.fullmatch(r"[A-Za-z0-9+/]*={0,2}", compact):
    raise SystemExit("bundle payload contains non-base64 data")
try:
    bundle = base64.b64decode(compact, validate=True)
except binascii.Error as error:
    raise SystemExit("bundle payload is invalid base64") from error
if hashlib.sha256(bundle).hexdigest() != os.environ["EXPECTED_BUNDLE_SHA256"]:
    raise SystemExit("decoded bundle digest mismatch")

header_bytes = bundle.split(b"\n\n", 1)[0]
try:
    header = header_bytes.decode("utf-8").splitlines()
except UnicodeDecodeError as error:
    raise SystemExit("bundle header is not UTF-8") from error
expected_header = [
    "# v2 git bundle",
    f'-{os.environ["EXPECTED_BASELINE_COMMIT"]} chore(sdk): update rust client to 0.10.0 (#3126)',
    f'{os.environ["EXPECTED_SOURCE_COMMIT"]} {os.environ["EXPECTED_BUNDLE_REF"]}',
]
if header != expected_header:
    raise SystemExit("bundle v2 prerequisite/ref header mismatch")
bundle_path.write_bytes(bundle)
PY

require_equal "bundle head" "$(rtk proxy git bundle list-heads "${BUNDLE}")" \
    "${EXPECTED_SOURCE_COMMIT} ${EXPECTED_BUNDLE_REF}"

rtk proxy git init --quiet "${RECONSTRUCTED}"
rtk proxy git -C "${RECONSTRUCTED}" -c protocol.file.allow=always fetch --quiet \
    --no-tags --no-write-fetch-head "file://${IGGY_REPOSITORY}" \
    "${EXPECTED_BASELINE_COMMIT}:refs/heads/baseline"
require_equal "baseline seed" \
    "$(rtk proxy git -C "${RECONSTRUCTED}" rev-parse refs/heads/baseline^{commit})" \
    "${EXPECTED_BASELINE_COMMIT}"
if rtk proxy git -C "${RECONSTRUCTED}" cat-file -e "${EXPECTED_SOURCE_COMMIT}^{commit}" 2>/dev/null; then
    die "baseline-only repository already contains the compatible commit"
fi
[[ -z "$(rtk proxy git -C "${RECONSTRUCTED}" remote)" ]] || die "reconstruction repository recorded a remote"
rtk proxy git -C "${RECONSTRUCTED}" checkout --quiet --detach "${EXPECTED_BASELINE_COMMIT}"
require_clean_repository "baseline reconstruction" "${RECONSTRUCTED}"

rtk proxy git -C "${RECONSTRUCTED}" bundle verify "${BUNDLE}" >/dev/null
rtk proxy git -C "${RECONSTRUCTED}" fetch --quiet --no-tags --no-write-fetch-head \
    "${BUNDLE}" "${EXPECTED_BUNDLE_REF}:refs/heads/compatible"
require_equal "reconstructed commit" \
    "$(rtk proxy git -C "${RECONSTRUCTED}" rev-parse refs/heads/compatible^{commit})" \
    "${EXPECTED_SOURCE_COMMIT}"
require_equal "reconstructed parent" \
    "$(rtk proxy git -C "${RECONSTRUCTED}" show -s --format=%P "${EXPECTED_SOURCE_COMMIT}")" \
    "${EXPECTED_PARENT_COMMIT}"
require_equal "reconstructed commit count" \
    "$(rtk proxy git -C "${RECONSTRUCTED}" rev-list --count "${EXPECTED_BASELINE_COMMIT}..${EXPECTED_SOURCE_COMMIT}")" "3"
require_equal "reconstructed tree" \
    "$(rtk proxy git -C "${RECONSTRUCTED}" rev-parse "${EXPECTED_SOURCE_COMMIT}^{tree}")" \
    "${EXPECTED_SOURCE_TREE}"

rtk proxy git -C "${RECONSTRUCTED}" checkout --quiet --detach "${EXPECTED_SOURCE_COMMIT}"
require_clean_repository "reconstructed compatible checkout" "${RECONSTRUCTED}"
require_equal "reconstructed Cargo.lock digest" \
    "$(rtk sha256sum "${RECONSTRUCTED}/Cargo.lock" | rtk awk '{print $1}')" "${EXPECTED_LOCK_SHA256}"
require_equal "reconstructed toolchain manifest digest" \
    "$(rtk sha256sum "${RECONSTRUCTED}/rust-toolchain.toml" | rtk awk '{print $1}')" "${EXPECTED_TOOLCHAIN_SHA256}"
require_equal "complete patch digest" \
    "$(rtk proxy git -C "${RECONSTRUCTED}" -c core.abbrev=40 -c diff.renames=false diff \
        --binary --full-index --no-ext-diff --no-renames \
        "${EXPECTED_BASELINE_COMMIT}" "${EXPECTED_SOURCE_COMMIT}" -- | \
        rtk sha256sum | rtk awk '{print $1}')" "${EXPECTED_DIFF_SHA256}"
rtk proxy git -C "${RECONSTRUCTED}" fsck --strict --no-dangling >/dev/null

cleanup 0
if (( SOURCE_RECONSTRUCTION == 1 )); then
    rtk echo "compatible Iggy source reconstructed before Chirps clean: commit=${EXPECTED_SOURCE_COMMIT} tree=${EXPECTED_SOURCE_TREE}"
else
    rtk echo "compatible Iggy patch reconstructed: commit=${EXPECTED_SOURCE_COMMIT} tree=${EXPECTED_SOURCE_TREE}"
fi
