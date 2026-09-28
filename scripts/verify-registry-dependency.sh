#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  verify-registry-dependency.sh --version 0.7.0 (--feature-off | --feature-on) \
    --registry-only [--static-only] [--offline]

Legacy v0.6 evidence mode:
  verify-registry-dependency.sh --output FILE --release-version X.Y.Z [--source-commit SHA] [--offline]

The v0.7 mode rejects path/git/package/alternate-registry substitutions before
Cargo runs. --static-only validates manifests, the exact publish DAG, the known
Iggy lock identities, and all negative controls without resolving unpublished
Chirps artifacts. Normal mode additionally requires the fixture lock to contain
all nine registry package checksums and builds the isolated consumer with it.
No mode publishes a package.
USAGE
}

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

run_legacy() {
  source_commit=""
  output=""
  release_version=""
  offline=false
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --output) output="${2:?missing value for --output}"; shift 2 ;;
      --release-version) release_version="${2:?missing value for --release-version}"; shift 2 ;;
      --source-commit) source_commit="${2:?missing value for --source-commit}"; shift 2 ;;
      --offline) offline=true; shift ;;
      -h|--help) usage; exit 0 ;;
      *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
    esac
  done

  [[ -n "$output" && -n "$release_version" ]] || { printf '%s\n' '--output and --release-version are required' >&2; exit 2; }
  source_commit="${source_commit:-$(git -C "$repo_root" rev-parse HEAD)}"
  fixture="$repo_root/scripts/fixtures/alopex-core-registry-check"
  [[ -f "$fixture/Cargo.lock" ]] || {
    printf 'fixture lock is missing: %s\n' "$fixture/Cargo.lock" >&2
    exit 1
  }

  scratch="$(mktemp -d "${TMPDIR:-/tmp}/chirps-registry-check.XXXXXXXX")"
  cleanup() { rm -rf "$scratch"; }
  trap cleanup EXIT
  mkdir -p "$scratch/crates/chirps-raft-storage"
  cp "$fixture/Cargo.toml" "$fixture/Cargo.lock" "$scratch/"
  cp "$repo_root/crates/chirps-raft-storage/Cargo.toml" "$scratch/crates/chirps-raft-storage/Cargo.toml"
  cp -R "$repo_root/crates/chirps-raft-storage/src" "$scratch/crates/chirps-raft-storage/src"

  cargo_args=(build --locked --manifest-path "$scratch/Cargo.toml" -p alopex-chirps-raft-storage)
  if [[ "$offline" == true ]]; then
    cargo_args+=(--offline)
  fi
  cargo "${cargo_args[@]}"

  python3 "$repo_root/scripts/release/verify-registry-dependency.py" \
    --root-manifest "$repo_root/crates/chirps-raft-storage/Cargo.toml" \
    --root-lock "$repo_root/Cargo.lock" \
    --fixture "$scratch" \
    --schema "$repo_root/docs/release/evidence/v${release_version}/registry-dependency.schema.json" \
    --release-version "$release_version" \
    --source-commit "$source_commit" \
    --output "$output"
}

if [[ " $* " == *" --output "* ]]; then
  run_legacy "$@"
  exit 0
fi

version=""
mode=""
registry_only=false
static_only=false
offline=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --version) version="${2:?missing value for --version}"; shift 2 ;;
    --feature-off|--feature-on)
      [[ -z "$mode" ]] || { printf '%s\n' 'select exactly one feature mode' >&2; exit 2; }
      mode="${1#--}"
      shift
      ;;
    --registry-only) registry_only=true; shift ;;
    --static-only) static_only=true; shift ;;
    --offline) offline=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ "$version" == "0.7.0" ]] || { printf '%s\n' '--version must be exactly 0.7.0' >&2; exit 2; }
[[ -n "$mode" ]] || { printf '%s\n' 'select --feature-off or --feature-on' >&2; exit 2; }
[[ "$registry_only" == true ]] || { printf '%s\n' '--registry-only is required' >&2; exit 2; }

fixture="$repo_root/scripts/fixtures/chirps-v0.7-registry-check"
[[ -f "$fixture/Cargo.toml" && -f "$fixture/Cargo.lock" && -f "$fixture/src/main.rs" ]] || {
  printf 'v0.7 registry fixture is incomplete: %s\n' "$fixture" >&2
  exit 1
}

require_complete=false
if [[ "$static_only" == false ]]; then require_complete=true; fi

python3 - "$repo_root" "$fixture" "$version" "$require_complete" <<'PY'
from __future__ import annotations

import copy
import hashlib
import re
import sys
import tomllib
from pathlib import Path

root, fixture, version = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]
require_complete = sys.argv[4] == "true"
registry = "registry+https://github.com/rust-lang/crates.io-index"
publish = [
    ("alopex-chirps-wire", "crates/chirps-wire"),
    ("alopex-chirps-raft-storage", "crates/chirps-raft-storage"),
    ("alopex-chirps-core", "crates/chirps-core"),
    ("alopex-chirps-gossip-swim", "crates/chirps-gossip-swim"),
    ("alopex-chirps-mock", "crates/chirps-mock"),
    ("alopex-chirps-transport-quic", "crates/chirps-transport-quic"),
    ("alopex-chirps-backend-iggy", "crates/chirps-backend-iggy"),
    ("alopex-chirps-file-transfer", "crates/chirps-file-transfer"),
    ("alopex-chirps", "crates/alopex-chirps"),
]
expected_internal = {
    "alopex-chirps-wire": set(),
    "alopex-chirps-raft-storage": set(),
    "alopex-chirps-core": {"alopex-chirps-wire"},
    "alopex-chirps-gossip-swim": {"alopex-chirps-wire"},
    "alopex-chirps-mock": {"alopex-chirps-core", "alopex-chirps-wire"},
    "alopex-chirps-transport-quic": {"alopex-chirps-core", "alopex-chirps-wire"},
    "alopex-chirps-backend-iggy": {"alopex-chirps-core", "alopex-chirps-wire"},
    "alopex-chirps-file-transfer": {
        "alopex-chirps-core", "alopex-chirps-wire", "alopex-chirps-mock",
        "alopex-chirps-transport-quic",
    },
    "alopex-chirps": {
        "alopex-chirps-core", "alopex-chirps-backend-iggy",
        "alopex-chirps-file-transfer", "alopex-chirps-gossip-swim",
        "alopex-chirps-mock", "alopex-chirps-raft-storage",
        "alopex-chirps-transport-quic", "alopex-chirps-wire",
    },
}
iggy = {
    "iggy": ("0.10.0", "6a470a78ccd8a6602402817906d842a3f361f84516cb880282565365df4eba2b"),
    "iggy_binary_protocol": ("0.10.0", "9e6da7f3a07797ef6a4248400bdf9d752e0eb7d83d14bf9cadcaae6bffdc9235"),
    "iggy_common": ("0.10.0", "6193adb66b1f12b6b2f9725573a6a6f69ce714c49830e122ed0d5ce7e7ffbde8"),
}

def fail(message: str) -> None:
    raise SystemExit(f"registry dependency rejected: {message}")

def load(path: Path) -> dict:
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        fail(f"cannot parse {path}: {exc}")

def dependency_tables(document: dict) -> list[dict]:
    tables = []
    for key in ("dependencies", "dev-dependencies", "build-dependencies"):
        value = document.get(key, {})
        if isinstance(value, dict):
            tables.append(value)
    for target in document.get("target", {}).values():
        if isinstance(target, dict):
            tables.extend(dependency_tables(target))
    return tables

def dep_version(value) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return value.get("version")
    return None

def reject_substitution(name: str, value) -> None:
    if isinstance(value, dict):
        forbidden = set(value).intersection({"path", "git", "registry", "package"})
        if forbidden:
            fail(f"{name} uses forbidden dependency substitution: {sorted(forbidden)}")

def validate_fixture(document: dict) -> None:
    if document.get("package", {}).get("publish") is not False:
        fail("registry consumer fixture must be publish=false")
    expected_order = [name for name, _ in publish]
    actual_order = (
        document.get("package", {})
        .get("metadata", {})
        .get("chirps-v07-registry", {})
        .get("publish-order")
    )
    if actual_order != expected_order:
        fail("nine-package publish order drifted")
    expected_features = {"default": [], "durable-iggy": ["alopex-chirps/durable-iggy"]}
    if document.get("features") != expected_features:
        fail("registry consumer feature contract drifted")
    dependencies = document.get("dependencies", {})
    if set(dependencies) != {"alopex-chirps", "alopex-chirps-mock"}:
        fail("registry consumer dependencies drifted")
    umbrella = dependencies["alopex-chirps"]
    if (
        not isinstance(umbrella, dict)
        or umbrella.get("version") != f"={version}"
        or umbrella.get("default-features") is not False
    ):
        fail("umbrella fixture dependency must be exact and default-feature-free")
    if dep_version(dependencies["alopex-chirps-mock"]) != f"={version}":
        fail("mock fixture dependency must use the exact release version")
    for name, value in dependencies.items():
        reject_substitution(name, value)
    if "patch" in document or "replace" in document:
        fail("registry consumer may not patch or replace registry packages")

def lock_records(document: dict) -> dict[tuple[str, str], list[dict]]:
    records: dict[tuple[str, str], list[dict]] = {}
    for item in document.get("package", []):
        records.setdefault((item.get("name"), item.get("version")), []).append(item)
    return records

def one(records: dict, name: str, package_version: str) -> dict:
    matches = records.get((name, package_version), [])
    if len(matches) != 1:
        fail(f"expected one lock entry for {name} {package_version}, got {len(matches)}")
    return matches[0]

def validate_lock(document: dict, complete: bool) -> None:
    records = lock_records(document)
    for name, (package_version, checksum) in iggy.items():
        versions = [item.get("version") for item in document.get("package", []) if item.get("name") == name]
        if versions != [package_version]:
            fail(f"{name} lock must contain only exact version {package_version}")
        item = one(records, name, package_version)
        if item.get("source") != registry or item.get("checksum") != checksum:
            fail(f"{name} lock source/checksum drifted")
    for item in document.get("package", []):
        source = item.get("source", "")
        if source and source != registry:
            fail(f"non-crates.io lock source is forbidden: {item.get('name')}")
    if complete:
        for name, _ in publish:
            versions = [item.get("version") for item in document.get("package", []) if item.get("name") == name]
            if versions != [version]:
                fail(f"{name} lock must contain only exact version {version}")
            item = one(records, name, version)
            checksum = item.get("checksum", "")
            if item.get("source") != registry or not re.fullmatch(r"[0-9a-f]{64}", checksum):
                fail(f"{name} lacks a crates.io registry source/checksum")

def internal_dependencies(document: dict, public_names: set[str]) -> set[str]:
    result = set()
    for table in dependency_tables(document):
        for name, value in table.items():
            resolved = value.get("package", name) if isinstance(value, dict) else name
            if resolved in public_names:
                result.add(resolved)
    return result

def validate_public_manifest(name: str, document: dict, public_names: set[str]) -> None:
    package = document.get("package", {})
    allowed_registries = package.get("publish")
    if allowed_registries is False or (
        isinstance(allowed_registries, list) and "crates-io" not in allowed_registries
    ):
        fail(f"public package {name} is not publishable to crates.io")
    if document.get("features", {}).get("default", []) != []:
        fail(f"public package {name} changed its empty default feature set")
    actual = internal_dependencies(document, public_names)
    if actual != expected_internal[name]:
        fail(f"public dependency graph drifted for {name}")
    for table in dependency_tables(document):
        for dependency, value in table.items():
            resolved = value.get("package", dependency) if isinstance(value, dict) else dependency
            if resolved not in public_names:
                continue
            if not isinstance(value, dict) or value.get("workspace") is not True:
                fail(f"{name} must inherit public dependency {resolved} from the workspace")
            if dependency != resolved:
                fail(f"{name} may not rename public dependency {resolved}")

def validate_workspace_lock(document: dict) -> None:
    records = lock_records(document)
    for name, _ in publish:
        versions = [item.get("version") for item in document.get("package", []) if item.get("name") == name]
        if versions != [version]:
            fail(f"workspace lock must contain only exact version {version} for {name}")
        item = one(records, name, version)
        if item.get("source") is not None or item.get("checksum") is not None:
            fail(f"workspace lock identity drifted for {name}")
    for name, (package_version, checksum) in iggy.items():
        versions = [item.get("version") for item in document.get("package", []) if item.get("name") == name]
        if versions != [package_version]:
            fail(f"workspace lock must contain only exact version {package_version} for {name}")
        item = one(records, name, package_version)
        if item.get("source") != registry or item.get("checksum") != checksum:
            fail(f"workspace lock identity drifted for {name}")

workspace = load(root / "Cargo.toml")
if workspace.get("workspace", {}).get("package", {}).get("version") != version:
    fail("workspace version is not exactly 0.7.0")
workspace_dependencies = workspace.get("workspace", {}).get("dependencies", {})
public_names = {name for name, _ in publish}
positions = {name: index for index, (name, _) in enumerate(publish)}
public_manifests = {}
for name, relative in publish:
    declared = workspace_dependencies.get(name)
    if (
        not isinstance(declared, dict)
        or declared.get("version") != version
        or declared.get("path") != relative
    ):
        fail(f"workspace dependency contract drifted for {name}")
    manifest = load(root / relative / "Cargo.toml")
    public_manifests[name] = manifest
    package = manifest.get("package", {})
    if package.get("name") != name or package.get("version") != {"workspace": True}:
        fail(f"package identity drifted for {name}")
    validate_public_manifest(name, manifest, public_names)
    for dependency in expected_internal[name]:
        if positions[dependency] >= positions[name]:
            fail(f"publish order puts {name} before dependency {dependency}")

validate_workspace_lock(load(root / "Cargo.lock"))

backend = load(root / "crates/chirps-backend-iggy/Cargo.toml")
for name in iggy:
    if backend.get("dependencies", {}).get(name) != "=0.10.0":
        fail(f"backend must pin {name} to =0.10.0")
for name, relative in publish:
    if name == "alopex-chirps-backend-iggy":
        continue
    manifest = load(root / relative / "Cargo.toml")
    for table in dependency_tables(manifest):
        if set(table).intersection(iggy):
            fail(f"Iggy dependency escaped optional backend into {name}")

umbrella = load(root / "crates/alopex-chirps/Cargo.toml")
umbrella_features = umbrella.get("features", {})
adapter_dependency = umbrella.get("dependencies", {}).get("alopex-chirps-backend-iggy")
if umbrella_features.get("default") != []:
    fail("umbrella default features must remain empty")
if umbrella_features.get("durable-iggy") != ["dep:alopex-chirps-backend-iggy"]:
    fail("durable-iggy must be the only normal adapter activation edge")
if not isinstance(adapter_dependency, dict) or adapter_dependency != {"workspace": True, "optional": True}:
    fail("umbrella adapter dependency must remain optional and workspace-owned")

fixture_document = load(fixture / "Cargo.toml")
lock_document = load(fixture / "Cargo.lock")
validate_fixture(fixture_document)
validate_lock(lock_document, require_complete)

controls = []
for kind in ("path", "git", "package", "version"):
    mutated = copy.deepcopy(fixture_document)
    dep = mutated["dependencies"]["alopex-chirps"]
    if kind == "path":
        dep["path"] = "../../../../crates/alopex-chirps"
    elif kind == "git":
        dep["git"] = "https://example.invalid/fork.git"
    elif kind == "package":
        dep["package"] = "alopex-chirps-fork"
    else:
        dep["version"] = "=0.7.1"
    try:
        validate_fixture(mutated)
    except SystemExit:
        controls.append(kind)
    else:
        fail(f"negative control unexpectedly passed: {kind}")
for kind in ("fork", "iggy-version", "checksum"):
    mutated = copy.deepcopy(lock_document)
    item = next(entry for entry in mutated["package"] if entry["name"] == "iggy")
    if kind == "fork":
        item["source"] = "registry+https://example.invalid/index"
    elif kind == "iggy-version":
        item["version"] = "0.10.1"
    else:
        item["checksum"] = "0" * 64
    try:
        validate_lock(mutated, False)
    except SystemExit:
        controls.append(kind)
    else:
        fail(f"negative control unexpectedly passed: {kind}")
mutated = copy.deepcopy(fixture_document)
wrong_order = mutated["package"]["metadata"]["chirps-v07-registry"]["publish-order"]
wrong_order[0], wrong_order[2] = wrong_order[2], wrong_order[0]
try:
    validate_fixture(mutated)
except SystemExit:
    controls.append("publish-order")
else:
    fail("negative control unexpectedly passed: publish-order")
mutated = copy.deepcopy(public_manifests["alopex-chirps-core"])
mutated["dependencies"]["alopex-chirps-raft-storage"] = {"workspace": True}
try:
    validate_public_manifest("alopex-chirps-core", mutated, public_names)
except SystemExit:
    controls.append("public-dag")
else:
    fail("negative control unexpectedly passed: public-dag")
for kind in ("publish-disabled", "default-feature"):
    mutated = copy.deepcopy(public_manifests["alopex-chirps-wire"])
    if kind == "publish-disabled":
        mutated["package"]["publish"] = False
    else:
        mutated["features"]["default"] = ["hlc"]
    try:
        validate_public_manifest("alopex-chirps-wire", mutated, public_names)
    except SystemExit:
        controls.append(kind)
    else:
        fail(f"negative control unexpectedly passed: {kind}")

projection = "\n".join(name for name, _ in publish).encode()
print(
    "v0.7 registry contract validated: "
    f"publish-order-sha256={hashlib.sha256(projection).hexdigest()} "
    f"negative-controls={','.join(controls)}"
)
PY

if [[ "$static_only" == true ]]; then
  printf 'v0.7 %s static registry verification passed; package resolution was intentionally not run\n' "$mode"
  exit 0
fi

scratch="$(mktemp -d "${TMPDIR:-/tmp}/chirps-registry-check-v07.XXXXXXXX")"
cleanup() { rm -rf "$scratch"; }
trap cleanup EXIT
cp -R "$fixture/." "$scratch/"

cargo_args=(build --locked --manifest-path "$scratch/Cargo.toml" --no-default-features)
metadata_args=(metadata --locked --format-version 1 --manifest-path "$scratch/Cargo.toml" --no-default-features)
if [[ "$mode" == "feature-on" ]]; then
  cargo_args+=(--features durable-iggy)
  metadata_args+=(--features durable-iggy)
fi
if [[ "$offline" == true ]]; then
  cargo_args+=(--offline)
  metadata_args+=(--offline)
fi
cargo "${cargo_args[@]}"
cargo "${metadata_args[@]}" > "$scratch/metadata.json"

python3 - "$scratch/metadata.json" "$mode" <<'PY'
import json
import sys
from pathlib import Path

metadata = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
mode = sys.argv[2]
registry = "registry+https://github.com/rust-lang/crates.io-index"
public = {
    "alopex-chirps-wire", "alopex-chirps-raft-storage", "alopex-chirps-core",
    "alopex-chirps-gossip-swim", "alopex-chirps-mock", "alopex-chirps-transport-quic",
    "alopex-chirps-backend-iggy", "alopex-chirps-file-transfer", "alopex-chirps",
}
iggy = {"iggy", "iggy_binary_protocol", "iggy_common"}
packages = {package["id"]: package for package in metadata["packages"]}
root_id = next(
    package["id"]
    for package in metadata["packages"]
    if package["name"] == "chirps-v07-registry-check"
)
nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
seen, pending = set(), [root_id]
while pending:
    package_id = pending.pop()
    if package_id in seen:
        continue
    seen.add(package_id)
    pending.extend(nodes[package_id]["dependencies"])
active_packages = [packages[package_id] for package_id in seen]
active_names = {package["name"] for package in active_packages}
expected_public = public if mode == "feature-on" else public - {"alopex-chirps-backend-iggy"}
if active_names.intersection(public) != expected_public:
    raise SystemExit("active public package graph does not match the selected feature mode")
if mode == "feature-off" and active_names.intersection(iggy | {"alopex-chirps-backend-iggy"}):
    raise SystemExit("feature-off graph contains the optional backend or Iggy")
if mode == "feature-on" and not iggy.issubset(active_names):
    raise SystemExit("feature-on graph is missing exact Iggy packages")
for name in expected_public | (iggy if mode == "feature-on" else set()):
    matches = [package for package in active_packages if package["name"] == name]
    if len(matches) != 1:
        raise SystemExit(f"{name} resolved more than once")
    package = matches[0]
    if package["source"] != registry:
        raise SystemExit(f"{name} did not resolve from crates.io")
    expected_version = "0.10.0" if name in iggy else "0.7.0"
    if package["version"] != expected_version:
        raise SystemExit(f"{name} version drifted")
print(f"v0.7 {mode} registry-only active graph validated")
PY
