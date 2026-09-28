#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
test_server_manifest="$repo_root/server/iggy-compatible/test-manifest.toml"

usage() {
  cat <<'USAGE' >&2
Usage: publish-v0.7-bundle.sh --bundle FILE --candidate FILE --evidence FILE
       --require-environment-approval --resume-only-on-checksum-match
       [--validate-only]
       [--fixture-registry URL --fixture-image URL --fixture-github URL]

Production mode uploads the exact stored .crate archives to crates.io, copies
the stored production OCI archive, and attaches the exact stored GitHub assets.
The evidence index must bind the supplied manifest as the release-bundle artifact.
Fixture endpoints replace all external services for the executable self-test.
The command never invokes Cargo, packages source, or selects an implicit HEAD.
USAGE
}

bundle=""
candidate=""
evidence=""
require_approval=false
checksum_resume=false
validate_only=false
fixture_registry=""
fixture_image=""
fixture_github=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bundle) bundle="${2:?missing value for --bundle}"; shift 2 ;;
    --candidate) candidate="${2:?missing value for --candidate}"; shift 2 ;;
    --evidence) evidence="${2:?missing value for --evidence}"; shift 2 ;;
    --require-environment-approval) require_approval=true; shift ;;
    --resume-only-on-checksum-match) checksum_resume=true; shift ;;
    --validate-only) validate_only=true; shift ;;
    --fixture-registry) fixture_registry="${2:?missing fixture registry URL}"; shift 2 ;;
    --fixture-image) fixture_image="${2:?missing fixture image URL}"; shift 2 ;;
    --fixture-github) fixture_github="${2:?missing fixture GitHub URL}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage; exit 2 ;;
  esac
done

[[ -n "$bundle" && -n "$candidate" && -n "$evidence" ]] || {
  printf '%s\n' '--bundle, --candidate, and --evidence are required' >&2
  exit 2
}
[[ "$require_approval" == true ]] || {
  printf '%s\n' '--require-environment-approval is mandatory' >&2
  exit 2
}
[[ "$checksum_resume" == true ]] || {
  printf '%s\n' '--resume-only-on-checksum-match is mandatory' >&2
  exit 2
}

fixture_count=0
for endpoint in "$fixture_registry" "$fixture_image" "$fixture_github"; do
  if [[ -n "$endpoint" ]]; then fixture_count=$((fixture_count + 1)); fi
done
[[ "$fixture_count" == 0 || "$fixture_count" == 3 ]] || {
  printf '%s\n' 'all three fixture endpoints must be supplied together' >&2
  exit 2
}

scratch="$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/chirps-v07-publish.XXXXXX")"
cleanup() { rm -rf -- "$scratch"; }
trap cleanup EXIT

python3 "$repo_root/scripts/release/verify-v0.7-evidence.py" \
  --schema "$repo_root/docs/release/v0.7.0-evidence-schema.json" \
  "$evidence" >/dev/null

python3 - "$bundle" "$candidate" "$evidence" "$test_server_manifest" "$scratch" <<'PY'
from __future__ import annotations

import base64
import hashlib
import io
import json
import re
import sys
import tarfile
import tomllib
from pathlib import Path

bundle_path = Path(sys.argv[1]).resolve(strict=True)
candidate_path = Path(sys.argv[2]).resolve(strict=True)
evidence_path = Path(sys.argv[3]).resolve(strict=True)
test_server_manifest_path = Path(sys.argv[4]).resolve(strict=True)
output = Path(sys.argv[5])
expected_packages = [
    "alopex-chirps-wire",
    "alopex-chirps-raft-storage",
    "alopex-chirps-core",
    "alopex-chirps-gossip-swim",
    "alopex-chirps-mock",
    "alopex-chirps-transport-quic",
    "alopex-chirps-backend-iggy",
    "alopex-chirps-file-transfer",
    "alopex-chirps",
]
sha256_pattern = re.compile(r"[0-9a-f]{64}")
commit_pattern = re.compile(r"[0-9a-f]{40}")
safe_name = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+-]*")


def fail(message: str) -> None:
    raise SystemExit(f"publication bundle rejected: {message}")


def load(path: Path) -> dict:
    try:
        value = json.loads(path.read_bytes())
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"cannot read {path}: {exc}")
    if not isinstance(value, dict):
        fail(f"{path} root must be an object")
    return value


def exact_keys(value: dict, expected: set[str], label: str) -> None:
    if set(value) != expected:
        fail(f"{label} fields differ from the frozen publication contract")


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def oci_blob(files: dict[str, bytes], descriptor: dict, label: str) -> bytes:
    if not isinstance(descriptor, dict):
        fail(f"{label} descriptor must be an object")
    descriptor_digest = descriptor.get("digest")
    descriptor_size = descriptor.get("size")
    if not isinstance(descriptor_digest, str) or re.fullmatch(
        r"sha256:[0-9a-f]{64}", descriptor_digest
    ) is None:
        fail(f"{label} descriptor digest is invalid")
    path = f"blobs/sha256/{descriptor_digest.removeprefix('sha256:')}"
    value = files.get(path)
    if value is None:
        fail(f"{label} blob is missing")
    if descriptor_size != len(value) or descriptor_digest != f"sha256:{sha256_bytes(value)}":
        fail(f"{label} descriptor does not match its blob")
    return value


def inspect_oci_server(path: Path, expected_manifest_digest: str) -> tuple[str, dict]:
    try:
        with tarfile.open(path, mode="r:*") as archive:
            files: dict[str, bytes] = {}
            for member in archive.getmembers():
                name = member.name.removeprefix("./")
                if name in files:
                    fail(f"OCI archive contains duplicate entry {name}")
                if member.isfile():
                    source = archive.extractfile(member)
                    if source is None:
                        fail(f"OCI archive entry {name} is unreadable")
                    files[name] = source.read()
                elif name in {"index.json", "oci-layout"} or name.startswith("blobs/"):
                    fail(f"OCI archive metadata entry {name} is not a regular file")
    except (OSError, tarfile.TarError) as exc:
        fail(f"production image is not a readable OCI archive: {exc}")

    try:
        index = json.loads(files["index.json"])
    except (KeyError, json.JSONDecodeError) as exc:
        fail(f"OCI index is missing or invalid: {exc}")
    manifests = index.get("manifests") if isinstance(index, dict) else None
    if not isinstance(manifests, list) or len(manifests) != 1:
        fail("OCI archive must contain exactly one image manifest")
    manifest_descriptor = manifests[0]
    manifest_bytes = oci_blob(files, manifest_descriptor, "OCI manifest")
    if manifest_descriptor.get("digest") != expected_manifest_digest:
        fail("OCI index manifest digest differs from the release bundle")
    try:
        manifest = json.loads(manifest_bytes)
    except json.JSONDecodeError as exc:
        fail(f"OCI manifest is invalid: {exc}")
    if not isinstance(manifest, dict) or manifest.get("schemaVersion") != 2:
        fail("OCI manifest schema is invalid")
    try:
        config = json.loads(oci_blob(files, manifest["config"], "OCI config"))
    except (KeyError, json.JSONDecodeError) as exc:
        fail(f"OCI config is missing or invalid: {exc}")
    labels = config.get("config", {}).get("Labels") if isinstance(config, dict) else None
    if not isinstance(labels, dict) or not all(
        isinstance(key, str) and isinstance(value, str) for key, value in labels.items()
    ):
        fail("OCI config labels are missing or invalid")

    layers = manifest.get("layers")
    if not isinstance(layers, list) or not layers:
        fail("OCI manifest has no filesystem layers")
    server_bytes: bytes | None = None
    server_whiteouts = {
        ".wh..wh..opq",
        ".wh.usr",
        "usr/.wh..wh..opq",
        "usr/.wh.local",
        "usr/local/.wh..wh..opq",
        "usr/local/.wh.bin",
        "usr/local/bin/.wh..wh..opq",
        "usr/local/bin/.wh.iggy-server",
    }
    for index, descriptor in enumerate(layers):
        layer_bytes = oci_blob(files, descriptor, f"OCI layer[{index}]")
        try:
            with tarfile.open(fileobj=io.BytesIO(layer_bytes), mode="r:*") as layer:
                deletes_lower_server = False
                layer_server: bytes | None = None
                for member in layer.getmembers():
                    name = member.name.removeprefix("./").lstrip("/")
                    if name in server_whiteouts:
                        deletes_lower_server = True
                    elif name in {"usr", "usr/local", "usr/local/bin"} and not member.isdir():
                        fail(f"OCI layer[{index}] replaces a server path directory")
                    elif name == "usr/local/bin/iggy-server":
                        if not member.isfile():
                            fail("OCI iggy-server is not a regular file")
                        if layer_server is not None:
                            fail(f"OCI layer[{index}] contains duplicate iggy-server entries")
                        source = layer.extractfile(member)
                        if source is None:
                            fail("OCI iggy-server is unreadable")
                        layer_server = source.read()
                if deletes_lower_server:
                    server_bytes = None
                if layer_server is not None:
                    server_bytes = layer_server
        except tarfile.TarError as exc:
            fail(f"OCI layer[{index}] is not a readable layer archive: {exc}")
    if server_bytes is None:
        fail("OCI image does not contain /usr/local/bin/iggy-server")
    return sha256_bytes(server_bytes), labels


def stored_file(root: Path, entry: dict, label: str) -> Path:
    exact_keys(entry, {"path", "sha256", "size"}, label)
    relative = entry["path"]
    if not isinstance(relative, str) or Path(relative).is_absolute():
        fail(f"{label}.path must be relative")
    parts = Path(relative).parts
    if not parts or any(part in {"", ".", ".."} for part in parts):
        fail(f"{label}.path is unsafe")
    for index in range(1, len(parts) + 1):
        if root.joinpath(*parts[:index]).is_symlink():
            fail(f"{label}.path may not traverse a symlink")
    path = root.joinpath(*parts).resolve(strict=True)
    try:
        path.relative_to(root)
    except ValueError:
        fail(f"{label}.path escapes the bundle directory")
    if not path.is_file():
        fail(f"{label}.path is not a regular file")
    expected_digest = entry["sha256"]
    if not isinstance(expected_digest, str) or not sha256_pattern.fullmatch(expected_digest):
        fail(f"{label}.sha256 is invalid")
    if not isinstance(entry["size"], int) or entry["size"] < 1:
        fail(f"{label}.size is invalid")
    if path.stat().st_size != entry["size"] or digest(path) != expected_digest:
        fail(f"{label} stored bytes differ")
    return path


def referenced_file(root: Path, entry: dict, label: str) -> Path:
    exact_keys(entry, {"path", "sha256"}, label)
    relative = entry["path"]
    if not isinstance(relative, str) or Path(relative).is_absolute():
        fail(f"{label}.path must be relative")
    parts = Path(relative).parts
    if not parts or any(part in {"", ".", ".."} for part in parts):
        fail(f"{label}.path is unsafe")
    for index in range(1, len(parts) + 1):
        if root.joinpath(*parts[:index]).is_symlink():
            fail(f"{label}.path may not traverse a symlink")
    path = root.joinpath(*parts).resolve(strict=True)
    try:
        path.relative_to(root)
    except ValueError:
        fail(f"{label}.path escapes the evidence directory")
    expected_digest = entry["sha256"]
    if not isinstance(expected_digest, str) or not sha256_pattern.fullmatch(expected_digest):
        fail(f"{label}.sha256 is invalid")
    if not path.is_file() or digest(path) != expected_digest:
        fail(f"{label} stored bytes differ")
    return path


bundle_object = load(bundle_path)
exact_keys(
    bundle_object,
    {
        "schema",
        "release_version",
        "tag",
        "source_commit",
        "candidate_sha256",
        "packages",
        "production_image",
        "github_repository",
        "github_assets",
    },
    "bundle",
)
if bundle_object["schema"] != "chirps.v0.7.release-bundle/v1":
    fail("unknown release bundle schema")
if bundle_object["release_version"] != "0.7.0" or bundle_object["tag"] != "chirps-v0.7.0":
    fail("release version or tag drifted")
source_commit = bundle_object["source_commit"]
if not isinstance(source_commit, str) or not commit_pattern.fullmatch(source_commit):
    fail("source_commit must be an exact lowercase commit")
candidate_sha256 = bundle_object["candidate_sha256"]
if not isinstance(candidate_sha256, str) or not sha256_pattern.fullmatch(candidate_sha256):
    fail("candidate_sha256 is invalid")
if digest(candidate_path) != candidate_sha256:
    fail("candidate bytes differ from candidate_sha256")
candidate_object = load(candidate_path)
if (
    candidate_object.get("schema") != "chirps.v0.7.candidate/v1"
    or candidate_object.get("release_version") != "0.7.0"
    or candidate_object.get("source_commit") != source_commit
):
    fail("candidate identity differs from the release bundle")

evidence_root = evidence_path.parent
evidence_object = load(evidence_path)
exact_keys(
    evidence_object,
    {"schema", "release_version", "schema_sha256", "candidate", "bundle", "evidence"},
    "evidence index",
)
if (
    evidence_object["schema"] != "chirps.v0.7.evidence/v1"
    or evidence_object["release_version"] != "0.7.0"
):
    fail("evidence index identity drifted")
if referenced_file(evidence_root, evidence_object["candidate"], "evidence candidate") != candidate_path:
    fail("evidence index references another candidate")
evidence_bundle_path = referenced_file(
    evidence_root, evidence_object["bundle"], "evidence bundle"
)
evidence_bundle = load(evidence_bundle_path)
exact_keys(
    evidence_bundle,
    {"schema", "release_version", "candidate_sha256", "environment_sha256", "artifacts"},
    "evidence bundle",
)
if (
    evidence_bundle["schema"] != "chirps.v0.7.bundle/v1"
    or evidence_bundle["release_version"] != "0.7.0"
    or evidence_bundle["candidate_sha256"] != candidate_sha256
    or evidence_bundle["environment_sha256"] != candidate_object.get("environment_sha256")
):
    fail("evidence bundle identity differs from the candidate")
artifacts = evidence_bundle["artifacts"]
if not isinstance(artifacts, list):
    fail("evidence bundle artifacts must be an array")
release_entries = [item for item in artifacts if isinstance(item, dict) and item.get("id") == "release-bundle"]
if len(release_entries) != 1:
    fail("evidence bundle must contain exactly one release-bundle artifact")
release_entry = release_entries[0]
exact_keys(
    release_entry,
    {
        "id",
        "kind",
        "result",
        "candidate_sha256",
        "environment_sha256",
        "path",
        "sha256",
    },
    "release-bundle artifact",
)
if (
    release_entry["kind"] != "process"
    or release_entry["result"] != "pass"
    or release_entry["candidate_sha256"] != candidate_sha256
    or release_entry["environment_sha256"] != candidate_object.get("environment_sha256")
):
    fail("release-bundle artifact identity differs from the candidate")
release_manifest_path = referenced_file(
    evidence_root,
    {key: release_entry[key] for key in ("path", "sha256")},
    "release-bundle artifact",
)
if release_manifest_path != bundle_path:
    fail("publisher bundle is not the release-bundle artifact bound by evidence")

packages = bundle_object["packages"]
if not isinstance(packages, list) or [item.get("name") for item in packages] != expected_packages:
    fail("nine-package order drifted")
package_lines = []
for index, item in enumerate(packages):
    exact_keys(
        item,
        {"name", "version", "path", "sha256", "size", "registry_metadata"},
        f"packages[{index}]",
    )
    name = item["name"]
    if item["version"] != "0.7.0":
        fail(f"{name} version drifted")
    path = stored_file(
        bundle_path.parent,
        {key: item[key] for key in ("path", "sha256", "size")},
        f"packages[{index}]",
    )
    if path.name != f"{name}-0.7.0.crate":
        fail(f"{name} is not the exact stored .crate archive")
    metadata = item["registry_metadata"]
    if not isinstance(metadata, dict) or metadata.get("name") != name or metadata.get("vers") != "0.7.0":
        fail(f"{name} registry metadata identity drifted")
    metadata_bytes = json.dumps(metadata, separators=(",", ":"), sort_keys=True).encode()
    package_lines.append(
        "\t".join(
            (
                name,
                str(path),
                item["sha256"],
                base64.b64encode(metadata_bytes).decode(),
            )
        )
    )

image = bundle_object["production_image"]
exact_keys(
    image,
    {"artifact_kind", "path", "sha256", "size", "reference", "manifest_digest"},
    "production_image",
)
if image["artifact_kind"] != "production":
    fail("test or failpoint server artifacts may not be published")
image_path = stored_file(
    bundle_path.parent,
    {key: image[key] for key in ("path", "sha256", "size")},
    "production_image",
)
if any(token in image_path.name.lower() for token in ("test", "failpoint")):
    fail("production image filename identifies a test artifact")
if not isinstance(image["reference"], str) or ":0.7.0" not in image["reference"]:
    fail("production image reference must use the exact release tag")
if not isinstance(image["manifest_digest"], str) or not re.fullmatch(
    r"sha256:[0-9a-f]{64}", image["manifest_digest"]
):
    fail("production image manifest digest is invalid")
candidate_server_sha256 = candidate_object.get("server_sha256")
if not isinstance(candidate_server_sha256, str) or not sha256_pattern.fullmatch(
    candidate_server_sha256
):
    fail("candidate.server_sha256 is invalid")
try:
    with test_server_manifest_path.open("rb") as handle:
        test_server_sha256 = tomllib.load(handle)["artifact"]["output_sha256"]
except (OSError, KeyError, tomllib.TOMLDecodeError) as exc:
    fail(f"cannot read the publish-disabled server identity: {exc}")
if not isinstance(test_server_sha256, str) or not sha256_pattern.fullmatch(test_server_sha256):
    fail("publish-disabled server digest is invalid")
image_server_sha256, image_labels = inspect_oci_server(image_path, image["manifest_digest"])
if image_server_sha256 != candidate_server_sha256:
    fail("OCI server bytes differ from candidate.server_sha256")
if image_server_sha256 == test_server_sha256:
    fail("publish-disabled test server bytes may not be published")
expected_labels = {
    "org.alopex.chirps.artifact-kind": "production",
    "org.alopex.chirps.failpoints": "disabled",
    "org.alopex.chirps.server-sha256": image_server_sha256,
}
if any(image_labels.get(key) != value for key, value in expected_labels.items()):
    fail("OCI production labels do not match the candidate server bytes")

repository = bundle_object["github_repository"]
if not isinstance(repository, str) or re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository) is None:
    fail("github_repository must be owner/name")
assets = bundle_object["github_assets"]
if not isinstance(assets, list) or not assets:
    fail("at least one GitHub asset is required")
asset_lines = []
asset_names = set()
for index, item in enumerate(assets):
    exact_keys(item, {"name", "path", "sha256", "size"}, f"github_assets[{index}]")
    name = item["name"]
    if not isinstance(name, str) or safe_name.fullmatch(name) is None or name in asset_names:
        fail("GitHub asset names must be safe and unique")
    path = stored_file(
        bundle_path.parent,
        {key: item[key] for key in ("path", "sha256", "size")},
        f"github_assets[{index}]",
    )
    if any(token in name.lower() for token in ("test", "failpoint")):
        fail("test or failpoint assets may not be published")
    asset_names.add(name)
    asset_lines.append("\t".join((name, str(path), item["sha256"])))

(output / "packages.tsv").write_text("\n".join(package_lines) + "\n", encoding="utf-8")
(output / "assets.tsv").write_text("\n".join(asset_lines) + "\n", encoding="utf-8")
(output / "identity.tsv").write_text(
    "\t".join(
        (
            source_commit,
            bundle_object["tag"],
            str(image_path),
            image["sha256"],
            image["reference"],
            image["manifest_digest"],
            repository,
        )
    )
    + "\n",
    encoding="utf-8",
)
PY

if [[ "$validate_only" == true ]]; then
  printf '%s\n' 'publication bundle local validation passed; no remote operations performed'
  exit 0
fi

IFS=$'\t' read -r source_commit release_tag image_path image_sha256 \
  image_reference image_manifest_digest github_repository < "$scratch/identity.tsv"

expected_approval="release:$source_commit"
[[ "${CHIRPS_RELEASE_ENVIRONMENT_APPROVAL:-}" == "$expected_approval" ]] || {
  printf '%s\n' 'protected release environment approval is absent or bound to another commit' >&2
  exit 1
}

digest_file() {
  sha256sum -- "$1" | awk '{print $1}'
}

fixture_transfer() {
  local endpoint="$1"
  local relative="$2"
  local path="$3"
  local expected="$4"
  local label="$5"
  local remote="$scratch/remote.bin"
  local status
  status="$(curl --silent --show-error --output "$remote" --write-out '%{http_code}' \
    "$endpoint/$relative")"
  case "$status" in
    200)
      [[ "$(digest_file "$remote")" == "$expected" ]] || {
        printf '%s checksum mismatch; publication stopped\n' "$label" >&2
        return 1
      }
      printf '%s already exists with matching checksum; skipped\n' "$label"
      ;;
    404)
      curl --fail --silent --show-error --request PUT \
        --header "X-Chirps-SHA256: $expected" \
        --data-binary "@$path" "$endpoint/$relative" >/dev/null
      ;;
    *)
      printf '%s lookup failed with HTTP %s\n' "$label" "$status" >&2
      return 1
      ;;
  esac
}

publish_registry_archive() {
  local name="$1"
  local path="$2"
  local expected="$3"
  local metadata_base64="$4"
  if [[ "$fixture_count" == 3 ]]; then
    fixture_transfer "$fixture_registry" "$name/0.7.0" "$path" "$expected" "$name"
    return
  fi

  local downloaded="$scratch/registry-$name.crate"
  local status
  status="$(curl --silent --show-error --location --output "$downloaded" --write-out '%{http_code}' \
    "https://crates.io/api/v1/crates/$name/0.7.0/download")"
  if [[ "$status" == 200 ]]; then
    [[ "$(digest_file "$downloaded")" == "$expected" ]] || {
      printf '%s registry checksum mismatch; publication stopped\n' "$name" >&2
      return 1
    }
    printf '%s already exists with matching checksum; skipped\n' "$name"
    return
  fi
  [[ "$status" == 404 ]] || {
    printf '%s registry lookup failed with HTTP %s\n' "$name" "$status" >&2
    return 1
  }
  [[ -n "${CARGO_REGISTRY_TOKEN:-}" ]] || {
    printf '%s\n' 'CARGO_REGISTRY_TOKEN is required for registry upload' >&2
    return 1
  }
  local request="$scratch/registry-$name.request"
  python3 - "$metadata_base64" "$path" "$request" <<'PY'
import base64
import struct
import sys
from pathlib import Path

metadata = base64.b64decode(sys.argv[1], validate=True)
archive = Path(sys.argv[2]).read_bytes()
Path(sys.argv[3]).write_bytes(
    struct.pack("<I", len(metadata)) + metadata + struct.pack("<I", len(archive)) + archive
)
PY
  curl --fail --silent --show-error \
    --header "Authorization: ${CARGO_REGISTRY_TOKEN}" \
    --header 'Content-Type: application/octet-stream' \
    --data-binary "@$request" \
    'https://crates.io/api/v1/crates/new' >/dev/null
  for _attempt in {1..20}; do
    status="$(curl --silent --show-error --location --output "$downloaded" \
      --write-out '%{http_code}' \
      "https://crates.io/api/v1/crates/$name/0.7.0/download")"
    if [[ "$status" == 200 ]]; then
      [[ "$(digest_file "$downloaded")" == "$expected" ]] || {
        printf '%s registry checksum mismatch after upload; publication stopped\n' "$name" >&2
        return 1
      }
      return
    fi
    [[ "$status" == 404 ]] || {
      printf '%s registry verification failed with HTTP %s\n' "$name" "$status" >&2
      return 1
    }
    sleep 3
  done
  printf '%s registry bytes did not become readable after upload\n' "$name" >&2
  return 1
}

while IFS=$'\t' read -r package_name package_path package_sha256 metadata_base64; do
  publish_registry_archive "$package_name" "$package_path" "$package_sha256" "$metadata_base64"
done < "$scratch/packages.tsv"

if [[ "$fixture_count" == 3 ]]; then
  fixture_transfer "$fixture_image" "production/0.7.0" \
    "$image_path" "$image_sha256" production-image
else
  command -v skopeo >/dev/null || {
    printf '%s\n' 'skopeo is required to copy the stored production image' >&2
    exit 1
  }
  inspect_error="$scratch/image-inspect.error"
  if remote_digest="$(skopeo inspect --format '{{.Digest}}' "docker://$image_reference" \
    2>"$inspect_error")"; then
    [[ "$remote_digest" == "$image_manifest_digest" ]] || {
      printf '%s\n' 'production image digest mismatch; publication stopped' >&2
      exit 1
    }
    printf '%s\n' 'production image already exists with matching digest; skipped'
  else
    grep -Eqi 'manifest unknown|name unknown|not found' "$inspect_error" || {
      printf '%s\n' 'production image lookup failed without an authoritative missing result' >&2
      exit 1
    }
    skopeo copy --preserve-digests "oci-archive:$image_path" "docker://$image_reference"
    remote_digest="$(skopeo inspect --format '{{.Digest}}' "docker://$image_reference")"
    [[ "$remote_digest" == "$image_manifest_digest" ]] || {
      printf '%s\n' 'production image digest differs after copy' >&2
      exit 1
    }
  fi
fi

if [[ "$fixture_count" == 0 ]]; then
  command -v gh >/dev/null || { printf '%s\n' 'gh is required for GitHub assets' >&2; exit 1; }
  [[ -n "${GH_TOKEN:-}" ]] || { printf '%s\n' 'GH_TOKEN is required for GitHub assets' >&2; exit 1; }
  if ! gh release view "$release_tag" --repo "$github_repository" >/dev/null 2>&1; then
    gh release create "$release_tag" --repo "$github_repository" \
      --verify-tag --title "Alopex Chirps $release_tag" --generate-notes
  fi
fi

while IFS=$'\t' read -r asset_name asset_path asset_sha256; do
  if [[ "$fixture_count" == 3 ]]; then
    fixture_transfer "$fixture_github" "$release_tag/$asset_name" \
      "$asset_path" "$asset_sha256" "GitHub asset $asset_name"
    continue
  fi
  asset_id="$(gh api "repos/$github_repository/releases/tags/$release_tag" \
    --jq ".assets[] | select(.name == \"$asset_name\") | .id" | head -n 1)"
  if [[ -n "$asset_id" ]]; then
    downloaded="$scratch/github-$asset_name"
    gh api --header 'Accept: application/octet-stream' \
      "repos/$github_repository/releases/assets/$asset_id" > "$downloaded"
    [[ "$(digest_file "$downloaded")" == "$asset_sha256" ]] || {
      printf 'GitHub asset %s checksum mismatch; publication stopped\n' "$asset_name" >&2
      exit 1
    }
    printf 'GitHub asset %s already exists with matching checksum; skipped\n' "$asset_name"
  else
    gh release upload "$release_tag" "$asset_path#$asset_name" \
      --repo "$github_repository"
  fi
done < "$scratch/assets.tsv"

printf 'v0.7 exact-byte bundle publication completed for %s\n' "$source_commit"
