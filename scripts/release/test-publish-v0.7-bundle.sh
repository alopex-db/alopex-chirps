#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
scratch="$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/chirps-v07-publication-test.XXXXXX")"
fixture_repo="$scratch/repo"
publisher="$fixture_repo/scripts/release/publish-v0.7-bundle.sh"
server_pid=""

cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -rf -- "$scratch"
}
trap cleanup EXIT

mkdir -p "$scratch/bundle" "$scratch/server" \
  "$fixture_repo/scripts/release" "$fixture_repo/docs/release" \
  "$fixture_repo/server/iggy-compatible"
cp "$repo_root/scripts/release/publish-v0.7-bundle.sh" "$publisher"
cp "$repo_root/scripts/release/verify-v0.7-evidence.py" \
  "$fixture_repo/scripts/release/verify-v0.7-evidence.py"
cp "$repo_root/scripts/release/v07_e2e_evidence.py" \
  "$repo_root/scripts/release/test-v07-e2e-evidence.py" "$fixture_repo/scripts/release/"
cp "$repo_root/docs/release/v0.7.0-evidence-schema.json" \
  "$fixture_repo/docs/release/v0.7.0-evidence-schema.json"
test_server_sha256="$(printf '%s\n' 'fixture-publish-disabled-test-server' | sha256sum | awk '{print $1}')"
printf '[artifact]\noutput_sha256 = "%s"\n' "$test_server_sha256" \
  > "$fixture_repo/server/iggy-compatible/test-manifest.toml"

python3 - "$scratch/bundle" "$fixture_repo/docs/release/v0.7.0-evidence-schema.json" <<'PY'
from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import sys
import tarfile
from pathlib import Path

root = Path(sys.argv[1])
schema_path = Path(sys.argv[2])
sys.dont_write_bytecode = True
fixture_script = schema_path.parents[2] / "scripts/release/test-v07-e2e-evidence.py"
fixture_spec = importlib.util.spec_from_file_location("e2e_fixtures", fixture_script)
fixture_module = importlib.util.module_from_spec(fixture_spec)
fixture_spec.loader.exec_module(fixture_module)
source_commit = "1" * 40
production_server = b"fixture-production-server\n"
test_server = b"fixture-publish-disabled-test-server\n"
required_kinds = {
    "source",
    "specification",
    "model",
    "configuration",
    "tool",
    "environment",
    "process",
    "fault",
    "performance",
    "package",
    "compatibility",
    "server",
    "security",
}
package_names = [
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


def write(path: Path, value: bytes) -> dict:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(value)
    return {
        "path": str(path.relative_to(root)),
        "sha256": hashlib.sha256(value).hexdigest(),
        "size": len(value),
    }


def json_bytes(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()


def descriptor(value: bytes, media_type: str) -> dict:
    return {
        "mediaType": media_type,
        "digest": "sha256:" + hashlib.sha256(value).hexdigest(),
        "size": len(value),
    }


def tar_entry(archive: tarfile.TarFile, name: str, value: bytes, mode: int = 0o644) -> None:
    info = tarfile.TarInfo(name)
    info.size = len(value)
    info.mode = mode
    info.mtime = 0
    archive.addfile(info, io.BytesIO(value))


def oci_image(
    path: Path,
    server: bytes,
    artifact_kind: str,
    failpoints: str,
    root_whiteout: bool = False,
) -> dict:
    server_sha256 = hashlib.sha256(server).hexdigest()
    layer_output = io.BytesIO()
    with tarfile.open(fileobj=layer_output, mode="w") as layer:
        tar_entry(layer, "usr/local/bin/iggy-server", server, 0o755)
    layer_bytes = layer_output.getvalue()
    config_bytes = json_bytes(
        {
            "architecture": "amd64",
            "os": "linux",
            "config": {
                "Labels": {
                    "org.alopex.chirps.artifact-kind": artifact_kind,
                    "org.alopex.chirps.failpoints": failpoints,
                    "org.alopex.chirps.server-sha256": server_sha256,
                }
            },
        }
    )
    config_descriptor = descriptor(
        config_bytes, "application/vnd.oci.image.config.v1+json"
    )
    layer_descriptor = descriptor(
        layer_bytes, "application/vnd.oci.image.layer.v1.tar"
    )
    layer_blobs = [(layer_bytes, layer_descriptor)]
    if root_whiteout:
        whiteout_output = io.BytesIO()
        with tarfile.open(fileobj=whiteout_output, mode="w") as layer:
            tar_entry(layer, ".wh..wh..opq", b"")
        whiteout_bytes = whiteout_output.getvalue()
        whiteout_descriptor = descriptor(
            whiteout_bytes, "application/vnd.oci.image.layer.v1.tar"
        )
        layer_blobs.append((whiteout_bytes, whiteout_descriptor))
    manifest_bytes = json_bytes(
        {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": config_descriptor,
            "layers": [item for _, item in layer_blobs],
        }
    )
    manifest_descriptor = descriptor(
        manifest_bytes, "application/vnd.oci.image.manifest.v1+json"
    )
    index_bytes = json_bytes({"schemaVersion": 2, "manifests": [manifest_descriptor]})
    layout_bytes = json_bytes({"imageLayoutVersion": "1.0.0"})
    path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(path, mode="w") as archive:
        tar_entry(archive, "oci-layout", layout_bytes)
        tar_entry(archive, "index.json", index_bytes)
        for value, item in [
            (config_bytes, config_descriptor),
            *layer_blobs,
            (manifest_bytes, manifest_descriptor),
        ]:
            tar_entry(archive, "blobs/sha256/" + item["digest"].removeprefix("sha256:"), value)
    image = write(path, path.read_bytes())
    image["manifest_digest"] = manifest_descriptor["digest"]
    image["server_sha256"] = server_sha256
    return image


production_image = oci_image(
    root / "images" / "compatible-iggy-production.oci.tar",
    production_server,
    "production",
    "disabled",
)
test_image = oci_image(
    root / "images" / "compatible-iggy-renamed.oci.tar",
    test_server,
    "production",
    "disabled",
)
hidden_image = oci_image(
    root / "images" / "compatible-iggy-hidden.oci.tar",
    production_server,
    "production",
    "disabled",
    root_whiteout=True,
)

evidence_files = {
    kind: write(
        root / "artifacts" / f"{kind}.bin",
        production_server if kind == "server" else f"fixture-{kind}\n".encode(),
    )
    for kind in required_kinds - {"process"}
}
for kind, lane in (("process", "production"), ("fault", "fault")):
    lane_path = fixture_module.write_lane_fixture(root / "runtime" / lane, lane, source_commit, "2" * 40)
    evidence_files[kind] = {"path": lane_path.relative_to(root).as_posix(), "sha256": hashlib.sha256(lane_path.read_bytes()).hexdigest()}

candidate = {
    "schema": "chirps.v0.7.candidate/v1",
    "release_version": "0.7.0",
    "source_commit": source_commit,
    "iggy_commit": "2" * 40,
    "source_sha256": evidence_files["source"]["sha256"],
    "specification_sha256": evidence_files["specification"]["sha256"],
    "model_sha256": evidence_files["model"]["sha256"],
    "configuration_sha256": evidence_files["configuration"]["sha256"],
    "tool_sha256": evidence_files["tool"]["sha256"],
    "environment_sha256": evidence_files["environment"]["sha256"],
    "server_sha256": production_image["server_sha256"],
    "package_graph_sha256": evidence_files["package"]["sha256"],
    "performance": {"fixture": True},
}
candidate_path = root / "candidate.json"
candidate_path.write_text(json.dumps(candidate, sort_keys=True) + "\n", encoding="utf-8")

packages = []
for index, name in enumerate(package_names):
    stored = write(
        root / "packages" / f"{name}-0.7.0.crate",
        f"stored-crate-{index}-{name}\n".encode(),
    )
    packages.append(
        {
            "name": name,
            "version": "0.7.0",
            **stored,
            "registry_metadata": {
                "name": name,
                "vers": "0.7.0",
                "deps": [],
                "features": {},
                "authors": [],
                "description": "publication fixture",
            },
        }
    )

asset = write(root / "assets" / "v0.7.0-evidence.json", b'{"fixture":"evidence"}\n')
bundle = {
    "schema": "chirps.v0.7.release-bundle/v1",
    "release_version": "0.7.0",
    "tag": "chirps-v0.7.0",
    "source_commit": source_commit,
    "candidate_sha256": hashlib.sha256(candidate_path.read_bytes()).hexdigest(),
    "packages": packages,
    "production_image": {
        "artifact_kind": "production",
        **{key: production_image[key] for key in ("path", "sha256", "size")},
        "reference": "ghcr.io/alopex-db/chirps-compatible-iggy:0.7.0",
        "manifest_digest": production_image["manifest_digest"],
    },
    "github_repository": "alopex-db/alopex-chirps",
    "github_assets": [{"name": "v0.7.0-evidence.json", **asset}],
}
(root / "release-bundle.json").write_text(
    json.dumps(bundle, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
test_bundle = json.loads(json.dumps(bundle))
test_bundle["production_image"] = {
    "artifact_kind": "production",
    **{key: test_image[key] for key in ("path", "sha256", "size")},
    "reference": "ghcr.io/alopex-db/chirps-compatible-iggy:0.7.0",
    "manifest_digest": test_image["manifest_digest"],
}
(root / "test-release-bundle.json").write_text(
    json.dumps(test_bundle, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
hidden_bundle = json.loads(json.dumps(bundle))
hidden_bundle["production_image"] = {
    "artifact_kind": "production",
    **{key: hidden_image[key] for key in ("path", "sha256", "size")},
    "reference": "ghcr.io/alopex-db/chirps-compatible-iggy:0.7.0",
    "manifest_digest": hidden_image["manifest_digest"],
}
(root / "hidden-release-bundle.json").write_text(
    json.dumps(hidden_bundle, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)


def write_evidence(index_name: str, bundle_name: str, release_name: str) -> None:
    release_path = root / release_name
    candidate_sha256 = hashlib.sha256(candidate_path.read_bytes()).hexdigest()
    entries = []
    for kind in sorted(required_kinds):
        stored = (
            {
                "path": release_name,
                "sha256": hashlib.sha256(release_path.read_bytes()).hexdigest(),
            }
            if kind == "process"
            else evidence_files[kind]
        )
        entries.append(
            {
                "id": "release-bundle" if kind == "process" else f"fixture-{kind}",
                "kind": kind,
                "result": "pass",
                "candidate_sha256": candidate_sha256,
                "environment_sha256": candidate["environment_sha256"],
                "path": stored["path"],
                "sha256": stored["sha256"],
            }
        )
    entries.append({
        "id": "fixture-production-e2e", "kind": "process", "result": "pass",
        "candidate_sha256": candidate_sha256,
        "environment_sha256": candidate["environment_sha256"],
        **evidence_files["process"],
    })
    entries.sort(key=lambda entry: entry["id"])
    evidence_bundle = {
        "schema": "chirps.v0.7.bundle/v1",
        "release_version": "0.7.0",
        "candidate_sha256": candidate_sha256,
        "environment_sha256": candidate["environment_sha256"],
        "artifacts": entries,
    }
    evidence_bundle_path = root / bundle_name
    evidence_bundle_path.write_text(
        json.dumps(evidence_bundle, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    evidence = {
        "schema": "chirps.v0.7.evidence/v1",
        "release_version": "0.7.0",
        "schema_sha256": hashlib.sha256(schema_path.read_bytes()).hexdigest(),
        "candidate": {
            "path": "candidate.json",
            "sha256": candidate_sha256,
        },
        "bundle": {
            "path": bundle_name,
            "sha256": hashlib.sha256(evidence_bundle_path.read_bytes()).hexdigest(),
        },
        "evidence": entries,
    }
    (root / index_name).write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


write_evidence("evidence.json", "bundle.json", "release-bundle.json")
write_evidence("test-evidence.json", "test-bundle.json", "test-release-bundle.json")
write_evidence("hidden-evidence.json", "hidden-bundle.json", "hidden-release-bundle.json")
invalid_evidence = json.loads((root / "evidence.json").read_text(encoding="utf-8"))
invalid_evidence["evidence"] = []
(root / "invalid-evidence.json").write_text(
    json.dumps(invalid_evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
PY

python3 - "$scratch/server" "$scratch/server-url" <<'PY' &
from __future__ import annotations

import hashlib
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlsplit

root = Path(sys.argv[1])
ready = Path(sys.argv[2])
(root / "objects").mkdir(parents=True, exist_ok=True)


def object_path(request_path: str) -> Path:
    return root / "objects" / hashlib.sha256(request_path.encode()).hexdigest()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, _format: str, *_args: object) -> None:
        return

    def do_GET(self) -> None:
        request_path = unquote(urlsplit(self.path).path)
        path = object_path(request_path)
        if not path.is_file():
            self.send_response(404)
            self.end_headers()
            return
        body = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_PUT(self) -> None:
        request_path = unquote(urlsplit(self.path).path)
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        expected = self.headers.get("X-Chirps-SHA256", "")
        actual = hashlib.sha256(body).hexdigest()
        if actual != expected:
            self.send_response(422)
            self.end_headers()
            return
        object_path(request_path).write_bytes(body)
        with (root / "requests.log").open("a", encoding="utf-8") as log:
            log.write(f"PUT\t{request_path}\t{actual}\n")
        self.send_response(201)
        self.end_headers()


server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
ready.write_text(f"http://127.0.0.1:{server.server_port}\n", encoding="utf-8")
server.serve_forever()
PY
server_pid=$!

for _attempt in 1 2 3 4 5; do
  [[ -s "$scratch/server-url" ]] && break
  sleep 1
done
[[ -s "$scratch/server-url" ]] || {
  printf '%s\n' 'fixture publication endpoint did not start' >&2
  exit 1
}
endpoint="$(tr -d '\n' < "$scratch/server-url")"
bundle="$scratch/bundle/release-bundle.json"
candidate="$scratch/bundle/candidate.json"
evidence="$scratch/bundle/evidence.json"
mkdir -p "$scratch/bin"
printf '#!/usr/bin/env bash\nexit 97\n' > "$scratch/bin/cargo"
printf '#!/usr/bin/env bash\nexit 98\n' > "$scratch/bin/git"
chmod 755 "$scratch/bin/cargo" "$scratch/bin/git"
fixture_path="$scratch/bin:$PATH"
common=(
  --bundle "$bundle"
  --candidate "$candidate"
  --evidence "$evidence"
  --require-environment-approval
  --resume-only-on-checksum-match
)

PATH="$fixture_path" "$publisher" "${common[@]}" --validate-only \
  --fixture-registry "$endpoint/readonly-registry" \
  --fixture-image "$endpoint/readonly-image" \
  --fixture-github "$endpoint/readonly-github" >/dev/null
[[ ! -e "$scratch/server/requests.log" ]] || {
  printf '%s\n' 'local validation contacted a remote endpoint' >&2
  exit 1
}

if PATH="$fixture_path" "$publisher" "${common[@]}" \
  --fixture-registry "$endpoint/approval-registry" \
  --fixture-image "$endpoint/approval-image" \
  --fixture-github "$endpoint/approval-github" >/dev/null 2>&1; then
  printf '%s\n' 'publisher accepted missing environment approval' >&2
  exit 1
fi
[[ ! -e "$scratch/server/requests.log" ]] || {
  printf '%s\n' 'publisher contacted an endpoint before environment approval' >&2
  exit 1
}

if PATH="$fixture_path" \
  CHIRPS_RELEASE_ENVIRONMENT_APPROVAL="release:$(printf '1%.0s' {1..40})" \
  "$publisher" \
  --bundle "$scratch/bundle/test-release-bundle.json" \
  --candidate "$candidate" \
  --evidence "$evidence" \
  --require-environment-approval \
  --resume-only-on-checksum-match \
  --fixture-registry "$endpoint/unbound-registry" \
  --fixture-image "$endpoint/unbound-image" \
  --fixture-github "$endpoint/unbound-github" >/dev/null 2>&1; then
  printf '%s\n' 'publisher accepted a release manifest not bound by evidence' >&2
  exit 1
fi
[[ ! -e "$scratch/server/requests.log" ]] || {
  printf '%s\n' 'publisher contacted an endpoint for an unbound release manifest' >&2
  exit 1
}

if PATH="$fixture_path" \
  CHIRPS_RELEASE_ENVIRONMENT_APPROVAL="release:$(printf '1%.0s' {1..40})" \
  "$publisher" \
  --bundle "$bundle" \
  --candidate "$candidate" \
  --evidence "$scratch/bundle/invalid-evidence.json" \
  --require-environment-approval \
  --resume-only-on-checksum-match \
  --fixture-registry "$endpoint/invalid-evidence-registry" \
  --fixture-image "$endpoint/invalid-evidence-image" \
  --fixture-github "$endpoint/invalid-evidence-github" >/dev/null 2>&1; then
  printf '%s\n' 'publisher accepted an evidence index without the required inventory' >&2
  exit 1
fi
[[ ! -e "$scratch/server/requests.log" ]] || {
  printf '%s\n' 'publisher contacted an endpoint for an invalid evidence index' >&2
  exit 1
}

if PATH="$fixture_path" \
  CHIRPS_RELEASE_ENVIRONMENT_APPROVAL="release:$(printf '1%.0s' {1..40})" \
  "$publisher" \
  --bundle "$scratch/bundle/hidden-release-bundle.json" \
  --candidate "$candidate" \
  --evidence "$scratch/bundle/hidden-evidence.json" \
  --require-environment-approval \
  --resume-only-on-checksum-match \
  --fixture-registry "$endpoint/hidden-registry" \
  --fixture-image "$endpoint/hidden-image" \
  --fixture-github "$endpoint/hidden-github" >/dev/null 2>&1; then
  printf '%s\n' 'publisher accepted an OCI image whose effective root hides iggy-server' >&2
  exit 1
fi
[[ ! -e "$scratch/server/requests.log" ]] || {
  printf '%s\n' 'publisher contacted an endpoint for an invalid OCI filesystem' >&2
  exit 1
}

PATH="$fixture_path" \
  CHIRPS_RELEASE_ENVIRONMENT_APPROVAL="release:$(printf '1%.0s' {1..40})" \
  "$publisher" "${common[@]}" \
  --fixture-registry "$endpoint/registry" \
  --fixture-image "$endpoint/image" \
  --fixture-github "$endpoint/github"

python3 - "$bundle" "$scratch/server/requests.log" <<'PY'
import json
import sys
from pathlib import Path

bundle = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
lines = Path(sys.argv[2]).read_text(encoding="utf-8").splitlines()
expected = [
    f"/registry/{item['name']}/0.7.0"
    for item in bundle["packages"]
]
expected += [
    "/image/production/0.7.0",
    "/github/chirps-v0.7.0/v0.7.0-evidence.json",
]
actual = [line.split("\t")[1] for line in lines]
if actual != expected:
    raise SystemExit(f"exact-byte publication order drifted: {actual!r}")
PY

request_count="$(wc -l < "$scratch/server/requests.log")"
PATH="$fixture_path" \
  CHIRPS_RELEASE_ENVIRONMENT_APPROVAL="release:$(printf '1%.0s' {1..40})" \
  "$publisher" "${common[@]}" \
  --fixture-registry "$endpoint/registry" \
  --fixture-image "$endpoint/image" \
  --fixture-github "$endpoint/github" >/dev/null
[[ "$(wc -l < "$scratch/server/requests.log")" == "$request_count" ]] || {
  printf '%s\n' 'checksum-matched resume uploaded bytes again' >&2
  exit 1
}

first_crate="$scratch/bundle/packages/alopex-chirps-wire-0.7.0.crate"
first_sha="$(sha256sum "$first_crate" | awk '{print $1}')"
curl --fail --silent --request PUT --header "X-Chirps-SHA256: $first_sha" \
  --data-binary "@$first_crate" "$endpoint/mismatch-registry/alopex-chirps-wire/0.7.0" >/dev/null
wrong="$scratch/wrong.crate"
printf '%s' 'different stored bytes' > "$wrong"
wrong_sha="$(sha256sum "$wrong" | awk '{print $1}')"
curl --fail --silent --request PUT --header "X-Chirps-SHA256: $wrong_sha" \
  --data-binary "@$wrong" "$endpoint/mismatch-registry/alopex-chirps-raft-storage/0.7.0" >/dev/null
mismatch_count="$(wc -l < "$scratch/server/requests.log")"
if PATH="$fixture_path" \
  CHIRPS_RELEASE_ENVIRONMENT_APPROVAL="release:$(printf '1%.0s' {1..40})" \
  "$publisher" "${common[@]}" \
  --fixture-registry "$endpoint/mismatch-registry" \
  --fixture-image "$endpoint/mismatch-image" \
  --fixture-github "$endpoint/mismatch-github" >/dev/null 2>&1; then
  printf '%s\n' 'publisher accepted a partial checksum mismatch' >&2
  exit 1
fi
[[ "$(wc -l < "$scratch/server/requests.log")" == "$mismatch_count" ]] || {
  printf '%s\n' 'publisher continued uploading after a checksum mismatch' >&2
  exit 1
}

test_count="$(wc -l < "$scratch/server/requests.log")"
test_common=(
  --bundle "$scratch/bundle/test-release-bundle.json"
  --candidate "$candidate"
  --evidence "$scratch/bundle/test-evidence.json"
  --require-environment-approval
  --resume-only-on-checksum-match
)
if PATH="$fixture_path" \
  CHIRPS_RELEASE_ENVIRONMENT_APPROVAL="release:$(printf '1%.0s' {1..40})" \
  "$publisher" "${test_common[@]}" \
  --fixture-registry "$endpoint/test-registry" \
  --fixture-image "$endpoint/test-image" \
  --fixture-github "$endpoint/test-github" >/dev/null 2>&1; then
  printf '%s\n' 'publisher accepted renamed test server bytes with a production outer label' >&2
  exit 1
fi
[[ "$(wc -l < "$scratch/server/requests.log")" == "$test_count" ]] || {
  printf '%s\n' 'publisher contacted an endpoint for renamed test server bytes' >&2
  exit 1
}

printf '%s\n' 'v0.7 publication structure passed: approval, exact order, resume, mismatch, and test-artifact controls'

# Exercise the actual wrapper -> Python -> local publisher validation chain.
# Only the test interpreter substitutes read-only remotes; production has no
# fixture endpoint switch that could be mistaken for publication evidence.
cp "$repo_root/scripts/release/verify-published-v0.7.sh" "$fixture_repo/scripts/release/"
cp "$repo_root/scripts/release/verify-published-v0.7.py" "$fixture_repo/scripts/release/"
mkdir -p "$scratch/readonly-bin"
export CHIRPS_TEST_REAL_PYTHON="$(command -v python3)"
export CHIRPS_TEST_VERIFY_TESTS="$repo_root/scripts/release/test-verify-published-v0.7.py"
cat > "$scratch/readonly-bin/python3" <<'PYTHON_SH'
#!/usr/bin/env bash
if [[ "${1##*/}" == "verify-published-v0.7.py" ]]; then
  exec "$CHIRPS_TEST_REAL_PYTHON" "$CHIRPS_TEST_VERIFY_TESTS" --fixture-cli "$@"
fi
exec "$CHIRPS_TEST_REAL_PYTHON" "$@"
PYTHON_SH
chmod +x "$scratch/readonly-bin/python3"
readonly_args=(
  --candidate "$candidate" --candidate-sha256 "$(sha256sum "$candidate" | awk '{print $1}')"
  --evidence "$evidence" --evidence-sha256 "$(sha256sum "$evidence" | awk '{print $1}')"
  --bundle "$scratch/bundle/bundle.json"
  --bundle-sha256 "$(sha256sum "$scratch/bundle/bundle.json" | awk '{print $1}')"
  --tag-object "$(printf 'a%.0s' {1..40})"
)
PATH="$scratch/readonly-bin:$PATH" PYTHONDONTWRITEBYTECODE=1 \
  bash "$fixture_repo/scripts/release/verify-published-v0.7.sh" "${readonly_args[@]}" >/dev/null
if PATH="$scratch/readonly-bin:$PATH" PYTHONDONTWRITEBYTECODE=1 CHIRPS_TEST_REMOTE_DRIFT=1 \
  bash "$fixture_repo/scripts/release/verify-published-v0.7.sh" "${readonly_args[@]}" >/dev/null 2>&1; then
  printf '%s\n' 'published CLI accepted remote archive substitution' >&2
  exit 1
fi
printf '%s\n' 'published CLI fixture passed: full wrapper chain and remote substitution rejection'
