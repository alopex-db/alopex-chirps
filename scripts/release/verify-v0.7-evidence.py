#!/usr/bin/env python3
"""Verify one immutable Chirps v0.7.0 candidate evidence bundle."""

from __future__ import annotations

import argparse
import copy
import hashlib
import importlib.util
import json
import re
import sys
import tempfile
from pathlib import Path
from typing import Callable

sys.dont_write_bytecode = True
from v07_e2e_evidence import verify_lane

VERSION = "0.7.0"
SCHEMA_URI = "https://json-schema.org/draft/2020-12/schema"
CANDIDATE_SCHEMA = "chirps.v0.7.candidate/v1"
EVIDENCE_SCHEMA = "chirps.v0.7.evidence/v1"
BUNDLE_SCHEMA = "chirps.v0.7.bundle/v1"
SHA40 = re.compile(r"^[0-9a-f]{40}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
ENTRY_ID = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
REQUIRED_KINDS = {
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
CANDIDATE_DIGEST_KINDS = {
    "source_sha256": "source",
    "specification_sha256": "specification",
    "model_sha256": "model",
    "configuration_sha256": "configuration",
    "tool_sha256": "tool",
    "environment_sha256": "environment",
    "server_sha256": "server",
    "package_graph_sha256": "package",
}
SECRET_KEY_MARKERS = {
    "password",
    "token",
    "secret",
    "privatekey",
    "credentialvalue",
    "apikey",
}
PUBLIC_METADATA_SUFFIXES = {"sha256", "digest", "id", "identity"}


class EvidenceError(ValueError):
    """The evidence set violates the frozen v0.7 contract."""


def fail(message: str) -> None:
    raise EvidenceError(message)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    try:
        return sha256_bytes(path.read_bytes())
    except OSError as exc:
        fail(f"cannot read {path}: {exc}")


def load_object(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"cannot read JSON {path}: {exc}")
    if not isinstance(value, dict):
        fail(f"JSON root must be an object: {path}")
    reject_secret_fields(value, path.name)
    return value


def reject_secret_fields(value: object, label: str) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", key)
            tokens = [part for part in re.split(r"[^a-z0-9]+", normalized.lower()) if part]
            compact = "".join(tokens)
            is_public_metadata = bool(tokens) and tokens[-1] in PUBLIC_METADATA_SUFFIXES
            if not is_public_metadata and any(
                marker in compact for marker in SECRET_KEY_MARKERS
            ):
                fail(f"{label} contains forbidden secret-bearing field {key!r}")
            reject_secret_fields(child, label)
    elif isinstance(value, list):
        for child in value:
            reject_secret_fields(child, label)


def exact_keys(value: dict, expected: set[str], label: str) -> None:
    actual = set(value)
    if actual != expected:
        fail(
            f"{label} fields differ: missing={sorted(expected - actual)}, "
            f"extra={sorted(actual - expected)}"
        )


def require_sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or not SHA256.fullmatch(value):
        fail(f"{label} must be a lowercase SHA-256")
    return value


def safe_file(root: Path, relative: object, label: str) -> Path:
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        fail(f"{label} must be a non-empty relative path")
    candidate = root.joinpath(relative)
    parts = Path(relative).parts
    if any(part == ".." for part in parts):
        fail(f"{label} escapes the evidence root")
    for index in range(1, len(parts) + 1):
        if root.joinpath(*parts[:index]).is_symlink():
            fail(f"{label} may not traverse a symlink")
    resolved = candidate.resolve()
    try:
        resolved.relative_to(root)
    except ValueError:
        fail(f"{label} escapes the evidence root")
    if not resolved.is_file():
        fail(f"{label} is missing: {relative}")
    return resolved


def validate_schema_contract(schema: dict) -> None:
    exact_keys(schema, {"$schema", "$id", "title", "oneOf", "$defs"}, "schema")
    if schema["$schema"] != SCHEMA_URI:
        fail("unknown JSON Schema dialect")
    expected_refs = [
        {"$ref": "#/$defs/candidate"},
        {"$ref": "#/$defs/evidenceIndex"},
        {"$ref": "#/$defs/bundle"},
    ]
    if schema["oneOf"] != expected_refs:
        fail("schema must expose the exact candidate/evidence/bundle variants")
    definitions = schema["$defs"]
    expected_definitions = {
        "sha256",
        "commit",
        "relativePath",
        "digestReference",
        "candidate",
        "evidenceEntry",
        "bundle",
        "evidenceIndex",
    }
    if not isinstance(definitions, dict) or set(definitions) != expected_definitions:
        fail("schema definitions differ from the frozen contract")
    expected_shapes = {
        "candidate": {
            "schema",
            "release_version",
            "source_commit",
            "iggy_commit",
            *CANDIDATE_DIGEST_KINDS,
            "performance",
        },
        "evidenceEntry": {
            "id",
            "kind",
            "result",
            "candidate_sha256",
            "environment_sha256",
            "path",
            "sha256",
        },
        "bundle": {
            "schema",
            "release_version",
            "candidate_sha256",
            "environment_sha256",
            "artifacts",
        },
        "evidenceIndex": {
            "schema",
            "release_version",
            "schema_sha256",
            "candidate",
            "bundle",
            "evidence",
        },
    }
    for name, fields in expected_shapes.items():
        definition = definitions[name]
        if (
            definition.get("type") != "object"
            or definition.get("additionalProperties") is not False
            or set(definition.get("required", [])) != fields
            or set(definition.get("properties", {})) != fields
        ):
            fail(f"schema definition {name} is not exact")
    digest_reference = definitions["digestReference"]
    if (
        digest_reference.get("type") != "object"
        or digest_reference.get("additionalProperties") is not False
        or set(digest_reference.get("required", [])) != {"path", "sha256"}
        or set(digest_reference.get("properties", {})) != {"path", "sha256"}
    ):
        fail("digest reference schema is not exact")
    if definitions["candidate"]["properties"]["schema"].get("const") != CANDIDATE_SCHEMA:
        fail("candidate schema identifier drifted")
    if definitions["bundle"]["properties"]["schema"].get("const") != BUNDLE_SCHEMA:
        fail("bundle schema identifier drifted")
    if definitions["evidenceIndex"]["properties"]["schema"].get("const") != EVIDENCE_SCHEMA:
        fail("evidence schema identifier drifted")
    for name in ("candidate", "bundle", "evidenceIndex"):
        if definitions[name]["properties"]["release_version"].get("const") != VERSION:
            fail(f"{name} release version drifted")
    if definitions["evidenceEntry"]["properties"]["result"].get("const") != "pass":
        fail("evidence result contract drifted")
    if set(definitions["evidenceEntry"]["properties"]["kind"].get("enum", [])) != REQUIRED_KINDS:
        fail("evidence kind inventory drifted")
    for name, field in (("bundle", "artifacts"), ("evidenceIndex", "evidence")):
        if definitions[name]["properties"][field].get("minItems") != len(REQUIRED_KINDS):
            fail(f"{name}.{field} minimum inventory drifted")
    performance = definitions["candidate"]["properties"]["performance"]
    if performance.get("type") != "object" or performance.get("minProperties") != 1:
        fail("candidate performance plan contract drifted")


def validate_candidate(candidate: dict) -> None:
    expected = {
        "schema",
        "release_version",
        "source_commit",
        "iggy_commit",
        *CANDIDATE_DIGEST_KINDS,
        "performance",
    }
    exact_keys(candidate, expected, "candidate")
    if candidate["schema"] != CANDIDATE_SCHEMA:
        fail("unknown candidate schema version")
    if candidate["release_version"] != VERSION:
        fail("candidate targets another release")
    for field in ("source_commit", "iggy_commit"):
        if not isinstance(candidate[field], str) or not SHA40.fullmatch(candidate[field]):
            fail(f"candidate.{field} must be a lowercase 40-character commit")
    for field in CANDIDATE_DIGEST_KINDS:
        require_sha256(candidate[field], f"candidate.{field}")
    if not isinstance(candidate["performance"], dict) or not candidate["performance"]:
        fail("candidate.performance must be a non-empty Task 6.12 plan")


def validate_reference(value: object, root: Path, label: str) -> tuple[Path, str]:
    if not isinstance(value, dict):
        fail(f"{label} must be an object")
    exact_keys(value, {"path", "sha256"}, label)
    expected = require_sha256(value["sha256"], f"{label}.sha256")
    path = safe_file(root, value["path"], f"{label}.path")
    if sha256_file(path) != expected:
        fail(f"{label} digest mismatch")
    return path, expected


def validate_entry(
    value: object,
    root: Path,
    candidate_sha256: str,
    environment_sha256: str,
    label: str,
) -> tuple[dict, Path]:
    if not isinstance(value, dict):
        fail(f"{label} must be an object")
    expected = {
        "id",
        "kind",
        "result",
        "candidate_sha256",
        "environment_sha256",
        "path",
        "sha256",
    }
    exact_keys(value, expected, label)
    if not isinstance(value["id"], str) or not ENTRY_ID.fullmatch(value["id"]):
        fail(f"{label}.id is invalid")
    if value["kind"] not in REQUIRED_KINDS:
        fail(f"{label}.kind is unknown")
    if value["result"] != "pass":
        fail(f"{label} is not passing evidence")
    if value["candidate_sha256"] != candidate_sha256:
        fail(f"{label} is bound to a different candidate")
    if value["environment_sha256"] != environment_sha256:
        fail(f"{label} is bound to a different environment")
    expected_digest = require_sha256(value["sha256"], f"{label}.sha256")
    path = safe_file(root, value["path"], f"{label}.path")
    if sha256_file(path) != expected_digest:
        fail(f"{label} artifact digest mismatch")
    return value, path


def validate_entries(
    values: object,
    root: Path,
    candidate_sha256: str,
    environment_sha256: str,
    label: str,
) -> list[dict]:
    if not isinstance(values, list):
        fail(f"{label} must be an array")
    validated = [
        validate_entry(value, root, candidate_sha256, environment_sha256, f"{label}[{index}]")
        for index, value in enumerate(values)
    ]
    entries = [entry for entry, _ in validated]
    resolved_paths = [path for _, path in validated]
    ids = [entry["id"] for entry in entries]
    if ids != sorted(ids) or len(ids) != len(set(ids)):
        fail(f"{label} ids must be unique and sorted")
    if len(resolved_paths) != len(set(resolved_paths)):
        fail(f"{label} artifact paths must be unique")
    kinds = {entry["kind"] for entry in entries}
    if kinds != REQUIRED_KINDS:
        fail(
            f"{label} kind inventory differs: missing={sorted(REQUIRED_KINDS - kinds)}, "
            f"extra={sorted(kinds - REQUIRED_KINDS)}"
        )
    return entries


def verify(evidence_path: Path, schema_path: Path) -> None:
    evidence_path = evidence_path.resolve()
    schema_path = schema_path.resolve()
    schema = load_object(schema_path)
    validate_schema_contract(schema)
    root = evidence_path.parent.resolve()
    index = load_object(evidence_path)
    exact_keys(
        index,
        {"schema", "release_version", "schema_sha256", "candidate", "bundle", "evidence"},
        "evidence index",
    )
    if index["schema"] != EVIDENCE_SCHEMA:
        fail("unknown evidence schema version")
    if index["release_version"] != VERSION:
        fail("evidence index targets another release")
    if index["schema_sha256"] != sha256_file(schema_path):
        fail("evidence index schema digest mismatch")

    candidate_path, candidate_sha256 = validate_reference(index["candidate"], root, "candidate")
    candidate = load_object(candidate_path)
    validate_candidate(candidate)
    environment_sha256 = candidate["environment_sha256"]

    bundle_path, _ = validate_reference(index["bundle"], root, "bundle")
    bundle = load_object(bundle_path)
    exact_keys(
        bundle,
        {"schema", "release_version", "candidate_sha256", "environment_sha256", "artifacts"},
        "bundle",
    )
    if bundle["schema"] != BUNDLE_SCHEMA:
        fail("unknown bundle schema version")
    if bundle["release_version"] != VERSION:
        fail("bundle targets another release")
    if bundle["candidate_sha256"] != candidate_sha256:
        fail("bundle is bound to a different candidate")
    if bundle["environment_sha256"] != environment_sha256:
        fail("bundle is bound to a different environment")

    entries = validate_entries(
        index["evidence"], root, candidate_sha256, environment_sha256, "evidence"
    )
    bundled = validate_entries(
        bundle["artifacts"], root, candidate_sha256, environment_sha256, "bundle.artifacts"
    )
    if entries != bundled:
        fail("evidence index and bundle artifact inventories differ")
    for candidate_field, kind in CANDIDATE_DIGEST_KINDS.items():
        evidence_digests = {
            entry["sha256"] for entry in entries if entry["kind"] == kind
        }
        if evidence_digests != {candidate[candidate_field]}:
            fail(f"candidate.{candidate_field} differs from {kind} evidence")
    verify_e2e_categories(root, entries, candidate)


def verify_e2e_categories(root: Path, entries: list[dict], candidate: dict) -> None:
    """Re-evaluate complete process/fault results, not only their pass labels."""
    identities = []
    for kind, lane in (("process", "production"), ("fault", "fault")):
        # The stored release-bundle inventory is an additional process artifact;
        # publication validates its content. It cannot stand in for E2E evidence.
        reports = [entry for entry in entries if entry["kind"] == kind and entry["id"] != "release-bundle"]
        if len(reports) != 1:
            fail(f"{kind} requires exactly one complete {lane} E2E lane report")
        path = safe_file(root, reports[0]["path"], f"{kind} lane")
        try:
            identities.append(verify_lane(path, lane, candidate["source_commit"], candidate["iggy_commit"]))
        except (ValueError, OSError, KeyError, TypeError) as error:
            fail(f"{kind} E2E evidence rejected: {error}")
    production, fault = identities
    if production["source"] != fault["source"] or production["corpus"]["sha256"] != fault["corpus"]["sha256"] or production["environment"]["sha256"] != fault["environment"]["sha256"]:
        fail("production and fault E2E lanes mix source, corpus, or environment identities")


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def self_test(schema_path: Path) -> None:
    schema_path = schema_path.resolve()
    schema = load_object(schema_path)
    validate_schema_contract(schema)
    with tempfile.TemporaryDirectory(prefix="chirps-v07-evidence-self-test.") as directory:
        root = Path(directory)
        artifact_digests: dict[str, str] = {}
        for kind in sorted(REQUIRED_KINDS):
            path = root / "artifacts" / f"{kind}.json"
            write_json(path, {"schema": f"chirps.self-test.{kind}/v1"})
            artifact_digests[kind] = sha256_file(path)
        fixture_spec = importlib.util.spec_from_file_location(
            "e2e_fixtures", Path(__file__).with_name("test-v07-e2e-evidence.py")
        )
        fixture_module = importlib.util.module_from_spec(fixture_spec)
        fixture_spec.loader.exec_module(fixture_module)
        runtime_paths = {}
        for kind, lane in (("process", "production"), ("fault", "fault")):
            path = fixture_module.write_lane_fixture(root / "runtime" / lane, lane, "1" * 40, "2" * 40)
            runtime_paths[kind] = path.relative_to(root).as_posix()
            artifact_digests[kind] = sha256_file(path)
        candidate = {
            "schema": CANDIDATE_SCHEMA,
            "release_version": VERSION,
            "source_commit": "1" * 40,
            "iggy_commit": "2" * 40,
            "source_sha256": artifact_digests["source"],
            "specification_sha256": artifact_digests["specification"],
            "model_sha256": artifact_digests["model"],
            "configuration_sha256": artifact_digests["configuration"],
            "tool_sha256": artifact_digests["tool"],
            "environment_sha256": artifact_digests["environment"],
            "server_sha256": artifact_digests["server"],
            "package_graph_sha256": artifact_digests["package"],
            "performance": {
                "self_test": True,
                "secret_scan_sha256": "6" * 64,
                "resource_identity": "synthetic-self-test",
            },
        }
        candidate_path = root / "candidate.json"
        write_json(candidate_path, candidate)
        candidate_sha256 = sha256_file(candidate_path)
        entries = [
            {
                "id": f"self-test-{kind}",
                "kind": kind,
                "result": "pass",
                "candidate_sha256": candidate_sha256,
                "environment_sha256": candidate["environment_sha256"],
                "path": runtime_paths.get(kind, f"artifacts/{kind}.json"),
                "sha256": artifact_digests[kind],
            }
            for kind in sorted(REQUIRED_KINDS)
        ]
        bundle = {
            "schema": BUNDLE_SCHEMA,
            "release_version": VERSION,
            "candidate_sha256": candidate_sha256,
            "environment_sha256": candidate["environment_sha256"],
            "artifacts": entries,
        }
        bundle_path = root / "bundle.json"
        write_json(bundle_path, bundle)
        index = {
            "schema": EVIDENCE_SCHEMA,
            "release_version": VERSION,
            "schema_sha256": sha256_file(schema_path),
            "candidate": {"path": "candidate.json", "sha256": candidate_sha256},
            "bundle": {"path": "bundle.json", "sha256": sha256_file(bundle_path)},
            "evidence": entries,
        }
        index_path = root / "evidence.json"
        write_json(index_path, index)
        verify(index_path, schema_path)

        rejected: list[str] = []

        def expect_rejected(name: str, operation: Callable[[], None]) -> None:
            try:
                operation()
            except EvidenceError:
                rejected.append(name)
            else:
                fail(f"self-test negative fixture passed: {name}")

        def reject_index(name: str, mutate: Callable[[dict], None]) -> None:
            changed = copy.deepcopy(index)
            mutate(changed)
            path = root / f"negative-{name}.json"
            write_json(path, changed)
            expect_rejected(name, lambda: verify(path, schema_path))

        def rebound_candidate_index(name: str, changed_candidate: dict) -> Path:
            changed_candidate_path = root / f"negative-{name}-candidate.json"
            write_json(changed_candidate_path, changed_candidate)
            changed_candidate_sha256 = sha256_file(changed_candidate_path)
            changed_entries = copy.deepcopy(entries)
            for entry in changed_entries:
                entry["candidate_sha256"] = changed_candidate_sha256
            changed_bundle = copy.deepcopy(bundle)
            changed_bundle["candidate_sha256"] = changed_candidate_sha256
            changed_bundle["artifacts"] = changed_entries
            changed_bundle_path = root / f"negative-{name}-bundle.json"
            write_json(changed_bundle_path, changed_bundle)
            changed_index = copy.deepcopy(index)
            changed_index["candidate"] = {
                "path": changed_candidate_path.name,
                "sha256": changed_candidate_sha256,
            }
            changed_index["bundle"] = {
                "path": changed_bundle_path.name,
                "sha256": sha256_file(changed_bundle_path),
            }
            changed_index["evidence"] = changed_entries
            changed_index_path = root / f"negative-{name}.json"
            write_json(changed_index_path, changed_index)
            return changed_index_path

        reject_index("missing-field", lambda value: value.pop("evidence"))
        reject_index("missing-bundle", lambda value: value.pop("bundle"))
        reject_index("missing-schema-digest", lambda value: value.pop("schema_sha256"))
        reject_index("missing-candidate-digest", lambda value: value["candidate"].pop("sha256"))
        reject_index("missing-artifact-digest", lambda value: value["evidence"][0].pop("sha256"))
        reject_index("missing-bundle-digest", lambda value: value["bundle"].pop("sha256"))
        reject_index(
            "mixed-candidate",
            lambda value: value["evidence"][0].update(candidate_sha256="3" * 64),
        )
        reject_index(
            "mixed-environment",
            lambda value: value["evidence"][0].update(environment_sha256="4" * 64),
        )
        reject_index(
            "unknown-version",
            lambda value: value.update(schema="chirps.v0.7.evidence/v2"),
        )
        reject_index(
            "path-traversal",
            lambda value: value["evidence"][0].update(path="../outside.json"),
        )
        reject_index(
            "canonical-path-alias",
            lambda value: value["evidence"][1].update(
                path=f"./{value['evidence'][0]['path']}",
                sha256=value["evidence"][0]["sha256"],
            ),
        )

        symlink_path = root / "artifacts" / "symlink.json"
        symlink_path.symlink_to(root / entries[0]["path"])
        reject_index(
            "symlink-traversal",
            lambda value: value["evidence"][0].update(
                path="artifacts/symlink.json",
                sha256=entries[0]["sha256"],
            ),
        )
        symlink_path.unlink()

        secret_candidate = copy.deepcopy(candidate)
        secret_candidate["performance"] = {"access_token": "self-test-canary"}
        secret_index_path = rebound_candidate_index("secret-field", secret_candidate)
        expect_rejected("secret-field", lambda: verify(secret_index_path, schema_path))

        unknown_candidate = copy.deepcopy(candidate)
        unknown_candidate["schema"] = "chirps.v0.7.candidate/v2"
        unknown_candidate_index_path = rebound_candidate_index(
            "unknown-candidate-version", unknown_candidate
        )
        expect_rejected(
            "unknown-candidate-version",
            lambda: verify(unknown_candidate_index_path, schema_path),
        )

        unknown_bundle = copy.deepcopy(bundle)
        unknown_bundle["schema"] = "chirps.v0.7.bundle/v2"
        unknown_bundle_path = root / "negative-unknown-bundle-version-bundle.json"
        write_json(unknown_bundle_path, unknown_bundle)
        unknown_bundle_index = copy.deepcopy(index)
        unknown_bundle_index["bundle"] = {
            "path": unknown_bundle_path.name,
            "sha256": sha256_file(unknown_bundle_path),
        }
        unknown_bundle_index_path = root / "negative-unknown-bundle-version.json"
        write_json(unknown_bundle_index_path, unknown_bundle_index)
        expect_rejected(
            "unknown-bundle-version",
            lambda: verify(unknown_bundle_index_path, schema_path),
        )
        expect_rejected(
            "missing-configuration-digest",
            lambda: validate_candidate(
                {
                    key: value
                    for key, value in candidate.items()
                    if key != "configuration_sha256"
                }
            ),
        )
        expect_rejected(
            "missing-environment-digest",
            lambda: validate_candidate(
                {
                    key: value
                    for key, value in candidate.items()
                    if key != "environment_sha256"
                }
            ),
        )

        mixed_candidate = copy.deepcopy(candidate)
        mixed_candidate["configuration_sha256"] = "5" * 64
        write_json(candidate_path, mixed_candidate)
        mixed_candidate_sha256 = sha256_file(candidate_path)
        mixed_entries = copy.deepcopy(entries)
        for entry in mixed_entries:
            entry["candidate_sha256"] = mixed_candidate_sha256
        mixed_bundle = copy.deepcopy(bundle)
        mixed_bundle["candidate_sha256"] = mixed_candidate_sha256
        mixed_bundle["artifacts"] = mixed_entries
        write_json(bundle_path, mixed_bundle)
        mixed_index = copy.deepcopy(index)
        mixed_index["candidate"]["sha256"] = mixed_candidate_sha256
        mixed_index["bundle"]["sha256"] = sha256_file(bundle_path)
        mixed_index["evidence"] = mixed_entries
        mixed_index_path = root / "negative-mixed-configuration.json"
        write_json(mixed_index_path, mixed_index)
        expect_rejected(
            "mixed-configuration",
            lambda: verify(mixed_index_path, schema_path),
        )
        write_json(candidate_path, candidate)
        write_json(bundle_path, bundle)

        artifact_path = root / entries[0]["path"]
        original_artifact = artifact_path.read_bytes()
        artifact_path.write_bytes(original_artifact + b"tampered")
        expect_rejected("tampered-artifact", lambda: verify(index_path, schema_path))
        artifact_path.write_bytes(original_artifact)

        original_bundle = bundle_path.read_bytes()
        bundle_path.write_bytes(original_bundle + b" ")
        expect_rejected("tampered-bundle", lambda: verify(index_path, schema_path))
        bundle_path.write_bytes(original_bundle)

        # Rebind all outer hashes after replacing a runtime report. This must
        # fail on execution semantics, not on the already-tested byte binding.
        changed_entries = copy.deepcopy(entries)
        runtime_entry = next(entry for entry in changed_entries if entry["kind"] == "fault")
        fake_path = root / "forged-pass.json"
        write_json(fake_path, {"schema": "chirps.v0.7.e2e-lane/v1", "lane": "fault", "targets": {}})
        runtime_entry.update(path=fake_path.name, sha256=sha256_file(fake_path))
        fake_bundle = copy.deepcopy(bundle)
        fake_bundle["artifacts"] = changed_entries
        fake_bundle_path = root / "forged-pass-bundle.json"
        write_json(fake_bundle_path, fake_bundle)
        fake_index = copy.deepcopy(index)
        fake_index["evidence"] = changed_entries
        fake_index["bundle"] = {"path": fake_bundle_path.name, "sha256": sha256_file(fake_bundle_path)}
        fake_index_path = root / "forged-pass-index.json"
        write_json(fake_index_path, fake_index)
        expect_rejected("forged-runtime-pass", lambda: verify(fake_index_path, schema_path))

        expected = {
            "forged-runtime-pass",
            "canonical-path-alias",
            "missing-field",
            "missing-bundle",
            "missing-schema-digest",
            "missing-candidate-digest",
            "missing-artifact-digest",
            "missing-bundle-digest",
            "mixed-candidate",
            "mixed-configuration",
            "mixed-environment",
            "unknown-version",
            "missing-configuration-digest",
            "missing-environment-digest",
            "path-traversal",
            "secret-field",
            "symlink-traversal",
            "tampered-artifact",
            "tampered-bundle",
            "unknown-bundle-version",
            "unknown-candidate-version",
        }
        if set(rejected) != expected:
            fail("self-test did not execute the exact negative fixture inventory")
        print(f"v0.7 evidence schema self-test passed: {len(rejected)} negative fixtures")


def default_schema_path() -> Path:
    return Path(__file__).resolve().parents[2] / "docs/release/v0.7.0-evidence-schema.json"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("evidence", nargs="?", type=Path)
    parser.add_argument("--schema", type=Path, default=default_schema_path())
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    try:
        if args.self_test:
            if args.evidence is None:
                fail("--self-test requires the schema path as its positional argument")
            self_test(args.evidence)
        else:
            if args.evidence is None:
                fail("evidence index path is required")
            verify(args.evidence, args.schema)
            print(f"v0.7 evidence bundle validated: {args.evidence}")
    except EvidenceError as exc:
        print(f"v0.7 evidence rejected: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
