#!/usr/bin/env python3
"""Strict verification for a version-bound Chirps release evidence bundle."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

SHA40 = re.compile(r"^[0-9a-f]{40}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")


def fail(message: str) -> None:
    raise ValueError(message)


def load_object(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"cannot read JSON {path}: {exc}")
    if not isinstance(value, dict):
        fail(f"JSON root must be an object: {path}")
    return value


def exact_keys(value: dict, expected: set[str], label: str) -> None:
    actual = set(value)
    if actual != expected:
        fail(f"{label} fields differ: missing={sorted(expected - actual)}, extra={sorted(actual - expected)}")


def verify_artifact(entry: object, *, label: str, version: str, commit: str, bundle: Path) -> str:
    if not isinstance(entry, dict):
        fail(f"{label} must be an object")
    exact_keys(entry, {"id", "release_version", "source_commit", "result", "path", "sha256"}, label)
    evidence_id = entry["id"]
    if not isinstance(evidence_id, str) or not evidence_id:
        fail(f"{label}.id must be a non-empty string")
    if entry["release_version"] != version:
        fail(f"{label} targets {entry['release_version']!r}, expected {version!r}")
    if entry["source_commit"] != commit:
        fail(f"{label} is stale or from another commit")
    if entry["result"] != "pass":
        fail(f"{label} did not pass")
    rel = entry["path"]
    if not isinstance(rel, str) or not rel or Path(rel).is_absolute():
        fail(f"{label}.path must be relative to the manifest")
    artifact = (bundle / rel).resolve()
    try:
        artifact.relative_to(bundle)
    except ValueError:
        fail(f"{label}.path escapes the evidence bundle")
    if not artifact.is_file():
        fail(f"{label} artifact is missing: {rel}")
    expected_digest = entry["sha256"]
    if not isinstance(expected_digest, str) or not SHA256.fullmatch(expected_digest):
        fail(f"{label}.sha256 is invalid")
    actual_digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    if actual_digest != expected_digest:
        fail(f"{label} digest mismatch: {rel}")
    return evidence_id


def verify(args: argparse.Namespace) -> None:
    manifest_path = args.manifest.resolve()
    requirements_path = args.requirements.resolve()
    schema_path = args.schema.resolve()
    manifest = load_object(manifest_path)
    requirements = load_object(requirements_path)
    schema = load_object(schema_path)

    if not SHA40.fullmatch(args.source_commit):
        fail("--source-commit must be a lowercase 40-character commit SHA")
    if schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema":
        fail("unsupported or malformed manifest schema")
    schema_properties = schema.get("properties", {})
    if (
        schema.get("type") != "object"
        or schema.get("additionalProperties") is not False
        or schema_properties.get("schema", {}).get("const") != "chirps.release-evidence/v1"
        or schema_properties.get("release_version", {}).get("const") != args.version
        or schema_properties.get("contract", {}).get("const") != requirements.get("contract")
    ):
        fail("manifest schema does not encode the selected release contract")
    if requirements.get("schema") != "chirps.release-required-evidence/v1":
        fail("unsupported required-evidence catalog")
    if requirements.get("release_version") != args.version:
        fail("required-evidence catalog targets another version")

    exact_keys(manifest, {"schema", "release_version", "source_commit", "contract", "target_gate", "evidence"}, "manifest")
    if manifest["schema"] != "chirps.release-evidence/v1":
        fail("unsupported evidence manifest schema identifier")
    if manifest["release_version"] != args.version:
        fail("evidence manifest targets another version")
    if manifest["source_commit"] != args.source_commit:
        fail("evidence manifest is stale or from another commit")
    if manifest["contract"] != requirements.get("contract"):
        fail("evidence manifest points to the wrong release contract")

    bundle = manifest_path.parent.resolve()
    gate_id = verify_artifact(
        manifest["target_gate"], label="target_gate", version=args.version,
        commit=args.source_commit, bundle=bundle
    )
    if gate_id != requirements.get("target_gate_id"):
        fail("target-version gate id does not match the required catalog")

    required_records = requirements.get("required_evidence")
    if not isinstance(required_records, list) or not required_records:
        fail("required-evidence catalog has no entries")
    required_ids: list[str] = []
    for item in required_records:
        if not isinstance(item, dict) or not isinstance(item.get("id"), str):
            fail("required-evidence catalog contains a malformed entry")
        required_ids.append(item["id"])
    if len(required_ids) != len(set(required_ids)):
        fail("required-evidence catalog contains duplicate ids")

    entries = manifest["evidence"]
    if not isinstance(entries, list):
        fail("manifest.evidence must be an array")
    actual_ids = [
        verify_artifact(entry, label=f"evidence[{index}]", version=args.version,
                        commit=args.source_commit, bundle=bundle)
        for index, entry in enumerate(entries)
    ]
    if len(actual_ids) != len(set(actual_ids)):
        fail("evidence manifest contains duplicate ids")
    if set(actual_ids) != set(required_ids):
        fail(
            "evidence set differs from the target-version catalog: "
            f"missing={sorted(set(required_ids) - set(actual_ids))}, "
            f"extra={sorted(set(actual_ids) - set(required_ids))}"
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--requirements", type=Path, required=True)
    parser.add_argument("--schema", type=Path, required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--source-commit", required=True)
    args = parser.parse_args()
    try:
        verify(args)
    except ValueError as exc:
        print(f"release evidence rejected: {exc}", file=sys.stderr)
        return 1
    print(f"release evidence validated: {args.manifest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
