#!/usr/bin/env python3
"""Validate and attest the alopex-core registry-only dependency contract."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

EXPECTED_VERSION = "0.3.4"
EXPECTED_REQUIREMENT = "0.3"
EXPECTED_SOURCE = "registry+https://github.com/rust-lang/crates.io-index"
EXPECTED_CHECKSUM = "f91246870a303fd5bde67d9d9871295ce38a389da21a167044db847f47d1866d"
SHA40 = re.compile(r"^[0-9a-f]{40}$")


def fail(message: str) -> None:
    raise ValueError(message)


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        fail(f"cannot read TOML {path}: {exc}")


def dependency_value(manifest: str, name: str) -> str | None:
    section = ""
    for raw_line in manifest.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if line.startswith("[") and line.endswith("]"):
            section = line
            continue
        if section != "[dependencies]":
            continue
        match = re.fullmatch(rf"{re.escape(name)}\s*=\s*\"([^\"]+)\"", line)
        if match:
            return match.group(1)
        if re.match(rf"{re.escape(name)}\s*=", line):
            return "<non-string dependency>"
    return None


def lock_packages(lock: str) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    for chunk in lock.split("[[package]]")[1:]:
        record: dict[str, str] = {}
        for line in chunk.splitlines():
            match = re.fullmatch(r'(name|version|source|checksum)\s*=\s*"([^"]+)"', line.strip())
            if match:
                record[match.group(1)] = match.group(2)
        records.append(record)
    return records


def package(lock: str, name: str, version: str) -> dict[str, str]:
    matches = [
        item for item in lock_packages(lock)
        if item.get("name") == name and item.get("version") == version
    ]
    if len(matches) != 1:
        fail(f"expected one {name} {version} entry in Cargo.lock, got {len(matches)}")
    return matches[0]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_tree_sha256(crate: Path) -> str:
    digest = hashlib.sha256()
    paths = [crate / "Cargo.toml", *sorted((crate / "src").rglob("*.rs"))]
    for path in paths:
        relative = path.relative_to(crate).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        contents = path.read_bytes()
        digest.update(len(contents).to_bytes(8, "big"))
        digest.update(contents)
    return digest.hexdigest()


def validate(args: argparse.Namespace) -> dict:
    try:
        schema = json.loads(args.schema.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"cannot read registry evidence schema: {exc}")
    properties = schema.get("properties", {}) if isinstance(schema, dict) else {}
    dependency_properties = properties.get("dependency", {}).get("properties", {})
    if (
        schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema"
        or schema.get("additionalProperties") is not False
        or properties.get("schema", {}).get("const") != "chirps.registry-dependency-evidence/v1"
        or properties.get("release_version", {}).get("const") != args.release_version
        or properties.get("command", {}).get("const")
        != "cargo build --locked -p alopex-chirps-raft-storage"
        or dependency_properties.get("requirement", {}).get("const") != EXPECTED_REQUIREMENT
        or dependency_properties.get("resolved_version", {}).get("const") != EXPECTED_VERSION
        or dependency_properties.get("source", {}).get("const") != EXPECTED_SOURCE
        or dependency_properties.get("checksum", {}).get("const") != EXPECTED_CHECKSUM
    ):
        fail("registry evidence schema does not encode the expected dependency contract")

    root_manifest = read_text(args.root_manifest)
    dependency = dependency_value(root_manifest, "alopex-core")
    if dependency != EXPECTED_REQUIREMENT:
        fail(
            "chirps-raft-storage must use the registry SemVer requirement "
            f"alopex-core = {EXPECTED_REQUIREMENT!r}; path/git/table dependencies are rejected"
        )

    root_entry = package(read_text(args.root_lock), "alopex-core", EXPECTED_VERSION)
    for field, expected in (("source", EXPECTED_SOURCE), ("checksum", EXPECTED_CHECKSUM)):
        if root_entry.get(field) != expected:
            fail(f"workspace lock has unexpected alopex-core {field}")

    fixture_crate = args.fixture / "crates" / "chirps-raft-storage"
    fixture_manifest_path = fixture_crate / "Cargo.toml"
    fixture_manifest = read_text(fixture_manifest_path)
    if fixture_manifest != root_manifest:
        fail("isolated fixture manifest diverges from production chirps-raft-storage")
    fixture_dependency = dependency_value(fixture_manifest, "alopex-core")
    if fixture_dependency != EXPECTED_REQUIREMENT:
        fail("isolated fixture must preserve the production alopex-core SemVer requirement")
    fixture_entry = package(read_text(args.fixture / "Cargo.lock"), "alopex-core", EXPECTED_VERSION)
    for field, expected in (("source", EXPECTED_SOURCE), ("checksum", EXPECTED_CHECKSUM)):
        if fixture_entry.get(field) != expected:
            fail(f"isolated fixture lock has unexpected alopex-core {field}")

    if not SHA40.fullmatch(args.source_commit):
        fail("--source-commit must be a lowercase 40-character commit SHA")
    source = fixture_crate / "src" / "lib.rs"
    if not source.is_file():
        fail("isolated fixture source is missing")

    return {
        "schema": "chirps.registry-dependency-evidence/v1",
        "release_version": args.release_version,
        "source_commit": args.source_commit,
        "result": "pass",
        "dependency": {
            "crate": "alopex-core",
            "requirement": EXPECTED_REQUIREMENT,
            "resolved_version": EXPECTED_VERSION,
            "source": EXPECTED_SOURCE,
            "checksum": EXPECTED_CHECKSUM,
        },
        "fixture": {
            "manifest_sha256": sha256(fixture_manifest_path),
            "lock_sha256": sha256(args.fixture / "Cargo.lock"),
            "source_sha256": source_tree_sha256(fixture_crate),
        },
        "command": "cargo build --locked -p alopex-chirps-raft-storage",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root-manifest", type=Path, required=True)
    parser.add_argument("--root-lock", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--schema", type=Path, required=True)
    parser.add_argument("--release-version", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        evidence = validate(args)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except (OSError, ValueError) as exc:
        print(f"registry dependency rejected: {exc}", file=sys.stderr)
        return 1
    print("alopex-core registry dependency validated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
