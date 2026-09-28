#!/usr/bin/env python3
"""Preflight and assemble a cryptographically bound release evidence bundle."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import sys
from pathlib import Path

SHA40 = re.compile(r"^[0-9a-f]{40}$")


def fail(message: str) -> None:
    raise ValueError(message)


def load(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"cannot read JSON {path}: {exc}")
    if not isinstance(value, dict):
        fail(f"JSON root must be an object: {path}")
    return value


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def catalog_sources(repo: Path, catalog: dict) -> list[tuple[str, Path]]:
    result: list[tuple[str, Path]] = []
    for record in catalog.get("required_evidence", []):
        evidence_id = record.get("id")
        if not isinstance(evidence_id, str) or not evidence_id:
            fail("required-evidence catalog has a malformed id")
        source_path = record.get("source_path")
        if source_path is None:
            continue
        if not isinstance(source_path, str) or Path(source_path).is_absolute():
            fail(f"invalid source_path for {evidence_id}")
        source = (repo / source_path).resolve()
        try:
            source.relative_to(repo)
        except ValueError:
            fail(f"source_path escapes repository for {evidence_id}")
        if not source.is_file():
            fail(f"required release evidence is missing: {source_path}")
        result.append((evidence_id, source))
    return result


def manifest_entry(evidence_id: str, artifact: Path, relative: Path, commit: str, version: str) -> dict:
    return {
        "id": evidence_id,
        "release_version": version,
        "source_commit": commit,
        "result": "pass",
        "path": relative.as_posix(),
        "sha256": digest(artifact),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--source-commit")
    parser.add_argument("--registry-evidence", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--target-command", action="append", default=[])
    parser.add_argument("--preflight", action="store_true")
    args = parser.parse_args()
    try:
        repo = args.repo_root.resolve()
        catalog = load(args.catalog)
        commit_version = args.version
        if catalog.get("release_version") != commit_version:
            fail("required-evidence catalog targets another release version")
        sources = catalog_sources(repo, catalog)
        if args.preflight:
            print(f"release evidence preflight passed: {len(sources)} checked-in artifacts")
            return 0

        if not args.source_commit or not SHA40.fullmatch(args.source_commit):
            fail("--source-commit must be a lowercase 40-character commit SHA")
        if not args.registry_evidence or not args.output_dir:
            fail("--registry-evidence and --output-dir are required for assembly")
        registry = load(args.registry_evidence)
        if (
            registry.get("schema") != "chirps.registry-dependency-evidence/v1"
            or registry.get("release_version") != commit_version
            or registry.get("source_commit") != args.source_commit
            or registry.get("result") != "pass"
        ):
            fail("registry evidence is stale, failed, or targets another version")

        output = args.output_dir.resolve()
        output.mkdir(parents=True, exist_ok=True)
        if any(output.iterdir()):
            fail(f"output directory must be empty: {output}")
        artifacts = output / "artifacts"
        artifacts.mkdir()

        entries: list[dict] = []
        for evidence_id, source in sources:
            destination = artifacts / f"{evidence_id}.json"
            shutil.copyfile(source, destination)
            entries.append(
                manifest_entry(evidence_id, destination, destination.relative_to(output), args.source_commit, commit_version)
            )

        registry_destination = artifacts / "alopex-core-registry.json"
        shutil.copyfile(args.registry_evidence, registry_destination)
        entries.append(
            manifest_entry(
                "alopex-core-registry", registry_destination,
                registry_destination.relative_to(output), args.source_commit, commit_version
            )
        )

        target_report = output / "target-gate.json"
        target_commands = args.target_command or []
        target_report.write_text(
            json.dumps(
                {
                    "schema": "chirps.target-version-gate/v1",
                    "release_version": commit_version,
                    "source_commit": args.source_commit,
                    "result": "pass",
                    "commands": target_commands,
                },
                indent=2,
                sort_keys=True,
            ) + "\n",
            encoding="utf-8",
        )
        target_entry = manifest_entry(
            catalog["target_gate_id"], target_report,
            target_report.relative_to(output), args.source_commit, commit_version
        )
        manifest = {
            "schema": "chirps.release-evidence/v1",
            "release_version": commit_version,
            "source_commit": args.source_commit,
            "contract": catalog["contract"],
            "target_gate": target_entry,
            "evidence": entries,
        }
        (output / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    except (OSError, ValueError) as exc:
        print(f"release evidence bundle rejected: {exc}", file=sys.stderr)
        return 1
    print(f"release evidence bundle assembled: {args.output_dir / 'manifest.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
