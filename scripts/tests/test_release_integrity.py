#!/usr/bin/env python3
from __future__ import annotations

import copy
import hashlib
import json
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
MANIFEST_VERIFIER = REPO / "scripts/release/verify-evidence-manifest.py"
REGISTRY_VERIFIER = REPO / "scripts/release/verify-registry-dependency.py"
COMMIT = "a" * 40


class EvidenceManifestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        (self.root / "one.json").write_text('{"real": true}\n', encoding="utf-8")
        (self.root / "gate.json").write_text('{"result": "pass"}\n', encoding="utf-8")
        self.catalog = self.root / "required.json"
        self.schema = self.root / "schema.json"
        self.manifest = self.root / "manifest.json"
        self.catalog.write_text(
            json.dumps(
                {
                    "schema": "chirps.release-required-evidence/v1",
                    "release_version": "0.6.0",
                    "contract": "docs/release/v0.6.0.md",
                    "target_gate_id": "chirps-v0.6-target-gate",
                    "required_evidence": [{"id": "one", "source_path": "unused"}],
                }
            ),
            encoding="utf-8",
        )
        self.schema.write_text(
            (REPO / "docs/release/evidence/v0.6.0/manifest.schema.json").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        self.value = {
            "schema": "chirps.release-evidence/v1",
            "release_version": "0.6.0",
            "source_commit": COMMIT,
            "contract": "docs/release/v0.6.0.md",
            "target_gate": self.entry("chirps-v0.6-target-gate", "gate.json"),
            "evidence": [self.entry("one", "one.json")],
        }

    def tearDown(self) -> None:
        self.temp.cleanup()

    def entry(self, evidence_id: str, path: str) -> dict:
        artifact = self.root / path
        return {
            "id": evidence_id,
            "release_version": "0.6.0",
            "source_commit": COMMIT,
            "result": "pass",
            "path": path,
            "sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
        }

    def run_verifier(self, value: dict, *, expected: int) -> subprocess.CompletedProcess[str]:
        self.manifest.write_text(json.dumps(value), encoding="utf-8")
        completed = subprocess.run(
            [
                "python3", str(MANIFEST_VERIFIER), "--manifest", str(self.manifest),
                "--requirements", str(self.catalog), "--schema", str(self.schema),
                "--version", "0.6.0", "--source-commit", COMMIT,
            ],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(expected, completed.returncode, completed.stderr)
        return completed

    def test_accepts_exact_target_bound_bundle(self) -> None:
        self.run_verifier(self.value, expected=0)

    def test_rejects_other_version(self) -> None:
        value = copy.deepcopy(self.value)
        value["release_version"] = "0.5.2"
        self.run_verifier(value, expected=1)

    def test_rejects_stale_commit(self) -> None:
        value = copy.deepcopy(self.value)
        value["source_commit"] = "b" * 40
        self.run_verifier(value, expected=1)

    def test_rejects_digest_mismatch(self) -> None:
        value = copy.deepcopy(self.value)
        value["evidence"][0]["sha256"] = "0" * 64
        self.run_verifier(value, expected=1)

    def test_rejects_missing_required_evidence(self) -> None:
        value = copy.deepcopy(self.value)
        value["evidence"] = []
        self.run_verifier(value, expected=1)

    def test_rejects_other_version_gate(self) -> None:
        value = copy.deepcopy(self.value)
        value["target_gate"]["release_version"] = "0.5.2"
        self.run_verifier(value, expected=1)

    def test_rejects_failed_gate(self) -> None:
        value = copy.deepcopy(self.value)
        value["target_gate"]["result"] = "fail"
        self.run_verifier(value, expected=1)

    def test_rejects_bundle_escape(self) -> None:
        outside = self.root.parent / "outside-release-evidence.json"
        outside.write_text("outside\n", encoding="utf-8")
        self.addCleanup(outside.unlink)
        value = copy.deepcopy(self.value)
        value["evidence"][0]["path"] = "../outside-release-evidence.json"
        value["evidence"][0]["sha256"] = hashlib.sha256(outside.read_bytes()).hexdigest()
        self.run_verifier(value, expected=1)


class RegistryDependencyTests(unittest.TestCase):
    def command(self, manifest: Path) -> list[str]:
        return [
            "python3", str(REGISTRY_VERIFIER),
            "--root-manifest", str(manifest),
            "--root-lock", str(REPO / "Cargo.lock"),
            "--fixture", str(REPO / "scripts/fixtures/alopex-core-registry-check"),
            "--schema", str(REPO / "docs/release/evidence/v0.6.0/registry-dependency.schema.json"),
            "--source-commit", COMMIT,
        ]

    def test_accepts_registry_requirement_and_lock(self) -> None:
        completed = subprocess.run(
            self.command(REPO / "crates/chirps-raft-storage/Cargo.toml"),
            text=True, capture_output=True, check=False,
        )
        self.assertEqual(0, completed.returncode, completed.stderr)

    def test_rejects_path_dependency(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "Cargo.toml"
            manifest.write_text(
                '[package]\nname="bad"\nversion="0.0.0"\n'
                '[dependencies]\nalopex-core={version="0.3",path="../alopex"}\n',
                encoding="utf-8",
            )
            completed = subprocess.run(
                self.command(manifest), text=True, capture_output=True, check=False
            )
            self.assertEqual(1, completed.returncode)
            self.assertIn("path/git/table dependencies are rejected", completed.stderr)


if __name__ == "__main__":
    unittest.main()
