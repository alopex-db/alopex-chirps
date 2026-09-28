#!/usr/bin/env python3
"""Synthetic rejection fixtures; not evidence of legacy interoperability."""
import copy
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.dont_write_bytecode = True
import v07_wire_evidence as wire


class WireTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)
        self.source_root = Path(__file__).resolve().parents[2]
        self.commit = wire.command_output(self.source_root, "git", "rev-parse", "HEAD")
        lock = subprocess.check_output(["git", "show", f"{self.commit}:Cargo.lock"], cwd=self.source_root)
        self.report = {"schema": wire.SCHEMA, "result": "pass", "rustc": "synthetic", "cargo": "synthetic",
            "source": {"source_commit": self.commit,
                "source_tree": wire.command_output(self.source_root, "git", "rev-parse", "HEAD^{tree}"),
                "lock_sha256": hashlib.sha256(lock).hexdigest()}, "jobs": {}}
        for mode in wire.FEATURES:
            for target, required in wire.TARGETS.items():
                directory = self.root / mode / target
                directory.mkdir(parents=True)
                tests = sorted(required)
                (directory / "build.log").write_text(json.dumps({"reason": "compiler-artifact", "target": {"name": target}, "profile": {"test": True}, "executable": "/synthetic/test"}) + '\n{"reason":"build-finished","success":true}\n')
                (directory / "list.log").write_text("\n".join(f"{name}: test" for name in tests))
                (directory / "run.log").write_text("\n".join(f"test {name} ... ok" for name in tests) + f"\ntest result: ok. {len(tests)} passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.1s\n")
                self.report["jobs"][f"{mode}/{target}"] = {"commands": wire.commands(mode, target),
                    "exit_codes": dict.fromkeys(("build", "list", "run"), 0), "tests": tests,
                    "binary_sha256": "a" * 64,
                    "logs": {stage: wire.reference(directory / f"{stage}.log", self.root) for stage in ("build", "list", "run")}}
        self.path = self.root / "report.json"
        self.identifier = "default/profile_compatibility"

    def verify(self, value=None):
        self.path.write_text(json.dumps(self.report if value is None else value))
        return wire.verify_report(self.source_root, self.path, self.commit)

    def test_complete_matrix_passes(self):
        self.assertEqual(len(self.verify()["jobs"]), 8)

    def test_stale_source_or_lock_is_rejected(self):
        for field in ("source_commit", "source_tree", "lock_sha256"):
            changed = copy.deepcopy(self.report)
            changed["source"][field] = "b" * len(changed["source"][field])
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.verify(changed)

    def test_missing_cell_or_altered_feature_command_is_rejected(self):
        changed = copy.deepcopy(self.report)
        del changed["jobs"][self.identifier]
        with self.assertRaises(ValueError):
            self.verify(changed)
        self.report["jobs"][self.identifier]["commands"]["build"].append("--no-default-features")
        with self.assertRaises(ValueError):
            self.verify()

    def test_failure_and_boolean_exit_are_not_success(self):
        for code in (1, True, None):
            self.report["jobs"][self.identifier]["exit_codes"]["run"] = code
            with self.subTest(code=code), self.assertRaises(ValueError):
                self.verify()

    def test_rehashed_failed_log_is_rejected(self):
        job = self.report["jobs"][self.identifier]
        log = self.root / job["logs"]["run"]["path"]
        log.write_text(log.read_text().replace("... ok", "... FAILED", 1))
        job["logs"]["run"] = wire.reference(log, self.root)
        with self.assertRaises(ValueError):
            self.verify()

    def test_rehashed_inventory_cannot_omit_the_golden(self):
        job = self.report["jobs"][self.identifier]
        names = [name for name in job["tests"] if "golden" not in name]
        log = self.root / job["logs"]["list"]["path"]
        log.write_text("\n".join(f"{name}: test" for name in names))
        job["logs"]["list"] = wire.reference(log, self.root)
        job["tests"] = names
        with self.assertRaises(ValueError):
            self.verify()


if __name__ == "__main__":
    unittest.main()
