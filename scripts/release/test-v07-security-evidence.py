#!/usr/bin/env python3
"""Synthetic diagnostics test checker composition, never qualify a server."""
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import v07_security_evidence as security
from v07_e2e_evidence import digest, reference

spec = importlib.util.spec_from_file_location("e2e_tests", Path(__file__).with_name("test-v07-e2e-evidence.py"))
fixtures = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixtures)


class SecurityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = "1" * 40
        self.iggy = "2" * 40
        self.reports = {}
        self.canaries = [b"synthetic-credential-a", b"synthetic-credential-b"]
        for lane in ("production", "fault"):
            fixture = fixtures.EvidenceTests()
            fixture.root = self.root / lane
            fixture.root.mkdir()
            fixture.source, fixture.iggy = self.source, self.iggy
            fixture.test_names = ["diagnostics_are_authorized_and_secret_free_in_fresh_processes"]
            path, report = fixture.fixture("durable_diagnostics", lane)
            # The generic composition fixture uses two tests in its summary.
            run = path.parent / "run.log"
            run.write_text(run.read_text().replace("2 passed", "1 passed"))
            report["logs"]["run"] = reference(run, path.parent)
            binary = "8" * 64 if lane == "production" else "9" * 64
            report["server"]["binary_sha256"] = binary
            rows = [{**fixture.scenario(lane, "durable_diagnostics"), "artifact_sha256": binary,
                     "scenario": name, "verdict": verdict} for name, verdict in security.expected_scenarios(lane).items()]
            scenarios = path.parent / "scenarios.jsonl"
            scenarios.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
            report["scenarios"] = reference(scenarios, path.parent)
            path.write_text(json.dumps(report))
            self.reports[lane] = path

    def inspect(self):
        with patch.object(security, "source_canaries", return_value=self.canaries):
            return security.inspect(self.root, self.source, self.iggy, self.reports)

    def alter_scenarios(self, lane, change):
        path = self.reports[lane]
        report = json.loads(path.read_bytes())
        scenarios = path.parent / "scenarios.jsonl"
        rows = [json.loads(line) for line in scenarios.read_bytes().splitlines()]
        change(rows)
        scenarios.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
        report["scenarios"] = reference(scenarios, path.parent)
        path.write_text(json.dumps(report))

    def test_full_synthetic_diagnostics_and_scan(self):
        result = self.inspect()
        self.assertEqual(result["scans"]["production"]["scenario_count"], 23)
        self.assertEqual(result["scans"]["fault"]["scenario_count"], 4)
        self.assertEqual(result["negative_controls_detected"], 2)

    def test_missing_auth_cell_rejected_even_after_rehash(self):
        self.alter_scenarios("production", lambda rows: rows.pop())
        with self.assertRaises(ValueError):
            self.inspect()

    def test_duplicate_or_false_control_rejected(self):
        self.alter_scenarios("fault", lambda rows: rows.append(copy.deepcopy(rows[0])))
        with self.assertRaises(ValueError):
            self.inspect()

    def test_wrong_verdict_rejected(self):
        self.alter_scenarios("production", lambda rows: rows[0].update(verdict="accepted"))
        with self.assertRaises(ValueError):
            self.inspect()

    def test_leaked_runtime_canary_rejected_without_echo(self):
        path = self.reports["production"]
        report = json.loads(path.read_bytes())
        run = path.parent / "run.log"
        with run.open("ab") as stream:
            stream.write(b"\n" + self.canaries[0] + b"\n")
        report["logs"]["run"] = reference(run, path.parent)
        path.write_text(json.dumps(report))
        with self.assertRaises(ValueError) as caught:
            self.inspect()
        self.assertNotIn(self.canaries[0].decode(), str(caught.exception))

    def test_disabled_scanner_fails_live_negative_control(self):
        with patch.object(security, "scan", return_value=None), self.assertRaisesRegex(ValueError, "negative control"):
            self.inspect()

    def test_outer_report_scan_is_recomputed(self):
        report = {"schema": security.SCHEMA, "source_commit": self.source, "iggy_commit": self.iggy,
                  **{lane: reference(path, self.root) for lane, path in self.reports.items()}, "scan": self.inspect()}
        path = self.root / "security.json"
        path.write_text(json.dumps(report))
        with patch.object(security, "source_canaries", return_value=self.canaries):
            security.verify(self.root, path, self.source, self.iggy)
            report["scan"]["scans"]["production"]["scenario_count"] = 1
            path.write_text(json.dumps(report))
            with self.assertRaises(ValueError):
                security.verify(self.root, path, self.source, self.iggy)

    def test_immutable_candidate_canaries_include_encoded_forms(self):
        root = Path(__file__).resolve().parents[2]
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
        canaries = security.source_canaries(root, commit)
        self.assertEqual(len(canaries), 21)
        for canary in canaries:
            with self.assertRaises(ValueError):
                security.scan(b"surrounding " + canary + b" bytes", canaries)

    def test_oversize_scan_is_rejected(self):
        path = self.root / "oversize.log"
        path.write_bytes(b"12345")
        with patch.object(security, "MAX_LOG_BYTES", 4), self.assertRaises(ValueError):
            security.bounded_bytes(path)


if __name__ == "__main__":
    unittest.main()
