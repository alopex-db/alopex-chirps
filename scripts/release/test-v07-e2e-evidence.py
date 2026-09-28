#!/usr/bin/env python3
"""Synthetic fixtures test rejection behavior, never constitute release proof."""

import copy
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location("e2e", Path(__file__).with_name("v07_e2e_evidence.py"))
e2e = importlib.util.module_from_spec(spec)
spec.loader.exec_module(e2e)


def write(path, value):
    path.write_text(json.dumps(value))


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)
        self.source = "1" * 40
        self.iggy = "2" * 40
        self.test_names = ["integration::accept", "integration::reject"]

    def fixture(self, target="durable_send", lane="fault"):
        directory = self.root / target
        directory.mkdir()
        build = [
            {"reason": "compiler-artifact", "target": {"name": target}, "profile": {"test": True}, "executable": "/synthetic/test"},
            {"reason": "build-finished", "success": True},
        ]
        (directory / "build.log").write_text("\n".join(json.dumps(item) for item in build))
        (directory / "list.log").write_text("\n".join(f"{name}: test" for name in self.test_names))
        (directory / "run.log").write_text(
            "\n".join(f"test {name} ... ok" for name in self.test_names)
            + "\ntest result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.1s\n"
        )
        write(directory / "corpus.json", {"synthetic.json": "3" * 64})
        write(directory / "environment.json", {key: "synthetic" for key in ("system", "release", "machine", "node", "rustc", "cargo")})
        report = {
            "schema": e2e.SCHEMA, "lane": lane, "target": target, "status": "pass",
            "source": {"source_commit": self.source, "source_tree": "4" * 40, "lock_sha256": "5" * 64},
            "server": {"source_commit": self.iggy, "source_tree": "6" * 40, "manifest_sha256": "7" * 64, "binary_sha256": "8" * 64},
            "environment": e2e.reference(directory / "environment.json", directory),
            "corpus": e2e.reference(directory / "corpus.json", directory),
            "commands": e2e.commands(target), "exit_codes": {stage: 0 for stage in ("build", "list", "run")},
            "logs": {stage: e2e.reference(directory / f"{stage}.log", directory) for stage in ("build", "list", "run")},
            "test_binary_sha256": "9" * 64, "tests": self.test_names,
        }
        path = directory / "report.json"
        write(path, report)
        return path, report

    def verify(self, path, target="durable_send"):
        return e2e.verify_target(path, "fault", target, self.source, self.iggy)

    def test_complete_fixture_and_portable_references(self):
        path, _ = self.fixture()
        self.assertEqual(self.verify(path)["tests"], self.test_names)

    def test_pass_label_does_not_hide_nonzero_or_missing_exit(self):
        path, report = self.fixture()
        for code in (1, -9, True, None):
            with self.subTest(code=code):
                report["exit_codes"]["run"] = code
                write(path, report)
                with self.assertRaises(ValueError):
                    self.verify(path)

    def test_pass_label_does_not_hide_another_candidate(self):
        path, report = self.fixture()
        for field, nested in (("source", "source_commit"), ("server", "source_commit")):
            changed = copy.deepcopy(report)
            changed[field][nested] = "a" * 40
            write(path, changed)
            with self.assertRaises(ValueError):
                self.verify(path)

    def test_test_filter_is_rejected(self):
        path, report = self.fixture()
        report["commands"]["run"].append("integration::accept")
        write(path, report)
        with self.assertRaises(ValueError):
            self.verify(path)

    def test_logs_are_reparsed_even_if_digest_is_updated(self):
        path, report = self.fixture()
        raw = (path.parent / "run.log").read_text()
        for changed in (
            raw.replace("test integration::reject ... ok\n", ""),
            raw.replace("reject ... ok", "reject ... ignored"),
            raw.replace("reject ... ok", "reject ... FAILED"),
            raw.replace("2 passed", "0 passed"),
            raw + "test integration::accept ... ok\n",
            "test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.1s\n",
        ):
            with self.subTest(changed=changed):
                (path.parent / "run.log").write_text(changed)
                report["logs"]["run"] = e2e.reference(path.parent / "run.log", path.parent)
                write(path, report)
                with self.assertRaises(ValueError):
                    self.verify(path)

    def test_empty_duplicate_or_benchmark_inventory_is_rejected(self):
        for raw in ("", "x: test\nx: test\n", "x: test\ny: benchmark\n"):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                e2e.listed_tests(raw)

    def test_cargo_failed_or_wrong_executable_is_rejected(self):
        path, report = self.fixture()
        log = path.parent / "build.log"
        log.write_text('{"reason":"build-finished","success":true}\n')
        report["logs"]["build"] = e2e.reference(log, path.parent)
        write(path, report)
        with self.assertRaises(ValueError):
            self.verify(path)

    def test_reference_tamper_traversal_and_symlink_are_rejected(self):
        path, report = self.fixture()
        (path.parent / "run.log").write_text("changed")
        with self.assertRaises(ValueError):
            self.verify(path)
        for name in ("../run.log", "/tmp/run.log"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                e2e.resolve_reference(self.root, {"path": name, "sha256": "1" * 64})
        (self.root / "link").symlink_to(path.parent / "run.log")
        with self.assertRaises(ValueError):
            e2e.resolve_reference(self.root, {"path": "link", "sha256": e2e.digest(self.root / "link")})

    def test_full_lane_requires_all_targets_and_same_server(self):
        targets = {}
        for name in e2e.TARGETS["fault"]:
            path, _ = self.fixture(name)
            targets[name] = e2e.reference(path, self.root)
        lane = {"schema": e2e.LANE_SCHEMA, "lane": "fault", "targets": targets}
        path = self.root / "lane.json"
        write(path, lane)
        e2e.verify_lane(path, "fault", self.source, self.iggy)
        del lane["targets"]["durable_metadata_recovery"]
        write(path, lane)
        with self.assertRaises(ValueError):
            e2e.verify_lane(path, "fault", self.source, self.iggy)

    def test_mixed_server_is_rejected_even_with_passing_targets(self):
        targets = {}
        for name in e2e.TARGETS["fault"]:
            path, report = self.fixture(name)
            if name == "durable_send":
                report["server"]["binary_sha256"] = "a" * 64
                write(path, report)
            targets[name] = e2e.reference(path, self.root)
        path = self.root / "lane.json"
        write(path, {"schema": e2e.LANE_SCHEMA, "lane": "fault", "targets": targets})
        with self.assertRaises(ValueError):
            e2e.verify_lane(path, "fault", self.source, self.iggy)

    def test_process_deadline_and_output_budget_are_enforced(self):
        with self.assertRaisesRegex(ValueError, "deadline"):
            e2e.run_bounded([sys.executable, "-c", "import time; time.sleep(10)"], self.root, self.root / "timeout.log", 0.1)
        with patch.object(e2e, "MAX_LOG_BYTES", 32), self.assertRaisesRegex(ValueError, "output budget"):
            e2e.run_bounded([sys.executable, "-c", "print('x' * 100)"], self.root, self.root / "budget.log", 10)

    def test_collector_replays_the_actual_selected_binary_and_retains_failure(self):
        repo = self.root / "repo"
        repo.mkdir()
        corpus = self.root / "input-corpus"
        corpus.mkdir()
        (corpus / "fixture").write_text("synthetic")
        server = self.root / "server"
        server.write_text("synthetic server")
        manifest = self.root / "manifest.toml"
        manifest.write_text(f'[source]\ncommit = "{self.iggy}"\n')
        executable = self.root / "test-binary"
        executable.write_text("synthetic binary")
        source = {"source_commit": self.source, "source_tree": "4" * 40, "lock_sha256": "5" * 64}
        env = {
            "CHIRPS_SERVER_MANIFEST": str(manifest), "CHIRPS_SERVER_BINARY": str(server),
            "CHIRPS_SERVER_SOURCE_COMMIT": self.iggy, "CHIRPS_SERVER_SOURCE_TREE": "6" * 40,
            "CHIRPS_SERVER_SHA256": e2e.digest(server), "CHIRPS_LOCAL_CORPUS_ROOT": str(corpus),
        }
        invoked = []

        def fake_run(argv, root, log, timeout):
            invoked.append(argv)
            if log.name == "build.log":
                log.write_text(json.dumps({"reason": "compiler-artifact", "target": {"name": "durable_send"}, "profile": {"test": True}, "executable": str(executable)}) + '\n{"reason":"build-finished","success":true}\n')
            elif log.name == "list.log":
                log.write_text("integration::accept: test\n")
            else:
                log.write_text("test integration::accept ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.1s\n")
            return 0

        output = self.root / "output"
        with patch.dict(os.environ, env), patch.object(e2e, "source_identity", return_value=source), patch.object(e2e, "command_output", return_value="synthetic tool"), patch.object(e2e, "run_bounded", side_effect=fake_run):
            e2e.collect(repo, output, "fault", "durable_send", 10)
            self.assertEqual(invoked[1][0], str(executable))
            self.assertEqual(invoked[2][0], str(executable))
            self.verify(output / "durable_send/report.json")
            with self.assertRaises(FileExistsError):
                e2e.collect(repo, output, "fault", "durable_send", 10)
            with patch.object(e2e, "run_bounded", side_effect=ValueError("synthetic failure")), self.assertRaises(ValueError):
                e2e.collect(repo, self.root / "failed", "fault", "durable_send", 10)
        failed = e2e.load(self.root / "failed/durable_send/report.json")
        self.assertEqual(failed["status"], "fail")


def write_lane_fixture(root: Path, lane: str, source: str, iggy: str) -> Path:
    """Explicitly synthetic logs for release-gate rejection tests only."""
    case = EvidenceTests()
    case.root = root
    root.mkdir(parents=True, exist_ok=True)
    case.source, case.iggy = source, iggy
    case.test_names = ["synthetic::accept", "synthetic::reject"]
    refs = {}
    for target in e2e.TARGETS[lane]:
        path, _ = case.fixture(target, lane)
        refs[target] = e2e.reference(path, root)
    path = root / "lane.json"
    write(path, {"schema": e2e.LANE_SCHEMA, "lane": lane, "targets": refs})
    return path


if __name__ == "__main__":
    unittest.main()
