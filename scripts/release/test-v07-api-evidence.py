#!/usr/bin/env python3
"""Synthetic parser/packager regressions; these fixtures are not release evidence."""

import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import v07_api_evidence as api


COMMIT = "a" * 40


def passing_log(check):
    package = check["package"]
    old, new = check["versions"]
    return (f"    Building {package} v{new} (current)\n"
            f"    Building {package} v{old} (baseline)\n"
            f"    Checking {package} v{old} -> v{new} (assume minor change)\n"
            "     Checked [   0.010s] 196 checks: 196 pass, 58 skip\n"
            "     Summary no semver update required\n"
            f"    Finished [   1.000s] {package}\n").encode()


def fake_git(root, *args):
    if args[0] == "rev-parse":
        return args[1].split("^")[0].encode()
    if args[0] == "archive":
        if args != ("archive", "--format=tar", api.BASELINE):
            raise AssertionError("archive must use immutable baseline")
        return b"synthetic baseline archive"
    commit, path = args[1].split(":")
    if path == "Cargo.lock":
        return b"synthetic lockfile"
    version = "0.6.1" if commit == api.BASELINE else "0.7.0"
    if path == "Cargo.toml":
        return f'[workspace.package]\nversion="{version}"\n'.encode()
    directory = path.split("/")[1]
    package = directory if directory == "alopex-chirps" else "alopex-" + directory
    extra = ""
    if directory in {"chirps-wire", "chirps-gossip-swim"}:
        extra = "[features]\nhlc=[]\n"
    elif directory == "chirps-transport-quic":
        extra = '[features]\nmetrics-export=[]\n[dependencies]\nmetrics-exporter-prometheus={optional=true}\n'
    elif directory == "alopex-chirps":
        extra = "[features]\nhlc=[]\nmulti-raft=[]\nsnapshot=[]\ntso=[]\n"
        if commit != api.BASELINE:
            extra += "durable-iggy=[]\ndurable-verification=[]\n"
    return f'[package]\nname="{package}"\nversion.workspace=true\n{extra}'.encode()


class ApiEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.report_path = self.root / "report.json"
        with patch.object(api, "git_bytes", side_effect=fake_git):
            self.contract = api.source_contract(self.root, COMMIT)
        (self.root / "logs").mkdir()
        entries = []
        for check in self.contract["checks"]:
            name = f'logs/{check["package"]}--{check["mode"]}.log'
            raw = passing_log(check)
            (self.root / name).write_bytes(raw)
            entries.append({key: check[key] for key in ("package", "manifest", "mode", "flags")})
            entries[-1].update({
                "command": ["/tool/cargo-semver-checks", "semver-checks", "--manifest-path", "/candidate/" + check["manifest"],
                            "--package", check["package"], "--baseline-root", "/baseline/" + check["manifest"],
                            "--release-type", "minor", "--color", "never", *check["flags"]],
                "exit_code": 0, "elapsed_seconds": 1.0, "log": name, "log_sha256": api.sha(raw),
            })
        self.report = {
            "schema_version": 1, "baseline_commit": api.BASELINE, "candidate_commit": COMMIT,
            "tool": api.TOOL, "rustc": "rustc 1.96.0 (test)\nrelease: 1.96.0\nLLVM version: test",
            "candidate_lock_sha256": self.contract["lock"], "baseline_archive_sha256": self.contract["archive"],
            "target_dir": "/target", "expected_checks": len(entries), "status": "pass",
            "checks": entries, "candidate_unchanged": True,
        }

    def verify(self):
        return api.verify_report(self.report, self.report_path, COMMIT, self.contract)

    def replace_log(self, text):
        entry = self.report["checks"][0]
        raw = text.encode()
        (self.root / entry["log"]).write_bytes(raw)
        entry["log_sha256"] = api.sha(raw)

    def test_full_34_matrix_and_raw_summaries_pass(self):
        self.assertEqual(len(self.contract["checks"]), 34)
        self.assertEqual(len(self.verify()), 34)

    def test_pass_labels_do_not_hide_bad_identities(self):
        for field, invalid in (
            ("candidate_commit", "b" * 40), ("baseline_commit", "b" * 40),
            ("tool", "cargo-semver-checks 0.51.0"), ("candidate_unchanged", False),
            ("candidate_lock_sha256", "0" * 64), ("baseline_archive_sha256", "0" * 64),
            ("status", "fail"), ("schema_version", True), ("expected_checks", 1),
            ("rustc", "rustc 1.95.0"),
        ):
            with self.subTest(field=field):
                old = self.report[field]
                self.report[field] = invalid
                with self.assertRaises(ValueError):
                    self.verify()
                self.report[field] = old

    def test_missing_duplicate_and_wrong_feature_checks_reject(self):
        original = copy.deepcopy(self.report)
        changes = (
            lambda value: value["checks"].pop(),
            lambda value: value["checks"].__setitem__(1, copy.deepcopy(value["checks"][0])),
            lambda value: value["checks"][0].update(mode="default"),
            lambda value: value["checks"][-1].update(flags=["--default-features"]),
        )
        for change in changes:
            self.report = copy.deepcopy(original)
            change(self.report)
            with self.assertRaises(ValueError):
                self.verify()

    def test_failure_and_boolean_exit_codes_reject(self):
        for code in (100, 101, -9, False):
            with self.subTest(code=code), self.assertRaises(ValueError):
                self.report["checks"][0]["exit_code"] = code
                self.verify()

    def test_commands_cannot_filter_suppress_or_compare_self(self):
        original = self.report["checks"][0]["command"].copy()
        variants = [original + ["--exclude", "alopex-chirps-wire"],
                    original[:9] + ["major"] + original[10:],
                    original[:7] + [original[3]] + original[8:],
                    original[:-1] + ["--default-features"],
                    ["/tool/fake-checker"] + original[1:]]
        for argv in variants:
            with self.subTest(argv=argv), self.assertRaises(ValueError):
                self.report["checks"][0]["command"] = argv
                self.verify()

    def test_windows_invocations_are_data_not_local_paths(self):
        for entry in self.report["checks"]:
            entry["command"][0] = r"C:\tools\cargo-semver-checks.exe"
            entry["command"][3] = "C:\\candidate\\" + entry["manifest"].replace("/", "\\")
            entry["command"][7] = "C:\\baseline\\" + entry["manifest"].replace("/", "\\")
        self.assertEqual(len(self.verify()), 34)

    def test_wrong_or_failed_raw_logs_reject_even_after_rehash(self):
        raw = passing_log(self.contract["checks"][0]).decode()
        bad = ["pass\n", raw.replace("196 checks: 196 pass", "0 checks: 0 pass"),
               raw.replace("196 pass", "195 pass, 1 fail, 0 warn"),
               raw.replace("58 skip", "59 skip"), raw.replace("v0.6.1", "v0.6.3"),
               raw.replace("assume minor", "assume major"),
               raw.replace("alopex-chirps-wire", "alopex-chirps-core"),
               raw + "error: actual check failed\n", raw + raw,
               raw.replace("     Summary no semver update required\n", ""),
               raw.replace("    Finished [   1.000s] alopex-chirps-wire\n", ""),
               raw.replace("    Checking", "\x1b[0m    Checking"),
               raw.replace("    Building", "error: failed\n    Building", 1)]
        for text in bad:
            with self.subTest(text=text), self.assertRaises(ValueError):
                self.replace_log(text)
                self.verify()

    def test_missing_changed_reused_or_escaping_logs_reject(self):
        entry = self.report["checks"][0]
        original = entry.copy()
        for name in ("logs/missing.log", "../outside.log", "/absolute.log", "C:\\log.txt", "logs//alias.log", "logs/./alias.log"):
            entry["log"] = name
            with self.subTest(name=name), self.assertRaises((ValueError, OSError)):
                self.verify()
        entry.update(original)
        entry["log_sha256"] = "0" * 64
        with self.assertRaises(ValueError):
            self.verify()
        entry.update(original)
        self.report["checks"][1]["log"] = entry["log"]
        with self.assertRaisesRegex(ValueError, "reuse"):
            self.verify()

    def test_symlink_log_rejects(self):
        entry = self.report["checks"][0]
        (self.root / "alias").symlink_to(self.root / "logs", target_is_directory=True)
        entry["log"] = entry["log"].replace("logs/", "alias/")
        with self.assertRaisesRegex(ValueError, "symlink"):
            self.verify()

    def test_pinned_source_not_worktree_is_used_and_baseline_missing_rejects(self):
        with patch.object(api, "git_bytes", side_effect=fake_git) as calls:
            self.assertEqual(api.source_contract(self.root, COMMIT), self.contract)
        self.assertTrue(all("HEAD" not in str(call) for call in calls.call_args_list))
        with patch.object(api, "git_bytes", return_value=b"b" * 40), self.assertRaises(ValueError):
            api.source_contract(self.root, COMMIT)

    def test_source_suppression_and_removed_baseline_features_reject(self):
        for extra in ('\n[package.metadata.cargo-semver-checks]\n', "remove"):
            def changed(root, *args):
                data = fake_git(root, *args)
                if args[0] == "show" and args[1] == f"{COMMIT}:crates/chirps-wire/Cargo.toml":
                    return data.replace(b"hlc=[]", b"") if extra == "remove" else data + extra.encode()
                return data
            with patch.object(api, "git_bytes", side_effect=changed), self.assertRaises(ValueError):
                api.source_contract(self.root, COMMIT)

    def test_packager_accepts_old_absolute_logs_and_preserves_bytes(self):
        for entry in self.report["checks"]:
            entry["log"] = str(self.root / entry["log"])
        self.report_path.write_text(json.dumps(self.report))
        with self.assertRaises(ValueError):
            self.verify()
        with patch.object(api, "source_contract", return_value=self.contract):
            output = api.package_api_report(self.root, self.report_path, COMMIT, self.root / "portable")
            value = api.verify_api_report(self.root, output, COMMIT)
        for before, after in zip(self.report["checks"], value["checks"]):
            self.assertEqual(before["log_sha256"], after["log_sha256"])
            self.assertEqual(before["command"], after["command"])
            self.assertFalse(Path(after["log"]).is_absolute())
        self.assertEqual(json.loads(self.report_path.read_text()), self.report)

    def test_packager_rejects_failure_before_creating_output(self):
        self.report["checks"][0]["exit_code"] = 100
        self.report_path.write_text(json.dumps(self.report))
        with patch.object(api, "source_contract", return_value=self.contract), self.assertRaises(ValueError):
            api.package_api_report(self.root, self.report_path, COMMIT, self.root / "portable")
        self.assertFalse((self.root / "portable").exists())

    def test_duplicate_json_keys_reject(self):
        self.report_path.write_text('{"status":"fail","status":"pass"}')
        with self.assertRaisesRegex(ValueError, "duplicate"):
            api.load(self.report_path)


if __name__ == "__main__":
    unittest.main()
