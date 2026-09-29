import importlib.util
import io
import json
from pathlib import Path
import tempfile
import tarfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "verify-v061-public-api.py"
SPEC = importlib.util.spec_from_file_location("api_gate", SCRIPT)
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)


class PublicApiGateTests(unittest.TestCase):
    def test_implicit_features_and_dep_suppression(self):
        self.assertEqual(gate.features({
            "features": {"default": [], "public": ["dep:private"]},
            "dependencies": {"private": {"optional": True}, "implicit": {"optional": True}},
            "target": {"cfg(unix)": {"dependencies": {"platform": {"optional": True}}}},
        }), {"public", "implicit", "platform"})

    def test_lint_suppression_cannot_turn_release_green(self):
        for scope in ("workspace", "package"):
            with self.subTest(scope=scope), self.assertRaises(ValueError):
                gate.reject_overrides({scope: {"metadata": {
                    "cargo-semver-checks": {"lints": {"function_missing": "allow"}}
                }}})

    def test_command_never_infers_major_release_from_version(self):
        argv = gate.command("checker", Path("/source"), Path("/baseline"), {
            "package": "alopex-chirps", "manifest": "crates/alopex-chirps/Cargo.toml",
            "flags": ["--only-explicit-features"]
        })
        self.assertEqual(argv[argv.index("--baseline-root") + 1], str(Path("/baseline/crates/alopex-chirps/Cargo.toml")))
        self.assertEqual(argv[argv.index("--release-type") + 1], "minor")
        self.assertIn("--only-explicit-features", argv)

    def fixture(self, root, feature_text):
        (root / "Cargo.toml").write_text("[workspace]\n")
        for directory in gate.CRATES:
            path = root / "crates" / directory
            path.mkdir(parents=True)
            (path / "Cargo.toml").write_text(f'[package]\nname="{directory}"\n' + feature_text)

    @staticmethod
    def baseline(root, *args):
        if args[0] == "rev-parse":
            return gate.BASELINE
        directory = args[1].split("/")[1]
        return f'[package]\nname="{directory}"\n[features]\nlegacy=[]\n'

    def test_full_matrix_preserves_feature_off_and_new_feature_checks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root, "[features]\nlegacy=[]\nnew=[]\n")
            with patch.object(gate, "git", side_effect=self.baseline):
                checks = gate.matrix(root)
        self.assertEqual(len(checks), 8 * 5)
        for package in gate.CRATES:
            modes = {c["mode"]: c["flags"] for c in checks if c["package"] == package}
            self.assertEqual(modes["no-default"], ["--only-explicit-features"])
            self.assertIn("--baseline-features", modes["feature-legacy"])
            self.assertNotIn("--baseline-features", modes["feature-new"])

    def test_removed_public_feature_rejects_before_rustdoc(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root, "[features]\nnew=[]\n")
            with patch.object(gate, "git", side_effect=self.baseline), self.assertRaisesRegex(ValueError, "features removed"):
                gate.matrix(root)

    def test_failed_or_incomplete_commands_cannot_be_passing_evidence(self):
        for exit_code in (100, 101):
            with self.subTest(exit_code=exit_code), tempfile.TemporaryDirectory() as directory:
                base = Path(directory)
                root = base / "source"
                root.mkdir()
                (root / "Cargo.lock").write_text("# fixture\n")
                output = base / "evidence.json"
                check = {"package": "alopex-chirps-wire", "manifest": "crates/chirps-wire/Cargo.toml",
                         "mode": "all", "flags": ["--all-features"]}
                archive = io.BytesIO()
                with tarfile.open(fileobj=archive, mode="w"):
                    pass

                def identity(_root, *args):
                    return "" if args[0] == "status" else "a" * 40

                with patch.object(gate, "__file__", str(root / "scripts" / "gate.py")), \
                     patch.object(gate, "matrix", return_value=[check]), \
                     patch.object(gate, "git", side_effect=identity), \
                     patch.object(gate.sys, "argv", ["gate", "--output", str(output)]), \
                     patch.dict(gate.os.environ, {"SEMVER_CHECKS_BIN": "checker"}), \
                     patch.object(gate.subprocess, "check_output", side_effect=[gate.TOOL_VERSION, archive.getvalue(), "rustc fixture"]) as commands, \
                     patch.object(gate.subprocess, "run", return_value=gate.subprocess.CompletedProcess([], exit_code)):
                    self.assertEqual(gate.main(), 1)
                self.assertIn(["git", "-C", str(root.resolve()), "archive", "--format=tar", gate.BASELINE],
                              [call.args[0] for call in commands.call_args_list])
                report = json.loads(output.read_text())
                self.assertEqual(report["status"], "fail")
                self.assertEqual(report["checks"][0]["exit_code"], exit_code)


if __name__ == "__main__":
    unittest.main()
