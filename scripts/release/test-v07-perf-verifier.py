#!/usr/bin/env python3
"""Synthetic trusted-tool provenance/CLI fixtures; never performance evidence."""
import json
import os
from pathlib import Path
import platform
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import v07_perf_verifier as subject


def write_tool_fixture(root, source_commit, return_code=0):
    root.mkdir(parents=True, exist_ok=True)
    binary = root / subject.NAME
    binary.write_text('#!/bin/sh\n# synthetic verifier fixture; not release evidence\n'
                      'test "$1" = --mode && test "$2" = verify || exit 2\n'
                      f'exit {return_code}\n')
    binary.chmod(0o755)
    (root / 'build.log').write_text('synthetic tool fixture; not a real build\n')
    manifest = dict(schema=subject.SCHEMA, source_commit=source_commit, source_tree='b'*40,
                    lock_sha256='c'*64, binary=subject.NAME, sha256=subject.digest(binary),
                    system=platform.system(), machine=platform.machine(), rustc='fixture',
                    build_log_sha256=subject.digest(root / 'build.log'))
    (root / 'verifier.json').write_text(json.dumps(manifest))
    return binary


class ToolTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.commit = 'a'*40
        self.binary = write_tool_fixture(self.root / 'tool', self.commit)

    def test_same_run_tool_and_readonly_cli_wiring(self):
        candidate = self.root / 'candidate.json'
        candidate.write_text('{}')
        with patch.dict(os.environ, CHIRPS_PERF_VERIFIER=str(self.binary), CHIRPS_RELEASE_TOOLS_COMMIT=self.commit):
            subject.verify(candidate, self.root)
            subprocess.run(['python3', str(Path(subject.__file__)), 'verify', '--candidate', str(candidate), '--evidence-root', str(self.root)], check=True)

    def test_rejects_wrong_source_platform_and_artifact_bytes(self):
        manifest_path = self.binary.parent / 'verifier.json'
        original = json.loads(manifest_path.read_text())
        for field, value in [('source_commit','b'*40),('system','other'),('machine','other'),('sha256','0'*64),('build_log_sha256','0'*64),('binary','probe')]:
            changed = dict(original, **{field:value})
            manifest_path.write_text(json.dumps(changed))
            with self.subTest(field=field), self.assertRaises(ValueError):
                subject.validate_tool(self.binary, self.commit)
        manifest_path.write_text(json.dumps(original))
        self.binary.write_text('changed')
        with self.assertRaises(ValueError):
            subject.validate_tool(self.binary, self.commit)

    def test_missing_expected_source_and_executable_reject(self):
        for source in ('', 'HEAD', 'a'*39):
            with self.subTest(source=source), self.assertRaises(ValueError):
                subject.validate_tool(self.binary, source)
        self.binary.chmod(0o644)
        with self.assertRaises(ValueError):
            subject.validate_tool(self.binary, self.commit)

    def test_failed_replay_is_fatal(self):
        binary = write_tool_fixture(self.root / 'failure', self.commit, 9)
        with patch.dict(os.environ, CHIRPS_PERF_VERIFIER=str(binary), CHIRPS_RELEASE_TOOLS_COMMIT=self.commit):
            with self.assertRaises(subprocess.CalledProcessError):
                subject.verify(self.root / 'candidate.json', self.root)

    def test_tool_change_during_replay_rejects(self):
        def modify(*args, **kwargs):
            self.binary.write_text('changed after replay')
        with patch.dict(os.environ, CHIRPS_PERF_VERIFIER=str(self.binary), CHIRPS_RELEASE_TOOLS_COMMIT=self.commit), patch.object(subject.subprocess, 'run', side_effect=modify):
            with self.assertRaises(ValueError):
                subject.verify(self.root / 'candidate.json', self.root)

    def test_build_requires_clean_exact_source_before_any_cargo(self):
        for values in ([self.commit, ' M Cargo.toml'], ['b'*40]):
            with patch.object(subject, 'git', side_effect=values), patch.object(subject.subprocess, 'run') as run:
                with self.assertRaises(ValueError):
                    subject.build(self.root, self.root/'output', self.commit)
                run.assert_not_called()

if __name__ == '__main__':
    unittest.main()
