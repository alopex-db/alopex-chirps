#!/usr/bin/env python3
"""Source-only negatives; these fixtures never qualify runtime evidence."""
import contextlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
import v07_public_structure as structure

REPO = Path(__file__).resolve().parents[2]


class StructureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        shutil.copytree(REPO / 'tests/e2e', self.root / 'tests/e2e')
        schema = self.root / structure.SCHEMA_PATH
        schema.parent.mkdir(parents=True)
        shutil.copyfile(REPO / structure.SCHEMA_PATH, schema)
        shutil.copytree(REPO / 'server/iggy-compatible', self.root / 'server/iggy-compatible')

    def edit(self, relative, old, new):
        path = self.root / relative
        raw = path.read_text()
        self.assertIn(old, raw)
        path.write_text(raw.replace(old, new))

    def reject(self, message):
        with self.assertRaisesRegex(ValueError, message):
            structure.validate_sources(self.root)

    def test_actual_source_and_schema_and_both_strict_lists(self):
        structure.validate_sources(self.root)
        structure.validate_schema(self.root)
        structure.validate_manifests(self.root)
        for lane, expected in structure.TARGETS.items():
            out = io.StringIO()
            with patch('sys.argv', ['check', '--source-root', str(self.root), '--lane', lane, '--mode', 'strict']), contextlib.redirect_stdout(out):
                structure.main()
            self.assertEqual(out.getvalue().splitlines(), list(expected))

    def test_autotests_true_is_rejected(self):
        self.edit('tests/e2e/Cargo.toml', 'autotests = false', 'autotests = true')
        self.reject('autotests')

    def test_missing_primary_is_rejected(self):
        (self.root / 'tests/e2e/tests/durable_send.rs').unlink()
        self.reject('missing')

    def test_missing_companion_is_rejected(self):
        (self.root / 'tests/e2e/tests/durable_server_faults.rs').unlink()
        self.reject('missing')

    def test_companion_import_cannot_be_removed_or_redirected(self):
        path = self.root / 'tests/e2e/tests/durable_send.rs'
        raw = path.read_text()
        for changed in (raw.replace('mod durable_server_faults;', '// mod durable_server_faults;'),
                        raw.replace('"durable_server_faults.rs"', '"task_6_5_support.rs"')):
            path.write_text(changed)
            self.reject('companion')

    def test_unknown_durable_source_is_rejected(self):
        (self.root / 'tests/e2e/tests/durable_unknown.rs').write_text('')
        self.reject('unknown durable')

    def test_comments_and_literals_cannot_supply_a_missing_import(self):
        path = self.root / 'tests/e2e/tests/durable_compaction.rs'
        raw = path.read_text().replace('mod durable_capacity;', '')
        for decoy in ('const DECOY: &str = r###"\nmod durable_capacity;\n"###;',
                      'const DECOY: &str = "\nmod durable_capacity;\n";',
                      '/* outer /* nested */\nmod durable_capacity;\n*/',
                      '// mod durable_capacity;'):
            with self.subTest(decoy=decoy):
                path.write_text(decoy + '\n' + raw)
                self.reject('companion')

    def test_url_and_raw_string_comments_do_not_hide_real_imports(self):
        path = self.root / 'tests/e2e/tests/durable_compaction.rs'
        path.write_text('const URL: &str = "https://example.invalid/";\n'
                        'const TEXT: &str = r#"/* not comment */ // not comment"#;\n'
                        + path.read_text())
        structure.validate_sources(self.root)

    def test_unsupported_or_unterminated_module_syntax_fails_closed(self):
        path = self.root / 'tests/e2e/tests/durable_compaction.rs'
        raw = path.read_text()
        for changed in (raw.replace('mod durable_capacity;', '#[cfg(any())]\nmod durable_capacity;'),
                        raw.replace('mod durable_capacity;', '#[path="task_6_5_support.rs"]\npub(crate) mod durable_capacity;'),
                        '/* never closed\n' + raw,
                        'const TEXT: &str = r##"never closed\n' + raw):
            path.write_text(changed)
            self.reject('unsupported|unterminated')

    def test_target_selection_rejects_the_other_lane(self):
        with patch('sys.argv', ['check', '--source-root', str(self.root), '--lane', 'production', '--mode', 'target', '--target', 'durable_metadata_recovery']), contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                structure.main()
            self.assertEqual(error.exception.code, 1)

    def test_non_durable_orphan_source_is_rejected(self):
        (self.root / 'tests/e2e/tests/unknown_support.rs').write_text('')
        self.reject('orphan')

    def test_primary_cargo_path_must_match_source(self):
        self.edit('tests/e2e/Cargo.toml', 'path = "tests/durable_send.rs"', 'path = "tests/durable_poll.rs"')
        self.reject('wrong primary path')

    def test_missing_allowlist_entry_is_rejected(self):
        for lane in structure.TARGETS:
            with patch.dict(structure.TARGETS, {lane: structure.TARGETS[lane][1:]}):
                self.reject('allowlist')

    def test_missing_cargo_declaration_is_rejected(self):
        self.edit('tests/e2e/Cargo.toml', '[[test]]\nname = "durable_send"\npath = "tests/durable_send.rs"', '')
        self.reject('orphan|missing allowlisted')

    def test_companion_cannot_be_declared_as_independent_test(self):
        path = self.root / 'tests/e2e/Cargo.toml'
        with path.open('a') as output:
            output.write('\n[[test]]\nname="durable_owner"\npath="tests/durable_owner.rs"\n')
        self.reject('standalone')

    def test_missing_or_invalid_real_schema_is_rejected(self):
        path = self.root / structure.SCHEMA_PATH
        original = path.read_text()
        for raw in ('{}', original.replace('"configuration_sha256",', '', 1)):
            path.write_text(raw)
            with self.assertRaises(ValueError): structure.validate_schema(self.root)
        path.unlink()
        with self.assertRaisesRegex(ValueError, 'missing'): structure.validate_schema(self.root)

    def test_actual_manifest_binding_drift_is_rejected(self):
        self.edit('server/iggy-compatible/manifest.toml', 'publishable = true', 'publishable = false')
        with self.assertRaisesRegex(ValueError, 'manifest digest'):
            structure.validate_manifests(self.root)

    def test_lane_runner_checks_sources_before_requesting_artifacts(self):
        scripts = self.root / 'scripts/release'
        scripts.mkdir(parents=True)
        for name in ('v07_public_structure.py', 'v07_e2e_evidence.py'):
            shutil.copyfile(REPO / 'scripts/release' / name, scripts / name)
        self.edit('tests/e2e/Cargo.toml', 'autotests = false', 'autotests = true')
        env = {key: value for key, value in os.environ.items() if not key.startswith('CHIRPS_')}
        # The runtime runner targets Linux Bash; macOS's system Bash 3 lacks
        # BASHPID. This source-failure check exits before mapfile or runtime work.
        env['BASHPID'] = str(os.getpid())
        result = subprocess.run(['bash', str(self.root / 'tests/e2e/scripts/run-v07-lane.sh'), '--lane', 'production', '--strict-all'], env=env, capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('autotests must be false', result.stderr)
        self.assertNotIn('CHIRPS_SERVER_MANIFEST is required', result.stderr)


if __name__ == '__main__':
    unittest.main()
