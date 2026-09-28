#!/usr/bin/env python3
"""Exercise real cleanup helpers and corpus traps using only owned temp paths."""
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
HELPER = ROOT / 'scripts/release/owned-target.sh'
CORPUS = Path(os.environ.get('CHIRPS_TEST_CORPUS_SCRIPT', ROOT / 'scripts/generate-v0.7-local-state-corpora.sh'))


class OwnedTarget(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.target = self.root / 'target'

    def run_helper(self, commands):
        env = dict(os.environ, TARGET=str(self.target), MARKER=str(self.root / 'called'))
        env.pop('BASH_ENV', None)
        return subprocess.run(['bash', '-c', 'set -eu; source ' + shlex.quote(str(HELPER)) + '; ' + commands],
                              env=env, text=True, capture_output=True, timeout=10)

    def test_absent_cleanup_is_idempotent_and_does_not_invoke_command(self):
        result = self.run_helper('chirps_target_clean "$TARGET" false; chirps_target_clean "$TARGET" false')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(self.target.exists())

    def test_empty_cleanup_then_repeat(self):
        self.target.mkdir()
        result = self.run_helper('chirps_target_clean "$TARGET" true; chirps_target_clean "$TARGET" false')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(self.target.exists())

    def test_nonempty_leftovers_are_preserved(self):
        self.target.mkdir()
        sentinel = self.target / 'keep'
        sentinel.write_bytes(b'preserve me')
        result = self.run_helper('chirps_target_clean "$TARGET" true')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(sentinel.read_bytes(), b'preserve me')

    def test_failed_cleaner_is_not_followed_by_recursive_removal(self):
        self.target.mkdir()
        sentinel = self.target / 'keep'
        sentinel.write_bytes(b'keep failure evidence')
        result = self.run_helper('chirps_target_clean "$TARGET" false')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(sentinel.read_bytes(), b'keep failure evidence')

    def test_symlink_and_dangling_symlink_never_invoke_cleaner(self):
        for existing in (False, True):
            with self.subTest(existing=existing):
                destination = self.root / 'destination'
                if existing:
                    destination.mkdir()
                self.target.symlink_to(destination, target_is_directory=True)
                result = self.run_helper('chirps_target_clean "$TARGET" touch "$MARKER"')
                self.assertNotEqual(result.returncode, 0)
                self.assertTrue(self.target.is_symlink())
                self.assertFalse((self.root / 'called').exists())
                self.target.unlink()

    def test_claim_is_exclusive_and_rejects_files(self):
        self.assertEqual(self.run_helper('chirps_target_claim "$TARGET"').returncode, 0)
        self.assertNotEqual(self.run_helper('chirps_target_claim "$TARGET"').returncode, 0)
        self.target.rmdir()
        self.target.write_bytes(b'file')
        self.assertNotEqual(self.run_helper('chirps_target_claim "$TARGET"').returncode, 0)
        self.assertEqual(self.target.read_bytes(), b'file')

    def test_invalid_paths_rejected_before_cleaner(self):
        result = self.run_helper('for path in "" / relative /tmp/.. /tmp/. /tmp/; do '
                                 'if chirps_target_clean "$path" touch "$MARKER"; then exit 9; fi; done')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse((self.root / 'called').exists())


class CorpusOwnership(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo = self.root / 'repo'
        scripts = self.repo / 'scripts'
        (scripts / 'release').mkdir(parents=True)
        shutil.copyfile(HELPER, scripts / 'release/owned-target.sh')
        self.target = self.root / 'target'
        self.output = self.root / 'output'
        self.budget = self.root / 'budget.sh'
        self.budget.write_text('exit 0\n')
        source = CORPUS.read_text()
        for name, path in [('TARGET_DIR', self.target), ('EXPECTED_OUTPUT', self.output),
                           ('BUDGET_GATE', self.budget), ('CLEANUP_GATE', self.budget)]:
            source, count = re.subn(r'^readonly ' + name + r'=.*$', 'readonly ' + name + '=' + shlex.quote(str(path)),
                                   source, flags=re.MULTILINE)
            self.assertEqual(count, 1)
        self.script = scripts / 'generate-v0.7-local-state-corpora.sh'
        self.script.write_text(source)
        # No real Cargo, server, corpus, network or shared target is accessed.
        binary = self.root / 'bin'
        binary.mkdir()
        spy = binary / 'rtk'
        spy.write_text('''#!/usr/bin/env python3
import os, pathlib, shutil, subprocess, sys
args=sys.argv[1:]
if args[0]=='cargo':
    target=pathlib.Path(args[args.index('--target-dir')+1])
    assert str(target)==os.environ['OWNED_TEST_TARGET'] and not target.is_symlink()
    pathlib.Path(os.environ['CLEANER_CALLED']).write_text('called')
    if target.exists(): shutil.rmtree(target)
    sys.exit(0)
if args[0]=='rsync': sys.exit(77)
sys.exit(subprocess.call(args))
''')
        spy.chmod(0o700)
        self.called = self.root / 'cleaner-called'
        self.env = dict(os.environ, PATH=str(binary) + os.pathsep + os.environ['PATH'],
                        OWNED_TEST_TARGET=str(self.target), CLEANER_CALLED=str(self.called))
        self.env.pop('BASH_ENV', None)

    def run_corpus(self):
        return subprocess.run(['bash', str(self.script), '--output', str(self.output), '--freeze', '--verify'],
                              env=self.env, text=True, capture_output=True, timeout=10)

    def test_existing_nonempty_target_is_preserved(self):
        self.target.mkdir()
        marker = self.target / 'keep'
        marker.write_bytes(b'preexisting data')
        result = self.run_corpus()
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertTrue(marker.exists(), 'rejection deleted preexisting target')
        self.assertEqual(marker.read_bytes(), b'preexisting data')
        self.assertFalse(self.called.exists())

    def test_existing_empty_target_is_not_claimed_or_cleaned(self):
        self.target.mkdir()
        self.assertEqual(self.run_corpus().returncode, 2)
        self.assertTrue(self.target.is_dir())
        self.assertFalse(self.called.exists())

    def test_budget_failure_before_claim_preserves_existing_target(self):
        self.target.mkdir()
        self.budget.write_text('exit 42\n')
        self.assertEqual(self.run_corpus().returncode, 42)
        self.assertTrue(self.target.is_dir())
        self.assertFalse(self.called.exists())

    def test_new_owned_target_is_cleaned_on_later_failure(self):
        self.assertEqual(self.run_corpus().returncode, 77)
        self.assertTrue(self.called.exists())
        self.assertFalse(self.target.exists())
        self.assertEqual(list(self.root.glob('.chirps-v07-corpus.*')), [])

    def test_dangling_target_symlink_is_rejected_and_preserved(self):
        self.target.symlink_to(self.root / 'missing', target_is_directory=True)
        self.assertEqual(self.run_corpus().returncode, 2)
        self.assertTrue(self.target.is_symlink())
        self.assertFalse(self.called.exists())


if __name__ == '__main__':
    unittest.main()
