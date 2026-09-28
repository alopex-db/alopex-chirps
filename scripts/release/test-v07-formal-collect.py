#!/usr/bin/env python3
"""Collector contract tests; no solver or container runtime is executed."""
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

SPEC=importlib.util.spec_from_file_location('collector',Path(__file__).with_name('v07_formal_collect.py'))
collector=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(collector)
ROOT=Path(__file__).resolve().parents[2]

class CollectorContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory()
        cls.snapshot=Path(cls.temp.name)/'snapshot'
        cls.commit=subprocess.check_output(['git','-C',str(ROOT),'rev-parse','HEAD'],text=True).strip()
        subprocess.run(['python3','-B',str(Path(__file__).with_name('export-v07-formal.py')),
                        str(ROOT),cls.commit,str(cls.snapshot)],check=True,stdout=subprocess.DEVNULL)
    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()
    def changed_snapshot(self):
        path=Path(self.temp.name)/self.id().split('.')[-1]
        shutil.copytree(self.snapshot,path)
        return path
    def test_complete_inventory_and_bounds(self):
        identity,jobs=collector.jobs(self.snapshot)
        self.assertEqual(identity['source_commit'],self.commit)
        self.assertEqual(len(jobs),160)
        self.assertEqual([j['bound'] for j in jobs if j['kind']=='normal'],[24,28,24,20])
        self.assertEqual(sum(j['kind']=='profile' for j in jobs),125)
        self.assertEqual(sum(j['kind']=='witness' for j in jobs),27)
    def test_profile_and_witness_materialization(self):
        _,jobs=collector.jobs(self.snapshot)
        profile=next(j for j in jobs if j['id']=='chirps-v0-7-send-lease:duplicate-append')
        witness=next(j for j in jobs if j['id']=='chirps-v0-7-metadata-recovery:crash-before-file-sync-recovers-old')
        self.assertIn('CONSTANT UnsafeMode = "duplicate-append"',profile['config'])
        self.assertTrue(profile['config'].endswith('INVARIANT TypeOK\nINVARIANT AtMostOneAppendPerAttempt\n'))
        self.assertIn('INIT OldCrashWitnessInit\nNEXT OldCrashWitnessNext',witness['config'])
        self.assertEqual((profile['bound'],witness['bound']),(8,5))
        self.assertEqual((profile['expected'],witness['expected']),(12,12))
    def test_changed_input_rejected(self):
        path=self.changed_snapshot();(path/'formal/chirps-durable/SendLease.tla').write_text('bad')
        with self.assertRaisesRegex(ValueError,'source input changed'):collector.jobs(path)
    def test_registry_hash_rejects_rehashed_snapshot(self):
        path=self.changed_snapshot();name='formal/chirps-durable/SendLease.tla';(path/name).write_text('bad')
        identity=json.loads((path/'source.json').read_text());identity['inputs'][name]=collector.sha(b'bad')
        collector.save(path/'source.json',identity)
        with self.assertRaisesRegex(ValueError,'registry digest mismatch'):collector.jobs(path)
    def test_missing_input_rejected(self):
        path=self.changed_snapshot();identity=json.loads((path/'source.json').read_text())
        identity['inputs'].pop('formal/compose.yml');collector.save(path/'source.json',identity)
        with self.assertRaisesRegex(ValueError,'14 source inputs'):collector.jobs(path)
    def test_symlink_input_rejected(self):
        path=self.changed_snapshot();name='formal/chirps-durable/SendLease.tla';(path/name).unlink()
        (path/name).symlink_to(self.snapshot/name)
        with self.assertRaisesRegex(ValueError,'symlink'):collector.jobs(path)
    def test_cleanup_attempts_remove_when_stop_times_out(self):
        process=Mock();process.wait.return_value=143
        with patch.object(collector.subprocess,'run',side_effect=[subprocess.TimeoutExpired('stop',15),Mock(returncode=0),Mock(returncode=1)]) as run:
            collector.cleanup_owned_container('owned-fixture',process)
        self.assertEqual(run.call_args_list[1].args[0],['podman','rm','--force','owned-fixture'])
    def test_cleanup_uncertainty_rejected(self):
        with patch.object(collector.subprocess,'run',return_value=Mock(returncode=0)):
            with self.assertRaisesRegex(RuntimeError,'could not establish'):collector.cleanup_owned_container('owned-fixture',Mock())

if __name__=='__main__':unittest.main()
