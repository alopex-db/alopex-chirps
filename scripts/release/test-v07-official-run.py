#!/usr/bin/env python3
"""Synthetic raw-execution fixtures; no actual broker is started here."""
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import v07_official_run as run
import v07_official_evidence as observation

spec = importlib.util.spec_from_file_location('official_fixtures',Path(__file__).with_name('test-v07-official-evidence.py'))
fixtures = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixtures)


class ExecutionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name).resolve()
        self.root = self.base/'source'
        self.root.mkdir()
        (self.root/'Cargo.lock').write_text('version = 4\n')
        for argv in (['init','-q'], ['add','Cargo.lock'],
            ['-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','-qm','synthetic source']):
            subprocess.run(['git','-C',str(self.root),*argv],check=True,capture_output=True)
        self.source = run.source_identity(self.root)
        self.commit = self.source['source_commit']
        self.output = self.base/'output'
        self.output.mkdir()
        self.probe_dir = self.output/'observations'
        self.probe_dir.mkdir()
        self.build = [dict(reason='compiler-artifact',target=dict(name='durable_official_interop',kind=['example']),
            executable='/owned/target/durable_official_interop'),dict(reason='build-finished',success=True)]
        self.write_build()
        (self.output/'run.log').write_text('')
        (self.output/'server-build.log').write_text('synthetic build record, never runtime evidence\n')
        manifest = dict(schema='chirps-official-devbaseline-v1', artifact_kind='official-development-interoperability',
            publishable=False, profile='dev', source_commit=observation.BASELINE, source_tree=observation.TREE,
            cargo_lock_sha256=observation.LOCK, source_clean=True, binary_size=5, binary_sha256='c'*64,
            build_log_sha256=run.digest(self.output/'server-build.log'))
        raw = json.dumps(manifest).encode()
        (self.probe_dir/'server-manifest.json').write_bytes(raw)
        self.probe = dict(schema='chirps.v0.7.official-interoperability/v2', source_commit=self.commit,
            api_surface='DurableConfig', public_durable_config_validated=True,
            official_manifest_sha256=observation.sha(raw),server_binary_sha256='c'*64,
            server_source_commit=observation.BASELINE,client_binary_sha256='b'*64,
            observation=fixtures.fixture(True),cleanup=dict(graceful=True,forced=False),result='pass')
        self.write_probe()
        self.report = dict(schema=run.SCHEMA,source=self.source,
            commands=dict(build=run.BUILD.copy(),run=['<probe-binary>','<official-manifest>',self.commit,'<new-observations-directory>']),
            exit_codes=dict(build=0,run=0),logs={stage:run.reference(self.output/f'{stage}.log',self.output) for stage in ('build','run')},
            probe=run.reference(self.probe_dir/'report.json',self.output),
            server_build_log=run.reference(self.output/'server-build.log',self.output),client_binary_sha256='b'*64,
            rustc='synthetic tool identity',cargo='synthetic tool identity',result='pass')
        self.path = self.output/'execution.json'

    def write_build(self):
        (self.output/'build.log').write_text('\n'.join(json.dumps(row) for row in self.build)+'\n')

    def write_probe(self):
        (self.probe_dir/'report.json').write_text(json.dumps(self.probe))

    def verify(self):
        self.path.write_text(json.dumps(self.report))
        return run.verify(self.root,self.path,self.commit,require_public=True)

    def test_complete_public_evidence_replays(self):
        self.assertTrue(self.verify()['public_durable_config_validated'])

    def test_changed_git_tree_and_lock_are_rejected(self):
        for field in ('source_tree','lock_sha256'):
            old = self.source[field]
            self.source[field] = '0'*len(old)
            with self.subTest(field=field),self.assertRaises(ValueError): self.verify()
            self.source[field] = old

    def test_failed_or_boolean_exit_code_is_not_pass(self):
        for code in (1,False,None):
            self.report['exit_codes']['run'] = code
            with self.subTest(code=code),self.assertRaises(ValueError): self.verify()

    def test_filtered_build_command_rejected(self):
        self.report['commands']['build'].append('--features=other')
        with self.assertRaises(ValueError): self.verify()

    def test_wrong_or_duplicate_binary_not_accepted_after_rehash(self):
        self.build[0]['target']['name'] = 'other'
        self.write_build()
        self.report['logs']['build'] = run.reference(self.output/'build.log',self.output)
        with self.assertRaises(ValueError): self.verify()
        self.build[0]['target']['name'] = 'durable_official_interop'
        self.build.append(self.build[0])
        self.write_build()
        self.report['logs']['build'] = run.reference(self.output/'build.log',self.output)
        with self.assertRaises(ValueError): self.verify()

    def test_raw_log_or_client_binary_substitution_rejected(self):
        (self.output/'run.log').write_text('changed bytes')
        with self.assertRaises(ValueError): self.verify()
        (self.output/'run.log').write_text('')
        self.probe['client_binary_sha256'] = 'd'*64
        self.write_probe()
        self.report['probe'] = run.reference(self.probe_dir/'report.json',self.output)
        with self.assertRaises(ValueError): self.verify()

    def test_server_build_log_binding_is_rechecked(self):
        (self.output/'server-build.log').write_text('substituted raw log')
        self.report['server_build_log'] = run.reference(self.output/'server-build.log',self.output)
        with self.assertRaises(ValueError): self.verify()

    def test_v1_cannot_qualify_public_facade(self):
        self.probe.update(schema='chirps.v0.7.official-interoperability/v1',api_surface='DevelopmentAppendConnection',
            public_durable_config_validated=False,observation=fixtures.fixture())
        self.write_probe()
        self.report['probe'] = run.reference(self.probe_dir/'report.json',self.output)
        with self.assertRaises(ValueError): self.verify()

    def test_failed_build_retains_failed_report_and_never_runs_probe(self):
        # This tests collector failure cleanup/provenance only; the fake Cargo output
        # cannot create a successful report or be used as runtime evidence.
        manifest = self.probe_dir/'server-manifest.json'
        def fail_build(argv,root,log,timeout,env):
            log.write_text('synthetic compiler failure\n')
            return 101
        destination = self.base/'failed-collection'
        with patch.object(run,'run_bounded',side_effect=fail_build) as runner:
            with self.assertRaises(ValueError):
                run.collect(self.root,destination,manifest,self.output/'server-build.log')
            runner.assert_called_once()
        failed = run.load(destination/'execution.json')
        self.assertEqual(failed['result'],'fail')
        self.assertEqual(failed['exit_codes'],{'build':101})
        self.assertIsNone(failed['probe'])


if __name__ == '__main__':
    unittest.main()
