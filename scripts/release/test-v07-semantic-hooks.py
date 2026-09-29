#!/usr/bin/env python3
"""Central category wiring tests; mocked fixtures are not release evidence."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('central', Path(__file__).with_name('verify-v0.7-evidence.py'))
v = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v)

class HookTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.entries = []
        for kind, identifier, path in [('package','packages','consumer/report.json'), ('compatibility','compatibility-matrix','compatibility-matrix.json'), ('performance','perf','performance/paired/paired.json'), ('environment','environment','environment.json'), ('process','production','production/lane.json'), ('fault','fault','fault/lane.json'), ('security','security','security.json')]:
            file = self.root / path
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text(json.dumps({'targets':{'durable_diagnostics':{'sha256':'e'*64}}}) if kind in ('process','fault') else '{}')
            self.entries.append(dict(kind=kind,id=identifier,path=path))
        self.candidate = {'source_commit':'a'*40,'iggy_commit':'b'*40}
        self.security_report = {lane:{'sha256':'e'*64} for lane in ('production','fault')}
        self.mocks = {}
        for name in ('verify_consumer_report','verify_compatibility','verify_performance','verify_environment','verify_security'):
            value = self.candidate if name=='verify_consumer_report' else self.security_report if name=='verify_security' else None
            self.mocks[name] = self.enterContext(patch.object(v,name,return_value=value))

    def verify(self):
        v.verify_release_categories(self.root, self.entries, self.candidate, self.root/'candidate.json')

    def test_all_semantic_verifiers_receive_exact_paths(self):
        self.verify()
        self.mocks['verify_consumer_report'].assert_called_once_with(self.root/'consumer/package-set.json',self.root/'consumer/report.json','stored-archives')
        self.assertEqual(self.mocks['verify_compatibility'].call_args.args[1:],(self.root/'compatibility-matrix.json','a'*40,'b'*40))
        self.mocks['verify_performance'].assert_called_once_with(self.root/'candidate.json',self.root/'performance')
        self.mocks['verify_environment'].assert_called_once_with(self.root/'candidate.json',self.root/'environment.json',self.root/'production/lane.json',self.root/'fault/lane.json',self.root/'performance/paired/paired.json')
        self.assertEqual(self.mocks['verify_security'].call_args.args[1:],(self.root/'security.json','a'*40,'b'*40))

    def test_pass_labels_cannot_hide_any_semantic_failure(self):
        for failing in self.mocks.values():
            failing.side_effect=ValueError('raw result rejected')
            with self.assertRaises(v.EvidenceError): self.verify()
            failing.side_effect=None

    def test_missing_api_and_wrong_package_source_reject(self):
        self.mocks['verify_consumer_report'].return_value={'source_commit':'b'*40}
        with self.assertRaises(v.EvidenceError): self.verify()
        self.mocks['verify_consumer_report'].return_value=self.candidate
        self.entries[1]['id']='generic-pass-label'
        with self.assertRaises(v.EvidenceError): self.verify()

    def test_each_required_environment_or_security_entry_is_mandatory(self):
        original=self.entries[:]
        for kind in ('environment','security','process','fault'):
            self.entries=[entry for entry in original if entry['kind']!=kind]
            with self.assertRaises(v.EvidenceError): self.verify()
        self.entries=original
        self.entries.append(dict(kind='process',id='release-bundle',path='release-bundle.json'))
        self.verify()

    def test_security_cannot_substitute_another_diagnostics_run(self):
        self.security_report['fault']['sha256']='f'*64
        with self.assertRaisesRegex(v.EvidenceError,'complete E2E lane'): self.verify()

if __name__ == '__main__': unittest.main()
