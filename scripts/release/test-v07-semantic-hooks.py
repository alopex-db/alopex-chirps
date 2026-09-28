#!/usr/bin/env python3
"""Central category wiring tests; mocked fixtures are not release evidence."""
import importlib.util
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
        for kind, identifier, path in [('package','packages','consumer/report.json'), ('compatibility','compatibility-matrix','compatibility-matrix.json'), ('performance','perf','performance/paired/paired.json')]:
            file = self.root / path
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text('{}')
            self.entries.append(dict(kind=kind,id=identifier,path=path))
        self.candidate = {'source_commit':'a'*40,'iggy_commit':'b'*40}

    def verify(self):
        v.verify_release_categories(self.root, self.entries, self.candidate, self.root/'candidate.json')

    def test_calls_all_three_semantic_verifiers_with_exact_paths(self):
        with patch.object(v,'verify_consumer_report',return_value=self.candidate) as consumer, patch.object(v,'verify_compatibility') as compatibility, patch.object(v,'verify_performance') as perf:
            self.verify()
            consumer.assert_called_once_with(self.root/'consumer/package-set.json',self.root/'consumer/report.json','stored-archives')
            self.assertEqual(compatibility.call_args.args[1:],(self.root/'compatibility-matrix.json','a'*40,'b'*40))
            perf.assert_called_once_with(self.root/'candidate.json',self.root/'performance')

    def test_pass_labels_cannot_hide_any_semantic_failure(self):
        for failing in ('verify_consumer_report','verify_compatibility','verify_performance'):
            with patch.object(v,'verify_consumer_report',return_value=self.candidate), patch.object(v,'verify_compatibility'), patch.object(v,'verify_performance'):
                with patch.object(v,failing,side_effect=ValueError('raw result rejected')), self.assertRaises(v.EvidenceError):
                    self.verify()

    def test_missing_api_and_wrong_package_source_reject(self):
        with patch.object(v,'verify_consumer_report',return_value={'source_commit':'b'*40}), self.assertRaises(v.EvidenceError):
            self.verify()
        self.entries[1]['id'] = 'generic-pass-label'
        with patch.object(v,'verify_consumer_report',return_value=self.candidate), self.assertRaises(v.EvidenceError):
            self.verify()

if __name__ == '__main__':
    unittest.main()
