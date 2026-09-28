#!/usr/bin/env python3
"""Synthetic catalog observations exercise verifier rejection; no solver runs."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import yaml
SPEC=importlib.util.spec_from_file_location('formal_catalog',Path(__file__).with_name('v07_formal_catalog.py'))
c=importlib.util.module_from_spec(SPEC);sys.modules[SPEC.name]=c;SPEC.loader.exec_module(c)
ROOT=Path(__file__).resolve().parents[2]

class CatalogContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.commit=subprocess.check_output(['git','-C',str(ROOT),'rev-parse','HEAD'],text=True).strip()
        cls.inputs,_=c.e.trusted_contract(ROOT,cls.commit)
        raw=subprocess.check_output(['git','-C',str(ROOT),'show',cls.commit+':formal/chirps-durable/compose.yml'])
        cls.service=yaml.safe_load(raw)['services']['suite'];cls.expected=c.expected_logs(cls.service)
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
        self.report=dict(schema='chirps.formal-catalog-collection/v1',source=dict(source_commit=self.commit,inputs=self.inputs),image=c.e.IMAGE,command_sha256=c.e.sha(self.service['command'][0].replace('$$','$').encode()),status='collected-unverified',results=[])
        for probe,(code,log) in self.expected.items():
            (self.root/(probe+'.log')).write_text(log)
            self.report['results'].append(dict(probe=probe,exit_code=code,log=probe+'.log',sha256=c.e.sha(log.encode()),failure=None))
    def check(self):
        path=self.root/'report.json';path.write_text(json.dumps(self.report))
        return c.verify_catalog_report(ROOT,path,self.commit)
    def test_synthetic_full_catalog(self):self.assertEqual(self.check()['probes'],11)
    def test_wrong_failure_reason_with_correct_exit(self):
        record=self.report['results'][1];raw=b'Permission denied\n';(self.root/record['log']).write_bytes(raw);record['sha256']=c.e.sha(raw)
        with self.assertRaisesRegex(ValueError,'wrong reason'):self.check()
    def test_pass_label_does_not_hide_missing_probe(self):
        self.report['results'].pop();self.report['status']='collected-unverified'
        with self.assertRaisesRegex(ValueError,'inventory differs'):self.check()
    def test_boolean_is_not_exit_zero(self):
        self.report['results'][0]['exit_code']=False
        with self.assertRaisesRegex(ValueError,'unexpected exit'):self.check()
    def test_exit_zero_not_negative_success(self):
        self.report['results'][1]['exit_code']=0
        with self.assertRaisesRegex(ValueError,'unexpected exit'):self.check()
    def test_stale_commit(self):
        self.report['source']['source_commit']='1'*40
        with self.assertRaisesRegex(ValueError,'candidate binding'):self.check()
    def test_changed_command(self):
        self.report['command_sha256']='0'*64
        with self.assertRaisesRegex(ValueError,'command/image'):self.check()
    def test_negative_probe_launched_checker(self):
        record=self.report['results'][1];raw=(self.expected['missing-tool'][1]+'checker phase started\n').encode()
        (self.root/record['log']).write_bytes(raw);record['sha256']=c.e.sha(raw)
        with self.assertRaisesRegex(ValueError,'wrong reason'):self.check()
    def test_digest_mismatch(self):
        (self.root/'none.log').write_text('PASS')
        with self.assertRaisesRegex(ValueError,'digest differs'):self.check()

if __name__=='__main__':unittest.main()
