#!/usr/bin/env python3
"""Composite wiring tests explicitly mock solvers; no release data is generated."""
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).parent))
import v07_formal_release as f
from v07_formal_evidence import sha
SPEC=importlib.util.spec_from_file_location('central',Path(__file__).with_name('verify-v0.7-evidence.py'))
v=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(v)

class FormalComposite(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name).resolve()
        self.refinements={'status':'pass','references':[{'synthetic':True}]}
        self.report=dict(schema='chirps.formal-release/v1',source_commit='a'*40,iggy_commit='b'*40)
        for kind in ('raw','catalog','refinements'):
            path=self.root/(kind+'.json');path.write_text(json.dumps(self.refinements if kind=='refinements' else {'synthetic':True}))
            self.report[kind]={'path':path.name,'sha256':sha(path.read_bytes())}
        self.path=self.root/'models.json';self.path.write_text(json.dumps(self.report))
        self.raw=self.enterContext(patch.object(f,'verify_formal_report',return_value={'jobs':[{}]*160}))
        self.catalog=self.enterContext(patch.object(f,'verify_catalog_report',return_value={'probes':11}))
        self.refs=self.enterContext(patch.object(f,'verify_refinements',return_value=self.refinements))
    def check(self):return f.verify_release_models(self.root/'trusted-chirps',self.root/'trusted-iggy',self.path,'a'*40,'b'*40)
    def test_every_required_check_uses_caller_sources(self):
        result=self.check();self.assertEqual(result['checker_jobs'],160)
        self.raw.assert_called_once_with(self.root/'trusted-chirps',self.root/'raw.json','a'*40)
        self.catalog.assert_called_once_with(self.root/'trusted-chirps',self.root/'catalog.json','a'*40)
        self.refs.assert_called_once_with(self.root/'trusted-chirps','a'*40,self.root/'trusted-iggy','b'*40)
    def test_each_failure_rejects_composite(self):
        for verifier in (self.raw,self.catalog,self.refs):
            verifier.side_effect=ValueError('synthetic rejection')
            with self.assertRaises(ValueError):self.check()
            verifier.side_effect=None
    def test_stale_refinement_even_with_pass_label(self):
        self.refs.return_value={'status':'pass','references':[]}
        with self.assertRaisesRegex(ValueError,'exact Git objects'):self.check()
    def test_subset_cannot_be_requested_by_report(self):
        self.report['require_complete']=False;self.path.write_text(json.dumps(self.report))
        with self.assertRaisesRegex(ValueError,'schema differs'):self.check()
    def test_source_root_cannot_be_requested_by_report(self):
        self.report['iggy_root']='/arbitrary';self.path.write_text(json.dumps(self.report))
        with self.assertRaisesRegex(ValueError,'schema differs'):self.check()
    def test_packager_requires_all_gates_before_creating_output(self):
        output=self.root/'packaged.json'
        args=(self.root/'trusted-chirps',self.root/'trusted-iggy','a'*40,'b'*40,self.root/'raw.json',self.root/'catalog.json',self.root/'refinements.json',output)
        self.raw.side_effect=ValueError('full160 required')
        with self.assertRaisesRegex(ValueError,'full160 required'):f.package_release_models(*args)
        self.assertFalse(output.exists())
        self.raw.side_effect=None;f.package_release_models(*args);self.assertTrue(output.is_file())
        with self.assertRaises(FileExistsError):f.package_release_models(*args)
    def test_missing_component_rejected(self):
        self.report.pop('catalog');self.path.write_text(json.dumps(self.report))
        with self.assertRaisesRegex(ValueError,'schema differs'):self.check()
    def test_central_requires_foreign_objects(self):
        with patch.dict(os.environ,{},clear=True),self.assertRaisesRegex(v.EvidenceError,'CHIRPS_IGGY_SOURCE_ROOT'):
            v.verify_model_category(self.root,[{'kind':'model','id':'formal-models','path':'models.json'}],{'source_commit':'a'*40,'iggy_commit':'b'*40})
    def test_central_calls_composite_and_propagates_failure(self):
        entries=[{'kind':'model','id':'formal-models','path':'models.json'}];candidate={'source_commit':'a'*40,'iggy_commit':'b'*40}
        with patch.dict(os.environ,{'CHIRPS_SOURCE_ROOT':str(self.root/'trusted-chirps'),'CHIRPS_IGGY_SOURCE_ROOT':str(self.root/'trusted-iggy')}),patch.object(v,'verify_release_models') as call:
            v.verify_model_category(self.root,entries,candidate)
            call.assert_called_once_with(self.root/'trusted-chirps',self.root/'trusted-iggy',self.path,'a'*40,'b'*40)
            call.side_effect=ValueError('subset')
            with self.assertRaisesRegex(v.EvidenceError,'subset'):v.verify_model_category(self.root,entries,candidate)

if __name__=='__main__':unittest.main()
