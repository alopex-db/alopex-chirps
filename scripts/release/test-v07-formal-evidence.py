#!/usr/bin/env python3
"""Explicit synthetic protocol fixtures, never release evidence."""
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

SPEC=importlib.util.spec_from_file_location('formal_evidence',Path(__file__).with_name('v07_formal_evidence.py'))
e=importlib.util.module_from_spec(SPEC);sys.modules[SPEC.name]=e;SPEC.loader.exec_module(e)
ROOT=Path(__file__).resolve().parents[2]

class EvidenceContract(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name);self.addCleanup(self.temp.cleanup)
        self.source='TypeOK == state.ready \\in BOOLEAN\nAbsent == ~state.ready\n===='
        self.config='CONSTANT UnsafeMode = "none"\n\nINIT Init\nNEXT Next\n\nINVARIANT TypeOK\nINVARIANT Absent\n'
        self.job=dict(id='fixture:witness',kind='witness',tla='fixture.tla',expected=12,bound=3,target='Absent',source=self.source,config=self.config)
        self.command=['--out-dir=/out/checker','check','--config=/out/model.cfg','--length=3','/inputs/fixture.tla']
        trace={'#meta':{'format':'ITF'},'vars':['state'],'params':['UnsafeMode'],'states':[
            {'#meta':{'index':i},'UnsafeMode':'none','state':{'ready':bool(i)}} for i in range(2)]}
        self.files={
            'console.log':'# APALACHE version: 0.58.3 | build: v0.58.3\nAll expressions are typed\nUsing inv predicate(s) TypeOK, Absent from the TLC config\nState 1: state invariant 3 violated.\nFound 1 error(s)\nThe outcome is: Error\nEXITCODE: ERROR (12)\n',
            'detailed.log':'Producing verification conditions from the invariant TypeOK\nProducing verification conditions from the invariant Absent\n',
            'run.txt':' '.join(self.command), 'model.cfg':self.config,
            'violation.itf.json':json.dumps(trace),'violation1.itf.json':json.dumps(trace),
            'violation.tla':'InvariantViolation == state.ready\n'}
        self.record={k:v for k,v in self.job.items() if k not in ('source','config')}
        self.record.update(command=self.command,exit_code=12,failure=None,artifacts={})
        self.flush()
    def flush(self):
        for name,text in self.files.items():
            (self.root/name).write_text(text);self.record['artifacts'][name]=e.sha(text.encode())
    def check(self):return e.verify_job(self.root,self.record,self.job)
    def test_synthetic_valid_counterexample(self):self.assertEqual(self.check()['witness']['states'],2)
    def test_typeok_failure_cannot_hide_under_exit12(self):
        for name in ('violation.itf.json','violation1.itf.json'):
            data=json.loads(self.files[name]);data['states'][1]['state']['ready']='TRUE';self.files[name]=json.dumps(data)
        self.flush()
        with self.assertRaisesRegex(ValueError,'TypeOK violation'):self.check()
    def test_wrong_witness_cannot_hide_under_exit12(self):
        for name in ('violation.itf.json','violation1.itf.json'):
            data=json.loads(self.files[name]);data['states'][1]['state']['ready']=False;self.files[name]=json.dumps(data)
        self.flush()
        with self.assertRaisesRegex(ValueError,'does not reach'):self.check()
    def test_filtered_command(self):
        self.record['command']=[*self.command,'--inv=TypeOK']
        with self.assertRaisesRegex(ValueError,'altered checker command'):self.check()
    def test_actual_shorter_bound(self):
        self.files['run.txt']=self.files['run.txt'].replace('--length=3','--length=1');self.flush()
        with self.assertRaisesRegex(ValueError,'actual checker invocation'):self.check()
    def test_config_suppression(self):
        self.files['model.cfg']=self.config.replace('INVARIANT TypeOK\n','');self.flush()
        with self.assertRaisesRegex(ValueError,'configuration differs'):self.check()
    def test_rehashed_failure_log(self):
        self.files['console.log']=self.files['console.log'].replace('EXITCODE: ERROR (12)','EXITCODE: ERROR (1)');self.flush()
        with self.assertRaisesRegex(ValueError,'counterexample absent'):self.check()
    def test_missing_raw_log(self):
        self.record['artifacts'].pop('console.log')
        with self.assertRaisesRegex(ValueError,'missing/duplicate'):self.check()
    def test_bad_hash(self):
        (self.root/'console.log').write_text('pass')
        with self.assertRaisesRegex(ValueError,'digest differs'):self.check()
    def test_timeout_even_with_success_bytes(self):
        self.record['failure']='job timeout'
        with self.assertRaisesRegex(ValueError,'failed or timed out'):self.check()
    def test_trace_swapped(self):
        data=json.loads(self.files['violation1.itf.json']);data['states'][1]['state']['ready']=False
        self.files['violation1.itf.json']=json.dumps(data);self.flush()
        with self.assertRaisesRegex(ValueError,'trace files differ'):self.check()
    def test_path_escape(self):
        with self.assertRaisesRegex(ValueError,'unsafe'):e.artifact(self.root,'../escape','0'*64)
    def test_inventory_and_candidate_binding(self):
        commit='1'*40;inputs={'formal/source':'2'*64}
        report=dict(schema='chirps.formal-raw-collection/v1',source={'source_commit':commit,'inputs':inputs},checker_image=e.IMAGE,status='collected-unverified',mode='development-subset',planned_job_ids=[self.job['id']],jobs=[self.record])
        path=self.root/'report.json';path.write_text(json.dumps(report))
        with patch.object(e,'trusted_contract',return_value=(inputs,{self.job['id']:self.job})):
            self.assertEqual(e.verify_formal_report(ROOT,path,commit,False)['status'],'development-verified')
            with self.assertRaisesRegex(ValueError,'full 160-job'):e.verify_formal_report(ROOT,path,commit)
            with self.assertRaisesRegex(ValueError,'exact candidate'):e.verify_formal_report(ROOT,path,'3'*40,False)
            report['jobs']=[self.record,self.record];path.write_text(json.dumps(report))
            with self.assertRaisesRegex(ValueError,'duplicate'):e.verify_formal_report(ROOT,path,commit,False)
    def test_normal_requires_complete_declared_bound(self):
        self.job.update(kind='normal',expected=0);self.job.pop('target')
        self.record.update(kind='normal',expected=0,exit_code=0);self.record.pop('target')
        self.files={k:v for k,v in self.files.items() if not k.startswith('violation')}
        self.record['artifacts']={}
        self.files['console.log']='# APALACHE version: 0.58.3 | build: v0.58.3\nAll expressions are typed\nUsing inv predicate(s) TypeOK, Absent from the TLC config\nThe outcome is: NoError\nChecker reports no error up to computation length 3\nEXITCODE: OK\n'
        self.flush();self.assertEqual(self.check()['bound'],3)
        self.files['console.log']=self.files['console.log'].replace('computation length 3','computation length 2');self.flush()
        with self.assertRaisesRegex(ValueError,'bound not completed'):self.check()
    def test_real_trusted_contract(self):
        commit=subprocess.check_output(['git','-C',str(ROOT),'rev-parse','HEAD'],text=True).strip()
        inputs,jobs=e.trusted_contract(ROOT,commit)
        self.assertEqual(len(inputs),14);self.assertEqual(len(jobs),160)
        self.assertEqual(sorted(j['bound'] for j in jobs.values() if j['kind']=='normal'),[20,24,24,28])

if __name__=='__main__':unittest.main()
