#!/usr/bin/env python3
import importlib.util
from pathlib import Path
import unittest
import yaml
SPEC=importlib.util.spec_from_file_location('witness',Path(__file__).with_name('v07_formal_witness.py'))
w=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(w)
ROOT=Path(__file__).resolve().parents[2]/'formal/chirps-durable'

class WitnessContract(unittest.TestCase):
    def fixture(self):
        return {'#meta':{'format':'ITF'},'vars':['state'],'states':[
            {'#meta':{'index':0},'state':{'ready':False,'count':{'#bigint':'0'}}},
            {'#meta':{'index':1},'state':{'ready':True,'count':{'#bigint':'2'}}}]}
    def test_all_27_current_predicates_parse(self):
        pairs=[('catalog.yaml','SendLease.tla'),('subscription-catalog.yaml','Subscription.tla'),('lifecycle-catalog.yaml','LifecycleState.tla'),('metadata-catalog.yaml','MetadataRecovery.tla')]
        count=0
        for cat,tla in pairs:
            for entry in yaml.safe_load((ROOT/cat).read_text())['model']['checker']['witness_profiles']:
                w.Parser(w.predicate_source((ROOT/tla).read_text(),entry['target'])).complete();count+=1
        self.assertEqual(count,27)
    def test_actual_predicate_semantics(self):
        source='Absent == ~(state.ready /\\ state.count = 2)\n===='
        self.assertEqual(w.verify_witness(source,'Absent',self.fixture(),2)['states'],2)
        bad=self.fixture();bad['states'][-1]['state']['count']={'#bigint':'1'}
        with self.assertRaisesRegex(ValueError,'does not reach'):w.verify_witness(source,'Absent',bad,2)
    def test_missing_boolean_is_not_truthy(self):
        with self.assertRaisesRegex(ValueError,'non-boolean'):w.evaluate(w.Parser('~state.ready').complete(),{'state':{'ready':'TRUE'}})
    def test_unknown_syntax_fails_closed(self):
        for source in ('Cardinality(state.ready)',r'\E x \in Things: x = 1','state.ready \\/ TRUE'):
            with self.assertRaises(ValueError):w.Parser(source).complete()
    def test_maps_sets_arithmetic(self):
        node=w.Parser('~(state.counts["payload"] = MaxCapacity - 1 /\\ state.active = {1, 2})').complete()
        env={'state':{'counts':{'payload':2},'active':frozenset((1,2))},'MaxCapacity':3}
        self.assertFalse(w.evaluate(node,env))
    def test_typed_equality(self):
        with self.assertRaisesRegex(ValueError,'type mismatch'):w.evaluate(w.Parser('TRUE = 1').complete(),{})
    def test_boolean_is_not_trace_index(self):
        trace=self.fixture();trace['states'][0]['#meta']['index']=False
        with self.assertRaisesRegex(ValueError,'non-contiguous'):w.verify_witness('Absent == ~state.ready\n====','Absent',trace,2)
    def test_bad_index_and_length(self):
        for change in ('index','length'):
            trace=self.fixture()
            if change=='index':trace['states'][1]['#meta']['index']=3
            with self.assertRaises(ValueError):w.verify_witness('Absent == ~state.ready\n====','Absent',trace,0 if change=='length' else 2)
    def test_duplicate_map_rejected(self):
        with self.assertRaisesRegex(ValueError,'duplicate'):w.decode_itf({'#map':[['x',True],['x',False]]})

if __name__=='__main__':unittest.main()
