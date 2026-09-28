#!/usr/bin/env python3
import importlib.util
from pathlib import Path
import re
import sys
import unittest

SPEC=importlib.util.spec_from_file_location('formal_types',Path(__file__).with_name('v07_formal_types.py'))
t=importlib.util.module_from_spec(SPEC);sys.modules[SPEC.name]=t;SPEC.loader.exec_module(t)
ROOT=Path(__file__).resolve().parents[2]/'formal/chirps-durable'

class TypeContract(unittest.TestCase):
    def fixture(self):
        return {'vars':['state'],'params':['Limit'],'states':[{'#meta':{'index':0},'Limit':{'#bigint':'2'},'state':{'count':{'#bigint':'1'},'ready':True,'counts':{'#map':[['x',{'#bigint':'1'}]]}}}]}
    def test_all_four_typeok_grammars(self):
        total=0
        for model in ('SendLease','Subscription','LifecycleState','MetadataRecovery'):
            source=(ROOT/(model+'.tla')).read_text();ops=t.definitions(source)
            config=(ROOT/(model+'.cfg')).read_text()
            env={name:int(raw) if raw.isdigit() else raw.strip('"') for name,raw in re.findall(r'^CONSTANT (\w+) = (.+)$',config,re.M)}
            for clause in ops['TypeOK'].split('/\\'):
                if not clause.strip():continue
                match=re.fullmatch(r'(state\.\w+|\w+)\s+(\\in|\\subseteq|=)\s+(.*)',clause.strip(),re.S)
                self.assertIsNotNone(match)
                t.DomainParser(match[3],env,ops).complete();total+=1
        self.assertGreater(total,380)
    def test_valid_and_invalid_typeok(self):
        source='TypeOK == /\\ state.count \\in 0..Limit /\\ state.ready \\in BOOLEAN\n===='
        trace=self.fixture();self.assertEqual(t.verify_typeok(source,trace,'CONSTANT Limit = 2\n')['states'],1)
        trace['states'][0]['state']['count']={'#bigint':'3'}
        with self.assertRaisesRegex(ValueError,'TypeOK violation'):t.verify_typeok(source,trace,'CONSTANT Limit = 2\n')
    def test_boolean_is_not_integer_member(self):
        self.assertFalse(t.member(True,frozenset((1,2))))
        self.assertFalse(t.member(1,t.BOOLEAN))
    def test_function_domain_and_range_exact(self):
        domain=t.DomainParser('[Keys -> 0..2]',{}, {'Keys':'{"x", "y"}'}).complete()
        self.assertTrue(t.member({'x':1,'y':2},domain))
        for invalid in ({'x':1},{'x':1,'y':3},{'x':True,'y':1},{'x':1,'y':2,'z':0}):self.assertFalse(t.member(invalid,domain))
    def test_constants_must_match_every_state(self):
        trace=self.fixture();trace['states'][0]['Limit']={'#bigint':'3'}
        with self.assertRaisesRegex(ValueError,'constant changed'):t.verify_typeok('TypeOK == state.ready \\in BOOLEAN\n====',trace,'CONSTANT Limit = 2\n')
    def test_trace_cannot_override_trusted_domains(self):
        trace=self.fixture();trace['states'][0]['Choices']={'#set':[True]}
        with self.assertRaisesRegex(ValueError,'unexpected ITF state'):t.verify_typeok('Choices == {1}\nTypeOK == state.ready \\in Choices\n====',trace,'CONSTANT Limit = 2\n')
    def test_unknown_and_recursive_syntax_rejected(self):
        for text,ops in [('SUBSET Keys',{}),('Keys',{'Keys':'Keys'}),('0..10001',{}),('1 = 1',{})]:
            with self.assertRaises(ValueError):t.DomainParser(text,{},ops).complete()
    def test_union_difference_parenthesized_range(self):
        actual=t.DomainParser('(0..(Limit + 1) \\cup {8}) \\ {1}',{'Limit':2},{}).complete()
        self.assertEqual(actual,frozenset((0,2,3,8)))

if __name__=='__main__':unittest.main()
