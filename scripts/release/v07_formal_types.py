#!/usr/bin/env python3
"""Check the four models' finite TypeOK contracts against decoded ITF states.

Only the current finite-domain grammar is accepted. Unknown syntax, recursive
operators, excessive ranges, untyped equality and incomplete maps fail closed.
"""
from dataclasses import dataclass
import importlib.util
import json
from pathlib import Path
import re

SPEC=importlib.util.spec_from_file_location('formal_witness',Path(__file__).with_name('v07_formal_witness.py'))
w=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(w)
TOKEN=re.compile(r'\s*(\\cup|\\|\.\.|->|[(){}\[\],+-]|"(?:[^"\\]|\\.)*"|[0-9]+|[A-Za-z_][A-Za-z0-9_]*)')

@dataclass(frozen=True)
class FunctionDomain:
    keys: frozenset
    values: object

BOOLEAN=object()

def clean_source(tla):
    return re.sub(r'\\\*[^\n]*','',re.sub(r'\(\*.*?\*\)','',tla,flags=re.S))

def definitions(tla):
    clean=clean_source(tla)
    matches=list(re.finditer(r'^(\w+)(?:\([^\n]*\))?\s*==',clean,re.M))
    result={}
    for i,m in enumerate(matches):
        end=matches[i+1].start() if i+1<len(matches) else len(clean)
        result[m[1]]=clean[m.end():end].split('====',1)[0].strip()
    return result

class DomainParser:
    def __init__(self,text,environment,operators,active=()):
        self.tokens=[];self.pos=0;self.environment=environment;self.operators=operators;self.active=active
        pos=0
        while pos<len(text):
            match=TOKEN.match(text,pos)
            if not match:
                if not text[pos:].strip():break
                raise ValueError('unsupported finite TypeOK domain syntax')
            self.tokens.append(match[1]);pos=match.end()
    def peek(self):return self.tokens[self.pos] if self.pos<len(self.tokens) else None
    def take(self,expected=None):
        value=self.peek()
        if value is None or expected is not None and value!=expected:raise ValueError('invalid TypeOK domain')
        self.pos+=1;return value
    def parse(self,minimum=0):
        token=self.take()
        if token=='(':
            value=self.parse();self.take(')')
        elif token=='[':
            keys=self.parse();self.take('->');values=self.parse();self.take(']')
            if type(keys) is not frozenset:raise ValueError('invalid TypeOK function domain')
            value=FunctionDomain(keys,values)
        elif token=='{':
            entries=[]
            while self.peek()!='}':
                entries.append(self.parse())
                if self.peek()!=',':break
                self.take(',')
            self.take('}');value=frozenset(entries)
        elif token.startswith('"'):value=json.loads(token)
        elif token.isdigit():value=int(token)
        elif token=='BOOLEAN':value=BOOLEAN
        elif token in self.environment:value=self.environment[token]
        elif token in self.operators:
            if token in self.active:raise ValueError('recursive TypeOK domain')
            value=DomainParser(self.operators[token],self.environment,self.operators,(*self.active,token)).complete()
        else:raise ValueError('unknown TypeOK domain '+token)
        priorities={'\\cup':10,'\\':10,'..':20,'+':30,'-':30}
        while self.peek() in priorities and priorities[self.peek()]>=minimum:
            op=self.take();rhs=self.parse(priorities[op]+1)
            if op in ('\\cup','\\'):
                if type(value) is not frozenset or type(rhs) is not frozenset:raise ValueError('non-set TypeOK domain')
                value=value|rhs if op=='\\cup' else value-rhs
            else:
                if type(value) is not int or type(rhs) is not int:raise ValueError('non-integer TypeOK domain')
                if op=='..':
                    if not 0<=rhs-value<=10000:raise ValueError('excessive TypeOK domain')
                    value=frozenset(range(value,rhs+1))
                else:value=value+rhs if op=='+' else value-rhs
        return value
    def complete(self):
        result=self.parse()
        if self.peek() is not None:raise ValueError('trailing TypeOK domain')
        return result

def member(value,domain):
    if domain is BOOLEAN:return type(value) is bool
    if isinstance(domain,FunctionDomain):
        return (type(value) is dict and len(value)==len(domain.keys)
                and all(any(type(k) is type(d) and k==d for d in domain.keys) for k in value)
                and all(member(v,domain.values) for v in value.values()))
    if type(domain) is not frozenset:raise ValueError('invalid membership domain')
    return any(type(value) is type(item) and value==item for item in domain)

def verify_typeok(tla,trace,config):
    operators=definitions(tla)
    if 'TypeOK' not in operators:raise ValueError('missing TypeOK')
    clauses=[line.strip() for line in operators['TypeOK'].split('/\\') if line.strip()]
    constants={}
    for name,raw in re.findall(r'^CONSTANT (\w+) = (.+)$',config,re.M):
        if raw.startswith('"'):value=json.loads(raw)
        elif re.fullmatch('[0-9]+',raw):value=int(raw)
        else:raise ValueError('unsupported configuration constant')
        constants[name]=value
    if len(trace.get('params',[]))!=len(constants) or set(trace.get('params',[]))!=set(constants):raise ValueError('ITF constants differ from CFG')
    if trace.get('vars')!=['state'] or not trace.get('states'):raise ValueError('invalid TypeOK trace')
    for index,raw in enumerate(trace['states']):
        if type(raw.get('#meta',{}).get('index')) is not int or raw['#meta']['index']!=index:raise ValueError('invalid TypeOK trace indices')
        if set(raw)!={'#meta','state',*constants}:raise ValueError('unexpected ITF state variables')
        environment={key:w.decode_itf(value) for key,value in raw.items() if key!='#meta'}
        for key,value in constants.items():
            if key not in environment or type(environment[key]) is not type(value) or environment[key]!=value:raise ValueError('ITF constant changed')
        for clause in clauses:
            match=re.fullmatch(r'(state\.\w+|\w+)\s+(\\in|\\subseteq|=)\s+(.*)',clause,re.S)
            if match is None:raise ValueError('unsupported TypeOK clause')
            lhs,operation,expression=match.groups()
            value=w.evaluate(w.Parser(lhs).complete(),environment)
            expected=DomainParser(expression,environment,operators).complete()
            if operation=='\\in':valid=member(value,expected)
            elif operation=='\\subseteq':valid=type(value) is frozenset and all(member(item,expected) for item in value)
            else:valid=type(value) is type(expected) and value==expected
            if not valid:raise ValueError(f'TypeOK violation at state {index}: {lhs}')
    return {'states':len(trace['states']),'typeok_clauses':len(clauses)}
