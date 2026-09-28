#!/usr/bin/env python3
"""Evaluate the deliberately small current witness-predicate language on ITF.

This is not a TLA+ interpreter: unrecognized syntax fails closed. The source
predicate must come from the trusted candidate Git object, never from evidence.
It establishes reachability of that predicate only, not model/trace correctness.
"""
import json
import re

TOKEN=re.compile(r'\s*(/\\|<=|>=|[(){}\[\].,=#+~<>-]|"(?:[^"\\]|\\.)*"|[0-9]+|[A-Za-z_][A-Za-z0-9_]*)')
PRECEDENCE={'/\\':10,'=':20,'#':20,'<':20,'>':20,'<=':20,'>=':20,'+':30,'-':30}

def predicate_source(tla,target):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',target):raise ValueError('unsafe operator')
    match=re.search(r'^'+target+r'\s*==\s*(.*?)(?=^\w+(?:\([^\n]*\))?\s*==|^=+)',tla,re.M|re.S)
    if match is None:raise ValueError('missing witness operator')
    result=match[1].strip()
    if not result.startswith('~'):raise ValueError('witness must negate reachability')
    return result

class Parser:
    def __init__(self,text):
        self.tokens=[];pos=0
        while pos<len(text):
            match=TOKEN.match(text,pos)
            if not match:
                if not text[pos:].strip():break
                raise ValueError('unsupported witness syntax')
            self.tokens.append(match[1]);pos=match.end()
        self.pos=0
    def peek(self):return self.tokens[self.pos] if self.pos<len(self.tokens) else None
    def take(self,expected=None):
        value=self.peek()
        if value is None or expected is not None and value!=expected:raise ValueError('invalid witness syntax')
        self.pos+=1;return value
    def parse(self,minimum=0):
        token=self.take()
        if token=='~':node=('not',self.parse(40))
        elif token=='(':
            node=self.parse();self.take(')')
        elif token=='{':
            values=[]
            while self.peek()!='}':
                values.append(self.parse())
                if self.peek()!=',':break
                self.take(',')
            self.take('}');node=('set',values)
        elif token.startswith('"'):node=('literal',json.loads(token))
        elif token.isdigit():node=('literal',int(token))
        elif token in ('TRUE','FALSE'):node=('literal',token=='TRUE')
        elif re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',token):node=('name',token)
        else:raise ValueError('unexpected witness token')
        while True:
            token=self.peek()
            if token=='.':
                self.take();field=self.take()
                if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',field):raise ValueError('invalid field')
                node=('field',node,field)
            elif token=='[':
                self.take();key=self.parse();self.take(']');node=('index',node,key)
            elif token in PRECEDENCE and PRECEDENCE[token]>=minimum:
                self.take();node=(token,node,self.parse(PRECEDENCE[token]+1))
            else:break
        return node
    def complete(self):
        node=self.parse()
        if self.peek() is not None:raise ValueError('trailing witness syntax')
        return node

def boolean(value):
    if type(value) is not bool:raise ValueError('non-boolean witness expression')
    return value

def evaluate(node,environment):
    op=node[0]
    if op=='literal':return node[1]
    if op=='name':
        if node[1] not in environment:raise ValueError('missing witness variable '+node[1])
        return environment[node[1]]
    if op=='not':return not boolean(evaluate(node[1],environment))
    if op=='set':return frozenset(evaluate(n,environment) for n in node[1])
    if op in ('field','index'):
        container=evaluate(node[1],environment)
        key=node[2] if op=='field' else evaluate(node[2],environment)
        if not isinstance(container,dict) or key not in container:raise ValueError('missing witness field/index')
        return container[key]
    left=evaluate(node[1],environment);right=evaluate(node[2],environment)
    if op=='/\\':return boolean(left) & boolean(right)
    if type(left) is not type(right):raise ValueError('witness operand type mismatch')
    if op=='=':return left==right
    if op=='#':return left!=right
    if type(left) is not int:raise ValueError('non-integer witness arithmetic')
    if op=='+':return left+right
    if op=='-':return left-right
    if op=='<':return left<right
    if op=='>':return left>right
    if op=='<=':return left<=right
    if op=='>=':return left>=right
    raise ValueError('unsupported witness operator')

def decode_itf(value):
    if type(value) in (bool,str):return value
    if not isinstance(value,dict):raise ValueError('unsupported ITF value')
    if set(value)=={'#bigint'}:
        if not isinstance(value['#bigint'],str) or not re.fullmatch(r'-?(0|[1-9][0-9]*)',value['#bigint']):raise ValueError('invalid ITF integer')
        return int(value['#bigint'])
    if set(value)=={'#set'}:
        entries=[decode_itf(x) for x in value['#set']]
        if len(frozenset(entries))!=len(entries):raise ValueError('duplicate ITF set member')
        return frozenset(entries)
    if set(value)=={'#map'}:
        entries=[(decode_itf(k),decode_itf(v)) for k,v in value['#map']]
        result=dict(entries)
        if len(result)!=len(entries):raise ValueError('duplicate ITF map key')
        return result
    if any(key.startswith('#') for key in value):raise ValueError('unsupported ITF tag')
    return {key:decode_itf(item) for key,item in value.items()}

def verify_witness(tla,target,trace,bound):
    if trace.get('#meta',{}).get('format')!='ITF' or trace.get('vars')!=['state']:
        raise ValueError('unexpected witness ITF format')
    states=trace.get('states')
    if not isinstance(states,list) or not 1<=len(states)<=bound+1:raise ValueError('invalid witness trace length')
    syntax=Parser(predicate_source(tla,target)).complete()
    results=[]
    for index,raw in enumerate(states):
        if type(raw.get('#meta',{}).get('index')) is not int or raw['#meta']['index']!=index:raise ValueError('non-contiguous witness trace')
        environment={key:decode_itf(value) for key,value in raw.items() if key!='#meta'}
        results.append(boolean(evaluate(syntax,environment)))
    if results[-1] is not False:raise ValueError('witness terminal state does not reach target')
    if any(value is not True for value in results[:-1]):raise ValueError('witness reached before reported terminal state')
    return {'target':target,'states':len(states),'terminal_predicate':False}
