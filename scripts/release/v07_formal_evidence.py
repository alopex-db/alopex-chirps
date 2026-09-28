#!/usr/bin/env python3
"""Independently revalidate immutable inputs and raw bounded-model evidence."""
import argparse
from collections import Counter
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import shlex
import subprocess
import sys
import yaml

IMAGE='ghcr.io/apalache-mc/apalache@sha256:fde994fd109323934b9abb7ad169de37b29acf2141483367f2913cae30ff3795'

def module(name,filename):
    spec=importlib.util.spec_from_file_location(name,Path(__file__).with_name(filename))
    value=importlib.util.module_from_spec(spec);sys.modules[name]=value;spec.loader.exec_module(value);return value
w=module('formal_witness','v07_formal_witness.py')
t=module('formal_types','v07_formal_types.py')

def sha(raw):return hashlib.sha256(raw).hexdigest()

def trusted_contract(root,commit):
    if not re.fullmatch('[0-9a-f]{40}',commit):raise ValueError('immutable candidate commit required')
    kind=subprocess.check_output(['git','-C',str(root),'cat-file','-t',commit],stderr=subprocess.PIPE,timeout=30).decode().strip()
    if kind!='commit':raise ValueError('candidate is not a Git commit')
    def git(path):
        return subprocess.check_output(['git','-C',str(root),'show',commit+':'+path],stderr=subprocess.PIPE,timeout=30)
    inputs={name:git(name) for name in ('formal/compose.yml','formal/chirps-durable/compose.yml')}
    service=yaml.safe_load(inputs['formal/chirps-durable/compose.yml'])['services']['suite']
    if service['image']!=IMAGE or service['environment']['CHIRPS_DURABLE_CHECKER_IMAGE']!=IMAGE:raise ValueError('trusted checker image differs')
    jobs=[]
    for row in service['environment']['CHIRPS_DURABLE_MODELS'].splitlines():
        fields=row.split('|')
        if len(fields)!=12:raise ValueError('unexpected model registry shape')
        mid,cat,tla,cfg,bound,nred,nwit,htla,hcfg,hcat,_,_=fields
        for path,expected in ((cat,hcat),(tla,htla),(cfg,hcfg)):
            path='formal/'+path
            if Path(path).is_absolute() or '..' in Path(path).parts:raise ValueError('unsafe model source path')
            raw=git(path)
            if sha(raw)!=expected:raise ValueError('trusted model registry hash mismatch')
            inputs[path]=raw
        catalog=yaml.safe_load(inputs['formal/'+cat])['model']['checker']
        base=inputs['formal/'+cfg].decode();source=inputs['formal/'+tla].decode()
        jobs.extend([dict(id=mid+':typecheck',kind='typecheck',tla=tla,expected=0,source=source),dict(id=mid+':normal',kind='normal',tla=tla,expected=0,bound=int(bound),config=base,source=source)])
        if len(catalog['mutation_profiles'])!=int(nred) or len(catalog['witness_profiles'])!=int(nwit):raise ValueError('catalog profile count differs')
        for kind,entries in [('profile',catalog['mutation_profiles']),('witness',catalog['witness_profiles'])]:
            for entry in entries:
                expected='counterexample' if kind=='profile' else 'reachable'
                if entry['expected']!=expected:raise ValueError('catalog expectation differs')
                constants=re.findall(r'^CONSTANT .*$',base,re.M)
                init=re.search(r'^INIT (\w+)$',base,re.M)[1];nxt=re.search(r'^NEXT (\w+)$',base,re.M)[1]
                if kind=='profile':
                    if sum(line.startswith('CONSTANT UnsafeMode = ') for line in constants)!=1:raise ValueError('UnsafeMode absent')
                    constants=[f'CONSTANT UnsafeMode = "{entry["constant_overrides"]["UnsafeMode"]}"' if line.startswith('CONSTANT UnsafeMode = ') else line for line in constants]
                    replacement=catalog['execution']['profile_materialization'].get('next_replacement')
                    if replacement:nxt=replacement['to']
                else:init=entry.get('init',init);nxt=entry['next']
                cfg_text='\n'.join(constants)+f'\n\nINIT {init}\nNEXT {nxt}\n\nINVARIANT TypeOK\nINVARIANT {entry["target"]}\n'
                jobs.append(dict(id=mid+':'+entry['id'],kind=kind,tla=tla,expected=12,bound=entry['max_steps'],config=cfg_text,target=entry['target'],source=source))
    if len(inputs)!=14 or len(jobs)!=160 or len({job['id'] for job in jobs})!=160:raise ValueError('incomplete trusted model inventory')
    if Counter(job['kind'] for job in jobs)!=dict(typecheck=4,normal=4,profile=125,witness=27):raise ValueError('wrong model phase counts')
    return {name:sha(raw) for name,raw in inputs.items()}, {job['id']:job for job in jobs}

def artifact(root,name,digest):
    path=Path(name)
    if path.is_absolute() or str(path)!=name or '..' in path.parts:raise ValueError('unsafe formal artifact path')
    if any(root.joinpath(*path.parts[:i]).is_symlink() for i in range(1,len(path.parts)+1)):raise ValueError('symlink formal artifact')
    file=root/path
    if not file.is_file() or file.stat().st_size>1024**3:raise ValueError('missing or oversized formal artifact')
    raw=file.read_bytes()
    if sha(raw)!=digest:raise ValueError('formal artifact digest differs')
    return raw

def verify_job(root,record,job):
    for key in ('id','kind','tla','expected','bound','target'):
        if record.get(key)!=job.get(key):raise ValueError('formal job contract differs: '+key)
    if record.get('failure') is not None or type(record.get('exit_code')) is not int or record['exit_code']!=job['expected']:raise ValueError('formal job failed or timed out')
    command=['--out-dir=/out/checker','typecheck','/inputs/'+job['tla']]
    if job['kind']!='typecheck':command=['--out-dir=/out/checker','check','--config=/out/model.cfg',f'--length={job["bound"]}','/inputs/'+job['tla']]
    if record.get('command')!=command:raise ValueError('filtered or altered checker command')
    artifacts=record.get('artifacts',{})
    if not artifacts:raise ValueError('missing raw artifacts')
    raw={name:artifact(root,name,digest) for name,digest in artifacts.items()}
    def unique(basename):
        matches=[data for name,data in raw.items() if Path(name).name==basename]
        if len(matches)!=1:raise ValueError('missing/duplicate '+basename)
        return matches[0]
    console=unique('console.log').decode();detail=unique('detailed.log').decode()
    if '\x1b' in console or '# APALACHE version: 0.58.3 | build: v0.58.3' not in console:raise ValueError('checker version absent/different')
    if 'All expressions are typed' not in console:raise ValueError('typecheck did not complete')
    if Counter(shlex.split(unique('run.txt').decode()))!=Counter(command):raise ValueError('actual checker invocation differs')
    if job['kind']=='typecheck':
        if 'Type checker [OK]' not in console or not console.rstrip().endswith('EXITCODE: OK'):raise ValueError('typecheck raw result failed')
        return {'id':job['id'],'status':'pass'}
    cfg=unique('model.cfg').decode()
    if cfg!=job['config']:raise ValueError('generated model configuration differs')
    invariants=re.findall(r'^INVARIANT (\w+)$',cfg,re.M)
    used=re.findall(r'Using inv predicate\(s\) (.*?) from the TLC config',console)
    if used!=[', '.join(invariants)]:raise ValueError('actual invariants differ')
    for invariant in invariants:
        if f'Producing verification conditions from the invariant {invariant}\n' not in detail:raise ValueError('invariant missing from VCGen')
    if job['kind']=='normal':
        if not console.rstrip().endswith('EXITCODE: OK') or 'The outcome is: NoError' not in console:raise ValueError('normal check did not pass')
        if re.search(r'Checker reports no error up to computation length '+str(job['bound'])+r'\b',console) is None:raise ValueError('normal declared bound not completed')
        if any(Path(name).name.startswith('violation') for name in raw):raise ValueError('normal counterexample present')
        return {'id':job['id'],'status':'pass','bound':job['bound']}
    if not console.rstrip().endswith('EXITCODE: ERROR (12)') or 'The outcome is: Error' not in console or 'Found 1 error(s)' not in console:raise ValueError('expected counterexample absent')
    violations=re.findall(r'State (\d+): state invariant (\d+) violated\.',console)
    if len(violations)!=1:raise ValueError('ambiguous counterexample')
    trace=json.loads(unique('violation.itf.json'))
    numbered=json.loads(unique('violation1.itf.json'))
    for value in (trace,numbered):
        value.get('#meta',{}).pop('description',None)
    if trace!=numbered:raise ValueError('counterexample trace files differ')
    states=trace.get('states',[])
    if not 1<=len(states)<=job['bound']+1 or int(violations[0][0])!=len(states)-1:raise ValueError('counterexample bound/state differs')
    if 'InvariantViolation ==' not in unique('violation.tla').decode():raise ValueError('counterexample condition missing')
    type_result=t.verify_typeok(job['source'],trace,cfg)
    # The actual CFG/VCGen contains exactly TypeOK and the declared target; all
    # trace states satisfy trusted TypeOK, excluding accidental type violations.
    result={'id':job['id'],'status':'pass','counterexample_states':len(states),'typeok':type_result}
    if job['kind']=='witness':result['witness']=w.verify_witness(job['source'],job['target'],trace,job['bound'])
    return result

def verify_formal_report(source_root,report_path,source_commit,require_complete=True):
    report_path=Path(report_path).resolve();root=report_path.parent
    report=json.loads(report_path.read_text())
    expected_inputs,jobs=trusted_contract(source_root,source_commit)
    if report.get('schema')!='chirps.formal-raw-collection/v1':raise ValueError('unknown formal report schema')
    if report.get('source')!={'source_commit':source_commit,'inputs':expected_inputs}:raise ValueError('formal report not bound to exact candidate')
    if report.get('checker_image')!=IMAGE or report.get('status')!='collected-unverified':raise ValueError('formal collection failed or wrong checker')
    selected=report.get('planned_job_ids',[])
    actual=[entry.get('id') for entry in report.get('jobs',[])]
    if len(set(actual))!=len(actual) or actual!=selected or not set(actual)<=set(jobs):raise ValueError('formal jobs missing, duplicate or unexpected')
    if require_complete and (report.get('mode')!='all' or set(actual)!=set(jobs)):raise ValueError('full 160-job model evidence required')
    if not actual:raise ValueError('empty formal evidence')
    results=[verify_job(root,entry,jobs[entry['id']]) for entry in report['jobs']]
    return {'status':'pass' if require_complete else 'development-verified','source_commit':source_commit,'jobs':results,'complete':require_complete}

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root',required=True,type=Path);parser.add_argument('--source-commit',required=True)
    parser.add_argument('--report',required=True,type=Path);parser.add_argument('--development-subset',action='store_true')
    args=parser.parse_args()
    print(json.dumps(verify_formal_report(args.source_root,args.report,args.source_commit,not args.development_subset),indent=2))
