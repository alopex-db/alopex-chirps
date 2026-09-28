#!/usr/bin/env python3
"""Collect or independently verify all eleven catalog completeness observations."""
import argparse
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import time
import uuid
import yaml

SPEC=importlib.util.spec_from_file_location('formal_evidence',Path(__file__).with_name('v07_formal_evidence.py'))
e=importlib.util.module_from_spec(SPEC);sys.modules[SPEC.name]=e;SPEC.loader.exec_module(e)
PROBES={
    'missing-tool':(2,'checker tool missing: /opt/apalache/bin/absent-apalache-mc'),
    'missing-model':(9,'model registry mismatch'),
    'missing-action':(255,'chirps-durable/SendLease.tla: AllActions count mismatch'),
    'missing-action-catalog':(255,'chirps-durable/lifecycle-catalog.yaml: meta property gate not executed: chirps-v0-7-lifecycle-state|AllConcurrentCapabilitiesModeled (all-actions-catalog-bijection)'),
    'missing-scenario-matrix':(255,'chirps-durable/catalog.yaml: meta property gate not executed: chirps-v0-7-send-lease|AllRequiredScenarioMatricesCatalogued (scenario-matrix-signature)'),
    'missing-red-profile':(255,'chirps-durable/catalog.yaml: requirement V7-SEND-003 references unknown RED profile duplicate-append'),
    'missing-refinement':(255,'chirps-durable/catalog.yaml: empty refinement for V7-ARCH-005'),
    'missing-requirement':(255,'chirps-durable/catalog.yaml: exact requirement set mismatch'),
    'bad-property':(255,'chirps-durable/catalog.yaml: unknown meta property TypoProperty for V7-ARCH-005'),
    'bad-repository':(255,'chirps-durable/catalog.yaml: unknown repository fictional for V7-ARCH-005'),
}

def expected_logs(service):
    env=service['environment'];rows=[line.split('|') for line in env['CHIRPS_DURABLE_MODELS'].splitlines()]
    count=sum(len(row[11].split(',')) for row in rows)
    header=f'catalog PASS models={len(rows)} profiles={sum(int(r[5]) for r in rows)} witnesses={sum(int(r[6]) for r in rows)} requirement_mappings={count} requirements=V7-MODEL-001,V7-MODEL-002,V7-MODEL-003,V7-MODEL-004,V7-MODEL-005,V7-MODEL-006'
    lines=[header]+[f'input {r[0]} tla={r[7]} cfg={r[8]} catalog={r[9]} bound={r[4]} actions={r[10]}' for r in rows]
    return {'none':(0,'\n'.join(lines)+'\n'),**{key:(code,message+'\n') for key,(code,message) in PROBES.items()}}

def verify_catalog_report(source_root,report_path,source_commit):
    report_path=Path(report_path).resolve();report=json.loads(report_path.read_text())
    inputs,_=e.trusted_contract(source_root,source_commit)
    raw=subprocess.check_output(['git','-C',str(source_root),'show',source_commit+':formal/chirps-durable/compose.yml'],timeout=30)
    service=yaml.safe_load(raw)['services']['suite'];command=service['command'][0].replace('$$','$')
    if report.get('schema')!='chirps.formal-catalog-collection/v1':raise ValueError('unknown catalog collection schema')
    if report.get('source')!={'source_commit':source_commit,'inputs':inputs}:raise ValueError('catalog candidate binding differs')
    if report.get('image')!=e.IMAGE or report.get('command_sha256')!=e.sha(command.encode()):raise ValueError('catalog command/image differs')
    if report.get('status')!='collected-unverified':raise ValueError('catalog collection failed')
    expected=expected_logs(service);results=report.get('results',[])
    if [r.get('probe') for r in results]!=list(expected):raise ValueError('catalog probe inventory differs')
    for result in results:
        code,log=expected[result['probe']]
        if result.get('failure') is not None or result.get('exit_code')!=code:raise ValueError('catalog probe unexpected exit')
        actual=e.artifact(report_path.parent,result['log'],result['sha256']).decode()
        if actual!=log:raise ValueError('catalog probe failed for wrong reason')
    return {'status':'pass','source_commit':source_commit,'probes':len(results)}

def collect(snapshot,output):
    snapshot=Path(snapshot).resolve();output=Path(output).resolve()
    identity=json.loads((snapshot/'source.json').read_text())
    for name,digest in identity['inputs'].items():e.artifact(snapshot,name,digest)
    service=yaml.safe_load((snapshot/'formal/chirps-durable/compose.yml').read_text())['services']['suite']
    if service['image']!=e.IMAGE:raise ValueError('checker image differs')
    command=service['command'][0].replace('$$','$');expected=expected_logs(service)
    inspection=json.loads(subprocess.check_output(['podman','image','inspect',e.IMAGE],text=True))[0]
    if inspection['Digest']!=e.IMAGE.split('@')[1]:raise ValueError('local image digest differs')
    output.mkdir();report=dict(schema='chirps.formal-catalog-collection/v1',source=identity,image=e.IMAGE,
        command_sha256=e.sha(command.encode()),collector_sha256=e.sha(Path(__file__).read_bytes()),status='running',results=[])
    def save():(output/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    save()
    for probe,(expected_code,expected_log) in expected.items():
        name='chirps-formal-catalog-'+uuid.uuid4().hex[:16]
        argv=['podman','run','--rm','--name',name,'--network=none','--cpus=0.2','--memory=256m','--memory-swap=256m','--entrypoint','/bin/sh','-v',str(snapshot/'formal')+':/var/apalache:ro']
        environment={**service['environment'],'CHIRPS_DURABLE_MODE':'catalog','CHIRPS_DURABLE_PROBE':probe}
        for key,value in environment.items():argv.extend(['-e',key+'='+str(value)])
        argv.extend([e.IMAGE,'-eu','-c',command]);start=time.monotonic();failure=None
        with (output/(probe+'.log')).open('xb') as log:
            process=subprocess.Popen(argv,stdout=log,stderr=subprocess.STDOUT)
            try:process.wait(timeout=30)
            except subprocess.TimeoutExpired:failure='catalog probe timeout'
            finally:
                for action in (['stop','--time','2'],['rm','--force']):
                    try:subprocess.run(['podman',*action,name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=15)
                    except subprocess.TimeoutExpired:failure='catalog cleanup timeout'
                try:process.wait(timeout=10)
                except subprocess.TimeoutExpired:process.kill();process.wait(timeout=5);failure='catalog launcher cleanup timeout'
                if subprocess.run(['podman','container','exists',name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=10).returncode!=1:failure='catalog owned container remains'
        raw=(output/(probe+'.log')).read_bytes()
        if failure is None and (process.returncode!=expected_code or raw.decode()!=expected_log):failure='catalog probe unexpected result'
        report['results'].append(dict(probe=probe,exit_code=process.returncode,elapsed_seconds=round(time.monotonic()-start,3),log=probe+'.log',sha256=e.sha(raw),failure=failure))
        save()
        if failure:break
    report['status']='collected-unverified' if len(report['results'])==11 and not any(r['failure'] for r in report['results']) else 'fail';save()
    return report

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);sub=parser.add_subparsers(dest='mode',required=True)
    run=sub.add_parser('collect');run.add_argument('snapshot',type=Path);run.add_argument('output',type=Path)
    verify=sub.add_parser('verify');verify.add_argument('--source-root',required=True,type=Path);verify.add_argument('--source-commit',required=True);verify.add_argument('--report',required=True,type=Path)
    args=parser.parse_args()
    result=collect(args.snapshot,args.output) if args.mode=='collect' else verify_catalog_report(args.source_root,args.report,args.source_commit)
    print(json.dumps(result,indent=2));raise SystemExit(1 if result['status']=='fail' else 0)
