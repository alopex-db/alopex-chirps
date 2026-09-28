#!/usr/bin/env python3
"""Collect bounded pinned Apalache runs from an exported immutable formal snapshot.

This collector preserves raw outputs; it does not certify a release. Inputs must
be exported from an immutable Git commit with export-v07-formal.py. Subsets are
always labeled development-subset, including successful runs.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time
import uuid
import yaml

IMAGE = 'ghcr.io/apalache-mc/apalache@sha256:fde994fd109323934b9abb7ad169de37b29acf2141483367f2913cae30ff3795'
MAX_BYTES = 1024 ** 3

def sha(raw):
    return hashlib.sha256(raw).hexdigest()

def save(path, value):
    temporary=path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True)+'\n')
    temporary.replace(path)

def usage(root):
    return sum(path.stat().st_size for path in root.rglob('*') if path.is_file())

def jobs(snapshot):
    snapshot=snapshot.resolve()
    identity = json.loads((snapshot/'source.json').read_text())
    if not re.fullmatch('[0-9a-f]{40}', identity['source_commit']):
        raise ValueError('immutable source commit required')
    if len(identity['inputs']) != 14:
        raise ValueError('expected 14 source inputs')
    for name, digest in identity['inputs'].items():
        path=Path(name)
        if path.is_absolute() or '..' in path.parts or str(path)!=name:
            raise ValueError('unsafe source input path')
        if any((snapshot.joinpath(*path.parts[:index])).is_symlink() for index in range(1,len(path.parts)+1)):
            raise ValueError('symlink source input')
        if sha((snapshot/name).read_bytes()) != digest:
            raise ValueError('source input changed: '+name)
    compose = yaml.safe_load((snapshot/'formal/chirps-durable/compose.yml').read_text())
    environment = compose['services']['suite']['environment']
    if environment['CHIRPS_DURABLE_CHECKER_IMAGE'] != IMAGE:
        raise ValueError('checker differs')
    result=[]
    expected_inputs={'formal/compose.yml','formal/chirps-durable/compose.yml'}
    for row in environment['CHIRPS_DURABLE_MODELS'].splitlines():
        mid, catalog, tla, cfg, bound, nred, nwit, htla, hcfg, hcat, *_ = row.split('|')
        for path, digest in ((tla,htla),(cfg,hcfg),(catalog,hcat)):
            expected_inputs.add('formal/'+path)
            if 'formal/'+path not in identity['inputs']:
                raise ValueError('registry input not in snapshot')
            if sha((snapshot/'formal'/path).read_bytes()) != digest:
                raise ValueError('suite registry digest mismatch')
        checker=yaml.safe_load((snapshot/'formal'/catalog).read_text())['model']['checker']
        base=(snapshot/'formal'/cfg).read_text()
        if len(checker['mutation_profiles']) != int(nred) or len(checker['witness_profiles']) != int(nwit):
            raise ValueError('catalog counts differ')
        result.append(dict(id=mid+':typecheck',kind='typecheck',tla=tla,expected=0))
        result.append(dict(id=mid+':normal',kind='normal',tla=tla,expected=0,bound=int(bound),config=base))
        for kind,key in (('profile','mutation_profiles'),('witness','witness_profiles')):
            for entry in checker[key]:
                constants=re.findall(r'^CONSTANT .*$',base,re.M)
                init=re.search(r'^INIT (\w+)',base,re.M)[1]
                nxt=re.search(r'^NEXT (\w+)',base,re.M)[1]
                if kind=='profile':
                    constants=[f'CONSTANT UnsafeMode = "{entry["constant_overrides"]["UnsafeMode"]}"' if line.startswith('CONSTANT UnsafeMode = ') else line for line in constants]
                    nxt=checker['execution']['profile_materialization'].get('next_replacement',{}).get('to',nxt)
                else:
                    init=entry.get('init',init)
                    nxt=entry['next']
                if kind=='profile' and entry['expected']!='counterexample' or kind=='witness' and entry['expected']!='reachable':
                    raise ValueError('unexpected catalog result semantics')
                config='\n'.join(constants)+f'\n\nINIT {init}\nNEXT {nxt}\n\nINVARIANT TypeOK\nINVARIANT {entry["target"]}\n'
                result.append(dict(id=mid+':'+entry['id'],kind=kind,tla=tla,expected=12,
                                   bound=entry['max_steps'],target=entry['target'],config=config))
    if expected_inputs != set(identity['inputs']):
        raise ValueError('source inventory differs from registry')
    order={'typecheck':0,'normal':1,'profile':2,'witness':3}
    result.sort(key=lambda job:order[job['kind']])
    if len(result)!=160 or len({job['id'] for job in result})!=160:
        raise ValueError('expected unique complete 160-job suite')
    if {kind:sum(j['kind']==kind for j in result) for kind in order} != dict(typecheck=4,normal=4,profile=125,witness=27):
        raise ValueError('suite phase counts differ')
    return identity, result

def cleanup_owned_container(name,process):
    """Remove only this invocation's container, including failed/timeout launchers."""
    for action in (['stop','--time','2'], ['rm','--force']):
        try:
            subprocess.run(['podman',*action,name],stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL,timeout=15,check=False)
        except subprocess.TimeoutExpired:
            continue
    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
    # Fail closed when the runtime cannot establish container cleanup.
    inspection=subprocess.run(['podman','container','exists',name],stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL,timeout=10,check=False)
    if inspection.returncode!=1:
        raise RuntimeError('could not establish owned-container removal: '+name)

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('snapshot',type=Path)
    parser.add_argument('output',type=Path)
    parser.add_argument('--smoke',action='store_true')
    parser.add_argument('--timeout',type=int,default=600)
    parser.add_argument('--workers',type=int,choices=(1,2),default=1)
    parser.add_argument('--job',action='append',default=[])
    parser.add_argument('--kind',choices=('typecheck','normal','profile','witness'))
    args=parser.parse_args()
    snapshot=args.snapshot.resolve();output=args.output.resolve()
    identity, planned=jobs(snapshot)
    if args.timeout <= 0 or args.timeout > 2700:
        parser.error('timeout must be 1..2700 seconds')
    if sum((args.smoke, bool(args.job), bool(args.kind))) > 1:
        parser.error('select only one of smoke/job/kind')
    if args.job:
        selected=set(args.job)
        if not selected <= {j['id'] for j in planned}:
            parser.error('unknown job id')
        planned=[j for j in planned if j['id'] in selected]
    if args.kind:
        planned=[j for j in planned if j['kind']==args.kind]
    if args.smoke:
        planned=[next(j for j in planned if j['kind']==kind) for kind in ('typecheck','normal')]
    output.mkdir()
    inspection=json.loads(subprocess.check_output(['podman','image','inspect',IMAGE],text=True))[0]
    if inspection['Digest'] != IMAGE.split('@')[1]:
        raise ValueError('local image digest differs')
    report=dict(schema='chirps.formal-raw-collection/v1',source=identity,checker_image=IMAGE,
                image_id=inspection['Id'],mode='development-subset' if len(planned)!=160 else 'all',
                collector_sha256=sha(Path(__file__).read_bytes()),
                resources=dict(workers=args.workers,cpu_per_worker=1,memory_bytes_per_worker=2*1024**3,
                               output_budget_bytes=MAX_BYTES,timeout_seconds=args.timeout),
                planned_job_ids=[j['id'] for j in planned],status='running',jobs=[])
    save(output/'report.json',report)
    budget_exceeded=threading.Event()
    cancelled=threading.Event()
    def run_job(index,job):
        if budget_exceeded.is_set() or cancelled.is_set():
            return dict(id=job['id'],kind=job['kind'],failure='collection cancelled before start',exit_code=None,artifacts={})
        work=output/f'{index:03d}-{job["kind"]}'
        work.mkdir()
        command=['--out-dir=/out/checker','typecheck','/inputs/'+job['tla']]
        if 'config' in job:
            (work/'model.cfg').write_text(job['config'])
            command=['--out-dir=/out/checker','check','--config=/out/model.cfg',f'--length={job["bound"]}','/inputs/'+job['tla']]
        name='chirps-formal-'+uuid.uuid4().hex[:16]
        argv=['podman','run','--rm','--name',name,'--network=none','--cpus=1','--memory=2g',
              '--memory-swap=2g','--entrypoint','/opt/apalache/bin/apalache-mc',
              '-e','JAVA_TOOL_OPTIONS=-Xmx1400m -XX:ErrorFile=/out/hs_err_pid%p.log -XX:ReplayDataFile=/out/replay_pid%p.log',
              '-w','/out','-v',str(snapshot/'formal')+':/inputs:ro',
              '-v',str(work)+':/out:rw',IMAGE,*command]
        start=time.monotonic();failure=None
        with (work/'console.log').open('xb') as log:
            process=subprocess.Popen(argv,stdout=log,stderr=subprocess.STDOUT)
            try:
                while process.poll() is None:
                    if cancelled.is_set():
                        raise RuntimeError('collection interrupted')
                    if time.monotonic()-start > args.timeout:
                        raise RuntimeError('job timeout')
                    if budget_exceeded.is_set() or usage(output)>MAX_BYTES:
                        budget_exceeded.set()
                        raise RuntimeError('1GiB output budget exceeded')
                    time.sleep(0.5)
            except Exception as exc:
                failure=str(exc)
            finally:
                try:
                    cleanup_owned_container(name,process)
                except Exception as exc:
                    failure=(failure+'; ' if failure else '')+str(exc)
        artifacts={str(path.relative_to(output)):sha(path.read_bytes()) for path in work.rglob('*') if path.is_file()}
        entry={key:value for key,value in job.items() if key!='config'}
        entry.update(command=command,exit_code=process.returncode,elapsed_seconds=round(time.monotonic()-start,3),
                     artifacts=artifacts,failure=failure)
        return entry
    for phase in ('typecheck','normal','profile','witness'):
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures={pool.submit(run_job,index,job):job for index,job in enumerate(planned) if job['kind']==phase}
            try:
                for future in as_completed(futures):
                    try:
                        entry=future.result()
                    except Exception as exc:
                        entry=dict(id=futures[future]['id'],kind=phase,exit_code=None,artifacts={},failure=str(exc))
                    report['jobs'].append(entry)
                    save(output/'report.json',report)
                    print(entry['id'],entry['exit_code'],entry.get('elapsed_seconds'),entry['failure'],flush=True)
            except KeyboardInterrupt:
                cancelled.set()
                report['status']='interrupted'
                save(output/'report.json',report)
                raise
        if budget_exceeded.is_set():
            break
    report['jobs'].sort(key=lambda entry:report['planned_job_ids'].index(entry['id']))
    report['status']='fail' if len(report['jobs'])!=len(planned) or any(j['failure'] or j['exit_code']!=j.get('expected') for j in report['jobs']) else 'collected-unverified'
    report['output_bytes']=usage(output)
    save(output/'report.json',report)
    return 0 if report['status']=='collected-unverified' else 1

if __name__=='__main__':
    raise SystemExit(main())
