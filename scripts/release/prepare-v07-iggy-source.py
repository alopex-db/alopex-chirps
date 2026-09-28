#!/usr/bin/env python3
"""Prepare immutable foreign Git objects for release verification, without builds.

Creates a new bare repository from one shallow Apache baseline plus the candidate
bundle. Existing repositories and refs are read-only. The optional local baseline
repository is for offline operators/tests; no history is copied beyond depth 1.
"""
import argparse
import base64
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tomllib

UPSTREAM='https://github.com/apache/iggy.git'
MANIFEST='server/iggy-compatible/manifest.toml'
TEST_MANIFEST='server/iggy-compatible/test-manifest.toml'
SERIES='server/iggy-compatible/patches/series.toml'
BUNDLE_REF='refs/heads/chirps/v0.7.0-compatible'
MAX_INPUT=16*1024**2

def sha(raw):return hashlib.sha256(raw).hexdigest()

def git(root,*args):
    return subprocess.check_output(['git','-C',str(root),'-c','core.hooksPath=/dev/null',*args],stderr=subprocess.PIPE,timeout=300)

def require(actual,expected,label):
    if actual!=expected:raise ValueError(label+' differs')

def source_contract(root,commit,iggy_commit):
    for value in (commit,iggy_commit):
        if not isinstance(value,str) or not re.fullmatch('[0-9a-f]{40}',value):raise ValueError('full immutable commits required')
    require(git(root,'cat-file','-t',commit).decode().strip(),'commit','Chirps Git object type')
    def read(path):
        size=int(git(root,'cat-file','-s',commit+':'+path))
        if size>MAX_INPUT:raise ValueError('source manifest size limit exceeded')
        return git(root,'show',commit+':'+path)
    production_raw=read(MANIFEST);test_raw=read(TEST_MANIFEST)
    production=tomllib.loads(production_raw.decode());test=tomllib.loads(test_raw.decode())
    series_doc=tomllib.loads(read(SERIES).decode());require(series_doc.get('schema_version'),1,'series schema')
    series=series_doc['series']
    require(series['repository'],UPSTREAM,'upstream')
    require(series['commit'],iggy_commit,'Iggy candidate')
    for manifest in (production,test):
        for key in ('repository','baseline_commit','commit','tree','cargo_lock_sha256'):
            require(manifest['source'][key],series[key],'manifest source '+key)
        require(manifest['toolchain']['manifest_sha256'],series['toolchain_manifest_sha256'],'toolchain digest')
    for key,value in [('production_manifest',MANIFEST),('test_manifest',TEST_MANIFEST),('bundle_format','git-bundle-v2'),('bundle_encoding','base64'),('bundle_ref',BUNDLE_REF)]:require(series[key],value,key)
    require(series['production_manifest_sha256'],sha(production_raw),'production manifest digest')
    require(series['test_manifest_sha256'],sha(test_raw),'test manifest digest')
    require(production['patch_series'],{'path':SERIES,'bundle_sha256':series['bundle_sha256'],'complete_diff_sha256':series['complete_diff_sha256']},'patch series binding')
    if 'patch_series' in test:raise ValueError('test manifest must consume production series binding')
    for key in ('baseline_commit','parent_commit','tree'):
        if not re.fullmatch('[0-9a-f]{40}',series[key]):raise ValueError('invalid '+key)
    if type(series['commit_count']) is not int or series['commit_count']<1:raise ValueError('invalid patch commit count')
    bundle=base64.b64decode(''.join(series['bundle_base64'].split()),validate=True)
    require(sha(bundle),series['bundle_sha256'],'decoded bundle digest')
    header=bundle.split(b'\n\n',1)[0].decode().splitlines()
    if len(header)!=3 or header[0]!='# v2 git bundle' or not header[1].startswith('-'+series['baseline_commit']+' ') or header[2]!=iggy_commit+' '+BUNDLE_REF:
        raise ValueError('bundle prerequisite/ref header differs')
    return series,bundle

def prepare(source_root,source_commit,iggy_commit,output,baseline_repository=None):
    series,bundle=source_contract(source_root,source_commit,iggy_commit)
    output=Path(output).resolve();output.mkdir()
    subprocess.run(['git','init','--bare','--quiet',str(output)],check=True,timeout=30)
    bundle_path=output/'candidate.bundle';bundle_path.write_bytes(bundle)
    upstream=UPSTREAM if baseline_repository is None else Path(baseline_repository).resolve().as_uri()
    baseline=series['baseline_commit']
    git(output,'-c','protocol.file.allow=always','fetch','--quiet','--depth=1','--no-tags','--no-write-fetch-head',upstream,baseline+':refs/heads/baseline')
    require(git(output,'rev-parse','refs/heads/baseline^{commit}').decode().strip(),baseline,'baseline seed')
    git(output,'bundle','verify',str(bundle_path))
    git(output,'fetch','--quiet','--no-tags','--no-write-fetch-head',str(bundle_path),BUNDLE_REF+':refs/heads/compatible')
    require(git(output,'rev-parse','refs/heads/compatible^{commit}').decode().strip(),iggy_commit,'reconstructed commit')
    require(git(output,'rev-parse',iggy_commit+'^{tree}').decode().strip(),series['tree'],'reconstructed tree')
    require(git(output,'show','-s','--format=%P',iggy_commit).decode().strip(),series['parent_commit'],'reconstructed parent')
    require(int(git(output,'rev-list','--count',baseline+'..'+iggy_commit)),series['commit_count'],'patch commit count')
    require(sha(git(output,'show',iggy_commit+':Cargo.lock')),series['cargo_lock_sha256'],'Cargo.lock digest')
    require(sha(git(output,'show',iggy_commit+':rust-toolchain.toml')),series['toolchain_manifest_sha256'],'toolchain file digest')
    diff=git(output,'-c','core.abbrev=40','-c','diff.renames=false','diff','--binary','--full-index','--no-ext-diff','--no-textconv','--no-renames',baseline,iggy_commit,'--')
    require(sha(diff),series['complete_diff_sha256'],'complete patch digest')
    git(output,'symbolic-ref','HEAD','refs/heads/compatible')
    git(output,'fsck','--strict','--no-dangling')
    bundle_path.unlink()
    return {'schema':'chirps.iggy-verification-source/v1','source_commit':source_commit,'iggy_commit':iggy_commit,
            'iggy_tree':series['tree'],'baseline_commit':baseline,'bundle_sha256':series['bundle_sha256']}

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root',required=True,type=Path);parser.add_argument('--source-commit',required=True)
    parser.add_argument('--iggy-commit',required=True);parser.add_argument('--output',required=True,type=Path)
    parser.add_argument('--baseline-repository',type=Path)
    args=parser.parse_args()
    print(json.dumps(prepare(args.source_root,args.source_commit,args.iggy_commit,args.output,args.baseline_repository),indent=2))
