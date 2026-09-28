#!/usr/bin/env python3
"""Small local Git fixtures validate reconstruction without network or builds."""
import base64
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
SPEC=importlib.util.spec_from_file_location('prepare',Path(__file__).with_name('prepare-v07-iggy-source.py'))
p=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(p)

class Preparation(unittest.TestCase):
    def git(self,root,*args):return subprocess.check_output(['git','-C',str(root),*args],stderr=subprocess.PIPE).decode().strip()
    def init(self,path):
        path.mkdir();self.git(path,'init','--quiet');self.git(path,'config','user.email','fixture@example.invalid');self.git(path,'config','user.name','Synthetic fixture')
    def commit(self,root):
        self.git(root,'add','.');self.git(root,'commit','--quiet','-m','synthetic fixture');return self.git(root,'rev-parse','HEAD')
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name).resolve()
        self.iggy=self.root/'iggy';self.init(self.iggy)
        (self.iggy/'Cargo.lock').write_text('synthetic fixture lock\n');(self.iggy/'rust-toolchain.toml').write_text('[toolchain]\nchannel="fixture"\n')
        for i in range(4):
            (self.iggy/'history.txt').write_text(str(i));self.baseline=self.commit(self.iggy)
        (self.iggy/'feature.txt').write_text('synthetic candidate\n');self.candidate=self.commit(self.iggy)
        self.git(self.iggy,'update-ref',p.BUNDLE_REF,self.candidate)
        bundle=self.root/'fixture.bundle';self.git(self.iggy,'bundle','create',str(bundle),self.baseline+'..'+p.BUNDLE_REF);raw=bundle.read_bytes()
        diff=subprocess.check_output(['git','-C',str(self.iggy),'-c','core.abbrev=40','-c','diff.renames=false','diff','--binary','--full-index','--no-ext-diff','--no-textconv','--no-renames',self.baseline,self.candidate,'--'])
        self.series=dict(repository=p.UPSTREAM,baseline_commit=self.baseline,commit=self.candidate,parent_commit=self.baseline,
            tree=self.git(self.iggy,'rev-parse','HEAD^{tree}'),commit_count=1,bundle_format='git-bundle-v2',bundle_encoding='base64',bundle_ref=p.BUNDLE_REF,bundle_sha256=p.sha(raw),complete_diff_sha256=p.sha(diff),
            cargo_lock_sha256=p.sha((self.iggy/'Cargo.lock').read_bytes()),toolchain_manifest_sha256=p.sha((self.iggy/'rust-toolchain.toml').read_bytes()),production_manifest=p.MANIFEST,test_manifest=p.TEST_MANIFEST,bundle_base64=base64.b64encode(raw).decode())
        self.chirps=self.root/'chirps';self.init(self.chirps)
        common='[source]\n'+''.join(f'{key} = {json.dumps(self.series[key])}\n' for key in ('repository','baseline_commit','commit','tree','cargo_lock_sha256'))+'[toolchain]\nmanifest_sha256 = '+json.dumps(self.series['toolchain_manifest_sha256'])+'\n'
        production=common+'[patch_series]\npath = '+json.dumps(p.SERIES)+'\n'+''.join(f'{key} = {json.dumps(self.series[key])}\n' for key in ('bundle_sha256','complete_diff_sha256'))
        for name,text in ((p.MANIFEST,production),(p.TEST_MANIFEST,common)):
            path=self.chirps/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text(text)
        self.series['production_manifest_sha256']=p.sha(production.encode());self.series['test_manifest_sha256']=p.sha(common.encode())
        self.write_series();self.source=self.commit(self.chirps)
    def write_series(self):
        path=self.chirps/p.SERIES;path.parent.mkdir(parents=True,exist_ok=True)
        path.write_text('schema_version = 1\n[series]\n'+''.join(f'{key} = {json.dumps(value)}\n' for key,value in self.series.items()))
    def test_local_shallow_reconstruction_preserves_existing_refs(self):
        refs=self.git(self.iggy,'show-ref');output=self.root/'verified.git'
        result=p.prepare(self.chirps,self.source,self.candidate,output,self.iggy)
        self.assertEqual(result['iggy_commit'],self.candidate)
        self.assertEqual(self.git(output,'rev-list','--all','--count'),'2')
        self.assertEqual(self.git(self.iggy,'show-ref'),refs)
        self.assertEqual(self.git(self.iggy,'status','--porcelain'),'')
        self.assertFalse((output/'candidate.bundle').exists())
    def test_existing_output_is_never_reused(self):
        output=self.root/'existing';output.mkdir();(output/'keep').write_text('untouched')
        with self.assertRaises(FileExistsError):p.prepare(self.chirps,self.source,self.candidate,output,self.iggy)
        self.assertEqual((output/'keep').read_text(),'untouched')
    def test_bundle_digest_corruption_rejected_before_output(self):
        self.series['bundle_base64']=base64.b64encode(b'corrupt fixture').decode();self.write_series();commit=self.commit(self.chirps)
        output=self.root/'absent'
        with self.assertRaisesRegex(ValueError,'bundle digest'):p.prepare(self.chirps,commit,self.candidate,output,self.iggy)
        self.assertFalse(output.exists())
    def test_candidate_mismatch_rejected(self):
        with self.assertRaisesRegex(ValueError,'Iggy candidate'):p.source_contract(self.chirps,self.source,'1'*40)
    def test_wrong_upstream_rejected(self):
        self.series['repository']='https://example.invalid/foreign';self.write_series();commit=self.commit(self.chirps)
        with self.assertRaisesRegex(ValueError,'upstream'):p.source_contract(self.chirps,commit,self.candidate)
    def test_current_real_source_contract(self):
        root=Path(__file__).resolve().parents[2]
        # Read the checked-in source pins; no network or source build is used.
        import tomllib
        source=self.git(root,'rev-parse','HEAD')
        manifest=tomllib.loads(subprocess.check_output(['git','-C',str(root),'show',source+':'+p.MANIFEST]).decode())
        series,bundle=p.source_contract(root,source,manifest['source']['commit'])
        self.assertEqual(p.sha(bundle),series['bundle_sha256'])

if __name__=='__main__':unittest.main()
