#!/usr/bin/env python3
"""Synthetic identity fixtures only; never complete E2E or PERF evidence."""
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import v07_environment_evidence as env
from v07_e2e_evidence import TARGETS, reference


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True) + '\n')
    return path


def axes_fixture():
    return dict(host_fingerprint='synthetic-host', server_image_digest='1'*64,
        server_source_digest='2'*64, server_config_digest='3'*64,
        payload_digest='4'*64, payload_bytes=32, partition_set_digest='5'*64,
        full_confirmation_profile='broker_accepted', offered_load_per_second=10,
        warmup_millis=1000, measure_millis=1000, drain_millis=1000,
        client_placement='same-host', execution_class='loopback', host_count=1,
        broker_count=1, replication_factor=1)


def write_environment_fixture(directory, source_commit, iggy_commit, axes, observed):
    """Reusable manifest fixture with no candidate/result digest cycle."""
    environment = write(directory/'e2e-environment.json', observed)
    return write(directory/'manifest.json', dict(schema=env.SCHEMA,
        source_commit=source_commit, iggy_commit=iggy_commit,
        e2e_environment=reference(environment, directory), performance_axes=axes))


def write_perf_identity_fixture(root, candidate_path):
    """Only identity fields, deliberately insufficient for Rust semantic replay."""
    candidate = json.loads(candidate_path.read_bytes())
    axes = candidate['performance']['axes']
    identity = dict(candidate_sha256=hashlib.sha256(candidate_path.read_bytes()).hexdigest(), axes=axes)
    observation = dict(axes=axes, observed_axes_begin=axes, observed_axes_finish=axes)
    paths = {}
    for name, extra in {
        'aa/aa': dict(left=[observation],right=[observation]),
        'aa/bounds': {},
        'safety/safety': dict(controls=[dict(control='synthetic-fixture',observation=observation)]),
        'safety/freeze': {},
        'paired/paired': dict(direct=[observation],full=[observation]),
    }.items():
        kind = name.split('/')[-1]
        paths[name] = write(root/(name+'.json'), dict(identity, schema=f'chirps.durable-perf-{kind}/v1', **extra))
    return paths


class EnvironmentTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source, self.iggy = 'a'*40, 'b'*40
        self.axes = axes_fixture()
        self.observed = dict(system='Linux', release='synthetic', machine='aarch64', node='fixture', rustc='rustc fixture', cargo='cargo fixture')
        self.manifest = write_environment_fixture(self.root/'environment',self.source,self.iggy,self.axes,self.observed)
        self.candidate = write(self.root/'candidate.json',dict(source_commit=self.source,iggy_commit=self.iggy,
            environment_sha256=hashlib.sha256(self.manifest.read_bytes()).hexdigest(),performance=dict(axes=self.axes)))
        self.lanes = {}
        self.reports = {}
        for lane, targets in TARGETS.items():
            directory = self.root/lane
            reports = {}
            for target in targets:
                target_dir = directory/target
                observed = write(target_dir/'environment.json',self.observed)
                report = write(target_dir/'report.json',dict(source=dict(source_commit=self.source),
                    server=dict(source_commit=self.iggy),environment=reference(observed,target_dir)))
                self.reports[(lane,target)] = report
                reports[target] = reference(report,directory)
            self.lanes[lane] = write(directory/'lane.json',dict(lane=lane,targets=reports))
        self.perf = write_perf_identity_fixture(self.root/'performance',self.candidate)

    def verify(self):
        env.verify_environment(self.candidate,self.manifest,self.lanes['production'],self.lanes['fault'],self.perf['paired/paired'])

    def change(self,path,mutate):
        value=json.loads(path.read_bytes())
        mutate(value)
        write(path,value)

    def rebind_candidate(self):
        self.change(self.candidate,lambda value:value.update(environment_sha256=hashlib.sha256(self.manifest.read_bytes()).hexdigest()))
        self.perf = write_perf_identity_fixture(self.root/'performance',self.candidate)

    def rebind_lane(self,lane,target):
        self.change(self.lanes[lane],lambda value:value['targets'].update({target:reference(self.reports[(lane,target)],self.lanes[lane].parent)}))

    def test_valid_bindings_do_not_constrain_api_or_wire_hosts(self):
        write(self.root/'api-unrelated.json',dict(system='Windows',source_commit=self.source))
        write(self.root/'wire-unrelated.json',dict(system='Darwin',source_commit=self.source))
        self.verify()

    def test_candidate_digest_must_bind_actual_manifest(self):
        self.change(self.manifest,lambda value:value['performance_axes'].update(host_fingerprint='another-host'))
        with self.assertRaisesRegex(ValueError,'digest differs'):
            self.verify()

    def test_rehashed_manifest_cannot_claim_different_source(self):
        for field in ('source_commit','iggy_commit'):
            original=self.manifest.read_bytes()
            self.change(self.manifest,lambda value:value.update({field:'c'*40}))
            self.rebind_candidate()
            with self.assertRaisesRegex(ValueError,'source differs'):
                self.verify()
            self.manifest.write_bytes(original)
            self.rebind_candidate()

    def test_rehashed_target_cannot_substitute_candidate_source(self):
        lane,target='production',TARGETS['production'][-1]
        self.change(self.reports[(lane,target)],lambda value:value['source'].update(source_commit='c'*40))
        self.rebind_lane(lane,target)
        with self.assertRaisesRegex(ValueError,'another source'):
            self.verify()

    def test_rehashed_observation_cannot_substitute_actual_environment(self):
        lane,target='fault',TARGETS['fault'][-1]
        report=self.reports[(lane,target)]
        observation=report.parent/'environment.json'
        self.change(observation,lambda value:value.update(node='other-host'))
        self.change(report,lambda value:value.update(environment=reference(observation,report.parent)))
        self.rebind_lane(lane,target)
        with self.assertRaisesRegex(ValueError,'actual environment differs'):
            self.verify()

    def test_frozen_environment_reference_is_verified(self):
        self.change(self.root/'environment/e2e-environment.json',lambda value:value.update(rustc='different compiler'))
        with self.assertRaises(ValueError):
            self.verify()

    def test_perf_candidate_cannot_be_relabelled(self):
        self.change(self.perf['aa/aa'],lambda value:value.update(candidate_sha256='c'*64))
        with self.assertRaisesRegex(ValueError,'another candidate'):
            self.verify()

    def test_changed_plan_requires_new_environment_manifest(self):
        self.change(self.candidate,lambda value:value['performance']['axes'].update(host_fingerprint='new-host'))
        self.perf=write_perf_identity_fixture(self.root/'performance',self.candidate)
        with self.assertRaisesRegex(ValueError,'candidate plan'):
            self.verify()

    def test_each_actual_perf_observation_must_match_frozen_axes(self):
        for path,key in [(self.perf['aa/aa'],'left'),(self.perf['aa/aa'],'right'),(self.perf['paired/paired'],'direct'),(self.perf['paired/paired'],'full'),(self.perf['safety/safety'],'controls')]:
            for field in ('axes','observed_axes_begin','observed_axes_finish'):
                original=path.read_bytes()
                def mutate(value):
                    observation=value[key][0]
                    if key=='controls': observation=observation['observation']
                    observation[field]['host_fingerprint']='different actual host'
                self.change(path,mutate)
                with self.assertRaisesRegex(ValueError,'actual environment differs'):
                    self.verify()
                path.write_bytes(original)

    def test_missing_targets_reject(self):
        self.change(self.lanes['fault'],lambda value:value['targets'].pop(TARGETS['fault'][-1]))
        with self.assertRaisesRegex(ValueError,'inventory'):
            self.verify()

    def test_every_perf_phase_requires_nonempty_observations(self):
        for name,key in [('aa/aa','left'),('aa/aa','right'),('safety/safety','controls'),('paired/paired','direct'),('paired/paired','full')]:
            path=self.perf[name]
            original=path.read_bytes()
            self.change(path,lambda value:value.update({key:[]}))
            with self.assertRaisesRegex(ValueError,'observations missing'):
                self.verify()
            path.write_bytes(original)

    def test_all_artifact_axes_and_manifest_shape_are_checked(self):
        for path in self.perf.values():
            original=path.read_bytes()
            self.change(path,lambda value:value['axes'].update(host_count=99))
            with self.assertRaisesRegex(ValueError,'artifact axes differ'):
                self.verify()
            path.write_bytes(original)
        self.change(self.manifest,lambda value:value.update(result='pass'))
        self.rebind_candidate()
        with self.assertRaisesRegex(ValueError,'shape differs'):
            self.verify()

    def test_boolean_and_integer_axes_are_not_interchangeable(self):
        self.change(self.manifest,lambda value:value['performance_axes'].update(host_count=True))
        self.rebind_candidate()
        with self.assertRaisesRegex(ValueError,'candidate plan'):
            self.verify()

    def test_reads_are_bounded_and_symlinks_reject(self):
        with patch.object(env,'MAX_BYTES',10), self.assertRaisesRegex(ValueError,'exceeds'):
            self.verify()
        target=self.root/'environment/e2e-environment.json'
        alias=target.with_name('aliased.json')
        target.rename(alias)
        target.symlink_to(alias.name)
        with self.assertRaises(ValueError):
            self.verify()

if __name__=='__main__':
    unittest.main()
