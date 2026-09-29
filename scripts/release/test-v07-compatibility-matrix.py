#!/usr/bin/env python3
"""Composition-only negative tests; component raw validators have separate tests."""
import copy
from contextlib import ExitStack
import json
import hashlib
import importlib.util
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import v07_compatibility_matrix as matrix


def write_matrix_fixture(root, source_root, source_commit, iggy_commit, api, production, fault):
    """Build explicit synthetic raw reports for publisher/schema rejection tests."""
    def module(name):
        spec = importlib.util.spec_from_file_location(name,Path(__file__).with_name(f'test-v07-{name}.py'))
        value = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(value)
        return value
    def write(path,value):
        path.write_text(json.dumps(value))
    source = dict(source_commit=source_commit,
        source_tree=subprocess.check_output(['git','-C',str(source_root),'rev-parse',f'{source_commit}^{{tree}}'],text=True).strip(),
        lock_sha256=hashlib.sha256(subprocess.check_output(['git','-C',str(source_root),'show',f'{source_commit}:Cargo.lock'])).hexdigest())
    directory = root/'compatibility'
    directory.mkdir()
    module('wire-evidence').write_wire_fixture(directory/'wire',source_root,source_commit)
    official_case = module('official-run').ExecutionTests()
    official_case.setUp()
    try:
        official_case.probe['source_commit'] = source_commit
        official_case.write_probe()
        official_case.report['source'] = source
        official_case.report['commands']['run'][2] = source_commit
        official_case.report['probe'] = matrix.reference(official_case.probe_dir/'report.json',official_case.output)
        official_case.path.write_text(json.dumps(official_case.report))
        shutil.copytree(official_case.output,directory/'official')
    finally:
        official_case.doCleanups()
    for lane,path in (('production',production),('fault',fault)):
        index = matrix.load(path)
        for target,ref in index['targets'].items():
            report_path = matrix.resolve(path.parent,ref)
            value = matrix.load(report_path)
            value['source'] = source
            value['server']['binary_sha256'] = ('8' if lane == 'production' else '9')*64
            value['server']['manifest_sha256'] = ('6' if lane == 'production' else '7')*64
            raw_path = matrix.resolve(report_path.parent,value['scenarios'])
            scenario = matrix.load(raw_path)
            scenario['artifact_sha256'] = value['server']['binary_sha256']
            write(raw_path,scenario)
            value['scenarios'] = matrix.reference(raw_path,report_path.parent)
            write(report_path,value)
            index['targets'][target] = matrix.reference(report_path,path.parent)
        write(path,index)
    output = root/'compatibility-matrix.json'
    matrix.seal(source_root,output,source_commit,iggy_commit,
        dict(api=api,wire=directory/'wire/report.json',official=directory/'official/execution.json',production=production,fault=fault))
    return output


class MatrixTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.path = self.root/'matrix.json'
        self.cells = {}
        for cell in matrix.CELLS:
            path = self.root/f'{cell}.json'
            path.write_text('{}')
            self.cells[cell] = path
        self.value = dict(schema=matrix.SCHEMA,source_commit='a'*40,iggy_commit='b'*40,
            cells={key:matrix.reference(path,self.root) for key,path in self.cells.items()},result='pass')
        self.source = dict(source_commit='a'*40,source_tree='c'*40,lock_sha256='d'*64)
        self.production = dict(source=self.source,corpus=dict(sha256='e'*64),environment=dict(sha256='f'*64),
            server=dict(binary_sha256='1'*64,manifest_sha256='2'*64))
        self.fault = copy.deepcopy(self.production)
        self.fault['server'] = dict(binary_sha256='3'*64,manifest_sha256='4'*64)
        stack = ExitStack()
        self.addCleanup(stack.close)
        self.api = stack.enter_context(patch.object(matrix,'verify_api_report'))
        self.wire = stack.enter_context(patch.object(matrix,'verify_wire',return_value=dict(source=self.source)))
        self.official = stack.enter_context(patch.object(matrix,'verify_official'))
        self.lane = stack.enter_context(patch.object(matrix,'verify_lane',side_effect=lambda path,lane,*_: self.production if lane == 'production' else self.fault))

    def verify(self):
        self.path.write_text(json.dumps(self.value))
        return matrix.verify(self.root,self.path,'a'*40,'b'*40)

    def test_all_cells_and_public_scope_are_mandatory(self):
        self.verify()
        self.api.assert_called_once_with(self.root,self.cells['api'],'a'*40)
        self.wire.assert_called_once_with(self.root,self.cells['wire'],'a'*40)
        self.official.assert_called_once_with(self.root,self.cells['official'],'a'*40,require_public=True)
        self.assertEqual([call.args[1] for call in self.lane.call_args_list],['production','fault'])

    def test_each_missing_cell_rejects_before_replay(self):
        for cell in matrix.CELLS:
            removed = self.value['cells'].pop(cell)
            with self.subTest(cell=cell),self.assertRaises(ValueError): self.verify()
            self.value['cells'][cell] = removed
        self.api.assert_not_called()

    def test_any_component_failure_rejects(self):
        for mock in (self.api,self.wire,self.official,self.lane):
            previous = mock.side_effect
            mock.side_effect = ValueError('component raw evidence rejected')
            with self.assertRaises(ValueError): self.verify()
            mock.side_effect = previous

    def test_git_source_and_environment_mixing_rejected(self):
        for key, field in [('source','source_tree'),('source','lock_sha256'),('environment','sha256'),('corpus','sha256')]:
            original = self.fault[key][field]
            self.fault[key][field] = '0'*len(original)
            with self.subTest(key=key,field=field),self.assertRaises(ValueError): self.verify()
            self.fault[key][field] = original

    def test_same_production_and_fault_artifact_rejected(self):
        for field in ('binary_sha256','manifest_sha256'):
            original = self.fault['server'][field]
            self.fault['server'][field] = self.production['server'][field]
            with self.subTest(field=field),self.assertRaises(ValueError): self.verify()
            self.fault['server'][field] = original

    def test_changed_or_escaping_reference_rejected(self):
        self.cells['api'].write_text('changed')
        with self.assertRaises(ValueError): self.verify()
        self.cells['api'].write_text('{}')
        self.value['cells']['api']['path'] = '../outside.json'
        with self.assertRaises(ValueError): self.verify()

    def test_failed_seal_never_leaves_passing_matrix(self):
        self.official.side_effect = ValueError('lower-level evidence is insufficient')
        output = self.root/'sealed.json'
        with self.assertRaises(ValueError): matrix.seal(self.root,output,'a'*40,'b'*40,self.cells)
        self.assertFalse(output.exists())
        self.assertFalse(output.with_name('sealed.json.validation').exists())


if __name__ == '__main__':
    unittest.main()
