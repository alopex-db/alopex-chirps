#!/usr/bin/env python3
"""Synthetic reference-integrity tests, not formal model evidence."""
from pathlib import Path
import unittest
from unittest.mock import patch

import v07_formal_refinements as gate

CATALOG = '''schema: alopex-formal-catalog/v1
requirements:
  - id: V7-MODEL-001
    refinement: [{repository: chirps, path: crates/example/src/lib.rs, task: "1.1"}, {repository: iggy-compatible, path: core/server/example.rs, task: "5.3"}]
    planned_test: [{repository: chirps, path: tests/check.rs, task: "6.4"}]
'''


class RefinementTests(unittest.TestCase):
    def test_multiline_and_flow_requirements(self):
        self.assertEqual(len(gate.requirement_references(CATALOG)),3)
        flow = CATALOG.replace('  - id:', '  - {id:').replace('\n    refinement:', ', refinement:').replace('\n    planned_test:', ', planned_test:').rstrip()+'}\n'
        self.assertEqual(gate.requirement_references(flow),gate.requirement_references(CATALOG))

    def test_missing_production_test_unknown_repo_and_traversal_reject(self):
        for bad in (CATALOG.replace('refinement:', 'absent:'), CATALOG.replace('planned_test:', 'absent:'),
                    CATALOG.replace('repository: chirps','repository: invented'),
                    CATALOG.replace('crates/example/src/lib.rs','crates/../secret'),
                    CATALOG.replace('crates/example/src/lib.rs','/crates/example/src/lib.rs'),
                    CATALOG.replace('task: "1.1"','other: "1.1"')):
            with self.subTest(bad=bad),self.assertRaises(ValueError):
                gate.requirement_references(bad)

    def test_missing_referenced_file_cannot_pass_release(self):
        with patch.object(gate,'source_files',return_value=set()),patch.object(gate,'git',return_value=CATALOG.encode()),self.assertRaisesRegex(ValueError,'absent release'):
            gate.verify_refinements(Path('/chirps'),'a'*40,Path('/iggy'),'b'*40)

    def test_all_refs_exist_in_exact_candidates(self):
        inventory={'crates/example/src/lib.rs','core/server/example.rs','tests/check.rs'}
        with patch.object(gate,'source_files',return_value=inventory) as files,patch.object(gate,'git',return_value=CATALOG.encode()):
            result=gate.verify_refinements(Path('/chirps'),'a'*40,Path('/iggy'),'b'*40)
        self.assertEqual(result['status'],'pass')
        self.assertEqual(len(result['references']),12)
        self.assertEqual(files.call_args_list[0].args,(Path('/chirps'),'a'*40))
        self.assertEqual(files.call_args_list[1].args,(Path('/iggy'),'b'*40))

    def test_symlinks_and_submodules_are_not_real_refinement_files(self):
        records=b'100644 blob '+b'f'*40+b'\tcrates/good.rs\0'+b'120000 blob '+b'e'*40+b'\tcrates/link.rs\0'+b'160000 commit '+b'd'*40+b'\tcore/submodule\0'
        with patch.object(gate,'git',side_effect=[b'a'*40,records]):
            self.assertEqual(gate.source_files(Path('/repo'),'a'*40),{'crates/good.rs'})

    def test_missing_and_abbreviated_commits_reject(self):
        with self.assertRaises(ValueError):
            gate.source_files(Path('/repo'),'abcdef')
        with patch.object(gate,'git',return_value=b'b'*40),self.assertRaises(ValueError):
            gate.source_files(Path('/repo'),'a'*40)


if __name__=='__main__':
    unittest.main()
