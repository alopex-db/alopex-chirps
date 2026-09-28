#!/usr/bin/env python3
"""Synthetic parser tests; these fixtures are not private-spec or release evidence."""
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

PATH = Path(__file__).with_name('v07_task_structure.py')
SPEC = importlib.util.spec_from_file_location('task_structure', PATH)
v = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(v)


def prompt():
    return '  - _Prompt: Introductory prose. ' + ' | '.join(name + ': synthetic value' for name in v.FIELDS) + '_'


def fixture(reference='Tasks 1.1-1.3; Task `1.2`; Tasks 1.1/1.3; Tasks 1.1, 1.2 and 1.3.'):
    return (reference + '\n' + '\n'.join('- [ ] ' + key + ' Synthetic\n' + prompt()
            for key in ('1.1', '1.2', '1.3')) + '\n').encode()


def verify(raw):
    return v.verify_bytes(raw, hashlib.sha256(raw).hexdigest())


class TaskStructureTests(unittest.TestCase):
    def reject(self, raw, code):
        with self.assertRaisesRegex(v.TaskStructureError, code):
            verify(raw)

    def test_synthetic_ranges_lists_inline_code_and_counts(self):
        result = verify(fixture())
        self.assertEqual((result['task_count'], result['prompt_count'], result['reference_count']), (3, 3, 9))
        self.assertEqual(set(result), {'schema', 'source_sha256', 'task_count', 'prompt_count', 'reference_count', 'result'})
        self.assertEqual(verify(fixture('Tasks **1.1–1.3**.'))['reference_count'], 3)

    def test_each_reference_is_checked_including_middle_of_range(self):
        for reference in ('Task 9.9', 'Tasks 1.1/9.9', 'Tasks 1.1, 9.9', 'Tasks 1.1 and 9.9', 'Tasks 1.1-1.4'):
            with self.subTest(reference=reference):self.reject(fixture(reference), 'unknown-reference')
        self.reject(fixture('Tasks 1.1-1.3').replace(b'- [ ] 1.2', b'- [ ] 2.2'), 'unknown-reference')

    def test_reference_in_prompt_body_is_not_exempt(self):
        self.reject(fixture('No introductory references.').replace(b'Task: synthetic value', b'Task: refer to Task 8.8', 1), 'unknown-reference')

    def test_malformed_reference_or_range_fails(self):
        for reference in ('Task 01.1', 'Task 1..2', 'Task 1.2.x', 'Task 1.2foo', 'Task 1_2', 'Task -1.2', 'Tasks 1.3-1.1', 'Tasks 1.1-2.1', 'Tasks 1.1-1.999999', 'Task 1.' + '1' * 70):
            with self.subTest(reference=reference):self.reject(fixture(reference), 'invalid-')

    def test_task_ids_are_real_unique_canonical_headers(self):
        self.reject(fixture().replace(b'- [ ] 1.2', b'- [ ] 1.1'), 'duplicate-task-id')
        for replacement in (b'- [ ] 01.2', b'- [ ] 1..2', b'- [ ] NOT_AN_ID', b'- [z] 1.2'):
            with self.subTest(replacement=replacement):self.reject(fixture().replace(b'- [ ] 1.2', replacement), 'invalid-task-')

    def test_task_status_is_not_an_approval_decision(self):
        raw = fixture().replace(b'[ ]', b'[x]', 1).replace(b'[ ]', b'[-]', 1)
        self.assertEqual(verify(raw)['task_count'], 3)

    def test_missing_duplicate_or_orphan_prompt_fails(self):
        raw = fixture()
        self.reject(raw.replace(prompt().encode(), b'', 1), 'missing-prompt')
        self.reject(raw.replace(prompt().encode(), (prompt() + '\n' + prompt()).encode(), 1), 'duplicate-prompt')
        self.reject((prompt() + '\n').encode() + raw, 'orphan-prompt')

    def test_field_name_order_count_and_nonempty_values_are_exact(self):
        mutations = (
            (' | Validation: synthetic value', ''),
            (' | Validation:', ' | Unexpected:'),
            (' | Validation:', ' | Task:'),
            (' | Success: synthetic value | Instructions: synthetic value', ' | Instructions: synthetic value | Success: synthetic value'),
            (' | Success: synthetic value', ' | Extra: synthetic value | Success: synthetic value'),
            ('Role: synthetic value', 'Role: '),
            ('Introductory prose. Role:', 'Extra: hidden Role:'),
            ('Role: synthetic value', 'Role: another Role: value'),
        )
        for before, after in mutations:
            with self.subTest(before=before, after=after):
                self.reject(fixture().replace(before.encode(), after.encode(), 1), 'invalid-prompt-fields')
        self.reject(fixture().replace(b'_Prompt:', b' _Prompt:unexpected', 1), 'invalid-prompt-container')

    def test_empty_and_non_utf8_inputs_fail(self):
        self.reject(b'', 'no-tasks')
        self.reject(b'\xff', 'invalid-utf8')

    def test_exact_digest_is_mandatory(self):
        for expected in ('', 'a' * 40, 'G' * 64):
            with self.assertRaisesRegex(v.TaskStructureError, 'invalid-expected-digest'):
                v.verify_bytes(fixture(), expected)
        with self.assertRaisesRegex(v.TaskStructureError, 'digest-mismatch'):
            v.verify_bytes(fixture(), '0' * 64)

    def test_size_and_expansion_budgets_fail_closed(self):
        with patch.object(v, 'MAX_INPUT_BYTES', 4):self.reject(fixture(), 'input-too-large')
        with patch.object(v, 'MAX_TASKS', 2):self.reject(fixture(), 'too-many-tasks')
        with patch.object(v, 'MAX_REFERENCES', 2):self.reject(fixture(), 'too-many-references')

    def test_cli_errors_never_echo_path_or_private_text(self):
        with tempfile.TemporaryDirectory(prefix='PRIVATE_PATH_CANARY-') as directory:
            path = Path(directory) / 'PRIVATE_FILE_CANARY.md'
            raw = fixture().replace(b' | Validation:', b' | PRIVATE_BODY_CANARY:', 1)
            path.write_bytes(raw)
            digest = hashlib.sha256(raw).hexdigest()
            for args in (
                ['--tasks-file', str(path), '--expected-sha256', digest],
                ['--tasks-file', str(path)],
                ['--tasks-file', str(path / 'PRIVATE_CHILD'), '--expected-sha256', digest],
                ['--tasks-file', str(path), '--expected-sha256', digest, '--PRIVATE_ARGUMENT_CANARY'],
            ):
                result = subprocess.run([sys.executable, '-B', str(PATH), *args], capture_output=True, text=True)
                self.assertEqual(result.returncode, 2)
                self.assertEqual(result.stdout, '')
                self.assertNotIn('PRIVATE_', result.stderr)
                self.assertNotIn(directory, result.stderr)
                self.assertNotIn('Traceback', result.stderr)
                self.assertLess(len(result.stderr), 120)

    def test_file_read_and_cli_success_only_emit_digest_and_counts(self):
        with tempfile.TemporaryDirectory(prefix='PRIVATE_PATH_CANARY-') as directory:
            path = Path(directory) / 'PRIVATE_FILE_CANARY.md'
            raw = fixture();path.write_bytes(raw);digest = hashlib.sha256(raw).hexdigest()
            self.assertEqual(v.verify_task_structure(path, digest), verify(raw))
            result = subprocess.run([sys.executable, '-B', str(PATH), '--tasks-file', str(path), '--expected-sha256', digest], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0)
            self.assertEqual(result.stderr, '')
            self.assertEqual(json.loads(result.stdout), verify(raw))
            self.assertNotIn('PRIVATE_', result.stdout)
            self.assertEqual(path.read_bytes(), raw)
            with self.assertRaisesRegex(v.TaskStructureError, 'input-not-regular'):
                v.verify_task_structure(Path(directory), digest)


if __name__ == '__main__':
    unittest.main()
