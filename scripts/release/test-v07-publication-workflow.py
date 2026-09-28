#!/usr/bin/env python3
"""Source/run separation regressions; never invokes publication commands."""
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]

class WorkflowTests(unittest.TestCase):
    def check(self, content):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / '.github/workflows/release.yml'
            path.parent.mkdir(parents=True)
            path.write_text(content)
            return subprocess.run(['bash', str(ROOT / 'scripts/verify-release-contract.sh'), '--publication-workflow', '--repo-root', str(root)], capture_output=True, text=True)

    def test_exact_workflow_passes(self):
        result = self.check((ROOT / '.github/workflows/release.yml').read_text())
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_wrong_tool_source_run_or_missing_failure_evidence_rejects(self):
        content = (ROOT / '.github/workflows/release.yml').read_text()
        changes = [
            ('CHIRPS_RELEASE_TOOLS_COMMIT: ${{ github.sha }}', 'CHIRPS_RELEASE_TOOLS_COMMIT: ${{ inputs.commit }}'),
            ('name: chirps-v07-perf-verifier-${{ github.run_id }}', 'name: chirps-v07-perf-verifier-${{ inputs.v07_artifact_run_id }}'),
            ('--source-commit "${{ github.sha }}"', '--source-commit "${{ inputs.commit }}"'),
            ('CHIRPS_POSTPUBLISH_EVIDENCE_DIR=${RUNNER_TEMP}/chirps-v07-registry-consumer', 'CHIRPS_POSTPUBLISH_EVIDENCE_DIR=/tmp/discarded'),
            ('if: always()', 'if: success()'),
        ]
        for old, new in changes:
            with self.subTest(change=old):
                self.assertIn(old, content)
                self.assertNotEqual(self.check(content.replace(old,new)).returncode, 0)

if __name__ == '__main__':
    unittest.main()
