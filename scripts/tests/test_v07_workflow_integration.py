"""Reject workflow changes that validate a different checkout than the candidate."""
from pathlib import Path
import subprocess
import tempfile
import unittest

REPO = Path(__file__).resolve().parents[2]
VERIFIER = REPO / "scripts/verify-release-contract.sh"
WORKFLOW = (REPO / ".github/workflows/release.yml").read_text()


class CandidateCheckoutTests(unittest.TestCase):
    def verify(self, workflow):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / ".github/workflows/release.yml"
            path.parent.mkdir(parents=True)
            path.write_text(workflow)
            return subprocess.run(
                ["bash", str(VERIFIER), "--publication-workflow", "--repo-root", str(root)],
                text=True, capture_output=True, check=False,
            )

    def test_isolated_candidate_and_current_release_tools_are_accepted(self):
        result = self.verify(WORKFLOW)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_candidate_checkout_cannot_follow_the_workflow_revision(self):
        changed = WORKFLOW.replace(
            "ref: ${{ inputs.commit }}", "ref: ${{ github.sha }}", 1
        )
        result = self.verify(changed)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("explicit commit input", result.stderr)

    def test_gate_commands_cannot_run_in_the_tooling_checkout(self):
        changed = WORKFLOW.replace(
            "working-directory: source", "working-directory: release-tools", 1
        )
        result = self.verify(changed)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("candidate source checkout", result.stderr)

    def test_release_tools_cannot_follow_a_historical_candidate(self):
        changed = WORKFLOW.replace(
            "ref: ${{ github.sha }}", "ref: ${{ inputs.commit }}", 1
        )
        result = self.verify(changed)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("workflow revision", result.stderr)


if __name__ == "__main__":
    unittest.main()
