#!/usr/bin/env python3
"""Build/hash a trusted verification tool, or invoke its read-only PERF replay."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import tempfile

SCHEMA = "chirps.v0.7.perf-verifier-tool/v1"
NAME = "chirps-durable-perf"


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], text=True, timeout=30).strip()


def build(source_root: Path, output: Path, expected_commit: str) -> None:
    if git(source_root, "rev-parse", "HEAD") != expected_commit or git(source_root, "status", "--porcelain", "--untracked-files=all"):
        raise ValueError("verification tool source must be the exact clean trusted checkout")
    output.mkdir(parents=True, exist_ok=False)
    with tempfile.TemporaryDirectory(prefix="chirps-perf-verifier-build-") as temporary:
        command = ["cargo", "build", "--locked", "--release", "--manifest-path", str(source_root / "Cargo.toml"),
                   "--package", NAME, "--bin", NAME, "--target-dir", temporary]
        with (output / "build.log").open("xb") as log:
            subprocess.run(command, cwd=source_root, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=1800)
        binary = output / NAME
        shutil.copyfile(Path(temporary) / "release" / NAME, binary)
        binary.chmod(0o755)
    if git(source_root, "rev-parse", "HEAD") != expected_commit or git(source_root, "status", "--porcelain", "--untracked-files=all"):
        raise ValueError("verification tool source changed during build")
    manifest = {"schema": SCHEMA, "source_commit": expected_commit, "source_tree": git(source_root, "rev-parse", "HEAD^{tree}"),
                "lock_sha256": digest(source_root / "Cargo.lock"), "binary": NAME, "sha256": digest(binary),
                "system": platform.system(), "machine": platform.machine(),
                "rustc": subprocess.check_output(["rustc", "--version", "--verbose"], text=True).strip(),
                "build_log_sha256": digest(output / "build.log")}
    with (output / "verifier.json").open("x") as stream:
        json.dump(manifest, stream, indent=2, sort_keys=True)
        stream.write("\n")


def validate_tool(binary: Path, expected_commit: str) -> dict:
    if not re.fullmatch(r"[0-9a-f]{40}", expected_commit):
        raise ValueError("trusted release-tools commit is required")
    if binary.name != NAME or not binary.is_absolute() or binary.is_symlink() or not os.access(binary, os.X_OK):
        raise ValueError("trusted PERF verifier binary is missing or not executable")
    value = json.loads((binary.parent / "verifier.json").read_text())
    fields = {"schema", "source_commit", "source_tree", "lock_sha256", "binary", "sha256", "system", "machine", "rustc", "build_log_sha256"}
    if set(value) != fields or value["schema"] != SCHEMA or value["source_commit"] != expected_commit or value["binary"] != NAME:
        raise ValueError("verification tool identity differs from the trusted CI checkout")
    if value["system"] != platform.system() or value["machine"] != platform.machine():
        raise ValueError("verification tool platform differs")
    for name in ("lock_sha256", "sha256", "build_log_sha256"):
        if not re.fullmatch(r"[0-9a-f]{64}", value[name]):
            raise ValueError("verification tool artifact digest is invalid")
    if not re.fullmatch(r"[0-9a-f]{40}", value["source_tree"]) or not value["rustc"]:
        raise ValueError("verification tool build identity is incomplete")
    if digest(binary) != value["sha256"] or digest(binary.parent / "build.log") != value["build_log_sha256"]:
        raise ValueError("verification tool bytes differ from the same-run artifact")
    return value


def verify(candidate: Path, evidence_root: Path) -> None:
    binary = Path(os.environ.get("CHIRPS_PERF_VERIFIER", ""))
    commit = os.environ.get("CHIRPS_RELEASE_TOOLS_COMMIT", "")
    identity = validate_tool(binary, commit)
    command = [str(binary), "--mode", "verify", "--candidate-manifest", str(candidate), "--evidence-root", str(evidence_root)]
    subprocess.run(command, check=True, timeout=300)
    if validate_tool(binary, commit) != identity:
        raise ValueError("trusted verification tool changed during replay")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="mode", required=True)
    prepare = modes.add_parser("build")
    prepare.add_argument("--source-root", type=Path, required=True)
    prepare.add_argument("--output", type=Path, required=True)
    prepare.add_argument("--source-commit", required=True)
    replay = modes.add_parser("verify")
    replay.add_argument("--candidate", type=Path, required=True)
    replay.add_argument("--evidence-root", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.mode == "build":
            build(args.source_root.resolve(), args.output.resolve(), args.source_commit)
        else:
            verify(args.candidate.resolve(), args.evidence_root.resolve())
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        parser.exit(1, f"trusted PERF verification rejected: {error}\n")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
