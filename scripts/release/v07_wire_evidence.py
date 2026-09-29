#!/usr/bin/env python3
"""Collect/replay the complete legacy runtime and wire compatibility matrix."""
from __future__ import annotations
import argparse
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import re

sys.dont_write_bytecode = True
from v07_e2e_evidence import (command_output, digest, executable_from_build, listed_tests,
    load, passed_tests, reference, resolve_reference, run_bounded, source_identity, write_new)

SCHEMA = "chirps.v0.7.legacy-wire/v1"
FEATURES = {"default": [], "no-default": ["--no-default-features"],
    "durable-enabled": ["--features", "durable-iggy"], "all": ["--all-features"]}
TARGETS = {
    "profile_compatibility": {"v061_quic_frame_handshake_and_envelope_bytes_match_golden", "default_backend_extension_never_falls_back_from_durable"},
    "v061_public_compatibility": {"v061_required_trait_surface_and_defaults_compile_and_run_unchanged", "v061_default_profile_extensions_preserve_delivery_and_reject_durable_fallback"},
}


def commands(mode: str, target: str) -> dict:
    return {
        "build": ["cargo", "test", "--locked", "-p", "alopex-chirps", "--test", target,
            *FEATURES[mode], "--no-run", "--message-format=json"],
        "list": ["<test-binary>", "--list", "--format=terse"],
        "run": ["<test-binary>", "--test-threads=1", "--color=never"],
    }


def verify_report(source_root: Path, path: Path, source_commit: str) -> dict:
    value = load(path)
    if set(value) != {"schema", "source", "rustc", "cargo", "jobs", "result"} or value["schema"] != SCHEMA or value["result"] != "pass":
        raise ValueError("legacy wire report fields or result differ")
    source = value["source"]
    tree = command_output(source_root, "git", "rev-parse", f"{source_commit}^{{tree}}")
    lock = subprocess.check_output(["git", "show", f"{source_commit}:Cargo.lock"], cwd=source_root, timeout=30)
    if source != {"source_commit": source_commit, "source_tree": tree, "lock_sha256": hashlib.sha256(lock).hexdigest()}:
        raise ValueError("legacy wire source differs from candidate Git objects")
    if any(not isinstance(value[key], str) or not value[key] for key in ("rustc", "cargo")):
        raise ValueError("legacy wire tool identity is missing")
    expected_jobs = [f"{mode}/{target}" for mode in FEATURES for target in TARGETS]
    if set(value["jobs"]) != set(expected_jobs):
        raise ValueError("legacy wire feature/target matrix is incomplete")
    for identifier in expected_jobs:
        mode, target = identifier.split("/")
        job = value["jobs"][identifier]
        if set(job) != {"commands", "exit_codes", "logs", "tests", "binary_sha256"} or job["commands"] != commands(mode, target):
            raise ValueError("legacy wire command was filtered or changed")
        if set(job["exit_codes"]) != {"build", "list", "run"} or any(type(code) is not int or code != 0 for code in job["exit_codes"].values()):
            raise ValueError("legacy wire command failed or was not executed")
        if not isinstance(job["binary_sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", job["binary_sha256"]):
            raise ValueError("legacy wire executable identity is missing")
        if set(job["logs"]) != {"build", "list", "run"}:
            raise ValueError("legacy wire raw log inventory differs")
        logs = {stage: resolve_reference(path.parent, ref) for stage, ref in job["logs"].items()}
        executable_from_build(logs["build"], target)
        tests = listed_tests(logs["list"].read_text())
        if not TARGETS[target] <= set(tests) or tests != job["tests"]:
            raise ValueError("legacy wire golden or runtime contract test is missing")
        passed_tests(logs["run"].read_text(), tests)
        if "; 0 filtered out;" not in logs["run"].read_text():
            raise ValueError("legacy wire execution filtered tests")
    return value


def collect(root: Path, output: Path) -> None:
    root, output = root.resolve(), output.resolve()
    if output.is_relative_to(root):
        raise ValueError("legacy wire evidence must be stored outside source")
    source = source_identity(root)
    output.mkdir(parents=True, exist_ok=False)
    report = {"schema": SCHEMA, "source": source,
        "rustc": command_output(root, "rustc", "--version", "--verbose"),
        "cargo": command_output(root, "cargo", "--version"), "jobs": {}, "result": "fail"}
    environment = dict(os.environ)
    environment["CARGO_TERM_COLOR"] = "never"
    try:
        for mode in FEATURES:
            for target in TARGETS:
                directory = output / mode / target
                directory.mkdir(parents=True)
                job = {"commands": commands(mode, target), "exit_codes": {}, "logs": {}, "tests": [], "binary_sha256": ""}
                report["jobs"][f"{mode}/{target}"] = job
                for stage, command in job["commands"].items():
                    argv = command.copy()
                    if stage != "build":
                        argv[0] = str(executable)
                    log = directory / f"{stage}.log"
                    code = run_bounded(argv, root, log, 900, env=environment)
                    job["logs"][stage] = reference(log, output)
                    job["exit_codes"][stage] = code
                    if code:
                        raise ValueError(f"legacy wire {mode}/{target}/{stage} failed: {code}")
                    if stage == "build":
                        executable = executable_from_build(log, target)
                        job["binary_sha256"] = digest(executable)
                    elif stage == "list":
                        job["tests"] = listed_tests(log.read_text())
                    else:
                        passed_tests(log.read_text(), job["tests"])
                if digest(executable) != job["binary_sha256"]:
                    raise ValueError("legacy wire executable changed during execution")
        if source_identity(root) != source:
            raise ValueError("legacy wire source changed during execution")
        report["result"] = "pass"
    finally:
        write_new(output / "report.json", report)
    verify_report(root, output / "report.json", source["source_commit"])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", required=True, type=Path)
    operation = parser.add_mutually_exclusive_group(required=True)
    operation.add_argument("--output", type=Path)
    operation.add_argument("--report", type=Path)
    parser.add_argument("--source-commit")
    args = parser.parse_args()
    try:
        if args.output:
            collect(args.source_root, args.output)
        else:
            if not args.source_commit:
                raise ValueError("read-only verification requires a source commit")
            verify_report(args.source_root, args.report, args.source_commit)
    except (ValueError, OSError, KeyError, TypeError, subprocess.SubprocessError) as error:
        parser.exit(1, f"legacy wire evidence rejected: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
