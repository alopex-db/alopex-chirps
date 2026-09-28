#!/usr/bin/env python3
"""Collect and independently check complete, immutable v0.7 E2E results."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import signal
import subprocess
import sys
import time
import tomllib

TARGETS = {
    "production": (
        "durable_session", "durable_send", "durable_shutdown", "durable_creation",
        "durable_checkpoint", "durable_poll", "durable_delivery", "durable_compaction",
        "durable_diagnostics", "durable_observability",
    ),
    "fault": ("durable_session", "durable_send", "durable_diagnostics", "durable_metadata_recovery"),
}
SCHEMA = "chirps.v0.7.e2e-target/v1"
LANE_SCHEMA = "chirps.v0.7.e2e-lane/v1"
MAX_LOG_BYTES = 32 * 1024 * 1024
SHA256 = re.compile(r"[0-9a-f]{64}")
COMMIT = re.compile(r"[0-9a-f]{40}")


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def write_new(path: Path, value: dict) -> None:
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")


def command_output(root: Path, *args: str) -> str:
    return subprocess.check_output(args, cwd=root, text=True, timeout=30).strip()


def source_identity(root: Path) -> dict:
    if command_output(root, "git", "status", "--porcelain", "--untracked-files=all"):
        raise ValueError("E2E evidence requires a committed, clean source checkout")
    return {
        "source_commit": command_output(root, "git", "rev-parse", "HEAD"),
        "source_tree": command_output(root, "git", "rev-parse", "HEAD^{tree}"),
        "lock_sha256": digest(root / "Cargo.lock"),
    }


def inventory(root: Path) -> dict[str, str]:
    files = sorted(root.rglob("*"))
    if any(path.is_symlink() for path in files):
        raise ValueError("corpus inventory may not include symlinks")
    result = {path.relative_to(root).as_posix(): digest(path) for path in files if path.is_file()}
    if not result:
        raise ValueError("corpus inventory is empty")
    return result


def run_bounded(argv: list[str], root: Path, log: Path, timeout: float) -> int:
    """Retain raw output and stop only this invocation's process group on failure."""
    with log.open("xb") as stream:
        process = subprocess.Popen(
            argv, cwd=root, stdout=stream, stderr=subprocess.STDOUT, start_new_session=True,
        )
        deadline = time.monotonic() + timeout
        try:
            while process.poll() is None:
                if log.stat().st_size > MAX_LOG_BYTES:
                    raise ValueError(f"{log.name} exceeded the 32 MiB output budget")
                if time.monotonic() >= deadline:
                    raise ValueError(f"{log.name} exceeded its {timeout:g}s deadline")
                time.sleep(0.1)
            if log.stat().st_size > MAX_LOG_BYTES:
                raise ValueError(f"{log.name} exceeded the 32 MiB output budget")
            return process.returncode
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()


def listed_tests(raw: str) -> list[str]:
    names = [line[:-6] for line in raw.splitlines() if line.endswith(": test")]
    if not names or len(names) != len(set(names)):
        raise ValueError("test inventory is empty or contains duplicates")
    # --ignored is applied to both discovery and execution. Benchmarks are not E2E tests.
    if any(line.endswith(": benchmark") for line in raw.splitlines()):
        raise ValueError("E2E inventory unexpectedly includes benchmarks")
    return sorted(names)


def passed_tests(raw: str, expected: list[str]) -> list[str]:
    results = re.findall(r"^test (\S+) \.\.\. (ok|FAILED|ignored)\s*$", raw, re.MULTILINE)
    if sorted(name for name, _ in results) != expected:
        raise ValueError("executed test names differ from the complete binary inventory")
    if any(result != "ok" for _, result in results):
        raise ValueError("E2E suite contains failed or ignored tests")
    summaries = re.findall(
        r"^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; "
        r"(\d+) measured; (\d+) filtered out; finished in .+$", raw, re.MULTILINE,
    )
    if len(summaries) != 1:
        raise ValueError("E2E suite has no unique successful test summary")
    passed, failed, ignored, measured, _ = map(int, summaries[0])
    if (passed, failed, ignored, measured) != (len(expected), 0, 0, 0):
        raise ValueError("E2E summary counts differ from individual test results")
    return expected


def reference(path: Path, root: Path) -> dict:
    return {"path": path.relative_to(root).as_posix(), "sha256": digest(path)}


def resolve_reference(root: Path, value: object) -> Path:
    if not isinstance(value, dict) or set(value) != {"path", "sha256"}:
        raise ValueError("invalid evidence reference")
    name, expected = value["path"], value["sha256"]
    if not isinstance(name, str) or not name or Path(name).is_absolute() or ".." in Path(name).parts:
        raise ValueError("evidence reference must remain under its report directory")
    if not isinstance(expected, str) or not SHA256.fullmatch(expected):
        raise ValueError("evidence reference has invalid digest")
    path = root / name
    if any(root.joinpath(*Path(name).parts[:i]).is_symlink() for i in range(1, len(Path(name).parts) + 1)):
        raise ValueError("evidence reference traverses a symlink")
    if not path.is_file() or digest(path) != expected:
        raise ValueError(f"evidence bytes missing or changed: {name}")
    return path


def load(path: Path) -> dict:
    if path.stat().st_size > MAX_LOG_BYTES:
        raise ValueError("evidence JSON exceeds its size budget")
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError("evidence JSON must be an object")
    return value


def verify_target(path: Path, lane: str, target: str, source_commit: str, iggy_commit: str) -> dict:
    value = load(path)
    expected_keys = {
        "schema", "lane", "target", "status", "source", "server", "environment",
        "test_binary_sha256", "corpus", "commands", "exit_codes", "logs", "tests",
    }
    if set(value) != expected_keys:
        raise ValueError("E2E report fields differ")
    if (value["schema"], value["lane"], value["target"], value["status"]) != (SCHEMA, lane, target, "pass"):
        raise ValueError("E2E report identity or result differs")
    source, server = value["source"], value["server"]
    if set(source) != {"source_commit", "source_tree", "lock_sha256"}:
        raise ValueError("incomplete E2E source identity")
    if source["source_commit"] != source_commit or server.get("source_commit") != iggy_commit:
        raise ValueError("E2E results belong to another candidate")
    for item in (source_commit, source["source_tree"], iggy_commit, server.get("source_tree")):
        if not isinstance(item, str) or not COMMIT.fullmatch(item):
            raise ValueError("invalid source commit/tree identity")
    if set(server) != {"source_commit", "source_tree", "manifest_sha256", "binary_sha256"}:
        raise ValueError("incomplete server identity")
    for item in (source["lock_sha256"], value["test_binary_sha256"], server["manifest_sha256"], server["binary_sha256"]):
        if not isinstance(item, str) or not SHA256.fullmatch(item):
            raise ValueError("invalid E2E artifact digest")
    stages = {"build", "list", "run"}
    if set(value["exit_codes"]) != stages or any(type(code) is not int or code != 0 for code in value["exit_codes"].values()):
        raise ValueError("E2E command did not exit successfully")
    if set(value["logs"]) != stages or set(value["commands"]) != stages:
        raise ValueError("E2E command/log inventory differs")
    if value["commands"] != commands(target):
        raise ValueError("E2E command was filtered or changed")
    logs = {key: resolve_reference(path.parent, value["logs"][key]) for key in stages}
    # Validate Cargo actually built the requested test target, not merely an exit-0 stub.
    executable_from_build(logs["build"], target)
    expected = listed_tests(logs["list"].read_text())
    passed_tests(logs["run"].read_text(), expected)
    if value["tests"] != expected:
        raise ValueError("E2E reported test set differs from raw logs")
    corpus = load(resolve_reference(path.parent, value["corpus"]))
    if not corpus or any(not isinstance(key, str) or not key or not isinstance(item, str) or not SHA256.fullmatch(item) for key, item in corpus.items()):
        raise ValueError("invalid or empty corpus inventory")
    environment = load(resolve_reference(path.parent, value["environment"]))
    if set(environment) != {"system", "release", "machine", "node", "rustc", "cargo"} or any(not isinstance(item, str) or not item for item in environment.values()):
        raise ValueError("incomplete E2E environment observation")
    return value


def commands(target: str) -> dict[str, list[str]]:
    return {
        "build": ["cargo", "test", "--locked", "--manifest-path", "Cargo.toml", "-p", "chirps-e2e", "--test", target, "--no-run", "--message-format=json"],
        "list": ["<test-binary>", "--ignored", "--list", "--format=terse"],
        "run": ["<test-binary>", "--ignored", "--test-threads=1", "--color=never"],
    }


def executable_from_build(log: Path, target: str) -> Path:
    artifacts = []
    finished = []
    for line in log.read_text().splitlines():
        if not line.startswith("{"):
            continue
        record = json.loads(line)
        if record.get("reason") == "compiler-artifact" and record.get("target", {}).get("name") == target and record.get("profile", {}).get("test"):
            if record.get("executable"):
                artifacts.append(Path(record["executable"]))
        if record.get("reason") == "build-finished":
            finished.append(record.get("success"))
    if len(artifacts) != 1 or finished != [True]:
        raise ValueError("Cargo did not report exactly one successful test executable")
    return artifacts[0]


def collect(root: Path, output: Path, lane: str, target: str, timeout: float) -> None:
    if target not in TARGETS[lane]:
        raise ValueError("target is not in the selected mandatory lane")
    root = root.resolve()
    output = output.resolve()
    if output.is_relative_to(root):
        raise ValueError("store E2E evidence outside the source checkout")
    source = source_identity(root)
    manifest = Path(os.environ["CHIRPS_SERVER_MANIFEST"])
    binary = Path(os.environ["CHIRPS_SERVER_BINARY"])
    server_doc = tomllib.loads(manifest.read_text())
    server = {
        "source_commit": os.environ["CHIRPS_SERVER_SOURCE_COMMIT"],
        "source_tree": os.environ["CHIRPS_SERVER_SOURCE_TREE"],
        "manifest_sha256": digest(manifest), "binary_sha256": digest(binary),
    }
    if server["binary_sha256"] != os.environ["CHIRPS_SERVER_SHA256"] or server_doc["source"]["commit"] != server["source_commit"]:
        raise ValueError("verified server identity changed before collection")
    corpus_root = Path(os.environ["CHIRPS_LOCAL_CORPUS_ROOT"])
    corpus = inventory(corpus_root)
    directory = output / target
    directory.mkdir(parents=True, exist_ok=False)
    write_new(directory / "corpus.json", corpus)
    write_new(directory / "environment.json", {
        "system": platform.system(), "release": platform.release(), "machine": platform.machine(),
        "node": platform.node(), "rustc": command_output(root, "rustc", "--version", "--verbose"),
        "cargo": command_output(root, "cargo", "--version"),
    })
    command = commands(target)
    report = {
        "schema": SCHEMA, "lane": lane, "target": target, "status": "fail", "source": source,
        "server": server, "environment": reference(directory / "environment.json", directory),
        "corpus": reference(directory / "corpus.json", directory), "test_binary_sha256": "",
        "commands": command, "exit_codes": {}, "logs": {}, "tests": [],
    }
    try:
        for stage in ("build", "list", "run"):
            argv = command[stage].copy()
            if stage != "build":
                argv[0] = str(executable)
            log = directory / f"{stage}.log"
            code = run_bounded(argv, root, log, timeout)
            report["exit_codes"][stage] = code
            report["logs"][stage] = reference(log, directory)
            if code:
                raise ValueError(f"{target} {stage} exited {code}; raw output: {log}")
            if stage == "build":
                executable = executable_from_build(log, target)
                report["test_binary_sha256"] = digest(executable)
            elif stage == "list":
                report["tests"] = listed_tests(log.read_text())
            else:
                passed_tests(log.read_text(), report["tests"])
        if source_identity(root) != source or inventory(corpus_root) != corpus or digest(executable) != report["test_binary_sha256"] or digest(binary) != server["binary_sha256"] or digest(manifest) != server["manifest_sha256"]:
            raise ValueError("source, corpus, or binary identity changed during E2E execution")
        report["status"] = "pass"
    finally:
        write_new(directory / "report.json", report)
    verify_target(directory / "report.json", lane, target, source["source_commit"], server["source_commit"])
    print(f'{lane}/{target}: {len(report["tests"])} tests passed; {directory / "report.json"}')


def verify_lane(path: Path, lane: str, source_commit: str, iggy_commit: str) -> dict:
    value = load(path)
    if set(value) != {"schema", "lane", "targets"} or value["schema"] != LANE_SCHEMA or value["lane"] != lane:
        raise ValueError("E2E lane report identity differs")
    if not isinstance(value["targets"], dict) or set(value["targets"]) != set(TARGETS[lane]):
        raise ValueError("E2E lane is missing mandatory targets or contains unknown targets")
    reports = [
        verify_target(resolve_reference(path.parent, value["targets"][target]), lane, target, source_commit, iggy_commit)
        for target in TARGETS[lane]
    ]
    first = reports[0]
    for report in reports[1:]:
        if any(report[key] != first[key] for key in ("source", "server")) or any(report[key]["sha256"] != first[key]["sha256"] for key in ("environment", "corpus")):
            raise ValueError("E2E lane mixes source, server, environment, or corpus identities")
    return first


def seal_lane(root: Path, output: Path, lane: str) -> None:
    source = source_identity(root)
    value = {
        "schema": LANE_SCHEMA, "lane": lane,
        "targets": {target: reference(output / target / "report.json", output) for target in TARGETS[lane]},
    }
    # Validate in memory through a temporary file; never leave a passing-looking
    # lane index behind if one target is missing or stale.
    scratch = output / ".lane-validation.json"
    write_new(scratch, value)
    try:
        verify_lane(scratch, lane, source["source_commit"], os.environ["CHIRPS_SERVER_SOURCE_COMMIT"])
    finally:
        scratch.unlink()
    write_new(output / "lane.json", value)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--lane", choices=TARGETS, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--target")
    mode.add_argument("--seal-lane", action="store_true")
    parser.add_argument("--timeout-seconds", type=int, default=900)
    args = parser.parse_args()
    if not 1 <= args.timeout_seconds <= 3600:
        parser.error("timeout must be between 1 and 3600 seconds")
    try:
        if args.seal_lane:
            seal_lane(args.repo_root.resolve(), args.output.resolve(), args.lane)
        else:
            collect(args.repo_root, args.output, args.lane, args.target, args.timeout_seconds)
    except (ValueError, OSError, KeyError, subprocess.SubprocessError) as error:
        print(f"E2E evidence rejected: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
