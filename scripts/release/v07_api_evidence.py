#!/usr/bin/env python3
"""Independently verify/package v0.6.1 API comparisons; never run Cargo.

Usage: v07_api_evidence.py --source-root REPO --source-commit SHA --report JSON
Add --package NEW_DIRECTORY to copy legacy local evidence into a portable bundle.
The report/logs are observations, not signed attestations of a trusted runner.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path, PurePosixPath, PureWindowsPath
import re
import subprocess
import sys
import tomllib

BASELINE = "3ff0ce6a631fd235fc4a3e3e08c8a9665f3d8bd9"
TOOL = "cargo-semver-checks 0.50.0"
CRATES = (
    "chirps-wire", "chirps-core", "chirps-gossip-swim", "chirps-mock",
    "chirps-transport-quic", "chirps-file-transfer", "chirps-raft-storage", "alopex-chirps",
)
MAX_BYTES = 32 * 1024 * 1024
COMMIT = re.compile(r"[0-9a-f]{40}")
SHA = re.compile(r"[0-9a-f]{64}")
REPORT_FIELDS = {
    "schema_version", "baseline_commit", "candidate_commit", "tool", "rustc",
    "candidate_lock_sha256", "baseline_archive_sha256", "target_dir",
    "expected_checks", "status", "checks", "candidate_unchanged",
}
CHECK_FIELDS = {
    "package", "manifest", "mode", "flags", "command", "exit_code",
    "elapsed_seconds", "log", "log_sha256",
}


def ensure(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def unique_object(pairs: list[tuple]) -> dict:
    result = {}
    for key, value in pairs:
        ensure(key not in result, f"duplicate JSON key: {key}")
        result[key] = value
    return result


def bounded_read(path: Path) -> bytes:
    with path.open("rb") as stream:
        value = stream.read(MAX_BYTES + 1)
    ensure(len(value) <= MAX_BYTES, "evidence exceeds 32 MiB budget")
    return value


def load(path: Path) -> dict:
    value = json.loads(bounded_read(path), object_pairs_hook=unique_object)
    ensure(isinstance(value, dict), "API report must be an object")
    return value


def git_bytes(root: Path, *args: str) -> bytes:
    return subprocess.check_output(["git", "-C", str(root), *args], timeout=30)


def manifest(root: Path, commit: str, path: str) -> dict:
    value = tomllib.loads(git_bytes(root, "show", f"{commit}:{path}").decode())
    for scope in ("workspace", "package"):
        ensure("cargo-semver-checks" not in value.get(scope, {}).get("metadata", {}),
               "semver lint overrides are forbidden")
    return value


def public_features(value: dict) -> set[str]:
    declared = value.get("features", {})
    hidden = {item[4:] for items in declared.values() for item in items if item.startswith("dep:")}
    dependencies = [value.get("dependencies", {})]
    dependencies.extend(target.get("dependencies", {}) for target in value.get("target", {}).values())
    implicit = {name for table in dependencies for name, spec in table.items()
                if isinstance(spec, dict) and spec.get("optional") and name not in hidden}
    return (set(declared) | implicit) - {"default"}


def source_contract(root: Path, commit: str) -> dict:
    """Derive the matrix from immutable Git objects, never candidate Python code."""
    ensure(isinstance(commit, str) and COMMIT.fullmatch(commit) is not None, "full candidate SHA required")
    for revision in (BASELINE, commit):
        ensure(git_bytes(root, "rev-parse", f"{revision}^{{commit}}").decode().strip() == revision,
               "immutable source commit is unavailable")
    workspaces = {revision: manifest(root, revision, "Cargo.toml") for revision in (BASELINE, commit)}
    checks = []
    for directory in CRATES:
        path = f"crates/{directory}/Cargo.toml"
        old, new = (manifest(root, revision, path) for revision in (BASELINE, commit))
        package = old["package"]["name"]
        ensure(new["package"]["name"] == package, "baseline package renamed")
        before, after = public_features(old), public_features(new)
        ensure(not before - after, f"baseline features removed: {package}")
        versions = []
        for revision, value in ((BASELINE, old), (commit, new)):
            version = value["package"]["version"]
            if version == {"workspace": True}:
                version = workspaces[revision]["workspace"]["package"]["version"]
            ensure(isinstance(version, str), "unsupported package version")
            versions.append(version)
        modes = [("all", ["--all-features"]), ("default", ["--default-features"]),
                 ("no-default", ["--only-explicit-features"])]
        for feature in sorted(before | after):
            flags = ["--only-explicit-features", "--current-features", feature]
            if feature in before:
                flags += ["--baseline-features", feature]
            modes.append((f"feature-{feature}", flags))
        checks.extend({"package": package, "manifest": path, "mode": mode, "flags": flags,
                       "versions": versions} for mode, flags in modes)
    return {"checks": checks, "lock": sha(git_bytes(root, "show", f"{commit}:Cargo.lock")),
            "archive": sha(git_bytes(root, "archive", "--format=tar", BASELINE))}


def portable_log(report: Path, name: object, *, allow_local: bool = False) -> Path:
    ensure(isinstance(name, str) and bool(name), "missing API log reference")
    path = Path(name)
    if allow_local and path.is_absolute():
        # This migration input is accepted only by the packager, never the release verifier.
        ensure(not any(part.is_symlink() for part in (path, *path.parents)), "local log traverses symlink")
        return path
    relative = PurePosixPath(name)
    ensure(not relative.is_absolute() and not PureWindowsPath(name).drive and "\\" not in name
           and relative.as_posix() == name and all(part not in {".", ".."} for part in relative.parts),
           "API log reference must be a canonical relative path")
    path = report.parent / name
    ensure(not any(report.parent.joinpath(*relative.parts[:i]).is_symlink()
                   for i in range(1, len(relative.parts) + 1)), "API log traverses symlink")
    return path


def invocation_path(value: str, suffix: str) -> str:
    # Commands retain original machine paths; they are never executed by this verifier.
    path = PureWindowsPath(value) if "\\" in value or re.match(r"^[A-Za-z]:", value) else PurePosixPath(value)
    expected = PurePosixPath(suffix).parts
    ensure(path.is_absolute() and ".." not in path.parts and tuple(path.parts[-len(expected):]) == expected,
           "API command points at a different crate manifest")
    return str(path.parents[len(expected) - 1])


def verify_command(argv: object, check: dict) -> tuple[str, str, str]:
    ensure(isinstance(argv, list) and all(isinstance(item, str) for item in argv), "invalid API command")
    ensure(len(argv) == 12 + len(check["flags"]), "API command includes filtering or extra flags")
    tool = argv[0].replace("\\", "/").rsplit("/", 1)[-1]
    ensure(tool in {"cargo-semver-checks", "cargo-semver-checks.exe"}, "unexpected API checker")
    ensure(argv[1:3] == ["semver-checks", "--manifest-path"] and argv[4:7] == ["--package", check["package"], "--baseline-root"]
           and argv[8:12] == ["--release-type", "minor", "--color", "never"]
           and argv[12:] == check["flags"], "API command was suppressed or changed")
    current = invocation_path(argv[3], check["manifest"])
    baseline = invocation_path(argv[7], check["manifest"])
    ensure(current != baseline, "API command compares candidate with itself")
    return argv[0], current, baseline


def verify_log(raw: bytes, check: dict) -> None:
    text = raw.decode("utf-8")
    ensure(not re.search(r"[\x00-\x08\x0b-\x1f\x7f]", text), "API log contains terminal controls")
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    package = check["package"]
    old, new = check["versions"]
    checking = f"Checking {package} v{old} -> v{new} (assume minor change)"
    ensure(sum(line.startswith("Checking ") for line in lines) == 1 and checking in lines,
           "API log compared another package/version or release type")
    # 0.50.0 with forced minor checks 196 major lints and skips its 58 minor lints.
    # Pinning these counts rejects filtered/zero-work checks without forbidding
    # the tool's intentional release-type skip behavior.
    summaries = [line for line in lines if line.startswith("Checked ")]
    ensure(len(summaries) == 1 and re.fullmatch(
        r"Checked \[\s*\d+\.\d+s\] 196 checks: 196 pass, 58 skip", summaries[0]) is not None,
        "API raw log is incomplete, filtered, failed, or has unexpected lint counts")
    summary = "Summary no semver update required"
    ensure(lines.count(summary) == 1 and sum(line.startswith("Summary ") for line in lines) == 1,
           "API raw log has no unique passing summary")
    ensure(len(lines) >= 4 and re.fullmatch(r"Finished \[\s*\d+\.\d+s\] " + re.escape(package), lines[-1]) is not None,
           "API raw log has no final completion for this package")
    ensure(sum(line.startswith("Finished ") for line in lines) == 1
           and lines.index(checking) < lines.index(summaries[0]) < lines.index(summary) < len(lines) - 1,
           "API log completion order differs")
    ensure(not any(re.match(r"(?i)(?:error\b|failure\b|failed\b|fail\s|warning:.*(?:semver|lint)|.*\b[1-9]\d* (?:fail|warn)\b)", line)
                   for line in lines), "API log contains failure or suppressed-lint diagnostics")


def verify_report(value: dict, report: Path, source_commit: str, contract: dict,
                  *, allow_local: bool = False) -> list[tuple[dict, bytes]]:
    ensure(set(value) == REPORT_FIELDS, "API report fields differ")
    ensure(type(value["schema_version"]) is int and value["schema_version"] == 1, "unknown API report schema")
    ensure(value["candidate_commit"] == source_commit and COMMIT.fullmatch(source_commit) is not None,
           "API results belong to another candidate")
    ensure(value["baseline_commit"] == BASELINE and value["tool"] == TOOL, "API baseline/tool differs")
    ensure(value["status"] == "pass" and value["candidate_unchanged"] is True, "API candidate changed or failed")
    ensure(value["candidate_lock_sha256"] == contract["lock"] and value["baseline_archive_sha256"] == contract["archive"],
           "API lockfile or immutable baseline archive differs")
    ensure(isinstance(value["rustc"], str) and value["rustc"].startswith("rustc 1.96.0 ")
           and "\nrelease: 1.96.0\n" in value["rustc"], "API Rust toolchain differs")
    ensure(isinstance(value["target_dir"], str) and bool(value["target_dir"]), "missing API target directory")
    expected = contract["checks"]
    ensure(type(value["expected_checks"]) is int and value["expected_checks"] == len(expected)
           and isinstance(value["checks"], list) and len(value["checks"]) == len(expected), "API matrix is incomplete")
    seen = set()
    logs = []
    invocations = set()
    for entry, check in zip(value["checks"], expected):
        ensure(isinstance(entry, dict) and set(entry) == CHECK_FIELDS, "API check fields differ")
        ensure(all(entry[key] == check[key] for key in ("package", "manifest", "mode", "flags")),
               "API package/feature matrix differs from immutable source")
        ensure(type(entry["exit_code"]) is int and entry["exit_code"] == 0, "API command did not succeed")
        ensure(type(entry["elapsed_seconds"]) in (int, float) and math.isfinite(entry["elapsed_seconds"])
               and entry["elapsed_seconds"] > 0, "invalid API elapsed time")
        invocations.add(verify_command(entry["command"], check))
        path = portable_log(report, entry["log"], allow_local=allow_local)
        ensure(path not in seen, "API checks reuse a raw log")
        seen.add(path)
        raw = bounded_read(path)
        ensure(isinstance(entry["log_sha256"], str) and SHA.fullmatch(entry["log_sha256"]) is not None
               and sha(raw) == entry["log_sha256"], "API log missing or digest differs")
        verify_log(raw, check)
        logs.append((entry, raw))
    ensure(len(invocations) == 1, "API checks changed checker/candidate/baseline roots")
    return logs


def verify_api_report(source_root: Path, report_path: Path, source_commit: str) -> dict:
    """Verify a portable report against the central release candidate identity."""
    value = load(report_path)
    verify_report(value, report_path, source_commit, source_contract(source_root, source_commit))
    return value


def package_api_report(source_root: Path, report_path: Path, source_commit: str, output: Path) -> Path:
    value = load(report_path)
    contract = source_contract(source_root, source_commit)
    logs = verify_report(value, report_path, source_commit, contract, allow_local=True)
    # Verify every input before creating output; never rewrite or relabel a failed report.
    output.mkdir(parents=False, exist_ok=False)
    (output / "logs").mkdir()
    for entry, raw in logs:
        name = f'logs/{entry["package"]}--{entry["mode"]}.log'
        with (output / name).open("xb") as stream:
            stream.write(raw)
        entry["log"] = name
    destination = output / "api-report.json"
    with destination.open("x") as stream:
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")
    verify_report(value, destination, source_commit, contract)
    return destination


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--package", type=Path)
    args = parser.parse_args()
    if args.package is not None:
        result = package_api_report(args.source_root, args.report, args.source_commit, args.package)
        print(result)
    else:
        value = verify_api_report(args.source_root, args.report, args.source_commit)
        print(f'API evidence: {value["expected_checks"]} comparisons verified for {args.source_commit}')
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError, KeyError, TypeError, subprocess.SubprocessError) as error:
        print(f"API evidence rejected: {error}", file=sys.stderr)
        raise SystemExit(1)
