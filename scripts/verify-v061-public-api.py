#!/usr/bin/env python3
"""Check the eight pre-existing crates against the immutable v0.6.1 API."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import tomllib

BASELINE = "3ff0ce6a631fd235fc4a3e3e08c8a9665f3d8bd9"
TOOL_VERSION = "cargo-semver-checks 0.50.0"
CRATES = (
    "chirps-wire", "chirps-core", "chirps-gossip-swim", "chirps-mock",
    "chirps-transport-quic", "chirps-file-transfer", "chirps-raft-storage",
    "alopex-chirps",
)


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def features(manifest: dict) -> set[str]:
    """Include Cargo's implicit optional-dependency features, too."""
    declared = manifest.get("features", {})
    hidden = {
        value[4:] for values in declared.values() for value in values
        if value.startswith("dep:")
    }
    tables = [manifest.get("dependencies", {})]
    tables += [target.get("dependencies", {}) for target in manifest.get("target", {}).values()]
    optional = {
        name for table in tables for name, spec in table.items()
        if isinstance(spec, dict) and spec.get("optional") and name not in hidden
    }
    return (set(declared) | optional) - {"default"}


def reject_overrides(manifest: dict) -> None:
    for scope in ("workspace", "package"):
        if "cargo-semver-checks" in manifest.get(scope, {}).get("metadata", {}):
            raise ValueError("release API gate does not permit cargo-semver-checks lint overrides")


def matrix(root: Path) -> list[dict]:
    if git(root, "rev-parse", f"{BASELINE}^{{commit}}") != BASELINE:
        raise ValueError("immutable v0.6.1 baseline commit is missing")
    reject_overrides(tomllib.loads((root / "Cargo.toml").read_text()))
    checks = []
    for directory in CRATES:
        path = f"crates/{directory}/Cargo.toml"
        old = tomllib.loads(git(root, "show", f"{BASELINE}:{path}"))
        new = tomllib.loads((root / path).read_text())
        reject_overrides(new)
        package = old["package"]["name"]
        if new["package"]["name"] != package:
            raise ValueError(f"baseline package renamed: {package}")
        before, after = features(old), features(new)
        if before - after:
            raise ValueError(f"public features removed from {package}: {sorted(before - after)}")
        modes = [
            ("all", ["--all-features"]),
            ("default", ["--default-features"]),
            ("no-default", ["--only-explicit-features"]),
        ]
        for feature in sorted(before | after):
            flags = ["--only-explicit-features", "--current-features", feature]
            if feature in before:
                flags += ["--baseline-features", feature]
            modes.append((f"feature-{feature}", flags))
        for mode, flags in modes:
            checks.append({"package": package, "mode": mode, "flags": flags})
    return checks


def command(tool: str, root: Path, check: dict) -> list[str]:
    # 0.6 -> 0.7 ordinarily permits breaking changes. Force a compatible minor
    # comparison so the version bump cannot turn this release contract into a skip.
    return [tool, "semver-checks", "--manifest-path", str(root / "Cargo.toml"),
            "--package", check["package"], "--baseline-rev", BASELINE,
            "--release-type", "minor", "--color", "never", *check["flags"]]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="new evidence JSON path outside the checkout")
    parser.add_argument("--plan", action="store_true", help="print matrix only; not release evidence")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    checks = matrix(root)
    if args.plan:
        print(json.dumps({"baseline": BASELINE, "tool": TOOL_VERSION, "checks": checks}, indent=2))
        return 0
    if args.output is None:
        parser.error("--output is required unless --plan is used")
    output = args.output.resolve()
    if output.is_relative_to(root):
        parser.error("evidence output must be outside the source checkout")
    if git(root, "status", "--porcelain", "--untracked-files=all"):
        raise ValueError("commit the candidate first: API release evidence requires a clean checkout")
    tool = os.environ.get("SEMVER_CHECKS_BIN") or shutil.which("cargo-semver-checks")
    if not tool:
        raise ValueError(f"install {TOOL_VERSION}; see docs/v061-public-api-gate.md")
    version = subprocess.check_output([tool, "semver-checks", "--version"], text=True).strip()
    if version != TOOL_VERSION:
        raise ValueError(f"expected {TOOL_VERSION}, found {version}")
    if output.exists() or output.with_suffix(".logs").exists():
        raise ValueError("evidence output or log directory already exists; choose a fresh path")
    output.parent.mkdir(parents=True, exist_ok=True)
    logs = output.with_suffix(".logs")
    logs.mkdir()
    env = os.environ.copy()
    # Do not let ad hoc cfgs hide public items from the release API comparison.
    for name in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTDOCFLAGS", "CARGO_ENCODED_RUSTDOCFLAGS"):
        env.pop(name, None)
    env.setdefault("CARGO_BUILD_JOBS", "2")
    env.setdefault("CARGO_PROFILE_DEV_DEBUG", "0")
    env.setdefault("CARGO_TARGET_DIR", str(output.parent / "v061-api-target"))
    report = {
        "schema_version": 1, "baseline_commit": BASELINE,
        "candidate_commit": git(root, "rev-parse", "HEAD"), "tool": version,
        "rustc": subprocess.check_output(["rustc", "--version", "--verbose"], text=True).strip(),
        "candidate_lock_sha256": hashlib.sha256((root / "Cargo.lock").read_bytes()).hexdigest(),
        "target_dir": env["CARGO_TARGET_DIR"], "expected_checks": len(checks),
        "status": "running", "checks": [],
    }
    output.write_text(json.dumps(report, indent=2) + "\n")
    for check in checks:
        log = logs / f'{check["package"]}--{check["mode"]}.log'
        argv = command(tool, root, check)
        print(f'Checking {check["package"]}: {check["mode"]}', flush=True)
        start = time.monotonic()
        with log.open("w") as stream:
            result = subprocess.run(argv, cwd=root, env=env, stdout=stream, stderr=subprocess.STDOUT)
        entry = {**check, "command": argv, "exit_code": result.returncode,
                 "elapsed_seconds": round(time.monotonic() - start, 3),
                 "log": str(log), "log_sha256": hashlib.sha256(log.read_bytes()).hexdigest()}
        report["checks"].append(entry)
        output.write_text(json.dumps(report, indent=2) + "\n")
        if result.returncode:
            print(f'API check failed ({result.returncode}): {log}', file=sys.stderr)
    unchanged = (git(root, "rev-parse", "HEAD") == report["candidate_commit"]
                 and not git(root, "status", "--porcelain", "--untracked-files=all"))
    report["status"] = "pass" if unchanged and all(c["exit_code"] == 0 for c in report["checks"]) else "fail"
    report["candidate_unchanged"] = unchanged
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(f'Public API gate: {report["status"]}; {output}', flush=True)
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        print(f"API gate rejected: {error}", file=sys.stderr)
        raise SystemExit(1)
