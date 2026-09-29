#!/usr/bin/env python3
"""Replay credential/diagnostic controls and scan their retained runtime output."""
from __future__ import annotations

import argparse
import base64
import json
from pathlib import Path
import re
import subprocess

from v07_e2e_evidence import COMMIT, MAX_LOG_BYTES, digest, load, reference, resolve_reference, verify_target, write_new

SCHEMA = "chirps.v0.7.security-evidence/v1"
POLICY = "candidate-diagnostic-canaries-literal-base64-hex/v1"
CASES = ("filesystem", "processes", "resources", "test", "logs", "config", "all")


def expected_scenarios(lane: str) -> dict[str, str]:
    expected = {"artifact-kind-mismatch": "rejected", "artifact-digest-mismatch": "rejected"}
    if lane == "production":
        expected.update({f"bootstrap-missing-{index}": "rejected-secret-free" for index in range(6)})
        expected["runtime-migration"] = "rejected-secret-free"
        expected.update({f"matrix-{transport}-{case}": "authorized-admin-denied-runtime-restart-secret-free"
                         for transport in ("binary", "http") for case in CASES})
    elif lane == "fault":
        expected.update({f"collector-{transport}": "all-or-error" for transport in ("binary", "http")})
    else:
        raise ValueError("unknown security lane")
    return expected


def source_canaries(root: Path, commit: str) -> list[bytes]:
    if not isinstance(commit, str) or not COMMIT.fullmatch(commit):
        raise ValueError("invalid security source commit")
    values = []
    for path, names in (
        ("tests/e2e/src/lib.rs", ("ROOT_USERNAME", "ROOT_PASSWORD", "ADMIN_USERNAME", "ADMIN_PASSWORD", "RUNTIME_USERNAME", "RUNTIME_PASSWORD")),
        ("tests/e2e/tests/durable_bootstrap_security.rs", ("JWT_SECRET_CANARY",)),
    ):
        mode = subprocess.check_output(["git", "ls-tree", commit, "--", path], cwd=root, timeout=30)
        if not mode.startswith((b"100644 blob ", b"100755 blob ")):
            raise ValueError("security canaries must come from regular candidate Git files")
        raw = subprocess.check_output(["git", "show", f"{commit}:{path}"], cwd=root, timeout=30).decode()
        for name in names:
            matches = re.findall(rf'\bconst {name}: &str = "([^"\\\r\n]+)";', raw)
            if len(matches) != 1 or len(matches[0]) < 8:
                raise ValueError("candidate security canary inventory differs")
            value = matches[0].encode()
            values.extend((value, base64.b64encode(value), value.hex().encode()))
    if len(values) != len(set(values)):
        raise ValueError("candidate security canaries are not distinct")
    return values


def scan(raw: bytes, canaries: list[bytes]) -> None:
    if any(canary in raw for canary in canaries):
        # Deliberately omit the matching content from the error.
        raise ValueError("runtime evidence contains a credential canary")


def bounded_bytes(path: Path) -> bytes:
    if path.stat().st_size > MAX_LOG_BYTES:
        raise ValueError("security scan input exceeds size budget")
    with path.open("rb") as stream:
        raw = stream.read(MAX_LOG_BYTES + 1)
    if len(raw) > MAX_LOG_BYTES:
        raise ValueError("security scan input grew beyond size budget")
    return raw


def inspect(root: Path, source_commit: str, iggy_commit: str, reports: dict[str, Path]) -> dict:
    canaries = source_canaries(root, source_commit)
    scans = {}
    identities = []
    for lane in ("production", "fault"):
        path = reports[lane]
        report = verify_target(path, lane, "durable_diagnostics", source_commit, iggy_commit)
        scenarios = resolve_reference(path.parent, report["scenarios"])
        raw = bounded_bytes(scenarios)
        rows = [json.loads(line) for line in raw.splitlines() if line.strip()]
        names = [row["scenario"] for row in rows]
        if len(names) != len(set(names)) or any(row["target"] != "durable_diagnostics" for row in rows):
            raise ValueError("security scenarios are duplicated or belong to another target")
        if {row["scenario"]: row["verdict"] for row in rows} != expected_scenarios(lane):
            raise ValueError("security scenario inventory or result differs")
        run = resolve_reference(path.parent, report["logs"]["run"])
        scan(raw, canaries)
        scan(bounded_bytes(run), canaries)
        scans[lane] = {"scenarios_sha256": digest(scenarios), "runtime_log_sha256": digest(run),
                       "scenario_count": len(rows)}
        identities.append(report)
    if identities[0]["source"] != identities[1]["source"] or identities[0]["environment"]["sha256"] != identities[1]["environment"]["sha256"]:
        raise ValueError("security lanes differ in source or environment")
    if identities[0]["server"]["binary_sha256"] == identities[1]["server"]["binary_sha256"]:
        raise ValueError("security lanes must use distinct production and fault binaries")
    scan(b"ordinary diagnostic output", canaries)
    for canary in canaries:
        try:
            scan(b"negative control: " + canary, canaries)
        except ValueError:
            continue
        raise ValueError("security scanner failed its credential negative control")
    return {"policy": POLICY, "scans": scans, "negative_controls_detected": len(canaries)}


def verify(root: Path, path: Path, source_commit: str, iggy_commit: str) -> dict:
    report = load(path)
    if set(report) != {"schema", "source_commit", "iggy_commit", "production", "fault", "scan"} or report["schema"] != SCHEMA:
        raise ValueError("security report schema differs")
    if report["source_commit"] != source_commit or report["iggy_commit"] != iggy_commit:
        raise ValueError("security report belongs to another candidate")
    reports = {lane: resolve_reference(path.parent, report[lane]) for lane in ("production", "fault")}
    if report["scan"] != inspect(root, source_commit, iggy_commit, reports):
        raise ValueError("security scan differs from recomputed runtime evidence")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--iggy-commit", required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--report", type=Path)
    mode.add_argument("--output", type=Path)
    parser.add_argument("--production", type=Path)
    parser.add_argument("--fault", type=Path)
    args = parser.parse_args()
    if args.report:
        verify(args.source_root, args.report, args.source_commit, args.iggy_commit)
    else:
        if args.production is None or args.fault is None:
            parser.error("sealing requires both diagnostic target reports")
        reports = {"production": args.production, "fault": args.fault}
        scan_result = inspect(args.source_root, args.source_commit, args.iggy_commit, reports)
        value = {"schema": SCHEMA, "source_commit": args.source_commit, "iggy_commit": args.iggy_commit,
                 **{lane: reference(path, args.output.parent) for lane, path in reports.items()}, "scan": scan_result}
        write_new(args.output, value)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
