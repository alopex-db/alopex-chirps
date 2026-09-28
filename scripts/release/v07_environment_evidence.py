#!/usr/bin/env python3
"""Bind already-replayed E2E/PERF observations to the frozen environment."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re

from v07_e2e_evidence import TARGETS, resolve_reference

SCHEMA = "chirps.v0.7.environment/v1"
# Bound reads before decoding; these are metadata plus existing PERF observations.
MAX_BYTES = 256 * 1024 * 1024


def ensure(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def load(path: Path) -> tuple[dict, str]:
    ensure(not path.is_symlink(), "environment evidence may not be a symlink")
    with path.open("rb") as stream:
        raw = stream.read(MAX_BYTES + 1)
    ensure(len(raw) <= MAX_BYTES, "environment evidence exceeds 256 MiB")
    def reject_constant(value):
        raise ValueError(f"non-finite JSON constant: {value}")
    value = json.loads(raw, parse_constant=reject_constant)
    ensure(isinstance(value, dict), "environment evidence must be an object")
    return value, hashlib.sha256(raw).hexdigest()


def equal(left: object, right: object) -> bool:
    # Unlike Python equality, this does not conflate true/1 or 1.0/1.
    return json.dumps(left, sort_keys=True, allow_nan=False) == json.dumps(right, sort_keys=True, allow_nan=False)


def verify_environment(
    candidate_path: Path,
    manifest_path: Path,
    production_lane: Path,
    fault_lane: Path,
    paired_path: Path,
) -> None:
    """Identity hook, additional to complete E2E and Rust PERF semantic replay.

    No API/wire host constraint is introduced. The environment manifest is fixed
    before candidate freeze and contains no references to candidate-bound results.
    """
    candidate, candidate_hash = load(candidate_path)
    manifest, manifest_hash = load(manifest_path)
    ensure(set(manifest) == {"schema", "source_commit", "iggy_commit", "e2e_environment", "performance_axes"}
           and manifest["schema"] == SCHEMA, "environment manifest shape differs")
    ensure(candidate.get("environment_sha256") == manifest_hash,
           "candidate environment digest differs from manifest bytes")
    for field in ("source_commit", "iggy_commit"):
        ensure(isinstance(manifest[field], str) and re.fullmatch(r"[0-9a-f]{40}", manifest[field]) is not None
               and manifest[field] == candidate.get(field), "environment source differs from candidate")
    environment_path = resolve_reference(manifest_path.parent, manifest["e2e_environment"])
    environment, environment_hash = load(environment_path)
    ensure(set(environment) == {"system", "release", "machine", "node", "rustc", "cargo"}
           and all(isinstance(value, str) and value for value in environment.values()),
           "frozen E2E environment observation is incomplete")
    axes = manifest["performance_axes"]
    performance = candidate.get("performance")
    ensure(isinstance(axes, dict) and axes and isinstance(performance, dict)
           and equal(axes, performance.get("axes")),
           "frozen PERF axes differ from candidate plan")

    for lane_name, lane_path in (("production", production_lane), ("fault", fault_lane)):
        lane, _ = load(lane_path)
        ensure(lane.get("lane") == lane_name and isinstance(lane.get("targets"), dict)
               and set(lane["targets"]) == set(TARGETS[lane_name]), "environment E2E lane inventory differs")
        for target in TARGETS[lane_name]:
            report_path = resolve_reference(lane_path.parent, lane["targets"][target])
            report, _ = load(report_path)
            ensure(report.get("source", {}).get("source_commit") == candidate["source_commit"]
                   and report.get("server", {}).get("source_commit") == candidate["iggy_commit"],
                   "environment E2E observation belongs to another source")
            observed_path = resolve_reference(report_path.parent, report["environment"])
            observed, observed_hash = load(observed_path)
            ensure(observed_hash == environment_hash and equal(observed, environment),
                   "E2E actual environment differs from frozen manifest")

    ensure(paired_path.name == "paired.json" and paired_path.parent.name == "paired",
           "environment hook requires paired/paired.json")
    perf_root = paired_path.parent.parent
    documents = {}
    for relative, schema in (
        ("aa/aa.json", "chirps.durable-perf-aa/v1"),
        ("aa/bounds.json", "chirps.durable-perf-bounds/v1"),
        ("safety/safety.json", "chirps.durable-perf-safety/v1"),
        ("safety/freeze.json", "chirps.durable-perf-freeze/v1"),
        ("paired/paired.json", "chirps.durable-perf-paired/v1"),
    ):
        path = perf_root / relative
        ensure(not perf_root.is_symlink() and not path.parent.is_symlink(),
               "PERF evidence may not traverse a symlink")
        document, _ = load(path)
        ensure(document.get("schema") == schema and document.get("candidate_sha256") == candidate_hash,
               "PERF environment observation belongs to another candidate")
        ensure(equal(document.get("axes"), axes), "PERF artifact axes differ from frozen environment")
        documents[relative] = document
    aa = documents["aa/aa.json"]
    paired = documents["paired/paired.json"]
    controls = documents["safety/safety.json"].get("controls")
    ensure(isinstance(controls, list) and controls, "PERF safety environment observations missing")
    groups = [aa.get("left"), aa.get("right"), paired.get("direct"), paired.get("full"),
              [control.get("observation") for control in controls if isinstance(control, dict)]]
    ensure(len(groups[-1]) == len(controls), "malformed PERF safety environment observation")
    for group in groups:
        ensure(isinstance(group, list) and group, "PERF environment observations missing")
        for observation in group:
            ensure(isinstance(observation, dict) and all(equal(observation.get(key), axes)
                   for key in ("axes", "observed_axes_begin", "observed_axes_finish")),
                   "PERF actual environment differs from frozen manifest")
