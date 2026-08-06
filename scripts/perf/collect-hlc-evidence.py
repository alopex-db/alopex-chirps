#!/usr/bin/env python3
"""Convert Criterion's real estimates into versioned HLC evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import subprocess
import sys
from pathlib import Path


def source_digest(repo: Path) -> str:
    paths = [
        repo / "crates/chirps-wire/src/hlc.rs",
        repo / "crates/chirps-gossip-swim/src/hlc.rs",
        repo / "crates/chirps-gossip-swim/src/engine.rs",
        repo / "crates/alopex-chirps/benches/hlc_bench.rs",
    ]
    digest = hashlib.sha256()
    for path in paths:
        relative = path.relative_to(repo).as_posix().encode("utf-8")
        contents = path.read_bytes()
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        digest.update(len(contents).to_bytes(8, "big"))
        digest.update(contents)
    return digest.hexdigest()


def estimate(value: dict) -> dict:
    interval = value["confidence_interval"]
    return {
        "point": value["point_estimate"],
        "lower": interval["lower_bound"],
        "upper": interval["upper_bound"],
        "confidence_level": interval["confidence_level"],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--estimates", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        values = json.loads(args.estimates.read_text(encoding="utf-8"))
        mean = estimate(values["mean"])
        median = estimate(values["median"])
        threshold = 100.0
        rustc = subprocess.run(
            ["rustc", "--version"], check=True, text=True, capture_output=True
        ).stdout.strip()
        evidence = {
            "schema": "chirps.hlc-benchmark/v1",
            "release_version": "0.6.0",
            "source_tree_sha256": source_digest(args.repo_root.resolve()),
            "command": "cargo bench -p alopex-chirps --features hlc --bench hlc_bench -- --noplot",
            "host": {
                "kernel": platform.release(),
                "machine": platform.machine(),
                "rustc": rustc,
            },
            "benchmark": {
                "id": "local_hlc_tick",
                "unit": "ns",
                "mean": mean,
                "median": median,
                "threshold_upper_ns": threshold,
            },
            "result": "pass" if mean["upper"] <= threshold else "fail",
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    except (KeyError, OSError, ValueError, json.JSONDecodeError, subprocess.CalledProcessError) as exc:
        print(f"failed to collect HLC evidence: {exc}", file=sys.stderr)
        return 1
    print(f"HLC evidence written: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
