#!/usr/bin/env python3
"""Chirps v0.6.0 executable demo and evidence harness.

The harness follows the alopex-tool demo convention: every scene has a stable
name, command, exit status, duration, raw stdout/stderr files, and a compact
machine-readable result.  It is intentionally a functional demo runner, not a
benchmark; performance claims belong to the calibrated evidence workflow.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO = Path(__file__).resolve().parents[3]
SCENES = {
    "workspace-all-features": [
        "cargo",
        "test",
        "--locked",
        "--workspace",
        "--all-features",
        "--",
        "--test-threads=1",
    ],
    "wire": ["cargo", "test", "--locked", "-p", "alopex-chirps-wire", "--all-features", "--", "--test-threads=1"],
    "core": ["cargo", "test", "--locked", "-p", "alopex-chirps-core", "--all-features", "--", "--test-threads=1"],
    "raft-storage": [
        "cargo", "test", "--locked", "-p", "alopex-chirps-raft-storage", "--all-features", "--", "--test-threads=1"
    ],
    "gossip-swim": [
        "cargo", "test", "--locked", "-p", "alopex-chirps-gossip-swim", "--all-features", "--", "--test-threads=1"
    ],
    "transport-quic": [
        "cargo", "test", "--locked", "-p", "alopex-chirps-transport-quic", "--all-features", "--", "--test-threads=1"
    ],
    "quic-ignored": [
        "cargo", "test", "--locked", "-p", "alopex-chirps-transport-quic", "--test", "quic_integration", "--", "--ignored", "--test-threads=1"
    ],
    "raft-cluster": [
        "cargo", "test", "--locked", "-p", "alopex-chirps", "--all-features", "--test", "raft_cluster", "--", "--test-threads=1"
    ],
    "deterministic-harness": ["cargo", "test", "--locked", "-p", "chirps-deterministic-harness", "--", "--test-threads=1"],
    "perf-harness": ["cargo", "test", "--locked", "-p", "chirps-multi-raft-perf", "--", "--test-threads=1"],
    "three-node-mesh": [
        "cargo", "test", "--locked", "-p", "chirps-e2e", "--test", "three_node_mesh", "--", "--ignored", "--test-threads=1"
    ],
    "multinode-read-write": [
        "cargo",
        "run",
        "--locked",
        "-p",
        "chirps-v06-demo",
        "--",
        "--writes",
        "12",
    ],
    "multi-raft": [
        "cargo",
        "test",
        "--locked",
        "-p",
        "alopex-chirps",
        "--all-features",
        "--test",
        "multi_raft_three_voter",
        "--",
        "three_voters_elect_one_leader_and_commit_consistently",
        "--test-threads=1",
    ],
    "tso": [
        "cargo",
        "test",
        "--locked",
        "-p",
        "alopex-chirps",
        "--all-features",
        "--test",
        "tso_client",
        "--",
        "--test-threads=1",
    ],
    "snapshot": [
        "cargo",
        "test",
        "--locked",
        "-p",
        "alopex-chirps",
        "--all-features",
        "--test",
        "snapshot_transfer",
        "--",
        "--test-threads=1",
    ],
    "hlc-metrics": [
        "cargo",
        "test",
        "--locked",
        "-p",
        "alopex-chirps",
        "--all-features",
        "--test",
        "hlc_metrics",
        "--",
        "--test-threads=1",
    ],
    "file-transfer": [
        "cargo",
        "test",
        "--locked",
        "-p",
        "alopex-chirps",
        "--all-features",
        "--test",
        "mesh_file_transfer",
        "--",
        "--test-threads=1",
    ],
}


def run_scene(name: str, command: list[str], output: Path, writes: int) -> dict[str, Any]:
    if name == "multinode-read-write":
        command = [*command[:-1], str(writes)]
    started = time.monotonic()
    proc = subprocess.run(
        command,
        cwd=REPO,
        text=True,
        capture_output=True,
        env={**os.environ, "CARGO_TERM_COLOR": "never"},
    )
    elapsed = time.monotonic() - started
    stdout_path = output / f"{name}.stdout.log"
    stderr_path = output / f"{name}.stderr.log"
    stdout_path.write_text(proc.stdout, encoding="utf-8")
    stderr_path.write_text(proc.stderr, encoding="utf-8")
    result: dict[str, Any] = {
        "name": name,
        "command": command,
        "exit_code": proc.returncode,
        "ok": proc.returncode == 0,
        "elapsed_seconds": round(elapsed, 3),
        "stdout": str(stdout_path.relative_to(output)),
        "stderr": str(stderr_path.relative_to(output)),
    }
    if name == "multinode-read-write" and proc.returncode == 0:
        try:
            result["demo"] = json.loads(proc.stdout)
        except json.JSONDecodeError as exc:
            result["ok"] = False
            result["parse_error"] = str(exc)
    return result


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Chirps v0.6.0 デモ実行結果",
        "",
        f"- 実行時刻: `{report['started_at']}`",
        f"- commit: `{report['commit']}`",
        f"- host: `{report['host']}`",
        f"- overall: **{'PASS' if report['ok'] else 'FAIL'}**",
        "",
        "## シーン結果",
        "",
        "| シーン | 結果 | 秒 | 証跡 |",
        "| --- | --- | ---: | --- |",
    ]
    for scene in report["scenes"]:
        status = "PASS" if scene["ok"] else "FAIL"
        lines.append(
            f"| `{scene['name']}` | {status} | {scene['elapsed_seconds']:.3f} | "
            f"`{scene['stdout']}` / `{scene['stderr']}` |"
        )
    rw = next((s.get("demo") for s in report["scenes"] if s["name"] == "multinode-read-write"), None)
    if rw:
        lines.extend(
            [
                "",
                "## マルチノード read/write",
                "",
                f"- group: `{rw['group_id']}` / leader: `node-{rw['leader']}`",
                f"- write commit: `{rw['writes_committed']}/{rw['writes_requested']}`",
                f"- replica read consistency: **{rw['reads_consistent']}**",
                f"- key counts: `{json.dumps(rw['replica_key_counts'], ensure_ascii=False)}`",
                f"- scope: {rw['scope']}",
            ]
        )
    lines.extend(
        [
            "",
            "## 解釈上の注意",
            "",
            "このデモは機能・整合性の実行証跡であり、性能SLOの測定ではない。",
            "論理3ノードを1プロセス内のMockNetworkで実行しているため、物理3ノード、",
            "実ネットワーク障害、TiKVのRawKV/YCSB性能を主張しない。",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", choices=["all", "quick", *SCENES], default="all")
    parser.add_argument("--writes", type=int, default=12)
    parser.add_argument("--output", type=Path, default=Path("docs/demo/results/v0.6.0/latest"))
    args = parser.parse_args()
    args.output = (REPO / args.output).resolve() if not args.output.is_absolute() else args.output
    args.output.mkdir(parents=True, exist_ok=True)

    if args.scenario == "all":
        names = list(SCENES)
    elif args.scenario == "quick":
        names = ["multinode-read-write"]
    else:
        names = [args.scenario]
    report: dict[str, Any] = {
        "schema": "chirps-v0.6.0-demo-result-v1",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip(),
        "host": f"{platform.system()} {platform.machine()} {platform.release()}",
        "scenes": [],
    }
    for name in names:
        print(f"[demo] {name}: {' '.join(SCENES[name])}", flush=True)
        scene = run_scene(name, SCENES[name], args.output, args.writes)
        report["scenes"].append(scene)
        print(f"[demo] {name}: {'PASS' if scene['ok'] else 'FAIL'}", flush=True)
    report["ok"] = all(scene["ok"] for scene in report["scenes"])
    (args.output / "result.json").write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    (args.output / "RESULT.md").write_text(render_markdown(report), encoding="utf-8")
    print(args.output / "RESULT.md")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
