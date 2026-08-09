"""Chirps v0.6.0 multi-node read/write notebook.

Run interactively with:
    marimo edit scripts/marimo/chirps_v06_multinode_rw.py
or headlessly with:
    marimo run scripts/marimo/chirps_v06_multinode_rw.py
"""

import json
import subprocess
from pathlib import Path

import marimo

app = marimo.App(width="medium")


@app.cell
def _():
    import pandas as pd
    import marimo as mo

    return mo, pd


@app.cell
def _(mo):
    writes = mo.ui.number(start=1, stop=1000, value=12, step=1, label="committed writes")
    mo.md("## Chirps v0.6.0: 3-node Multi-Raft read/write demo")
    return writes,


@app.cell
def _(writes):
    root = Path(__file__).resolve().parents[2]
    output = root / "docs/demo/results/v0.6.0/marimo"
    output.mkdir(parents=True, exist_ok=True)
    command = [
        "python3",
        str(root / "scripts/demo/v0_6/demo.py"),
        "--scenario",
        "multinode-read-write",
        "--writes",
        str(int(writes.value)),
        "--output",
        str(output),
    ]
    completed = subprocess.run(command, cwd=root, text=True, capture_output=True)
    report_path = output / "result.json"
    report = json.loads(report_path.read_text(encoding="utf-8")) if report_path.exists() else {
        "ok": False,
        "error": completed.stderr or completed.stdout,
    }
    return command, completed, report


@app.cell
def _(mo, report):
    if report.get("ok"):
        demo = next(scene["demo"] for scene in report["scenes"] if scene["name"] == "multinode-read-write")
        rows = [
            {"node": node, "keys_read": count}
            for node, count in demo["replica_key_counts"].items()
        ]
        mo.md(
            f"**PASS** — leader `node-{demo['leader']}`, "
            f"committed `{demo['writes_committed']}/{demo['writes_requested']}` writes, "
            f"consistent reads: `{demo['reads_consistent']}`"
        )
        return rows,
    mo.md(f"**FAIL** — {report.get('error', 'see result.json')}")
    return [],


@app.cell
def _(pd, rows):
    pd.DataFrame(rows)
    return


if __name__ == "__main__":
    app.run()
