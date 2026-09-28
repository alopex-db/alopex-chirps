#!/usr/bin/env python3
"""Require all public API, wire, official, and compatible-server evidence cells."""
from __future__ import annotations
import argparse
from pathlib import Path
import re
import sys

sys.dont_write_bytecode = True
from v07_api_evidence import verify_api_report
from v07_e2e_evidence import reference, verify_lane, write_new
from v07_official_evidence import ensure, load
from v07_official_run import resolve, verify as verify_official
from v07_wire_evidence import verify_report as verify_wire

SCHEMA = "chirps.v0.7.compatibility-matrix/v1"
CELLS = {"api", "wire", "official", "production", "fault"}


def verify(root: Path, path: Path, source_commit: str, iggy_commit: str) -> dict:
    ensure(all(isinstance(commit,str) and re.fullmatch(r"[0-9a-f]{40}",commit) is not None
               for commit in (source_commit,iggy_commit)), "compatibility requires full immutable commits")
    value = load(path)
    ensure(set(value) == {"schema", "source_commit", "iggy_commit", "cells", "result"}
        and value["schema"] == SCHEMA and value["result"] == "pass", "compatibility matrix fields differ")
    ensure(value["source_commit"] == source_commit and value["iggy_commit"] == iggy_commit,
           "compatibility matrix identifies another candidate")
    ensure(isinstance(value["cells"],dict) and set(value["cells"]) == CELLS,
           "compatibility matrix is missing mandatory cells")
    cells = {key:resolve(path.parent,ref) for key,ref in value["cells"].items()}
    verify_api_report(root,cells["api"],source_commit)
    wire = verify_wire(root,cells["wire"],source_commit)
    verify_official(root,cells["official"],source_commit,require_public=True)
    production = verify_lane(cells["production"],"production",source_commit,iggy_commit)
    fault = verify_lane(cells["fault"],"fault",source_commit,iggy_commit)
    ensure(production["source"] == fault["source"] == wire["source"],
           "compatible lanes differ from candidate Git tree or lock")
    ensure(all(production[key]["sha256"] == fault[key]["sha256"] for key in ("corpus","environment")),
           "compatible lanes mix corpus or environment identities")
    ensure(production["server"]["binary_sha256"] != fault["server"]["binary_sha256"]
        and production["server"]["manifest_sha256"] != fault["server"]["manifest_sha256"],
        "production and fault lanes substituted the same server artifact")
    return value


def seal(root: Path, output: Path, source_commit: str, iggy_commit: str, cells: dict) -> None:
    # All reports/logs must already be portable within this evidence directory.
    value = {"schema":SCHEMA,"source_commit":source_commit,"iggy_commit":iggy_commit,
        "cells":{key:reference(path.resolve(),output.parent.resolve()) for key,path in cells.items()},"result":"pass"}
    scratch = output.with_name(output.name + ".validation")
    write_new(scratch,value)
    try:
        verify(root,scratch,source_commit,iggy_commit)
        write_new(output,value)
    finally:
        scratch.unlink()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root",required=True,type=Path)
    parser.add_argument("--source-commit",required=True)
    parser.add_argument("--iggy-commit",required=True)
    operation = parser.add_mutually_exclusive_group(required=True)
    operation.add_argument("--report",type=Path)
    operation.add_argument("--output",type=Path)
    for key in sorted(CELLS):
        parser.add_argument(f"--{key}",type=Path)
    args = parser.parse_args()
    if args.output:
        cells = {key:getattr(args,key) for key in CELLS}
        ensure(all(path is not None for path in cells.values()),"sealing requires all five cell reports")
        seal(args.source_root,args.output,args.source_commit,args.iggy_commit,cells)
    else:
        verify(args.source_root,args.report,args.source_commit,args.iggy_commit)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError,OSError,KeyError,TypeError) as error:
        print(f"compatibility matrix rejected: {error}",file=sys.stderr)
        raise SystemExit(1)
