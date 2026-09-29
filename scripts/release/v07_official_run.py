#!/usr/bin/env python3
"""Bind an actual official probe execution to a clean candidate and its Cargo binary."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

sys.dont_write_bytecode = True
from v07_api_evidence import bounded_read, portable_log
from v07_e2e_evidence import command_output, digest, reference, run_bounded, source_identity, write_new
from v07_official_evidence import ensure, load, verify_report

SCHEMA = "chirps.v0.7.official-execution/v1"
BUILD = ["cargo", "build", "--locked", "-p", "chirps-e2e", "--example", "durable_official_interop", "--message-format=json"]


def executable(log: Path) -> Path:
    artifacts, finished = [], []
    for line in bounded_read(log).decode().splitlines():
        if not line.startswith("{"):
            continue
        record = json.loads(line)
        if record.get("reason") == "compiler-artifact" and record.get("target", {}).get("name") == "durable_official_interop":
            if record.get("target", {}).get("kind") == ["example"] and record.get("executable"):
                artifacts.append(Path(record["executable"]))
        if record.get("reason") == "build-finished":
            finished.append(record.get("success"))
    ensure(len(artifacts) == 1 and artifacts[0].is_absolute() and finished == [True],
           "Cargo did not report one successful official probe executable")
    return artifacts[0]


def resolve(root: Path, ref: dict) -> Path:
    ensure(isinstance(ref, dict) and set(ref) == {"path", "sha256"}, "invalid official evidence reference")
    path = portable_log(root / "execution.json", ref["path"])
    ensure(digest(path) == ref["sha256"], "official evidence bytes differ")
    return path


def verify(root: Path, path: Path, commit: str, *, require_public: bool = False) -> dict:
    ensure(isinstance(commit,str) and re.fullmatch(r"[0-9a-f]{40}",commit) is not None,
           "official execution requires a full candidate commit")
    value = load(path)
    ensure(set(value) == {"schema", "source", "commands", "exit_codes", "logs", "probe", "server_build_log",
        "client_binary_sha256", "rustc", "cargo", "result"}
        and value["schema"] == SCHEMA and value["result"] == "pass", "official execution fields or status differ")
    tree = command_output(root, "git", "rev-parse", f"{commit}^{{tree}}")
    lock = subprocess.check_output(["git", "show", f"{commit}:Cargo.lock"], cwd=root, timeout=30)
    ensure(value["source"] == {"source_commit":commit,"source_tree":tree,"lock_sha256":hashlib.sha256(lock).hexdigest()},
           "official execution source differs from candidate Git objects")
    run = ["<probe-binary>", "<official-manifest>", commit, "<new-observations-directory>"]
    ensure(value["commands"] == {"build":BUILD,"run":run}, "official probe command differs")
    ensure(set(value["exit_codes"]) == {"build","run"}
           and all(type(code) is int and code == 0 for code in value["exit_codes"].values()), "official probe failed or was not executed")
    ensure(set(value["logs"]) == {"build","run"}, "official execution raw logs are incomplete")
    executable(resolve(path.parent, value["logs"]["build"]))
    bounded_read(resolve(path.parent, value["logs"]["run"]))
    ensure(all(isinstance(value[key], str) and value[key] for key in ("rustc","cargo")), "missing official client tool provenance")
    probe = resolve(path.parent, value["probe"])
    observation = verify_report(probe, commit, value["client_binary_sha256"], require_public=require_public)
    manifest = load(probe.parent / "server-manifest.json")
    build_log = resolve(path.parent, value["server_build_log"])
    ensure(digest(build_log) == manifest["build_log_sha256"], "official server build log differs")
    ensure(bounded_read(build_log).strip(), "official server build log is empty")
    return observation


def collect(root: Path, output: Path, manifest: Path, server_build_log: Path, *, require_public: bool = False) -> None:
    root, output, manifest, server_build_log = (path.resolve() for path in (root,output,manifest,server_build_log))
    ensure(not output.is_relative_to(root), "store official evidence outside source checkout")
    source = source_identity(root)
    manifest_digest, log_digest = digest(manifest), digest(server_build_log)
    ensure(load(manifest)["build_log_sha256"] == log_digest, "server build log does not match official manifest")
    ensure(bounded_read(server_build_log).strip(), "official server build log is empty")
    output.mkdir(parents=True, exist_ok=False)
    shutil.copyfile(server_build_log, output / "server-build.log")
    run = ["<probe-binary>", "<official-manifest>", source["source_commit"], "<new-observations-directory>"]
    result = {"schema":SCHEMA, "source":source, "commands":{"build":BUILD,"run":run},
        "exit_codes":{}, "logs":{}, "probe":None, "server_build_log":reference(output/"server-build.log",output),
        "client_binary_sha256":"", "rustc":command_output(root,"rustc","--version","--verbose"),
        "cargo":command_output(root,"cargo","--version"), "result":"fail"}
    environment = dict(os.environ)
    environment["CARGO_TERM_COLOR"] = "never"
    try:
        for stage in ("build","run"):
            argv = BUILD if stage == "build" else [str(binary),str(manifest),source["source_commit"],str(output/"observations")]
            raw = output / f"{stage}.log"
            code = run_bounded(argv,root,raw,900 if stage == "build" else 90,env=environment)
            result["exit_codes"][stage] = code
            result["logs"][stage] = reference(raw,output)
            ensure(code == 0, f"official probe {stage} failed: {code}")
            if stage == "build":
                binary = executable(raw)
                result["client_binary_sha256"] = digest(binary)
        ensure(source_identity(root) == source and digest(binary) == result["client_binary_sha256"]
            and digest(manifest) == manifest_digest and digest(server_build_log) == log_digest,
            "official execution input changed during collection")
        probe = output / "observations/report.json"
        result["probe"] = reference(probe,output)
        verify_report(probe,source["source_commit"],result["client_binary_sha256"],require_public=require_public)
        result["result"] = "pass"
    finally:
        write_new(output / "execution.json",result)
    verify(root,output/"execution.json",source["source_commit"],require_public=require_public)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", required=True, type=Path)
    operation = parser.add_mutually_exclusive_group(required=True)
    operation.add_argument("--output", type=Path)
    operation.add_argument("--report", type=Path)
    parser.add_argument("--official-manifest",type=Path)
    parser.add_argument("--server-build-log",type=Path)
    parser.add_argument("--source-commit")
    parser.add_argument("--require-public", action="store_true")
    args = parser.parse_args()
    if args.output:
        ensure(args.official_manifest is not None and args.server_build_log is not None,
               "collection requires official manifest and original build log")
        collect(args.source_root,args.output,args.official_manifest,args.server_build_log,require_public=args.require_public)
    else:
        ensure(args.source_commit is not None,"verification requires immutable candidate commit")
        verify(args.source_root,args.report,args.source_commit,require_public=args.require_public)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError,OSError,KeyError,TypeError,subprocess.SubprocessError) as error:
        print(f"official execution rejected: {error}",file=sys.stderr)
        raise SystemExit(1)
