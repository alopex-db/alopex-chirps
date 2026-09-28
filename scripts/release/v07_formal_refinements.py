#!/usr/bin/env python3
"""Release-only existence gate for formal refinement/test references in Git objects.

The development catalog gate permits planned paths. This additional release
gate requires every declared production/test path at both exact candidates.
It establishes reference integrity, not that a test passes or proves a model.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys

CATALOGS = ("catalog.yaml", "subscription-catalog.yaml", "lifecycle-catalog.yaml", "metadata-catalog.yaml")
COMMIT = re.compile(r"[0-9a-f]{40}")


def git(root: Path, *args: str) -> bytes:
    return subprocess.check_output(["git", "-C", str(root), *args], timeout=30)


def requirement_references(text: str) -> list[dict]:
    if text.count("\nrequirements:\n") != 1:
        raise ValueError("catalog must contain one requirements section")
    section = text.split("\nrequirements:\n", 1)[1]
    blocks = re.findall(r"^  - .*?(?=^  - |\Z)", section, re.M | re.S)
    references = []
    seen = set()
    for block in blocks:
        match = re.search(r"\bid:\s*(V7-[A-Z]+-\d{3})\b", block)
        if match is None or match[1] in seen:
            raise ValueError("missing or duplicate requirement ID")
        requirement = match[1]
        seen.add(requirement)
        found = set()
        for kind in ("refinement", "planned_test", "planned_local_component_test"):
            matches = re.findall(r"\b" + kind + r":\s*\[([^\]]*)\]", block, re.S)
            if not matches:
                continue
            if len(matches) != 1:
                raise ValueError("duplicate reference list")
            entries = re.findall(r"\{([^{}]*)\}", matches[0])
            if not entries or re.sub(r"\{[^{}]*\}|[\s,]", "", matches[0]):
                raise ValueError("malformed reference list")
            found.add(kind)
            for entry in entries:
                repository = re.search(r"\brepository:\s*([a-z-]+)\b", entry)
                path = re.search(r"\bpath:\s*([^,}\s]+)", entry)
                task = re.search(r'\btask:\s*"([0-9.]+)"', entry)
                if repository is None or path is None or task is None:
                    raise ValueError("incomplete formal reference")
                repository, path, task = repository[1], path[1], task[1]
                pure = PurePosixPath(path)
                if repository not in {"chirps", "iggy-compatible"} or pure.is_absolute() or pure.as_posix() != path or ".." in pure.parts:
                    raise ValueError("unsafe formal reference")
                roots = {"formal", "crates", "tools", "tests", "scripts"} if repository == "chirps" else {"core", "tests"}
                if pure.parts[0] not in roots or len(pure.parts) < 2:
                    raise ValueError("formal reference outside repository roots")
                references.append(dict(requirement=requirement,kind=kind,repository=repository,path=path,task=task))
        if "refinement" not in found or not found.intersection({"planned_test", "planned_local_component_test"}):
            raise ValueError("requirement lacks production or test reference")
    if not seen:
        raise ValueError("empty catalog requirement inventory")
    return references


def source_files(root: Path, commit: str) -> set[str]:
    if not isinstance(commit, str) or COMMIT.fullmatch(commit) is None:
        raise ValueError("full immutable commit required")
    if git(root, "rev-parse", f"{commit}^{{commit}}").decode().strip() != commit:
        raise ValueError("source commit is missing")
    result = set()
    for record in git(root, "ls-tree", "-r", "-z", commit).split(b"\0"):
        if not record:
            continue
        metadata, name = record.split(b"\t", 1)
        mode, kind, _ = metadata.decode().split()
        if kind == "blob" and mode in {"100644", "100755"}:
            result.add(name.decode())
    return result


def verify_refinements(chirps_root: Path, chirps_commit: str, iggy_root: Path, iggy_commit: str) -> dict:
    files = {"chirps": source_files(chirps_root, chirps_commit), "iggy-compatible": source_files(iggy_root, iggy_commit)}
    catalogs = {}
    references = []
    for name in CATALOGS:
        path = f"formal/chirps-durable/{name}"
        raw = git(chirps_root, "show", f"{chirps_commit}:{path}")
        catalogs[path] = hashlib.sha256(raw).hexdigest()
        for ref in requirement_references(raw.decode()):
            if ref["path"] not in files[ref["repository"]]:
                raise ValueError(f'absent release refinement: {ref["repository"]}:{ref["path"]} ({ref["requirement"]})')
            references.append({"catalog":path,**ref})
    return {"schema":"chirps.formal-refinements/v1", "source_commit":chirps_commit,
            "iggy_commit":iggy_commit,"catalogs":catalogs,"references":references,"status":"pass"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("chirps-root", "iggy-root"):
        parser.add_argument("--"+name, type=Path, required=True)
    for name in ("chirps-commit", "iggy-commit"):
        parser.add_argument("--"+name, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    value = verify_refinements(args.chirps_root,args.chirps_commit,args.iggy_root,args.iggy_commit)
    text = json.dumps(value,indent=2,sort_keys=True)+"\n"
    if args.output:
        with args.output.open("x") as stream:
            stream.write(text)
    else:
        print(text,end="")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        print(f"Formal refinement evidence rejected: {error}",file=sys.stderr)
        raise SystemExit(1)
