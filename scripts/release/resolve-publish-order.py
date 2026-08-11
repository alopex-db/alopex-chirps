#!/usr/bin/env python3
"""Resolve the publish layers for the publishable workspace packages."""

from __future__ import annotations

import argparse
import sys
import tomllib
from pathlib import Path


def fail(message: str) -> None:
    raise ValueError(message)


def load_toml(path: Path) -> dict:
    try:
        with path.open("rb") as handle:
            value = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        fail(f"cannot read {path}: {exc}")
    return value


def dependency_names(value: object, path: tuple[str, ...] = ()) -> set[str]:
    """Collect dependency keys, including target-specific dependency tables."""
    if not isinstance(value, dict):
        return set()
    result: set[str] = set()
    if path and path[-1] in {"dependencies", "build-dependencies", "dev-dependencies"}:
        for key, dependency in value.items():
            target = dependency.get("package") if isinstance(dependency, dict) else None
            result.add(str(target or key))
    for key, child in value.items():
        if isinstance(child, dict):
            result.update(dependency_names(child, (*path, str(key))))
    return result


def workspace_members(root: Path, workspace: dict) -> list[Path]:
    members = workspace.get("workspace", {}).get("members", [])
    if not isinstance(members, list):
        fail("workspace.members must be an array")
    result: list[Path] = []
    for member in members:
        if not isinstance(member, str):
            fail("workspace.members contains a non-string entry")
        matches = sorted(root.glob(member)) if any(char in member for char in "*?[") else [root / member]
        if not matches:
            fail(f"workspace member does not exist: {member}")
        for path in matches:
            manifest = path / "Cargo.toml"
            if not manifest.is_file():
                fail(f"workspace member has no Cargo.toml: {member}")
            result.append(manifest)
    return result


def resolve(root: Path) -> list[list[str]]:
    workspace = load_toml(root / "Cargo.toml")
    workspace_version = workspace.get("workspace", {}).get("package", {}).get("version")
    if not isinstance(workspace_version, str):
        fail("workspace.package.version must be a string")
    packages: dict[str, dict] = {}
    all_packages: set[str] = set()
    for manifest in workspace_members(root, workspace):
        data = load_toml(manifest)
        package = data.get("package")
        if not isinstance(package, dict) or not isinstance(package.get("name"), str):
            continue
        name = package["name"]
        all_packages.add(name)
        if package.get("publish", True) is False:
            continue
        declared_version = package.get("version")
        if isinstance(declared_version, str) and declared_version != workspace_version:
            fail(f"publishable package {name} has version {declared_version}, expected workspace version {workspace_version}")
        packages[name] = {"manifest": manifest, "data": data}

    if not packages:
        fail("workspace contains no publishable packages")

    graph = {name: set() for name in packages}
    for name, record in packages.items():
        dependencies = dependency_names(record["data"])
        excluded = sorted(dependency for dependency in dependencies if dependency in all_packages and dependency not in packages)
        if excluded:
            fail(f"publishable package {name} depends on non-publishable workspace package(s): {', '.join(excluded)}")
        graph[name] = {dependency for dependency in dependencies if dependency in packages and dependency != name}

    layers: list[list[str]] = []
    remaining = {name: set(dependencies) for name, dependencies in graph.items()}
    while remaining:
        ready = sorted(name for name, dependencies in remaining.items() if not dependencies)
        if not ready:
            cycle = ", ".join(sorted(remaining))
            fail(f"workspace publish dependency cycle detected among: {cycle}")
        layers.append(ready)
        for name in ready:
            del remaining[name]
        for dependencies in remaining.values():
            dependencies.difference_update(ready)
    return layers


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, required=True)
    args = parser.parse_args()
    try:
        layers = resolve(args.repo_root.resolve())
    except ValueError as exc:
        print(f"publish order rejected: {exc}", file=sys.stderr)
        return 1
    for layer, packages in enumerate(layers, start=1):
        for package in packages:
            print(f"{layer}\t{package}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
