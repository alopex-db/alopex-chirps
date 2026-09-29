#!/usr/bin/env python3
"""Build isolated consumers before upload and from the real registry after upload."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile
import tomllib
import urllib.request
import urllib.parse

VERSION = "0.7.0"
REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"
PACKAGES = (
    "alopex-chirps-wire", "alopex-chirps-raft-storage", "alopex-chirps-core",
    "alopex-chirps-gossip-swim", "alopex-chirps-mock", "alopex-chirps-transport-quic",
    "alopex-chirps-backend-iggy", "alopex-chirps-file-transfer", "alopex-chirps",
)
IGGY = {"iggy", "iggy_common", "iggy_binary_protocol"}
SCHEMA = "chirps.v0.7.consumer-evidence/v1"
MAX_ARCHIVE_BYTES = 128 * 1024 * 1024
MAX_EXPANDED_BYTES = 512 * 1024 * 1024


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def archives(bundle_path: Path) -> tuple[dict, dict[str, tuple[Path, str]]]:
    bundle = json.loads(bundle_path.read_text())
    if not re.fullmatch(r"[0-9a-f]{40}", bundle["source_commit"]):
        raise ValueError("archive source commit is invalid")
    items = bundle["packages"]
    if [item["name"] for item in items] != list(PACKAGES):
        raise ValueError("exact nine-package order is required")
    root = bundle_path.parent.resolve()
    result = {}
    for item in items:
        name = item["name"]
        relative = Path(item["path"])
        path = (root / relative).resolve()
        if relative.is_absolute() or ".." in relative.parts or not path.is_relative_to(root) or any((root.joinpath(*relative.parts[:i])).is_symlink() for i in range(1, len(relative.parts) + 1)):
            raise ValueError("archive path escapes bundle")
        if item["version"] != VERSION or path.name != f"{name}-{VERSION}.crate":
            raise ValueError("archive identity differs")
        if type(item["size"]) is not int or not 0 < item["size"] <= MAX_ARCHIVE_BYTES or path.stat().st_size != item["size"]:
            raise ValueError("archive size differs or exceeds budget")
        if not re.fullmatch(r"[0-9a-f]{64}", item["sha256"]) or digest(path) != item["sha256"]:
            raise ValueError("archive checksum differs")
        with tarfile.open(path, "r:gz") as archive:
            member = archive.getmember(f"{name}-{VERSION}/.cargo_vcs_info.json")
            if not member.isfile() or member.size > 8192:
                raise ValueError("archive source identity is missing or oversized")
            vcs = json.loads(archive.extractfile(member).read())
        directory = "alopex-chirps" if name == "alopex-chirps" else name.removeprefix("alopex-")
        if vcs.get("git", {}).get("sha1") != bundle["source_commit"] or vcs["git"].get("dirty", False) is not False or vcs.get("path_in_vcs") != f"crates/{directory}":
            raise ValueError("archive is not from the exact clean candidate source")
        result[name] = path, item["sha256"]
    return bundle, result


def package_set_digest(bundle: dict) -> str:
    value = {"source_commit": bundle["source_commit"], "packages": [
        {key: item[key] for key in ("name", "version", "size", "sha256")}
        for item in bundle["packages"]
    ]}
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def unpack(archive: Path, destination: Path, name: str) -> Path:
    prefix = f"{name}-{VERSION}"
    seen = set()
    total = 0
    with tarfile.open(archive, "r:gz") as source:
        members = source.getmembers()
        if len(members) > 100_000:
            raise ValueError("archive contains too many entries")
        for member in members:
            path = Path(member.name)
            if path.is_absolute() or ".." in path.parts or not path.parts or path.parts[0] != prefix:
                raise ValueError("archive member escapes its package")
            if not (member.isfile() or member.isdir()) or path.as_posix() in seen:
                raise ValueError("archive contains links, special files, or duplicate members")
            seen.add(path.as_posix())
            total += member.size
            if total > MAX_EXPANDED_BYTES:
                raise ValueError("expanded archive exceeds budget")
        for member in members:
            target = destination / member.name
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                with source.extractfile(member) as stream, target.open("xb") as output:
                    while chunk := stream.read(1024 * 1024):
                        output.write(chunk)
    package = destination / prefix
    manifest = tomllib.loads((package / "Cargo.toml").read_text())
    if manifest.get("package", {}).get("name") != name or manifest["package"].get("version") != VERSION:
        raise ValueError("normalized package manifest identity differs")
    if "workspace" in manifest or "patch" in manifest or "replace" in manifest:
        raise ValueError("packaged manifest contains workspace or dependency overrides")
    return package


def consumer_manifest(patches: dict[str, Path]) -> str:
    value = '''[package]
name = "chirps-v07-exact-consumer"
version = "0.0.0"
edition = "2024"
publish = false
[workspace]
[features]
default = []
durable-iggy = ["alopex-chirps/durable-iggy"]
[dependencies]
alopex-chirps = { version = "=0.7.0", default-features = false }
alopex-chirps-mock = "=0.7.0"
'''
    if patches:
        value += "[patch.crates-io]\n"
        value += "".join(f'{name} = {{ path = {json.dumps(str(path))} }}\n' for name, path in patches.items())
    return value


def validate_resolution(metadata: dict, lock: dict, patches: dict[str, Path], checksums: dict[str, str], enabled: bool) -> dict:
    packages = {item["id"]: item for item in metadata["packages"]}
    nodes = {item["id"]: item for item in metadata["resolve"]["nodes"]}
    pending = [metadata["resolve"]["root"]]
    active = set()
    while pending:
        identifier = pending.pop()
        if identifier in active:
            continue
        active.add(identifier)
        pending.extend(item["pkg"] for item in nodes[identifier]["deps"])
    names = {packages[item]["name"] for item in active}
    if enabled:
        if not IGGY <= names or "alopex-chirps-backend-iggy" not in names:
            raise ValueError("feature-on does not activate the real backend and pinned SDK")
    elif IGGY.intersection(names) or "alopex-chirps-backend-iggy" in names:
        raise ValueError("feature-off leaks the optional Iggy backend")
    required = set(PACKAGES) - (set() if enabled else {"alopex-chirps-backend-iggy"})
    if not required <= names:
        raise ValueError("consumer omits required public packages")
    locked = {(item["name"], item["version"]): item for item in lock["package"]}
    resolved = {}
    for identifier in active:
        item = packages[identifier]
        name = item["name"]
        if identifier == metadata["resolve"]["root"]:
            continue
        if name in PACKAGES:
            if item["version"] != VERSION:
                raise ValueError("consumer selected a different Chirps version")
            if patches:
                if item.get("source") is not None or Path(item["manifest_path"]).resolve() != (patches[name] / "Cargo.toml").resolve():
                    raise ValueError("stored archive consumer resolved a different source")
            elif item.get("source") != REGISTRY or locked[(name, VERSION)].get("checksum") != checksums[name]:
                raise ValueError("registry consumer source or checksum differs from stored archive")
            resolved[name] = checksums[name]
        elif item.get("source") != REGISTRY:
            raise ValueError("third-party path/git/alternate registry substitution")
        if name in IGGY and item["version"] != "0.10.0":
            raise ValueError("SDK version differs")
    return dict(sorted(resolved.items()))


def download(name: str, destination: Path, expected: str, fixture_registry: str | None = None) -> None:
    url = f"https://crates.io/api/v1/crates/{name}/{VERSION}/download" if fixture_registry is None else f"{fixture_registry}/{name}/{VERSION}"
    request = urllib.request.Request(url, headers={"User-Agent": "alopex-chirps-release-verification"})
    total = 0
    with urllib.request.urlopen(request, timeout=60) as source, destination.open("xb") as output:
        while chunk := source.read(1024 * 1024):
            total += len(chunk)
            if total > MAX_ARCHIVE_BYTES:
                raise ValueError("registry archive exceeds budget")
            output.write(chunk)
    if digest(destination) != expected:
        raise ValueError("actual crates.io bytes differ from stored archive")


def run(argv: list[str], cwd: Path, environment: dict, log: Path) -> str:
    with log.open("xb") as output, log.with_suffix(log.suffix + ".stderr").open("xb") as errors:
        completed = subprocess.run(argv, cwd=cwd, env=environment, stdout=output, stderr=errors, timeout=1800, check=False)
    if completed.returncode:
        raise ValueError(f"consumer command failed ({completed.returncode}); inspect {log.name}")
    return log.read_text()


def consumer_environment(root: Path) -> dict[str, str]:
    # Cargo invokes package build scripts: publication credentials must not leak.
    allowed = {"PATH", "SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT", "TMPDIR", "TEMP", "TMP", "LANG", "LC_ALL", "RUSTUP_HOME", "RUSTUP_TOOLCHAIN", "SSL_CERT_FILE", "SSL_CERT_DIR", "SDKROOT", "MACOSX_DEPLOYMENT_TARGET"}
    environment = {key: value for key, value in os.environ.items() if key.upper() in allowed}
    environment.setdefault("RUSTUP_HOME", str(Path.home() / ".rustup"))
    home = root / "home"
    home.mkdir()
    environment.update(HOME=str(home), USERPROFILE=str(home), CARGO_HOME=str(root / "cargo-home"), CARGO_TARGET_DIR=str(root / "target"), CARGO_BUILD_JOBS="2", CARGO_TERM_COLOR="never")
    return environment


def collect(bundle_path: Path, output: Path, phase: str, fixture_registry: str | None = None) -> None:
    if phase == "fixture-registry":
        parsed = urllib.parse.urlsplit(fixture_registry or "")
        if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"} or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("fixture registry must be an unauthenticated loopback HTTP endpoint")
    elif fixture_registry is not None:
        raise ValueError("production verification cannot use a fixture registry")
    bundle, stored = archives(bundle_path)
    output.mkdir(parents=True, exist_ok=False)
    checksums = {name: value[1] for name, value in stored.items()}
    copied = output / "archives"
    copied.mkdir()
    catalog = {"source_commit": bundle["source_commit"], "packages": []}
    for item in bundle["packages"]:
        target = copied / stored[item["name"]][0].name
        shutil.copyfile(stored[item["name"]][0], target)
        catalog["packages"].append({**{key: item[key] for key in ("name", "version", "sha256", "size")}, "path": target.relative_to(output).as_posix()})
    (output / "package-set.json").write_text(json.dumps(catalog, indent=2, sort_keys=True) + "\n")
    archives(output / "package-set.json")
    with tempfile.TemporaryDirectory(prefix="chirps-v07-consumer-") as temporary:
        root = Path(temporary)
        patches = {}
        if phase == "stored-archives":
            patches = {name: unpack(path, root / "archives", name) for name, (path, _) in stored.items()}
        else:
            for name, (_, expected) in stored.items():
                download(name, root / f"{name}.crate", expected, fixture_registry)
        consumer = root / "consumer"
        (consumer / "src").mkdir(parents=True)
        (consumer / "Cargo.toml").write_text(consumer_manifest(patches))
        (consumer / "src/main.rs").write_text('''fn main() {
    let _ = alopex_chirps::NodeId::new();
    let _ = std::any::TypeId::of::<alopex_chirps_mock::MockBackend>();
    #[cfg(feature = "durable-iggy")]
    let _ = std::any::TypeId::of::<alopex_chirps::durable::DurableConfig>();
}
''')
        environment = consumer_environment(root)
        records = []
        for mode, enabled in (("feature-off", False), ("feature-on", True)):
            directory = output / mode
            directory.mkdir()
            features = ["--no-default-features"] + (["--features", "durable-iggy"] if enabled else [])
            # Resolve freshly; the checked-in unpublished fixture lock is never used.
            metadata_cmd = ["cargo", "metadata", "--format-version", "1", *features]
            metadata = json.loads(run(metadata_cmd, consumer, environment, directory / "metadata.json"))
            lock_path = consumer / "Cargo.lock"
            lock = tomllib.loads(lock_path.read_text())
            resolution = validate_resolution(metadata, lock, patches, checksums, enabled)
            build_cmd = ["cargo", "build", "--locked", "--message-format=json", *features]
            build_output = run(build_cmd, consumer, environment, directory / "build.log")
            executable = verify_build_output(build_output)
            executable_sha256 = digest(executable)
            (directory / "Cargo.lock").write_bytes(lock_path.read_bytes())
            records.append({"mode": mode, "resolved": resolution, "commands": [metadata_cmd, build_cmd],
                "metadata_sha256": digest(directory / "metadata.json"), "lock_sha256": digest(directory / "Cargo.lock"), "build_log_sha256": digest(directory / "build.log"), "metadata_stderr_sha256": digest(directory / "metadata.json.stderr"), "build_stderr_sha256": digest(directory / "build.log.stderr"), "exit_codes": [0, 0], "executable_sha256": executable_sha256})
        if any(digest(path) != expected for path, expected in stored.values()):
            raise ValueError("stored archive changed during consumer verification")
        report = {"schema": SCHEMA, "phase": phase, "source_commit": bundle["source_commit"],
            "package_set_sha256": package_set_digest(bundle), "catalog_sha256": digest(output / "package-set.json"), "archives": checksums, "archive_locations": {name: str(path) for name, path in patches.items()}, "modes": records, "result": "pass"}
        with (output / "report.json").open("x") as stream:
            json.dump(report, stream, indent=2, sort_keys=True)
            stream.write("\n")


def verify_build_output(raw: str) -> Path:
    records = [json.loads(line) for line in raw.splitlines() if line.startswith("{")]
    finished = [item.get("success") for item in records if item.get("reason") == "build-finished"]
    executables = [Path(item["executable"]) for item in records if item.get("reason") == "compiler-artifact" and item.get("target", {}).get("name") == "chirps-v07-exact-consumer" and item.get("executable")]
    if finished != [True] or len(executables) != 1:
        raise ValueError("Cargo did not report one successful consumer executable")
    return executables[0]


def verify_report(bundle_path: Path, report_path: Path, phase: str) -> dict:
    bundle, stored = archives(bundle_path)
    report = json.loads(report_path.read_text())
    expected = {"schema", "phase", "source_commit", "package_set_sha256", "catalog_sha256", "archives", "archive_locations", "modes", "result"}
    if set(report) != expected or report["schema"] != SCHEMA or report["phase"] != phase or report["result"] != "pass":
        raise ValueError("consumer report identity or phase differs")
    checksums = {name: value[1] for name, value in stored.items()}
    if report["source_commit"] != bundle["source_commit"] or report["package_set_sha256"] != package_set_digest(bundle) or report["archives"] != checksums:
        raise ValueError("consumer report belongs to a different frozen archive bundle")
    catalog_path = report_path.parent / "package-set.json"
    if catalog_path.is_symlink() or digest(catalog_path) != report["catalog_sha256"]:
        raise ValueError("consumer package catalog bytes differ")
    catalog, _ = archives(catalog_path)
    if package_set_digest(catalog) != report["package_set_sha256"]:
        raise ValueError("consumer package catalog identities differ")
    locations = report["archive_locations"]
    if (phase == "stored-archives" and set(locations) != set(PACKAGES)) or (phase in {"registry", "fixture-registry"} and locations):
        raise ValueError("consumer dependency source mode differs")
    patches = {name: Path(path) for name, path in locations.items()}
    if any(not path.is_absolute() or path.name != f"{name}-{VERSION}" for name, path in patches.items()):
        raise ValueError("stored archive location identity differs")
    if [item.get("mode") for item in report["modes"]] != ["feature-off", "feature-on"]:
        raise ValueError("both consumer feature modes are required")
    for record in report["modes"]:
        directory = report_path.parent / record["mode"]
        fields = {"metadata_sha256": "metadata.json", "lock_sha256": "Cargo.lock", "build_log_sha256": "build.log", "metadata_stderr_sha256": "metadata.json.stderr", "build_stderr_sha256": "build.log.stderr"}
        if set(record) != {"mode", "resolved", "commands", "exit_codes", "executable_sha256", *fields} or record["exit_codes"] != [0, 0] or any(type(code) is not int for code in record["exit_codes"]):
            raise ValueError("consumer execution fields or exit statuses differ")
        if not re.fullmatch(r"[0-9a-f]{64}", record["executable_sha256"]):
            raise ValueError("consumer executable digest is missing")
        for key, name in fields.items():
            path = directory / name
            if directory.is_symlink() or path.is_symlink() or not re.fullmatch(r"[0-9a-f]{64}", record[key]) or digest(path) != record[key]:
                raise ValueError("consumer raw evidence bytes differ")
        enabled = record["mode"] == "feature-on"
        features = ["--no-default-features"] + (["--features", "durable-iggy"] if enabled else [])
        commands = [["cargo", "metadata", "--format-version", "1", *features], ["cargo", "build", "--locked", "--message-format=json", *features]]
        if record["commands"] != commands:
            raise ValueError("consumer command or feature mode was changed")
        metadata = json.loads((directory / "metadata.json").read_text())
        lock = tomllib.loads((directory / "Cargo.lock").read_text())
        resolved = validate_resolution(metadata, lock, patches, checksums, enabled)
        if record["resolved"] != resolved:
            raise ValueError("reported consumer graph differs from Cargo metadata")
        verify_build_output((directory / "build.log").read_text())
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    destination = parser.add_mutually_exclusive_group(required=True)
    destination.add_argument("--output", type=Path)
    destination.add_argument("--verify-report", type=Path)
    parser.add_argument("--phase", choices=("stored-archives", "registry", "fixture-registry"), required=True)
    parser.add_argument("--fixture-registry")
    args = parser.parse_args()
    try:
        if args.verify_report:
            verify_report(args.bundle.resolve(), args.verify_report.resolve(), args.phase)
        else:
            collect(args.bundle.resolve(), args.output.resolve(), args.phase, args.fixture_registry)
    except (ValueError, OSError, KeyError, subprocess.SubprocessError) as error:
        parser.exit(1, f"consumer verification rejected: {error}\n")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
