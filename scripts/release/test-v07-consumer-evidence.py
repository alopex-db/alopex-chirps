#!/usr/bin/env python3
"""Self-contained source-resolution/archive boundary tests; no registry writes."""
import copy
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
import os
from unittest.mock import patch
import v07_consumer_evidence as subject


def graph(enabled=True, patches=None):
    patches = patches or {}
    names = list(subject.PACKAGES) if enabled else [name for name in subject.PACKAGES if name != "alopex-chirps-backend-iggy"]
    names += sorted(subject.IGGY) if enabled else []
    packages = [{"id": "root", "name": "chirps-v07-exact-consumer", "version": "0.0.0", "source": None}]
    lock = []
    nodes = [{"id": "root", "deps": [{"pkg": name} for name in names]}]
    checksums = {name: "a" * 64 for name in subject.PACKAGES}
    for name in names:
        version = subject.VERSION if name in subject.PACKAGES else "0.10.0"
        source = None if name in patches else subject.REGISTRY
        packages.append({"id": name, "name": name, "version": version, "source": source,
            "manifest_path": str(patches.get(name, Path("/registry") / name) / "Cargo.toml")})
        nodes.append({"id": name, "deps": []})
        lock.append({"name": name, "version": version, "source": source, "checksum": checksums.get(name, "b" * 64)})
    return {"packages": packages, "resolve": {"root": "root", "nodes": nodes}}, {"package": lock}, checksums


def archive(path, name, extras=(), source_commit="a"*40):
    with tarfile.open(path, "w:gz") as output:
        content = f'[package]\nname = "{name}"\nversion = "0.7.0"\n'.encode()
        item = tarfile.TarInfo(f"{name}-0.7.0/Cargo.toml")
        item.size = len(content)
        output.addfile(item, io.BytesIO(content))
        directory = "alopex-chirps" if name == "alopex-chirps" else name.removeprefix("alopex-")
        content = json.dumps({"git":{"sha1":source_commit}, "path_in_vcs":f"crates/{directory}"}).encode()
        item = tarfile.TarInfo(f"{name}-0.7.0/.cargo_vcs_info.json")
        item.size = len(content)
        output.addfile(item, io.BytesIO(content))
        for item, content in extras:
            output.addfile(item, io.BytesIO(content))


def write_consumer_fixture(root, source_commit, bundle_path=None, phase="stored-archives"):
    """Synthetic Cargo records for boundary tests; not build/release evidence."""
    import shutil
    root.mkdir(parents=True, exist_ok=True)
    copied = root / "archives"
    copied.mkdir()
    bundle = {"source_commit": source_commit, "packages": []}
    if bundle_path:
        original, files = subject.archives(bundle_path)
    for name in subject.PACKAGES:
        path = copied / f"{name}-0.7.0.crate"
        if bundle_path:
            shutil.copyfile(files[name][0], path)
        else:
            archive(path, name, source_commit=source_commit)
        bundle["packages"].append(dict(name=name, version="0.7.0", path=path.relative_to(root).as_posix(), size=path.stat().st_size, sha256=subject.digest(path)))
    catalog_path = root / "package-set.json"
    catalog_path.write_text(json.dumps(bundle))
    patches = {name: Path("/synthetic/archives") / f"{name}-0.7.0" for name in subject.PACKAGES} if phase == "stored-archives" else {}
    checksums = {item["name"]: item["sha256"] for item in bundle["packages"]}
    modes = []
    for enabled in (False, True):
        mode = "feature-on" if enabled else "feature-off"
        directory = root / mode
        directory.mkdir()
        metadata, lock, _ = graph(enabled, patches)
        for item in lock["package"]:
            item["checksum"] = checksums.get(item["name"], "b"*64)
        (directory / "metadata.json").write_text(json.dumps(metadata))
        (directory / "Cargo.lock").write_text("\n".join("[[package]]\n" + "\n".join(f"{key} = {json.dumps(value)}" for key, value in item.items() if value is not None) for item in lock["package"]))
        (directory / "build.log").write_text("\n".join(map(json.dumps, [
            {"reason": "compiler-artifact", "target": {"name": "chirps-v07-exact-consumer"}, "executable": "/synthetic/consumer"},
            {"reason": "build-finished", "success": True}])))
        for name in ("metadata.json.stderr", "build.log.stderr"):
            (directory / name).write_text("synthetic fixture; not release evidence\n")
        features = ["--no-default-features"] + (["--features", "durable-iggy"] if enabled else [])
        record = {"mode": mode, "resolved": subject.validate_resolution(metadata, lock, patches, checksums, enabled),
            "commands": [["cargo", "metadata", "--format-version", "1", *features], ["cargo", "build", "--locked", "--message-format=json", *features]],
            "exit_codes": [0,0], "executable_sha256": "a"*64}
        for key, name in {"metadata_sha256":"metadata.json", "lock_sha256":"Cargo.lock", "build_log_sha256":"build.log", "metadata_stderr_sha256":"metadata.json.stderr", "build_stderr_sha256":"build.log.stderr"}.items():
            record[key] = subject.digest(directory / name)
        modes.append(record)
    report = dict(schema=subject.SCHEMA, phase=phase, source_commit=source_commit, package_set_sha256=subject.package_set_digest(bundle),
        catalog_sha256=subject.digest(catalog_path), archives=checksums, archive_locations={name:str(path) for name,path in patches.items()}, modes=modes, result="pass")
    report_path = root / "report.json"
    report_path.write_text(json.dumps(report))
    return report_path


class ConsumerEvidenceTests(unittest.TestCase):
    def test_full_reports_replay_both_phases_and_reject_cross_phase_or_tampering(self):
        with tempfile.TemporaryDirectory() as temporary:
            for phase in ("stored-archives", "registry"):
                root = Path(temporary) / phase
                report = write_consumer_fixture(root, "a"*40, phase=phase)
                subject.verify_report(root / "package-set.json", report, phase)
                with self.assertRaises(ValueError):
                    subject.verify_report(root / "package-set.json", report, "registry" if phase == "stored-archives" else "stored-archives")
                raw = json.loads(report.read_text())
                raw["modes"][0]["exit_codes"] = [False, 0]
                report.write_text(json.dumps(raw))
                with self.assertRaises(ValueError):
                    subject.verify_report(root / "package-set.json", report, phase)

    def test_cargo_children_do_not_receive_publication_credentials_or_home(self):
        with tempfile.TemporaryDirectory() as temporary, patch.dict(os.environ, GH_TOKEN="fixture-secret", GITHUB_TOKEN="fixture-secret", CARGO_REGISTRY_TOKEN="fixture-secret", CUSTOM_PASSWORD="fixture-secret", RUSTC_WRAPPER="/wrong/compiler"):
            environment = subject.consumer_environment(Path(temporary))
            self.assertFalse(any("TOKEN" in key or "PASSWORD" in key or "WRAPPER" in key for key in environment))
            self.assertNotEqual(environment["HOME"], str(Path.home()))
            self.assertIn("RUSTUP_HOME", environment)

    def test_archive_vcs_identity_cannot_be_rebound_by_rehashing_catalog(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            write_consumer_fixture(root, "a"*40)
            path = root / "package-set.json"
            catalog = json.loads(path.read_text())
            catalog["source_commit"] = "b"*40
            path.write_text(json.dumps(catalog))
            with self.assertRaisesRegex(ValueError, "candidate source"):
                subject.archives(path)

    def test_exact_registry_feature_graphs(self):
        for enabled in (False, True):
            metadata, lock, checksums = graph(enabled)
            result = subject.validate_resolution(metadata, lock, {}, checksums, enabled)
            self.assertEqual(len(result), 9 if enabled else 8)

    def test_stored_archives_are_explicit_paths_not_registry_claims(self):
        patches = {name: Path("/isolated/archives") / f"{name}-0.7.0" for name in subject.PACKAGES}
        metadata, lock, checksums = graph(True, patches)
        self.assertEqual(len(subject.validate_resolution(metadata, lock, patches, checksums, True)), 9)
        with self.assertRaises(ValueError):
            subject.validate_resolution(metadata, lock, {}, checksums, True)
        metadata["packages"][1]["manifest_path"] = "/workspace/Cargo.toml"
        with self.assertRaises(ValueError):
            subject.validate_resolution(metadata, lock, patches, checksums, True)

    def test_rejects_source_version_checksum_and_feature_drift(self):
        for field, value in (("source", "git+https://example.invalid/fork"), ("source", None), ("version", "0.6.3")):
            metadata, lock, checksums = graph()
            metadata["packages"][1][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                subject.validate_resolution(metadata, lock, {}, checksums, True)
        metadata, lock, checksums = graph()
        lock["package"][0]["checksum"] = "b" * 64
        with self.assertRaises(ValueError):
            subject.validate_resolution(metadata, lock, {}, checksums, True)
        for expected in (False, True):
            metadata, lock, checksums = graph(not expected)
            with self.assertRaises(ValueError):
                subject.validate_resolution(metadata, lock, {}, checksums, expected)

    def test_archive_extraction_rejects_escape_alias_link_and_identity(self):
        name = subject.PACKAGES[0]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            good = root / "good.crate"
            archive(good, name)
            self.assertEqual(subject.unpack(good, root / "good", name).name, f"{name}-0.7.0")
            for index, member_name in enumerate(("../escape", "/absolute", f"{name}-0.7.0/../../escape", f"{name}-0.7.0/Cargo.toml")):
                item = tarfile.TarInfo(member_name)
                bad = root / f"bad{index}.crate"
                archive(bad, name, [(item, b"")])
                with self.assertRaises(ValueError):
                    subject.unpack(bad, root / f"bad{index}", name)
            item = tarfile.TarInfo(f"{name}-0.7.0/link")
            item.type = tarfile.SYMTYPE
            item.linkname = "/outside"
            bad = root / "link.crate"
            archive(bad, name, [(item, b"")])
            with self.assertRaises(ValueError):
                subject.unpack(bad, root / "link", name)
            with self.assertRaises(ValueError):
                subject.unpack(good, root / "wrong", "another-name")

    def test_requires_real_successful_cargo_executable_records(self):
        good = '\n'.join(map(json.dumps, [
            {"reason": "compiler-artifact", "target": {"name": "chirps-v07-exact-consumer"}, "executable": "/tmp/consumer"},
            {"reason": "build-finished", "success": True},
        ]))
        self.assertEqual(subject.verify_build_output(good), Path("/tmp/consumer"))
        for raw in ("", good.replace('true', 'false'), good + '\n' + good):
            with self.assertRaises(ValueError):
                subject.verify_build_output(raw)

    def test_package_set_binding_does_not_introduce_candidate_hash_cycle(self):
        bundle = {"source_commit": "1" * 40, "packages": [{"name": "crate", "version": "0.7.0", "size": 3, "sha256": "a" * 64}]}
        digest = subject.package_set_digest(bundle)
        changed = copy.deepcopy(bundle)
        changed["candidate_sha256"] = "b" * 64
        self.assertEqual(digest, subject.package_set_digest(changed))
        changed["packages"][0]["sha256"] = "c" * 64
        self.assertNotEqual(digest, subject.package_set_digest(changed))

if __name__ == "__main__":
    unittest.main()
