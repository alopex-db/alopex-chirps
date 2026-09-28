#!/usr/bin/env python3
"""Collector tests: synthetic bindings are never production measurements."""

import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import time
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location("audit", Path(__file__).with_name("chirps-durable-audit.py"))
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


def image(path, server, server_name="usr/local/bin/iggy-server"):
    def member(archive, name, raw):
        entry = tarfile.TarInfo(name)
        entry.size = len(raw)
        archive.addfile(entry, io.BytesIO(raw))

    layer = io.BytesIO()
    with tarfile.open(fileobj=layer, mode="w") as archive:
        member(archive, server_name, server)
    blobs = {}

    def blob(raw):
        digest = audit.digest_bytes(raw)
        blobs[f"blobs/sha256/{digest}"] = raw
        return {"digest": f"sha256:{digest}", "size": len(raw)}

    config = blob(audit.canonical({"config": {"Labels": {
        "org.alopex.chirps.artifact-kind": "production",
        "org.alopex.chirps.failpoints": "disabled",
        "org.alopex.chirps.server-sha256": audit.digest_bytes(server),
    }}}))
    manifest = blob(audit.canonical({"schemaVersion": 2, "config": config, "layers": [blob(layer.getvalue())]}))
    with tarfile.open(path, "w") as archive:
        member(archive, "index.json", audit.canonical({"manifests": [manifest]}))
        member(archive, "oci-layout", b'{"imageLayoutVersion":"1.0.0"}')
        for name, raw in blobs.items():
            member(archive, name, raw)
    return manifest["digest"]


class AuditTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name).resolve()

    def test_stat_parser_handles_parentheses_spaces_and_pid_reuse_field(self):
        rest = ["S"] + ["0"] * 18 + ["123456"] + ["0"] * 4
        raw = "77 (name (with spaces)) " + " ".join(rest)
        self.assertEqual(audit.parse_stat(raw), (123456, "S"))
        with self.assertRaises(ValueError):
            audit.parse_stat("bad")

    def test_json_publication_is_atomic_and_does_not_replace_prior_evidence(self):
        path = self.root / "result.json"
        original_link = os.link

        def inspect_link(source, target):
            self.assertFalse(Path(target).exists())
            self.assertEqual(json.loads(Path(source).read_bytes()), {"complete": True})
            return original_link(source, target)

        with patch.object(audit.os, "link", side_effect=inspect_link):
            audit.write_json(path, {"complete": True})
        with self.assertRaises(FileExistsError):
            audit.write_json(path, {"complete": False})
        self.assertEqual(json.loads(path.read_bytes()), {"complete": True})
        self.assertEqual(list(self.root.glob("*.tmp")), [])

    def test_disk_counts_allocated_bytes_and_deduplicates_hardlinks(self):
        directory = self.root / "data"
        directory.mkdir()
        path = directory / "one"
        path.write_bytes(b"x" * 8192)
        os.link(path, directory / "two")
        self.assertEqual(audit.disk_bytes([directory]), path.stat().st_blocks * 512)
        (directory / "link").symlink_to(path)
        with self.assertRaises(ValueError):
            audit.disk_bytes([directory])

    def test_oci_aliases_cannot_hide_the_effective_server(self):
        manifest = image(self.root / "good.tar", b"synthetic")
        actual, _ = audit.inspect_oci_server(self.root / "good.tar", manifest)
        self.assertEqual(actual, audit.digest_bytes(b"synthetic"))
        for index, name in enumerate(("usr//local/bin/iggy-server", "usr/local/bin/../bin/iggy-server", "/usr/local/bin/iggy-server", "././usr/local/bin/iggy-server")):
            with self.subTest(name=name):
                path = self.root / f"bad-{index}.tar"
                manifest = image(path, b"synthetic", name)
                with self.assertRaisesRegex(ValueError, "noncanonical"):
                    audit.inspect_oci_server(path, manifest)

    def test_pinned_file_cannot_change_or_traverse_symlinks(self):
        path = self.root / "file"
        path.write_bytes(b"one")
        ref = {"path": str(path), "sha256": audit.file_digest(path)}
        self.assertEqual(audit.checked_file(ref), path)
        path.write_bytes(b"two")
        with self.assertRaises(ValueError):
            audit.checked_file(ref)
        link = self.root / "link"
        link.symlink_to(path)
        with self.assertRaises(ValueError):
            audit.checked_file({"path": str(link), "sha256": audit.file_digest(path)})

    def test_oci_byte_and_entry_budgets_are_enforced(self):
        path = self.root / "image.tar"
        manifest = image(path, b"synthetic")
        with patch.object(audit.oci_artifact, "MAX_ARCHIVE_BYTES", 1), self.assertRaisesRegex(ValueError, "budget"):
            audit.inspect_oci_server(path, manifest)
        with patch.object(audit.oci_artifact, "MAX_ENTRIES", 1), self.assertRaisesRegex(ValueError, "budget"):
            audit.inspect_oci_server(path, manifest)

    def test_sampling_gap_is_a_failure_not_a_low_peak(self):
        directory = self.root / "observation"
        directory.mkdir()
        config_path = self.root / "config.json"
        config_path.write_text("{}")
        config = {"sample_interval_millis": 10, "max_observation_millis": 5000,
            "server": {"data_root": str(self.root)}, "checkpoint_root": str(self.root)}
        state = {"config_path": str(config_path), "config_sha256": audit.file_digest(config_path),
            "binding": {"client": {}, "server": {}}, "disk_bytes_before": 0}
        audit.write_json(directory / "state.json", state)
        with patch.object(audit, "load_config", return_value=config), patch.object(audit, "process_start", return_value=1), patch.object(audit, "rss_bytes", return_value=1000), patch.object(audit, "disk_bytes", return_value=0), patch.object(audit.time, "monotonic_ns", side_effect=[0, 1, 100_000_000]), patch.object(audit.time, "sleep"):
            audit.sample(directory / "state.json")
        result = audit.read_json(directory / "sampler-result.json")
        self.assertEqual(result["status"], "fail")
        self.assertIn("fell behind", result["error"])

    @unittest.skipUnless(sys.platform == "linux", "real /proc execution requires Linux")
    def test_real_sampler_binds_processes_and_detects_disk_and_rss(self):
        state, data, checkpoints = [self.root / name for name in ("state", "data", "checkpoints")]
        for path in (state, data, checkpoints):
            path.mkdir(mode=0o700)
        env = os.environ.copy()
        env["IGGY_SYSTEM_PATH"] = str(data)
        server = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(90)"], env=env)
        self.addCleanup(lambda: server.wait(timeout=5))
        self.addCleanup(server.terminate)
        identity = audit.process_identity(server.pid)
        manifest = self.root / "server.toml"
        manifest.write_text('[artifact]\nkind="production"\npublishable=true\noutput_sha256="' + identity["executable_sha256"] + '"\n[source]\ncommit="' + "1" * 40 + '"\n')
        configuration = self.root / "server-config.toml"
        configuration.write_text('# synthetic test process, not an Iggy benchmark\n')
        payload = self.root / "payload"
        payload.write_bytes(b"synthetic")
        partitions = self.root / "partitions.json"
        partitions.write_text('[0]')
        archive = self.root / "image.tar"
        image_digest = image(archive, (Path("/proc") / str(server.pid) / "exe").read_bytes())

        def ref(path):
            return {"path": str(path), "sha256": audit.file_digest(path)}

        config = {
            "schema": audit.SCHEMA, "state_root": str(state),
            "server": {**identity, "manifest": ref(manifest), "config": ref(configuration),
                "command_sha256": audit.digest_bytes((Path("/proc") / str(server.pid) / "cmdline").read_bytes()),
                "environment_sha256": audit.environment_digest(server.pid), "data_root": str(data)},
            "client_executable_sha256": audit.process_identity(os.getpid())["executable_sha256"],
            "checkpoint_root": str(checkpoints), "payload": ref(payload), "partition_set": ref(partitions),
            "image": {**ref(archive), "manifest_digest": image_digest},
            "planned_axes": {"full_confirmation_profile": "broker_accepted", "offered_load_per_second": 10,
                "warmup_millis": 100, "measure_millis": 100, "drain_millis": 100,
                "client_placement": "same-host", "execution_class": "loopback", "host_count": 1,
                "broker_count": 1, "replication_factor": 1},
            "sample_interval_millis": 100, "max_observation_millis": 30_000,
            "max_rss_bytes": 2**40, "max_disk_growth_bytes": 2**30, "hard_control_rss_bytes": 1,
            "inspector_sha256": audit.file_digest(Path(audit.oci_artifact.__file__)),
        }
        path = self.root / "collector.json"
        path.write_bytes(audit.canonical(config))
        config_hash = audit.file_digest(path)
        request = {"schema": audit.REQUEST, "action": "begin", "observation_id": "a" * 64,
            "phase": "paired_direct", "arm": "direct", "sample_index": 0, "safety_control": None,
            "completed_operations": 0, "payload_sha256": ref(payload)["sha256"],
            "client_pid": os.getpid(), "checkpoint_root": str(checkpoints)}
        begin = audit.audit(path, config_hash, request)
        self.assertIsNone(begin["metrics"])
        self.assertEqual(begin["observed_axes"]["payload_digest"], audit.file_digest(payload))
        (data / "growth").write_bytes(b"x" * 8192)
        time.sleep(0.25)
        finish = audit.audit(path, config_hash, {**request, "action": "finish", "completed_operations": 1})
        self.assertGreater(finish["metrics"]["peak_rss_bytes"], 0)
        self.assertGreaterEqual(finish["metrics"]["disk_growth_bytes"], 8192)
        self.assertFalse(finish["metrics"]["hard_resource_limit_exceeded"])
        self.assertEqual(begin["observed_axes"], finish["observed_axes"])
        with self.assertRaises(ValueError):
            audit.audit(path, config_hash, {**request, "action": "finish"})
        request.update(observation_id="b" * 64, phase="safety_hard_resource_limit", safety_control="hard_resource_limit")
        audit.audit(path, config_hash, request)
        time.sleep(0.25)
        finish = audit.audit(path, config_hash, {**request, "action": "finish"})
        self.assertTrue(finish["metrics"]["hard_resource_limit_exceeded"])

        # Deleting data before Finish must not erase an observed hard-limit
        # violation. Net growth and peak growth are distinct measurements.
        config["max_disk_growth_bytes"] = 1
        path.write_bytes(audit.canonical(config))
        config_hash = audit.file_digest(path)
        request.update(observation_id="d" * 64, phase="paired_direct", safety_control=None)
        audit.audit(path, config_hash, request)
        temporary = data / "temporary"
        temporary.write_bytes(b"y" * 8192)
        time.sleep(0.25)
        temporary.unlink()
        finish = audit.audit(path, config_hash, {**request, "action": "finish"})
        self.assertEqual(finish["metrics"]["disk_growth_bytes"], 0)
        self.assertTrue(finish["metrics"]["hard_resource_limit_exceeded"])

        # A correctly hashed config still cannot redirect disk measurement to
        # an unrelated empty directory or reuse the wrong server PID generation.
        for field, value in (("data_root", str(checkpoints)), ("start_time_ticks", identity["start_time_ticks"] + 1)):
            changed = copy.deepcopy(config)
            changed["server"][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                audit.observed_binding(changed, os.getpid())
        with self.assertRaises(ValueError):
            audit.audit(path, config_hash, {**request, "observation_id": "c" * 64, "checkpoint_root": str(data)})
        request.update(observation_id="e" * 64)
        audit.audit(path, config_hash, request)
        with self.assertRaisesRegex(ValueError, "another observation"):
            audit.audit(path, config_hash, {**request, "action": "finish", "phase": "wrong_phase"})
        audit.wait_for(state / request["observation_id"] / "sampler-result.json", time.monotonic() + 5)
        self.assertTrue((state / request["observation_id"] / "stop").exists())
        self.assertFalse((state / request["observation_id"] / "finish.json").exists())


if __name__ == "__main__":
    unittest.main()
