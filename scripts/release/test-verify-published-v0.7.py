#!/usr/bin/env python3
"""Negative fixtures for read-only publication verification; no service writes."""
import copy
import importlib.util
import io
import os
import json
import sys
from pathlib import Path
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("published", Path(__file__).with_name("verify-published-v0.7.py"))
v = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v)


class FixtureRemote(v.Remote):
    def __init__(self):
        self.object = "a" * 40
        self.commit = "b" * 40
        self.tag_name = "chirps-v0.7.0"
        self.tag_bytes = (f"{self.object}\trefs/tags/{self.tag_name}\n"
                          f"{self.commit}\trefs/tags/{self.tag_name}^{{}}\n").encode()
        self.crates = {f"crate-{i}": f"archive-{i}".encode() for i in range(9)}
        self.image = b'{"schemaVersion":2,"layers":[]}'
        self.release = {"id": 42, "tag_name": self.tag_name, "draft": False,
                        "prerelease": False, "published_at": "2026-09-28T00:00:00Z"}
        self.assets = [{"id": 7, "name": "release.zip", "state": "uploaded", "size": 7}]
        self.asset_bytes = {7: b"release"}
        self.calls = []
        self.bundle = {"github_repository": "owner/repo", "tag": self.tag_name,
                       "source_commit": self.commit,
                       "packages": [{"name": name, "version": "0.7.0", "size": len(raw),
                                     "sha256": v.sha256(raw)} for name, raw in self.crates.items()],
                       "production_image": {"reference": "ghcr.io/owner/image:0.7.0",
                                            "manifest_digest": "sha256:" + v.sha256(self.image)},
                       "github_assets": [{"name": "release.zip", "size": 7,
                                          "sha256": v.sha256(b"release")}]}

    def tag(self, repository, tag):
        self.calls.append(("tag", repository, tag))
        return self.tag_bytes

    def crate(self, name, version, expected_size):
        self.calls.append(("crate", name, version))
        if name not in self.crates:
            raise v.VerificationError("404")
        raw = self.crates[name]
        return len(raw), v.sha256(raw)

    def image_manifest(self, reference):
        self.calls.append(("image", reference))
        return self.image

    def github(self, path):
        self.calls.append(("github", path))
        if "/releases/tags/" in path:
            return copy.deepcopy(self.release)
        return copy.deepcopy(self.assets)

    def asset(self, repository, asset_id, expected_size):
        self.calls.append(("asset", repository, asset_id))
        raw = self.asset_bytes[asset_id]
        return len(raw), v.sha256(raw)


class PublishedVerificationTests(unittest.TestCase):
    def verify(self, remote):
        return v.verify_remote(remote.bundle, remote.object, remote)

    def test_draft_promotion_reads_all_asset_pages_and_rejects_extra_page(self):
        remote = FixtureRemote()
        remote.release["draft"] = True
        remote.release["published_at"] = None
        v.verify_promotion_assets(remote.bundle, remote)
        originals = [{"id": i + 1, "name": f"asset-{i}", "state": "uploaded", "size": 1} for i in range(100)]
        remote.bundle["github_assets"] = [{"name": item["name"], "size": 1, "sha256": v.sha256(b"x")} for item in originals]
        remote.asset_bytes = {item["id"]: b"x" for item in originals}
        def pages(path):
            if "/tags/" in path:
                return remote.release
            if path.endswith("&page=1"):
                return originals
            return [{"id": 101, "name": "unapproved-second-page", "state": "uploaded", "size": 1}]
        remote.github = pages
        with self.assertRaisesRegex(v.VerificationError, "inventory"):
            v.verify_promotion_assets(remote.bundle, remote)

    def test_exact_bundle_reads_all_nine_archives_and_assets(self):
        r = FixtureRemote()
        self.assertEqual(self.verify(r)["result"], "pass")
        self.assertEqual(len([c for c in r.calls if c[0] == "crate"]), 9)
        self.assertEqual(len([c for c in r.calls if c[0] == "asset"]), 1)
        self.assertEqual(len([c for c in r.calls if c[0] == "tag"]), 2)
        self.assertEqual(len([c for c in r.calls if c[0] == "image"]), 2)

    def test_each_crate_missing_or_changed_is_rejected(self):
        for index in range(9):
            for missing in (False, True):
                with self.subTest(index=index, missing=missing):
                    r = FixtureRemote()
                    if missing:
                        del r.crates[f"crate-{index}"]
                    else:
                        r.crates[f"crate-{index}"] = b"different"
                    with self.assertRaises(v.VerificationError):
                        self.verify(r)

    def test_tag_requires_pinned_annotated_object_and_exact_peel(self):
        r = FixtureRemote()
        invalid = [b"", r.tag_bytes.splitlines()[0] + b"\n",
                   r.tag_bytes.replace(b"a" * 40, b"c" * 40),
                   r.tag_bytes.replace(b"b" * 40, b"c" * 40),
                   r.tag_bytes + r.tag_bytes, b"malformed"]
        for raw in invalid:
            with self.subTest(raw=raw):
                r.tag_bytes = raw
                with self.assertRaises(v.VerificationError):
                    self.verify(r)

    def test_image_digest_checks_bytes_not_reported_metadata(self):
        r = FixtureRemote()
        r.image += b" "
        with self.assertRaisesRegex(v.VerificationError, "manifest differs"):
            self.verify(r)

    def test_release_must_be_published_exact_tag(self):
        for key, value in [("draft", True), ("prerelease", True),
                           ("tag_name", "another"), ("published_at", None), ("id", 0)]:
            with self.subTest(key=key):
                r = FixtureRemote()
                r.release[key] = value
                with self.assertRaises(v.VerificationError):
                    self.verify(r)

    def test_asset_missing_extra_duplicate_and_bad_metadata_rejected(self):
        variations = [[], [{"id": 8, "name": "unapproved"}],
                      [{"id": 7, "name": "release.zip"}] * 2]
        for key, value in [("state", "new"), ("size", 8), ("id", -1)]:
            item = dict(FixtureRemote().assets[0])
            item[key] = value
            variations.append([item])
        for assets in variations:
            with self.subTest(assets=assets):
                r = FixtureRemote()
                r.assets = assets
                with self.assertRaises(v.VerificationError):
                    self.verify(r)

    def test_same_size_asset_substitution_rejected(self):
        r = FixtureRemote()
        r.asset_bytes[7] = b"changed"
        with self.assertRaisesRegex(v.VerificationError, "asset bytes differ"):
            self.verify(r)

    def test_remote_failures_are_not_missing_success(self):
        for method in ["tag", "crate", "image_manifest", "github", "asset"]:
            with self.subTest(method=method):
                r = FixtureRemote()
                with patch.object(r, method, side_effect=v.VerificationError("service unavailable")):
                    with self.assertRaises(v.VerificationError):
                        self.verify(r)

    def test_mutable_tag_and_image_are_rechecked(self):
        for method in ["tag", "image_manifest"]:
            with self.subTest(method=method):
                r = FixtureRemote()
                original = r.tag_bytes if method == "tag" else r.image
                with patch.object(r, method, side_effect=[original, b"moved"]):
                    with self.assertRaises(v.VerificationError):
                        self.verify(r)

    def test_real_adapter_uses_read_only_commands(self):
        r = v.Remote()
        with patch.object(r, "command", return_value=b"{}") as command:
            r.tag("owner/repo", "chirps-v0.7.0")
            r.image_manifest("ghcr.io/owner/image:0.7.0")
            r.github("repos/owner/repo/releases/tags/chirps-v0.7.0")
        with patch.object(r, "command_digest", return_value=(7, v.sha256(b"release"))) as streamed:
            r.asset("owner/repo", 7, 7)
        calls = [call.args[0] for call in command.call_args_list] + [streamed.call_args.args[0]]
        self.assertEqual(calls[0][:3], ["git", "ls-remote", "--exit-code"])
        self.assertEqual(calls[1][:3], ["skopeo", "inspect", "--raw"])
        for args in calls[2:]:
            self.assertEqual(args[:4], ["gh", "api", "--method", "GET"])

    def test_asset_streaming_is_bounded_and_detects_failed_process(self):
        remote = v.Remote()
        command = [sys.executable, "-c", "import sys; sys.stdout.buffer.write(b'asset')"]
        self.assertEqual(remote.command_digest(command, 5), (5, v.sha256(b"asset")))
        with self.assertRaisesRegex(v.VerificationError, "exceeds stored size"):
            remote.command_digest(command, 4)
        with self.assertRaisesRegex(v.VerificationError, "download failed"):
            remote.command_digest([sys.executable, "-c", "raise SystemExit(1)"], 5)

    def test_registry_adapter_streams_get_and_rejects_oversize(self):
        for raw, expected_size, valid in [(b"crate", 5, True), (b"different", 5, False)]:
            response = io.BytesIO(raw)
            response.status = 200
            response.url = "https://static.crates.io/crates/example/example-0.7.0.crate"
            with patch.object(v.urllib.request, "urlopen", return_value=response) as get:
                if valid:
                    self.assertEqual(v.Remote().crate("example", "0.7.0", expected_size),
                                     (5, v.sha256(b"crate")))
                else:
                    with self.assertRaises(v.VerificationError):
                        v.Remote().crate("example", "0.7.0", expected_size)
                self.assertEqual(get.call_args.args[0].get_method(), "GET")
                self.assertEqual(get.call_args.args[0].full_url,
                                 "https://crates.io/api/v1/crates/example/0.7.0/download")

    def test_asset_pagination_is_not_truncated_at_first_page(self):
        r = FixtureRemote()
        pages = [[{"id": i} for i in range(100)], [{"id": 100}]]
        with patch.object(r, "github", side_effect=pages) as get:
            self.assertEqual(len(v.release_assets(r, "owner/repo", 42)), 101)
            self.assertIn("page=2", get.call_args.args[0])

    def test_release_asset_set_is_rechecked_without_download_counter_noise(self):
        for move in (False, True):
            r = FixtureRemote()
            final = copy.deepcopy(r.assets)
            final[0]["id" if move else "download_count"] = 99
            with patch.object(r, "github", side_effect=[r.release, r.assets, r.release, final]):
                if move:
                    with self.assertRaises(v.VerificationError):
                        self.verify(r)
                else:
                    self.assertEqual(self.verify(r)["result"], "pass")


def fixture_cli():
    # Test-only launcher: production CLI has no fixture-service bypass.
    sys.argv = sys.argv[2:]
    evidence_path = Path(sys.argv[sys.argv.index("--evidence") + 1])
    evidence = json.loads(evidence_path.read_bytes())
    bundle = json.loads((evidence_path.parent / evidence["bundle"]["path"]).read_bytes())
    entry = next(item for item in bundle["artifacts"] if item["id"] == "release-bundle")
    manifest_path = evidence_path.parent / entry["path"]
    manifest = json.loads(manifest_path.read_bytes())
    remote = FixtureRemote()
    remote.bundle = manifest
    remote.commit = manifest["source_commit"]
    remote.tag_name = manifest["tag"]
    remote.tag_bytes = (f"{remote.object}\trefs/tags/{remote.tag_name}\n"
                        f"{remote.commit}\trefs/tags/{remote.tag_name}^{{}}\n").encode()
    remote.crates = {item["name"]: (manifest_path.parent / item["path"]).read_bytes()
                     for item in manifest["packages"]}
    import tarfile
    with tarfile.open(manifest_path.parent / manifest["production_image"]["path"]) as archive:
        digest = manifest["production_image"]["manifest_digest"].removeprefix("sha256:")
        remote.image = archive.extractfile("blobs/sha256/" + digest).read()
    remote.release["tag_name"] = remote.tag_name
    remote.assets, remote.asset_bytes = [], {}
    for index, item in enumerate(manifest["github_assets"], 1):
        remote.assets.append({"id": index, "name": item["name"], "state": "uploaded", "size": item["size"]})
        remote.asset_bytes[index] = (manifest_path.parent / item["path"]).read_bytes()
    if os.environ.get("CHIRPS_TEST_REMOTE_DRIFT"):
        remote.crates[manifest["packages"][0]["name"]] = b"drift"
    v.Remote = lambda: remote
    v.main()


if __name__ == "__main__":
    if sys.argv[1:2] == ["--fixture-cli"]:
        fixture_cli()
    else:
        unittest.main()
