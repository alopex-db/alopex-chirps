#!/usr/bin/env python3
"""Read-only verification of a frozen v0.7 bundle against public services."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import threading
import urllib.error
import urllib.request
from pathlib import Path


class VerificationError(ValueError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise VerificationError(message)


def sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


class Remote:
    """Only GET, git ls-remote, gh api GET, and skopeo inspect are available."""

    def command(self, args: list[str]) -> bytes:
        try:
            return subprocess.run(args, check=True, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, timeout=120).stdout
        except (OSError, subprocess.SubprocessError) as exc:
            # Do not echo authentication diagnostics or command environment.
            raise VerificationError(f"read-only {args[0]} request failed") from exc

    def tag(self, repository: str, tag: str) -> bytes:
        ref = f"refs/tags/{tag}"
        return self.command(["git", "ls-remote", "--exit-code", "--tags",
                             f"https://github.com/{repository}.git", ref, ref + "^{}"])

    def crate(self, name: str, version: str, expected_size: int) -> tuple[int, str]:
        url = f"https://crates.io/api/v1/crates/{name}/{version}/download"
        request = urllib.request.Request(url, headers={"User-Agent": "chirps-release-verifier/0.7"},
                                         method="GET")
        digest = hashlib.sha256()
        size = 0
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                require(response.status == 200, f"registry archive missing: {name}")
                require(response.url.startswith("https://"), "registry redirected outside HTTPS")
                while chunk := response.read(1024 * 1024):
                    size += len(chunk)
                    require(size <= expected_size, f"registry archive size differs: {name}")
                    digest.update(chunk)
        except (OSError, urllib.error.URLError) as exc:
            raise VerificationError(f"registry archive request failed: {name}") from exc
        return size, digest.hexdigest()

    def image_manifest(self, reference: str) -> bytes:
        return self.command(["skopeo", "inspect", "--raw", f"docker://{reference}"])

    def github(self, path: str) -> object:
        try:
            return json.loads(self.command(["gh", "api", "--method", "GET", path]))
        except json.JSONDecodeError as exc:
            raise VerificationError("GitHub returned invalid JSON") from exc

    def command_digest(self, args: list[str], expected_size: int) -> tuple[int, str]:
        """Bound RAM, bytes consumed, and process lifetime for large assets."""
        try:
            with subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL) as process:
                deadline = threading.Timer(120, process.kill)
                deadline.start()
                digest, size = hashlib.sha256(), 0
                try:
                    while chunk := process.stdout.read(min(1024 * 1024, expected_size - size + 1)):
                        size += len(chunk)
                        require(size <= expected_size, "GitHub asset exceeds stored size")
                        digest.update(chunk)
                    require(process.wait() == 0, "GitHub asset download failed")
                    return size, digest.hexdigest()
                finally:
                    deadline.cancel()
                    if process.poll() is None:
                        process.kill()
        except (OSError, subprocess.SubprocessError) as exc:
            raise VerificationError("read-only asset request failed") from exc

    def asset(self, repository: str, asset_id: int, expected_size: int) -> tuple[int, str]:
        return self.command_digest(["gh", "api", "--method", "GET", "--header",
                                    "Accept: application/octet-stream",
                                    f"repos/{repository}/releases/assets/{asset_id}"], expected_size)


def verify_tag(raw: bytes, tag: str, expected_object: str, expected_commit: str) -> None:
    refs = {}
    try:
        for line in raw.decode("ascii").splitlines():
            digest, ref = line.split("\t")
            require(re.fullmatch(r"[0-9a-f]{40}", digest) is not None, "invalid remote tag object")
            require(ref not in refs, "duplicate remote tag reference")
            refs[ref] = digest
    except (UnicodeError, ValueError) as exc:
        raise VerificationError("invalid remote tag response") from exc
    ref = f"refs/tags/{tag}"
    require(refs == {ref: expected_object, ref + "^{}": expected_commit},
            "annotated tag object or peeled source commit differs")


def release_assets(remote: Remote, repository: str, release_id: int) -> list:
    assets = []
    for page in range(1, 101):
        batch = remote.github(f"repos/{repository}/releases/{release_id}/assets?per_page=100&page={page}")
        require(isinstance(batch, list) and len(batch) <= 100, "invalid GitHub assets response")
        assets.extend(batch)
        if len(batch) < 100:
            return assets
    raise VerificationError("GitHub asset pagination exceeded the verification limit")


def verify_promotion_assets(bundle: dict, remote: Remote) -> None:
    """Read every asset page and exact bytes while the new release is still draft."""
    repository, tag = bundle["github_repository"], bundle["tag"]
    release = remote.github(f"repos/{repository}/releases/tags/{tag}")
    require(isinstance(release, dict) and release.get("tag_name") == tag
            and type(release.get("draft")) is bool and release.get("prerelease") is False,
            "GitHub promotion release identity differs")
    release_id = release.get("id")
    require(type(release_id) is int and release_id > 0, "invalid GitHub release ID")
    assets = release_assets(remote, repository, release_id)
    require(all(isinstance(item, dict) for item in assets), "invalid GitHub asset entry")
    expected = {item["name"]: item for item in bundle["github_assets"]}
    names = [item.get("name") for item in assets]
    require(all(isinstance(name, str) for name in names) and len(names) == len(set(names)) and set(names) == set(expected),
            "GitHub promotion asset inventory differs")
    ids = set()
    for item in assets:
        identifier = item.get("id")
        require(type(identifier) is int and identifier > 0 and identifier not in ids, "invalid GitHub asset ID")
        ids.add(identifier)
        stored = expected[item["name"]]
        require(item.get("state") == "uploaded" and item.get("size") == stored["size"], "GitHub promotion asset metadata differs")
        require(remote.asset(repository, identifier, stored["size"]) == (stored["size"], stored["sha256"]),
                "GitHub promotion asset bytes differ")
    final = remote.github(f"repos/{repository}/releases/tags/{tag}")
    require(isinstance(final, dict) and all(final.get(key) == release.get(key) for key in ("id", "tag_name", "draft", "prerelease", "published_at")), "GitHub release changed before promotion")
    identities = lambda values: sorted(json.dumps({key: item.get(key) for key in ("id", "name", "state", "size", "digest")}, sort_keys=True) for item in values)
    after = release_assets(remote, repository, release_id)
    require(all(isinstance(item, dict) for item in after) and identities(after) == identities(assets), "GitHub assets changed before promotion")


def verify_remote(bundle: dict, tag_object: str, remote: Remote) -> dict:
    """The caller first validates the local bundle with the publication validator."""
    repository, tag = bundle["github_repository"], bundle["tag"]
    source_commit = bundle["source_commit"]
    require(re.fullmatch(r"[0-9a-f]{40}", tag_object) is not None,
            "expected annotated tag object must be a lowercase Git SHA-1")
    first_tag = remote.tag(repository, tag)
    verify_tag(first_tag, tag, tag_object, source_commit)

    for package in bundle["packages"]:
        size, digest = remote.crate(package["name"], package["version"], package["size"])
        require(size == package["size"] and digest == package["sha256"],
                f"registry archive bytes differ: {package['name']}")

    image = bundle["production_image"]
    manifest = remote.image_manifest(image["reference"])
    require("sha256:" + sha256(manifest) == image["manifest_digest"],
            "remote production image manifest differs")

    release = remote.github(f"repos/{repository}/releases/tags/{tag}")
    require(isinstance(release, dict) and release.get("tag_name") == tag
            and release.get("draft") is False and release.get("prerelease") is False
            and isinstance(release.get("published_at"), str),
            "GitHub release is missing, draft, prerelease, or bound to another tag")
    release_id = release.get("id")
    require(type(release_id) is int and release_id > 0, "invalid GitHub release ID")
    # The release response can truncate assets. Read the dedicated paginated API.
    assets = release_assets(remote, repository, release_id)
    require(all(isinstance(item, dict) for item in assets), "invalid GitHub asset entry")
    expected = {item["name"]: item for item in bundle["github_assets"]}
    names = [item.get("name") for item in assets]
    require(all(isinstance(name, str) for name in names), "invalid GitHub asset name")
    require(len(names) == len(set(names)) and set(names) == set(expected),
            "GitHub release asset set differs from the stored bundle")
    ids = set()
    for item in assets:
        asset_id = item.get("id")
        require(type(asset_id) is int and asset_id > 0 and asset_id not in ids,
                "invalid or duplicate GitHub asset ID")
        ids.add(asset_id)
        stored = expected[item["name"]]
        require(item.get("state") == "uploaded" and item.get("size") == stored["size"],
                f"GitHub asset metadata differs: {item['name']}")
        size, digest = remote.asset(repository, asset_id, stored["size"])
        require(size == stored["size"] and digest == stored["sha256"],
                f"GitHub asset bytes differ: {item['name']}")

    # Detect tag/image movement while registry and release downloads were running.
    verify_tag(remote.tag(repository, tag), tag, tag_object, source_commit)
    require(remote.image_manifest(image["reference"]) == manifest,
            "production image tag moved during verification")
    final_release = remote.github(f"repos/{repository}/releases/tags/{tag}")
    require(isinstance(final_release, dict) and all(
        final_release.get(key) == release.get(key)
        for key in ("id", "tag_name", "draft", "prerelease", "published_at")
    ), "GitHub release changed during verification")
    final_assets = release_assets(remote, repository, release_id)
    require(all(isinstance(item, dict) for item in final_assets), "invalid final GitHub asset entry")
    def identities(items: list) -> list[str]:
        # Downloads change counters; only publication identities must stay fixed.
        return sorted(json.dumps({key: item.get(key) for key in
                                  ("id", "name", "state", "size", "digest")}, sort_keys=True)
                      for item in items)
    require(identities(final_assets) == identities(assets),
            "GitHub asset set changed during verification")
    return {"schema": "chirps.v0.7.published-verification/v1", "result": "pass",
            "source_commit": source_commit, "tag_object": tag_object,
            "packages": len(bundle["packages"]), "assets": len(assets),
            "image_manifest_digest": image["manifest_digest"]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--tag-object", required=True)
    args = parser.parse_args()
    evidence = json.loads(args.evidence.read_bytes())
    evidence_bundle = args.evidence.parent / evidence["bundle"]["path"]
    artifacts = json.loads(evidence_bundle.read_bytes())["artifacts"]
    entries = [item for item in artifacts if item.get("id") == "release-bundle"]
    require(len(entries) == 1, "evidence must bind one publication bundle")
    release_bundle = args.evidence.parent / entries[0]["path"]
    publisher = Path(__file__).with_name("publish-v0.7-bundle.sh")
    # Reuse the publisher's source/candidate/archive/OCI/allowlist validation.
    # This path exits before approval and before any external operation.
    subprocess.run(["bash", str(publisher), "--validate-only", "--bundle", str(release_bundle),
                    "--candidate", str(args.candidate), "--evidence", str(args.evidence),
                    "--require-environment-approval", "--resume-only-on-checksum-match"],
                   check=True)
    raw = release_bundle.read_bytes()
    require(sha256(raw) == entries[0]["sha256"], "publication bundle changed after validation")
    result = verify_remote(json.loads(raw), args.tag_object, Remote())
    require(release_bundle.read_bytes() == raw, "publication bundle changed during verification")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (VerificationError, OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as exc:
        print(f"published verification rejected: {exc}", file=sys.stderr)
        sys.exit(1)
