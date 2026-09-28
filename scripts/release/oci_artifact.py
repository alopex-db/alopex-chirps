"""Inspect the effective server bytes in a stored production OCI archive."""
from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
import re
import tarfile

MAX_ARCHIVE_BYTES = 1024 * 1024 * 1024
MAX_ENTRY_BYTES = 512 * 1024 * 1024
MAX_ENTRIES = 100_000


def fail(message: str) -> None:
    raise ValueError(message)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def archive_name(member: tarfile.TarInfo) -> str:
    name = member.name.removeprefix("./")
    if member.isdir():
        name = name.rstrip("/")
        if name in {"", "."}:
            return ""
    if not name or any(part in {"", ".", ".."} for part in name.split("/")):
        fail("OCI archive contains a noncanonical entry path")
    return name


def oci_blob(files: dict[str, bytes], descriptor: dict, label: str) -> bytes:
    if not isinstance(descriptor, dict):
        fail(f"{label} descriptor must be an object")
    descriptor_digest = descriptor.get("digest")
    descriptor_size = descriptor.get("size")
    if not isinstance(descriptor_digest, str) or re.fullmatch(
        r"sha256:[0-9a-f]{64}", descriptor_digest
    ) is None:
        fail(f"{label} descriptor digest is invalid")
    path = f"blobs/sha256/{descriptor_digest.removeprefix('sha256:')}"
    value = files.get(path)
    if value is None:
        fail(f"{label} blob is missing")
    if descriptor_size != len(value) or descriptor_digest != f"sha256:{sha256_bytes(value)}":
        fail(f"{label} descriptor does not match its blob")
    return value


def inspect_oci_server(path: Path, expected_manifest_digest: str) -> tuple[str, dict]:
    if path.stat().st_size > MAX_ARCHIVE_BYTES:
        fail("OCI archive exceeds the byte budget")
    try:
        with tarfile.open(path, mode="r:*") as archive:
            files: dict[str, bytes] = {}
            total_bytes = 0
            for count, member in enumerate(archive, 1):
                total_bytes += member.size
                if count > MAX_ENTRIES or member.size > MAX_ENTRY_BYTES or total_bytes > MAX_ARCHIVE_BYTES:
                    fail("OCI archive exceeds the entry or expanded-byte budget")
                name = archive_name(member)
                if name in files:
                    fail(f"OCI archive contains duplicate entry {name}")
                if member.isfile():
                    source = archive.extractfile(member)
                    if source is None:
                        fail(f"OCI archive entry {name} is unreadable")
                    files[name] = source.read()
                elif name in {"index.json", "oci-layout"} or name.startswith("blobs/"):
                    fail(f"OCI archive metadata entry {name} is not a regular file")
    except (OSError, tarfile.TarError) as exc:
        fail(f"production image is not a readable OCI archive: {exc}")

    try:
        index = json.loads(files["index.json"])
    except (KeyError, json.JSONDecodeError) as exc:
        fail(f"OCI index is missing or invalid: {exc}")
    manifests = index.get("manifests") if isinstance(index, dict) else None
    if not isinstance(manifests, list) or len(manifests) != 1:
        fail("OCI archive must contain exactly one image manifest")
    manifest_descriptor = manifests[0]
    manifest_bytes = oci_blob(files, manifest_descriptor, "OCI manifest")
    if manifest_descriptor.get("digest") != expected_manifest_digest:
        fail("OCI index manifest digest differs from the release bundle")
    try:
        manifest = json.loads(manifest_bytes)
    except json.JSONDecodeError as exc:
        fail(f"OCI manifest is invalid: {exc}")
    if not isinstance(manifest, dict) or manifest.get("schemaVersion") != 2:
        fail("OCI manifest schema is invalid")
    try:
        config = json.loads(oci_blob(files, manifest["config"], "OCI config"))
    except (KeyError, json.JSONDecodeError) as exc:
        fail(f"OCI config is missing or invalid: {exc}")
    labels = config.get("config", {}).get("Labels") if isinstance(config, dict) else None
    if not isinstance(labels, dict) or not all(
        isinstance(key, str) and isinstance(value, str) for key, value in labels.items()
    ):
        fail("OCI config labels are missing or invalid")

    layers = manifest.get("layers")
    if not isinstance(layers, list) or not layers:
        fail("OCI manifest has no filesystem layers")
    server_bytes: bytes | None = None
    server_whiteouts = {
        ".wh..wh..opq",
        ".wh.usr",
        "usr/.wh..wh..opq",
        "usr/.wh.local",
        "usr/local/.wh..wh..opq",
        "usr/local/.wh.bin",
        "usr/local/bin/.wh..wh..opq",
        "usr/local/bin/.wh.iggy-server",
    }
    for index, descriptor in enumerate(layers):
        layer_bytes = oci_blob(files, descriptor, f"OCI layer[{index}]")
        try:
            with tarfile.open(fileobj=io.BytesIO(layer_bytes), mode="r:*") as layer:
                deletes_lower_server = False
                layer_server: bytes | None = None
                total_bytes = 0
                for count, member in enumerate(layer, 1):
                    total_bytes += member.size
                    if count > MAX_ENTRIES or member.size > MAX_ENTRY_BYTES or total_bytes > MAX_ARCHIVE_BYTES:
                        fail("OCI layer exceeds the entry or expanded-byte budget")
                    name = archive_name(member)
                    if name in server_whiteouts:
                        deletes_lower_server = True
                    elif name in {"usr", "usr/local", "usr/local/bin"} and not member.isdir():
                        fail(f"OCI layer[{index}] replaces a server path directory")
                    elif name == "usr/local/bin/iggy-server":
                        if not member.isfile():
                            fail("OCI iggy-server is not a regular file")
                        if layer_server is not None:
                            fail(f"OCI layer[{index}] contains duplicate iggy-server entries")
                        source = layer.extractfile(member)
                        if source is None:
                            fail("OCI iggy-server is unreadable")
                        layer_server = source.read()
                if deletes_lower_server:
                    server_bytes = None
                if layer_server is not None:
                    server_bytes = layer_server
        except tarfile.TarError as exc:
            fail(f"OCI layer[{index}] is not a readable layer archive: {exc}")
    if server_bytes is None:
        fail("OCI image does not contain /usr/local/bin/iggy-server")
    return sha256_bytes(server_bytes), labels
