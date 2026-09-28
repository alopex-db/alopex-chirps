#!/usr/bin/env python3
"""Replay official-broker development observations and preserve their API scope."""
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
import re
import sys
import tomllib

sys.dont_write_bytecode = True
from v07_api_evidence import load, portable_log

BASELINE = "f5350d999d883fd3ca9dd33b3dc2754ddb0df049"
TREE = "0d6dcaf544588d3c0a54fe131a6de78b025eef14"
LOCK = "0e4ac6717cfb6ba04894f734b8f56afc56e265925fdd805242b6e39d4d676b41"
DOMAIN = b"ALOPEX-CHIRPS-DURABLE-ENVELOPE\0"
PAYLOADS = (b"official\0broker-accepted-one", bytes(range(256)))
ROW_FIELDS = {"message_id", "canonical_hex", "payload_hex", "source", "target", "generation", "partition", "ordering_key_hex"}


def ensure(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def hex_bytes(value: object, length: int | None = None) -> bytes:
    ensure(isinstance(value, str) and len(value) <= 8192
           and re.fullmatch(r"(?:[0-9a-f]{2})*", value) is not None, "invalid bounded canonical hex")
    raw = bytes.fromhex(value)
    ensure(length is None or len(raw) == length, "identity length differs")
    return raw


def uuid4(value: object) -> bytes:
    raw = hex_bytes(value, 16)
    ensure(raw[6] >> 4 == 4 and raw[8] >> 6 == 2, "identity is not UUIDv4")
    return raw


def unsigned(value: object, bits: int) -> int:
    ensure(type(value) is int and 0 <= value < 2 ** bits, "invalid numeric resource or route")
    return value


def canonical(row: dict, payload: bytes, partition: int) -> bytes:
    """Reconstruct the frozen v1 wire layout and both independent digests."""
    ensure(set(row) == ROW_FIELDS, "official expected envelope fields differ")
    key = hex_bytes(row["ordering_key_hex"])
    ensure(key == b"official-interoperability", "official probe ordering key differs")
    ensure(hex_bytes(row["payload_hex"]) == payload, "official probe payload differs")
    ensure(unsigned(row["generation"], 64) == 1
           and unsigned(row["partition"], 32) == partition, "official probe route differs")
    prefix = (b"\x00\x01" + uuid4(row["message_id"]) + uuid4(row["source"])
              + uuid4(row["target"]) + (1).to_bytes(8, "big") + partition.to_bytes(4, "big")
              + len(key).to_bytes(4, "big") + key + len(payload).to_bytes(8, "big") + payload)
    encoded = prefix + hashlib.sha256(payload).digest() + hashlib.sha256(DOMAIN + prefix).digest()
    ensure(hex_bytes(row["canonical_hex"]) == encoded, "canonical envelope or digest differs")
    return encoded


def verify_observation(value: dict, *, public: bool = False) -> None:
    fields = {"boundary", "strong_preflight", "strong_receipt", "startup_config_hex",
        "startup_config_sha256", "stream_id", "topic_id", "partition_id", "expected", "observed"}
    if not public:
        fields |= {"local_correlation_uuid", "local_correlation_is_broker_identity"}
    ensure(set(value) == fields, "official observation fields differ")
    ensure(value["boundary"] == "BrokerAccepted"
           and value["strong_preflight"] == ("Unavailable" if public else "Unsupported")
           and value["strong_receipt"] is False, "ordinary ACK was upgraded to strong acceptance")
    if not public:
        ensure(value["local_correlation_is_broker_identity"] is False,
               "local correlation was misrepresented as a broker identity")
        uuid4(value["local_correlation_uuid"])
    config = hex_bytes(value["startup_config_hex"])
    ensure(sha(config) == value["startup_config_sha256"], "startup config bytes differ")
    document = tomllib.loads(config.decode())
    ensure(document.get("system", {}).get("message_deduplication", {}).get("enabled") is False,
           "actual startup config does not explicitly disable deduplication")
    for key in ("stream_id", "topic_id", "partition_id"):
        unsigned(value[key], 32)
    expected, observed = value["expected"], value["observed"]
    ensure(isinstance(expected, list) and isinstance(observed, list)
           and len(expected) == len(observed) == 2, "official readback inventory differs")
    for offset, (before, after, payload) in enumerate(zip(expected, observed, PAYLOADS)):
        canonical(before, payload, value["partition_id"])
        ensure(set(after) == ROW_FIELDS | {"offset"}, "official observed envelope fields differ")
        ensure(unsigned(after["offset"], 64) == offset, "official readback offsets differ")
        observed_row = {key: after[key] for key in ROW_FIELDS}
        canonical(observed_row, payload, value["partition_id"])
        ensure(observed_row == before, "official bytes/identity/route readback differs")
    ensure(expected[0]["message_id"] != expected[1]["message_id"], "probe reused a message identity")
    ensure(all(expected[0][key] == expected[1][key] for key in ("source", "target")),
           "probe changed route between messages")


def verify_report(path: Path, source_commit: str, client_binary_sha256: str, *, require_public: bool = False) -> dict:
    """The caller must bind the client executable to its clean candidate build."""
    ensure(re.fullmatch(r"[0-9a-f]{40}", source_commit) is not None
           and re.fullmatch(r"[0-9a-f]{64}", client_binary_sha256) is not None, "missing client provenance")
    value = load(path)
    ensure(set(value) == {"schema", "source_commit", "api_surface", "public_durable_config_validated",
        "official_manifest_sha256", "server_binary_sha256", "server_source_commit", "client_binary_sha256",
        "observation", "cleanup", "result"}, "official report fields differ")
    ensure(value["schema"] in {"chirps.v0.7.official-interoperability/v1", "chirps.v0.7.official-interoperability/v2"}
           and value["result"] == "pass", "official report is not complete")
    public = value["schema"].endswith("/v2")
    ensure(not require_public or public, "lower-level v1 evidence does not validate the public facade")
    ensure(value["source_commit"] == source_commit and value["client_binary_sha256"] == client_binary_sha256,
           "official client identity differs from collected build")
    ensure(value["api_surface"] == ("DurableConfig" if public else "DevelopmentAppendConnection")
           and value["public_durable_config_validated"] is public, "evidence schema and API scope differ")
    cleanup = value["cleanup"]
    ensure(set(cleanup) == {"graceful", "forced"} and cleanup["graceful"] is True
           and cleanup["forced"] is False, "official fixture did not shut down cleanly")
    manifest_path = portable_log(path, "server-manifest.json")
    manifest = load(manifest_path)
    ensure(sha(manifest_path.read_bytes()) == value["official_manifest_sha256"], "official manifest changed")
    ensure(manifest["schema"] == "chirps-official-devbaseline-v1"
           and manifest["artifact_kind"] == "official-development-interoperability"
           and manifest["publishable"] is False and manifest["profile"] == "dev",
           "official development artifact kind differs")
    ensure(value["server_source_commit"] == manifest["source_commit"] == BASELINE
           and manifest["source_tree"] == TREE and manifest["cargo_lock_sha256"] == LOCK
           and manifest["source_clean"] is True, "official source was modified or substituted")
    ensure(value["server_binary_sha256"] == manifest["binary_sha256"]
           and re.fullmatch(r"[0-9a-f]{64}", value["server_binary_sha256"]) is not None
           and unsigned(manifest["binary_size"], 64) > 0, "official binary identity differs")
    verify_observation(value["observation"], public=public)
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--client-binary-sha256", required=True)
    parser.add_argument("--require-public", action="store_true")
    args = parser.parse_args()
    value = verify_report(args.report, args.source_commit, args.client_binary_sha256, require_public=args.require_public)
    print(f'Official development observations verified for {value["api_surface"]}')
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError, KeyError, TypeError) as error:
        print(f"official interoperability rejected: {error}", file=sys.stderr)
        raise SystemExit(1)
