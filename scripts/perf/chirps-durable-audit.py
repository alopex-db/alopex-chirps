#!/usr/bin/env python3
"""Linux single-host resource sampler for the durable performance driver.

This collector measures process RSS and allocated disk bytes. The Rust driver
owns workload queue, delivery/readback, identity, digest, error and timeout
observations. No field is filled with an assumed zero.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import secrets
import subprocess
import sys
import time
import tomllib

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "release"))
from oci_artifact import inspect_oci_server
import oci_artifact

SCHEMA = "chirps.durable-perf-audit-config/v1"
REQUEST = "chirps.durable-perf-audit-request/v2"
RESPONSE = "chirps.durable-perf-audit-response/v2"
HEX = re.compile(r"[0-9a-f]{64}")
CONTROLS = {"forbidden_error", "timeout", "unexpected_duplicate", "wrong_identity", "wrong_digest", "undrained_queue", "undrained_lag", "hard_resource_limit"}
PLANNED_AXES = {
    "full_confirmation_profile", "offered_load_per_second", "warmup_millis", "measure_millis",
    "drain_millis", "client_placement", "execution_class", "host_count", "broker_count", "replication_factor",
}
MAX_JSON_BYTES = 1024 * 1024


def ensure(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path: Path) -> dict:
    ensure(path.stat().st_size <= MAX_JSON_BYTES, "JSON exceeds its size budget")
    value = json.loads(path.read_bytes())
    ensure(isinstance(value, dict), "JSON must be an object")
    return value


def write_json(path: Path, value: dict) -> None:
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    try:
        with temporary.open("xb") as target:
            target.write(canonical(value) + b"\n")
            target.flush()
            os.fsync(target.fileno())
        # Publish the completed bytes atomically without replacing a prior
        # observation. Readers must never see the empty file created by open(x).
        os.link(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def absolute(value: object) -> Path:
    ensure(isinstance(value, str) and Path(value).is_absolute(), "configured path must be absolute")
    path = Path(value)
    ensure(".." not in path.parts, "configured path contains parent traversal")
    ensure(not any(part.is_symlink() for part in (path, *path.parents)), "configured path traverses a symlink")
    return path


def checked_file(value: dict) -> Path:
    ensure(isinstance(value, dict) and set(value) == {"path", "sha256"}, "invalid file reference")
    path = absolute(value["path"])
    ensure(isinstance(value["sha256"], str) and HEX.fullmatch(value["sha256"]) is not None, "invalid file digest")
    ensure(path.is_file() and file_digest(path) == value["sha256"], "referenced file is missing or changed")
    return path


def positive(value: object) -> bool:
    return type(value) is int and value > 0


def load_config(path: Path, expected: str) -> dict:
    ensure(platform.system() == "Linux", "resource collector only supports Linux")
    ensure(HEX.fullmatch(expected) is not None and file_digest(path) == expected, "collector configuration digest differs")
    value = read_json(path)
    ensure(set(value) == {"schema", "state_root", "server", "client_executable_sha256", "checkpoint_root", "payload", "partition_set", "image", "planned_axes", "sample_interval_millis", "max_observation_millis", "max_rss_bytes", "max_disk_growth_bytes", "hard_control_rss_bytes", "inspector_sha256"}, "collector configuration fields differ")
    ensure(value["schema"] == SCHEMA, "unsupported collector configuration")
    ensure(value["inspector_sha256"] == file_digest(Path(oci_artifact.__file__)), "OCI inspector source differs")
    for key in ("sample_interval_millis", "max_observation_millis", "max_rss_bytes", "max_disk_growth_bytes", "hard_control_rss_bytes"):
        ensure(positive(value[key]), f"invalid {key}")
    ensure(1 <= value["sample_interval_millis"] <= 1000, "sampling interval must be 1..1000 ms")
    ensure(value["sample_interval_millis"] < value["max_observation_millis"] <= 3_600_000, "observation deadline must be at most one hour")
    ensure(value["max_observation_millis"] // value["sample_interval_millis"] <= 200_000, "sampling output would exceed its budget")
    ensure(value["hard_control_rss_bytes"] < value["max_rss_bytes"], "hard-resource control must use a tighter RSS cap")
    ensure(isinstance(value["client_executable_sha256"], str) and HEX.fullmatch(value["client_executable_sha256"]) is not None, "invalid client executable digest")
    axes = value["planned_axes"]
    ensure(isinstance(axes, dict) and set(axes) == PLANNED_AXES, "planned axes fields differ")
    ensure((axes["execution_class"], axes["client_placement"], axes["host_count"], axes["broker_count"], axes["replication_factor"]) == ("loopback", "same-host", 1, 1, 1), "collector supports same-host single-broker loopback only")
    ensure(all(type(axes[key]) is int for key in ("host_count", "broker_count", "replication_factor")), "topology counts must be integers")
    for key in ("offered_load_per_second", "warmup_millis", "measure_millis", "drain_millis"):
        ensure(positive(axes[key]), f"invalid planned {key}")
    ensure(value["max_observation_millis"] <= axes["measure_millis"] + axes["drain_millis"] + 120_000, "sampler deadline exceeds the observation cleanup budget")
    ensure(axes["full_confirmation_profile"] in {"broker_accepted", "os_synced_accepted"}, "invalid confirmation profile")
    server = value["server"]
    ensure(isinstance(server, dict) and set(server) == {"pid", "start_time_ticks", "executable_sha256", "manifest", "config", "command_sha256", "environment_sha256", "data_root"}, "server binding fields differ")
    ensure(positive(server["pid"]) and positive(server["start_time_ticks"]), "invalid server process identity")
    for key in ("executable_sha256", "command_sha256", "environment_sha256"):
        ensure(isinstance(server[key], str) and HEX.fullmatch(server[key]) is not None, f"invalid server {key}")
    roots = [absolute(value["state_root"]), absolute(server["data_root"]), absolute(value["checkpoint_root"])]
    ensure(all(root.is_dir() for root in roots), "state/data/checkpoint roots must exist")
    ensure(all(not a.is_relative_to(b) and not b.is_relative_to(a) for i, a in enumerate(roots) for b in roots[i + 1:]), "state/data/checkpoint roots must be separate")
    ensure(roots[0].stat().st_uid == os.getuid() and roots[0].stat().st_mode & 0o077 == 0, "state root must be owned by the operator with mode 0700")
    return value


def parse_stat(raw: str) -> tuple[int, str]:
    # comm may itself contain spaces and parentheses. Field 22 follows the last
    # closing parenthesis; never split the whole line on whitespace.
    end = raw.rfind(")")
    ensure(end > 0, "malformed process stat")
    fields = raw[end + 2:].split()
    ensure(len(fields) > 19, "incomplete process stat")
    return int(fields[19]), fields[0]


def process_start(pid: int) -> int:
    root = Path("/proc") / str(pid)
    ensure(root.stat().st_uid == os.getuid(), "collector may only inspect its operator's processes")
    start, state = parse_stat((root / "stat").read_text())
    ensure(state not in {"Z", "X"}, "measured process has exited")
    return start


def process_identity(pid: int) -> dict:
    start = process_start(pid)
    digest = file_digest(Path("/proc") / str(pid) / "exe")
    ensure(process_start(pid) == start, "process changed during executable inspection")
    return {"pid": pid, "start_time_ticks": start, "executable_sha256": digest}


def rss_bytes(identity: dict) -> int:
    pid = identity["pid"]
    ensure(process_start(pid) == identity["start_time_ticks"], "PID was reused during sampling")
    raw = (Path("/proc") / str(pid) / "status").read_text()
    match = re.search(r"^VmRSS:\s+(\d+)\s+kB$", raw, re.MULTILINE)
    ensure(match is not None, "process RSS is unavailable")
    value = int(match[1]) * 1024
    ensure(value > 0, "process RSS is empty")
    ensure(process_start(pid) == identity["start_time_ticks"], "process changed during RSS sampling")
    return value


def server_environment(pid: int) -> dict:
    records = (Path("/proc") / str(pid) / "environ").read_bytes().split(b"\0")
    selected = {}
    for record in records:
        key, separator, value = record.partition(b"=")
        if not separator or not key.startswith(b"IGGY_"):
            continue
        # Credential values are deliberately outside the configuration digest.
        # TLS key FILE names remain configuration; no key contents are read.
        if any(word in key for word in (b"PASSWORD", b"TOKEN", b"SECRET")):
            continue
        name = key.decode()
        ensure(name not in selected, "duplicate server environment key")
        selected[name] = value.decode()
    return selected


def environment_digest(pid: int) -> str:
    return digest_bytes(canonical(server_environment(pid)))


def host_fingerprint() -> str:
    return digest_bytes(canonical({
        "uname": list(os.uname()),
        "machine_id": Path("/etc/machine-id").read_text().strip(),
        "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
    }))


def disk_bytes(roots: list[Path]) -> int:
    seen = set()
    total = 0
    for root in roots:
        for path in root.rglob("*"):
            ensure(not path.is_symlink(), "measured data root contains a symlink")
            stat = path.stat()
            identity = (stat.st_dev, stat.st_ino)
            if path.is_file() and identity not in seen:
                seen.add(identity)
                ensure(len(seen) <= 200_000, "data inventory exceeds its file budget")
                total += stat.st_blocks * 512
    return total


def observed_binding(config: dict, client_pid: int) -> dict:
    client = process_identity(client_pid)
    server = process_identity(config["server"]["pid"])
    ensure(client["pid"] != server["pid"], "client and server must be distinct processes")
    ensure(client["executable_sha256"] == config["client_executable_sha256"], "client executable differs")
    ensure(all(server[key] == config["server"][key] for key in server), "server process/executable identity differs")
    manifest = tomllib.loads(checked_file(config["server"]["manifest"]).read_text())
    ensure(manifest["artifact"]["kind"] == "production" and manifest["artifact"]["publishable"] is True, "performance requires a production server")
    ensure(manifest["artifact"]["output_sha256"] == server["executable_sha256"], "running server is not the manifest's production executable")
    configuration = checked_file(config["server"]["config"])
    command = digest_bytes((Path("/proc") / str(server["pid"]) / "cmdline").read_bytes())
    environment = environment_digest(server["pid"])
    ensure(command == config["server"]["command_sha256"] and environment == config["server"]["environment_sha256"], "running server command/configuration environment differs")
    ensure(server_environment(server["pid"]).get("IGGY_SYSTEM_PATH") == str(absolute(config["server"]["data_root"])), "measured data root differs from running server IGGY_SYSTEM_PATH")
    payload = checked_file(config["payload"])
    partitions = checked_file(config["partition_set"])
    image_ref = config["image"]
    ensure(isinstance(image_ref, dict) and set(image_ref) == {"path", "sha256", "manifest_digest"}, "invalid OCI archive reference")
    image = checked_file({key: image_ref[key] for key in ("path", "sha256")})
    image_server, labels = inspect_oci_server(image, image_ref["manifest_digest"])
    ensure(image_server == server["executable_sha256"] and labels.get("org.alopex.chirps.artifact-kind") == "production" and labels.get("org.alopex.chirps.failpoints") == "disabled" and labels.get("org.alopex.chirps.server-sha256") == image_server, "OCI production server differs from running executable")
    axes = {
        **config["planned_axes"], "host_fingerprint": host_fingerprint(),
        "server_image_digest": image_ref["manifest_digest"],
        "server_source_digest": digest_bytes(canonical(manifest["source"])),
        "server_config_digest": digest_bytes(canonical({"file_sha256": file_digest(configuration), "command_sha256": command, "environment_sha256": environment})),
        "payload_digest": file_digest(payload), "payload_bytes": payload.stat().st_size,
        "partition_set_digest": file_digest(partitions),
    }
    ensure(process_identity(client_pid) == client and process_identity(server["pid"]) == server, "process identity changed during binding")
    return {"client": client, "server": server, "axes": axes}


def validate_request(value: dict) -> None:
    ensure(set(value) == {"schema", "action", "observation_id", "phase", "arm", "sample_index", "safety_control", "completed_operations", "payload_sha256", "client_pid", "checkpoint_root"}, "audit request fields differ")
    ensure(value["schema"] == REQUEST and value["action"] in {"begin", "finish"}, "unsupported audit request")
    ensure(isinstance(value["observation_id"], str) and HEX.fullmatch(value["observation_id"]) is not None, "invalid observation ID")
    ensure(isinstance(value["payload_sha256"], str) and HEX.fullmatch(value["payload_sha256"]) is not None, "invalid payload digest")
    ensure(value["arm"] in {"direct", "full"} and isinstance(value["phase"], str) and re.fullmatch(r"[a-z_]+", value["phase"]) is not None, "invalid audit arm/phase")
    ensure(value["safety_control"] is None or value["safety_control"] in CONTROLS, "unknown safety control")
    ensure(positive(value["client_pid"]), "invalid client PID")
    absolute(value["checkpoint_root"])
    for key in ("sample_index", "completed_operations"):
        ensure(type(value[key]) is int and value[key] >= 0, f"invalid {key}")
    ensure(value["action"] != "begin" or value["completed_operations"] == 0, "begin cannot report completed measurement operations")


def response(request: dict, axes: dict, metrics: dict | None) -> dict:
    return {
        "schema": RESPONSE, **{key: request[key] for key in ("action", "observation_id", "phase", "arm", "sample_index", "safety_control")},
        "observed_axes": axes,
        "safety_control_active": request["action"] == "begin" and request["safety_control"] is not None,
        "metrics": metrics,
    }


def wait_for(path: Path, deadline: float) -> None:
    while not path.is_file():
        ensure(time.monotonic() < deadline, f"sampler did not produce {path.name} before its deadline")
        time.sleep(0.02)


def sample(state_path: Path) -> None:
    state = read_json(state_path)
    directory = state_path.parent
    config = load_config(Path(state["config_path"]), state["config_sha256"])
    own = {"pid": os.getpid(), "start_time_ticks": process_start(os.getpid())}
    start = time.monotonic_ns()
    count = peak = disk_peak = max_gap = 0
    last = start
    deadline = start + config["max_observation_millis"] * 1_000_000
    result = {"status": "fail"}
    try:
        with (directory / "samples.jsonl").open("x") as output:
            while True:
                now = time.monotonic_ns()
                ensure(now < deadline, "maximum observation duration exceeded")
                ensure(file_digest(Path(state["config_path"])) == state["config_sha256"], "configuration changed during sampling")
                client = rss_bytes(state["binding"]["client"])
                server = rss_bytes(state["binding"]["server"])
                disk = disk_bytes([absolute(config["server"]["data_root"]), absolute(config["checkpoint_root"])])
                disk_peak = max(disk_peak, disk - state["disk_bytes_before"])
                peak = max(peak, client + server)
                max_gap = max(max_gap, now - last)
                ensure(max_gap <= config["sample_interval_millis"] * 5_000_000, "RSS sampling fell behind its declared interval")
                last = now
                count += 1
                output.write(json.dumps({"elapsed_nanos": now - start, "client_rss_bytes": client, "server_rss_bytes": server, "allocated_disk_bytes": disk}) + "\n")
                output.flush()
                if count == 1:
                    write_json(directory / "ready.json", own)
                if (directory / "stop").exists():
                    break
                time.sleep(config["sample_interval_millis"] / 1000)
        result = {"status": "pass", "peak_rss_bytes": peak, "peak_disk_growth_bytes": disk_peak, "sample_count": count, "max_sample_gap_nanos": max_gap}
    except (OSError, ValueError, KeyError) as error:
        result["error"] = str(error)
    finally:
        write_json(directory / "sampler-result.json", result)


def audit(config_path: Path, expected: str, request: dict) -> dict:
    validate_request(request)
    config_path = config_path.resolve()
    config = load_config(config_path, expected)
    ensure(request["payload_sha256"] == config["payload"]["sha256"], "request payload differs from pinned collector configuration")
    ensure(request["checkpoint_root"] == str(absolute(config["checkpoint_root"])), "measured checkpoint root differs from workload request")
    directory = absolute(config["state_root"]) / request["observation_id"]
    roots = [absolute(config["server"]["data_root"]), absolute(config["checkpoint_root"])]
    if request["action"] == "begin":
        binding = observed_binding(config, request["client_pid"])
        directory.mkdir(mode=0o700)
        state = {
            "config_path": str(config_path), "config_sha256": expected, "request": request,
            "binding": binding, "disk_bytes_before": disk_bytes(roots),
        }
        write_json(directory / "state.json", state)
        try:
            with (directory / "sampler.log").open("xb") as log:
                subprocess.run([sys.executable, str(Path(__file__).resolve()), "--sample-state", str(directory / "state.json"), "--detach"], stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True, close_fds=True, timeout=5, check=True)
            wait_for(directory / "ready.json", time.monotonic() + 5)
            ensure(not (directory / "sampler-result.json").exists(), "sampler failed during startup")
        except (OSError, ValueError, subprocess.SubprocessError):
            (directory / "stop").touch(exist_ok=True)
            raise
        return response(request, binding["axes"], None)
    ensure(directory.is_dir() and not directory.is_symlink(), "observation was never started")
    ensure(not (directory / "finish.json").exists(), "observation was already finished")
    state = read_json(directory / "state.json")
    try:
        ensure(state["config_sha256"] == expected and state["config_path"] == str(config_path), "observation configuration changed")
        for key in request.keys() - {"action", "completed_operations"}:
            ensure(request[key] == state["request"][key], "finish request belongs to another observation")
        binding = observed_binding(config, request["client_pid"])
        ensure(binding == state["binding"], "observed axes or process identities changed")
        after = disk_bytes(roots)
    finally:
        # Cooperatively stop this observation's sampler; never signal a PID
        # taken from a potentially stale state file or touch the measured server.
        (directory / "stop").touch(exist_ok=True)
    wait_for(directory / "sampler-result.json", time.monotonic() + 5)
    result = read_json(directory / "sampler-result.json")
    ensure(result.get("status") == "pass" and result.get("sample_count", 0) >= 2, "resource sampler failed or produced too few observations")
    growth = max(0, after - state["disk_bytes_before"])
    cap = config["hard_control_rss_bytes"] if request["safety_control"] == "hard_resource_limit" else config["max_rss_bytes"]
    metrics = {
        "peak_rss_bytes": result["peak_rss_bytes"], "disk_growth_bytes": growth,
        "hard_resource_limit_exceeded": result["peak_rss_bytes"] > cap or max(growth, result["peak_disk_growth_bytes"]) > config["max_disk_growth_bytes"],
    }
    value = response(request, binding["axes"], metrics)
    write_json(directory / "finish.json", {"request": request, "response": value, "disk_bytes_after": after, "sampler": result})
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--config-sha256")
    parser.add_argument("--chirps-audit-request-json")
    parser.add_argument("--sample-state", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--detach", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    try:
        if args.sample_state is not None:
            ensure(not any((args.config, args.config_sha256, args.chirps_audit_request_json)), "sampler mode takes no request arguments")
            if args.detach and os.fork() != 0:
                return 0
            sample(args.sample_state)
        else:
            ensure(not args.detach, "detach is only valid for the internal sampler")
            ensure(all((args.config, args.config_sha256, args.chirps_audit_request_json)), "config, digest, and request are required")
            value = audit(args.config, args.config_sha256, json.loads(args.chirps_audit_request_json))
            print(json.dumps(value, sort_keys=True, allow_nan=False))
    except (ValueError, OSError, KeyError, TypeError, subprocess.SubprocessError) as error:
        print(f"durable resource audit rejected: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
