#!/usr/bin/env bash
# Runs the v0.5.2 service boundary with two local processes and real QUIC
# control/data planes. This is local regression evidence, not ft-1g-v1 release
# evidence (which requires the controlled two-container native-Linux profile).
set -euo pipefail
umask 077

usage() {
  cat <<'USAGE'
Usage: run-local-v0_5_2-baseline.sh --output DIR [--resumable true|false]

Builds two_node_transfer in a temporary target directory, transfers the fixed
128 MiB workload between two localhost processes, and writes machine-readable
result.json plus the raw sender/receiver reports and logs. Temporary payload,
TLS key, process directories, and build output are removed on every exit.
USAGE
}

output=""
resumable=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --output) output="${2:?missing value for --output}"; shift 2 ;;
    --resumable) resumable="${2:?missing value for --resumable}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done
[[ -n "$output" ]] || { printf '%s\n' '--output is required' >&2; exit 2; }
for command in cargo git mktemp python3 dd sha256sum; do
  command -v "$command" >/dev/null || { printf 'required command not found: %s\n' "$command" >&2; exit 127; }
done
if [[ -e "$output" ]] && [[ -n "$(find "$output" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]]; then
  printf 'refusing to overwrite non-empty baseline directory: %s\n' "$output" >&2
  exit 2
fi
mkdir -p "$output/raw"

readonly CONTRACT_PATH=formal/file-transfer/performance-contract.json
[[ -f "$CONTRACT_PATH" ]] || { printf 'missing performance contract: %s\n' "$CONTRACT_PATH" >&2; exit 2; }
FILE_BYTES="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["workload"]["file_bytes"])' "$CONTRACT_PATH")"
readonly FILE_BYTES
if [[ -z "$resumable" ]]; then
  resumable="$(python3 -c 'import json,sys; print(str(json.load(open(sys.argv[1]))["workload"]["resumable"]).lower())' "$CONTRACT_PATH")"
fi
[[ "$resumable" == true || "$resumable" == false ]] || { printf '%s\n' '--resumable must be true or false' >&2; exit 2; }
readonly SENDER_ID=00000000000000000000000000000001
readonly RECEIVER_ID=00000000000000000000000000000002
target_dir="$(mktemp -d /tmp/chirps-v0_5_2-local-target.XXXXXX)"
run_dir="$(mktemp -d /tmp/chirps-v0_5_2-local-run.XXXXXX)"
tls_dir="$run_dir/tls"
sender_dir="$run_dir/sender"
receiver_dir="$run_dir/receiver"
receiver_pid=""

cleanup() {
  if [[ -n "$receiver_pid" ]] && kill -0 "$receiver_pid" 2>/dev/null; then
    kill "$receiver_pid" 2>/dev/null || true
    wait "$receiver_pid" 2>/dev/null || true
  fi
  rm -rf -- "$target_dir" "$run_dir"
}
trap cleanup EXIT INT TERM

ports="$(python3 - <<'PY'
import socket
sockets = []
try:
    for _ in range(4):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("127.0.0.1", 0))
        sockets.append(sock)
    print(" ".join(str(sock.getsockname()[1]) for sock in sockets))
finally:
    for sock in sockets:
        sock.close()
PY
)"
read -r sender_control receiver_control sender_data receiver_data <<<"$ports"

mkdir -p "$sender_dir" "$receiver_dir"
bash scripts/perf/create-lab-tls.sh --output "$tls_dir" >/dev/null
dd if=/dev/zero of="$sender_dir/throughput-source.bin" bs="$FILE_BYTES" count=1 status=none
source_sha="$(git rev-parse HEAD)"

CARGO_TARGET_DIR="$target_dir" cargo build --release -p alopex-chirps-file-transfer --example two_node_transfer
binary="$target_dir/release/examples/two_node_transfer"

"$binary" \
  --role receiver --node-id "$RECEIVER_ID" --peer-id "$SENDER_ID" \
  --control-bind "127.0.0.1:$receiver_control" --peer-control "127.0.0.1:$sender_control" \
  --data-bind "127.0.0.1:$receiver_data" --peer-data "127.0.0.1:$sender_data" \
  --base-path "$receiver_dir" --cert "$tls_dir/cert.der" --key "$tls_dir/key.der" \
  --report "$receiver_dir/receiver-result.env" --source-sha "$source_sha" \
  --scope local-two-process --destination throughput-dest.bin \
  --expected-bytes "$FILE_BYTES" --resumable "$resumable" >"$output/raw/receiver.log" 2>&1 &
receiver_pid=$!
sleep 1

"$binary" \
  --role sender --node-id "$SENDER_ID" --peer-id "$RECEIVER_ID" \
  --control-bind "127.0.0.1:$sender_control" --peer-control "127.0.0.1:$receiver_control" \
  --data-bind "127.0.0.1:$sender_data" --peer-data "127.0.0.1:$receiver_data" \
  --base-path "$sender_dir" --cert "$tls_dir/cert.der" --key "$tls_dir/key.der" \
  --report "$sender_dir/sender-result.env" --source-sha "$source_sha" \
  --scope local-two-process --source throughput-source.bin \
  --destination throughput-dest.bin --expected-bytes "$FILE_BYTES" \
  --resumable "$resumable" \
  >"$output/raw/sender.log" 2>&1
wait "$receiver_pid"
receiver_pid=""

cp "$sender_dir/sender-result.env" "$output/raw/"
cp "$receiver_dir/receiver-result.env" "$output/raw/"
sha256sum "$sender_dir/throughput-source.bin" >"$output/raw/source.sha256"
sha256sum "$receiver_dir/throughput-dest.bin" >"$output/raw/destination.sha256"

python3 - "$output" "$source_sha" "$CONTRACT_PATH" "$resumable" <<'PY'
import json
import copy
import pathlib
import platform
import sys

root = pathlib.Path(sys.argv[1])
source_sha = sys.argv[2]
contract = json.loads(pathlib.Path(sys.argv[3]).read_text(encoding="utf-8"))
resumable = sys.argv[4] == "true"

def env(path):
    values = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            values[key] = value
    return values

def number(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

sender = env(root / "raw/sender-result.env")
receiver = env(root / "raw/receiver-result.env")
source_hash = (root / "raw/source.sha256").read_text(encoding="utf-8").split()[0]
destination_hash = (root / "raw/destination.sha256").read_text(encoding="utf-8").split()[0]
workload = dict(contract["workload"])
workload["resumable"] = resumable
file_bytes = workload["file_bytes"]
chunk_count = workload["chunk_count"]
service_contract = contract["layers"]["service"]
phase_specs = copy.deepcopy(service_contract["phases"])
if not resumable:
    phase_specs["receiver"].pop("receiver_checkpoint", None)

def phase_result(report, specs):
    results = {}
    for phase, expected in specs.items():
        count = number(report.get(f"phase_{phase}_duration_seconds_count"))
        byte_count = number(report.get(f"phase_{phase}_bytes"))
        expected_count = expected.get("count")
        minimum_count = expected.get("minimum_count")
        expected_bytes = expected["bytes"]
        results[phase] = {
            "count": count,
            "bytes": byte_count,
            "duration_seconds": number(report.get(f"phase_{phase}_duration_seconds_sum")),
            "contract_passed": (
                count is not None
                and count > 0
                and (expected_count is None or count == expected_count)
                and (minimum_count is None or count >= minimum_count)
                and byte_count == expected_bytes
            ),
        }
    return results

sender_phases = phase_result(sender, phase_specs["sender"])
receiver_phases = phase_result(receiver, phase_specs["receiver"])
integrity = bool(sender.get("sha256")) and sender["sha256"] == receiver.get("sha256") == source_hash == destination_hash
identity = all(
    report.get("scope") == service_contract["scope"]
    and report.get("source_sha") == source_sha
    and report.get("control_plane") == service_contract["control_plane"]
    and report.get("data_plane") == service_contract["data_plane"]
    and (report.get("role") != "sender" or report.get("resumable") == str(resumable).lower())
    for report in (sender, receiver)
)
receiver_control_parallelism = number(receiver.get("transport_max_concurrent_sends"))
contract_passed = (
    integrity
    and identity
    and all(item["contract_passed"] for item in [*sender_phases.values(), *receiver_phases.values()])
    and receiver_control_parallelism is not None
    and receiver_control_parallelism >= service_contract["minimum_receiver_control_max_concurrent_sends"]
)
result = {
    "schema": "chirps-file-transfer-local-service/v2",
    "scope": "local-two-process",
    "release_evidence": False,
    "source_sha": source_sha,
    "host": {"platform": platform.platform(), "python": platform.python_version()},
    "requirement_id": contract["requirement_id"],
    "workload": workload,
    "end_to_end_bytes_per_second": number(sender.get("end_to_end_bytes_per_second")),
    "payload_bytes_per_second": number(sender.get("payload_bytes_per_second")),
    "integrity_passed": integrity,
    "identity_passed": identity,
    "receiver_control_max_concurrent_sends": receiver_control_parallelism,
    "sender_phases": sender_phases,
    "receiver_phases": receiver_phases,
    "structural_contract_passed": contract_passed,
}
(root / "result.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
if not contract_passed:
    raise SystemExit("local service structural performance contract failed")
print(json.dumps(result, sort_keys=True))
PY
