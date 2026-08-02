#!/usr/bin/env bash
# Creates a hash-manifested evidence bundle. It never treats a local loopback
# result or a network-only preflight as a FileTransfer release pass.
set -euo pipefail
umask 077

usage() {
  cat <<'USAGE'
Usage:
  package-two-node-evidence.sh --output DIR
    [--sender-dir DIR --receiver-dir DIR]
    [--sender-result FILE --receiver-result FILE]
    [--local-baseline-dir DIR]
    [--minimum-network-bps INTEGER] [--minimum-app-bps INTEGER]

The sender/receiver result files are key=value reports from two_node_transfer.
Release eligibility needs physical-network preflight, two-node application
reports, matching SHA-256 values, and both throughput thresholds. Keys and
test payloads are never copied into this bundle.
USAGE
}

output=""
sender_dir=""
receiver_dir=""
sender_result=""
receiver_result=""
baseline_dir=""
minimum_network_bps="900000000"
minimum_app_bps="100000000"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output) output="${2:?missing value for --output}"; shift 2 ;;
    --sender-dir) sender_dir="${2:?missing value for --sender-dir}"; shift 2 ;;
    --receiver-dir) receiver_dir="${2:?missing value for --receiver-dir}"; shift 2 ;;
    --sender-result) sender_result="${2:?missing value for --sender-result}"; shift 2 ;;
    --receiver-result) receiver_result="${2:?missing value for --receiver-result}"; shift 2 ;;
    --local-baseline-dir) baseline_dir="${2:?missing value for --local-baseline-dir}"; shift 2 ;;
    --minimum-network-bps) minimum_network_bps="${2:?missing value for --minimum-network-bps}"; shift 2 ;;
    --minimum-app-bps) minimum_app_bps="${2:?missing value for --minimum-app-bps}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "$output" ]] || { printf '%s\n' '--output is required' >&2; exit 2; }
[[ "$minimum_network_bps" =~ ^[0-9]+$ && "$minimum_app_bps" =~ ^[0-9]+$ ]] || { printf '%s\n' 'throughput thresholds must be integers' >&2; exit 2; }
if [[ -e "$output" ]] && [[ -n "$(find "$output" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]]; then
  printf 'refusing to overwrite non-empty evidence directory: %s\n' "$output" >&2
  exit 2
fi
if [[ -n "$sender_dir$receiver_dir" && ( -z "$sender_dir" || -z "$receiver_dir" ) ]]; then
  printf '%s\n' '--sender-dir and --receiver-dir must be supplied together' >&2
  exit 2
fi
if [[ -n "$sender_result$receiver_result" && ( -z "$sender_result" || -z "$receiver_result" ) ]]; then
  printf '%s\n' '--sender-result and --receiver-result must be supplied together' >&2
  exit 2
fi
for command in git python3 sha256sum; do
  command -v "$command" >/dev/null || { printf 'required command not found: %s\n' "$command" >&2; exit 127; }
done

mkdir -p "$output"
source_sha="$(git rev-parse HEAD)"

copy_if_present() {
  local source="$1"
  local destination="$2"
  if [[ -f "$source" ]]; then
    mkdir -p "$(dirname "$destination")"
    cp "$source" "$destination"
  fi
}

if [[ -n "$sender_dir" ]]; then
  [[ -f "$sender_dir/iperf3-sender.json" && -f "$receiver_dir/iperf3-receiver.json" ]] || {
    printf '%s\n' 'preflight directories do not contain required iperf3 JSON files' >&2; exit 2;
  }
  for name in run.env host-facts.txt iperf3-sender.json iperf3-sender.stderr throughput.env; do
    copy_if_present "$sender_dir/$name" "$output/network/sender/$name"
  done
  for name in run.env host-facts.txt iperf3-receiver.json iperf3-receiver.stderr; do
    copy_if_present "$receiver_dir/$name" "$output/network/receiver/$name"
  done
fi
if [[ -n "$baseline_dir" ]]; then
  [[ -d "$baseline_dir" ]] || { printf 'local baseline directory not found: %s\n' "$baseline_dir" >&2; exit 2; }
  for name in result.env test.log host-facts.txt; do
    copy_if_present "$baseline_dir/$name" "$output/local-baseline/$name"
  done
fi
if [[ -n "$sender_result" ]]; then
  [[ -f "$sender_result" && -f "$receiver_result" ]] || { printf '%s\n' 'application result file not found' >&2; exit 2; }
  copy_if_present "$sender_result" "$output/application/sender-result.env"
  copy_if_present "$receiver_result" "$output/application/receiver-result.env"
fi

python3 - "$output" "$source_sha" "$minimum_network_bps" "$minimum_app_bps" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
source_sha = sys.argv[2]
minimum_network_bps = int(sys.argv[3])
minimum_app_bps = int(sys.argv[4])

def env(path):
    values = {}
    if not path.is_file():
        return values
    for raw in path.read_text(encoding="utf-8").splitlines():
        if "=" in raw:
            key, value = raw.split("=", 1)
            values[key] = value
    return values

def as_float(values, key):
    try:
        return float(values[key])
    except (KeyError, ValueError):
        return None

network = env(root / "network/sender/throughput.env")
sender = env(root / "application/sender-result.env")
receiver = env(root / "application/receiver-result.env")
local = env(root / "local-baseline/result.env")
network_bps = as_float(network, "iperf_bits_per_second")
app_bps = as_float(sender, "end_to_end_bytes_per_second")
sender_hash = sender.get("sha256")
receiver_hash = receiver.get("sha256")
same_hash = bool(sender_hash and receiver_hash and sender_hash == receiver_hash)
run_envs = [
    sender.get("source_sha"),
    receiver.get("source_sha"),
    env(root / "network/sender/run.env").get("source_sha"),
    env(root / "network/receiver/run.env").get("source_sha"),
]
same_sha = all(value == source_sha for value in run_envs)
application_contract = (
    sender.get("kind") == "chirps-file-transfer-two-node"
    and receiver.get("kind") == "chirps-file-transfer-two-node"
    and sender.get("scope") == "two-host-physical-network"
    and receiver.get("scope") == "two-host-physical-network"
    and sender.get("control_plane") == "chirps-quic"
    and sender.get("data_plane") == "quic-chunk-stream"
    and receiver.get("control_plane") == "chirps-quic"
    and receiver.get("data_plane") == "quic-chunk-stream"
    and sender.get("file_bytes") == receiver.get("file_bytes")
    and sender.get("completed") == "true"
    and receiver.get("completed") == "true"
)
network_passed = network_bps is not None and network_bps >= minimum_network_bps
application_passed = app_bps is not None and app_bps >= minimum_app_bps and same_hash and application_contract
eligible = network_passed and application_passed and same_sha
result = {
    "schema_version": 1,
    "source_sha": source_sha,
    "network_preflight_bits_per_second": network_bps,
    "minimum_network_bits_per_second": minimum_network_bps,
    "application_end_to_end_bytes_per_second": app_bps,
    "minimum_application_bytes_per_second": minimum_app_bps,
    "sender_receiver_sha256_match": same_hash,
    "all_evidence_from_checked_out_sha": same_sha,
    "physical_network_preflight_passed": network_passed,
    "two_node_application_passed": application_passed,
    "release_eligible": eligible,
    "local_loopback_baseline_present": bool(local),
    "local_loopback_is_release_evidence": False,
}
(root / "result.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
lines = [
    "# Chirps two-node performance evidence",
    "",
    f"- Source SHA: `{source_sha}`",
    f"- Physical-network preflight: `{'PASS' if network_passed else 'NOT PROVEN / FAIL'}`",
    f"- Two-node FileTransfer: `{'PASS' if application_passed else 'NOT PROVEN / FAIL'}`",
    f"- Release eligibility: `{'YES' if eligible else 'NO'}`",
    "",
    "A local-loopback baseline is diagnostic only; it never substitutes for either physical two-node result.",
]
if network_bps is not None:
    lines.append(f"- iperf3 sender throughput: `{network_bps:.0f} bit/s` (threshold `{minimum_network_bps} bit/s`)")
if app_bps is not None:
    lines.append(f"- FileTransfer end-to-end throughput: `{app_bps:.0f} B/s` (threshold `{minimum_app_bps} B/s`)")
if sender and receiver:
    lines.append(f"- Sender/receiver SHA-256 match: `{'yes' if same_hash else 'no'}`")
(root / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
PY

(
  cd "$output"
  find . -type f ! -name manifest.sha256 -print0 | sort -z | xargs -0 sha256sum > manifest.sha256
)
printf 'evidence bundle: %s\n' "$output"
