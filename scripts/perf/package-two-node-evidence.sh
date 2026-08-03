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

The sender/receiver result files are key=value reports from two_node_transfer.
This creates a two-host deployment diagnostic: PATH-UDP-100 preflight, actual
QUIC FileTransfer reports, and matching SHA-256 values. It never treats host
network throughput as Chirps product-performance or release-gate evidence.
Keys and test payloads are never copied into this bundle.
USAGE
}

output=""
sender_dir=""
receiver_dir=""
sender_result=""
receiver_result=""
baseline_dir=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output) output="${2:?missing value for --output}"; shift 2 ;;
    --sender-dir) sender_dir="${2:?missing value for --sender-dir}"; shift 2 ;;
    --receiver-dir) receiver_dir="${2:?missing value for --receiver-dir}"; shift 2 ;;
    --sender-result) sender_result="${2:?missing value for --sender-result}"; shift 2 ;;
    --receiver-result) receiver_result="${2:?missing value for --receiver-result}"; shift 2 ;;
    --local-baseline-dir) baseline_dir="${2:?missing value for --local-baseline-dir}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "$output" ]] || { printf '%s\n' '--output is required' >&2; exit 2; }
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

python3 - "$output" "$source_sha" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
source_sha = sys.argv[2]

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
sender_network_run = env(root / "network/sender/run.env")
receiver_network_run = env(root / "network/receiver/run.env")
sender = env(root / "application/sender-result.env")
receiver = env(root / "application/receiver-result.env")
local = env(root / "local-baseline/result.env")
network_bps = as_float(network, "iperf_sender_bits_per_second")
app_bps = as_float(sender, "end_to_end_bytes_per_second")

def receiver_loss_percent(path):
    if not path.is_file():
        return None
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    candidates = [
        document.get("end", {}).get("sum", {}).get("lost_percent"),
        document.get("end", {}).get("sum_received", {}).get("lost_percent"),
    ]
    for stream in document.get("end", {}).get("streams", []):
        candidates.append(stream.get("udp", {}).get("lost_percent"))
    for value in candidates:
        if isinstance(value, (int, float)):
            return float(value)
    return None

loss_percent = receiver_loss_percent(root / "network/receiver/iperf3-receiver.json")
sender_hash = sender.get("sha256")
receiver_hash = receiver.get("sha256")
same_hash = bool(sender_hash and receiver_hash and sender_hash == receiver_hash)
run_envs = [
    sender.get("source_sha"),
    receiver.get("source_sha"),
    sender_network_run.get("source_sha"),
    receiver_network_run.get("source_sha"),
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
path_udp_100 = (
    sender_network_run.get("protocol") == "udp"
    and receiver_network_run.get("protocol") == "udp"
    and sender_network_run.get("offered_bitrate") == "100M"
    and sender_network_run.get("duration_seconds") == "15"
    and network_bps is not None
    and loss_percent == 0.0
)
application_completed = app_bps is not None and same_hash and application_contract
deployment_compatible = path_udp_100 and application_completed and same_sha
result = {
    "schema_version": 1,
    "evidence_class": "deployment-two-host",
    "product_performance_evidence": False,
    "source_sha": source_sha,
    "path_udp_100_sender_bits_per_second": network_bps,
    "path_udp_100_receiver_lost_percent": loss_percent,
    "application_end_to_end_bytes_per_second": app_bps,
    "sender_receiver_sha256_match": same_hash,
    "all_evidence_from_checked_out_sha": same_sha,
    "path_udp_100_passed": path_udp_100,
    "two_host_application_completed": application_completed,
    "deployment_compatible": deployment_compatible,
    "release_eligible": False,
    "local_loopback_baseline_present": bool(local),
    "local_loopback_is_release_evidence": False,
}
(root / "result.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
lines = [
    "# Chirps two-host deployment diagnostic",
    "",
    f"- Source SHA: `{source_sha}`",
    f"- PATH-UDP-100: `{'PASS' if path_udp_100 else 'NOT PROVEN / FAIL'}`",
    f"- Two-host FileTransfer integrity: `{'PASS' if application_completed else 'NOT PROVEN / FAIL'}`",
    f"- Deployment compatibility: `{'YES' if deployment_compatible else 'NO'}`",
    "- Product-performance evidence: `NO` (requires controlled two-container `ft-1g-v1`)",
    "",
    "This artifact records one deployment path. Its host-network throughput is not a Chirps product SLO or release-performance result.",
]
if network_bps is not None:
    lines.append(f"- UDP offered-load observation: `{network_bps:.0f} bit/s` (offered `100M`)")
if loss_percent is not None:
    lines.append(f"- UDP receiver loss: `{loss_percent:.6g}%` (required `0%`)")
if app_bps is not None:
    lines.append(f"- FileTransfer observed end-to-end goodput: `{app_bps:.0f} B/s` (no product-SLO threshold in this evidence class)")
if sender and receiver:
    lines.append(f"- Sender/receiver SHA-256 match: `{'yes' if same_hash else 'no'}`")
(root / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
PY

(
  cd "$output"
  find . -type f ! -name manifest.sha256 -print0 | sort -z | xargs -0 sha256sum > manifest.sha256
)
printf 'evidence bundle: %s\n' "$output"
