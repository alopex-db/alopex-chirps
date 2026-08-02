#!/usr/bin/env bash
# Runs the v0.5.2 direct FileTransfer service harness across two physical hosts.
# The harness is deliberately separate from MeshHandle because that public
# construction API is not available in v0.5.2.
set -euo pipefail
umask 077

usage() {
  cat <<'USAGE'
Usage:
  run-two-node-file-transfer.sh --sender SSH_TARGET --receiver SSH_TARGET
    --remote-workdir DIR --remote-output-root DIR --output DIR
    --sender-control-bind HOST:PORT --sender-control-address HOST:PORT
    --receiver-control-bind HOST:PORT --receiver-control-address HOST:PORT
    --sender-data-bind HOST:PORT --sender-data-address HOST:PORT
    --receiver-data-bind HOST:PORT --receiver-data-address HOST:PORT
    [--receiver-iperf-address ADDRESS] [--file-bytes BYTES]

Each host must have the same checked-out commit, Rust toolchain, iperf3, OpenSSL,
and SSH access from the controller. The script creates a two-day, test-only TLS
key, copies it to both hosts, and removes it afterwards. It uploads neither the
key nor the generated payload into the final evidence directory.
USAGE
}

sender=""
receiver=""
remote_workdir=""
remote_output_root=""
output=""
sender_control_bind=""
sender_control_address=""
receiver_control_bind=""
receiver_control_address=""
sender_data_bind=""
sender_data_address=""
receiver_data_bind=""
receiver_data_address=""
receiver_iperf_address=""
file_bytes="134217728"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --sender) sender="${2:?missing value for --sender}"; shift 2 ;;
    --receiver) receiver="${2:?missing value for --receiver}"; shift 2 ;;
    --remote-workdir) remote_workdir="${2:?missing value for --remote-workdir}"; shift 2 ;;
    --remote-output-root) remote_output_root="${2:?missing value for --remote-output-root}"; shift 2 ;;
    --output) output="${2:?missing value for --output}"; shift 2 ;;
    --sender-control-bind) sender_control_bind="${2:?missing value for --sender-control-bind}"; shift 2 ;;
    --sender-control-address) sender_control_address="${2:?missing value for --sender-control-address}"; shift 2 ;;
    --receiver-control-bind) receiver_control_bind="${2:?missing value for --receiver-control-bind}"; shift 2 ;;
    --receiver-control-address) receiver_control_address="${2:?missing value for --receiver-control-address}"; shift 2 ;;
    --sender-data-bind) sender_data_bind="${2:?missing value for --sender-data-bind}"; shift 2 ;;
    --sender-data-address) sender_data_address="${2:?missing value for --sender-data-address}"; shift 2 ;;
    --receiver-data-bind) receiver_data_bind="${2:?missing value for --receiver-data-bind}"; shift 2 ;;
    --receiver-data-address) receiver_data_address="${2:?missing value for --receiver-data-address}"; shift 2 ;;
    --receiver-iperf-address) receiver_iperf_address="${2:?missing value for --receiver-iperf-address}"; shift 2 ;;
    --file-bytes) file_bytes="${2:?missing value for --file-bytes}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done
for value in sender receiver remote_workdir remote_output_root output sender_control_bind sender_control_address receiver_control_bind receiver_control_address sender_data_bind sender_data_address receiver_data_bind receiver_data_address; do
  [[ -n "${!value}" ]] || { printf 'missing required option --%s\n' "${value//_/-}" >&2; exit 2; }
done
[[ "$sender" != "$receiver" ]] || { printf '%s\n' '--sender and --receiver must be distinct physical hosts' >&2; exit 2; }
[[ "$file_bytes" =~ ^[1-9][0-9]*$ ]] || { printf '%s\n' '--file-bytes must be a positive integer' >&2; exit 2; }
if [[ -z "$receiver_iperf_address" ]]; then receiver_iperf_address="${receiver_data_address%:*}"; fi
for command in git ssh scp bash mktemp dd; do
  command -v "$command" >/dev/null || { printf 'required command not found: %s\n' "$command" >&2; exit 127; }
done
if [[ -e "$output" ]] && [[ -n "$(find "$output" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]]; then
  printf 'refusing to overwrite non-empty evidence directory: %s\n' "$output" >&2
  exit 2
fi
mkdir -p "$output"
source_sha="$(git rev-parse HEAD)"
run_id="${source_sha:0:12}-$(date -u +%Y%m%dT%H%M%SZ)"
sender_run="$remote_output_root/$run_id-sender-app"
receiver_run="$remote_output_root/$run_id-receiver-app"
sender_tls="$remote_output_root/$run_id-sender-tls"
receiver_tls="$remote_output_root/$run_id-receiver-tls"
local_tls="$(mktemp -d /tmp/chirps-two-node-tls.XXXXXX)"
receiver_ssh_pid=""

cleanup() {
  if [[ -n "$receiver_ssh_pid" ]] && kill -0 "$receiver_ssh_pid" 2>/dev/null; then
    kill "$receiver_ssh_pid" 2>/dev/null || true
  fi
  rm -rf -- "$local_tls"
  ssh -- "$sender" "rm -rf -- $(printf '%q' "$sender_tls")" >/dev/null 2>&1 || true
  ssh -- "$receiver" "rm -rf -- $(printf '%q' "$receiver_tls")" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

# The preflight result is evidence only. It is recorded before FileTransfer so
# a slow link cannot be mistaken for an application regression.
bash scripts/perf/run-two-node-preflight.sh \
  --sender "$sender" --receiver "$receiver" \
  --receiver-address "$receiver_iperf_address" \
  --remote-workdir "$remote_workdir" --remote-output-root "$remote_output_root" \
  --output "$output/preflight"

bash scripts/perf/create-lab-tls.sh --output "$local_tls"
for target_and_dir in "$sender:$sender_tls" "$receiver:$receiver_tls"; do
  target="${target_and_dir%%:*}"
  directory="${target_and_dir#*:}"
  ssh -- "$target" "mkdir -p -- $(printf '%q' "$directory") && chmod 700 -- $(printf '%q' "$directory")"
  scp -- "$local_tls/cert.der" "$local_tls/key.der" "$target:$directory/"
done

render_remote() {
  local -a command=("$@")
  local rendered=""
  printf -v rendered '%q ' "${command[@]}"
  printf 'cd %q && %s' "$remote_workdir" "$rendered"
}

prepare_source_command=(bash -c 'mkdir -p -- "$1"; dd if=/dev/zero of="$1/throughput-source.bin" bs=1048576 count="$2" status=none; remainder=$(( $3 % 1048576 )); if [[ "$remainder" -gt 0 ]]; then dd if=/dev/zero of="$1/throughput-source.bin" bs=1 count="$remainder" seek=$(( $2 * 1048576 )) conv=notrunc status=none; fi; sync' _ "$sender_run" "$((file_bytes / 1048576))" "$file_bytes")
ssh -- "$sender" "$(render_remote "${prepare_source_command[@]}")"
ssh -- "$receiver" "$(render_remote mkdir -p -- "$receiver_run")"

sender_id="00000000000000000000000000000001"
receiver_id="00000000000000000000000000000002"
receiver_command=(cargo run --release -p alopex-chirps-file-transfer --example two_node_transfer --
  --role receiver --node-id "$receiver_id" --peer-id "$sender_id"
  --control-bind "$receiver_control_bind" --peer-control "$sender_control_address"
  --data-bind "$receiver_data_bind" --peer-data "$sender_data_address"
  --base-path "$receiver_run" --cert "$receiver_tls/cert.der" --key "$receiver_tls/key.der"
  --report "$receiver_run/receiver-result.env" --source-sha "$source_sha"
  --scope two-host-physical-network --destination throughput-dest.bin --expected-bytes "$file_bytes")
sender_command=(cargo run --release -p alopex-chirps-file-transfer --example two_node_transfer --
  --role sender --node-id "$sender_id" --peer-id "$receiver_id"
  --control-bind "$sender_control_bind" --peer-control "$receiver_control_address"
  --data-bind "$sender_data_bind" --peer-data "$receiver_data_address"
  --base-path "$sender_run" --cert "$sender_tls/cert.der" --key "$sender_tls/key.der"
  --report "$sender_run/sender-result.env" --source-sha "$source_sha"
  --scope two-host-physical-network --source throughput-source.bin --destination throughput-dest.bin --expected-bytes "$file_bytes")

ssh -- "$receiver" "$(render_remote "${receiver_command[@]}")" >"$output/receiver-application.log" 2>&1 &
receiver_ssh_pid=$!
sleep 2
ssh -- "$sender" "$(render_remote "${sender_command[@]}")" >"$output/sender-application.log" 2>&1
wait "$receiver_ssh_pid"
receiver_ssh_pid=""

mkdir -p "$output/application"
scp -- "$sender:$sender_run/sender-result.env" "$output/application/sender-result.env"
scp -- "$receiver:$receiver_run/receiver-result.env" "$output/application/receiver-result.env"
bash scripts/perf/package-two-node-evidence.sh \
  --output "$output/evidence" \
  --sender-dir "$output/preflight/raw/sender" \
  --receiver-dir "$output/preflight/raw/receiver" \
  --sender-result "$output/application/sender-result.env" \
  --receiver-result "$output/application/receiver-result.env"
cat "$output/evidence/summary.md"
