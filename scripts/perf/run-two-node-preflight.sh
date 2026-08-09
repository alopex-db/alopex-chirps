#!/usr/bin/env bash
# Coordinates the two role scripts from a dedicated controller. The caller
# provisions SSH access and identical checkouts on both physical hosts.
set -euo pipefail
umask 077

usage() {
  cat <<'USAGE'
Usage:
  run-two-node-preflight.sh --sender SSH_TARGET --receiver SSH_TARGET
    --receiver-address ADDRESS --remote-workdir DIR --remote-output-root DIR
    --output DIR [--sender-bind ADDRESS] [--receiver-bind ADDRESS]
    [--port PORT] [--duration SECONDS] [--protocol udp|tcp] [--bitrate RATE]

SSH targets must be administrative lab hosts. The controller copies only
network facts and iperf3 results into --output; no TLS key or test payload is
copied into the evidence bundle. The default `udp`/`100M`/`15` profile is the
PATH-UDP-100 deployment reachability diagnostic, not a Chirps throughput test.
USAGE
}

sender=""
receiver=""
receiver_address=""
remote_workdir=""
remote_output_root=""
output=""
sender_bind=""
receiver_bind=""
port="5201"
duration="15"
protocol="udp"
bitrate="100M"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --sender) sender="${2:?missing value for --sender}"; shift 2 ;;
    --receiver) receiver="${2:?missing value for --receiver}"; shift 2 ;;
    --receiver-address) receiver_address="${2:?missing value for --receiver-address}"; shift 2 ;;
    --remote-workdir) remote_workdir="${2:?missing value for --remote-workdir}"; shift 2 ;;
    --remote-output-root) remote_output_root="${2:?missing value for --remote-output-root}"; shift 2 ;;
    --output) output="${2:?missing value for --output}"; shift 2 ;;
    --sender-bind) sender_bind="${2:?missing value for --sender-bind}"; shift 2 ;;
    --receiver-bind) receiver_bind="${2:?missing value for --receiver-bind}"; shift 2 ;;
    --port) port="${2:?missing value for --port}"; shift 2 ;;
    --duration) duration="${2:?missing value for --duration}"; shift 2 ;;
    --protocol) protocol="${2:?missing value for --protocol}"; shift 2 ;;
    --bitrate) bitrate="${2:?missing value for --bitrate}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done
for value in sender receiver receiver_address remote_workdir remote_output_root output; do
  [[ -n "${!value}" ]] || { printf 'missing required option --%s\n' "${value//_/-}" >&2; exit 2; }
done
[[ "$sender" != "$receiver" ]] || { printf '%s\n' '--sender and --receiver must be distinct physical hosts' >&2; exit 2; }
for command in git ssh scp bash; do
  command -v "$command" >/dev/null || { printf 'required command not found: %s\n' "$command" >&2; exit 127; }
done
if [[ -e "$output" ]] && [[ -n "$(find "$output" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]]; then
  printf 'refusing to overwrite non-empty evidence directory: %s\n' "$output" >&2
  exit 2
fi
mkdir -p "$output"
source_sha="$(git rev-parse HEAD)"
run_id="${source_sha:0:12}-$(date -u +%Y%m%dT%H%M%SZ)"
sender_remote="$remote_output_root/$run_id-sender"
receiver_remote="$remote_output_root/$run_id-receiver"

remote_command() {
  local remote_dir="$1"
  local role="$2"
  local remote_bind="$3"
  local command=(bash scripts/perf/two-node-preflight.sh --role "$role" --output "$remote_dir" --port "$port" --duration "$duration" --protocol "$protocol" --bitrate "$bitrate" --expected-sha "$source_sha")
  if [[ -n "$remote_bind" ]]; then command+=(--bind "$remote_bind"); fi
  if [[ "$role" == sender ]]; then command+=(--receiver "$receiver_address"); fi
  local rendered=""
  printf -v rendered '%q ' "${command[@]}"
  printf 'cd %q && mkdir -p %q && %s' "$remote_workdir" "$remote_output_root" "$rendered"
}

receiver_command="$(remote_command "$receiver_remote" receiver "$receiver_bind")"
sender_command="$(remote_command "$sender_remote" sender "$sender_bind")"
ssh -- "$receiver" "$receiver_command" >"$output/receiver-controller.log" 2>&1 &
receiver_ssh_pid=$!
cleanup() {
  if kill -0 "$receiver_ssh_pid" 2>/dev/null; then
    kill "$receiver_ssh_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

# The receiver exits after one transfer; this bounded delay avoids a retry loop
# while still allowing the remote command to bind its port.
sleep 2
ssh -- "$sender" "$sender_command" >"$output/sender-controller.log" 2>&1
wait "$receiver_ssh_pid"
trap - EXIT INT TERM

mkdir -p "$output/raw"
scp -r -- "$sender:$sender_remote" "$output/raw/sender"
scp -r -- "$receiver:$receiver_remote" "$output/raw/receiver"
bash scripts/perf/package-two-node-evidence.sh \
  --output "$output/evidence" \
  --sender-dir "$output/raw/sender" \
  --receiver-dir "$output/raw/receiver"
printf 'controller evidence: %s/evidence\n' "$output"
