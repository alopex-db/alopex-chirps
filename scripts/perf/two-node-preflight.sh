#!/usr/bin/env bash
# Capture a single-direction iperf3 measurement on one member of a physical
# two-host Chirps performance lab. This measures only the network prerequisite.
set -euo pipefail
umask 077

usage() {
  cat <<'USAGE'
Usage:
  two-node-preflight.sh --role receiver --output DIR [--bind ADDRESS] [--port PORT]
  two-node-preflight.sh --role sender --output DIR --receiver ADDRESS [--bind ADDRESS]
                        [--port PORT] [--duration SECONDS]

The output directory must not contain prior evidence. The receiver exits after
one sender run. Both roles record host facts and the checked-out source SHA.
USAGE
}

role=""
output=""
receiver=""
bind=""
port="5201"
duration="30"
expected_sha=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --role) role="${2:?missing value for --role}"; shift 2 ;;
    --output) output="${2:?missing value for --output}"; shift 2 ;;
    --receiver) receiver="${2:?missing value for --receiver}"; shift 2 ;;
    --bind) bind="${2:?missing value for --bind}"; shift 2 ;;
    --port) port="${2:?missing value for --port}"; shift 2 ;;
    --duration) duration="${2:?missing value for --duration}"; shift 2 ;;
    --expected-sha) expected_sha="${2:?missing value for --expected-sha}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

case "$role" in receiver|sender) ;; *) printf '%s\n' '--role must be receiver or sender' >&2; exit 2 ;; esac
[[ -n "$output" ]] || { printf '%s\n' '--output is required' >&2; exit 2; }
[[ "$port" =~ ^[0-9]+$ ]] || { printf '%s\n' '--port must be numeric' >&2; exit 2; }
[[ "$duration" =~ ^[1-9][0-9]*$ ]] || { printf '%s\n' '--duration must be a positive integer' >&2; exit 2; }
if [[ "$role" == sender && -z "$receiver" ]]; then
  printf '%s\n' '--receiver is required for the sender role' >&2
  exit 2
fi
if [[ -e "$output" ]] && [[ -n "$(find "$output" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]]; then
  printf 'refusing to overwrite non-empty evidence directory: %s\n' "$output" >&2
  exit 2
fi

for command in git iperf3 python3 sha256sum; do
  command -v "$command" >/dev/null || { printf 'required command not found: %s\n' "$command" >&2; exit 127; }
done

mkdir -p "$output"
source_sha="$(git rev-parse HEAD)"
if [[ -n "$expected_sha" && "$source_sha" != "$expected_sha" ]]; then
  printf 'checked-out SHA %s does not match expected SHA %s\n' "$source_sha" "$expected_sha" >&2
  exit 1
fi

{
  printf 'schema_version=1\n'
  printf 'kind=chirps-two-node-network-preflight\n'
  printf 'role=%s\n' "$role"
  printf 'source_sha=%s\n' "$source_sha"
  printf 'started_at_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'port=%s\n' "$port"
  printf 'duration_seconds=%s\n' "$duration"
  printf 'bind=%s\n' "$bind"
  printf 'receiver=%s\n' "$receiver"
} >"$output/run.env"

{
  printf '%s\n' '# hostname'; hostname 2>&1 || true
  printf '%s\n' '# uname'; uname -a 2>&1 || true
  printf '%s\n' '# kernel command line'; cat /proc/cmdline 2>&1 || true
  printf '%s\n' '# cpu'; lscpu 2>&1 || true
  printf '%s\n' '# memory'; free -h 2>&1 || true
  printf '%s\n' '# network links'; ip -j link 2>&1 || true
  printf '%s\n' '# routes'; ip route 2>&1 || true
  printf '%s\n' '# disk'; lsblk -o NAME,TYPE,SIZE,ROTA,MODEL,MOUNTPOINTS 2>&1 || true
} >"$output/host-facts.txt"

iperf_args=(--port "$port" --json)
if [[ -n "$bind" ]]; then
  iperf_args+=(--bind "$bind")
fi

if [[ "$role" == receiver ]]; then
  # --one-off prevents a controller interruption from leaving a daemon behind.
  iperf3 --server --one-off "${iperf_args[@]}" >"$output/iperf3-receiver.json" 2>"$output/iperf3-receiver.stderr"
  printf 'completed_at_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$output/run.env"
  exit 0
fi

if ! iperf3 --client "$receiver" --time "$duration" --get-server-output "${iperf_args[@]}" >"$output/iperf3-sender.json" 2>"$output/iperf3-sender.stderr"; then
  printf 'status=failed\n' >>"$output/run.env"
  exit 1
fi

python3 - "$output/iperf3-sender.json" >"$output/throughput.env" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    document = json.load(handle)
summary = document.get("end", {}).get("sum_sent", {})
bits_per_second = summary.get("bits_per_second")
if not isinstance(bits_per_second, (int, float)) or bits_per_second <= 0:
    raise SystemExit("iperf3 JSON does not contain a positive end.sum_sent.bits_per_second")
print(f"iperf_bits_per_second={bits_per_second:.0f}")
print(f"iperf_bytes_per_second={bits_per_second / 8:.0f}")
print("status=passed")
PY
printf 'completed_at_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$output/run.env"
