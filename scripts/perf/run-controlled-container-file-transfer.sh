#!/usr/bin/env bash
# Runs the ft-1g-v1 FileTransfer product-performance profile.
#
# This intentionally measures two independent Chirps processes in two Docker
# containers. It does not use host networking, a published port, a shared
# payload mount, or an external LAN. The resulting image digest, container
# limits, qdisc configuration, reports, and manifest are retained; TLS keys and
# payloads are removed by the cleanup trap and are never copied to --output.
set -euo pipefail
umask 077

readonly PROFILE_ID="ft-1g-v1"
readonly FILE_BYTES="134217728"
readonly SAMPLE_COUNT="5"
readonly CHUNK_SIZE="1048576"
readonly CONCURRENCY="4"
readonly NETWORK_RATE="1gbit"
readonly NETWORK_DELAY="1ms"
readonly NETWORK_LOSS="0%"
readonly NETWORK_MTU="1500"
readonly CONTAINER_MEMORY="2g"
readonly TMPFS_BYTES="536870912"
readonly TLS_TMPFS_BYTES="16777216"

usage() {
  cat <<'USAGE'
Usage:
  run-controlled-container-file-transfer.sh --output DIR
    [--image IMAGE] [--sender-cpus CPUSET] [--receiver-cpus CPUSET]

Builds (unless --image is supplied) a source-SHA-labelled performance image,
creates an internal user-defined Docker bridge, and runs one warm-up plus five
fresh sender/receiver process pairs. The profile is fixed as ft-1g-v1:

  128 MiB, compression=none, chunk=1 MiB, concurrency=4,
  sender/receiver tmpfs, 1gbit TBF + 1ms netem + 0% loss in both directions,
  MTU 1500, distinct cpusets, and a 2 GiB memory/no-swap limit per container.

--output must be absent or empty. The output contains no TLS key or payload.
USAGE
}

output=""
image=""
sender_cpus="0-1"
receiver_cpus="2-3"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output) output="${2:?missing value for --output}"; shift 2 ;;
    --image) image="${2:?missing value for --image}"; shift 2 ;;
    --sender-cpus) sender_cpus="${2:?missing value for --sender-cpus}"; shift 2 ;;
    --receiver-cpus) receiver_cpus="${2:?missing value for --receiver-cpus}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "$output" ]] || { printf '%s\n' '--output is required' >&2; exit 2; }
[[ -n "$sender_cpus" && -n "$receiver_cpus" && "$sender_cpus" != "$receiver_cpus" ]] || {
  printf '%s\n' 'sender and receiver must use distinct non-empty --*-cpus values' >&2
  exit 2
}
if [[ -e "$output" ]] && [[ -n "$(find "$output" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]]; then
  printf 'refusing to overwrite non-empty output directory: %s\n' "$output" >&2
  exit 2
fi
for command in docker git tar mktemp openssl python3 sha256sum; do
  command -v "$command" >/dev/null || { printf 'required command not found: %s\n' "$command" >&2; exit 127; }
done
[[ -z "$(git status --porcelain)" ]] || {
  printf '%s\n' 'refusing dirty source tree: commit the exact source before creating an immutable performance image' >&2
  exit 2
}

source_sha="$(git rev-parse HEAD)"
run_id="${source_sha:0:12}-$(date -u +%Y%m%dT%H%M%SZ)-$$"
network_name="chirps-ft-${run_id}"
sender_name="chirps-ft-sender-${run_id}"
receiver_name="chirps-ft-receiver-${run_id}"
build_context="$(mktemp -d /tmp/chirps-ft-1g-context.XXXXXX)"
tls_dir="$(mktemp -d /tmp/chirps-ft-1g-tls.XXXXXX)"
receiver_exec_pid=""
sender_started=false
receiver_started=false
network_started=false

cleanup() {
  local status=$?
  if [[ -n "$receiver_exec_pid" ]] && kill -0 "$receiver_exec_pid" 2>/dev/null; then
    kill "$receiver_exec_pid" 2>/dev/null || true
    wait "$receiver_exec_pid" 2>/dev/null || true
  fi
  if [[ "$sender_started" == true ]]; then
    docker rm -f "$sender_name" >/dev/null 2>&1 || true
  fi
  if [[ "$receiver_started" == true ]]; then
    docker rm -f "$receiver_name" >/dev/null 2>&1 || true
  fi
  if [[ "$network_started" == true ]]; then
    docker network rm "$network_name" >/dev/null 2>&1 || true
  fi
  rm -rf -- "$build_context" "$tls_dir"
  exit "$status"
}
trap cleanup EXIT INT TERM

mkdir -p "$output/samples" "$output/host" "$output/containers"

if [[ -z "$image" ]]; then
  image="chirps-ft-1g:${source_sha}"
  git archive --format=tar "$source_sha" | tar -x -C "$build_context"
  docker build \
    --file "$build_context/scripts/perf/container/Dockerfile" \
    --build-arg "SOURCE_SHA=$source_sha" \
    --tag "$image" \
    "$build_context" >"$output/image-build.log" 2>&1
fi

image_digest="$(docker image inspect --format '{{.Id}}' "$image")"
image_source_sha="$(docker image inspect --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' "$image")"
[[ "$image_source_sha" == "$source_sha" ]] || {
  printf 'image source label %s does not match checked-out SHA %s\n' "$image_source_sha" "$source_sha" >&2
  exit 2
}

{
  printf 'schema_version=1\n'
  printf 'kind=chirps-file-transfer-controlled-container\n'
  printf 'profile_id=%s\n' "$PROFILE_ID"
  printf 'source_sha=%s\n' "$source_sha"
  printf 'image=%s\n' "$image"
  printf 'image_digest=%s\n' "$image_digest"
  printf 'file_bytes=%s\n' "$FILE_BYTES"
  printf 'sample_count=%s\n' "$SAMPLE_COUNT"
  printf 'chunk_size=%s\n' "$CHUNK_SIZE"
  printf 'concurrency=%s\n' "$CONCURRENCY"
  printf 'network_rate=%s\n' "$NETWORK_RATE"
  printf 'network_delay=%s\n' "$NETWORK_DELAY"
  printf 'network_loss=%s\n' "$NETWORK_LOSS"
  printf 'network_mtu=%s\n' "$NETWORK_MTU"
  printf 'container_memory=%s\n' "$CONTAINER_MEMORY"
  printf 'sender_cpus=%s\n' "$sender_cpus"
  printf 'receiver_cpus=%s\n' "$receiver_cpus"
  printf 'started_at_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >"$output/run.env"

{
  printf '%s\n' '# uname'; uname -a
  printf '%s\n' '# cpu'; lscpu
  printf '%s\n' '# memory'; free -h
  printf '%s\n' '# docker version'; docker version
  printf '%s\n' '# docker info'; docker info --format '{{json .}}'
} >"$output/host/facts.txt" 2>&1

docker network create \
  --internal \
  --driver bridge \
  --opt "com.docker.network.driver.mtu=$NETWORK_MTU" \
  "$network_name" >"$output/containers/network-id.txt"
network_started=true

launch_container() {
  local name="$1"
  local cpus="$2"
  docker run --detach --rm \
    --name "$name" \
    --network "$network_name" \
    --cpuset-cpus "$cpus" \
    --memory "$CONTAINER_MEMORY" \
    --memory-swap "$CONTAINER_MEMORY" \
    --cap-add NET_ADMIN \
    --tmpfs "/work:rw,size=$TMPFS_BYTES,mode=700" \
    --tmpfs "/run/chirps:rw,size=$TLS_TMPFS_BYTES,mode=700" \
    "$image" sleep infinity >/dev/null
  docker exec --user root "$name" sh -ceu '
    chown chirps:chirps /work /run/chirps
    mkdir -p /run/chirps/tls
    chown chirps:chirps /run/chirps/tls
  '
}

launch_container "$sender_name" "$sender_cpus"
sender_started=true
launch_container "$receiver_name" "$receiver_cpus"
receiver_started=true

bash scripts/perf/create-lab-tls.sh --output "$tls_dir"
for name in "$sender_name" "$receiver_name"; do
  docker exec -i "$name" sh -ceu 'cat > /run/chirps/tls/cert.der' <"$tls_dir/cert.der"
  docker exec -i "$name" sh -ceu 'cat > /run/chirps/tls/key.der' <"$tls_dir/key.der"
  docker exec --user root "$name" sh -ceu '
    chown chirps:chirps /run/chirps/tls/cert.der /run/chirps/tls/key.der
    chmod 600 /run/chirps/tls/cert.der /run/chirps/tls/key.der
  '
  docker exec --user root "$name" tc qdisc replace dev eth0 root handle 1: \
    tbf rate "$NETWORK_RATE" burst 1mb latency 50ms
  docker exec --user root "$name" tc qdisc replace dev eth0 parent 1:1 handle 10: \
    netem delay "$NETWORK_DELAY" loss "$NETWORK_LOSS"
done

sender_ip="$(docker inspect --format "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" "$sender_name")"
receiver_ip="$(docker inspect --format "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" "$receiver_name")"
[[ -n "$sender_ip" && -n "$receiver_ip" ]] || { printf '%s\n' 'container IP discovery failed' >&2; exit 1; }

for name in "$sender_name" "$receiver_name"; do
  docker inspect "$name" >"$output/containers/$name.inspect.json"
done
docker network inspect "$network_name" >"$output/containers/network.inspect.json"

wait_for_receiver() {
  local attempt
  for attempt in $(seq 1 40); do
    if docker exec "$receiver_name" sh -c 'ss -lun | grep -q ":6202" && ss -lun | grep -q ":6302"'; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

run_transfer() {
  local label="$1"
  local sample_dir="$output/samples/$label"
  local sender_dir="/work/$label-sender"
  local receiver_dir="/work/$label-receiver"
  local receiver_log="$sample_dir/receiver.log"
  local sender_log="$sample_dir/sender.log"

  mkdir -p "$sample_dir"
  docker exec "$sender_name" sh -ceu '
    mkdir -p -- "$1"
    dd if=/dev/zero of="$1/throughput-source.bin" bs=1048576 count=128 status=none
    sync
  ' sh "$sender_dir"
  docker exec "$receiver_name" /usr/local/bin/two_node_transfer \
    --role receiver \
    --node-id 00000000000000000000000000000002 \
    --peer-id 00000000000000000000000000000001 \
    --control-bind "$receiver_ip:6202" \
    --peer-control "$sender_ip:6201" \
    --data-bind "$receiver_ip:6302" \
    --peer-data "$sender_ip:6301" \
    --base-path "$receiver_dir" \
    --cert /run/chirps/tls/cert.der \
    --key /run/chirps/tls/key.der \
    --report "$receiver_dir/receiver-result.env" \
    --source-sha "$source_sha" \
    --scope two-container-controlled \
    --profile-id "$PROFILE_ID" \
    --image-digest "$image_digest" \
    --destination throughput-dest.bin \
    --expected-bytes "$FILE_BYTES" >"$receiver_log" 2>&1 &
  receiver_exec_pid=$!
  wait_for_receiver || {
    printf 'receiver did not bind expected QUIC ports\n' >&2
    return 1
  }
  docker exec "$sender_name" /usr/local/bin/two_node_transfer \
    --role sender \
    --node-id 00000000000000000000000000000001 \
    --peer-id 00000000000000000000000000000002 \
    --control-bind "$sender_ip:6201" \
    --peer-control "$receiver_ip:6202" \
    --data-bind "$sender_ip:6301" \
    --peer-data "$receiver_ip:6302" \
    --base-path "$sender_dir" \
    --cert /run/chirps/tls/cert.der \
    --key /run/chirps/tls/key.der \
    --report "$sender_dir/sender-result.env" \
    --source-sha "$source_sha" \
    --scope two-container-controlled \
    --profile-id "$PROFILE_ID" \
    --image-digest "$image_digest" \
    --source throughput-source.bin \
    --destination throughput-dest.bin \
    --expected-bytes "$FILE_BYTES" >"$sender_log" 2>&1
  wait "$receiver_exec_pid"
  receiver_exec_pid=""
  docker cp "$sender_name:$sender_dir/sender-result.env" "$sample_dir/sender-result.env"
  docker cp "$receiver_name:$receiver_dir/receiver-result.env" "$sample_dir/receiver-result.env"
  docker exec "$receiver_name" sha256sum "$receiver_dir/throughput-dest.bin" >"$sample_dir/destination.sha256"
}

run_transfer warmup
for sample in $(seq 1 "$SAMPLE_COUNT"); do
  run_transfer "sample-$sample"
done

for name in "$sender_name" "$receiver_name"; do
  docker exec --user root "$name" tc -s qdisc show dev eth0 >"$output/containers/$name.qdisc.txt"
  docker exec "$name" cat /sys/fs/cgroup/cpu.stat >"$output/containers/$name.cpu.stat" 2>&1 || true
  docker exec "$name" cat /sys/fs/cgroup/memory.events >"$output/containers/$name.memory.events" 2>&1 || true
  docker stats --no-stream --format '{{json .}}' "$name" >"$output/containers/$name.stats.json"
done

python3 - "$output" "$source_sha" "$image_digest" <<'PY'
import json
import pathlib
import statistics
import sys

root = pathlib.Path(sys.argv[1])
source_sha = sys.argv[2]
image_digest = sys.argv[3]

def env(path):
    values = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        if "=" in raw:
            key, value = raw.split("=", 1)
            values[key] = value
    return values

samples = []
for number in range(1, 6):
    directory = root / "samples" / f"sample-{number}"
    sender = env(directory / "sender-result.env")
    receiver = env(directory / "receiver-result.env")
    destination_hash = (directory / "destination.sha256").read_text(encoding="utf-8").split()[0]
    try:
        goodput = float(sender["end_to_end_bytes_per_second"])
    except (KeyError, ValueError):
        goodput = None
    integrity = (
        sender.get("sha256")
        and sender.get("sha256") == receiver.get("sha256") == destination_hash
        and sender.get("completed") == receiver.get("completed") == "true"
    )
    identity = all(
        report.get("scope") == "two-container-controlled"
        and report.get("profile_id") == "ft-1g-v1"
        and report.get("source_sha") == source_sha
        and report.get("image_digest") == image_digest
        for report in (sender, receiver)
    )
    samples.append(
        {
            "sample": number,
            "end_to_end_goodput_bytes_per_second": goodput,
            "payload_progress_bytes_per_second": sender.get("payload_bytes_per_second"),
            "retry_count": None,
            "retry_count_observed": False,
            "sender_receiver_destination_sha256_match": bool(integrity),
            "identity_match": identity,
        }
    )

goodputs = [sample["end_to_end_goodput_bytes_per_second"] for sample in samples]
complete = all(value is not None and value > 0 for value in goodputs)
integrity = all(sample["sender_receiver_destination_sha256_match"] for sample in samples)
identity = all(sample["identity_match"] for sample in samples)
threshold = 100_000_000
threshold_passed = complete and all(value >= threshold for value in goodputs)
result = {
    "schema_version": 1,
    "evidence_class": "product-controlled-container",
    "profile_id": "ft-1g-v1",
    "source_sha": source_sha,
    "image_digest": image_digest,
    "file_bytes": 134217728,
    "sample_count": len(samples),
    "minimum_end_to_end_goodput_bytes_per_second": threshold,
    "samples": samples,
    "minimum_observed_end_to_end_goodput_bytes_per_second": min(goodputs) if complete else None,
    "median_end_to_end_goodput_bytes_per_second": statistics.median(goodputs) if complete else None,
    "integrity_passed": integrity,
    "identity_passed": identity,
    "product_performance_passed": threshold_passed and integrity and identity,
    "release_eligible": False,
    "release_eligibility_reason": "product-performance evidence is one v0.5.2 release input; it is not the complete release contract",
}
(root / "evidence").mkdir(exist_ok=True)
(root / "evidence" / "result.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
summary = [
    "# Chirps ft-1g-v1 controlled-container evidence",
    "",
    f"- Source SHA: `{source_sha}`",
    f"- Image digest: `{image_digest}`",
    f"- Samples: `{len(samples)}`",
    f"- Minimum required goodput: `{threshold} B/s`",
    f"- Product performance: `{'PASS' if result['product_performance_passed'] else 'FAIL'}`",
    f"- Integrity: `{'PASS' if integrity else 'FAIL'}`",
]
for sample in samples:
    value = sample["end_to_end_goodput_bytes_per_second"]
    summary.append(f"- Sample {sample['sample']} goodput: `{value:.0f} B/s`" if value is not None else f"- Sample {sample['sample']} goodput: `missing`")
(root / "evidence" / "summary.md").write_text("\n".join(summary) + "\n", encoding="utf-8")
PY

(
  cd "$output"
  find . -type f ! -name manifest.sha256 -print0 | sort -z | xargs -0 sha256sum > manifest.sha256
)
printf 'controlled-container evidence: %s/evidence\n' "$output"
