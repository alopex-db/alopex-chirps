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
    [--compression none|zstd|zstd-level:N] [--payload-profile PROFILE]
    [--detailed-metrics]
    [--skip-lower-validation]

First runs containerized FileTransfer/QUIC tests and component Criterion for the
same source SHA. It then builds (unless --image is supplied) a source-SHA-labelled
performance image, creates an internal user-defined Docker bridge, and runs one
warm-up plus five fresh sender/receiver process pairs. The profile is fixed as
ft-1g-v1:

  128 MiB, compression=none, chunk=1 MiB, concurrency=4,
  sender/receiver tmpfs, 1gbit TBF + 1ms netem + 0% loss in both directions,
  MTU 1500, distinct cpusets, and a 2 GiB memory/no-swap limit per container.

--output must be absent or empty. The output contains no TLS key or payload.
USAGE
}

output=""
image=""
# WSL2 calibration host: four dedicated vCPUs per process reduce scheduler
# contention while preserving disjoint sender/receiver CPU sets.
sender_cpus="0-3"
receiver_cpus="4-7"
compression="none"
payload_profile="mixed"
detailed_metrics=false
skip_lower_validation=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output) output="${2:?missing value for --output}"; shift 2 ;;
    --image) image="${2:?missing value for --image}"; shift 2 ;;
    --sender-cpus) sender_cpus="${2:?missing value for --sender-cpus}"; shift 2 ;;
    --receiver-cpus) receiver_cpus="${2:?missing value for --receiver-cpus}"; shift 2 ;;
    --compression) compression="${2:?missing value for --compression}"; shift 2 ;;
    --payload-profile) payload_profile="${2:?missing value for --payload-profile}"; shift 2 ;;
    --detailed-metrics) detailed_metrics=true; shift ;;
    --skip-lower-validation) skip_lower_validation=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

detailed_metrics_env=1
[[ "$detailed_metrics" == true ]] && detailed_metrics_env=0

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
host_platform="native-linux"
if grep -qiE 'microsoft|wsl' /proc/version; then
  host_platform="wsl"
fi
host_platform_eligible=false
if [[ "$host_platform" == "native-linux" || "$host_platform" == "wsl" ]]; then
  # WSL2 is an accepted controlled execution platform when all host/kernel/
  # container facts are recorded. Swap enforcement remains a separate datum;
  # it must not erase valid performance or memory-safety evidence.
  host_platform_eligible=true
fi
docker_warnings="$(docker info --format '{{range .Warnings}}{{println .}}{{end}}' 2>&1 || true)"
swap_limit_enforced=true
if grep -qi 'swap limit' <<<"$docker_warnings"; then
  swap_limit_enforced=false
fi
profile_environment_eligible="$host_platform_eligible"
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

# Lower-layer validation is deliberately mandatory and precedes creation of
# the product measurement containers. A host-only test pass is not sufficient
# evidence for the containerized binary path.
container_validation_image_digest="skipped"
if [[ "$skip_lower_validation" != true ]]; then
  bash scripts/perf/run-container-file-transfer-validation.sh \
    --output "$output/container-validation"
  container_validation_result="$output/container-validation/evidence/result.json"
  python3 - "$container_validation_result" "$source_sha" <<'PY'
import json
import pathlib
import sys

result = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
if result.get("source_sha") != sys.argv[2]:
    raise SystemExit("container validation source SHA does not match product source SHA")
if result.get("passed") is not True:
    raise SystemExit("container lower-layer validation did not pass")
PY
  container_validation_image_digest="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["image_digest"])' "$container_validation_result")"
fi

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
  printf 'host_platform=%s\n' "$host_platform"
  printf 'host_platform_eligible=%s\n' "$host_platform_eligible"
  printf 'swap_limit_enforced=%s\n' "$swap_limit_enforced"
  printf 'profile_environment_eligible=%s\n' "$profile_environment_eligible"
  printf 'container_validation_passed=true\n'
  printf 'container_validation_image_digest=%s\n' "$container_validation_image_digest"
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
  printf 'compression=%s\n' "$compression"
  printf 'payload_profile=%s\n' "$payload_profile"
  printf 'started_at_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >"$output/run.env"

{
  printf '%s\n' '# uname'; uname -a
  printf '%s\n' '# proc-version'; cat /proc/version
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
    --env "CHIRPS_DISABLE_DETAILED_METRICS=$detailed_metrics_env" \
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

generate_payload() {
  python3 - "$FILE_BYTES" "$payload_profile" <<'PY'
import sys

size = int(sys.argv[1])
profile = sys.argv[2]
if profile not in {"mixed", "incompressible", "highly-compressible"}:
    raise SystemExit("payload profile must be mixed, incompressible, or highly-compressible")
state = 0x9E3779B9
remaining = size
block = 1024 * 1024
while remaining:
    n = min(block, remaining)
    data = bytearray(n)
    for i in range(n):
        state = (1664525 * state + 1013904223) & 0xFFFFFFFF
        random_byte = (state >> 24) & 0xFF
        if profile == "highly-compressible":
            data[i] = 0 if (i % 64) else ord("A")
        elif profile == "incompressible":
            data[i] = random_byte
        else:
            # Approximate application files: repeated structured text, sparse
            # zero regions, and incompressible binary regions in each chunk.
            region = (i * 100) // n
            data[i] = 0 if region < 35 else (ord("{" if i % 2 else "\n") if region < 60 else random_byte)
    sys.stdout.buffer.write(data)
    remaining -= n
PY
}

run_transfer() {
  local label="$1"
  local sample_dir="$output/samples/$label"
  local sender_dir="/work/$label-sender"
  local receiver_dir="/work/$label-receiver"
  local receiver_log="$sample_dir/receiver.log"
  local sender_log="$sample_dir/sender.log"

  mkdir -p "$sample_dir"
  docker exec "$sender_name" sh -ceu 'mkdir -p -- "$1"' sh "$sender_dir"
  generate_payload | docker exec -i "$sender_name" sh -ceu 'cat > "$1/throughput-source.bin" && sync' sh "$sender_dir"
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
    --expected-bytes "$FILE_BYTES" \
    --compression "$compression" >"$receiver_log" 2>&1 &
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
    --expected-bytes "$FILE_BYTES" \
    --compression "$compression" \
    --resumable true >"$sender_log" 2>&1
  # The receiver is a long-lived service and may keep its QUIC listener open
  # after writing the completion report. Do not wait for process exit here;
  # wait for the product completion contract, then terminate the exec task.
  for attempt in $(seq 1 100); do
    if docker exec "$receiver_name" sh -ceu \
      'test -s "$1" && grep -q "^completed=true$" "$1"' sh "$receiver_dir/receiver-result.env"; then
      break
    fi
    sleep 0.05
  done
  docker exec "$receiver_name" sh -ceu \
    'test -s "$1" && grep -q "^completed=true$" "$1"' sh "$receiver_dir/receiver-result.env"
  # docker exec may retain the child process while the service keeps its
  # listener alive. The report is the completion contract; terminate only the
  # host-side exec wrapper and do not block the next sample on process exit.
  kill -KILL "$receiver_exec_pid" 2>/dev/null || true
  receiver_exec_pid=""
  docker exec "$sender_name" cat "$sender_dir/sender-result.env" >"$sample_dir/sender-result.env"
  docker exec "$receiver_name" cat "$receiver_dir/receiver-result.env" >"$sample_dir/receiver-result.env"
  docker exec "$receiver_name" sha256sum "$receiver_dir/throughput-dest.bin" >"$sample_dir/destination.sha256"
  # Capture cgroup-v2 memory usage before removing the per-sample workload.
  # `memory.peak` is the authoritative container cgroup peak when supported;
  # docker stats alone reports only an instantaneous value.
  for name in "$sender_name" "$receiver_name"; do
    docker exec "$name" sh -ceu 'cat /sys/fs/cgroup/cpu.stat' >"$sample_dir/$name.cpu.before" 2>&1 || true
  done
  for name in "$sender_name" "$receiver_name"; do
    docker exec "$name" sh -ceu 'cat /sys/fs/cgroup/memory.current' >"$sample_dir/$name.memory.current" 2>&1 || true
    docker exec "$name" sh -ceu 'cat /sys/fs/cgroup/memory.peak' >"$sample_dir/$name.memory.peak" 2>&1 || true
  done
  docker exec "$sender_name" rm -rf -- "$sender_dir"
  docker exec "$receiver_name" rm -rf -- "$receiver_dir"
  for name in "$sender_name" "$receiver_name"; do
    docker exec "$name" sh -ceu 'cat /sys/fs/cgroup/cpu.stat' >"$sample_dir/$name.cpu.after" 2>&1 || true
    docker exec "$name" sh -ceu 'cat /sys/fs/cgroup/memory.current' >"$sample_dir/$name.memory.post_cleanup" 2>&1 || true
  done
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

python3 - "$output" "$source_sha" "$image_digest" "$host_platform" "$host_platform_eligible" "$swap_limit_enforced" "$profile_environment_eligible" "$container_validation_image_digest" "$compression" "$payload_profile" "$detailed_metrics" <<'PY'
import json
import pathlib
import statistics
import sys

root = pathlib.Path(sys.argv[1])
source_sha = sys.argv[2]
image_digest = sys.argv[3]
host_platform = sys.argv[4]
host_platform_eligible = sys.argv[5] == "true"
swap_limit_enforced = sys.argv[6] == "true"
profile_environment_eligible = sys.argv[7] == "true"
container_validation_image_digest = sys.argv[8]
compression = sys.argv[9]
payload_profile = sys.argv[10]
detailed_metrics_enabled = sys.argv[11] == "true"

def env(path):
    values = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        if "=" in raw:
            key, value = raw.split("=", 1)
            values[key] = value
    return values

def phase_metrics(report):
    return {key: value for key, value in report.items() if key.startswith("phase_")}

def cgroup_value(directory, suffix, role):
    paths = sorted(directory.glob(f"*{role}*.memory.{suffix}"))
    if not paths:
        return None

def cpu_delta(directory, role, key):
    matches = sorted(directory.glob(f"*{role}*.cpu.*"))
    before = next((p for p in matches if p.name.endswith("cpu.before")), None)
    after = next((p for p in matches if p.name.endswith("cpu.after")), None)
    if not before or not after:
        return None
    def read(path):
        values = {}
        for line in path.read_text(encoding="utf-8").splitlines():
            parts = line.split()
            if len(parts) == 2:
                values[parts[0]] = int(parts[1])
        return values
    try:
        return read(after).get(key, 0) - read(before).get(key, 0)
    except (OSError, ValueError):
        return None
    try:
        return int(paths[0].read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None

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
    try:
        retry_count = int(sender["retry_count"])
    except (KeyError, ValueError):
        retry_count = None
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
        and report.get("resumable") == "true"
        for report in (sender, receiver)
    )
    samples.append(
        {
            "sample": number,
            "end_to_end_goodput_bytes_per_second": goodput,
            "payload_progress_bytes_per_second": sender.get("payload_bytes_per_second"),
            "retry_count": retry_count,
            "retry_count_observed": retry_count is not None,
            "sender_phase_metrics": phase_metrics(sender),
            "receiver_phase_metrics": phase_metrics(receiver),
            "sender_receiver_destination_sha256_match": bool(integrity),
            "identity_match": identity,
            "sender_memory_current_bytes": cgroup_value(directory, "current", "sender"),
            "sender_memory_peak_bytes": cgroup_value(directory, "peak", "sender"),
            "receiver_memory_current_bytes": cgroup_value(directory, "current", "receiver"),
            "receiver_memory_peak_bytes": cgroup_value(directory, "peak", "receiver"),
            "sender_memory_post_cleanup_bytes": cgroup_value(directory, "post_cleanup", "sender"),
            "receiver_memory_post_cleanup_bytes": cgroup_value(directory, "post_cleanup", "receiver"),
            "sender_cpu_usage_usec": cpu_delta(directory, "sender", "usage_usec"),
            "sender_cpu_user_usec": cpu_delta(directory, "sender", "user_usec"),
            "sender_cpu_system_usec": cpu_delta(directory, "sender", "system_usec"),
            "receiver_cpu_usage_usec": cpu_delta(directory, "receiver", "usage_usec"),
            "receiver_cpu_user_usec": cpu_delta(directory, "receiver", "user_usec"),
            "receiver_cpu_system_usec": cpu_delta(directory, "receiver", "system_usec"),
        }
    )

goodputs = [sample["end_to_end_goodput_bytes_per_second"] for sample in samples]
complete = all(value is not None and value > 0 for value in goodputs)
integrity = all(sample["sender_receiver_destination_sha256_match"] for sample in samples)
identity = all(sample["identity_match"] for sample in samples)
threshold = 100_000_000
threshold_passed = complete and all(value >= threshold for value in goodputs)
post_cleanup = [
    value
    for sample in samples
    for value in (
        sample["sender_memory_post_cleanup_bytes"],
        sample["receiver_memory_post_cleanup_bytes"],
    )
    if value is not None
]
post_cleanup_growth = None
post_cleanup_stable = None
if len(post_cleanup) >= 4:
    first = max(post_cleanup[:2])
    last = max(post_cleanup[-2:])
    post_cleanup_growth = last - first
    post_cleanup_stable = post_cleanup_growth <= 16 * 1024 * 1024
result = {
    "schema_version": 1,
    "evidence_class": "product-controlled-container",
    "profile_id": "ft-1g-v1",
    "source_sha": source_sha,
    "image_digest": image_digest,
    "host_platform": host_platform,
    "host_platform_eligible": host_platform_eligible,
    "swap_limit_enforced": swap_limit_enforced,
    "profile_environment_eligible": profile_environment_eligible,
    "container_validation_passed": True,
    "container_validation_image_digest": container_validation_image_digest,
    "file_bytes": 134217728,
    "compression": compression,
    "payload_profile": payload_profile,
    "detailed_metrics_enabled": detailed_metrics_enabled,
    "sample_count": len(samples),
    "minimum_end_to_end_goodput_bytes_per_second": threshold,
    "samples": samples,
    "minimum_observed_end_to_end_goodput_bytes_per_second": min(goodputs) if complete else None,
    "median_end_to_end_goodput_bytes_per_second": statistics.median(goodputs) if complete else None,
    "integrity_passed": integrity,
    "identity_passed": identity,
    "post_cleanup_memory_growth_bytes": post_cleanup_growth,
    "post_cleanup_memory_stable_within_16MiB": post_cleanup_stable,
    "product_performance_passed": profile_environment_eligible and threshold_passed and integrity and identity,
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
    f"- Host platform: `{host_platform}`",
    f"- Profile environment: `{'ELIGIBLE' if profile_environment_eligible else 'INELIGIBLE'}`",
    f"- Swap limit enforced: `{'YES' if swap_limit_enforced else 'NO'}`",
    f"- Samples: `{len(samples)}`",
    f"- Minimum required goodput: `{threshold} B/s`",
    f"- Product performance: `{'PASS' if result['product_performance_passed'] else 'FAIL'}`",
    f"- Integrity: `{'PASS' if integrity else 'FAIL'}`",
]
for sample in samples:
    value = sample["end_to_end_goodput_bytes_per_second"]
    summary.append(f"- Sample {sample['sample']} goodput: `{value:.0f} B/s`" if value is not None else f"- Sample {sample['sample']} goodput: `missing`")
    summary.append(f"  - peak sender/receiver: `{sample['sender_memory_peak_bytes']} / {sample['receiver_memory_peak_bytes']} B`; post-cleanup: `{sample['sender_memory_post_cleanup_bytes']} / {sample['receiver_memory_post_cleanup_bytes']} B`")
(root / "evidence" / "summary.md").write_text("\n".join(summary) + "\n", encoding="utf-8")
PY

(
  cd "$output"
  find . -type f ! -name manifest.sha256 -print0 | sort -z | xargs -0 sha256sum > manifest.sha256
)
printf 'controlled-container evidence: %s/evidence\n' "$output"
