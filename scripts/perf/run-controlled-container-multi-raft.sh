#!/usr/bin/env bash
set -euo pipefail
umask 077

readonly COMPOSE_FILE="scripts/perf/compose.multi-raft.yml"
readonly NODES="192.168.129.11:7101,192.168.129.12:7102,192.168.129.13:7103"
readonly ORDER=(multi_raft:0 single_group:0 single_group:1 multi_raft:1 multi_raft:2 single_group:2 single_group:3 multi_raft:3 multi_raft:4 single_group:4)

usage() {
  printf '%s\n' 'Usage: run-controlled-container-multi-raft.sh --output EMPTY_DIR [--image IMAGE] [--resource-audit]'
}

output=""
image=""
resource_audit=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --output) output="${2:?missing --output value}"; shift 2 ;;
    --image) image="${2:?missing --image value}"; shift 2 ;;
    --resource-audit) resource_audit=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; exit 2 ;;
  esac
done
[[ -n "$output" ]] || { usage >&2; exit 2; }
for command in docker git tar mktemp openssl jq sha256sum lscpu; do
  command -v "$command" >/dev/null || { printf 'missing command: %s\n' "$command" >&2; exit 127; }
done
[[ -z "$(git status --porcelain)" ]] || { printf '%s\n' 'refusing dirty source tree' >&2; exit 2; }
if [[ -e "$output" && -n "$(find "$output" -mindepth 1 -print -quit 2>/dev/null)" ]]; then
  printf 'refusing non-empty output: %s\n' "$output" >&2; exit 2
fi
mkdir -p "$output"
output="$(realpath "$output")"
source_sha="$(git rev-parse HEAD)"
context="$(mktemp -d /tmp/chirps-mr-context.XXXXXX)"
tls="$(mktemp -d /tmp/chirps-mr-tls.XXXXXX)"
project="chirps-mr-${source_sha:0:10}-$$"
export PERF_OUTPUT="$output" PERF_TLS="$tls"
if [[ "$resource_audit" == true ]]; then
  export RESOURCE_AUDIT_ARGS="--resource-audit" METRICS_INTERVAL_ARGS="--metrics-interval-ms 250"
else
  export RESOURCE_AUDIT_ARGS="" METRICS_INTERVAL_ARGS=""
fi
export NODE1_CPU="${NODE1_CPU:-0-2}" LOADGEN1_CPU="${LOADGEN1_CPU:-3}"
export NODE2_CPU="${NODE2_CPU:-4-6}" LOADGEN2_CPU="${LOADGEN2_CPU:-7}"
export NODE3_CPU="${NODE3_CPU:-8-10}" LOADGEN3_CPU="${LOADGEN3_CPU:-11}"
export CONTROLLER_CPU="${CONTROLLER_CPU:-11}"

cleanup() {
  local status=$?
  docker compose --project-name "$project" --file "$COMPOSE_FILE" --profile tools down --volumes --remove-orphans >/dev/null 2>&1 || true
  rm -rf -- "$context" "$tls"
  exit "$status"
}
trap cleanup EXIT INT TERM

bash scripts/perf/create-lab-tls.sh --output "$tls"
if [[ -z "$image" ]]; then
  image="chirps-multi-raft-perf:${source_sha}"
  git archive --format=tar "$source_sha" | tar -x -C "$context"
  docker build --file "$context/scripts/perf/container/Dockerfile.multi-raft" \
    --build-arg "SOURCE_SHA=$source_sha" --tag "$image" "$context" >"$output/image-build.log" 2>&1
fi
export CHIRPS_PERF_IMAGE="$image"
image_sha="$(docker image inspect --format '{{.Id}}' "$image")"
image_source="$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$image")"
[[ "$image_source" == "$source_sha" ]] || { printf '%s\n' 'image/source SHA mismatch' >&2; exit 2; }
binary_sha="$(docker run --rm --entrypoint sha256sum "$image" /usr/local/bin/chirps-multi-raft-perf | awk '{print $1}')"
swap_before="$(awk '/SwapTotal/{t=$2}/SwapFree/{f=$2}END{print (t-f)*1024}' /proc/meminfo)"
: >"$output/summaries.ndjson"

compose() { docker compose --project-name "$project" --file "$COMPOSE_FILE" --profile tools "$@"; }
controller() { compose run --rm --no-deps controller "$@"; }

phase_snapshot() {
  [[ "$resource_audit" == true ]] || return 0
  local sample_name="$1" phase="$2" host_rss host_cpu node1_stats node2_stats node3_stats
  host_rss="$(awk '/VmRSS:/{print $2*1024; exit}' /proc/$$/status)"
  host_cpu="$(ps -o %cpu= -p $$ | awk '{print $1+0}')"
  node1_stats=""; node2_stats=""; node3_stats=""
  for node in 1 2 3; do
    cid="$(compose ps --quiet "node${node}" 2>/dev/null || true)"
    stats=""
    if [[ -n "$cid" ]]; then
      stats="$(docker stats --no-stream --format '{{.MemUsage}}|{{.MemPerc}}|{{.CPUPerc}}' "$cid" 2>/dev/null || true)"
    fi
    case "$node" in
      1) node1_stats="$stats" ;;
      2) node2_stats="$stats" ;;
      3) node3_stats="$stats" ;;
    esac
  done
  jq -cn --arg sample "$sample_name" --arg phase "$phase" --argjson host_rss "${host_rss:-0}" \
    --argjson host_cpu "$host_cpu" --arg node1 "$node1_stats" --arg node2 "$node2_stats" --arg node3 "$node3_stats" \
    '{sample:$sample,phase:$phase,monotonic_ns:now,runner_rss_bytes:$host_rss,runner_cpu_percent:$host_cpu,
      node_stats:{node1:$node1,node2:$node2,node3:$node3}}' >>"$output/phase-metrics.ndjson"
}

measure_network_rtt() {
  local destination_ip destination_node output_path ping_output samples_json source_node
  local -a samples
  output_path="$1"
  : >"${output_path}.ndjson"
  for source_node in 1 2 3; do
    for destination_node in 1 2 3; do
      [[ "$source_node" != "$destination_node" ]] || continue
      destination_ip="192.168.128.$((10 + destination_node))"
      ping_output="$(compose exec --user root --no-TTY "node${source_node}" ping -n -c 200 -i 0.01 -W 1 "$destination_ip")"
      mapfile -t samples < <(printf '%s\n' "$ping_output" | sed -n 's/.* time[=<]\([0-9.]*\) ms.*/\1/p')
      [[ "${#samples[@]}" == 200 ]] || { printf 'incomplete RTT probe %s -> %s\n' "$source_node" "$destination_node" >&2; return 1; }
      samples_json="$(printf '%s\n' "${samples[@]}" | jq -R 'tonumber' | jq -s .)"
      jq -cn --argjson source "$source_node" --argjson destination "$destination_node" --argjson samples "$samples_json" '
        ($samples | sort) as $ordered |
        {source:$source,destination:$destination,p50:$ordered[99],p95:$ordered[189],raw_samples_ms:$samples}
      ' >>"${output_path}.ndjson"
    done
  done
  jq -s . "${output_path}.ndjson" >"$output_path"
  rm -f -- "${output_path}.ndjson"
}

for item in "${ORDER[@]}"; do
  mode="${item%%:*}"
  index="${item##*:}"
  groups=100
  [[ "$mode" == single_group ]] && groups=1
  sample="samples/${mode}-${index}"
  export SAMPLE_DIR="$sample"
  mkdir -p "$output/$sample"
  phase_snapshot "$sample" sample-start
  compose up --detach node1 node2 node3
  phase_snapshot "$sample" nodes-started
  for attempt in $(seq 1 120); do
    if controller ctl --address 192.168.129.11:7101 --operation health >/dev/null 2>&1 \
      && controller ctl --address 192.168.129.12:7102 --operation health >/dev/null 2>&1 \
      && controller ctl --address 192.168.129.13:7103 --operation health >/dev/null 2>&1; then break; fi
    sleep 0.25
    [[ "$attempt" != 120 ]] || { printf '%s\n' 'node health timeout' >&2; exit 1; }
  done
  phase_snapshot "$sample" health-ready

  measure_network_rtt "$output/$sample/rtt-unloaded.json"
  phase_snapshot "$sample" rtt-unloaded
  for node in node1 node2 node3; do
    compose exec --user root --no-TTY "$node" sh -ceu '
      iface=$(ip -o -4 addr show | awk '\''$4 ~ /^192[.]168[.]128[.]/{print $2; exit}'\'')
      test -n "$iface"
      tc qdisc replace dev "$iface" root netem delay 250us
      tc -s qdisc show dev "$iface"
    ' >"$output/$sample/${node}-qdisc.txt"
  done
  phase_snapshot "$sample" network-shaped
  measure_network_rtt "$output/$sample/rtt-shaped.json"
  phase_snapshot "$sample" rtt-shaped
  controller bootstrap --nodes "$NODES" --groups "$groups"
  phase_snapshot "$sample" bootstrap-complete

  start_at="$(( $(controller monotonic) + 10000000000 ))"
  phase_snapshot "$sample" loadgen-start
  pids=()
  for node in 1 2 3; do
    compose run --rm --no-deps "loadgen${node}" loadgen \
      --origin-node "$node" --nodes "$NODES" \
      --mode "$mode" --sample-index "$index" --start-at-ns "$start_at" \
      --output "/evidence/$sample/loadgen${node}.json" \
      ${RESOURCE_AUDIT_ARGS} \
      >"$output/$sample/loadgen${node}.log" 2>&1 &
    pids+=("$!")
  done
  for pid in "${pids[@]}"; do wait "$pid"; done
  phase_snapshot "$sample" loadgen-complete
  controller collect-membership --nodes "$NODES" --groups "$groups" --output "/evidence/$sample/membership.json"
  phase_snapshot "$sample" drain-complete

  ids=()
  oom=false
  restarted=false
  for node in node1 node2 node3; do
    cid="$(compose ps --quiet "$node")"
    ids+=("$cid")
    docker inspect "$cid" >"$output/$sample/${node}-inspect.json"
    [[ "$(docker inspect --format '{{.State.OOMKilled}}' "$cid")" == false ]] || oom=true
    [[ "$(docker inspect --format '{{.RestartCount}}' "$cid")" == 0 ]] || restarted=true
  done
  docker network inspect "${project}_data" "${project}_control" >"$output/$sample/network-inspect.json"
  phase_snapshot "$sample" resource-inspected

  jq -n --slurpfile unloaded "$output/$sample/rtt-unloaded.json" --slurpfile shaped "$output/$sample/rtt-shaped.json" '
    [range(0;6) as $i | {
      source:$unloaded[0][$i].source, destination:$unloaded[0][$i].destination,
      unloaded:{p50:$unloaded[0][$i].p50,p95:$unloaded[0][$i].p95},
      shaped:{p50:$shaped[0][$i].p50,p95:$shaped[0][$i].p95}
    }]
  ' >"$output/$sample/rtt.json"
  jq -n --arg mode "$mode" --argjson index "$index" \
    --arg id1 "${ids[0]}" --arg id2 "${ids[1]}" --arg id3 "${ids[2]}" \
    --slurpfile rtt "$output/$sample/rtt.json" --slurpfile membership "$output/$sample/membership.json" \
    --argjson oom "$oom" --argjson restarted "$restarted" '
    {
      mode:$mode,index:$index,process_or_container_ids:[$id1,$id2,$id3],
      network_rtt_ms:$rtt[0],group_membership_after_drain:$membership[0],
      loadgen_report_paths:["loadgen1.json","loadgen2.json","loadgen3.json"],
      node_metrics_paths:["node1-metrics.jsonl","node2-metrics.jsonl","node3-metrics.jsonl"],
      oom_killed:$oom,process_restarted:$restarted,shaper_mismatch:false
    }
  ' >"$output/$sample/observation.json"
  controller summarize-sample --input "/evidence/$sample/observation.json" --output "/evidence/$sample/summary.json"
  jq -c . "$output/$sample/summary.json" >>"$output/summaries.ndjson"
  phase_snapshot "$sample" summary-complete
  phase_snapshot "$sample" sample-end
  compose down --volumes --remove-orphans
done

{
  uname -a
  lscpu
  free -b
  docker version
  docker info
  git show --no-patch --format=fuller "$source_sha"
} >"$output/host-facts.txt" 2>&1
swap_after="$(awk '/SwapTotal/{t=$2}/SwapFree/{f=$2}END{print (t-f)*1024}' /proc/meminfo)"

raw_ndjson="$output/raw-artifacts.ndjson"
: >"$raw_ndjson"
while IFS= read -r relative; do
  case "$relative" in
    *node*-metrics.jsonl) kind=node_metrics_jsonl ;;
    *loadgen?.json) kind=loadgen_report ;;
    *network-inspect.json) kind=network_inspect ;;
    *-inspect.json) kind=container_inspect ;;
    *qdisc.txt) kind=shaper_config ;;
    *rtt-unloaded.json|*rtt-shaped.json|*membership.json) kind=control_observation ;;
    host-facts.txt) kind=host_facts ;;
    *) continue ;;
  esac
  digest="$(sha256sum "$output/$relative" | awk '{print $1}')"
  jq -cn --arg kind "$kind" --arg path "$relative" --arg sha "$digest" '{kind:$kind,path:$path,sha256:$sha}' >>"$raw_ndjson"
done < <(cd "$output" && { find samples -type f; printf '%s\n' host-facts.txt; } | LC_ALL=C sort)
jq -s . "$raw_ndjson" >"$output/raw-artifacts.json"
raw_set_digest="$(controller raw-set-digest --input /evidence/raw-artifacts.json)"

jq -s '[.[].sample]' "$output/summaries.ndjson" >"$output/samples.json"
jq -s '[.[].per_group[]]' "$output/summaries.ndjson" >"$output/per-group.json"
mapfile -t all_ids < <(jq -r '.[] | .process_or_container_ids[]' "$output/samples.json" | LC_ALL=C sort -u)
ids_json="$(printf '%s\n' "${all_ids[@]}" | jq -R . | jq -s .)"
cpu_model="$(lscpu | awk -F: '/Model name/{sub(/^[[:space:]]+/,"",$2);print $2;exit}')"
cores="$(getconf _NPROCESSORS_ONLN)"
ram="$(awk '/MemTotal/{print $2*1024}' /proc/meminfo)"
kernel="$(uname -srvm)"
governor="$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || printf unknown)"
class=container

jq -n --arg schema 'chirps.multi-raft-performance/v1' --arg commit "$source_sha" --arg binary "$binary_sha" \
  --arg class "$class" --argjson ids "$ids_json" --arg cpu "$cpu_model" --argjson cores "$cores" \
  --argjson ram "$ram" --arg kernel "$kernel" \
  --arg governor "$governor" --arg shaper 'tc netem 250us per node data egress; verified six directed RTT pairs' \
  --argjson swap_before "$swap_before" --argjson swap_after "$swap_after" \
  --slurpfile samples "$output/samples.json" --slurpfile per_group "$output/per-group.json" \
  --slurpfile raw "$output/raw-artifacts.json" --arg raw_digest "$raw_set_digest" \
  --arg n1 "$NODE1_CPU" --arg n2 "$NODE2_CPU" --arg n3 "$NODE3_CPU" \
  --arg l1 "$LOADGEN1_CPU" --arg l2 "$LOADGEN2_CPU" --arg l3 "$LOADGEN3_CPU" '
  {
    schema:$schema,commit_sha:$commit,binary_sha256:$binary,
    runner_command:["scripts/perf/run-controlled-container-multi-raft.sh","--output","<OUTPUT>"],
    execution_environment:{class:$class,host_count:1,logical_nodes:3,process_or_container_ids:$ids,
      node_cpu_sets:{"1":$n1,"2":$n2,"3":$n3},loadgen_cpu_sets:{"1":$l1,"2":$l2,"3":$l3},
      cpu:$cpu,cores:$cores,ram_bytes:$ram,kernel:$kernel,rust_version:"rustc 1.96 container build",
      storage:"per-sample container tmpfs mounted at /work",filesystem:"tmpfs",network_shaper:$shaper,governor:$governor,
      physical_deployment:false,swap_bytes_before:$swap_before,swap_bytes_after:$swap_after},
    resolved_config:{nodes:3,groups:100,payload_bytes:1024,rtt_ms:1.0,clients:300,clients_per_node:100,
      warmup_seconds:15,measure_seconds:60,drain_seconds:5,samples:5,fsync_interval:0,
      snapshot_threshold:512,send_queue_capacity:4096},
    samples:$samples[0],per_group:$per_group[0],raw_metrics_artifacts:$raw[0],raw_artifact_set_sha256:$raw_digest
  }
' >"$output/artifact-input.json"
controller assemble --input /evidence/artifact-input.json --artifact /evidence/multi-raft-performance.json
controller verify --artifact /evidence/multi-raft-performance.json
printf 'controlled artifact: %s\nimage: %s\n' "$output/multi-raft-performance.json" "$image_sha"
