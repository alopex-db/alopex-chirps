use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const SCHEMA: &str = "chirps.multi-raft-performance/v1";
pub const STATISTICS_SEED: &str = "0x0000000000000600";
pub const STATISTICS_RESAMPLES: u64 = 10_000;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    pub schema: String,
    pub commit_sha: String,
    pub binary_sha256: String,
    pub runner_command: Vec<String>,
    pub execution_environment: ExecutionEnvironment,
    pub resolved_config: ResolvedConfig,
    pub samples: Vec<Sample>,
    pub per_group: Vec<PerGroup>,
    pub raw_metrics_artifacts: Vec<RawArtifact>,
    pub raw_artifact_set_sha256: String,
    pub statistics: Statistics,
    pub verdict: Verdict,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactInput {
    pub schema: String,
    pub commit_sha: String,
    pub binary_sha256: String,
    pub runner_command: Vec<String>,
    pub execution_environment: ExecutionEnvironment,
    pub resolved_config: ResolvedConfig,
    pub samples: Vec<Sample>,
    pub per_group: Vec<PerGroup>,
    pub raw_metrics_artifacts: Vec<RawArtifact>,
    pub raw_artifact_set_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionEnvironment {
    pub class: ExecutionClass,
    pub host_count: u64,
    pub logical_nodes: u64,
    pub process_or_container_ids: Vec<String>,
    pub node_cpu_sets: BTreeMap<u64, String>,
    pub loadgen_cpu_sets: BTreeMap<u64, String>,
    pub cpu: String,
    pub cores: u64,
    pub ram_bytes: u64,
    pub kernel: String,
    pub rust_version: String,
    pub storage: String,
    pub filesystem: String,
    pub network_shaper: String,
    pub governor: String,
    pub physical_deployment: bool,
    pub swap_bytes_before: u64,
    pub swap_bytes_after: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionClass {
    Native,
    Container,
    Wsl,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedConfig {
    pub nodes: u64,
    pub groups: u64,
    pub payload_bytes: u64,
    pub rtt_ms: f64,
    pub clients: u64,
    pub clients_per_node: u64,
    pub warmup_seconds: u64,
    pub measure_seconds: u64,
    pub drain_seconds: u64,
    pub samples: u64,
    pub fsync_interval: u64,
    pub snapshot_threshold: u64,
    pub send_queue_capacity: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    MultiRaft,
    SingleGroup,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Sample {
    pub mode: Mode,
    pub index: u64,
    pub group_count: u64,
    pub clients: u64,
    pub process_or_container_ids: Vec<String>,
    pub actual_measure_duration_ms: u64,
    pub monotonic_start_ns: u64,
    pub monotonic_end_ns: u64,
    pub network_rtt_ms: Vec<DirectedRtt>,
    pub group_membership_after_drain: Vec<GroupMembership>,
    pub committed: u64,
    pub throughput_per_sec: f64,
    pub latency_ms: Latency,
    pub errors: u64,
    pub timeouts: u64,
    pub cpu_seconds: f64,
    pub peak_rss_bytes: u64,
    pub disk_bytes: u64,
    pub fsync_calls: u64,
    pub network_bytes: u64,
    pub oom_killed: bool,
    pub process_restarted: bool,
    pub shaper_mismatch: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DirectedRtt {
    pub source: u64,
    pub destination: u64,
    pub unloaded: Percentiles,
    pub shaped: Percentiles,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Percentiles {
    pub p50: f64,
    pub p95: f64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RttPhaseObservation {
    pub source: u64,
    pub destination: u64,
    pub p50: f64,
    pub p95: f64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RawRttObservation {
    pub source: u64,
    pub destination: u64,
    pub p50: f64,
    pub p95: f64,
    pub raw_samples_ms: Vec<f64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GroupMembership {
    pub group_id: u64,
    pub replicas: Vec<ReplicaState>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicaState {
    pub node_id: u64,
    pub voters: Vec<u64>,
    pub leader_id: u64,
    pub last_applied: u64,
    pub committed_digest: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Latency {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PerGroup {
    pub mode: Mode,
    pub sample_index: u64,
    pub group_id: u64,
    pub committed: u64,
    pub throughput_per_sec: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RawArtifact {
    pub kind: RawArtifactKind,
    pub path: String,
    pub sha256: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RawArtifactKind {
    NodeMetricsJsonl,
    LoadgenReport,
    ContainerInspect,
    NetworkInspect,
    ShaperConfig,
    HostFacts,
    ControlObservation,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Statistics {
    pub seed: String,
    pub resamples: u64,
    pub multi_raft_median: f64,
    pub multi_raft_ci95_lower: f64,
    pub single_group_median: f64,
    pub overhead_ratio: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Verdict {
    pub throughput: Gate,
    pub overhead: Gate,
    pub integrity: Gate,
    pub overall: Gate,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Gate {
    Pass,
    Fail,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RawMetricsLine {
    pub monotonic_ns: u64,
    pub node_id: u64,
    pub cpu_seconds: f64,
    pub rss_bytes: u64,
    pub disk_read_bytes: u64,
    pub disk_write_bytes: u64,
    pub fsync_calls: u64,
    pub network_rx_bytes: u64,
    pub network_tx_bytes: u64,
    pub transport_sent: u64,
    pub transport_received: u64,
    pub transport_dropped: u64,
    pub transport_retried: u64,
    pub per_group_queue_depth: BTreeMap<u64, u64>,
    /// Proposal RPCs admitted by the control-plane load generator; kept
    /// separate from the Raft dispatch queue to avoid conflating diagnostics.
    #[serde(default)]
    pub proposal_inflight: BTreeMap<u64, u64>,
    #[serde(default)]
    pub dispatch_queue_depth: u64,
    #[serde(default)]
    pub transport_queue_utilization: BTreeMap<String, u64>,
    #[serde(default)]
    pub retransmission_total: u64,
    #[serde(default)]
    pub retransmission_buffer_bytes: u64,
    #[serde(default)]
    pub queue_overflow_total: u64,
    #[serde(default)]
    pub backpressure_triggered_total: u64,
    /// Optional diagnostics for the detachable routed-response sender.
    #[serde(default)]
    pub response_send_inflight: u64,
    #[serde(default)]
    pub response_send_max_inflight: u64,
    #[serde(default)]
    pub response_send_dropped: u64,
    #[serde(default)]
    pub response_send_failed: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LoadgenReport {
    pub mode: Mode,
    pub sample_index: u64,
    pub origin_node: u64,
    pub clients: u64,
    pub payload_bytes: u64,
    pub monotonic_start_ns: u64,
    pub monotonic_end_ns: u64,
    pub committed: u64,
    pub errors: u64,
    pub timeouts: u64,
    pub per_group_committed: BTreeMap<u64, u64>,
    /// Exact latency histogram: key is integer microseconds, value is count.
    pub latency_us: BTreeMap<u64, u64>,
    #[serde(default)]
    pub peak_rss_bytes: u64,
    #[serde(default)]
    pub rss_start_bytes: u64,
    #[serde(default)]
    pub rss_warmup_peak_bytes: u64,
    #[serde(default)]
    pub rss_measure_peak_bytes: u64,
    #[serde(default)]
    pub rss_drain_peak_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SampleObservation {
    pub mode: Mode,
    pub index: u64,
    pub process_or_container_ids: Vec<String>,
    pub network_rtt_ms: Vec<DirectedRtt>,
    pub group_membership_after_drain: Vec<GroupMembership>,
    pub loadgen_report_paths: Vec<String>,
    pub node_metrics_paths: Vec<String>,
    pub oom_killed: bool,
    pub process_restarted: bool,
    pub shaper_mismatch: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SampleSummary {
    pub sample: Sample,
    pub per_group: Vec<PerGroup>,
}
