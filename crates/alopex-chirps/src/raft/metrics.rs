use std::collections::HashMap;
use std::fmt;
use std::sync::Mutex;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

use http::{Response, StatusCode};
use openraft::ServerState;
use openraft::metrics::RaftMetrics as OpenRaftMetrics;
use prometheus::{
    Counter, CounterVec, Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, Opts,
    Registry, TextEncoder,
};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use thiserror::Error;

#[cfg(feature = "hlc")]
use alopex_chirps_gossip_swim::hlc::{HlcAdvance, HlcMetricsSink, HlcReceiveResult};
use alopex_chirps_raft_storage::{SnapshotCompletionEvent, SnapshotCompletionHook};

use crate::raft::{BasicNode, ChirpsNodeId, GroupId};

impl SnapshotCompletionHook for RaftMetricsCollector {
    fn completed(&self, event: SnapshotCompletionEvent) {
        self.record_raft_snapshot_completed(event.group_id, event.size_bytes);
    }
}

const RAFT_STATES: [&str; 4] = ["follower", "candidate", "leader", "other"];
const RAFT_MESSAGE_TYPES: [&str; 7] = [
    "append_entries",
    "append_entries_response",
    "vote",
    "vote_response",
    "install_snapshot",
    "install_snapshot_response",
    "other",
];
const PROPOSAL_RESULTS: [&str; 2] = ["success", "failed"];
const PROPOSAL_FAILURE_REASONS: [&str; 4] = ["not_leader", "timeout", "shutdown", "other"];

/// One state observation plus counter deltas for a Raft group.
#[derive(Debug, Clone, PartialEq)]
pub struct RaftMetricsUpdate {
    pub node_id: ChirpsNodeId,
    pub group_id: GroupId,
    pub state: ServerState,
    pub term: u64,
    pub commit_index: Option<u64>,
    pub applied_index: Option<u64>,
    pub last_log_index: Option<u64>,
    pub leader_id: Option<ChirpsNodeId>,
    pub votes_granted: Option<u64>,
    pub log_entries_count: Option<u64>,
    pub snapshot_total: u64,
    pub snapshot_size_bytes: Option<u64>,
    pub proposals_total: u64,
    pub proposals_failed_total: u64,
    pub proposals_failed_reason: Option<String>,
    pub proposal_latency_seconds: Option<f64>,
    pub message_sent: Option<RaftMessageMetric>,
    pub message_received: Option<RaftMessageMetric>,
}

impl RaftMetricsUpdate {
    pub fn role_label(&self) -> &'static str {
        match self.state {
            ServerState::Follower => "follower",
            ServerState::Candidate => "candidate",
            ServerState::Leader => "leader",
            _ => "other",
        }
    }
}

impl Default for RaftMetricsUpdate {
    fn default() -> Self {
        Self {
            node_id: 0,
            group_id: GroupId(0),
            state: ServerState::Follower,
            term: 0,
            commit_index: None,
            applied_index: None,
            last_log_index: None,
            leader_id: None,
            votes_granted: None,
            log_entries_count: None,
            snapshot_total: 0,
            snapshot_size_bytes: None,
            proposals_total: 0,
            proposals_failed_total: 0,
            proposals_failed_reason: None,
            proposal_latency_seconds: None,
            message_sent: None,
            message_received: None,
        }
    }
}

impl From<(GroupId, OpenRaftMetrics<ChirpsNodeId, BasicNode>)> for RaftMetricsUpdate {
    fn from(value: (GroupId, OpenRaftMetrics<ChirpsNodeId, BasicNode>)) -> Self {
        let (group_id, metrics) = value;
        let purged = metrics.purged.as_ref().map(|id| id.index).unwrap_or(0);
        let last_log_index = metrics.last_log_index;
        let log_entries_count = last_log_index.map(|idx| idx.saturating_sub(purged));

        Self {
            node_id: metrics.id,
            group_id,
            state: metrics.state,
            term: metrics.current_term,
            // OpenRaft's public RaftMetrics omits the committed index. RaftNode
            // fills this from `with_raft_state()` before publishing an update.
            commit_index: None,
            applied_index: metrics.last_applied.as_ref().map(|id| id.index),
            last_log_index,
            leader_id: metrics.current_leader,
            votes_granted: Some(u64::from(metrics.vote.committed)),
            log_entries_count,
            ..Default::default()
        }
    }
}

/// A bounded-cardinality Raft message counter delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaftMessageMetric {
    pub msg_type: &'static str,
    pub count: u64,
}

impl RaftMessageMetric {
    pub const fn new(msg_type: &'static str, count: u64) -> Self {
        Self { msg_type, count }
    }
}

/// Counter deltas and latest state emitted by the TSO implementation.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TsoMetricsUpdate {
    pub result: Option<&'static str>,
    pub request_count: u64,
    pub request_latency_seconds: Option<f64>,
    pub allocated: u64,
    pub physical_time: Option<u64>,
    pub logical_counter: Option<u64>,
    pub batch_size: Option<u64>,
}

/// Counter deltas emitted by the local/Gossip HLC implementation.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct HlcMetricsUpdate {
    pub ticks: u64,
    pub receive_result: Option<&'static str>,
    pub receives: u64,
    pub clock_skew_seconds: Option<f64>,
    pub logical_advances: u64,
    pub physical_advances: u64,
}

/// Counter deltas and the current active-connection gauge from QUIC transport.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TransportMetricsUpdate {
    pub messages_sent: u64,
    pub messages_received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub active_connections: u64,
}

/// One SWIM membership state observation and its corresponding event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwimMetricsUpdate {
    pub node_id: u64,
    pub state: &'static str,
    pub event: &'static str,
}

#[derive(Debug, Error)]
pub enum MetricsError {
    #[error("メトリクスエンコードに失敗しました: {0}")]
    Encoding(String),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MetricsAuthError {
    #[error("metrics bearer token must not be empty or whitespace")]
    EmptyBearerToken,
}

/// Authorization policy for the HTTP adapter that exposes `/metrics`.
#[derive(Clone, PartialEq, Eq)]
pub enum MetricsEndpointAuth {
    Public,
    Bearer { token_digest: [u8; 32] },
}

impl MetricsEndpointAuth {
    pub fn bearer(token: impl AsRef<str>) -> Result<Self, MetricsAuthError> {
        let token = token.as_ref();
        if token.trim().is_empty() {
            return Err(MetricsAuthError::EmptyBearerToken);
        }
        Ok(Self::Bearer {
            token_digest: Sha256::digest(token.as_bytes()).into(),
        })
    }

    fn authorizes(&self, authorization: Option<&str>) -> bool {
        match self {
            Self::Public => true,
            Self::Bearer { token_digest } => authorization
                .and_then(|value| value.strip_prefix("Bearer "))
                .is_some_and(|supplied| {
                    let supplied_digest: [u8; 32] = Sha256::digest(supplied.as_bytes()).into();
                    bool::from(token_digest.ct_eq(&supplied_digest))
                }),
        }
    }
}

impl fmt::Debug for MetricsEndpointAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Public => formatter.write_str("Public"),
            Self::Bearer { .. } => formatter.write_str("Bearer { token_digest: [REDACTED] }"),
        }
    }
}

/// Prometheus registry for Multi-Raft, TSO, HLC, and snapshot state.
pub struct RaftMetricsCollector {
    registry: Registry,
    raft_groups_total: Gauge,
    raft_group_states_total: GaugeVec,
    raft_state: GaugeVec,
    raft_term: GaugeVec,
    raft_commit_index: GaugeVec,
    raft_applied_index: GaugeVec,
    raft_last_log_index: GaugeVec,
    raft_leader_id: GaugeVec,
    raft_votes_granted: GaugeVec,
    raft_proposals_total: CounterVec,
    raft_proposals_failed_total: CounterVec,
    raft_proposals_latency_seconds: HistogramVec,
    raft_messages_sent_total: CounterVec,
    raft_messages_received_total: CounterVec,
    raft_log_entries: GaugeVec,
    raft_snapshot_total: CounterVec,
    raft_snapshot_size_bytes: GaugeVec,
    raft_proposals_failed_by_reason_total: CounterVec,
    tso_requests_total: CounterVec,
    tso_request_latency_seconds: Histogram,
    tso_allocated_total: Counter,
    tso_physical_time: Gauge,
    tso_logical_counter: Gauge,
    tso_batch_size: Histogram,
    hlc_ticks_total: Counter,
    hlc_receives_total: CounterVec,
    hlc_clock_skew_seconds: Histogram,
    hlc_logical_advances_total: Counter,
    hlc_physical_advances_total: Counter,
    transport_messages_sent_total: Counter,
    transport_messages_received_total: Counter,
    transport_bytes_sent_total: Counter,
    transport_bytes_received_total: Counter,
    transport_connections_active: Gauge,
    transport_active_connections: Gauge,
    swim_node_state: GaugeVec,
    swim_events_total: CounterVec,
    membership_node_state: GaugeVec,
    membership_events_total: CounterVec,
    coherence: Mutex<()>,
    group_states: Mutex<HashMap<GroupId, &'static str>>,
    last_successful_output: Mutex<String>,
    #[cfg(test)]
    fail_next_encode: AtomicBool,
}

/// Canonical name for the unified Chirps metrics registry.
///
/// `RaftMetricsCollector` remains available for backward compatibility.
pub type ChirpsMetricsCollector = RaftMetricsCollector;

impl Default for RaftMetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftMetricsCollector {
    pub fn new() -> Self {
        let registry = Registry::new();
        let raft_groups_total = Gauge::new(
            "chirps_raft_groups_total",
            "Number of Raft groups managed by this node",
        )
        .expect("raft groups gauge");
        let raft_group_states_total = GaugeVec::new(
            Opts::new(
                "chirps_raft_group_states_total",
                "Summary count of groups in each Raft state",
            ),
            &["state"],
        )
        .expect("raft group state summary");
        let raft_state = GaugeVec::new(
            Opts::new("chirps_raft_state", "One-hot current Raft state"),
            &["group_id", "state"],
        )
        .expect("raft state gauge");
        let raft_term = gauge_vec("chirps_raft_term", "Current Raft term", &["group_id"]);
        let raft_commit_index = gauge_vec(
            "chirps_raft_commit_index",
            "Committed log index",
            &["group_id"],
        );
        let raft_applied_index = gauge_vec(
            "chirps_raft_applied_index",
            "Applied log index",
            &["group_id"],
        );
        let raft_last_log_index = gauge_vec(
            "chirps_raft_last_log_index",
            "Last Raft log index",
            &["group_id"],
        );
        let raft_leader_id = gauge_vec(
            "chirps_raft_leader_id",
            "Current Raft leader ID, or zero when unknown",
            &["group_id"],
        );
        let raft_votes_granted = gauge_vec(
            "chirps_raft_votes_granted",
            "Committed Raft vote count",
            &["group_id"],
        );
        let raft_proposals_total = counter_vec(
            "chirps_raft_proposals_total",
            "Raft proposals grouped by result",
            &["group_id", "result"],
        );
        let raft_proposals_failed_total = counter_vec(
            "chirps_raft_proposals_failed_total",
            "Failed Raft proposals",
            &["group_id"],
        );
        let raft_proposals_latency_seconds = HistogramVec::new(
            HistogramOpts::new(
                "chirps_raft_proposals_latency_seconds",
                "Raft proposal latency in seconds",
            ),
            &["group_id"],
        )
        .expect("raft proposal histogram");
        let raft_messages_sent_total = counter_vec(
            "chirps_raft_messages_sent_total",
            "Raft messages sent",
            &["group_id", "msg_type"],
        );
        let raft_messages_received_total = counter_vec(
            "chirps_raft_messages_received_total",
            "Raft messages received",
            &["group_id", "msg_type"],
        );
        let raft_log_entries = gauge_vec(
            "chirps_raft_log_entries",
            "Stored Raft log entries",
            &["group_id"],
        );
        let raft_snapshot_total = counter_vec(
            "chirps_raft_snapshot_total",
            "Raft snapshots created or installed",
            &["group_id"],
        );
        let raft_snapshot_size_bytes = gauge_vec(
            "chirps_raft_snapshot_size_bytes",
            "Size of the latest Raft snapshot",
            &["group_id"],
        );
        let raft_proposals_failed_by_reason_total = counter_vec(
            "chirps_raft_proposals_failed_by_reason_total",
            "Failed Raft proposals grouped by bounded reason",
            &["group_id", "reason"],
        );
        let tso_requests_total = counter_vec(
            "chirps_tso_requests_total",
            "TSO requests grouped by result",
            &["result"],
        );
        let tso_request_latency_seconds = Histogram::with_opts(HistogramOpts::new(
            "chirps_tso_request_latency_seconds",
            "TSO request latency in seconds",
        ))
        .expect("tso request histogram");
        let tso_allocated_total = Counter::new(
            "chirps_tso_allocated_total",
            "Total timestamps allocated by TSO",
        )
        .expect("tso allocated counter");
        let tso_physical_time = Gauge::new(
            "chirps_tso_physical_time",
            "Latest physical time observed by TSO",
        )
        .expect("tso physical gauge");
        let tso_logical_counter =
            Gauge::new("chirps_tso_logical_counter", "Current TSO logical counter")
                .expect("tso logical gauge");
        let tso_batch_size = Histogram::with_opts(HistogramOpts::new(
            "chirps_tso_batch_size",
            "TSO allocation batch size",
        ))
        .expect("tso batch histogram");
        let hlc_ticks_total = Counter::new("chirps_hlc_ticks_total", "Local HLC tick calls")
            .expect("hlc ticks counter");
        let hlc_receives_total = counter_vec(
            "chirps_hlc_receives_total",
            "HLC receive calls grouped by result",
            &["result"],
        );
        let hlc_clock_skew_seconds = Histogram::with_opts(HistogramOpts::new(
            "chirps_hlc_clock_skew_seconds",
            "Absolute physical clock skew observed by HLC",
        ))
        .expect("hlc skew histogram");
        let hlc_logical_advances_total = Counter::new(
            "chirps_hlc_logical_advances_total",
            "HLC logical counter advances",
        )
        .expect("hlc logical counter");
        let hlc_physical_advances_total = Counter::new(
            "chirps_hlc_physical_advances_total",
            "HLC physical time advances",
        )
        .expect("hlc physical counter");
        let transport_messages_sent_total = Counter::new(
            "chirps_transport_messages_sent_total",
            "Total messages sent by the transport",
        )
        .expect("transport sent counter");
        let transport_messages_received_total = Counter::new(
            "chirps_transport_messages_received_total",
            "Total messages received by the transport",
        )
        .expect("transport received counter");
        let transport_bytes_sent_total = Counter::new(
            "chirps_transport_bytes_sent_total",
            "Total bytes sent by the transport",
        )
        .expect("transport sent bytes counter");
        let transport_bytes_received_total = Counter::new(
            "chirps_transport_bytes_received_total",
            "Total bytes received by the transport",
        )
        .expect("transport received bytes counter");
        let transport_connections_active = Gauge::new(
            "chirps_transport_connections_active",
            "Number of active transport connections",
        )
        .expect("transport active connections gauge");
        let transport_active_connections = Gauge::new(
            "chirps_transport_active_connections",
            "Backward-compatible active transport connection gauge",
        )
        .expect("transport active connections alias gauge");
        let swim_node_state = gauge_vec(
            "chirps_swim_node_state",
            "One-hot SWIM state for each node",
            &["node_id", "state"],
        );
        let swim_events_total = counter_vec(
            "chirps_swim_events_total",
            "SWIM membership events",
            &["event"],
        );
        let membership_node_state = gauge_vec(
            "chirps_membership_node_state",
            "One-hot membership state for each node",
            &["node_id", "state"],
        );
        let membership_events_total = counter_vec(
            "chirps_membership_events_total",
            "Membership state events",
            &["event"],
        );

        let register = |collector| {
            registry
                .register(collector)
                .expect("register static Chirps metric");
        };
        register(Box::new(raft_groups_total.clone()));
        register(Box::new(raft_group_states_total.clone()));
        register(Box::new(raft_state.clone()));
        register(Box::new(raft_term.clone()));
        register(Box::new(raft_commit_index.clone()));
        register(Box::new(raft_applied_index.clone()));
        register(Box::new(raft_last_log_index.clone()));
        register(Box::new(raft_leader_id.clone()));
        register(Box::new(raft_votes_granted.clone()));
        register(Box::new(raft_proposals_total.clone()));
        register(Box::new(raft_proposals_failed_total.clone()));
        register(Box::new(raft_proposals_latency_seconds.clone()));
        register(Box::new(raft_messages_sent_total.clone()));
        register(Box::new(raft_messages_received_total.clone()));
        register(Box::new(raft_log_entries.clone()));
        register(Box::new(raft_snapshot_total.clone()));
        register(Box::new(raft_snapshot_size_bytes.clone()));
        register(Box::new(raft_proposals_failed_by_reason_total.clone()));
        register(Box::new(tso_requests_total.clone()));
        register(Box::new(tso_request_latency_seconds.clone()));
        register(Box::new(tso_allocated_total.clone()));
        register(Box::new(tso_physical_time.clone()));
        register(Box::new(tso_logical_counter.clone()));
        register(Box::new(tso_batch_size.clone()));
        register(Box::new(hlc_ticks_total.clone()));
        register(Box::new(hlc_receives_total.clone()));
        register(Box::new(hlc_clock_skew_seconds.clone()));
        register(Box::new(hlc_logical_advances_total.clone()));
        register(Box::new(hlc_physical_advances_total.clone()));
        register(Box::new(transport_messages_sent_total.clone()));
        register(Box::new(transport_messages_received_total.clone()));
        register(Box::new(transport_bytes_sent_total.clone()));
        register(Box::new(transport_bytes_received_total.clone()));
        register(Box::new(transport_connections_active.clone()));
        register(Box::new(transport_active_connections.clone()));
        register(Box::new(swim_node_state.clone()));
        register(Box::new(swim_events_total.clone()));
        register(Box::new(membership_node_state.clone()));
        register(Box::new(membership_events_total.clone()));

        Self {
            registry,
            raft_groups_total,
            raft_group_states_total,
            raft_state,
            raft_term,
            raft_commit_index,
            raft_applied_index,
            raft_last_log_index,
            raft_leader_id,
            raft_votes_granted,
            raft_proposals_total,
            raft_proposals_failed_total,
            raft_proposals_latency_seconds,
            raft_messages_sent_total,
            raft_messages_received_total,
            raft_log_entries,
            raft_snapshot_total,
            raft_snapshot_size_bytes,
            raft_proposals_failed_by_reason_total,
            tso_requests_total,
            tso_request_latency_seconds,
            tso_allocated_total,
            tso_physical_time,
            tso_logical_counter,
            tso_batch_size,
            hlc_ticks_total,
            hlc_receives_total,
            hlc_clock_skew_seconds,
            hlc_logical_advances_total,
            hlc_physical_advances_total,
            transport_messages_sent_total,
            transport_messages_received_total,
            transport_bytes_sent_total,
            transport_bytes_received_total,
            transport_connections_active,
            transport_active_connections,
            swim_node_state,
            swim_events_total,
            membership_node_state,
            membership_events_total,
            coherence: Mutex::new(()),
            group_states: Mutex::new(HashMap::new()),
            last_successful_output: Mutex::new(String::new()),
            #[cfg(test)]
            fail_next_encode: AtomicBool::new(false),
        }
    }

    pub fn set_groups_total(&self, count: usize) {
        let _coherence = self.coherence_guard();
        self.raft_groups_total.set(count as f64);
    }

    /// Removes all per-group series after the group is no longer routable.
    pub fn remove_group(&self, group_id: GroupId) {
        let _coherence = self.coherence_guard();
        let group_id_label = group_id.0.to_string();
        for state in RAFT_STATES {
            let _ = self
                .raft_state
                .remove_label_values(&[&group_id_label, state]);
        }
        let _ = self.raft_term.remove_label_values(&[&group_id_label]);
        let _ = self
            .raft_commit_index
            .remove_label_values(&[&group_id_label]);
        let _ = self
            .raft_applied_index
            .remove_label_values(&[&group_id_label]);
        let _ = self
            .raft_last_log_index
            .remove_label_values(&[&group_id_label]);
        let _ = self.raft_leader_id.remove_label_values(&[&group_id_label]);
        let _ = self
            .raft_votes_granted
            .remove_label_values(&[&group_id_label]);
        let _ = self
            .raft_log_entries
            .remove_label_values(&[&group_id_label]);
        let _ = self
            .raft_snapshot_total
            .remove_label_values(&[&group_id_label]);
        let _ = self
            .raft_snapshot_size_bytes
            .remove_label_values(&[&group_id_label]);
        let _ = self
            .raft_proposals_latency_seconds
            .remove_label_values(&[&group_id_label]);
        for result in PROPOSAL_RESULTS {
            let _ = self
                .raft_proposals_total
                .remove_label_values(&[&group_id_label, result]);
        }
        let _ = self
            .raft_proposals_failed_total
            .remove_label_values(&[&group_id_label]);
        for message_type in RAFT_MESSAGE_TYPES {
            let _ = self
                .raft_messages_sent_total
                .remove_label_values(&[&group_id_label, message_type]);
            let _ = self
                .raft_messages_received_total
                .remove_label_values(&[&group_id_label, message_type]);
        }
        for reason in PROPOSAL_FAILURE_REASONS {
            let _ = self
                .raft_proposals_failed_by_reason_total
                .remove_label_values(&[&group_id_label, reason]);
        }

        self.group_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&group_id);
        self.refresh_group_state_summary();
    }

    pub fn record_raft_message_sent(&self, group_id: GroupId, msg_type: &'static str, count: u64) {
        let _coherence = self.coherence_guard();
        self.raft_messages_sent_total
            .with_label_values(&[&group_id.0.to_string(), bounded_message_type(msg_type)])
            .inc_by(count as f64);
    }

    pub fn record_raft_message_received(
        &self,
        group_id: GroupId,
        msg_type: &'static str,
        count: u64,
    ) {
        let _coherence = self.coherence_guard();
        self.raft_messages_received_total
            .with_label_values(&[&group_id.0.to_string(), bounded_message_type(msg_type)])
            .inc_by(count as f64);
    }

    pub fn set_raft_commit_index(&self, group_id: GroupId, commit_index: u64) {
        let _coherence = self.coherence_guard();
        self.raft_commit_index
            .with_label_values(&[&group_id.0.to_string()])
            .set(commit_index as f64);
    }

    pub fn record_raft_proposal(
        &self,
        group_id: GroupId,
        result: &'static str,
        failure_reason: Option<&str>,
        latency_seconds: f64,
        committed_index: Option<u64>,
    ) {
        let _coherence = self.coherence_guard();
        let group_id = group_id.0.to_string();
        let result = if result == "success" {
            "success"
        } else {
            "failed"
        };
        self.raft_proposals_total
            .with_label_values(&[&group_id, result])
            .inc();
        self.raft_proposals_latency_seconds
            .with_label_values(&[&group_id])
            .observe(latency_seconds.max(0.0));
        if result == "failed" {
            self.raft_proposals_failed_by_reason_total
                .with_label_values(&[&group_id, bounded_proposal_reason(failure_reason)])
                .inc();
        }
        if let Some(commit_index) = committed_index {
            self.raft_commit_index
                .with_label_values(&[&group_id])
                .set(commit_index as f64);
        }
    }

    /// Records only a fully verified, durably checkpointed snapshot.
    pub fn record_raft_snapshot_completed(&self, group_id: GroupId, size_bytes: u64) {
        let _coherence = self.coherence_guard();
        let group_id = group_id.0.to_string();
        self.raft_snapshot_total
            .with_label_values(&[&group_id])
            .inc();
        self.raft_snapshot_size_bytes
            .with_label_values(&[&group_id])
            .set(size_bytes as f64);
    }

    pub fn update(&self, metrics: &RaftMetricsUpdate) {
        let _coherence = self.coherence_guard();
        let group_id = metrics.group_id.0.to_string();
        let role = metrics.role_label();
        for state in RAFT_STATES {
            self.raft_state
                .with_label_values(&[&group_id, state])
                .set(f64::from(state == role));
        }
        self.update_group_state_summary(metrics.group_id, role);
        self.raft_term
            .with_label_values(&[&group_id])
            .set(metrics.term as f64);
        if let Some(commit_index) = metrics.commit_index {
            self.raft_commit_index
                .with_label_values(&[&group_id])
                .set(commit_index as f64);
        }
        self.raft_applied_index
            .with_label_values(&[&group_id])
            .set(metrics.applied_index.unwrap_or(0) as f64);
        self.raft_last_log_index
            .with_label_values(&[&group_id])
            .set(metrics.last_log_index.unwrap_or(0) as f64);
        self.raft_leader_id
            .with_label_values(&[&group_id])
            .set(metrics.leader_id.unwrap_or(0) as f64);
        self.raft_votes_granted
            .with_label_values(&[&group_id])
            .set(metrics.votes_granted.unwrap_or(0) as f64);
        self.raft_log_entries
            .with_label_values(&[&group_id])
            .set(metrics.log_entries_count.unwrap_or(0) as f64);

        if metrics.proposals_total > 0 {
            self.raft_proposals_total
                .with_label_values(&[&group_id, "success"])
                .inc_by(metrics.proposals_total as f64);
        }
        if metrics.proposals_failed_total > 0 {
            self.raft_proposals_total
                .with_label_values(&[&group_id, "failed"])
                .inc_by(metrics.proposals_failed_total as f64);
            self.raft_proposals_failed_total
                .with_label_values(&[&group_id])
                .inc_by(metrics.proposals_failed_total as f64);
            let reason = bounded_proposal_reason(metrics.proposals_failed_reason.as_deref());
            self.raft_proposals_failed_by_reason_total
                .with_label_values(&[&group_id, reason])
                .inc_by(metrics.proposals_failed_total as f64);
        }
        if let Some(latency) = metrics.proposal_latency_seconds {
            self.raft_proposals_latency_seconds
                .with_label_values(&[&group_id])
                .observe(latency.max(0.0));
        }
        if let Some(message) = metrics.message_sent {
            self.raft_messages_sent_total
                .with_label_values(&[&group_id, bounded_message_type(message.msg_type)])
                .inc_by(message.count as f64);
        }
        if let Some(message) = metrics.message_received {
            self.raft_messages_received_total
                .with_label_values(&[&group_id, bounded_message_type(message.msg_type)])
                .inc_by(message.count as f64);
        }
        if metrics.snapshot_total > 0 {
            self.raft_snapshot_total
                .with_label_values(&[&group_id])
                .inc_by(metrics.snapshot_total as f64);
        }
        if let Some(size) = metrics.snapshot_size_bytes {
            self.raft_snapshot_size_bytes
                .with_label_values(&[&group_id])
                .set(size as f64);
        }
    }

    pub fn update_tso(&self, metrics: &TsoMetricsUpdate) {
        let _coherence = self.coherence_guard();
        if metrics.request_count > 0 {
            self.tso_requests_total
                .with_label_values(&[bounded_tso_result(metrics.result)])
                .inc_by(metrics.request_count as f64);
        }
        if let Some(latency) = metrics.request_latency_seconds {
            self.tso_request_latency_seconds.observe(latency.max(0.0));
        }
        if metrics.allocated > 0 {
            self.tso_allocated_total.inc_by(metrics.allocated as f64);
        }
        if let Some(physical) = metrics.physical_time {
            self.tso_physical_time.set(physical as f64);
        }
        if let Some(logical) = metrics.logical_counter {
            self.tso_logical_counter.set(logical as f64);
        }
        if let Some(batch_size) = metrics.batch_size {
            self.tso_batch_size.observe(batch_size as f64);
        }
    }

    pub fn update_hlc(&self, metrics: &HlcMetricsUpdate) {
        let _coherence = self.coherence_guard();
        if metrics.ticks > 0 {
            self.hlc_ticks_total.inc_by(metrics.ticks as f64);
        }
        if metrics.receives > 0 {
            self.hlc_receives_total
                .with_label_values(&[bounded_hlc_result(metrics.receive_result)])
                .inc_by(metrics.receives as f64);
        }
        if let Some(skew) = metrics.clock_skew_seconds {
            self.hlc_clock_skew_seconds.observe(skew.abs());
        }
        if metrics.logical_advances > 0 {
            self.hlc_logical_advances_total
                .inc_by(metrics.logical_advances as f64);
        }
        if metrics.physical_advances > 0 {
            self.hlc_physical_advances_total
                .inc_by(metrics.physical_advances as f64);
        }
    }

    /// Publishes transport counter deltas and the current connection gauge.
    pub fn update_transport(&self, metrics: &TransportMetricsUpdate) {
        let _coherence = self.coherence_guard();
        self.transport_messages_sent_total
            .inc_by(metrics.messages_sent as f64);
        self.transport_messages_received_total
            .inc_by(metrics.messages_received as f64);
        self.transport_bytes_sent_total
            .inc_by(metrics.bytes_sent as f64);
        self.transport_bytes_received_total
            .inc_by(metrics.bytes_received as f64);
        self.transport_connections_active
            .set(metrics.active_connections as f64);
        self.transport_active_connections
            .set(metrics.active_connections as f64);
    }

    /// Publishes a one-hot node state and increments the bounded event series.
    pub fn update_swim(&self, metrics: &SwimMetricsUpdate) {
        let _coherence = self.coherence_guard();
        let node_id = metrics.node_id.to_string();
        let state = bounded_swim_state(metrics.state);
        for candidate in SWIM_STATES {
            self.swim_node_state
                .with_label_values(&[&node_id, candidate])
                .set(f64::from(candidate == state));
            self.membership_node_state
                .with_label_values(&[&node_id, candidate])
                .set(f64::from(candidate == state));
        }
        let event = bounded_swim_event(metrics.event);
        self.swim_events_total.with_label_values(&[event]).inc();
        self.membership_events_total
            .with_label_values(&[event])
            .inc();
    }

    fn update_group_state_summary(&self, group_id: GroupId, role: &'static str) {
        let previous = self
            .group_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(group_id, role);
        if previous != Some(role) {
            self.refresh_group_state_summary();
        }
    }

    fn refresh_group_state_summary(&self) {
        let states = self
            .group_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for state in RAFT_STATES {
            let count = states.values().filter(|value| **value == state).count();
            self.raft_group_states_total
                .with_label_values(&[state])
                .set(count as f64);
        }
    }

    pub fn encode(&self) -> Result<String, MetricsError> {
        let _coherence = self.coherence_guard();
        #[cfg(test)]
        if self.fail_next_encode.swap(false, Ordering::SeqCst) {
            return Err(MetricsError::Encoding("injected failure".into()));
        }

        let mut buffer = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut buffer)
            .map_err(|error| MetricsError::Encoding(error.to_string()))?;
        let encoded =
            String::from_utf8(buffer).map_err(|error| MetricsError::Encoding(error.to_string()))?;
        let encoded = append_v05_raft_aliases(&encoded);
        *self
            .last_successful_output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = encoded.clone();
        Ok(encoded)
    }

    pub fn encode_with_fallback(&self) -> String {
        match self.encode() {
            Ok(output) => output,
            Err(error) => {
                tracing::error!(
                    target: "raft",
                    event = "metrics_encoding_error",
                    error = %error,
                    "Failed to encode metrics, returning last successful values"
                );
                self.last_successful_output()
            }
        }
    }

    fn last_successful_output(&self) -> String {
        self.last_successful_output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn coherence_guard(&self) -> std::sync::MutexGuard<'_, ()> {
        self.coherence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[cfg(test)]
    pub fn inject_encode_error(&self) {
        self.fail_next_encode.store(true, Ordering::SeqCst);
    }
}

fn append_v05_raft_aliases(encoded: &str) -> String {
    let aliases = [
        ("chirps_raft_state", "raft_state"),
        ("chirps_raft_term", "raft_term"),
        ("chirps_raft_commit_index", "raft_commit_index"),
        ("chirps_raft_applied_index", "raft_applied_index"),
        ("chirps_raft_last_log_index", "raft_last_log_index"),
        ("chirps_raft_leader_id", "raft_leader_id"),
        ("chirps_raft_votes_granted", "raft_votes_granted"),
        ("chirps_raft_log_entries", "raft_log_entries_count"),
        ("chirps_raft_snapshot_total", "raft_snapshot_total"),
        ("chirps_raft_proposals_total", "raft_proposals_total"),
        (
            "chirps_raft_proposals_failed_total",
            "raft_proposals_failed_total",
        ),
    ];
    let mut result = encoded.to_owned();
    for (canonical, alias) in aliases {
        for line in encoded.lines().filter(|line| {
            line.starts_with(&format!("# HELP {canonical}"))
                || line.starts_with(&format!("# TYPE {canonical}"))
                || line.starts_with(canonical)
        }) {
            result.push_str(&line.replace(canonical, alias));
            result.push('\n');
        }
    }
    result
}

const SWIM_STATES: [&str; 4] = ["alive", "suspect", "dead", "other"];

fn bounded_swim_state(value: &str) -> &'static str {
    match value {
        "alive" => "alive",
        "suspect" => "suspect",
        "dead" => "dead",
        _ => "other",
    }
}

fn bounded_swim_event(value: &str) -> &'static str {
    bounded_swim_state(value)
}

#[cfg(feature = "hlc")]
impl HlcMetricsSink for RaftMetricsCollector {
    fn record_tick(&self, advance: HlcAdvance) {
        let (logical_advances, physical_advances) = advance_counts(advance);
        self.update_hlc(&HlcMetricsUpdate {
            ticks: 1,
            logical_advances,
            physical_advances,
            ..Default::default()
        });
    }

    fn record_receive(
        &self,
        result: HlcReceiveResult,
        clock_skew: std::time::Duration,
        advance: Option<HlcAdvance>,
    ) {
        let (logical_advances, physical_advances) = advance.map_or((0, 0), advance_counts);
        self.update_hlc(&HlcMetricsUpdate {
            receive_result: Some(match result {
                HlcReceiveResult::Success => "success",
                HlcReceiveResult::SkewError => "skew_error",
            }),
            receives: 1,
            clock_skew_seconds: Some(clock_skew.as_secs_f64()),
            logical_advances,
            physical_advances,
            ..Default::default()
        });
    }
}

#[cfg(feature = "hlc")]
fn advance_counts(advance: HlcAdvance) -> (u64, u64) {
    match advance {
        HlcAdvance::Logical => (1, 0),
        HlcAdvance::Physical => (0, 1),
    }
}

fn gauge_vec(name: &str, help: &str, labels: &[&str]) -> GaugeVec {
    GaugeVec::new(Opts::new(name, help), labels).expect("static gauge definition")
}

fn counter_vec(name: &str, help: &str, labels: &[&str]) -> CounterVec {
    CounterVec::new(Opts::new(name, help), labels).expect("static counter definition")
}

fn bounded_message_type(value: &str) -> &'static str {
    match value {
        "append_entries" => "append_entries",
        "append_entries_response" => "append_entries_response",
        "vote" => "vote",
        "vote_response" => "vote_response",
        "install_snapshot" => "install_snapshot",
        "install_snapshot_response" => "install_snapshot_response",
        _ => "other",
    }
}

fn bounded_proposal_reason(value: Option<&str>) -> &'static str {
    let value = value.unwrap_or("unknown").to_ascii_lowercase();
    if value.contains("leader") {
        "not_leader"
    } else if value.contains("timeout") {
        "timeout"
    } else if value.contains("shutdown") {
        "shutdown"
    } else {
        "other"
    }
}

fn bounded_tso_result(value: Option<&str>) -> &'static str {
    match value {
        Some("success") => "success",
        Some("not_leader") => "not_leader",
        Some("unauthorized") => "unauthorized",
        Some("timeout") => "timeout",
        Some("lease_not_ready") => "lease_not_ready",
        Some("invalid_count") => "invalid_count",
        Some("invalid_config") => "invalid_config",
        Some("transport_error") => "transport_error",
        Some("raft_error") => "raft_error",
        Some("codec_error") => "codec_error",
        Some("overflow") => "overflow",
        Some("non_monotonic") => "non_monotonic",
        _ => "failed",
    }
}

fn bounded_hlc_result(value: Option<&str>) -> &'static str {
    match value {
        Some("success") => "success",
        Some("skew_error") => "skew_error",
        Some("duplicate") => "duplicate",
        _ => "other",
    }
}

/// Backward-compatible public `/metrics` response adapter.
pub fn serve_metrics(collector: &RaftMetricsCollector) -> Response<String> {
    serve_metrics_authorized(collector, &MetricsEndpointAuth::Public, None)
}

/// Builds a Prometheus response after enforcing the configured endpoint policy.
pub fn serve_metrics_authorized(
    collector: &RaftMetricsCollector,
    auth: &MetricsEndpointAuth,
    authorization_header: Option<&str>,
) -> Response<String> {
    if !auth.authorizes(authorization_header) {
        return Response::builder()
            .status(StatusCode::UNAUTHORIZED)
            .header("Content-Type", "text/plain; charset=utf-8")
            .header("WWW-Authenticate", "Bearer")
            .body("metrics authorization required".to_owned())
            .expect("build metrics unauthorized response");
    }

    let response =
        Response::builder().header("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
    match collector.encode() {
        Ok(body) => response
            .status(StatusCode::OK)
            .body(body)
            .expect("build metrics response"),
        Err(error) => {
            let fallback = collector.last_successful_output();
            if fallback.is_empty() {
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .header("Content-Type", "text/plain; charset=utf-8")
                    .body(format!("failed to encode metrics: {error}"))
                    .expect("build metrics error response")
            } else {
                response
                    .status(StatusCode::OK)
                    .body(fallback)
                    .expect("build metrics fallback response")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::CommittedLeaderId;
    use openraft::metrics::RaftMetrics;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn encode_and_fallback_work() {
        let collector = RaftMetricsCollector::new();
        collector.set_groups_total(1);
        collector.update(&RaftMetricsUpdate {
            node_id: 1,
            group_id: GroupId(0),
            state: ServerState::Leader,
            term: 2,
            commit_index: Some(3),
            applied_index: Some(3),
            last_log_index: Some(4),
            leader_id: Some(1),
            votes_granted: Some(1),
            log_entries_count: Some(4),
            snapshot_total: 1,
            proposals_total: 2,
            proposals_failed_total: 1,
            proposals_failed_reason: Some("timeout".into()),
            ..Default::default()
        });

        let encoded = collector.encode().expect("encode");
        assert!(encoded.contains("chirps_raft_state"));
        collector.inject_encode_error();
        assert_eq!(encoded, collector.encode_with_fallback());
    }

    #[test]
    fn update_from_openraft_metrics() {
        let mut metrics = RaftMetrics::new_initial(2);
        metrics.state = ServerState::Follower;
        metrics.current_term = 5;
        metrics.last_log_index = Some(10);
        metrics.purged = Some(openraft::LogId::new(CommittedLeaderId::new(4, 1), 4));
        metrics.current_leader = Some(2);

        let update = RaftMetricsUpdate::from((GroupId(7), metrics));
        assert_eq!(update.node_id, 2);
        assert_eq!(update.group_id, GroupId(7));
        assert_eq!(update.term, 5);
        assert_eq!(update.log_entries_count, Some(6));
        assert_eq!(update.role_label(), "follower");
    }

    #[test]
    fn endpoint_returns_last_complete_snapshot_on_encode_failure() {
        let collector = RaftMetricsCollector::new();
        collector.set_groups_total(1);
        let first = serve_metrics(&collector);
        assert_eq!(first.status(), StatusCode::OK);
        collector.set_groups_total(2);
        collector.inject_encode_error();
        let fallback = serve_metrics(&collector);
        assert_eq!(fallback.status(), StatusCode::OK);
        assert_eq!(fallback.body(), first.body());
    }

    #[test]
    fn bearer_debug_redacts_secret() {
        let auth = MetricsEndpointAuth::bearer("do-not-log-this").unwrap();
        let rendered = format!("{auth:?}");
        assert!(!rendered.contains("do-not-log-this"));
    }

    #[test]
    fn empty_bearer_tokens_are_rejected() {
        for token in ["", " ", "\t\n"] {
            assert_eq!(
                MetricsEndpointAuth::bearer(token),
                Err(MetricsAuthError::EmptyBearerToken)
            );
        }
    }

    #[test]
    fn concurrent_scrapes_observe_one_coherent_revision() {
        let collector = Arc::new(RaftMetricsCollector::new());
        collector.update(&RaftMetricsUpdate {
            group_id: GroupId(9),
            state: ServerState::Follower,
            term: 0,
            commit_index: Some(0),
            applied_index: Some(0),
            ..Default::default()
        });
        let barrier = Arc::new(Barrier::new(2));
        let writer = {
            let collector = Arc::clone(&collector);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                for term in 1..=500 {
                    collector.update(&RaftMetricsUpdate {
                        group_id: GroupId(9),
                        state: if term % 2 == 0 {
                            ServerState::Follower
                        } else {
                            ServerState::Leader
                        },
                        term,
                        commit_index: Some(term),
                        applied_index: Some(term),
                        ..Default::default()
                    });
                }
            })
        };

        barrier.wait();
        for _ in 0..500 {
            let body = collector.encode().unwrap();
            let term = metric_value(&body, "chirps_raft_term{group_id=\"9\"}");
            let follower = metric_value(
                &body,
                "chirps_raft_state{group_id=\"9\",state=\"follower\"}",
            );
            let leader = metric_value(&body, "chirps_raft_state{group_id=\"9\",state=\"leader\"}");
            assert_eq!(follower + leader, 1.0);
            assert_eq!(leader == 1.0, (term as u64) % 2 == 1);
        }
        writer.join().unwrap();
    }

    fn metric_value(body: &str, name: &str) -> f64 {
        body.lines()
            .find_map(|line| {
                line.strip_prefix(name)
                    .and_then(|rest| rest.strip_prefix(' '))
                    .and_then(|value| value.parse().ok())
            })
            .unwrap_or_else(|| panic!("missing metric {name}"))
    }
}
