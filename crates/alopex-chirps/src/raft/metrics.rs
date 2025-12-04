use std::sync::Mutex;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

use http::{Response, StatusCode};
use openraft::ServerState;
use openraft::metrics::RaftMetrics as OpenRaftMetrics;
use prometheus::{CounterVec, Encoder, GaugeVec, Opts, Registry, TextEncoder};
use thiserror::Error;

use crate::raft::{BasicNode, ChirpsNodeId, GroupId};

/// メトリクス更新用のスナップショット。
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
    pub proposals_total: u64,
    pub proposals_failed_total: u64,
    pub proposals_failed_reason: Option<String>,
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
            proposals_total: 0,
            proposals_failed_total: 0,
            proposals_failed_reason: None,
        }
    }
}

impl From<(GroupId, OpenRaftMetrics<ChirpsNodeId, BasicNode>)> for RaftMetricsUpdate {
    fn from(value: (GroupId, OpenRaftMetrics<ChirpsNodeId, BasicNode>)) -> Self {
        let (group_id, metrics) = value;
        let purged = metrics.purged.as_ref().map(|id| id.index).unwrap_or(0);
        let last_log_index = metrics.last_log_index;
        let log_entries_count = last_log_index.map(|idx| idx.saturating_sub(purged));

        RaftMetricsUpdate {
            node_id: metrics.id,
            group_id,
            state: metrics.state,
            term: metrics.current_term,
            commit_index: metrics.last_applied.as_ref().map(|id| id.index),
            applied_index: metrics.last_applied.as_ref().map(|id| id.index),
            last_log_index,
            leader_id: metrics.current_leader.clone(),
            votes_granted: if metrics.vote.committed {
                Some(1)
            } else {
                Some(0)
            },
            log_entries_count,
            snapshot_total: 0,
            proposals_total: 0,
            proposals_failed_total: 0,
            proposals_failed_reason: None,
        }
    }
}

#[derive(Debug, Error)]
pub enum MetricsError {
    #[error("メトリクスエンコードに失敗しました: {0}")]
    Encoding(String),
}

/// Prometheus出力を管理するコレクタ。
pub struct RaftMetricsCollector {
    registry: Registry,
    raft_state: GaugeVec,
    raft_term: GaugeVec,
    raft_commit_index: GaugeVec,
    raft_applied_index: GaugeVec,
    raft_last_log_index: GaugeVec,
    raft_leader_id: GaugeVec,
    raft_votes_granted: GaugeVec,
    raft_log_entries_count: GaugeVec,
    raft_snapshot_total: CounterVec,
    raft_proposals_total: CounterVec,
    raft_proposals_failed_total: CounterVec,
    last_successful_output: Mutex<String>,
    #[cfg(test)]
    fail_next_encode: AtomicBool,
}

impl RaftMetricsCollector {
    pub fn new() -> Self {
        let registry = Registry::new();
        let labels = &["node_id", "group_id", "role"];
        let raft_state = GaugeVec::new(
            Opts::new(
                "raft_state",
                "Current Raft state (0=Follower,1=Candidate,2=Leader)",
            ),
            labels,
        )
        .expect("raft_state gauge");
        let raft_term =
            GaugeVec::new(Opts::new("raft_term", "Current Raft term"), labels).expect("raft_term");
        let raft_commit_index = GaugeVec::new(
            Opts::new("raft_commit_index", "Committed log index"),
            labels,
        )
        .expect("raft_commit_index");
        let raft_applied_index =
            GaugeVec::new(Opts::new("raft_applied_index", "Applied log index"), labels)
                .expect("raft_applied_index");
        let raft_last_log_index = GaugeVec::new(
            Opts::new("raft_last_log_index", "Last appended log index"),
            labels,
        )
        .expect("raft_last_log_index");
        let raft_leader_id = GaugeVec::new(
            Opts::new("raft_leader_id", "Current known leader id"),
            labels,
        )
        .expect("raft_leader_id");
        let raft_votes_granted = GaugeVec::new(
            Opts::new("raft_votes_granted", "Votes granted in current term"),
            labels,
        )
        .expect("raft_votes_granted");
        let raft_log_entries_count = GaugeVec::new(
            Opts::new("raft_log_entries_count", "Stored log entries count"),
            labels,
        )
        .expect("raft_log_entries_count");
        let raft_snapshot_total = CounterVec::new(
            Opts::new("raft_snapshot_total", "Number of snapshots created"),
            labels,
        )
        .expect("raft_snapshot_total");
        let raft_proposals_total = CounterVec::new(
            Opts::new("raft_proposals_total", "Number of proposals"),
            labels,
        )
        .expect("raft_proposals_total");
        let raft_proposals_failed_total = CounterVec::new(
            Opts::new(
                "raft_proposals_failed_total",
                "Number of failed proposals grouped by reason",
            ),
            &["node_id", "group_id", "role", "reason"],
        )
        .expect("raft_proposals_failed_total");

        registry
            .register(Box::new(raft_state.clone()))
            .expect("register raft_state");
        registry
            .register(Box::new(raft_term.clone()))
            .expect("register raft_term");
        registry
            .register(Box::new(raft_commit_index.clone()))
            .expect("register raft_commit_index");
        registry
            .register(Box::new(raft_applied_index.clone()))
            .expect("register raft_applied_index");
        registry
            .register(Box::new(raft_last_log_index.clone()))
            .expect("register raft_last_log_index");
        registry
            .register(Box::new(raft_leader_id.clone()))
            .expect("register raft_leader_id");
        registry
            .register(Box::new(raft_votes_granted.clone()))
            .expect("register raft_votes_granted");
        registry
            .register(Box::new(raft_log_entries_count.clone()))
            .expect("register raft_log_entries_count");
        registry
            .register(Box::new(raft_snapshot_total.clone()))
            .expect("register raft_snapshot_total");
        registry
            .register(Box::new(raft_proposals_total.clone()))
            .expect("register raft_proposals_total");
        registry
            .register(Box::new(raft_proposals_failed_total.clone()))
            .expect("register raft_proposals_failed_total");

        Self {
            registry,
            raft_state,
            raft_term,
            raft_commit_index,
            raft_applied_index,
            raft_last_log_index,
            raft_leader_id,
            raft_votes_granted,
            raft_log_entries_count,
            raft_snapshot_total,
            raft_proposals_total,
            raft_proposals_failed_total,
            last_successful_output: Mutex::new(String::new()),
            #[cfg(test)]
            fail_next_encode: AtomicBool::new(false),
        }
    }

    pub fn update(&self, metrics: &RaftMetricsUpdate) {
        let labels = [
            metrics.node_id.to_string(),
            metrics.group_id.0.to_string(),
            metrics.role_label().to_string(),
        ];
        let label_refs = [&labels[0][..], &labels[1][..], &labels[2][..]];

        self.raft_state
            .with_label_values(&label_refs)
            .set(match metrics.state {
                ServerState::Follower => 0.0,
                ServerState::Candidate => 1.0,
                ServerState::Leader => 2.0,
                _ => -1.0,
            });
        self.raft_term
            .with_label_values(&label_refs)
            .set(metrics.term as f64);
        self.raft_commit_index
            .with_label_values(&label_refs)
            .set(metrics.commit_index.unwrap_or(0) as f64);
        self.raft_applied_index
            .with_label_values(&label_refs)
            .set(metrics.applied_index.unwrap_or(0) as f64);
        self.raft_last_log_index
            .with_label_values(&label_refs)
            .set(metrics.last_log_index.unwrap_or(0) as f64);
        self.raft_leader_id
            .with_label_values(&label_refs)
            .set(metrics.leader_id.unwrap_or(0) as f64);
        self.raft_votes_granted
            .with_label_values(&label_refs)
            .set(metrics.votes_granted.unwrap_or(0) as f64);
        self.raft_log_entries_count
            .with_label_values(&label_refs)
            .set(metrics.log_entries_count.unwrap_or(0) as f64);

        if metrics.snapshot_total > 0 {
            self.raft_snapshot_total
                .with_label_values(&label_refs)
                .inc_by(metrics.snapshot_total as f64);
        }
        if metrics.proposals_total > 0 {
            self.raft_proposals_total
                .with_label_values(&label_refs)
                .inc_by(metrics.proposals_total as f64);
        }
        if metrics.proposals_failed_total > 0 {
            let reason = metrics
                .proposals_failed_reason
                .as_deref()
                .unwrap_or("unknown");
            let extended = &[label_refs[0], label_refs[1], label_refs[2], reason];
            self.raft_proposals_failed_total
                .with_label_values(extended)
                .inc_by(metrics.proposals_failed_total as f64);
        }
    }

    pub fn encode(&self) -> Result<String, MetricsError> {
        #[cfg(test)]
        if self.fail_next_encode.swap(false, Ordering::SeqCst) {
            return Err(MetricsError::Encoding("injected failure".into()));
        }

        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        encoder
            .encode(&self.registry.gather(), &mut buffer)
            .map_err(|e| MetricsError::Encoding(e.to_string()))?;
        let encoded =
            String::from_utf8(buffer).map_err(|e| MetricsError::Encoding(e.to_string()))?;
        *self
            .last_successful_output
            .lock()
            .expect("lock last output") = encoded.clone();
        Ok(encoded)
    }

    pub fn encode_with_fallback(&self) -> String {
        match self.encode() {
            Ok(output) => output,
            Err(e) => {
                tracing::error!(
                    target: "raft",
                    event = "metrics_encoding_error",
                    error = %e,
                    "Failed to encode metrics, returning last successful values"
                );
                self.last_successful_output
                    .lock()
                    .expect("lock last output")
                    .clone()
            }
        }
    }

    #[cfg(test)]
    pub fn inject_encode_error(&self) {
        self.fail_next_encode.store(true, Ordering::SeqCst);
    }
}

/// `/metrics`エンドポイントのレスポンスを生成する。エンコード失敗時は直近成功値があれば200で返し、なければ5xxを返す。
pub fn serve_metrics(collector: &RaftMetricsCollector) -> Response<String> {
    let base =
        Response::builder().header("Content-Type", "text/plain; version=0.0.4; charset=utf-8");

    match collector.encode() {
        Ok(body) => base
            .status(StatusCode::OK)
            .body(body)
            .expect("build metrics response"),
        Err(e) => {
            let fallback = collector.encode_with_fallback();
            if fallback.is_empty() {
                Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .header("Content-Type", "text/plain; charset=utf-8")
                    .body(format!("failed to encode metrics: {e}"))
                    .expect("build metrics error response")
            } else {
                base.status(StatusCode::OK)
                    .body(fallback)
                    .expect("build metrics response with fallback")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::CommittedLeaderId;
    use openraft::metrics::RaftMetrics;

    #[test]
    fn encode_and_fallback_work() {
        let collector = RaftMetricsCollector::new();
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
        });

        let encoded = collector.encode().expect("encode");
        assert!(
            encoded.contains("raft_state"),
            "encoded metrics should contain raft_state"
        );

        collector.inject_encode_error();
        let fallback = collector.encode_with_fallback();
        assert_eq!(encoded, fallback, "fallback should return last success");
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
    fn metrics_endpoint_returns_fallback_body() {
        let collector = RaftMetricsCollector::new();
        collector.update(&RaftMetricsUpdate {
            node_id: 1,
            group_id: GroupId(0),
            state: ServerState::Leader,
            term: 1,
            commit_index: Some(1),
            applied_index: Some(1),
            last_log_index: Some(1),
            leader_id: Some(1),
            votes_granted: Some(1),
            log_entries_count: Some(1),
            ..Default::default()
        });

        let ok_resp = serve_metrics(&collector);
        let body_ok = ok_resp.body();
        assert_eq!(ok_resp.status(), http::StatusCode::OK);
        assert!(
            body_ok.contains("raft_state"),
            "initial metrics response should contain raft_state"
        );

        collector.inject_encode_error();
        let fallback_resp = serve_metrics(&collector);
        assert_eq!(fallback_resp.status(), http::StatusCode::OK);
        assert_eq!(
            fallback_resp.body(),
            body_ok,
            "fallback should return last successful body"
        );
    }
}
