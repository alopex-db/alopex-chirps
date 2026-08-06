use crate::raft::metrics::{RaftMetricsCollector, RaftMetricsUpdate};
use crate::raft::transport::{ChirpsRaftTransport, RaftFramePayload};
use crate::raft::{
    AppendEntriesRequest, AppendEntriesResponse, BasicNode, ChirpsNodeId, ChirpsTypeConfig,
    GroupId, InstallSnapshotRequest, InstallSnapshotResponse, RaftConfig, RaftError, RaftResult,
    VoteRequest, VoteResponse,
};
use anyhow::anyhow;
use openraft::Raft;
use openraft::error::{ClientWriteError, RaftError as OpenRaftError};
use openraft::metrics::RaftMetrics as OpenRaftMetrics;
use openraft::network::RaftNetworkFactory;
use openraft::raft::ClientWriteResponse;
use openraft::storage::{RaftLogStorage, RaftStateMachine};
use openraft::{Config, ConfigError, LogId, MessageSummary, ServerState, SnapshotPolicy};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::watch::Receiver;
use tokio::task::JoinHandle;
use tracing::info;

/// Chirps内部でやり取りするRaft RPC。リクエストとレスポンス両方を保持する。
///
/// # 例
///
/// ```rust,ignore
/// use alopex_chirps::raft::{RaftMessage, GroupId};
/// use alopex_chirps_raft_storage::types::{VoteRequest, Vote};
///
/// let msg = RaftMessage::Vote {
///     group_id: GroupId(1),
///     request: VoteRequest {
///         vote: Vote::new(1, 1),
///         last_log_id: None,
///     },
/// };
/// assert_eq!(msg.group_id(), GroupId(1));
/// ```
#[derive(Debug, Serialize, Deserialize)]
pub enum RaftMessage {
    AppendEntries {
        group_id: GroupId,
        request: AppendEntriesRequest<ChirpsTypeConfig>,
    },
    AppendEntriesResponse {
        group_id: GroupId,
        response: AppendEntriesResponse<ChirpsNodeId>,
    },
    Vote {
        group_id: GroupId,
        request: VoteRequest<ChirpsNodeId>,
    },
    VoteResponse {
        group_id: GroupId,
        response: VoteResponse<ChirpsNodeId>,
    },
    InstallSnapshot {
        group_id: GroupId,
        request: InstallSnapshotRequest<ChirpsTypeConfig>,
    },
    InstallSnapshotResponse {
        group_id: GroupId,
        response: InstallSnapshotResponse<ChirpsNodeId>,
    },
}

impl RaftMessage {
    pub fn group_id(&self) -> GroupId {
        match self {
            RaftMessage::AppendEntries { group_id, .. }
            | RaftMessage::AppendEntriesResponse { group_id, .. }
            | RaftMessage::Vote { group_id, .. }
            | RaftMessage::VoteResponse { group_id, .. }
            | RaftMessage::InstallSnapshot { group_id, .. }
            | RaftMessage::InstallSnapshotResponse { group_id, .. } => *group_id,
        }
    }

    pub(crate) fn metric_type(&self) -> &'static str {
        match self {
            Self::AppendEntries { .. } => "append_entries",
            Self::AppendEntriesResponse { .. } => "append_entries_response",
            Self::Vote { .. } => "vote",
            Self::VoteResponse { .. } => "vote_response",
            Self::InstallSnapshot { .. } => "install_snapshot",
            Self::InstallSnapshotResponse { .. } => "install_snapshot_response",
        }
    }
}

/// openraft Raftをラップし、Chirps固有のエラー/設定型を提供する。
///
/// # 例
///
/// ```rust,ignore
/// use alopex_chirps::raft::{RaftConfig, RaftNode};
/// use alopex_chirps::raft::transport::ChirpsRaftTransport;
/// use alopex_chirps_raft_storage::types::GroupId;
/// use std::sync::Arc;
///
/// # async fn build() -> anyhow::Result<()> {
/// let transport = Arc::new(ChirpsRaftTransport::new(mock_backend(), GroupId(1), 1));
/// let network = ChirpsRaftTransport::factory(transport.clone());
/// let log_store = build_log_store();      // RaftLogStorageを実装した型を使う
/// let state_machine = build_state_machine(); // RaftStateMachineを実装した型を使う
/// let mut node = RaftNode::new(
///     RaftConfig { group_id: GroupId(1), node_id: 1, ..Default::default() },
///     network,
///     log_store,
///     state_machine,
///     transport,
/// ).await?;
/// node.start().await?;
/// # Ok(()) }
/// ```
pub struct RaftNode {
    pub(crate) config: RaftConfig,
    pub(crate) raft: Raft<ChirpsTypeConfig>,
    #[allow(dead_code)]
    pub(crate) transport: Arc<ChirpsRaftTransport>,
    metrics_collector: Arc<Mutex<Option<Arc<RaftMetricsCollector>>>>,
    observer_handle: Mutex<Option<JoinHandle<()>>>,
}

impl RaftNode {
    /// Raftノードを初期化する。openraft::Raftの生成に必要なConfigを組み立てる。
    pub async fn new<NF, LS, SM>(
        config: RaftConfig,
        network: NF,
        log_store: LS,
        state_machine: SM,
        transport: Arc<ChirpsRaftTransport>,
    ) -> RaftResult<Self>
    where
        NF: RaftNetworkFactory<ChirpsTypeConfig> + Clone + Send + Sync + 'static,
        NF::Network: Send + Sync,
        LS: RaftLogStorage<ChirpsTypeConfig> + Send + Sync + 'static,
        SM: RaftStateMachine<ChirpsTypeConfig> + Send + Sync + 'static,
    {
        let cfg = build_openraft_config(&config)
            .map_err(|e| RaftError::Internal(anyhow!("config error: {e}")))?;
        let raft = Raft::new(config.node_id, cfg, network, log_store, state_machine)
            .await
            .map_err(RaftError::from)?;

        let collector = Arc::new(Mutex::new(None));
        let observer_handle =
            spawn_metrics_observer(config.group_id, raft.metrics(), Arc::clone(&collector));
        info!(
            target: "raft",
            event = "raft_initialized",
            group_id = %config.group_id.0,
            node_id = %config.node_id,
            term = %raft.metrics().borrow().current_term,
            "Raft node initialized"
        );

        Ok(Self {
            config,
            raft,
            transport,
            metrics_collector: collector,
            observer_handle: Mutex::new(Some(observer_handle)),
        })
    }

    /// Raft起動を行う。openraftでは生成時に起動するため、ここではNOP。
    pub async fn start(&mut self) -> RaftResult<()> {
        Ok(())
    }

    /// クラスターを初期化する。初回のみ呼び出すこと。
    pub async fn initialize(&self, members: BTreeSet<ChirpsNodeId>) -> RaftResult<()> {
        self.raft.initialize(members).await.map_err(RaftError::from)
    }

    /// 最新のメトリクススナップショットを取得する。
    pub fn metrics(&self) -> OpenRaftMetrics<ChirpsNodeId, BasicNode> {
        self.raft.metrics().borrow().clone()
    }

    /// 最終適用ログIDを返す。
    pub fn last_applied_log(&self) -> Option<LogId<ChirpsNodeId>> {
        self.raft.metrics().borrow().last_applied
    }

    /// メトリクスコレクタを登録する。登録後は状態変化に応じて自動更新される。
    pub fn set_metrics_collector(&self, collector: Arc<RaftMetricsCollector>) {
        self.transport.set_metrics_collector(Arc::clone(&collector));
        collector.update(&RaftMetricsUpdate::from((
            self.config.group_id,
            self.raft.metrics().borrow().clone(),
        )));
        if let Ok(mut slot) = self.metrics_collector.lock() {
            *slot = Some(collector);
        }
    }

    /// クライアントコマンドを提案する。NotLeaderの場合はリーダーIDを返す。
    pub async fn propose(&self, command: Vec<u8>) -> RaftResult<Vec<u8>> {
        let started = Instant::now();
        match self.raft.client_write(command).await {
            Ok(ClientWriteResponse { data, log_id, .. }) => {
                if let Some(collector) = self.metrics_collector() {
                    collector.record_raft_proposal(
                        self.config.group_id,
                        "success",
                        None,
                        started.elapsed().as_secs_f64(),
                        Some(log_id.index),
                    );
                }
                Ok(data)
            }
            Err(OpenRaftError::APIError(ClientWriteError::ForwardToLeader(fwd))) => {
                if let Some(collector) = self.metrics_collector() {
                    collector.record_raft_proposal(
                        self.config.group_id,
                        "failed",
                        Some("not leader"),
                        started.elapsed().as_secs_f64(),
                        None,
                    );
                }
                Err(RaftError::NotLeader(fwd.leader_id))
            }
            Err(other) => {
                let reason = other.to_string();
                tracing::warn!(
                    target: "raft",
                    event = "raft_propose_failed",
                    group_id = %self.config.group_id.0,
                    node_id = %self.config.node_id,
                    term = %self.raft.metrics().borrow().current_term,
                    reason = %reason,
                    "Proposal failed"
                );
                if let Some(collector) = self.metrics_collector() {
                    collector.record_raft_proposal(
                        self.config.group_id,
                        "failed",
                        Some(&reason),
                        started.elapsed().as_secs_f64(),
                        None,
                    );
                }
                Err(RaftError::Internal(anyhow!(reason)))
            }
        }
    }

    /// 現在のリーダーIDを返す。
    pub fn leader_id(&self) -> Option<ChirpsNodeId> {
        self.raft.metrics().borrow().current_leader
    }

    /// 自ノードがリーダーか判定する。
    pub fn is_leader(&self) -> bool {
        self.leader_id() == Some(self.config.node_id)
    }

    /// メンバーシップ変更（Joint Consensus対応）。
    pub async fn change_membership(&self, members: BTreeSet<ChirpsNodeId>) -> RaftResult<()> {
        self.raft
            .change_membership(members, false)
            .await
            .map(|response| self.record_committed_index(response.log_id.index))
            .map_err(RaftError::from)
    }

    /// Learner追加。
    pub async fn add_learner(&self, node_id: ChirpsNodeId, node: BasicNode) -> RaftResult<()> {
        self.raft
            .add_learner(node_id, node, true)
            .await
            .map(|response| self.record_committed_index(response.log_id.index))
            .map_err(RaftError::from)
    }

    /// 受信メッセージをopenraftへ橋渡しし、レスポンスを返す。
    pub async fn handle_message(&self, payload: RaftFramePayload) -> RaftResult<RaftMessage> {
        if payload.message.group_id() != self.config.group_id {
            return Err(RaftError::InvalidMessage(format!(
                "group mismatch: expected {}, got {:?}",
                self.config.group_id.0,
                payload.message.group_id()
            )));
        }
        match payload.message {
            RaftMessage::AppendEntries { request, .. } => {
                let leader_commit = request.leader_commit.map(|log_id| log_id.index);
                let resp = self
                    .raft
                    .append_entries(request)
                    .await
                    .map_err(RaftError::from)?;
                if let Some(commit_index) = leader_commit {
                    self.record_committed_index(commit_index);
                }
                Ok(RaftMessage::AppendEntriesResponse {
                    group_id: self.config.group_id,
                    response: resp,
                })
            }
            RaftMessage::Vote { request, .. } => {
                let resp = self.raft.vote(request).await.map_err(RaftError::from)?;
                Ok(RaftMessage::VoteResponse {
                    group_id: self.config.group_id,
                    response: resp,
                })
            }
            RaftMessage::InstallSnapshot { request, .. } => {
                let snapshot_index = request.meta.last_log_id.map(|log_id| log_id.index);
                let resp = self
                    .raft
                    .install_snapshot(request)
                    .await
                    .map_err(RaftError::from)?;
                if let Some(commit_index) = snapshot_index {
                    self.record_committed_index(commit_index);
                }
                Ok(RaftMessage::InstallSnapshotResponse {
                    group_id: self.config.group_id,
                    response: resp,
                })
            }
            RaftMessage::AppendEntriesResponse { response, .. } => {
                Ok(RaftMessage::AppendEntriesResponse {
                    group_id: self.config.group_id,
                    response,
                })
            }
            RaftMessage::VoteResponse { response, .. } => Ok(RaftMessage::VoteResponse {
                group_id: self.config.group_id,
                response,
            }),
            RaftMessage::InstallSnapshotResponse { response, .. } => {
                Ok(RaftMessage::InstallSnapshotResponse {
                    group_id: self.config.group_id,
                    response,
                })
            }
        }
    }

    /// openraftトリガーをそのまま公開。現在はハートビートのみ。
    pub async fn tick(&self) -> RaftResult<()> {
        self.raft
            .trigger()
            .heartbeat()
            .await
            .map_err(RaftError::from)
    }

    pub(crate) fn close_transport_admission(&self) {
        self.transport.close_rpc_admission();
    }

    pub(crate) fn cancel_pending_transport_rpcs(&self) {
        self.transport.cancel_pending_rpcs();
    }

    /// スナップショット生成を手動でトリガーする。
    pub async fn trigger_snapshot(&self) -> RaftResult<()> {
        self.raft
            .trigger()
            .snapshot()
            .await
            .map_err(RaftError::from)?;

        let last_log = self.raft.metrics().borrow().last_log_index;
        tracing::info!(
            target: "raft",
            event = "raft_snapshot_created",
            group_id = %self.config.group_id.0,
            node_id = %self.config.node_id,
            log_id = ?last_log,
            "Snapshot generation requested"
        );
        Ok(())
    }

    /// Stops OpenRaft and its metrics observer.
    ///
    /// Calling shutdown more than once is safe. The method does not return
    /// until the core task has stopped and the observer task has been joined.
    pub async fn shutdown(&self) -> RaftResult<()> {
        self.raft
            .shutdown()
            .await
            .map_err(|error| RaftError::Internal(anyhow!("Raft shutdown failed: {error}")))?;

        let observer = self
            .observer_handle
            .lock()
            .map_err(|_| RaftError::Internal(anyhow!("metrics observer lock poisoned")))?
            .take();
        if let Some(observer) = observer {
            observer.abort();
            match observer.await {
                Ok(()) => {}
                Err(error) if error.is_cancelled() => {}
                Err(error) => {
                    return Err(RaftError::Internal(anyhow!(
                        "metrics observer failed during shutdown: {error}"
                    )));
                }
            }
        }
        Ok(())
    }
}

fn build_openraft_config(src: &RaftConfig) -> Result<Arc<Config>, Box<ConfigError>> {
    let cfg = Config {
        cluster_name: format!("chirps-raft-{}", src.group_id.0),
        election_timeout_min: src.election_timeout_ms,
        election_timeout_max: src.election_timeout_ms * 2,
        heartbeat_interval: src.heartbeat_interval_ms,
        max_payload_entries: src.max_batch_size as u64,
        snapshot_policy: SnapshotPolicy::LogsSinceLast(src.snapshot_threshold),
        max_in_snapshot_log_to_keep: src.max_in_snapshot_log_to_keep,
        ..Default::default()
    };
    Ok(Arc::new(cfg.validate().map_err(Box::new)?))
}

fn spawn_metrics_observer(
    group_id: GroupId,
    mut rx: Receiver<OpenRaftMetrics<ChirpsNodeId, BasicNode>>,
    collector: Arc<Mutex<Option<Arc<RaftMetricsCollector>>>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut obs_state = ObservationState::default();

        loop {
            {
                let metrics = rx.borrow().clone();
                let metrics_collector = collector
                    .lock()
                    .ok()
                    .and_then(|slot| slot.as_ref().cloned());
                if let Some(col) = metrics_collector {
                    col.update(&RaftMetricsUpdate::from((group_id, metrics.clone())));
                }
                obs_state.handle(group_id, &metrics);
            }

            if rx.changed().await.is_err() {
                break;
            }
        }
    })
}

#[derive(Default)]
struct ObservationState {
    last_state: Option<ServerState>,
    last_leader: Option<ChirpsNodeId>,
    last_membership: String,
    last_snapshot: Option<LogId<ChirpsNodeId>>,
    last_purged: Option<LogId<ChirpsNodeId>>,
}

impl ObservationState {
    fn handle(&mut self, group_id: GroupId, metrics: &OpenRaftMetrics<ChirpsNodeId, BasicNode>) {
        if self.last_state != Some(metrics.state) {
            tracing::info!(
                target: "raft",
                event = "raft_state_changed",
                group_id = %group_id.0,
                node_id = %metrics.id,
                term = %metrics.current_term,
                old_state = ?self.last_state,
                new_state = ?metrics.state,
                "Raft state changed"
            );
            self.last_state = Some(metrics.state);
        }

        if metrics.current_leader != self.last_leader {
            if let Some(leader_id) = metrics.current_leader {
                tracing::info!(
                    target: "raft",
                    event = "raft_leader_elected",
                    group_id = %group_id.0,
                    node_id = %metrics.id,
                    term = %metrics.current_term,
                    leader_id = %leader_id,
                    "Leader elected"
                );
            }
            self.last_leader = metrics.current_leader;
        }

        let membership_summary = metrics.membership_config.summary();
        if membership_summary != self.last_membership {
            let membership = metrics.membership_config.membership();
            let voter_ids = membership
                .get_joint_config()
                .iter()
                .flatten()
                .cloned()
                .collect::<BTreeSet<_>>();
            let learners = membership
                .nodes()
                .filter(|(id, _)| !voter_ids.contains(id))
                .map(|(id, _)| *id)
                .collect::<Vec<_>>();
            tracing::info!(
                target: "raft",
                event = "raft_membership_changed",
                group_id = %group_id.0,
                node_id = %metrics.id,
                term = %metrics.current_term,
                voters = ?membership.get_joint_config(),
                learners = ?learners,
                "Membership changed"
            );
            self.last_membership = membership_summary;
        }

        if metrics.snapshot != self.last_snapshot {
            if let Some(log_id) = metrics.snapshot {
                tracing::info!(
                    target: "raft",
                    event = "raft_snapshot_installed",
                    group_id = %group_id.0,
                    node_id = %metrics.id,
                    term = %metrics.current_term,
                    log_id = ?log_id,
                    "Snapshot installed"
                );
            }
            self.last_snapshot = metrics.snapshot;
        }

        if metrics.purged != self.last_purged {
            if let Some(log_id) = metrics.purged {
                tracing::info!(
                    target: "raft",
                    event = "raft_log_compacted",
                    group_id = %group_id.0,
                    node_id = %metrics.id,
                    term = %metrics.current_term,
                    up_to_log_id = ?log_id,
                    "Log compacted"
                );
            }
            self.last_purged = metrics.purged;
        }
    }
}

impl RaftNode {
    fn metrics_collector(&self) -> Option<Arc<RaftMetricsCollector>> {
        self.metrics_collector
            .lock()
            .ok()
            .and_then(|slot| slot.clone())
    }

    fn record_committed_index(&self, commit_index: u64) {
        if let Some(collector) = self.metrics_collector() {
            collector.set_raft_commit_index(self.config.group_id, commit_index);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alopex_chirps_wire::frame::{Frame, RaftFrame};
    use bincode;
    use openraft::CommittedLeaderId;
    use openraft::ServerState;
    use openraft::metrics::RaftMetrics as OpenRaftMetrics;
    use serde_json::Value;
    use std::io;
    use tracing_subscriber::FmtSubscriber;
    use tracing_subscriber::fmt::writer::MakeWriter;

    #[test]
    fn config_defaults_match_design() {
        let cfg = RaftConfig::default();
        assert_eq!(cfg.election_timeout_ms, 150);
        assert_eq!(cfg.heartbeat_interval_ms, 50);
        assert_eq!(cfg.max_batch_size, 1_000);
        assert_eq!(cfg.snapshot_threshold, 10_000);
        assert_eq!(cfg.max_in_snapshot_log_to_keep, 1_000);
    }

    #[test]
    fn raft_message_reports_group() {
        let msg = RaftMessage::Vote {
            group_id: GroupId(42),
            request: VoteRequest {
                vote: alopex_chirps_raft_storage::types::Vote::new(0, 0),
                last_log_id: None,
            },
        };
        assert_eq!(msg.group_id(), GroupId(42));
    }

    #[test]
    fn decode_frame_roundtrip() {
        let payload = RaftFramePayload {
            correlation_id: 7,
            message: RaftMessage::AppendEntries {
                group_id: GroupId(1),
                request: AppendEntriesRequest {
                    vote: alopex_chirps_raft_storage::types::Vote::new(0, 0),
                    prev_log_id: None,
                    entries: Vec::new(),
                    leader_commit: None,
                },
            },
        };
        let bytes = bincode::serialize(&payload).expect("serialize");
        let frame = Frame::Raft(RaftFrame {
            group_id: 1,
            payload: bytes,
        });
        let decoded = ChirpsRaftTransport::decode_frame(frame).expect("decode");
        assert_eq!(decoded.correlation_id, 7);
        assert_eq!(decoded.message.group_id(), GroupId(1));

        let mismatched = Frame::Raft(RaftFrame {
            group_id: 2,
            payload: bincode::serialize(&payload).expect("serialize mismatch payload"),
        });
        assert!(ChirpsRaftTransport::decode_frame(mismatched).is_none());
    }

    #[test]
    fn observation_state_emits_structured_logs() {
        #[derive(Clone)]
        struct MemoryMakeWriter(Arc<Mutex<Vec<u8>>>);
        struct MemoryWriter(Arc<Mutex<Vec<u8>>>);

        impl<'a> MakeWriter<'a> for MemoryMakeWriter {
            type Writer = MemoryWriter;

            fn make_writer(&'a self) -> Self::Writer {
                MemoryWriter(Arc::clone(&self.0))
            }
        }

        impl io::Write for MemoryWriter {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                let mut lock = self.0.lock().unwrap();
                lock.extend_from_slice(buf);
                Ok(buf.len())
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let buffer = Arc::new(Mutex::new(Vec::new()));
        let subscriber = FmtSubscriber::builder()
            .json()
            .with_writer(MemoryMakeWriter(Arc::clone(&buffer)))
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            let mut obs = ObservationState::default();
            let mut metrics = OpenRaftMetrics::new_initial(1);
            metrics.state = ServerState::Leader;
            metrics.current_term = 3;
            metrics.current_leader = Some(1);
            metrics.snapshot = Some(LogId::new(CommittedLeaderId::new(3, 1), 2));
            metrics.purged = Some(LogId::new(CommittedLeaderId::new(2, 1), 1));

            obs.handle(GroupId(9), &metrics);
        });

        let logs = String::from_utf8(buffer.lock().unwrap().clone()).expect("utf8");
        let mut events = Vec::new();
        for line in logs.lines() {
            let v: Value = serde_json::from_str(line).expect("json");
            if let Some(ev) = v
                .get("fields")
                .and_then(|fields| fields.get("event"))
                .and_then(|e| e.as_str())
            {
                events.push(ev.to_string());
            }
            if let Some(target) = v.get("target").and_then(|t| t.as_str()) {
                assert_eq!(target, "raft", "log target should be raft");
            }
        }

        assert!(
            events.contains(&"raft_state_changed".to_string()),
            "state change event expected"
        );
        assert!(
            events.contains(&"raft_leader_elected".to_string()),
            "leader election event expected"
        );
        assert!(
            events.contains(&"raft_snapshot_installed".to_string()),
            "snapshot installed event expected"
        );
        assert!(
            events.contains(&"raft_log_compacted".to_string()),
            "log compacted event expected"
        );
    }
}
