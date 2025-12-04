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
use tokio::sync::watch::Receiver;
use tokio::task::JoinHandle;
use tracing::info;

/// Chirps内部でやり取りするRaft RPC。リクエストとレスポンス両方を保持する。
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
}

/// openraft Raftをラップし、Chirps固有のエラー/設定型を提供する。
pub struct RaftNode {
    pub(crate) config: RaftConfig,
    pub(crate) raft: Raft<ChirpsTypeConfig>,
    #[allow(dead_code)]
    pub(crate) transport: Arc<ChirpsRaftTransport>,
    metrics_collector: Arc<Mutex<Option<Arc<RaftMetricsCollector>>>>,
    #[allow(dead_code)]
    observer_handle: JoinHandle<()>,
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
            observer_handle,
        })
    }

    /// Raft起動を行う。openraftでは生成時に起動するため、ここではNOP。
    pub async fn start(&mut self) -> RaftResult<()> {
        Ok(())
    }

    /// メトリクスコレクタを登録する。登録後は状態変化に応じて自動更新される。
    pub fn set_metrics_collector(&self, collector: Arc<RaftMetricsCollector>) {
        if let Ok(mut slot) = self.metrics_collector.lock() {
            *slot = Some(collector);
        }
    }

    /// クライアントコマンドを提案する。NotLeaderの場合はリーダーIDを返す。
    pub async fn propose(&self, command: Vec<u8>) -> RaftResult<Vec<u8>> {
        match self.raft.client_write(command).await {
            Ok(ClientWriteResponse { data, .. }) => {
                self.push_metrics_update(RaftMetricsUpdate {
                    proposals_total: 1,
                    ..Default::default()
                });
                Ok(data)
            }
            Err(OpenRaftError::APIError(ClientWriteError::ForwardToLeader(fwd))) => {
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
                self.push_metrics_update(RaftMetricsUpdate {
                    proposals_failed_total: 1,
                    proposals_failed_reason: Some(reason.clone()),
                    ..Default::default()
                });
                Err(RaftError::Internal(anyhow!(reason)))
            }
        }
    }

    /// 現在のリーダーIDを返す。
    pub fn leader_id(&self) -> Option<ChirpsNodeId> {
        self.raft.metrics().borrow().current_leader.clone()
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
            .map(|_| ())
            .map_err(RaftError::from)
    }

    /// Learner追加。
    pub async fn add_learner(&self, node_id: ChirpsNodeId, node: BasicNode) -> RaftResult<()> {
        self.raft
            .add_learner(node_id, node, true)
            .await
            .map(|_| ())
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
                let resp = self
                    .raft
                    .append_entries(request)
                    .await
                    .map_err(RaftError::from)?;
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
                let resp = self
                    .raft
                    .install_snapshot(request)
                    .await
                    .map_err(RaftError::from)?;
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
            "Snapshot triggered"
        );
        self.push_metrics_update(RaftMetricsUpdate {
            snapshot_total: 1,
            ..Default::default()
        });
        Ok(())
    }
}

fn build_openraft_config(src: &RaftConfig) -> Result<Arc<Config>, ConfigError> {
    let mut cfg = Config::default();
    cfg.cluster_name = format!("chirps-raft-{}", src.group_id.0);
    cfg.election_timeout_min = src.election_timeout_ms;
    cfg.election_timeout_max = src.election_timeout_ms * 2;
    cfg.heartbeat_interval = src.heartbeat_interval_ms;
    cfg.max_payload_entries = src.max_batch_size as u64;
    cfg.snapshot_policy = SnapshotPolicy::LogsSinceLast(src.snapshot_threshold);
    cfg.max_in_snapshot_log_to_keep = src.max_in_snapshot_log_to_keep;
    Ok(Arc::new(cfg.validate()?))
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
                if let Ok(slot) = collector.lock() {
                    if let Some(col) = slot.as_ref() {
                        let update = RaftMetricsUpdate::from((group_id, metrics.clone()));
                        col.update(&update);
                    }
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
            if let Some(leader_id) = metrics.current_leader.clone() {
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
            self.last_leader = metrics.current_leader.clone();
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
            if let Some(log_id) = metrics.snapshot.clone() {
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
            self.last_snapshot = metrics.snapshot.clone();
        }

        if metrics.purged != self.last_purged {
            if let Some(log_id) = metrics.purged.clone() {
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
            self.last_purged = metrics.purged.clone();
        }
    }
}

impl RaftNode {
    fn push_metrics_update(&self, update: RaftMetricsUpdate) {
        if let Ok(slot) = self.metrics_collector.lock() {
            if let Some(col) = slot.as_ref() {
                let mut base = RaftMetricsUpdate::from((
                    self.config.group_id,
                    self.raft.metrics().borrow().clone(),
                ));
                base.snapshot_total = update.snapshot_total;
                base.proposals_total = update.proposals_total;
                base.proposals_failed_total = update.proposals_failed_total;
                base.proposals_failed_reason = update.proposals_failed_reason;
                col.update(&base);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode;
    use chirps_wire::frame::{Frame, RaftFrame};
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
                vote: chirps_raft_storage::types::Vote::new(0, 0),
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
                    vote: chirps_raft_storage::types::Vote::new(0, 0),
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
            if let Some(fields) = v.get("fields") {
                if let Some(ev) = fields.get("event").and_then(|e| e.as_str()) {
                    events.push(ev.to_string());
                }
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
