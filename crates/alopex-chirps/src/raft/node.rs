use crate::raft::transport::{ChirpsRaftTransport, RaftFramePayload};
use crate::raft::{
    AppendEntriesRequest, AppendEntriesResponse, BasicNode, ChirpsNodeId, ChirpsTypeConfig,
    GroupId, InstallSnapshotRequest, InstallSnapshotResponse, RaftConfig, RaftError, RaftResult,
    VoteRequest, VoteResponse,
};
use anyhow::anyhow;
use openraft::Raft;
use openraft::error::{ClientWriteError, RaftError as OpenRaftError};
use openraft::network::RaftNetworkFactory;
use openraft::raft::ClientWriteResponse;
use openraft::storage::{RaftLogStorage, RaftStateMachine};
use openraft::{Config, ConfigError, SnapshotPolicy};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::Arc;

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

        Ok(Self {
            config,
            raft,
            transport,
        })
    }

    /// Raft起動を行う。openraftでは生成時に起動するため、ここではNOP。
    pub async fn start(&mut self) -> RaftResult<()> {
        Ok(())
    }

    /// クライアントコマンドを提案する。NotLeaderの場合はリーダーIDを返す。
    pub async fn propose(&self, command: Vec<u8>) -> RaftResult<Vec<u8>> {
        match self.raft.client_write(command).await {
            Ok(ClientWriteResponse { data, .. }) => Ok(data),
            Err(OpenRaftError::APIError(ClientWriteError::ForwardToLeader(fwd))) => {
                Err(RaftError::NotLeader(fwd.leader_id))
            }
            Err(other) => Err(RaftError::Internal(anyhow!(other.to_string()))),
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
            .map_err(RaftError::from)
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

#[cfg(test)]
mod tests {
    use super::*;
    use bincode;
    use chirps_wire::frame::{Frame, RaftFrame};

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
}
