use std::any::Any;

use alopex_chirps_raft_storage::types::StorageError;
use anyhow::anyhow;
use openraft::BasicNode;
use openraft::error::{ChangeMembershipError, ClientWriteError, Fatal, RaftError as OpenRaftError};
use thiserror::Error;

/// Raftモジュール共通のResult型。
pub type RaftResult<T, E = RaftError> = Result<T, E>;

/// RaftNode/Transportで発生し得るエラー。
#[derive(Debug, Error)]
pub enum RaftError {
    #[error("リーダーではありません。現在のリーダー: {0:?}")]
    NotLeader(Option<u64>),

    #[error("提案がタイムアウトしました")]
    ProposalTimeout,

    #[error("ストレージエラー: {0}")]
    Storage(#[from] StorageError<u64>),

    #[error("ネットワークエラー: {0}")]
    Network(String),

    #[error("不正なメッセージ: {0}")]
    InvalidMessage(String),

    #[error("メンバーシップ変更が進行中です")]
    MembershipChangeInProgress,

    #[error("スナップショットエラー: {0}")]
    Snapshot(String),

    #[error("ノードが見つかりません: {0}")]
    NodeNotFound(u64),

    #[error("内部エラー: {0}")]
    Internal(#[from] anyhow::Error),
}

impl From<Fatal<u64>> for RaftError {
    fn from(err: Fatal<u64>) -> Self {
        match err {
            Fatal::StorageError(e) => RaftError::Storage(e),
            Fatal::Panicked => RaftError::Internal(anyhow!("Raft panicked")),
            Fatal::Stopped => RaftError::Internal(anyhow!("Raft stopped")),
        }
    }
}

impl<E> From<OpenRaftError<u64, E>> for RaftError
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(err: OpenRaftError<u64, E>) -> Self {
        match err {
            OpenRaftError::APIError(e) => {
                if membership_change_in_progress(&e) {
                    return RaftError::MembershipChangeInProgress;
                }

                RaftError::Internal(anyhow!(e.to_string()))
            }
            OpenRaftError::Fatal(f) => RaftError::from(f),
        }
    }
}

fn membership_change_in_progress<E>(err: &E) -> bool
where
    E: std::error::Error + Send + Sync + 'static,
{
    if let Some(change) = (err as &dyn Any).downcast_ref::<ChangeMembershipError<u64>>() {
        return matches!(change, ChangeMembershipError::InProgress(_));
    }

    if let Some(client_write) = (err as &dyn Any).downcast_ref::<ClientWriteError<u64, BasicNode>>()
    {
        return matches!(
            client_write,
            ClientWriteError::ChangeMembershipError(ChangeMembershipError::InProgress(_))
        );
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::error::{ChangeMembershipError, InProgress, RaftError as OpenRaftError};
    use openraft::{CommittedLeaderId, LogId};

    #[test]
    fn membership_change_in_progress_is_preserved() {
        let in_progress = ChangeMembershipError::<u64>::InProgress(InProgress {
            committed: Some(LogId::new(CommittedLeaderId::new(1, 1), 2)),
            membership_log_id: Some(LogId::new(CommittedLeaderId::new(1, 1), 3)),
        });

        let raft_err = RaftError::from(OpenRaftError::APIError(in_progress));

        assert!(matches!(raft_err, RaftError::MembershipChangeInProgress));
    }
}
