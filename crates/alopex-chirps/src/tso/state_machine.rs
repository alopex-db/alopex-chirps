use super::{HybridTimestamp, TimestampRange};
use alopex_chirps_raft_storage::traits::{AsyncSnapshotData, StateMachine, StateMachineResult};
use alopex_chirps_raft_storage::types::LogId;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::sync::RwLock;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TsoState {
    pub last: Option<HybridTimestamp>,
    pub lease_owner: Option<u64>,
    pub lease_expires_at_ms: u64,
    pub committed_ranges: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TsoCommand {
    AcquireLease {
        leader_id: u64,
        now_ms: u64,
        lease_duration_ms: u64,
    },
    Allocate {
        leader_id: u64,
        lease_now_ms: u64,
        physical_ms: u64,
        count: u32,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TsoResponse {
    LeaseAcquired { expires_at_ms: u64 },
    LeasePending { not_before_ms: u64 },
    Allocated(TimestampRange),
    InvalidCount,
    TimestampOverflow,
}

/// Deterministic replicated TSO state. All allocation and lease mutations are
/// reached through OpenRaft's committed state-machine application path.
#[derive(Clone, Default)]
pub struct TsoStateMachine {
    state: Arc<RwLock<TsoState>>,
}

impl TsoStateMachine {
    pub async fn state(&self) -> TsoState {
        self.state.read().await.clone()
    }

    fn transition(state: &mut TsoState, command: TsoCommand) -> TsoResponse {
        match command {
            TsoCommand::AcquireLease {
                leader_id,
                now_ms,
                lease_duration_ms,
            } => {
                if let Some(owner) = state.lease_owner
                    && owner != leader_id
                    && now_ms < state.lease_expires_at_ms
                {
                    return TsoResponse::LeasePending {
                        not_before_ms: state.lease_expires_at_ms,
                    };
                }
                if state.lease_owner == Some(leader_id) && now_ms < state.lease_expires_at_ms {
                    return TsoResponse::LeaseAcquired {
                        expires_at_ms: state.lease_expires_at_ms,
                    };
                }
                let Some(expires_at_ms) = now_ms.checked_add(lease_duration_ms) else {
                    return TsoResponse::TimestampOverflow;
                };
                state.lease_owner = Some(leader_id);
                state.lease_expires_at_ms = expires_at_ms;
                TsoResponse::LeaseAcquired { expires_at_ms }
            }
            TsoCommand::Allocate {
                leader_id,
                lease_now_ms,
                physical_ms,
                count,
            } => {
                if count == 0 {
                    return TsoResponse::InvalidCount;
                }
                if state.lease_owner != Some(leader_id) || lease_now_ms >= state.lease_expires_at_ms
                {
                    return TsoResponse::LeasePending {
                        not_before_ms: state.lease_expires_at_ms,
                    };
                }
                let start = match state.last {
                    Some(last) if physical_ms <= last.physical => match last.checked_next() {
                        Ok(next) => next,
                        Err(_) => return TsoResponse::TimestampOverflow,
                    },
                    _ => HybridTimestamp::new(physical_ms, 0),
                };
                let range = match TimestampRange::from_start(start, count) {
                    Ok(range) => range,
                    Err(_) => return TsoResponse::TimestampOverflow,
                };
                state.last = Some(range.end);
                state.committed_ranges = state.committed_ranges.saturating_add(1);
                TsoResponse::Allocated(range)
            }
        }
    }
}

#[async_trait]
impl StateMachine for TsoStateMachine {
    type Command = Vec<u8>;
    type Response = Vec<u8>;

    async fn apply(
        &mut self,
        _log_id: LogId<u64>,
        command: Self::Command,
    ) -> StateMachineResult<Self::Response> {
        let command: TsoCommand = bincode::deserialize(&command)?;
        let mut state = self.state.write().await;
        let response = Self::transition(&mut state, command);
        Ok(bincode::serialize(&response)?)
    }

    async fn snapshot(&self) -> StateMachineResult<Box<dyn AsyncSnapshotData>> {
        let bytes = bincode::serialize(&*self.state.read().await)?;
        Ok(Box::new(Cursor::new(bytes)))
    }

    async fn restore(
        &mut self,
        mut snapshot: Box<dyn AsyncSnapshotData>,
    ) -> StateMachineResult<()> {
        let mut bytes = Vec::new();
        snapshot.read_to_end(&mut bytes).await?;
        let restored: TsoState = bincode::deserialize(&bytes)?;
        *self.state.write().await = restored;
        Ok(())
    }
}
