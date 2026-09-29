use crate::multi_raft::GroupId;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Debug, Error, PartialEq, Eq, Serialize, Deserialize)]
pub enum TsoError {
    #[error("TSO request count must be greater than zero")]
    InvalidCount,
    #[error("group {actual:?} is not the dedicated TSO group {expected:?}")]
    InvalidTsoGroup { expected: GroupId, actual: GroupId },
    #[error("not the TSO leader; current leader: {0:?}")]
    NotLeader(Option<u64>),
    #[error("new leader cannot issue before the previous lease expires at {not_before_ms}")]
    LeaseNotReady { not_before_ms: u64 },
    #[error("node {node_id} is not authenticated")]
    Unauthenticated { node_id: u64 },
    #[error("TSO transport failed: {0}")]
    Transport(String),
    #[error("TSO Raft operation failed: {0}")]
    Raft(String),
    #[error("TSO codec failed: {0}")]
    Codec(String),
    #[error("hybrid timestamp overflow")]
    TimestampOverflow,
    #[error("TSO returned a timestamp that does not advance the client sequence")]
    NonMonotonicResponse,
    #[error("invalid TSO configuration: {0}")]
    InvalidConfig(String),
}
