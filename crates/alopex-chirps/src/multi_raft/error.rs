use super::GroupId;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Stable, typed failures returned by Multi-Raft lifecycle and routing APIs.
#[derive(Clone, Debug, Error, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MultiRaftError {
    #[error("group {group_id:?} already exists")]
    GroupAlreadyExists { group_id: GroupId },
    #[error("group {group_id:?} is unknown")]
    UnknownGroup { group_id: GroupId },
    #[error("group {group_id:?} is not accepting new work")]
    GroupUnavailable { group_id: GroupId },
    #[error("invalid group identifier or namespace: {value}")]
    InvalidGroupId { value: String },
    #[error("storage creation failed for group {group_id:?}: {reason}")]
    StorageCreation { group_id: GroupId, reason: String },
    #[error("Raft node initialization failed for group {group_id:?}: {reason}")]
    NodeInitialization { group_id: GroupId, reason: String },
    #[error("transport registration failed for group {group_id:?}: {reason}")]
    TransportRegistration { group_id: GroupId, reason: String },
    #[error("routing failed for group {group_id:?}: {reason}")]
    Routing { group_id: GroupId, reason: String },
    #[error("shutdown failed for group {group_id:?}: {reason}")]
    Shutdown { group_id: GroupId, reason: String },
    #[error("message profile {profile} is not supported: {reason}")]
    UnsupportedProfile { profile: String, reason: String },
}
