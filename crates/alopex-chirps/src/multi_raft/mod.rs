mod error;
mod group;
mod id;
mod manager;
mod storage;

pub use alopex_chirps_raft_storage::types::GroupId;
pub use error::MultiRaftError;
pub use group::GroupHandle;
pub use id::{group_namespace, parse_group_namespace};
pub use manager::{GroupTickResult, MultiRaftManager, RoutedRaftResponse};
pub use storage::{
    RaftStorageFactory, SharedRaftStorage, StorageTransaction, WalRaftStorageFactory,
};
