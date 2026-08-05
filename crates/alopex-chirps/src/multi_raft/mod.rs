mod error;
mod id;
mod storage;

pub use alopex_chirps_raft_storage::types::GroupId;
pub use error::MultiRaftError;
pub use id::{group_namespace, parse_group_namespace};
pub use storage::{
    RaftStorageFactory, SharedRaftStorage, StorageTransaction, WalRaftStorageFactory,
};
