//! Raftノード管理モジュール。openraft v0.9.17を用いて単一グループの合意を提供する。
//! RaftConfig/RaftError/ChirpsRaftTransport/RaftNodeを公開し、呼び出し側がシンプルに利用できる形にまとめる。

pub mod config;
pub mod error;
pub mod node;
pub mod transport;

pub use config::RaftConfig;
pub use error::{RaftError, RaftResult};
pub use node::{RaftMessage, RaftNode};
pub use transport::{
    ChirpsRaftNetworkClient, ChirpsRaftNetworkFactory, ChirpsRaftTransport, RaftFramePayload,
};

// 型エイリアスとopenraft型を一括で再エクスポートする。
pub use chirps_raft_storage::types::{
    AppendEntriesRequest, AppendEntriesResponse, BasicNode, ChirpsNodeId, ChirpsTypeConfig, Entry,
    GroupId, InstallSnapshotRequest, InstallSnapshotResponse, LogId, Membership, VoteRequest,
    VoteResponse,
};
