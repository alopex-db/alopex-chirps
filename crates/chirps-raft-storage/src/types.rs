use openraft::declare_raft_types;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

#[allow(unexpected_cfgs)]
mod raft_types_decl {
    use super::*;

    declare_raft_types!(
        /// Chirps向けのRaft型構成。openraft標準型をそのまま採用する。
        pub ChirpsTypeConfig:
            /// コマンドペイロード
            D = Vec<u8>,
            /// 応答ペイロード
            R = Vec<u8>,
            /// ノードID
            NodeId = u64,
            /// ノードメタ情報
            Node = openraft::BasicNode,
            /// ログエントリ型
            Entry = openraft::Entry<ChirpsTypeConfig>,
            /// スナップショットデータ
            SnapshotData = Cursor<Vec<u8>>,
    );
}

pub use raft_types_decl::*;

/// Chirps内で扱うノードIDエイリアス。
pub type ChirpsNodeId = u64;

/// Raftグループを識別するID。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GroupId(pub u64);

/// Voteのエイリアス（利便性のため）。
pub type ChirpsVote = Vote<ChirpsNodeId>;

// openraft v0.9.17で提供される主要型を再エクスポートする。
pub use openraft::{
    BasicNode, CommittedLeaderId, Entry, EntryPayload, LogId, LogState, Membership, OptionalSend,
    RaftTypeConfig, Snapshot, SnapshotMeta, StorageError, StoredMembership, Vote,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    storage::{LogFlushed, RaftLogReader, RaftSnapshotBuilder},
};
