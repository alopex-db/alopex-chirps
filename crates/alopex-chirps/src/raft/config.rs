use alopex_chirps_raft_storage::types::{ChirpsNodeId, GroupId};
use serde::{Deserialize, Serialize};

#[cfg(feature = "snapshot")]
use crate::snapshot::{
    DEFAULT_CHUNK_SIZE, DEFAULT_CHUNK_THRESHOLD, DEFAULT_MAX_CONCURRENT_CHUNKS,
    DEFAULT_MAX_RETRIES, SnapshotTransferConfig,
};

/// Raftグループの挙動を制御する設定値。
///
/// # 例
///
/// ```rust,ignore
/// use alopex_chirps::raft::RaftConfig;
/// use alopex_chirps_raft_storage::types::GroupId;
///
/// let mut cfg = RaftConfig::default();
/// cfg.group_id = GroupId(10);
/// cfg.node_id = 1;
/// cfg.election_timeout_ms = 300;
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RaftConfig {
    /// グループID
    pub group_id: GroupId,
    /// 自ノードのRaft ID（Chirpsではu64を採用）
    pub node_id: ChirpsNodeId,
    /// 選挙タイムアウト（ミリ秒）
    pub election_timeout_ms: u64,
    /// ハートビート間隔（ミリ秒）
    pub heartbeat_interval_ms: u64,
    /// AppendEntriesの最大バッチ数
    pub max_batch_size: usize,
    /// スナップショット生成のしきい値（ログエントリ数）
    pub snapshot_threshold: u64,
    /// スナップショット後に保持するログ本数
    pub max_in_snapshot_log_to_keep: u64,
    #[cfg(feature = "snapshot")]
    /// Bytes above which the verified parallel snapshot protocol is used.
    pub snapshot_chunk_threshold: usize,
    #[cfg(feature = "snapshot")]
    /// Payload bytes per verified snapshot chunk.
    pub snapshot_chunk_size: usize,
    #[cfg(feature = "snapshot")]
    /// Maximum number of snapshot chunks in flight at once.
    pub snapshot_max_concurrent_chunks: usize,
    #[cfg(feature = "snapshot")]
    /// Retry count after a chunk's initial attempt.
    pub snapshot_max_retries: usize,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            group_id: GroupId(0),
            node_id: 0,
            election_timeout_ms: 150,
            heartbeat_interval_ms: 50,
            max_batch_size: 1_000,
            snapshot_threshold: 10_000,
            max_in_snapshot_log_to_keep: 1_000,
            #[cfg(feature = "snapshot")]
            snapshot_chunk_threshold: DEFAULT_CHUNK_THRESHOLD,
            #[cfg(feature = "snapshot")]
            snapshot_chunk_size: DEFAULT_CHUNK_SIZE,
            #[cfg(feature = "snapshot")]
            snapshot_max_concurrent_chunks: DEFAULT_MAX_CONCURRENT_CHUNKS,
            #[cfg(feature = "snapshot")]
            snapshot_max_retries: DEFAULT_MAX_RETRIES,
        }
    }
}

#[cfg(feature = "snapshot")]
impl RaftConfig {
    pub(crate) fn snapshot_transfer_config(&self) -> SnapshotTransferConfig {
        SnapshotTransferConfig {
            chunk_threshold: self.snapshot_chunk_threshold,
            chunk_size: self.snapshot_chunk_size,
            max_concurrent_chunks: self.snapshot_max_concurrent_chunks,
            max_retries: self.snapshot_max_retries,
        }
    }
}
