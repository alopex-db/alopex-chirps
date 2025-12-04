use chirps_raft_storage::types::{ChirpsNodeId, GroupId};
use serde::{Deserialize, Serialize};

/// Raftグループの挙動を制御する設定値。
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
        }
    }
}
