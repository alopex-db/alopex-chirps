//! Alopex Chirps のメッシュAPI。QUICトランスポートとSWIMゴシップをまとめて起動し、送信・ブロードキャスト・イベント購読を提供する。

pub mod backend;
pub mod config;
pub mod error;
pub mod mesh;
#[cfg(feature = "multi-raft")]
pub mod multi_raft;
pub mod node_id;
pub mod profile;
pub mod raft;

use crate::config::NodeConfig;
use crate::error::MeshError;
use crate::mesh::Mesh;
pub use crate::mesh::MeshHandle;
pub use crate::node_id::NodeId;
pub use crate::profile::{MessageProfile, enforce_profile};
pub use crate::raft::{
    ChirpsRaftTransport, MetricsError, RaftConfig, RaftError, RaftMessage, RaftMetricsCollector,
    RaftMetricsUpdate, RaftNode, serve_metrics,
};
pub use alopex_chirps_file_transfer::{
    BroadcastHandle, CompressionAlgorithm, ConflictResolution, FileInfo, FileMetadata,
    FileTransferConfig, FileTransferError, FileTransferService, FileTransferServiceImpl,
    HashAlgorithm, ListOptions, RemoveOptions, RetryPolicy, SyncDirection, SyncHandle, SyncOptions,
    TransferHandle, TransferKind, TransferMode, TransferOptions, TransferSessionId,
    TransferSessionInfo, TransferState,
};
pub use alopex_chirps_wire::frame::{Frame, UserMessage};

/// 新しいメッシュを起動する。設定の検証・NodeId永続化・QUICトランスポート・ゴシップエンジンをまとめて初期化する。
///
/// # 返り値
/// 成功時は `MeshHandle` を返す。エラー時は設定・永続化・トランスポートの各失敗を `MeshError` で返す。
pub async fn start(config: NodeConfig) -> Result<MeshHandle, MeshError> {
    Mesh::start(config).await
}
