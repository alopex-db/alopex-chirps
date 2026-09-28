//! Alopex Chirps のメッシュAPI。QUICトランスポートとSWIMゴシップをまとめて起動し、送信・ブロードキャスト・イベント購読を提供する。

pub mod backend;
pub mod buffer;
pub mod config;
#[cfg(feature = "durable-iggy")]
pub mod durable;
pub mod error;
#[cfg(feature = "hlc")]
pub mod hlc;
pub mod memory;
pub mod mesh;
#[cfg(feature = "multi-raft")]
pub mod multi_raft;
pub mod node_id;
pub mod profile;
pub mod raft;
#[cfg(feature = "snapshot")]
pub mod snapshot;
#[cfg(feature = "tso")]
pub mod tso;

pub use crate::buffer::{
    BackpressureController, BackpressureLevel, BufferError, MessageBuffer, PriorityQueue,
};
pub use crate::config::NodeConfig;
#[cfg(feature = "durable-verification")]
pub use crate::durable::{AppendVerificationError, AppendVerificationObserver};
#[cfg(feature = "durable-iggy")]
pub use crate::durable::{
    DURABLE_CHECKPOINT_JOURNAL_LIMIT_BYTES, DurableBuildError, DurableBuilder,
    DurableCapacityConfig, DurableCapacityLimit, DurableCapacityUsage, DurableCheckpointConfig,
    DurableClockReading, DurableClockSource, DurableClockTrust, DurableCompactionOutcome,
    DurableConfig, DurableCredential, DurableCredentialProvider, DurableCredentialProviderError,
    DurableDeliveryClock, DurableDevelopmentResourceConfig, DurableExtensionConfig, DurableHandle,
    DurableLeaseConfig, DurableLocalStateError, DurableLocalStateStatus,
    DurableObservabilityReport, DurablePartitionProjection, DurablePoll, DurablePrepareError,
    DurableProfile, DurableResourceConfig, DurableRoutingConfig, DurableSendError,
    DurableShutdownError, DurableShutdownTrigger, DurableStateCategory, DurableSubscriptionError,
    DurableTlsConfig,
};
use crate::error::MeshError;
#[cfg(feature = "hlc")]
pub use crate::hlc::{
    Clock, HlcAdvance, HlcError, HlcMetricsSink, HlcReceiveResult, HybridTimestamp, LocalHlc,
    SystemClock,
};
pub use crate::memory::{
    AllocationRatio, BlockCacheHandle, IntegratedCacheManager, MemoryComponent, MemoryConfig,
    MemoryError, MemoryManager, MemoryStats, RaftLogCache, UnifiedMemoryMetrics, WorkloadProfile,
};
use crate::mesh::Mesh;
pub use crate::mesh::MeshHandle;
pub use crate::node_id::NodeId;
pub use crate::profile::{
    BackendCapabilities, EnvelopeMetadata, MessageProfile, ProfileError, enforce_profile,
    resolve_profile,
};
pub use crate::raft::{
    ChirpsMetricsCollector, ChirpsRaftTransport, HlcMetricsUpdate, MetricsAuthError,
    MetricsEndpointAuth, MetricsError, RaftConfig, RaftError, RaftMessage, RaftMessageMetric,
    RaftMetricsCollector, RaftMetricsUpdate, RaftNode, SwimMetricsUpdate, TransportMetricsUpdate,
    TsoMetricsUpdate, serve_metrics, serve_metrics_authorized,
};
pub use alopex_chirps_core::connectivity::{
    EndpointCandidate, EndpointResolver, EndpointSource, PeerEndpoints, StaticEndpointResolver,
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

/// Starts a mesh with a resolver that supplies network locations separately
/// from authenticated node identity.
pub async fn start_with_endpoint_resolver(
    config: NodeConfig,
    endpoint_resolver: std::sync::Arc<dyn EndpointResolver>,
) -> Result<MeshHandle, MeshError> {
    Mesh::start_with_endpoint_resolver(config, endpoint_resolver).await
}

/// Starts a mesh and connects real HLC operations to the unified registry.
#[cfg(feature = "hlc")]
pub async fn start_with_metrics(
    config: NodeConfig,
    metrics: std::sync::Arc<ChirpsMetricsCollector>,
) -> Result<MeshHandle, MeshError> {
    Mesh::start_with_metrics(config, metrics).await
}
