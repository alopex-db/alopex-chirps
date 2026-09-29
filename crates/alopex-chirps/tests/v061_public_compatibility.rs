//! Source fixture compiled as a v0.6.1 consumer.
//!
//! Struct literals and exhaustive matches deliberately avoid update syntax or
//! wildcard arms where a v0.6.1 public shape must remain exact. A removed
//! item, signature change, required trait addition, added public field, or
//! added exhaustive enum variant therefore fails at the affected call site.

use alopex_chirps::backend::MessageBackend;
use alopex_chirps::config::ConfigError;
use alopex_chirps::error::{GossipError, MeshError, TransportError};
use alopex_chirps::mesh::{Mesh, MeshHandle, MeshMetricsSnapshot};
use alopex_chirps::profile::{
    BackendCapabilities, BackendProfile, EnvelopeMetadata, MessageProfile, ProfileError,
};
use alopex_chirps::{NodeConfig, NodeId};
use alopex_chirps_wire::frame::{Frame, GossipMessage, RaftFrame, UserMessage};
use async_trait::async_trait;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;

#[derive(Default)]
struct V061RequiredMethodsOnlyBackend {
    sends: AtomicUsize,
    broadcasts: AtomicUsize,
}

#[async_trait]
impl MessageBackend for V061RequiredMethodsOnlyBackend {
    async fn send(&self, _target: NodeId, _frame: Frame) -> Result<(), TransportError> {
        self.sends.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn broadcast(&self, _frame: Frame) -> Result<usize, TransportError> {
        self.broadcasts.fetch_add(1, Ordering::SeqCst);
        Ok(7)
    }

    async fn subscribe(&self) -> Result<mpsc::Receiver<(NodeId, Frame)>, TransportError> {
        let (_sender, receiver) = mpsc::channel(1);
        Ok(receiver)
    }

    async fn close(&self) -> Result<(), TransportError> {
        Ok(())
    }

    fn connected_peers(&self) -> Vec<(NodeId, SocketAddr)> {
        Vec::new()
    }
}

fn v061_node_config_literal() -> NodeConfig {
    NodeConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        seeds: Vec::new(),
        cert_path: None,
        key_path: None,
        trusted_cert_paths: Vec::new(),
        ping_timeout: Duration::from_secs(1),
        indirect_ping_timeout: Duration::from_secs(3),
        suspect_to_dead_timeout: Duration::from_secs(6),
        gossip_interval: Duration::from_millis(200),
        max_clock_skew: Duration::from_secs(1),
        broadcast_timeout: Duration::from_millis(200),
        send_queue_capacity: 1024,
        fanout: None,
        convergence_rounds: 3,
        node_id_path: PathBuf::from(".chirps_node_id"),
    }
}

fn v061_capability_literal() -> BackendCapabilities {
    BackendCapabilities {
        control: true,
        ephemeral: true,
        durable: false,
    }
}

fn v061_metadata_literal() -> EnvelopeMetadata {
    EnvelopeMetadata {
        message_id: None,
        sequence: None,
        partition: None,
        acknowledgement: None,
        replay: false,
        checkpoint: None,
        offset: None,
    }
}

fn exhaust_v061_profile(profile: BackendProfile) -> u8 {
    match profile {
        BackendProfile::Control => 0,
        BackendProfile::Ephemeral => 1,
        BackendProfile::Durable => 2,
    }
}

fn exhaust_v061_profile_error(error: ProfileError) -> MessageProfile {
    match error {
        ProfileError::Unsupported { profile, reason: _ } => profile,
    }
}

#[allow(dead_code)]
fn exhaust_v061_frame(frame: Frame) -> u8 {
    match frame {
        Frame::Ping { seq: _, from: _ } => 0,
        Frame::Ack { seq: _, from: _ } => 1,
        Frame::PingReq {
            seq: _,
            from: _,
            target: _,
        } => 2,
        Frame::Gossip(_) => 3,
        Frame::User(_) => 4,
        Frame::Raft(_) => 5,
        Frame::RaftSnapshot(_) => 6,
        Frame::FileTransfer(_) => 7,
        #[cfg(feature = "hlc")]
        Frame::HlcGossip(_) => 8,
    }
}

#[allow(dead_code)]
fn exhaust_v061_config_error(error: ConfigError) -> &'static str {
    match error {
        ConfigError::Certificate(_) => "certificate",
        ConfigError::Io(_) => "io",
    }
}

#[allow(dead_code)]
fn exhaust_v061_transport_error(error: TransportError) -> &'static str {
    match error {
        TransportError::Connection(_) => "connection",
        TransportError::Tls(_) => "tls",
        TransportError::Send(_) => "send",
        TransportError::Subscribe(_) => "subscribe",
        TransportError::Io(_) => "io",
        TransportError::Timeout(_) => "timeout",
        TransportError::InvalidStreamKind(_) => "stream-kind",
        TransportError::NotImplemented(_) => "not-implemented",
    }
}

#[allow(dead_code)]
fn exhaust_v061_gossip_error(error: GossipError) -> &'static str {
    match error {
        GossipError::InvalidUpdate(_) => "invalid-update",
        GossipError::IncarnationConflict(_) => "incarnation-conflict",
        GossipError::Io(_) => "io",
        GossipError::NotImplemented(_) => "not-implemented",
    }
}

#[allow(dead_code)]
fn exhaust_v061_mesh_error(error: MeshError) -> &'static str {
    match error {
        MeshError::Persistence(_) => "persistence",
        MeshError::Config(_) => "config",
        MeshError::Transport(_) => "transport",
        MeshError::Gossip(_) => "gossip",
        MeshError::PeerNotFound(_) => "peer-not-found",
        MeshError::Timeout => "timeout",
        MeshError::NotImplemented(_) => "not-implemented",
        MeshError::Profile(_) => "profile",
    }
}

#[allow(dead_code)]
async fn unchanged_v061_mesh_call_site(
    handle: &MeshHandle,
    target: NodeId,
    frame: Frame,
) -> Result<(), MeshError> {
    handle.send_to(target, frame.clone()).await?;
    handle
        .send_to_with_profile(target, frame.clone(), MessageProfile::Control)
        .await?;
    handle
        .send_enveloped(
            target,
            frame.clone(),
            MessageProfile::Ephemeral,
            v061_metadata_literal(),
        )
        .await?;
    let _: usize = handle.broadcast(frame.clone()).await?;
    let _: usize = handle
        .broadcast_with_profile(frame.clone(), MessageProfile::Control)
        .await?;
    let _: usize = handle
        .broadcast_enveloped(frame, MessageProfile::Ephemeral, v061_metadata_literal())
        .await?;
    let _: mpsc::Receiver<(NodeId, Frame)> = handle.subscribe().await?;
    handle.on_node_join(|_: NodeId| {});
    handle.on_node_leave(|_: NodeId| {});
    handle.on_status_change(|_: NodeId| {});
    let _: NodeId = handle.node_id();
    let _: u64 = handle.incarnation();
    let _: Arc<NodeConfig> = handle.config();
    let _: MeshMetricsSnapshot = handle.metrics();
    let _ = handle.membership().await;
    Ok(())
}

#[allow(dead_code)]
fn unchanged_v061_start_call_sites(config: NodeConfig) {
    std::mem::drop(alopex_chirps::start(config.clone()));
    std::mem::drop(Mesh::start(config));
}

#[test]
fn v061_required_trait_surface_and_defaults_compile_and_run_unchanged() {
    let backend = V061RequiredMethodsOnlyBackend::default();
    assert_eq!(backend.capabilities(), v061_capability_literal());

    let config = v061_node_config_literal();
    let default = NodeConfig::default();
    assert_eq!(default.bind_addr, config.bind_addr);
    assert_eq!(default.seeds, config.seeds);
    assert_eq!(default.cert_path, config.cert_path);
    assert_eq!(default.key_path, config.key_path);
    assert_eq!(default.trusted_cert_paths, config.trusted_cert_paths);
    assert_eq!(default.ping_timeout, config.ping_timeout);
    assert_eq!(default.indirect_ping_timeout, config.indirect_ping_timeout);
    assert_eq!(
        default.suspect_to_dead_timeout,
        config.suspect_to_dead_timeout
    );
    assert_eq!(default.gossip_interval, config.gossip_interval);
    assert_eq!(default.max_clock_skew, config.max_clock_skew);
    assert_eq!(default.broadcast_timeout, config.broadcast_timeout);
    assert_eq!(default.send_queue_capacity, config.send_queue_capacity);
    assert_eq!(default.fanout, config.fanout);
    assert_eq!(default.convergence_rounds, config.convergence_rounds);
    assert_eq!(default.node_id_path, config.node_id_path);
    assert!(default.validate().is_ok());

    assert_eq!(exhaust_v061_profile(BackendProfile::Control), 0);
    assert_eq!(exhaust_v061_profile(BackendProfile::Ephemeral), 1);
    assert_eq!(exhaust_v061_profile(BackendProfile::Durable), 2);
    assert_eq!(v061_metadata_literal(), EnvelopeMetadata::default());

    let metrics = MeshMetricsSnapshot {
        joins: 1,
        leaves: 2,
        status_events: 3,
        delivered_frames: 4,
    };
    assert_eq!(metrics.joins, 1);
    assert_eq!(metrics.leaves, 2);
    assert_eq!(metrics.status_events, 3);
    assert_eq!(metrics.delivered_frames, 4);
}

#[test]
fn v061_profile_error_and_frame_constructor_source_remain_valid() {
    let frame = Frame::User(UserMessage {
        payload: b"v061".to_vec(),
    });
    let Frame::User(UserMessage { payload }) = frame else {
        panic!("v0.6.1 user frame shape changed")
    };
    assert_eq!(payload, b"v061");

    let _ = Frame::Gossip(GossipMessage {
        updates: Vec::new(),
    });
    let _ = Frame::Raft(RaftFrame {
        group_id: 7,
        payload: Vec::new(),
    });

    let error = ProfileError::Unsupported {
        profile: MessageProfile::Durable,
        reason: "fixture",
    };
    assert_eq!(exhaust_v061_profile_error(error), MessageProfile::Durable);
}

// This downstream contract intentionally permits internal bug fixes. Complete
// public API comparison is performed by the pinned v0.6.1 semver release gate.
#[tokio::test]
async fn v061_default_profile_extensions_preserve_delivery_and_reject_durable_fallback() {
    let backend = V061RequiredMethodsOnlyBackend::default();
    let target = NodeId::from([0x61; 16]);
    let frame = Frame::User(UserMessage {
        payload: b"v061".to_vec(),
    });

    for profile in [BackendProfile::Control, BackendProfile::Ephemeral] {
        backend
            .send_with_profile(target, frame.clone(), profile, v061_metadata_literal())
            .await
            .unwrap();
        assert_eq!(
            backend
                .broadcast_with_profile(frame.clone(), profile, v061_metadata_literal())
                .await
                .unwrap(),
            7
        );
    }
    assert!(matches!(
        backend
            .send_with_profile(
                target,
                frame.clone(),
                BackendProfile::Durable,
                v061_metadata_literal(),
            )
            .await,
        Err(TransportError::NotImplemented(_))
    ));
    assert!(matches!(
        backend
            .broadcast_with_profile(frame, BackendProfile::Durable, v061_metadata_literal())
            .await,
        Err(TransportError::NotImplemented(_))
    ));
    assert_eq!(backend.sends.load(Ordering::SeqCst), 2);
    assert_eq!(backend.broadcasts.load(Ordering::SeqCst), 2);
    assert!(backend.subscribe().await.unwrap().recv().await.is_none());
    assert!(backend.connected_peers().is_empty());
    backend.close().await.unwrap();
}

#[cfg(feature = "tso")]
#[test]
fn v061_tso_configuration_literals_remain_exhaustive() {
    use alopex_chirps::tso::{TsoClientConfig, TsoClientOptions, TsoConfig, TsoOracleOptions};
    let _ = TsoConfig {
        timestamp_ttl: Duration::from_secs(3),
    };
    let _ = TsoClientConfig {
        batch_size: 10_000,
        max_retries: 10,
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_secs(1),
    };
    assert_eq!(TsoClientOptions::default().prefetch_threshold, 1_000);
    assert_eq!(TsoOracleOptions::default().batch_size, 10_000);
}

#[cfg(feature = "snapshot")]
#[test]
fn v061_snapshot_shapes_remain_exhaustive() {
    use alopex_chirps::snapshot::{SnapshotTransferConfig, SnapshotTransferError};
    let _ = SnapshotTransferConfig {
        chunk_threshold: 1024,
        chunk_size: 64,
        max_concurrent_chunks: 4,
        max_retries: 3,
    };
    fn legacy_match(error: SnapshotTransferError) -> &'static str {
        match error {
            SnapshotTransferError::InvalidConfig(_) => "config",
            SnapshotTransferError::InvalidManifest(_) => "manifest",
            SnapshotTransferError::Integrity(_) => "integrity",
            SnapshotTransferError::Retryable(_) => "retryable",
            SnapshotTransferError::Terminal(_) => "terminal",
        }
    }
    assert_eq!(
        legacy_match(SnapshotTransferError::terminal("legacy")),
        "terminal"
    );
}
