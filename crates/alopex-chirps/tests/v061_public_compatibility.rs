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
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

const V061_BASELINE: &str = "chirps-v0.6.1";

fn git_output(repository: &std::path::Path, arguments: &[&str]) -> Vec<u8> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(arguments)
        .output()
        .expect("git must be available for the v0.6.1 source compatibility gate");
    assert!(
        output.status.success(),
        "git {:?} failed: {}",
        arguments,
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("alopex-chirps must remain under the workspace crates directory")
        .to_owned()
}

struct V061RequiredMethodsOnlyBackend;

#[async_trait]
impl MessageBackend for V061RequiredMethodsOnlyBackend {
    async fn send(&self, _target: NodeId, _frame: Frame) -> Result<(), TransportError> {
        Ok(())
    }

    async fn broadcast(&self, _frame: Frame) -> Result<usize, TransportError> {
        Ok(0)
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
    let backend = V061RequiredMethodsOnlyBackend;
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

#[test]
fn every_v061_crate_source_file_remains_byte_for_byte_compatible() {
    let repository = repository_root();
    let peeled = git_output(
        &repository,
        &["rev-parse", &format!("{V061_BASELINE}^{{commit}}")],
    );
    assert_eq!(
        String::from_utf8_lossy(&peeled).trim(),
        "3ff0ce6a631fd235fc4a3e3e08c8a9665f3d8bd9",
        "the compatibility fixture must stay pinned to the reviewed v0.6.1 commit"
    );

    let paths = git_output(
        &repository,
        &[
            "ls-tree",
            "-r",
            "--name-only",
            V061_BASELINE,
            "--",
            "crates",
        ],
    );
    let paths = String::from_utf8(paths).expect("v0.6.1 paths must be UTF-8");
    let legacy_sources = paths
        .lines()
        .filter(|path| path.contains("/src/") && path.ends_with(".rs"));
    let mut checked = 0usize;

    for path in legacy_sources {
        let baseline = git_output(&repository, &["show", &format!("{V061_BASELINE}:{path}")]);
        let current = std::fs::read(repository.join(path))
            .unwrap_or_else(|error| panic!("legacy source {path} is missing: {error}"));
        let current = if path == "crates/chirps-core/src/lib.rs" {
            let current = String::from_utf8(current).expect("chirps-core lib.rs must be UTF-8");
            assert_eq!(
                current.matches("pub mod durable;\n").count(),
                1,
                "chirps-core must contain exactly one reviewed additive durable module"
            );
            current.replacen("pub mod durable;\n", "", 1).into_bytes()
        } else {
            current
        };

        assert_eq!(
            current, baseline,
            "legacy v0.6.1 source changed at {path}; add new APIs in new modules or extend the compatibility fixture under independent review"
        );
        checked += 1;
    }

    assert!(
        checked > 20,
        "expected the complete v0.6.1 crate source set"
    );
}
