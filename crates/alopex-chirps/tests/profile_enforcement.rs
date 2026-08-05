use alopex_chirps::profile::{
    BackendCapabilities, EnvelopeMetadata, MessageProfile, ProfileError, enforce_profile,
    resolve_profile,
};
use alopex_chirps_core::backend::{BackendProfile, MessageBackend};
use alopex_chirps_core::error::TransportError;
use alopex_chirps_wire::frame::{Frame, RaftFrame, UserMessage};
use alopex_chirps_wire::node_id::NodeId;
use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;

#[derive(Default)]
struct CountingBackend {
    sends: AtomicUsize,
}

#[async_trait]
impl MessageBackend for CountingBackend {
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            durable: true,
            ..BackendCapabilities::default()
        }
    }

    async fn send(&self, _target: NodeId, _frame: Frame) -> Result<(), TransportError> {
        self.sends.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn broadcast(&self, _frame: Frame) -> Result<usize, TransportError> {
        self.sends.fetch_add(1, Ordering::SeqCst);
        Ok(1)
    }

    async fn subscribe(&self) -> Result<mpsc::Receiver<(NodeId, Frame)>, TransportError> {
        let (_tx, rx) = mpsc::channel(1);
        Ok(rx)
    }

    async fn close(&self) -> Result<(), TransportError> {
        Ok(())
    }

    fn connected_peers(&self) -> Vec<(NodeId, SocketAddr)> {
        Vec::new()
    }
}

fn user_frame() -> Frame {
    Frame::User(UserMessage {
        payload: b"hello".to_vec(),
    })
}

#[test]
fn control_pass_through_for_user_frame() {
    let frame = user_frame();
    let eff = enforce_profile(&frame, MessageProfile::Control).unwrap();
    assert_eq!(eff, MessageProfile::Control);
}

#[test]
fn ephemeral_pass_through_when_not_raft() {
    let frame = user_frame();
    let eff = enforce_profile(&frame, MessageProfile::Ephemeral).unwrap();
    assert_eq!(eff, MessageProfile::Ephemeral);
}

#[test]
fn durable_is_not_implemented() {
    let frame = user_frame();
    let res = enforce_profile(&frame, MessageProfile::Durable);
    assert!(res.is_err(), "Durable should return NotImplemented error");
}

#[test]
fn durable_is_a_typed_capability_error_and_metadata_is_reserved() {
    let frame = user_frame();
    let error = resolve_profile(
        &frame,
        MessageProfile::Durable,
        BackendCapabilities::default(),
    )
    .unwrap_err();
    assert!(matches!(
        error,
        ProfileError::Unsupported {
            profile: MessageProfile::Durable,
            ..
        }
    ));

    let metadata = EnvelopeMetadata {
        message_id: Some([7; 16]),
        sequence: Some(3),
        partition: Some(2),
        acknowledgement: Some(1),
        replay: true,
        checkpoint: Some(8),
        offset: Some(9),
    };
    let encoded = serde_json::to_vec(&metadata).unwrap();
    let decoded: EnvelopeMetadata = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, metadata);
}

#[tokio::test]
async fn default_backend_extension_never_falls_back_from_durable() {
    let backend = CountingBackend::default();
    let result = backend
        .send_with_profile(
            NodeId::new(),
            user_frame(),
            BackendProfile::Durable,
            EnvelopeMetadata::default(),
        )
        .await;

    assert!(matches!(result, Err(TransportError::NotImplemented(_))));
    assert_eq!(backend.sends.load(Ordering::SeqCst), 0);
}

#[test]
fn raft_frames_should_force_control_and_warn() {
    let frame = Frame::Raft(RaftFrame {
        group_id: 1,
        payload: Vec::new(),
    });
    let eff = enforce_profile(&frame, MessageProfile::Ephemeral).unwrap();
    assert_eq!(eff, MessageProfile::Control);
}
