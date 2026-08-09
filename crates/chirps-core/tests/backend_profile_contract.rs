use alopex_chirps_core::backend::{
    BackendCapabilities, BackendProfile, EnvelopeMetadata, MessageBackend,
};
use alopex_chirps_core::error::TransportError;
use alopex_chirps_wire::frame::{Frame, UserMessage};
use alopex_chirps_wire::node_id::NodeId;
use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;

#[derive(Default)]
struct BackendWithoutDurableOverride {
    raw_sends: AtomicUsize,
}

#[async_trait]
impl MessageBackend for BackendWithoutDurableOverride {
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            durable: true,
            ..BackendCapabilities::default()
        }
    }

    async fn send(&self, _target: NodeId, _frame: Frame) -> Result<(), TransportError> {
        self.raw_sends.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn broadcast(&self, _frame: Frame) -> Result<usize, TransportError> {
        self.raw_sends.fetch_add(1, Ordering::SeqCst);
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

fn frame() -> Frame {
    Frame::User(UserMessage {
        payload: b"contract".to_vec(),
    })
}

#[tokio::test]
async fn durable_requires_a_backend_override_and_never_uses_raw_send() {
    let backend = BackendWithoutDurableOverride::default();

    let error = backend
        .send_with_profile(
            NodeId::new(),
            frame(),
            BackendProfile::Durable,
            EnvelopeMetadata::default(),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, TransportError::NotImplemented(_)));
    assert_eq!(backend.raw_sends.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn control_and_ephemeral_keep_the_additive_legacy_path() {
    let backend = BackendWithoutDurableOverride::default();

    backend
        .send_with_profile(
            NodeId::new(),
            frame(),
            BackendProfile::Control,
            EnvelopeMetadata::default(),
        )
        .await
        .unwrap();
    backend
        .send_with_profile(
            NodeId::new(),
            frame(),
            BackendProfile::Ephemeral,
            EnvelopeMetadata::default(),
        )
        .await
        .unwrap();

    assert_eq!(backend.raw_sends.load(Ordering::SeqCst), 2);
}
