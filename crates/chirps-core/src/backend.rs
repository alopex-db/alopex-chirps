use crate::error::TransportError;
use alopex_chirps_wire::frame::Frame;
use alopex_chirps_wire::node_id::NodeId;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::sync::mpsc;

/// Delivery semantics requested from a message backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackendProfile {
    Control,
    Ephemeral,
    Durable,
}

/// Profiles a backend can implement without fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendCapabilities {
    pub control: bool,
    pub ephemeral: bool,
    pub durable: bool,
}

impl Default for BackendCapabilities {
    fn default() -> Self {
        Self {
            control: true,
            ephemeral: true,
            durable: false,
        }
    }
}

impl BackendCapabilities {
    pub fn supports(self, profile: BackendProfile) -> bool {
        match profile {
            BackendProfile::Control => self.control,
            BackendProfile::Ephemeral => self.ephemeral,
            BackendProfile::Durable => self.durable,
        }
    }
}

/// Additive metadata reserved for durable acknowledgement/replay backends.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct EnvelopeMetadata {
    pub message_id: Option<[u8; 16]>,
    pub sequence: Option<u64>,
    pub partition: Option<u64>,
    pub acknowledgement: Option<u64>,
    pub replay: bool,
    pub checkpoint: Option<u64>,
    pub offset: Option<u64>,
}

/// Abstraction over transport backends (QUIC, mock, etc.).
#[async_trait]
pub trait MessageBackend: Send + Sync {
    /// Reports profile support. Durable is opt-in and false by default.
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::default()
    }

    /// Sends a message to a specific target node.
    async fn send(&self, target: NodeId, frame: Frame) -> Result<(), TransportError>;

    /// Profile-aware extension point. Durable backends must override this method;
    /// the default implementation never falls back to the control path.
    async fn send_with_profile(
        &self,
        target: NodeId,
        frame: Frame,
        profile: BackendProfile,
        _metadata: EnvelopeMetadata,
    ) -> Result<(), TransportError> {
        if !self.capabilities().supports(profile) || profile == BackendProfile::Durable {
            return Err(TransportError::NotImplemented(
                "requested message profile is not supported by this backend",
            ));
        }
        self.send(target, frame).await
    }

    /// Broadcasts a message to all connected peers.
    /// Returns the number of peers the message was sent to.
    async fn broadcast(&self, frame: Frame) -> Result<usize, TransportError>;

    /// Broadcast counterpart of `send_with_profile`.
    async fn broadcast_with_profile(
        &self,
        frame: Frame,
        profile: BackendProfile,
        _metadata: EnvelopeMetadata,
    ) -> Result<usize, TransportError> {
        if !self.capabilities().supports(profile) || profile == BackendProfile::Durable {
            return Err(TransportError::NotImplemented(
                "requested message profile is not supported by this backend",
            ));
        }
        self.broadcast(frame).await
    }

    /// Subscribes to incoming messages.
    ///
    /// Returns a channel receiver that will be sent tuples of `(sender_node_id, message)`.
    async fn subscribe(&self) -> Result<mpsc::Receiver<(NodeId, Frame)>, TransportError>;

    /// Closes the transport.
    async fn close(&self) -> Result<(), TransportError>;

    /// Returns a list of currently connected peers.
    fn connected_peers(&self) -> Vec<(NodeId, SocketAddr)>;
}
