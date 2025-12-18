use crate::error::TransportError;
use async_trait::async_trait;
use alopex_chirps_wire::frame::Frame;
use alopex_chirps_wire::node_id::NodeId;
use std::net::SocketAddr;
use tokio::sync::mpsc;

/// Abstraction over transport backends (QUIC, mock, etc.).
#[async_trait]
pub trait MessageBackend: Send + Sync {
    /// Sends a message to a specific target node.
    async fn send(&self, target: NodeId, frame: Frame) -> Result<(), TransportError>;

    /// Broadcasts a message to all connected peers.
    /// Returns the number of peers the message was sent to.
    async fn broadcast(&self, frame: Frame) -> Result<usize, TransportError>;

    /// Subscribes to incoming messages.
    ///
    /// Returns a channel receiver that will be sent tuples of `(sender_node_id, message)`.
    async fn subscribe(&self) -> Result<mpsc::Receiver<(NodeId, Frame)>, TransportError>;

    /// Closes the transport.
    async fn close(&self) -> Result<(), TransportError>;

    /// Returns a list of currently connected peers.
    fn connected_peers(&self) -> Vec<(NodeId, SocketAddr)>;
}
