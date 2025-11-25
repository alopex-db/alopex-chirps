pub use chirps_core::error::{GossipError, TransportError};
use chirps_wire::node_id::NodeId;
use thiserror::Error;

/// Errors surfaced by the mesh API.
#[derive(Debug, Error)]
pub enum MeshError {
    #[error("persistence error: {0}")]
    Persistence(#[from] std::io::Error),
    #[error("config error: {0}")]
    Config(String),
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error(transparent)]
    Gossip(#[from] GossipError),
    #[error("peer not found: {0:?}")]
    PeerNotFound(NodeId),
    #[error("operation timed out")]
    Timeout,
    #[error("not implemented: {0}")]
    NotImplemented(&'static str),
}

impl MeshError {
    pub fn config<T: ToString>(msg: T) -> Self {
        MeshError::Config(msg.to_string())
    }
}
