use thiserror::Error;

/// Transport-layer errors (QUIC/TLS/IO).
#[derive(Debug, Error)]
pub enum TransportError {
    #[error("connection failed: {0}")]
    Connection(String),
    #[error("tls handshake failed: {0}")]
    Tls(String),
    #[error("send failed: {0}")]
    Send(String),
    #[error("subscribe failed: {0}")]
    Subscribe(String),
    #[error("transport IO error: {0}")]
    Io(String),
    #[error("not implemented: {0}")]
    NotImplemented(&'static str),
}

/// Gossip-layer errors.
#[derive(Debug, Error)]
pub enum GossipError {
    #[error("invalid update: {0}")]
    InvalidUpdate(String),
    #[error("incarnation conflict: {0}")]
    IncarnationConflict(String),
    #[error("gossip IO error: {0}")]
    Io(String),
    #[error("not implemented: {0}")]
    NotImplemented(&'static str),
}
