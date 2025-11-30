use chirps_wire::node_id::NodeId;
use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u16 = 0x0004;
pub const MIN_COMPATIBLE_VERSION: u16 = 0x0004;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HandshakeMessage {
    pub version: u16,
    pub node_id: NodeId,
    pub capabilities: Capabilities,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Capabilities {
    pub priority_streams: bool,
    pub retransmission: bool,
    pub qos: bool,
}

impl HandshakeMessage {
    pub fn new(node_id: NodeId) -> Self {
        HandshakeMessage {
            version: PROTOCOL_VERSION,
            node_id,
            capabilities: Capabilities {
                priority_streams: true,
                retransmission: true,
                qos: true,
            },
        }
    }

    pub fn is_compatible(&self) -> bool {
        self.version >= MIN_COMPATIBLE_VERSION
    }
}

#[derive(Debug)]
pub enum HandshakeError {
    VersionMismatch { local: u16, remote: u16 },
    IncompatibleCapabilities,
}

#[derive(Debug, Clone)]
pub struct NegotiatedCapabilities {
    pub priority_streams: bool,
    pub retransmission: bool,
    pub qos: bool,
}

pub fn negotiate(
    local: &HandshakeMessage,
    remote: &HandshakeMessage,
) -> Result<NegotiatedCapabilities, HandshakeError> {
    if !local.is_compatible() || !remote.is_compatible() || remote.version < MIN_COMPATIBLE_VERSION
    {
        return Err(HandshakeError::VersionMismatch {
            local: local.version,
            remote: remote.version,
        });
    }

    Ok(NegotiatedCapabilities {
        priority_streams: local.capabilities.priority_streams
            && remote.capabilities.priority_streams,
        retransmission: local.capabilities.retransmission && remote.capabilities.retransmission,
        qos: local.capabilities.qos && remote.capabilities.qos,
    })
}
