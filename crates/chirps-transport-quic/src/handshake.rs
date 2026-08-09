use alopex_chirps_wire::node_id::NodeId;
use serde::{Deserialize, Serialize};
use tracing::warn;

pub const PROTOCOL_VERSION: u16 = 0x0006;
pub const MIN_COMPATIBLE_VERSION: u16 = 0x0006;

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
    pub multi_raft: bool,
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
                multi_raft: true,
            },
        }
    }

    pub fn is_compatible(&self) -> bool {
        self.version == PROTOCOL_VERSION
    }
}

#[derive(Debug)]
pub enum HandshakeError {
    VersionMismatch { local: u16, remote: u16 },
    IncompatibleCapabilities,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NegotiatedCapabilities {
    pub priority_streams: bool,
    pub retransmission: bool,
    pub qos: bool,
    pub multi_raft: bool,
}

pub fn negotiate(
    local: &HandshakeMessage,
    remote: &HandshakeMessage,
) -> Result<NegotiatedCapabilities, HandshakeError> {
    if !local.is_compatible() || !remote.is_compatible() || local.version != remote.version {
        warn!(
            event = "version_mismatch",
            remote_version = remote.version,
            local_version = local.version,
            "version_mismatch"
        );
        return Err(HandshakeError::VersionMismatch {
            local: local.version,
            remote: remote.version,
        });
    }

    if !local.capabilities.multi_raft || !remote.capabilities.multi_raft {
        return Err(HandshakeError::IncompatibleCapabilities);
    }

    Ok(NegotiatedCapabilities {
        priority_streams: local.capabilities.priority_streams
            && remote.capabilities.priority_streams,
        retransmission: local.capabilities.retransmission && remote.capabilities.retransmission,
        qos: local.capabilities.qos && remote.capabilities.qos,
        multi_raft: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_test::traced_test;

    #[traced_test]
    #[test]
    fn rejects_v0_3_with_version_mismatch_log() {
        let local = HandshakeMessage::new(NodeId::new());
        let mut remote = HandshakeMessage::new(NodeId::new());
        remote.version = 0x0003;

        let res = negotiate(&local, &remote);
        match res {
            Err(HandshakeError::VersionMismatch { local, remote }) => {
                assert_eq!(local, PROTOCOL_VERSION);
                assert_eq!(remote, 0x0003);
            }
            other => panic!("expected version mismatch, got {other:?}"),
        }
        assert!(logs_contain("version_mismatch"));
        assert!(logs_contain("remote_version=3"));
    }

    #[test]
    fn is_compatible_boundary() {
        let mut msg = HandshakeMessage::new(NodeId::new());
        assert_eq!(PROTOCOL_VERSION, 0x0006);
        msg.version = PROTOCOL_VERSION;
        assert!(msg.is_compatible());
        msg.version = 0x0005;
        assert!(!msg.is_compatible());
        msg.version = 0x0007;
        assert!(!msg.is_compatible());
    }

    #[traced_test]
    #[test]
    fn negotiates_capabilities_by_intersection() {
        let local = HandshakeMessage::new(NodeId::new());
        let mut remote = HandshakeMessage::new(NodeId::new());
        remote.capabilities.priority_streams = false;
        remote.capabilities.retransmission = true;
        remote.capabilities.qos = true;

        let negotiated = negotiate(&local, &remote).expect("compatible");
        assert!(!negotiated.priority_streams);
        assert!(negotiated.retransmission);
        assert!(negotiated.qos);
        assert!(negotiated.multi_raft);
        assert!(!logs_contain("version_mismatch"));
    }

    #[test]
    fn rejects_peer_without_multi_raft_capability() {
        let local = HandshakeMessage::new(NodeId::new());
        let mut remote = HandshakeMessage::new(NodeId::new());
        remote.capabilities.multi_raft = false;

        assert!(matches!(
            negotiate(&local, &remote),
            Err(HandshakeError::IncompatibleCapabilities)
        ));
    }
}
