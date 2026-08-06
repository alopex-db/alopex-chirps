use crate::file_transfer::FileTransferFrame;
use crate::node_id::NodeId;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

/// A frame is the unit of communication between nodes.
#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Frame {
    Ping {
        seq: u64,
        from: NodeId,
    },
    Ack {
        seq: u64,
        from: NodeId,
    },
    PingReq {
        seq: u64,
        from: NodeId,
        target: NodeId,
    },
    Gossip(GossipMessage),
    User(UserMessage),
    /// Raft RPC用の汎用フレーム。payloadは上位層でbincodeシリアライズされたRPCを格納する。
    Raft(RaftFrame),
    /// スナップショット転送専用フレーム。
    RaftSnapshot(RaftFrame),
    /// ファイル転送制御フレーム。
    FileTransfer(FileTransferFrame),
    /// Versioned HLC gossip. Appending this feature-gated variant preserves
    /// every existing bincode enum discriminant.
    #[cfg(feature = "hlc")]
    HlcGossip(HlcGossipMessage),
}

/// A gossip message containing membership updates.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GossipMessage {
    pub updates: Vec<MembershipUpdate>,
}

/// Stable identity for an HLC-stamped event.
#[cfg(feature = "hlc")]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct HlcEventId {
    pub source: NodeId,
    pub sequence: u64,
}

/// A membership event carrying its origin identity and causal timestamp.
#[cfg(feature = "hlc")]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StampedMembershipUpdate {
    pub event_id: HlcEventId,
    pub timestamp: crate::hlc::HybridTimestamp,
    pub update: MembershipUpdate,
}

/// Gossip envelope stamped at send time.
#[cfg(feature = "hlc")]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct HlcGossipMessage {
    pub event_id: HlcEventId,
    pub timestamp: crate::hlc::HybridTimestamp,
    pub updates: Vec<StampedMembershipUpdate>,
}

/// A user-defined message.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UserMessage {
    pub payload: Vec<u8>,
}

/// Raft関連のペイロードを運ぶフレーム。
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RaftFrame {
    /// Raftグループを識別するID。
    pub group_id: u64,
    /// シリアライズ済みのRPC/レスポンス。
    pub payload: Vec<u8>,
}

/// An update to the membership list.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MembershipUpdate {
    pub node_id: NodeId,
    pub incarnation: u64,
    pub addr: SocketAddr,
    pub status: MemberStatus,
}

/// The status of a member in the cluster.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum MemberStatus {
    Alive,
    Suspect,
    Dead,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_ping_frame() {
        let node_id = NodeId::new();
        let frame = Frame::Ping {
            seq: 123,
            from: node_id,
        };
        match frame {
            Frame::Ping { seq, from } => {
                assert_eq!(seq, 123);
                assert_eq!(from, node_id);
            }
            _ => panic!("Expected a Ping frame"),
        }
    }

    #[test]
    fn test_create_ping_req_frame() {
        let node_id1 = NodeId::new();
        let node_id2 = NodeId::new();
        let frame = Frame::PingReq {
            seq: 456,
            from: node_id1,
            target: node_id2,
        };
        match frame {
            Frame::PingReq { seq, from, target } => {
                assert_eq!(seq, 456);
                assert_eq!(from, node_id1);
                assert_eq!(target, node_id2);
            }
            _ => panic!("Expected a PingReq frame"),
        }
    }

    #[cfg(feature = "hlc")]
    #[test]
    fn hlc_gossip_roundtrips_without_changing_legacy_gossip() {
        let source = NodeId::new();
        let peer = NodeId::new();
        let event_id = HlcEventId {
            source,
            sequence: 7,
        };
        let timestamp = crate::hlc::HybridTimestamp::new(100, 3);
        let frame = Frame::HlcGossip(HlcGossipMessage {
            event_id,
            timestamp,
            updates: vec![StampedMembershipUpdate {
                event_id,
                timestamp,
                update: MembershipUpdate {
                    node_id: peer,
                    incarnation: 1,
                    addr: "127.0.0.1:9000".parse().unwrap(),
                    status: MemberStatus::Alive,
                },
            }],
        });

        let bytes = bincode::serialize(&frame).unwrap();
        let decoded: Frame = bincode::deserialize(&bytes).unwrap();

        let Frame::HlcGossip(message) = decoded else {
            panic!("expected HLC gossip frame")
        };
        assert_eq!(message.event_id, event_id);
        assert_eq!(message.timestamp, timestamp);
        assert_eq!(message.updates.len(), 1);

        let legacy = bincode::serialize(&Frame::Gossip(GossipMessage {
            updates: Vec::new(),
        }))
        .unwrap();
        assert_eq!(&legacy[..4], &3u32.to_le_bytes());
    }
}
