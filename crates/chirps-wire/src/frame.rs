use crate::node_id::NodeId;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

/// A frame is the unit of communication between nodes.
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
}

/// A gossip message containing membership updates.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GossipMessage {
    pub updates: Vec<MembershipUpdate>,
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
#[derive(Serialize, Deserialize, Debug, Clone)]
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
}
