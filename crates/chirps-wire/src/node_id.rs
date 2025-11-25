use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A unique identifier for a node in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NodeId([u8; 16]);

impl NodeId {
    /// Creates a new random `NodeId`.
    pub fn new() -> Self {
        NodeId(*Uuid::new_v4().as_bytes())
    }

    /// Parses a `NodeId` from a byte slice.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.len() != 16 {
            return Err("NodeId must be 16 bytes long");
        }
        let mut arr = [0u8; 16];
        arr.copy_from_slice(bytes);
        Ok(NodeId(arr))
    }

    /// Returns the `NodeId` as a byte slice.
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

impl From<[u8; 16]> for NodeId {
    fn from(bytes: [u8; 16]) -> Self {
        NodeId(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::NodeId;

    #[test]
    fn roundtrip_from_bytes() {
        let bytes = [1u8; 16];
        let node_id = NodeId::from_bytes(&bytes).unwrap();
        assert_eq!(node_id.as_bytes(), &bytes);
        assert!(NodeId::from_bytes(&[0u8; 15]).is_err());
    }
}
