pub mod backend;
pub mod config;
pub mod error;
pub mod mesh;
pub mod node_id;

use crate::config::NodeConfig;
use crate::error::MeshError;
use crate::mesh::Mesh;
pub use crate::mesh::MeshHandle;

/// Starts a new `Mesh` instance.
pub async fn start(config: NodeConfig) -> Result<MeshHandle, MeshError> {
    Mesh::start(config).await
}
