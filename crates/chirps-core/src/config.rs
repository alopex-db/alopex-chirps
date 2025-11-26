use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Invalid certificate or key: {0}")]
    Certificate(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Configuration for a alopex-chirps node.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Address to bind for the transport.
    pub bind_addr: SocketAddr,
    /// A list of seed nodes to connect to for bootstrapping.
    pub seeds: Vec<SocketAddr>,
    /// Path to the TLS certificate file.
    pub cert_path: Option<PathBuf>,
    /// Path to the TLS private key file.
    pub key_path: Option<PathBuf>,
    /// Timeout for a direct ping to a node.
    pub ping_timeout: Duration,
    /// Timeout for an indirect ping (via neighbors).
    pub indirect_ping_timeout: Duration,
    /// Timeout after which a suspected node is declared dead.
    pub suspect_to_dead_timeout: Duration,
    /// Interval for periodic gossip ticks.
    pub gossip_interval: Duration,
    /// Timeout for send/broadcast operations.
    pub broadcast_timeout: Duration,
    /// Maximum number of in-flight send/broadcast requests.
    pub send_queue_capacity: usize,
    /// Fanout for gossip messages. If None, it's calculated as `max(3, ceil(sqrt(N)))`.
    pub fanout: Option<usize>,
    /// Number of convergence rounds for gossip.
    pub convergence_rounds: usize,
    /// Path to the file where the node ID is persisted.
    pub node_id_path: PathBuf,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            seeds: Vec::new(),
            cert_path: None,
            key_path: None,
            ping_timeout: Duration::from_secs(1),
            indirect_ping_timeout: Duration::from_secs(3),
            suspect_to_dead_timeout: Duration::from_secs(6),
            gossip_interval: Duration::from_millis(200),
            broadcast_timeout: Duration::from_millis(200),
            send_queue_capacity: 1024,
            fanout: None,
            convergence_rounds: 3,
            node_id_path: PathBuf::from(".chirps_node_id"),
        }
    }
}

impl NodeConfig {
    /// Validates the configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        match (&self.cert_path, &self.key_path) {
            (Some(cert), Some(key)) => {
                if !cert.exists() {
                    return Err(ConfigError::Certificate(format!(
                        "Certificate file not found: {}",
                        cert.display()
                    )));
                }
                if !key.exists() {
                    return Err(ConfigError::Certificate(format!(
                        "Key file not found: {}",
                        key.display()
                    )));
                }
            }
            (Some(_), None) => {
                return Err(ConfigError::Certificate(
                    "Key file must be provided if certificate is provided".to_string(),
                ));
            }
            (None, Some(_)) => {
                return Err(ConfigError::Certificate(
                    "Certificate file must be provided if key is provided".to_string(),
                ));
            }
            (None, None) => {
                // Self-signed certificates will be generated in this case.
            }
        }
        Ok(())
    }
}
