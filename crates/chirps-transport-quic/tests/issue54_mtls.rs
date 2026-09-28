use alopex_chirps_core::backend::MessageBackend;
use alopex_chirps_core::config::NodeConfig;
use alopex_chirps_transport_quic::QuicBackend;
use alopex_chirps_wire::node_id::NodeId;
use quinn::{ClientConfig, Endpoint, crypto::rustls::QuicClientConfig};
use rcgen::generate_simple_self_signed;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn server_rejects_tls_client_without_a_certificate() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let certificate = generate_simple_self_signed(["alopex.local".to_owned()])?;
    let cert_path = dir.path().join("node.crt");
    let key_path = dir.path().join("node.key");
    fs::write(&cert_path, certificate.serialize_der()?)?;
    fs::write(&key_path, certificate.serialize_private_key_der())?;

    let node_config = NodeConfig {
        bind_addr: "127.0.0.1:0".parse::<SocketAddr>()?,
        cert_path: Some(cert_path.clone()),
        key_path: Some(key_path),
        trusted_cert_paths: vec![cert_path.clone()],
        ..NodeConfig::default()
    };
    let backend = QuicBackend::new(NodeId::new(), Arc::new(node_config)).await?;

    let mut roots = RootCertStore::empty();
    roots.add(CertificateDer::from(fs::read(cert_path)?))?;
    let mut client_crypto = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_crypto.alpn_protocols = vec![b"alopex".to_vec()];
    let client_config = ClientConfig::new(Arc::new(QuicClientConfig::try_from(client_crypto)?));
    let endpoint = Endpoint::client("127.0.0.1:0".parse()?)?;
    let connecting = endpoint.connect_with(client_config, backend.local_addr()?, "alopex.local")?;
    let connection = connecting.await?;
    let closed = tokio::time::timeout(std::time::Duration::from_secs(1), connection.closed()).await;
    assert!(
        closed.is_ok(),
        "a client without a certificate must be closed before the Chirps handshake"
    );

    backend.close().await?;
    endpoint.close(0u32.into(), b"test complete");
    Ok(())
}
