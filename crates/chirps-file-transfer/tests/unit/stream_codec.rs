use alopex_chirps_file_transfer::{ChunkStreamCodec, TransferSessionId, CHUNK_STREAM_MAGIC};
use quinn::{ClientConfig, Endpoint, ServerConfig};
use rcgen::generate_simple_self_signed;
use rustls::{Certificate, PrivateKey, RootCertStore};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::io::AsyncReadExt;

fn build_configs() -> (ServerConfig, ClientConfig) {
    let cert = generate_simple_self_signed(["localhost".to_string()]).expect("cert");
    let cert_der = cert.serialize_der().expect("cert der");
    let key_der = cert.serialize_private_key_der();
    let cert_chain = vec![Certificate(cert_der.clone())];
    let key = PrivateKey(key_der);

    let server_config = ServerConfig::with_single_cert(cert_chain, key).expect("server config");

    let mut roots = RootCertStore::empty();
    roots.add(&Certificate(cert_der)).expect("add cert");
    let crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let client_config = ClientConfig::new(Arc::new(crypto));

    (server_config, client_config)
}

#[tokio::test]
async fn chunk_stream_codec_round_trip() {
    let (server_config, client_config) = build_configs();
    let server_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
    let server_endpoint = Endpoint::server(server_config, server_addr).expect("server endpoint");
    let local_addr = server_endpoint.local_addr().expect("local addr");

    let mut client_endpoint = Endpoint::client(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .expect("client endpoint");
    client_endpoint.set_default_client_config(client_config);

    let client_connect = client_endpoint
        .connect(local_addr, "localhost")
        .expect("connect");
    let client_conn = client_connect.await.expect("client conn");

    let server_conn = server_endpoint
        .accept()
        .await
        .expect("accept")
        .await
        .expect("server conn");

    let (mut send, mut recv) = tokio::try_join!(
        async { client_conn.open_uni().await },
        async { server_conn.accept_uni().await },
    )
    .expect("open stream");

    let session_id = TransferSessionId::new();
    let chunk_index = 7u32;
    let payload = b"chunk data".to_vec();

    ChunkStreamCodec::encode(&mut send, &session_id, chunk_index, &payload)
        .await
        .expect("encode");
    send.finish().await.expect("finish");

    let mut magic = [0u8; 1];
    recv.read_exact(&mut magic).await.expect("read magic");
    assert_eq!(magic[0], CHUNK_STREAM_MAGIC);

    let (decoded_id, decoded_index, decoded_data) =
        ChunkStreamCodec::decode(&mut recv).await.expect("decode");

    assert_eq!(decoded_id, session_id);
    assert_eq!(decoded_index, chunk_index);
    assert_eq!(decoded_data, payload);

    client_endpoint.wait_idle().await;
    server_endpoint.wait_idle().await;
}
