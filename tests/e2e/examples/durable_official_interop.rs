//! Real standard-protocol interoperability with the unmodified official baseline.
use alopex_chirps::{
    DURABLE_CHECKPOINT_JOURNAL_LIMIT_BYTES, DurableBuilder, DurableCheckpointConfig, DurableConfig,
    DurableCredential, DurableCredentialProvider, DurableCredentialProviderError,
    DurableDevelopmentResourceConfig, DurableRoutingConfig, DurableSendError, DurableTlsConfig,
    NodeId,
};
use alopex_chirps_backend_iggy::codec;
#[cfg(test)]
use alopex_chirps_backend_iggy::codec::EnvelopeFields;
use alopex_chirps_core::durable::{ConfirmationBoundary, DurableSendOutcome, PreparedDurableSend};
#[cfg(test)]
use alopex_chirps_core::durable::{DurableMessageRoute, PrepareFailure};
use anyhow::{Context, Result, ensure};
use chirps_e2e::v07::{ROOT_PASSWORD, ROOT_USERNAME, ServerProcess, provision_durable_fixture};
use iggy::prelude::*;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{fs, io::Read, path::Path, sync::Arc, time::Duration};
use tokio::time::{Instant, timeout};

const BASELINE: &str = "f5350d999d883fd3ca9dd33b3dc2754ddb0df049";
const BASELINE_TREE: &str = "0d6dcaf544588d3c0a54fe131a6de78b025eef14";
const BASELINE_LOCK: &str = "0e4ac6717cfb6ba04894f734b8f56afc56e265925fdd805242b6e39d4d676b41";

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn digest(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

fn file_identity(path: &Path) -> Result<(u64, String)> {
    let mut input = fs::File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut size = 0_u64;
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        size = size
            .checked_add(count as u64)
            .context("file size overflow")?;
        hash.update(&buffer[..count]);
    }
    Ok((size, hex(&hash.finalize())))
}

fn validate_manifest(value: &Value, size: u64, sha256: &str) -> Result<()> {
    ensure!(
        value["schema"] == "chirps-official-devbaseline-v1",
        "wrong official artifact schema"
    );
    ensure!(
        value["artifact_kind"] == "official-development-interoperability",
        "wrong artifact kind"
    );
    ensure!(
        value["source_commit"] == BASELINE && value["source_clean"] == true,
        "official source is not the clean baseline"
    );
    ensure!(
        value["source_tree"] == BASELINE_TREE && value["cargo_lock_sha256"] == BASELINE_LOCK,
        "official baseline tree or lock differs"
    );
    ensure!(
        value["publishable"] == false && value["profile"] == "dev",
        "official development artifact must not be publishable"
    );
    ensure!(
        value["binary_size"].as_u64() == Some(size) && value["binary_sha256"] == sha256,
        "official executable bytes differ"
    );
    for (field, length) in [
        ("source_tree", 40),
        ("cargo_lock_sha256", 64),
        ("build_log_sha256", 64),
    ] {
        let text = value[field]
            .as_str()
            .context("missing source/build identity")?;
        ensure!(
            text.len() == length
                && text
                    .bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)),
            "invalid source/build identity"
        );
    }
    ensure!(
        value["build_command"]
            .as_array()
            .is_some_and(|v| !v.is_empty())
            && value["rustc"].as_str().is_some_and(|v| !v.is_empty())
            && value["cargo"].as_str().is_some_and(|v| !v.is_empty()),
        "missing build provenance"
    );
    Ok(())
}

async fn sdk(server: &ServerProcess) -> Result<IggyClient> {
    let config = TcpClientConfig {
        server_address: server.address().to_string(),
        tls_enabled: true,
        tls_domain: "localhost".to_owned(),
        tls_ca_file: Some(server.certificate_path().display().to_string()),
        tls_validate_certificate: true,
        ..TcpClientConfig::default()
    };
    let tcp = TcpClient::create(Arc::new(config))?;
    Client::connect(&tcp).await?;
    let client = IggyClient::new(ClientWrapper::Tcp(tcp));
    client.login_user(ROOT_USERNAME, ROOT_PASSWORD).await?;
    Ok(client)
}

struct OfficialCredentials;

#[async_trait::async_trait]
impl DurableCredentialProvider for OfficialCredentials {
    async fn resolve(
        &self,
        reference: &str,
    ) -> Result<DurableCredential, DurableCredentialProviderError> {
        if reference != "official-fixture-root" {
            return Err(DurableCredentialProviderError::Rejected);
        }
        Ok(DurableCredential::username_password(
            ROOT_USERNAME.to_owned(),
            ROOT_PASSWORD.to_owned(),
        ))
    }
}

#[cfg(test)]

fn prepare(
    source: NodeId,
    target: NodeId,
    partition: u32,
    payload: &[u8],
) -> Result<PreparedDurableSend> {
    let key = b"official-interoperability";
    PreparedDurableSend::prepare(
        DurableMessageRoute::new(source, target, 1, partition, key.to_vec(), 1),
        |id| {
            codec::encode(
                id,
                EnvelopeFields::new(source, target, 1, partition, key, payload),
            )
            .map_err(|_| PrepareFailure::CanonicalEncoding)
        },
    )
    .map_err(|error| anyhow::anyhow!("prepare failed: {error:?}"))
}

fn verify_readback(
    prepared: &PreparedDurableSend,
    payload: &[u8],
    broker_id: u128,
    offset: u64,
    expected_offset: u64,
    bytes: &[u8],
) -> Result<Value> {
    ensure!(offset == expected_offset, "broker offset differs");
    ensure!(
        broker_id.to_be_bytes() == *prepared.message_id().as_bytes(),
        "broker message ID differs"
    );
    ensure!(
        bytes == prepared.canonical_bytes(),
        "broker canonical bytes differ"
    );
    let decoded = codec::decode(bytes)?;
    ensure!(
        decoded.message_id_bytes() == prepared.message_id().as_bytes(),
        "envelope message ID differs"
    );
    ensure!(
        decoded.source() == prepared.source() && decoded.target() == prepared.target(),
        "envelope node identity differs"
    );
    ensure!(
        decoded.generation() == prepared.generation()
            && decoded.partition() == prepared.partition(),
        "envelope route differs"
    );
    ensure!(
        decoded.ordering_key() == prepared.ordering_key() && decoded.payload() == payload,
        "envelope content differs"
    );
    Ok(
        json!({"offset": offset, "message_id": hex(&broker_id.to_be_bytes()), "canonical_hex": hex(bytes), "payload_hex": hex(decoded.payload()), "source": hex(decoded.source().as_bytes()), "target": hex(decoded.target().as_bytes()), "generation": decoded.generation(), "partition": decoded.partition(), "ordering_key_hex": hex(decoded.ordering_key())}),
    )
}

async fn exercise(server: &mut ServerProcess) -> Result<Value> {
    server.start(false, None).await?;
    let bootstrap = sdk(server).await?;
    let resource =
        provision_durable_fixture(&bootstrap, "official-interop", "standard-send").await?;
    bootstrap.shutdown().await?;
    let config = fs::read(server.configuration_path())?;
    let source = NodeId::new();
    let target = NodeId::new();
    let checkpoints = tempfile::tempdir()?;
    let facade_config = DurableConfig::broker_accepted(
        server.address(),
        DurableTlsConfig::new("localhost".to_owned(), vec![server.certificate_der()]),
        "official-fixture-root".to_owned(),
        config.clone(),
        DurableRoutingConfig::new(1, 1),
        DurableDevelopmentResourceConfig::new(
            resource.stream_id,
            resource.topic_id,
            vec![resource.partition_id],
        ),
        DurableCheckpointConfig::new(
            checkpoints.path().to_path_buf(),
            1,
            DURABLE_CHECKPOINT_JOURNAL_LIMIT_BYTES,
        ),
        1024 * 1024,
    );
    let mut handle = DurableBuilder::new(source)
        .connect(
            facade_config,
            &OfficialCredentials,
            Instant::now() + Duration::from_secs(10),
        )
        .await?;
    let payloads = [
        b"official\0broker-accepted-one".to_vec(),
        (0..=255).collect::<Vec<u8>>(),
    ];
    let ledger = payloads
        .iter()
        .map(|payload| {
            handle
                .prepare(target, b"official-interoperability".to_vec(), payload)
                .map_err(Into::into)
        })
        .collect::<Result<Vec<_>>>()?;
    let strong_error = timeout(
        Duration::from_secs(10),
        handle.send(&ledger[0], ConfirmationBoundary::OsSyncedAccepted),
    )
    .await?
    .err()
    .context("public facade accepted the strong boundary")?;
    ensure!(
        strong_error == DurableSendError::Unavailable,
        "strong boundary returned an unexpected public category"
    );
    for prepared in &ledger {
        let result = timeout(
            Duration::from_secs(10),
            handle.send(prepared, ConfirmationBoundary::BrokerAccepted),
        )
        .await??;
        ensure!(
            result.outcome() == DurableSendOutcome::BrokerAccepted && result.receipt().is_none(),
            "standard ACK was upgraded or did not confirm broker acceptance"
        );
    }
    let reader = sdk(server).await?;
    let poll = reader
        .poll_messages(
            &Identifier::numeric(resource.stream_id)?,
            &Identifier::numeric(resource.topic_id)?,
            Some(resource.partition_id),
            &Consumer::new(Identifier::numeric(1)?),
            &PollingStrategy::offset(0),
            3,
            false,
        )
        .await?;
    ensure!(
        poll.partition_id == resource.partition_id && poll.count == 2 && poll.messages.len() == 2,
        "official readback count/partition differs"
    );
    let observed = poll
        .messages
        .iter()
        .zip(ledger.iter().zip(&payloads))
        .enumerate()
        .map(|(index, (message, (prepared, payload)))| {
            verify_readback(
                prepared,
                payload,
                message.header.id,
                message.header.offset,
                index as u64,
                &message.payload,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    reader.shutdown().await?;
    let shutdown = handle
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await?;
    ensure!(
        shutdown.transport_closed() && shutdown.workers_joined(),
        "public facade shutdown did not join cleanly"
    );
    Ok(
        json!({"boundary": "BrokerAccepted", "strong_preflight": "Unavailable", "strong_receipt": false, "startup_config_hex": hex(&config), "startup_config_sha256": digest(&config), "stream_id":resource.stream_id, "topic_id":resource.topic_id, "partition_id":resource.partition_id, "expected":ledger.iter().zip(&payloads).map(|(prepared,payload)| json!({"message_id":hex(prepared.message_id().as_bytes()), "canonical_hex":hex(prepared.canonical_bytes()), "payload_hex":hex(payload), "source":hex(source.as_bytes()), "target":hex(target.as_bytes()), "generation":1, "partition":resource.partition_id, "ordering_key_hex":hex(prepared.ordering_key())})).collect::<Vec<_>>(), "observed":observed}),
    )
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    ensure!(
        args.len() == 3,
        "usage: durable_official_interop MANIFEST_JSON CHIRPS_SOURCE_COMMIT NEW_OUTPUT_DIR"
    );
    ensure!(
        args[1].len() == 40
            && args[1]
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)),
        "invalid candidate commit"
    );
    let manifest_bytes = fs::read(&args[0])?;
    let manifest: Value = serde_json::from_slice(&manifest_bytes)?;
    let binary = Path::new(
        manifest["binary_path"]
            .as_str()
            .context("missing executable path")?,
    );
    ensure!(binary.is_absolute(), "executable path must be absolute");
    let binary_identity = file_identity(binary)?;
    validate_manifest(&manifest, binary_identity.0, &binary_identity.1)?;
    let client_identity = file_identity(&std::env::current_exe()?)?;
    let output = Path::new(&args[2]);
    ensure!(output.is_absolute(), "output must be absolute");
    fs::create_dir(output)?;
    fs::write(output.join("server-manifest.json"), &manifest_bytes)?;
    let mut server = ServerProcess::new(binary.to_path_buf())?;
    let result = timeout(Duration::from_secs(60), exercise(&mut server)).await;
    let stopped = server.stop(Instant::now() + Duration::from_secs(5)).await?;
    ensure!(
        stopped.graceful() && !stopped.forced(),
        "official fixture required forced shutdown"
    );
    let observation = result??;
    ensure!(
        file_identity(binary)? == binary_identity && fs::read(&args[0])? == manifest_bytes,
        "artifact changed during observation"
    );
    fs::write(
        output.join("report.json"),
        serde_json::to_vec_pretty(
            &json!({"schema":"chirps.v0.7.official-interoperability/v2", "source_commit":args[1], "api_surface":"DurableConfig", "public_durable_config_validated":true, "official_manifest_sha256":digest(&manifest_bytes), "server_binary_sha256":binary_identity.1, "client_binary_sha256":client_identity.1, "server_source_commit":BASELINE, "observation":observation, "cleanup":{"graceful":true,"forced":false}, "result":"pass"}),
        )?,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn executable_hash_streams_all_bytes_across_buffer_boundaries() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("binary");
        assert!(file_identity(&path).is_err());
        for size in [0, 1, 65_535, 65_536, 65_537, 200_000] {
            let bytes = (0..size)
                .map(|index| (index % 251) as u8)
                .collect::<Vec<_>>();
            fs::write(&path, &bytes).unwrap();
            assert_eq!(file_identity(&path).unwrap(), (size as u64, digest(&bytes)));
        }
    }

    #[test]
    fn manifest_requires_exact_unmodified_nonpublishable_official_artifact() {
        let bytes = b"synthetic unit fixture; not an executable";
        let good = json!({"schema":"chirps-official-devbaseline-v1", "artifact_kind":"official-development-interoperability", "source_commit":BASELINE, "source_tree":BASELINE_TREE, "cargo_lock_sha256":BASELINE_LOCK, "source_clean":true, "publishable":false, "profile":"dev", "binary_size":bytes.len(), "binary_sha256":digest(bytes), "build_log_sha256":"a".repeat(64), "build_command":["cargo","build"], "rustc":"fixture", "cargo":"fixture"});
        assert!(validate_manifest(&good, bytes.len() as u64, &digest(bytes)).is_ok());
        for (field, invalid) in [
            ("source_commit", json!("a".repeat(40))),
            ("source_tree", json!("a".repeat(40))),
            ("cargo_lock_sha256", json!("a".repeat(64))),
            ("source_clean", json!(false)),
            ("publishable", json!(true)),
            ("profile", json!("release")),
            ("schema", json!("other")),
            ("artifact_kind", json!("production")),
            ("binary_size", json!(0)),
            ("binary_sha256", json!("b".repeat(64))),
            ("build_command", json!([])),
            ("rustc", json!("")),
            ("cargo", json!("")),
            ("build_log_sha256", json!("not a digest")),
        ] {
            let mut bad = good.clone();
            bad[field] = invalid;
            assert!(
                validate_manifest(&bad, bytes.len() as u64, &digest(bytes)).is_err(),
                "accepted {field}"
            );
        }
        assert!(validate_manifest(&good, 7, &digest(b"changed")).is_err());
    }

    #[test]
    fn readback_requires_independent_identity_offset_and_content() {
        let payload = b"independent payload";
        let prepared = prepare(NodeId::new(), NodeId::new(), 0, payload).unwrap();
        let id = u128::from_be_bytes(*prepared.message_id().as_bytes());
        let bytes = prepared.canonical_bytes();
        assert!(verify_readback(&prepared, payload, id, 0, 0, bytes).is_ok());
        assert!(verify_readback(&prepared, payload, id ^ 1, 0, 0, bytes).is_err());
        assert!(verify_readback(&prepared, payload, id, 1, 0, bytes).is_err());
        assert!(verify_readback(&prepared, b"different payload", id, 0, 0, bytes).is_err());
        for index in 0..bytes.len() {
            let mut corrupt = bytes.to_vec();
            corrupt[index] ^= 1;
            assert!(verify_readback(&prepared, payload, id, 0, 0, &corrupt).is_err());
        }
    }
}
