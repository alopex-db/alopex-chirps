//! Owned, bounded server fixture for the durable performance collector.
//! Run only after the production artifact verifier has supplied runner variables.

use anyhow::{Context, Result, ensure};
use chirps_e2e::v07::{
    ROOT_PASSWORD, ROOT_USERNAME, RUNTIME_PASSWORD, RUNTIME_USERNAME, ServerProcess,
    VerifiedArtifact, provision_durable_fixture, provision_production_admin,
};
use iggy::prelude::*;
use iggy_common::UserInfoDetails;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fs, io::Write, path::Path, sync::Arc, time::Duration};
use tokio::time::{Instant, sleep, timeout};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn file_reference(path: &Path) -> Result<serde_json::Value> {
    Ok(json!({"path": path, "sha256": hex(&Sha256::digest(fs::read(path)?))}))
}

fn write_new(path: &Path, value: &serde_json::Value) -> Result<()> {
    let pending = path.with_extension("pending");
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&pending)?;
    file.write_all(&serde_json::to_vec_pretty(value)?)?;
    file.sync_all()?;
    drop(file);
    fs::hard_link(&pending, path)?;
    fs::remove_file(pending)?;
    Ok(())
}

async fn provision(
    server: &ServerProcess,
) -> Result<(
    chirps_e2e::v07::ProvisionedFixture,
    UserInfoDetails,
    Permissions,
)> {
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
    let resource = provision_durable_fixture(&client, "chirps-perf-v07", "durable").await?;
    provision_production_admin(&client).await?;
    let permissions = Permissions {
        global: GlobalPermissions::default(),
        streams: Some(BTreeMap::from([(
            resource.stream_id as usize,
            StreamPermissions {
                topics: Some(BTreeMap::from([(
                    resource.topic_id as usize,
                    TopicPermissions {
                        poll_messages: true,
                        send_messages: true,
                        ..TopicPermissions::default()
                    },
                )])),
                ..StreamPermissions::default()
            },
        )])),
    };
    let user = client
        .create_user(
            RUNTIME_USERNAME,
            RUNTIME_PASSWORD,
            UserStatus::Active,
            Some(permissions.clone()),
        )
        .await?;
    client.shutdown().await?;
    Ok((resource, user, permissions))
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    ensure!(
        args.len() == 2,
        "usage: durable_perf_fixture NEW_OUTPUT_DIRECTORY LIFETIME_SECONDS"
    );
    let output = Path::new(&args[0]);
    let lifetime: u64 = args[1].parse().context("invalid lifetime")?;
    ensure!(
        (1..=7200).contains(&lifetime),
        "lifetime must be 1..7200 seconds"
    );
    ensure!(output.is_absolute(), "output must be absolute");
    let parent = output.parent().context("output has no parent")?;
    ensure!(
        parent.canonicalize()? == parent,
        "output parent must be canonical"
    );
    let artifact = VerifiedArtifact::from_runner_environment()?;
    ensure!(
        artifact.lane == "production" && artifact.kind == "production",
        "performance requires the production artifact"
    );
    fs::create_dir(output)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(output, fs::Permissions::from_mode(0o700))?;
    }
    let mut server = ServerProcess::new(artifact.binary.clone())?;
    server.start(false, None).await?;
    let (resource, user, permissions) =
        timeout(Duration::from_secs(30), provision(&server)).await??;
    let stopped = server.stop(Instant::now() + Duration::from_secs(5)).await?;
    ensure!(
        stopped.graceful() && !stopped.forced(),
        "bootstrap required forced shutdown"
    );
    let fixture = server.read_fixture_projection(
        &artifact,
        &resource,
        &user,
        RUNTIME_USERNAME,
        UserStatus::Active,
        &permissions,
    )?;
    fs::write(output.join("root.der"), server.certificate_der())?;
    let checkpoints = output.join("checkpoints");
    fs::create_dir(&checkpoints)?;
    server.start(true, None).await?;
    let pid = server.running_process_id()?;
    write_new(
        &output.join("fixture.json"),
        &json!({
            "schema": "chirps.durable-perf-fixture/v1", "server_pid": pid,
            "server_binary_sha256": artifact.sha256, "server_source_commit": artifact.source_commit,
            "server_source_tree": artifact.source_tree, "server_data_root": server.state_path(),
            "server_config": file_reference(server.configuration_path())?,
            "endpoint": server.address().to_string(), "tls_server_name": "localhost",
            "tls_ca_pem": file_reference(server.certificate_path())?,
            "tls_root_der": file_reference(&output.join("root.der"))?,
            "checkpoint_root": checkpoints, "credential_reference": "CHIRPS_PERF_CREDENTIAL",
            "projection": {
                "stream_id": fixture.stream_id, "topic_id": fixture.topic_id, "partition_id": fixture.partition_id,
                "resource_id_hex": hex(&fixture.resource_id), "resource_epoch": fixture.resource_epoch,
                "build_sha_hex": hex(&fixture.build_sha), "retention_bytes": fixture.retention_bytes,
                "retention_messages": fixture.retention_messages, "checksum_enabled": fixture.checksum_enabled,
                "configuration_digest_hex": hex(&fixture.configuration_digest),
                "security_digest_hex": hex(&fixture.security_digest), "capability_digest_hex": hex(&fixture.capability_digest)
            }
        }),
    )?;
    // Presence of ready, published after the complete descriptor, is the handoff.
    write_new(
        &output.join("ready.json"),
        &json!({"server_pid": pid, "lifetime_seconds": lifetime}),
    )?;
    let deadline = Instant::now() + Duration::from_secs(lifetime);
    while Instant::now() < deadline && !output.join("stop").exists() {
        server.running_process_id()?;
        sleep(Duration::from_millis(100)).await;
    }
    let stopped = server.stop(Instant::now() + Duration::from_secs(5)).await?;
    write_new(
        &output.join("stopped.json"),
        &json!({"graceful": stopped.graceful(), "forced": stopped.forced()}),
    )?;
    ensure!(
        stopped.graceful() && !stopped.forced(),
        "fixture required forced shutdown"
    );
    Ok(())
}
