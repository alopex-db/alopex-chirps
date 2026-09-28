use alopex_chirps::{DurableCredential, DurableCredentialProvider, DurableCredentialProviderError};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use chirps_e2e::v07::{
    FIXTURE_RETENTION_BYTES, FixtureIdentity, ProvisionedFixture, ROOT_PASSWORD, ROOT_USERNAME,
    RUNTIME_PASSWORD, RUNTIME_USERNAME, ServerProcess, ServerStopReport, VerifiedArtifact,
    provision_durable_fixture, provision_production_admin,
};
use iggy::prelude::*;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

#[allow(dead_code)] // shared integration support; not every test binary needs credentials
pub struct RuntimeCredentials;

#[async_trait]
impl DurableCredentialProvider for RuntimeCredentials {
    async fn resolve(
        &self,
        reference: &str,
    ) -> Result<DurableCredential, DurableCredentialProviderError> {
        if reference != RUNTIME_USERNAME {
            return Err(DurableCredentialProviderError::Rejected);
        }
        Ok(DurableCredential::username_password(
            RUNTIME_USERNAME.to_owned(),
            RUNTIME_PASSWORD.to_owned(),
        ))
    }
}

#[allow(dead_code)] // shared integration support; metadata recovery verifies the same lane directly
pub fn require_production_artifact(artifact: &VerifiedArtifact) -> Result<()> {
    ensure!(
        artifact.lane == "production" && artifact.kind == "production",
        "Task 6.5 accepts only the attested production artifact"
    );
    Ok(())
}

pub async fn bootstrap_fixture(
    server: &mut ServerProcess,
    artifact: &VerifiedArtifact,
    stream_name: &str,
    topic_name: &str,
    expiry: IggyExpiry,
) -> Result<FixtureIdentity> {
    server.start(false, None).await?;
    let root = sdk_client(server, ROOT_USERNAME, ROOT_PASSWORD).await?;
    let provisioned = if expiry == IggyExpiry::NeverExpire {
        provision_durable_fixture(&root, stream_name, topic_name).await?
    } else {
        let stream = root.create_stream(stream_name).await?;
        let topic = root
            .create_topic(
                &Identifier::numeric(stream.id)?,
                topic_name,
                1,
                CompressionAlgorithm::None,
                None,
                expiry,
                MaxTopicSize::from(FIXTURE_RETENTION_BYTES),
            )
            .await?;
        ensure!(
            topic.partitions_count == 1
                && topic.partitions.len() == 1
                && topic.max_topic_size.as_bytes_u64() == FIXTURE_RETENTION_BYTES,
            "official SDK returned a different retention fixture"
        );
        ProvisionedFixture {
            stream_id: stream.id,
            topic_id: topic.id,
            partition_id: topic.partitions[0].id,
            retention_bytes: topic.max_topic_size.as_bytes_u64(),
        }
    };
    provision_production_admin(&root).await?;
    let expected_runtime_permissions =
        runtime_permissions(provisioned.stream_id, provisioned.topic_id);
    let runtime_user = root
        .create_user(
            RUNTIME_USERNAME,
            RUNTIME_PASSWORD,
            UserStatus::Active,
            Some(expected_runtime_permissions.clone()),
        )
        .await?;
    drop(root);
    assert_clean_stop(
        server.stop(Instant::now() + Duration::from_secs(5)).await?,
        "bootstrap",
    )?;
    server.read_fixture_projection(
        artifact,
        &provisioned,
        &runtime_user,
        RUNTIME_USERNAME,
        UserStatus::Active,
        &expected_runtime_permissions,
    )
}

pub fn assert_clean_stop(report: ServerStopReport, phase: &str) -> Result<()> {
    ensure!(
        report.graceful() != report.forced(),
        "{phase} server stop report did not select exactly one termination path"
    );
    Ok(())
}

pub async fn sdk_client(
    server: &ServerProcess,
    username: &str,
    password: &str,
) -> Result<IggyClient> {
    let config = TcpClientConfig {
        server_address: server.address().to_string(),
        tls_enabled: true,
        tls_domain: "localhost".to_owned(),
        tls_ca_file: Some(server.certificate_path().display().to_string()),
        tls_validate_certificate: true,
        ..TcpClientConfig::default()
    };
    let client = TcpClient::create(Arc::new(config))?;
    Client::connect(&client).await?;
    let client = IggyClient::new(ClientWrapper::Tcp(client));
    client.login_user(username, password).await?;
    Ok(client)
}

fn runtime_permissions(stream_id: u32, topic_id: u32) -> Permissions {
    Permissions {
        global: GlobalPermissions::default(),
        streams: Some(BTreeMap::from([(
            stream_id as usize,
            StreamPermissions {
                topics: Some(BTreeMap::from([(
                    topic_id as usize,
                    TopicPermissions {
                        poll_messages: true,
                        send_messages: true,
                        ..TopicPermissions::default()
                    },
                )])),
                ..StreamPermissions::default()
            },
        )])),
    }
}
