use alopex_chirps::{
    DURABLE_CHECKPOINT_JOURNAL_LIMIT_BYTES, DurableBuildError, DurableBuilder,
    DurableCheckpointConfig, DurableConfig, DurableCredential, DurableCredentialProvider,
    DurableCredentialProviderError, DurableExtensionConfig, DurableHandle, DurableLeaseConfig,
    DurablePartitionProjection, DurableProfile, DurableResourceConfig, DurableRoutingConfig,
    DurableTlsConfig, NodeId,
};
use alopex_chirps_core::durable::{
    AttemptPhase, ConfirmationBoundary, DurableMessageId, DurableSendOutcome, EnvelopeDigest,
    PollResolution,
};
use anyhow::{Context, Result, anyhow, bail, ensure};
use async_trait::async_trait;
use chirps_e2e::v07::{
    ADMIN_PASSWORD, ADMIN_USERNAME, EvidenceSink, FIXTURE_RETENTION_BYTES, FixtureIdentity,
    OracleAppendObserver, ROOT_PASSWORD, ROOT_USERNAME, RUNTIME_PASSWORD, RUNTIME_USERNAME,
    RealSession, ServerProcess, ServerStopReport, VerifiedArtifact, connect_fixture_with_observer,
    provision_durable_fixture, provision_production_admin, read_append_observations,
    read_oracle_append_evidence,
};
use chirps_fault_oracle::OracleStore;
use iggy::prelude::*;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::{Instant, sleep, timeout};

const STREAM_NAME: &str = "chirps-v07";
const TOPIC_NAME: &str = "durable";
const LEASE_MILLIS: u32 = 800;
const DENIED_USERNAME: &str = "denied-v07";
const DENIED_PASSWORD: &str = "denied-secret-v07";

struct StaticCredentials {
    reference: &'static str,
    username: &'static str,
    password: &'static str,
}

#[async_trait]
impl DurableCredentialProvider for StaticCredentials {
    async fn resolve(
        &self,
        reference: &str,
    ) -> Result<DurableCredential, DurableCredentialProviderError> {
        if reference != self.reference {
            return Err(DurableCredentialProviderError::Rejected);
        }
        Ok(DurableCredential::username_password(
            self.username.to_owned(),
            self.password.to_owned(),
        ))
    }
}

const RUNTIME_CREDENTIALS: StaticCredentials = StaticCredentials {
    reference: "runtime-v07",
    username: RUNTIME_USERNAME,
    password: RUNTIME_PASSWORD,
};

#[derive(Debug, Clone, Copy)]
enum PreSendCase {
    WrongCa,
    WrongHostname,
    WrongCredential,
    PermissionDenied,
    CapabilityMismatch,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run through scripts/run-v07-lane.sh with an attested compatible server"]
async fn authenticated_binding_renewal_restart_and_fencing() -> Result<()> {
    let artifact = VerifiedArtifact::from_runner_environment()?;
    let mut evidence = EvidenceSink::new(artifact.clone())?;
    let mut server = ServerProcess::new(artifact.binary.clone())?;

    // An empty non-production state is provisioned exclusively through the official SDK.
    server.start(false, None).await?;
    let root = sdk_client(&server, ROOT_USERNAME, ROOT_PASSWORD).await?;
    let provisioned = provision_durable_fixture(&root, STREAM_NAME, TOPIC_NAME).await?;
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
    root.create_user(
        DENIED_USERNAME,
        DENIED_PASSWORD,
        UserStatus::Active,
        Some(Permissions::default()),
    )
    .await?;
    drop(root);
    let bootstrap_stop = server.stop(Instant::now() + Duration::from_secs(5)).await?;
    assert_stop_report(bootstrap_stop, "bootstrap")?;
    let fixture = server.read_fixture_projection(
        &artifact,
        &provisioned,
        &runtime_user,
        RUNTIME_USERNAME,
        UserStatus::Active,
        &expected_runtime_permissions,
    )?;
    evidence.record("independent-fixture-projection", "accepted")?;

    // The same persisted state must boot under the production profile and all six credentials.
    server.start(true, None).await?;
    assert_runtime_resource_mutations_denied(&server, fixture.stream_id, fixture.topic_id).await?;
    evidence.record("runtime-resource-mutations", "permission-denied")?;
    let mut first_session = RealSession::bind(
        server.address(),
        server.certificate_der(),
        RUNTIME_USERNAME,
        RUNTIME_PASSWORD,
        fixture.stream_id,
        fixture.topic_id,
        fixture.partition_id,
        LEASE_MILLIS,
    )
    .await?;
    let first_report = first_session.report();
    fixture.assert_report(first_report)?;
    evidence.record_binding(first_report)?;
    exercise_pre_send_rejections(&server, fixture, &mut evidence).await?;
    let checkpoints = tempfile::tempdir()?;
    let mut handle = connect_public(&server, fixture, &checkpoints).await?;
    evidence.record("authenticated-bind", "accepted")?;

    let admin = sdk_client(&server, ADMIN_USERNAME, ADMIN_PASSWORD).await?;
    assert_active_resource_mutations_rejected(&admin, fixture.stream_id, fixture.topic_id).await?;
    evidence.record("active-resource-mutations", "rejected")?;
    drop(admin);

    // The public handle must remain usable beyond the original lease through same-session renewal.
    sleep(Duration::from_millis(u64::from(LEASE_MILLIS) + 250)).await;
    send_accepted(&mut handle, b"renewed-session").await?;
    evidence.record("same-session-renewal", "accepted")?;

    // A stopped process lets the server-monotonic lease expire. Resuming cannot revive stale success.
    server.signal("-STOP")?;
    sleep(Duration::from_millis(u64::from(LEASE_MILLIS) + 250)).await;
    server.signal("-CONT")?;
    sleep(Duration::from_millis(150)).await;
    assert_no_success(&mut handle, b"expired-session").await?;
    evidence.record("expired-session", "fenced")?;

    // Restart changes the boot/session identity and the old handle must not return success.
    let old_boot_id = *first_report.boot_id();
    let stop = server.stop(Instant::now() + Duration::from_secs(5)).await?;
    assert_stop_report(stop, "production-restart")?;
    server.start(true, None).await?;
    assert_no_success(&mut handle, b"stale-after-restart").await?;
    assert_raw_session_fenced(&first_session, "first post-restart probe").await?;
    let mut restarted_session = RealSession::bind(
        server.address(),
        server.certificate_der(),
        RUNTIME_USERNAME,
        RUNTIME_PASSWORD,
        fixture.stream_id,
        fixture.topic_id,
        fixture.partition_id,
        LEASE_MILLIS,
    )
    .await?;
    let restarted_report = restarted_session.report();
    ensure!(
        old_boot_id != *restarted_report.boot_id(),
        "restart reused the boot identity"
    );
    fixture.assert_report(restarted_report)?;
    assert_raw_session_fenced(&first_session, "second post-restart probe").await?;
    evidence.record_binding(restarted_report)?;
    let restarted_checkpoints = tempfile::tempdir()?;
    let mut restarted = connect_public(&server, fixture, &restarted_checkpoints).await?;
    send_accepted(&mut restarted, b"fresh-after-restart").await?;
    assert_no_success(&mut handle, b"stale-after-fresh-session").await?;
    evidence.record("restart-fencing", "accepted")?;
    shutdown_public(&mut handle, "pre-restart-handle").await?;
    first_session
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await?;

    // The fault artifact's response failpoint must yield ambiguity, never a false success.
    let needs_oracle = artifact.lane == "fault";
    if needs_oracle {
        shutdown_public(&mut restarted, "pre-fault-handle").await?;
        restarted_session
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await?;
        let stop = server.stop(Instant::now() + Duration::from_secs(5)).await?;
        assert_stop_report(stop, "pre-fault")?;
        let response_observations = tempfile::tempdir()?;
        let response_observation_log = response_observations.path().join("append.jsonl");
        server
            .start_with_observation(true, Some("response"), Some(&response_observation_log))
            .await?;
        let mut response_session = RealSession::bind(
            server.address(),
            server.certificate_der(),
            RUNTIME_USERNAME,
            RUNTIME_PASSWORD,
            fixture.stream_id,
            fixture.topic_id,
            fixture.partition_id,
            LEASE_MILLIS,
        )
        .await?;
        fixture.assert_report(response_session.report())?;
        let response_checkpoints = tempfile::tempdir()?;
        let response_oracle_dir = tempfile::tempdir()?;
        let response_oracle_store = OracleStore::new(response_oracle_dir.path().join("oracle.log"));
        let response_observer = Arc::new(OracleAppendObserver::new(response_oracle_store.clone()));
        let mut response_handle = connect_fixture_with_observer(
            &server,
            fixture,
            response_checkpoints.path(),
            NodeId::new(),
            RUNTIME_CREDENTIALS.reference,
            &RUNTIME_CREDENTIALS,
            response_observer.clone(),
        )
        .await?;
        let prepared =
            response_handle.prepare(NodeId::new(), b"response-loss".to_vec(), b"body")?;
        let message_id = prepared.message_id();
        let envelope_digest = prepared.envelope_digest();
        let canonical_bytes = prepared.canonical_bytes().to_vec();
        let result = response_handle
            .send(&prepared, ConfirmationBoundary::OsSyncedAccepted)
            .await?;
        ensure!(
            matches!(result.outcome(), DurableSendOutcome::Indeterminate(_)),
            "response loss returned a false terminal success"
        );
        ensure!(
            result.receipt().is_none(),
            "ambiguous response carried a receipt"
        );
        ensure!(!response_observer.failed(), "response-loss oracle failed");
        let oracle = read_oracle_append_evidence(&response_oracle_store)?;
        ensure!(
            oracle.len() == 1 && oracle[0].matches_prepared(&prepared),
            "response-loss oracle did not bind the exact prepared envelope"
        );
        let result_binding = result.attempt_binding();
        ensure!(
            result_binding.phase() == AttemptPhase::AppendInvoked
                && result_binding
                    .attempt_id()
                    .is_some_and(|value| hex(value.as_bytes()) == oracle[0].attempt_id)
                && result_binding
                    .session_fingerprint()
                    .is_some_and(|value| hex(value.as_bytes()) == oracle[0].session_fingerprint)
                && result_binding.requested_boundary() == Some(oracle[0].requested_boundary),
            "response-loss result substituted its exact attempt binding"
        );
        evidence.record("response-loss", "indeterminate")?;
        shutdown_public(&mut response_handle, "response-loss-handle").await?;
        response_session
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await?;
        let stop = server
            .force_stop(Instant::now() + Duration::from_secs(2))
            .await?;
        assert_stop_report(stop, "response-loss")?;
        let server_observations = read_append_observations(&response_observation_log)?;
        let expected_stages = [
            chirps_e2e::v07::AppendObservationStage::WireWrite,
            chirps_e2e::v07::AppendObservationStage::JournalFlush,
            chirps_e2e::v07::AppendObservationStage::MessageSync,
            chirps_e2e::v07::AppendObservationStage::IndexSync,
        ];
        ensure!(
            server_observations.len() == 4
                && server_observations
                    .iter()
                    .zip(expected_stages)
                    .all(|(value, stage)| {
                        value.stage == stage
                            && value.attempt_id == oracle[0].attempt_id
                            && value.message_id == oracle[0].message_id
                            && value.envelope_digest == oracle[0].envelope_digest
                            && value.resource_id == hex(&fixture.resource_id)
                            && value.resource_epoch == fixture.resource_epoch
                            && value.partition_id == fixture.partition_id
                            && (stage == chirps_e2e::v07::AppendObservationStage::WireWrite
                                || (value.assigned_offset.is_some()
                                    && value.assigned_index.is_some()))
                    }),
            "response-loss server stages did not correlate to the oracle attempt"
        );
        let response_location = (
            server_observations[1]
                .assigned_offset
                .context("response-loss journal stage omitted offset")?,
            server_observations[1]
                .assigned_index
                .context("response-loss journal stage omitted index")?,
        );
        ensure!(
            server_observations[1..].iter().all(|value| {
                (value.assigned_offset, value.assigned_index)
                    == (Some(response_location.0), Some(response_location.1))
            }),
            "response-loss physical stages disagreed on exact location"
        );
        evidence.record("task-6.2-response-loss-observation", "accepted")?;
        server.start(true, None).await?;
        let mut readback_session = RealSession::bind(
            server.address(),
            server.certificate_der(),
            RUNTIME_USERNAME,
            RUNTIME_PASSWORD,
            fixture.stream_id,
            fixture.topic_id,
            fixture.partition_id,
            LEASE_MILLIS,
        )
        .await?;
        fixture.assert_report(readback_session.report())?;
        let stored_offsets = stored_envelope_offsets(
            &readback_session,
            message_id,
            envelope_digest,
            &canonical_bytes,
        )
        .await?;
        ensure!(
            stored_offsets == vec![response_location.0],
            "response-loss fresh-process readback disagreed with the exact sidecar location"
        );
        evidence.record("response-loss-fresh-process-readback", "exactly-once")?;

        // Emergency permission revocation is exercised only after the stable
        // projection was used for response-loss readback; revocation advances
        // the security epoch and intentionally invalidates that projection.
        let revoke_checkpoints = tempfile::tempdir()?;
        let mut revoke_handle = connect_public(&server, fixture, &revoke_checkpoints).await?;
        let admin = sdk_client(&server, ADMIN_USERNAME, ADMIN_PASSWORD).await?;
        admin
            .update_permissions(&Identifier::named(RUNTIME_USERNAME)?, None)
            .await?;
        sleep(Duration::from_millis(250)).await;
        assert_no_success(&mut revoke_handle, b"revoked-runtime").await?;
        evidence.record("emergency-revoke", "fenced")?;
        shutdown_public(&mut revoke_handle, "revoked-handle").await?;
        drop(admin);
        readback_session
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await?;
    } else {
        // Emergency permission revocation mutates the security projection and
        // must fence an already active public session.
        let admin = sdk_client(&server, ADMIN_USERNAME, ADMIN_PASSWORD).await?;
        admin
            .update_permissions(&Identifier::named(RUNTIME_USERNAME)?, None)
            .await?;
        sleep(Duration::from_millis(250)).await;
        assert_no_success(&mut restarted, b"revoked-runtime").await?;
        evidence.record("emergency-revoke", "fenced")?;
        shutdown_public(&mut restarted, "revoked-handle").await?;
        restarted_session
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await?;
        drop(admin);
    }

    let stop = server.stop(Instant::now() + Duration::from_secs(5)).await?;
    assert_stop_report(stop, "final")?;
    Ok(())
}

fn assert_stop_report(report: ServerStopReport, phase: &str) -> Result<()> {
    ensure!(
        report.graceful() != report.forced(),
        "{phase} server stop report did not select exactly one termination path"
    );
    Ok(())
}

async fn sdk_client(server: &ServerProcess, username: &str, password: &str) -> Result<IggyClient> {
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

async fn assert_runtime_resource_mutations_denied(
    server: &ServerProcess,
    stream_id: u32,
    topic_id: u32,
) -> Result<()> {
    let runtime = sdk_client(server, RUNTIME_USERNAME, RUNTIME_PASSWORD).await?;
    let stream = Identifier::numeric(stream_id)?;
    let topic = Identifier::numeric(topic_id)?;
    ensure!(
        runtime
            .create_stream("runtime-forbidden-v07")
            .await
            .is_err(),
        "runtime principal created a stream"
    );
    ensure!(
        runtime
            .update_topic(
                &stream,
                &topic,
                TOPIC_NAME,
                CompressionAlgorithm::None,
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::from(3_u64 << 30),
            )
            .await
            .is_err(),
        "runtime principal updated the durable topic"
    );
    ensure!(
        runtime.purge_topic(&stream, &topic).await.is_err(),
        "runtime principal purged the durable topic"
    );
    ensure!(
        runtime.delete_topic(&stream, &topic).await.is_err(),
        "runtime principal deleted the durable topic"
    );
    Ok(())
}

async fn connect_public(
    server: &ServerProcess,
    fixture: FixtureIdentity,
    checkpoints: &TempDir,
) -> Result<DurableHandle> {
    connect_with(
        server,
        fixture,
        DurableTlsConfig::new("localhost".to_owned(), vec![server.certificate_der()]),
        "runtime-v07",
        &RUNTIME_CREDENTIALS,
        checkpoints,
    )
    .await
    .context("public DurableBuilder connection")
}

async fn connect_with<P: DurableCredentialProvider + ?Sized>(
    server: &ServerProcess,
    fixture: FixtureIdentity,
    tls: DurableTlsConfig,
    credential_reference: &str,
    credentials: &P,
    checkpoints: &TempDir,
) -> Result<DurableHandle, DurableBuildError> {
    let projection = DurablePartitionProjection::new(
        fixture.partition_id,
        fixture.resource_id,
        fixture.resource_epoch,
        fixture.build_sha,
        fixture.retention_bytes,
        fixture.retention_messages,
        fixture.checksum_enabled,
        fixture.configuration_digest,
        fixture.security_digest,
        fixture.capability_digest,
    );
    let config = DurableConfig::new(
        server.address(),
        tls,
        credential_reference.to_owned(),
        DurableProfile::OsSyncedAccepted,
        DurableRoutingConfig::new(1, 1),
        DurableResourceConfig::new(fixture.stream_id, fixture.topic_id, vec![projection]),
        DurableCheckpointConfig::new(
            checkpoints.path().to_path_buf(),
            1,
            DURABLE_CHECKPOINT_JOURNAL_LIMIT_BYTES,
        ),
        DurableLeaseConfig::new(
            LEASE_MILLIS,
            Duration::from_millis(u64::from(LEASE_MILLIS) / 4),
        ),
        DurableExtensionConfig::required(1024 * 1024),
    );
    DurableBuilder::new(NodeId::new())
        .inbox_generation(1)
        .explicit_partitions(1)
        .connect(
            config,
            credentials,
            Instant::now() + Duration::from_secs(10),
        )
        .await
}

async fn exercise_pre_send_rejections(
    server: &ServerProcess,
    fixture: FixtureIdentity,
    evidence: &mut EvidenceSink,
) -> Result<()> {
    let wrong_certificate = rcgen::generate_simple_self_signed(["localhost".to_owned()])?;
    let wrong_credentials = StaticCredentials {
        reference: "wrong-v07",
        username: RUNTIME_USERNAME,
        password: "wrong-password-v07",
    };
    let denied_credentials = StaticCredentials {
        reference: "denied-v07",
        username: DENIED_USERNAME,
        password: DENIED_PASSWORD,
    };
    let mismatched = fixture.with_mismatched_capability();

    for case in [
        PreSendCase::WrongCa,
        PreSendCase::WrongHostname,
        PreSendCase::WrongCredential,
        PreSendCase::PermissionDenied,
        PreSendCase::CapabilityMismatch,
    ] {
        let checkpoints = tempfile::tempdir()?;
        let result = match case {
            PreSendCase::WrongCa => {
                connect_with(
                    server,
                    fixture,
                    DurableTlsConfig::new(
                        "localhost".to_owned(),
                        vec![wrong_certificate.serialize_der()?],
                    ),
                    "runtime-v07",
                    &RUNTIME_CREDENTIALS,
                    &checkpoints,
                )
                .await
            }
            PreSendCase::WrongHostname => {
                connect_with(
                    server,
                    fixture,
                    DurableTlsConfig::new(
                        "wrong.invalid".to_owned(),
                        vec![server.certificate_der()],
                    ),
                    "runtime-v07",
                    &RUNTIME_CREDENTIALS,
                    &checkpoints,
                )
                .await
            }
            PreSendCase::WrongCredential => {
                connect_with(
                    server,
                    fixture,
                    DurableTlsConfig::new("localhost".to_owned(), vec![server.certificate_der()]),
                    "wrong-v07",
                    &wrong_credentials,
                    &checkpoints,
                )
                .await
            }
            PreSendCase::PermissionDenied => {
                connect_with(
                    server,
                    fixture,
                    DurableTlsConfig::new("localhost".to_owned(), vec![server.certificate_der()]),
                    "denied-v07",
                    &denied_credentials,
                    &checkpoints,
                )
                .await
            }
            PreSendCase::CapabilityMismatch => {
                connect_with(
                    server,
                    mismatched,
                    DurableTlsConfig::new("localhost".to_owned(), vec![server.certificate_der()]),
                    "runtime-v07",
                    &RUNTIME_CREDENTIALS,
                    &checkpoints,
                )
                .await
            }
        };
        if !matches!(
            (&case, &result),
            (_, Err(DurableBuildError::BackendUnavailable))
                | (
                    PreSendCase::WrongCredential,
                    Err(DurableBuildError::CredentialRejected)
                )
        ) {
            match result {
                Err(error) => {
                    bail!("{case:?} returned the wrong typed pre-send rejection: {error:?}")
                }
                Ok(mut unexpected) => {
                    shutdown_public(&mut unexpected, "unexpected-negative-handle").await?;
                    bail!("{case:?} created a handle before the first send")
                }
            }
        }
        evidence.record(
            match case {
                PreSendCase::WrongCa => "pre-send-wrong-ca",
                PreSendCase::WrongHostname => "pre-send-wrong-hostname",
                PreSendCase::WrongCredential => "pre-send-wrong-credential",
                PreSendCase::PermissionDenied => "pre-send-permission-denied",
                PreSendCase::CapabilityMismatch => "pre-send-capability-mismatch",
            },
            "backend-unavailable-before-handle",
        )?;
    }
    Ok(())
}

async fn assert_active_resource_mutations_rejected(
    admin: &IggyClient,
    stream_id: u32,
    topic_id: u32,
) -> Result<()> {
    let stream = Identifier::numeric(stream_id)?;
    let topic = Identifier::numeric(topic_id)?;
    ensure!(
        admin
            .update_topic(
                &stream,
                &topic,
                TOPIC_NAME,
                CompressionAlgorithm::None,
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::from(3_u64 << 30),
            )
            .await
            .is_err(),
        "active session allowed topic configuration update"
    );
    ensure!(
        admin.purge_topic(&stream, &topic).await.is_err(),
        "active session allowed purge/reset"
    );
    ensure!(
        admin.delete_topic(&stream, &topic).await.is_err(),
        "active session allowed topic deletion"
    );
    ensure!(
        admin
            .create_topic(
                &stream,
                TOPIC_NAME,
                1,
                CompressionAlgorithm::None,
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::from(FIXTURE_RETENTION_BYTES),
            )
            .await
            .is_err(),
        "delete/recreate sequence replaced an active resource"
    );
    ensure!(
        admin.delete_stream(&stream).await.is_err(),
        "active session allowed stream deletion"
    );
    Ok(())
}

async fn shutdown_public(handle: &mut DurableHandle, label: &str) -> Result<()> {
    let report = handle
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .with_context(|| format!("shutdown DurableHandle {label}"))?;
    ensure!(
        report.transport_closed() && report.workers_joined(),
        "DurableHandle {label} shutdown omitted terminal ownership evidence"
    );
    let _phase_preserving_send_reports = report.send_operations();
    let _phase_preserving_checkpoint_reports = report.checkpoint_operations();
    Ok(())
}

async fn assert_raw_session_fenced(session: &RealSession, phase: &str) -> Result<()> {
    let result = timeout(Duration::from_secs(2), session.checked_poll(0))
        .await
        .with_context(|| format!("old raw session did not fail within deadline: {phase}"))?;
    ensure!(
        result.is_err(),
        "pre-restart raw session remained usable during {phase}"
    );
    Ok(())
}

async fn stored_envelope_offsets(
    session: &RealSession,
    message_id: DurableMessageId,
    envelope_digest: EnvelopeDigest,
    canonical_bytes: &[u8],
) -> Result<Vec<u64>> {
    let first = session.checked_poll(0).await?;
    let end = first.end_exclusive();
    ensure!(
        first.resource_epoch() == session.report().location().resource_epoch()
            && first.oldest_available() == 0
            && end <= 1024,
        "fresh-process readback exceeded fixture identity or retention bound"
    );
    let mut matching = Vec::new();
    for offset in 0..end {
        let observation = if offset == 0 {
            first.clone()
        } else {
            session.checked_poll(offset).await?
        };
        ensure!(
            observation.resource_epoch() == first.resource_epoch()
                && observation.oldest_available() == 0
                && observation.end_exclusive() == end,
            "fresh-process readback frontier changed during scan"
        );
        let record = match observation.observe(offset)? {
            PollResolution::Record(record) => record,
            PollResolution::Tail => {
                return Err(anyhow!("checked poll omitted retained offset {offset}"));
            }
        };
        if record.message_id() == message_id {
            ensure!(
                record.envelope_digest() == envelope_digest
                    && record.canonical_bytes() == canonical_bytes,
                "matching message identity carried different immutable envelope data"
            );
            matching.push(record.offset());
        }
    }
    Ok(matching)
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

async fn send_accepted(handle: &mut alopex_chirps::DurableHandle, key: &[u8]) -> Result<()> {
    let prepared = handle.prepare(NodeId::new(), key.to_vec(), b"body")?;
    let result = handle
        .send(&prepared, ConfirmationBoundary::OsSyncedAccepted)
        .await?;
    ensure!(
        matches!(result.outcome(), DurableSendOutcome::OsSyncedAccepted),
        "strong send did not reach OS-sync acceptance"
    );
    ensure!(
        result.receipt().is_some(),
        "strong acceptance omitted its receipt"
    );
    Ok(())
}

async fn assert_no_success(handle: &mut alopex_chirps::DurableHandle, key: &[u8]) -> Result<()> {
    let prepared = handle.prepare(NodeId::new(), key.to_vec(), b"body")?;
    if let Ok(result) = handle
        .send(&prepared, ConfirmationBoundary::OsSyncedAccepted)
        .await
    {
        ensure!(
            !matches!(
                result.outcome(),
                DurableSendOutcome::BrokerAccepted | DurableSendOutcome::OsSyncedAccepted
            ),
            "stale or revoked operation returned a success outcome"
        );
        ensure!(
            result.receipt().is_none(),
            "non-success result carried a receipt"
        );
    }
    Ok(())
}
