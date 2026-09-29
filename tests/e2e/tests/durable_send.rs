#[path = "durable_server_faults.rs"]
mod durable_server_faults;

use alopex_chirps::{
    DurableBuilder, DurableCredential, DurableCredentialProvider, DurableCredentialProviderError,
    DurableDeliveryClock, DurableHandle, DurablePoll, NodeId,
};
use alopex_chirps_core::durable::{
    AttemptPhase, CheckpointOutcome, ConfirmationBoundary, DurableReceipt, DurableSendOutcome,
    DurableSendResult, InitialPosition, PreparedDurableSend, SubscriptionCreationOutcome,
    SubscriptionId,
};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use chirps_e2e::v07::{
    AppendObservationRecord, AppendObservationStage, EvidenceSink, FixtureIdentity,
    OracleAppendEvidence, OracleAppendObserver, ProvisionedFixture, ROOT_PASSWORD, ROOT_USERNAME,
    RUNTIME_PASSWORD, RUNTIME_USERNAME, RealSession, ServerProcess, VerifiedArtifact,
    connect_fixture_with_observer, provision_durable_fixture, provision_production_admin,
    read_append_observations, read_oracle_append_evidence,
};
use chirps_fault_oracle::OracleStore;
use durable_server_faults::{AppendScenario, ExpectedTerminal, ORACLE_RUNTIME_STAGES, scenarios};
use iggy::prelude::*;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

const STREAM_NAME: &str = "chirps-v07-send";
const TOPIC_NAME: &str = "durable-send";
const FIRST_STRONG_OFFSET: u64 = 1;
const FIRST_STRONG_INDEX: u64 = 0;

struct RuntimeCredentials;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run through scripts/run-v07-lane.sh with an attested compatible server"]
async fn strong_append_requires_fresh_process_oracle_truth() -> Result<()> {
    let artifact = VerifiedArtifact::from_runner_environment()?;
    let matrix = scenarios(&artifact.lane)?;
    let mut evidence = EvidenceSink::for_target(artifact.clone(), "durable_send")?;

    ensure!(
        ORACLE_RUNTIME_STAGES.len() == 6,
        "Task 6.2 runtime stage vocabulary changed"
    );
    ensure!(
        (artifact.lane == "production" && matrix.len() == 1)
            || (artifact.lane == "fault" && matrix.len() == 7),
        "durable_send scenario count disagrees with the runner lane"
    );
    for scenario in matrix {
        run_scenario(&artifact, *scenario, &mut evidence).await?;
    }
    Ok(())
}

async fn run_scenario(
    artifact: &VerifiedArtifact,
    scenario: AppendScenario,
    evidence: &mut EvidenceSink,
) -> Result<()> {
    ensure!(
        !scenario.name.is_empty(),
        "durable_send scenario is unnamed"
    );
    let mut server = ServerProcess::new(artifact.binary.clone())?;
    server.start(false, None).await?;
    let root = sdk_client(&server, ROOT_USERNAME, ROOT_PASSWORD).await?;
    let provisioned = provision_durable_fixture(&root, STREAM_NAME, TOPIC_NAME).await?;
    provision_production_admin(&root).await?;
    prefill_valid_envelope(&root, &provisioned).await?;
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
    let stop = server.stop(Instant::now() + Duration::from_secs(5)).await?;
    assert_clean_stop(stop.graceful(), stop.forced(), "bootstrap")?;
    let fixture = server.read_fixture_projection(
        artifact,
        &provisioned,
        &runtime_user,
        RUNTIME_USERNAME,
        UserStatus::Active,
        &expected_runtime_permissions,
    )?;

    let observations = tempfile::tempdir()?;
    let observation_log = observations.path().join("append.jsonl");
    if scenario.expected == ExpectedTerminal::StartupRejected {
        let failure = server
            .start_with_observation(true, scenario.failpoint, Some(&observation_log))
            .await;
        ensure!(failure.is_err(), "{} started successfully", scenario.name);
        let stop = server.stop(Instant::now() + Duration::from_secs(5)).await?;
        assert_clean_stop(stop.graceful(), stop.forced(), scenario.name)?;
        if observation_log.exists() {
            ensure!(
                read_append_observations(&observation_log)?.is_empty(),
                "startup rejection manufactured append-stage evidence"
            );
        }
        evidence.record(scenario.name, "startup-rejected")?;
        return Ok(());
    }

    if artifact.lane == "fault" {
        server
            .start_with_observation(true, scenario.failpoint, Some(&observation_log))
            .await?;
    } else {
        server.start(true, None).await?;
    }

    let checkpoints = tempfile::tempdir()?;
    let oracle_directory = tempfile::tempdir()?;
    let oracle_store = OracleStore::new(oracle_directory.path().join("oracle.log"));
    let observer = Arc::new(OracleAppendObserver::new(oracle_store.clone()));
    let source = NodeId::new();
    let mut handle = connect_fixture_with_observer(
        &server,
        fixture,
        checkpoints.path(),
        source,
        RUNTIME_USERNAME,
        &RuntimeCredentials,
        observer.clone(),
    )
    .await?;
    let prepared = handle.prepare(NodeId::new(), scenario.name.as_bytes().to_vec(), b"body")?;
    let result = handle
        .send(&prepared, ConfirmationBoundary::OsSyncedAccepted)
        .await?;
    ensure!(
        !observer.failed(),
        "{} oracle callback failed",
        scenario.name
    );

    let clean_receipt = match scenario.expected {
        ExpectedTerminal::OsSyncedAccepted => {
            ensure!(
                result.outcome() == DurableSendOutcome::OsSyncedAccepted,
                "clean control did not reach OS-sync acceptance"
            );
            Some(
                result
                    .receipt()
                    .cloned()
                    .context("clean control omitted its exact receipt")?,
            )
        }
        ExpectedTerminal::Indeterminate => {
            ensure!(
                matches!(result.outcome(), DurableSendOutcome::Indeterminate(_)),
                "{} returned a false terminal outcome: {:?}",
                scenario.name,
                result.outcome()
            );
            ensure!(
                result.receipt().is_none(),
                "{} ambiguity carried an exact receipt",
                scenario.name
            );
            None
        }
        ExpectedTerminal::StartupRejected => unreachable!("handled before append"),
    };
    if is_storage_fault(scenario.failpoint) {
        assert_partition_fail_stopped(&mut handle, &server, fixture, scenario.name).await?;
    }
    shutdown_handle(&mut handle, scenario.name).await?;
    let stop = if artifact.lane == "fault" {
        server
            .force_stop(Instant::now() + Duration::from_secs(2))
            .await?
    } else {
        server.stop(Instant::now() + Duration::from_secs(5)).await?
    };
    assert_clean_stop(stop.graceful(), stop.forced(), scenario.name)?;

    let oracle = read_oracle_append_evidence(&oracle_store)?;
    ensure!(
        oracle.len() == 1 && oracle[0].matches_prepared(&prepared),
        "{} oracle did not bind the exact prepared envelope",
        scenario.name
    );
    assert_result_binding(&result, &oracle[0], &prepared)?;
    let observed_location = if artifact.lane == "fault" {
        let records = read_append_observations(&observation_log)?;
        let location = assert_stage_records(
            &records,
            scenario.observed_before_failure,
            &oracle[0],
            fixture,
        )?;
        evidence.record(
            &format!("{}-hash-chain-and-oracle", scenario.name),
            "accepted",
        )?;
        location
    } else {
        evidence.record("clean-control-oracle-intent", "accepted")?;
        None
    };

    if artifact.lane == "fault" {
        server
            .start_with_observation(true, None, Some(&observation_log))
            .await?;
    } else {
        server.start(true, None).await?;
    }
    let mut readback = RealSession::bind(
        server.address(),
        server.certificate_der(),
        RUNTIME_USERNAME,
        RUNTIME_PASSWORD,
        fixture.stream_id,
        fixture.topic_id,
        fixture.partition_id,
        800,
    )
    .await?;
    fixture.assert_report(readback.report())?;
    let first_readback = exact_readback_offsets(&readback, &prepared).await?;

    if let Some(receipt) = clean_receipt {
        ensure!(
            first_readback == vec![receipt.assigned_offset()]
                && receipt.assigned_offset() == FIRST_STRONG_OFFSET
                && receipt.assigned_index() == FIRST_STRONG_INDEX
                && receipt.partition() == fixture.partition_id
                && receipt.resource_epoch().resource_id().as_bytes() == &fixture.resource_id
                && receipt.resource_epoch().epoch() == fixture.resource_epoch
                && receipt.message_id() == prepared.message_id()
                && receipt.envelope_digest() == prepared.envelope_digest(),
            "clean receipt and fresh-process checked-poll disagree"
        );
        assert_receipt_binding(&receipt, &oracle[0])?;
        evidence.record("clean-control-fresh-process-readback", "exact")?;
    } else {
        ensure!(
            first_readback.len() <= 1,
            "{} first recovery appended more than once",
            scenario.name
        );
        evidence.record(
            &format!("{}-fresh-process-readback", scenario.name),
            if first_readback.is_empty() {
                "zero"
            } else {
                "one"
            },
        )?;
        if let Some((offset, _)) = observed_location
            && !first_readback.is_empty()
        {
            ensure!(
                first_readback == vec![offset],
                "{} sidecar location and fresh-process readback disagree",
                scenario.name
            );
        }
        if scenario.failpoint == Some("response") {
            ensure!(
                first_readback == vec![FIRST_STRONG_OFFSET],
                "response loss did not preserve its fully synced exact record"
            );
        }
    }

    if scenario.failpoint == Some("response") {
        readback
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await?;
        let retry_observer = Arc::new(OracleAppendObserver::new(oracle_store.clone()));
        let retry_checkpoints = tempfile::tempdir()?;
        let mut retry = connect_fixture_with_observer(
            &server,
            fixture,
            retry_checkpoints.path(),
            source,
            RUNTIME_USERNAME,
            &RuntimeCredentials,
            retry_observer.clone(),
        )
        .await?;
        let subscription_id = SubscriptionId::from_bytes([0x64; 16]);
        let creation = retry
            .create_subscription(
                subscription_id,
                prepared.target(),
                prepared.partition(),
                [0x65; 32],
                InitialPosition::Exact(FIRST_STRONG_OFFSET),
            )
            .await?;
        ensure!(
            matches!(creation, SubscriptionCreationOutcome::Created(_)),
            "response retry subscription was not durably created"
        );
        let mut first_delivery = require_delivery(
            retry
                .next_delivery(subscription_id, 0, DurableDeliveryClock::Trusted)
                .await?,
            &prepared,
            FIRST_STRONG_OFFSET,
        )?;
        retry.release(subscription_id, first_delivery.handle_mut())?;

        let retry_result = retry
            .send(&prepared, ConfirmationBoundary::OsSyncedAccepted)
            .await?;
        ensure!(
            retry_result.outcome() == DurableSendOutcome::OsSyncedAccepted
                && retry_result.receipt().is_some()
                && !retry_observer.failed(),
            "explicit same-Prepared retry did not produce one valid acknowledgement"
        );

        let mut first_redelivery = require_delivery(
            retry
                .next_delivery(subscription_id, 0, DurableDeliveryClock::Trusted)
                .await?,
            &prepared,
            FIRST_STRONG_OFFSET,
        )?;
        ensure!(
            retry.ack(subscription_id, first_redelivery.handle_mut())?
                == CheckpointOutcome::CheckpointCommitted,
            "valid first-delivery acknowledgement did not commit its frontier"
        );
        let mut duplicate = require_delivery(
            retry
                .next_delivery(subscription_id, 0, DurableDeliveryClock::Trusted)
                .await?,
            &prepared,
            FIRST_STRONG_OFFSET + 1,
        )?;
        retry.release(subscription_id, duplicate.handle_mut())?;
        let mut duplicate_redelivery = require_delivery(
            retry
                .next_delivery(subscription_id, 0, DurableDeliveryClock::Trusted)
                .await?,
            &prepared,
            FIRST_STRONG_OFFSET + 1,
        )?;
        ensure!(
            retry.ack(subscription_id, duplicate_redelivery.handle_mut())?
                == CheckpointOutcome::CheckpointCommitted,
            "valid duplicate acknowledgement did not commit its frontier"
        );
        ensure!(
            retry
                .next_delivery(subscription_id, 0, DurableDeliveryClock::Trusted)
                .await?
                == DurablePoll::Tail,
            "frontier did not advance to tail after the valid duplicate acknowledgement"
        );
        evidence.record("response-retry-frontier", "release-stable-ack-only-advance")?;
        shutdown_handle(&mut retry, "response-retry").await?;
        let stop = server.stop(Instant::now() + Duration::from_secs(5)).await?;
        assert_clean_stop(stop.graceful(), stop.forced(), "response-retry")?;

        let oracle = read_oracle_append_evidence(&oracle_store)?;
        ensure!(
            oracle.len() == 2
                && oracle.iter().all(|value| value.matches_prepared(&prepared))
                && oracle[0].attempt_id != oracle[1].attempt_id,
            "same-Prepared retry did not create two distinct observed attempts"
        );
        assert_result_binding(&retry_result, &oracle[1], &prepared)?;
        let records = read_append_observations(&observation_log)?;
        let first_len = scenario.observed_before_failure.len();
        ensure!(
            records.len() == first_len + 5,
            "response retry observation log omitted or added a physical stage"
        );
        assert_stage_records(
            &records[..first_len],
            scenario.observed_before_failure,
            &oracle[0],
            fixture,
        )?;
        let retry_location = assert_stage_records(
            &records[first_len..],
            &[
                AppendObservationStage::WireWrite,
                AppendObservationStage::JournalFlush,
                AppendObservationStage::MessageSync,
                AppendObservationStage::IndexSync,
                AppendObservationStage::Response,
            ],
            &oracle[1],
            fixture,
        )?
        .context("response retry omitted its exact physical location")?;
        let retry_receipt = retry_result
            .receipt()
            .context("response retry omitted its exact receipt")?;
        assert_receipt_binding(retry_receipt, &oracle[1])?;
        ensure!(
            retry_location
                == (
                    retry_receipt.assigned_offset(),
                    retry_receipt.assigned_index()
                )
                && retry_location == (FIRST_STRONG_OFFSET + 1, FIRST_STRONG_INDEX + 1)
                && retry_receipt.partition() == fixture.partition_id
                && retry_receipt.resource_epoch().resource_id().as_bytes() == &fixture.resource_id
                && retry_receipt.resource_epoch().epoch() == fixture.resource_epoch
                && retry_receipt.message_id() == prepared.message_id()
                && retry_receipt.envelope_digest() == prepared.envelope_digest(),
            "response retry receipt and sidecar location disagree"
        );

        server
            .start_with_observation(true, None, Some(&observation_log))
            .await?;
        let mut final_readback = RealSession::bind(
            server.address(),
            server.certificate_der(),
            RUNTIME_USERNAME,
            RUNTIME_PASSWORD,
            fixture.stream_id,
            fixture.topic_id,
            fixture.partition_id,
            800,
        )
        .await?;
        let offsets = exact_readback_offsets(&final_readback, &prepared).await?;
        ensure!(
            offsets == vec![FIRST_STRONG_OFFSET, FIRST_STRONG_OFFSET + 1],
            "response retry did not expose both exact logical deliveries"
        );
        evidence.record("response-retry-duplicate-delivery", "observed")?;
        final_readback
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await?;
    } else {
        readback
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await?;
    }
    let stop = server.stop(Instant::now() + Duration::from_secs(5)).await?;
    assert_clean_stop(stop.graceful(), stop.forced(), "final")?;
    evidence.record(scenario.name, "accepted")?;
    Ok(())
}

fn assert_stage_records(
    records: &[AppendObservationRecord],
    expected: &[AppendObservationStage],
    oracle: &OracleAppendEvidence,
    fixture: FixtureIdentity,
) -> Result<Option<(u64, u64)>> {
    ensure!(
        records.len() == expected.len(),
        "server observation length differs from the actual pre-failure stages: expected={expected:?}, observed={:?}",
        records
            .iter()
            .map(|record| record.stage)
            .collect::<Vec<_>>()
    );
    let mut location = None;
    for (record, stage) in records.iter().zip(expected) {
        ensure!(
            record.stage == *stage
                && record.attempt_id == oracle.attempt_id
                && record.message_id == oracle.message_id
                && record.envelope_digest == oracle.envelope_digest
                && record.resource_id == hex(&fixture.resource_id)
                && record.resource_epoch == fixture.resource_epoch
                && record.partition_id == fixture.partition_id,
            "server hash-chain record does not correlate to oracle evidence"
        );
        match (record.assigned_offset, record.assigned_index) {
            (None, None) => ensure!(
                *stage == AppendObservationStage::WireWrite,
                "only wire-write may omit the assigned location"
            ),
            (Some(offset), Some(index)) => {
                ensure!(
                    *stage != AppendObservationStage::WireWrite,
                    "wire-write manufactured an assigned location"
                );
                ensure!(
                    location.is_none_or(|expected| expected == (offset, index)),
                    "physical stages disagreed on the assigned location"
                );
                location = Some((offset, index));
            }
            _ => anyhow::bail!("server observation returned a partial assigned location"),
        }
    }
    Ok(location)
}

async fn exact_readback_offsets(
    session: &RealSession,
    prepared: &PreparedDurableSend,
) -> Result<Vec<u64>> {
    let first = session.checked_poll(0).await?;
    ensure!(
        first.resource_epoch() == session.report().location().resource_epoch()
            && first.oldest_available() == 0
            && first.end_exclusive() <= 8,
        "isolated append scenario exceeded its bounded record count"
    );
    let mut matches = Vec::new();
    for offset in 0..first.end_exclusive() {
        let observation = if offset == 0 {
            first.clone()
        } else {
            session.checked_poll(offset).await?
        };
        ensure!(
            observation.resource_epoch() == first.resource_epoch()
                && observation.oldest_available() == 0
                && observation.end_exclusive() == first.end_exclusive(),
            "fresh-process checked-poll frontier changed during the bounded scan"
        );
        let record = match observation.observe(offset)? {
            alopex_chirps_core::durable::PollResolution::Record(record) => record,
            alopex_chirps_core::durable::PollResolution::Tail => {
                anyhow::bail!("checked-poll returned tail for retained offset {offset}")
            }
        };
        if record.message_id() == prepared.message_id() {
            ensure!(
                record.envelope_digest() == prepared.envelope_digest()
                    && record.canonical_bytes() == prepared.canonical_bytes(),
                "logical message identity was stored with different immutable bytes"
            );
            matches.push(record.offset());
        }
    }
    Ok(matches)
}

fn assert_result_binding(
    result: &DurableSendResult,
    oracle: &OracleAppendEvidence,
    prepared: &PreparedDurableSend,
) -> Result<()> {
    let binding = result.attempt_binding();
    ensure!(
        result.message_id() == prepared.message_id()
            && binding.phase() == AttemptPhase::AppendInvoked
            && binding
                .attempt_id()
                .is_some_and(|value| hex(value.as_bytes()) == oracle.attempt_id)
            && binding
                .session_fingerprint()
                .is_some_and(|value| hex(value.as_bytes()) == oracle.session_fingerprint)
            && binding.requested_boundary() == Some(oracle.requested_boundary),
        "send result substituted an exact attempt binding field"
    );
    Ok(())
}

fn assert_receipt_binding(receipt: &DurableReceipt, oracle: &OracleAppendEvidence) -> Result<()> {
    let binding = receipt.attempt_binding();
    ensure!(
        binding.phase() == AttemptPhase::AppendInvoked
            && binding
                .attempt_id()
                .is_some_and(|value| hex(value.as_bytes()) == oracle.attempt_id)
            && binding
                .session_fingerprint()
                .is_some_and(|value| hex(value.as_bytes()) == oracle.session_fingerprint)
            && binding.requested_boundary() == Some(oracle.requested_boundary),
        "strong receipt substituted an exact attempt binding field"
    );
    Ok(())
}

fn require_delivery(
    poll: DurablePoll,
    prepared: &PreparedDurableSend,
    expected_offset: u64,
) -> Result<alopex_chirps_core::durable::Delivery> {
    let DurablePoll::Delivery(delivery) = poll else {
        anyhow::bail!("expected one exact public delivery")
    };
    ensure!(
        delivery.canonical_bytes() == prepared.canonical_bytes()
            && delivery.handle().offset() == expected_offset
            && delivery.handle().message_id() == prepared.message_id()
            && delivery.handle().envelope_digest() == prepared.envelope_digest()
            && delivery.handle().target() == prepared.target()
            && delivery.handle().generation() == prepared.generation()
            && delivery.handle().partition() == prepared.partition(),
        "public delivery disagreed with the exact prepared envelope"
    );
    Ok(delivery)
}

async fn assert_partition_fail_stopped(
    handle: &mut DurableHandle,
    server: &ServerProcess,
    fixture: FixtureIdentity,
    phase: &str,
) -> Result<()> {
    let followup = handle.prepare(
        NodeId::new(),
        format!("{phase}-fail-stop").into_bytes(),
        b"body",
    )?;
    let send_rejected = match handle
        .send(&followup, ConfirmationBoundary::OsSyncedAccepted)
        .await
    {
        Err(_) => true,
        Ok(result) => !matches!(
            result.outcome(),
            DurableSendOutcome::BrokerAccepted | DurableSendOutcome::OsSyncedAccepted
        ),
    };
    ensure!(send_rejected, "{phase} partition accepted a second append");

    if let Ok(mut probe) = RealSession::bind(
        server.address(),
        server.certificate_der(),
        RUNTIME_USERNAME,
        RUNTIME_PASSWORD,
        fixture.stream_id,
        fixture.topic_id,
        fixture.partition_id,
        800,
    )
    .await
    {
        let poll_rejected = probe.checked_poll(FIRST_STRONG_OFFSET).await.is_err();
        probe
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await?;
        ensure!(poll_rejected, "{phase} partition accepted a checked poll");
    }
    Ok(())
}

fn is_storage_fault(failpoint: Option<&str>) -> bool {
    matches!(
        failpoint,
        Some("append" | "journal-flush" | "message-sync" | "index-sync")
    )
}

async fn prefill_valid_envelope(client: &IggyClient, fixture: &ProvisionedFixture) -> Result<()> {
    let preparer = DurableBuilder::new(NodeId::new())
        .inbox_generation(1)
        .explicit_partitions(1)
        .build()?;
    let prepared = preparer.prepare(NodeId::new(), b"prefill".to_vec(), b"prefill")?;
    let mut messages = vec![
        IggyMessage::builder()
            .id(u128::from_be_bytes(*prepared.message_id().as_bytes()))
            .payload(prepared.canonical_bytes().to_vec().into())
            .build()?,
    ];
    client
        .send_messages(
            &Identifier::numeric(fixture.stream_id)?,
            &Identifier::numeric(fixture.topic_id)?,
            &Partitioning::partition_id(fixture.partition_id),
            &mut messages,
        )
        .await?;
    Ok(())
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

async fn shutdown_handle(handle: &mut DurableHandle, phase: &str) -> Result<()> {
    let report = handle
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .with_context(|| format!("shutdown DurableHandle after {phase}"))?;
    ensure!(
        report.transport_closed() && report.workers_joined(),
        "DurableHandle shutdown omitted terminal ownership evidence"
    );
    Ok(())
}

fn assert_clean_stop(graceful: bool, forced: bool, phase: &str) -> Result<()> {
    ensure!(
        graceful != forced,
        "{phase} server stop did not select exactly one termination path"
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
                        read_topic: true,
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
