mod durable_retention;
mod task_6_5_support;

use alopex_chirps::{DurableDeliveryClock, DurablePoll, DurableSubscriptionError, NodeId};
use alopex_chirps_core::durable::{
    CheckpointOutcome, ConfirmationBoundary, DurableSendOutcome, InitialPosition, PartitionState,
    PollResolution, ReplayError, SubscriptionCreationOutcome, SubscriptionId, expected_offset,
};
use anyhow::{Context, Result, ensure};
use chirps_e2e::v07::{
    EvidenceSink, OracleAppendObserver, RUNTIME_PASSWORD, RUNTIME_USERNAME, RealSession,
    ServerProcess, VerifiedArtifact, connect_fixture_with_observer, read_oracle_append_evidence,
};
use chirps_fault_oracle::OracleStore;
use iggy::prelude::{Identifier, IggyExpiry, IggyMessage, MessageClient, Partitioning};
use std::sync::Arc;
use std::time::Duration;
use task_6_5_support::{
    RuntimeCredentials, assert_clean_stop, bootstrap_fixture, require_production_artifact,
    sdk_client,
};
use tokio::time::Instant;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run through scripts/run-v07-lane.sh with an attested production server"]
async fn checked_poll_truth_table_and_malformed_record_fail_stop() -> Result<()> {
    let artifact = VerifiedArtifact::from_runner_environment()?;
    require_production_artifact(&artifact)?;
    let mut evidence = EvidenceSink::for_target(artifact.clone(), "durable_poll")?;
    let mut server = ServerProcess::new(artifact.binary.clone())?;
    let fixture = bootstrap_fixture(
        &mut server,
        &artifact,
        "chirps-v07-poll",
        "durable-poll",
        IggyExpiry::NeverExpire,
    )
    .await?;
    server.start(true, None).await?;

    let checkpoint_root = tempfile::tempdir()?;
    let oracle_root = tempfile::tempdir()?;
    let oracle = OracleStore::new(oracle_root.path().join("oracle.log"));
    let observer = Arc::new(OracleAppendObserver::new(oracle.clone()));
    let source = NodeId::new();
    let target = NodeId::new();
    let mut handle = connect_fixture_with_observer(
        &server,
        fixture,
        checkpoint_root.path(),
        source,
        RUNTIME_USERNAME,
        &RuntimeCredentials,
        observer.clone(),
    )
    .await?;
    let prepared = handle.prepare(target, b"poll-truth-table".to_vec(), b"valid-record")?;
    let sent = handle
        .send(&prepared, ConfirmationBoundary::OsSyncedAccepted)
        .await?;
    ensure!(
        sent.outcome() == DurableSendOutcome::OsSyncedAccepted,
        "truth-table record was not strongly accepted"
    );
    let receipt = sent
        .receipt()
        .cloned()
        .context("strong poll result omitted its receipt")?;
    ensure!(!observer.failed(), "poll oracle callback failed");
    let oracle_evidence = read_oracle_append_evidence(&oracle)?;
    ensure!(
        oracle_evidence.len() == 1 && oracle_evidence[0].matches_prepared(&prepared),
        "poll oracle did not bind the exact prepared envelope"
    );

    let shutdown = handle
        .shutdown(Instant::now() + Duration::from_secs(3))
        .await?;
    ensure!(shutdown.transport_closed() && shutdown.workers_joined());
    drop(handle);
    assert_clean_stop(
        server.stop(Instant::now() + Duration::from_secs(5)).await?,
        "poll-fixture-restart",
    )?;
    server.start(true, None).await?;
    let mut handle = connect_fixture_with_observer(
        &server,
        fixture,
        checkpoint_root.path(),
        source,
        RUNTIME_USERNAME,
        &RuntimeCredentials,
        observer.clone(),
    )
    .await?;

    let mut raw = RealSession::bind(
        server.address(),
        server.certificate_der(),
        RUNTIME_USERNAME,
        RUNTIME_PASSWORD,
        fixture.stream_id,
        fixture.topic_id,
        fixture.partition_id,
        5_000,
    )
    .await?;
    fixture.assert_report(raw.report())?;

    let expected = receipt.assigned_offset();
    let record_observation = raw.checked_poll(expected).await?;
    let PollResolution::Record(record) = record_observation.observe(expected)? else {
        anyhow::bail!("retained expected offset resolved as tail")
    };
    ensure!(
        record_observation.resource_epoch() == receipt.resource_epoch()
            && record_observation.oldest_available() <= expected
            && expected < record_observation.end_exclusive()
            && record.offset() == expected
            && record.message_id() == prepared.message_id()
            && record.envelope_digest() == prepared.envelope_digest()
            && record.canonical_bytes() == prepared.canonical_bytes(),
        "record branch disagreed with the exact receipt and envelope"
    );
    evidence.record("oldest<=expected<h", "exact-one-record")?;

    let end = record_observation.end_exclusive();
    let tail = raw.checked_poll(end).await?;
    ensure!(
        matches!(tail.observe(end), Ok(PollResolution::Tail))
            && tail.resource_epoch() == record_observation.resource_epoch()
            && tail.end_exclusive() == end,
        "expected==H did not resolve to the same-epoch empty tail"
    );
    evidence.record("expected==h", "tail")?;

    let ahead = end.checked_add(1).context("test end offset exhausted")?;
    let conflict = raw.checked_poll(ahead).await?;
    ensure!(
        matches!(
            conflict.observe(ahead),
            Err(ReplayError::CheckpointConflict {
                expected,
                end_exclusive,
            }) if expected == ahead && end_exclusive == end
        ) && conflict.resource_epoch() == record_observation.resource_epoch(),
        "expected>H was clamped or misclassified"
    );
    evidence.record("expected>h", "checkpoint-conflict")?;
    ensure!(
        matches!(
            expected_offset(Some(u64::MAX), 0),
            Err(ReplayError::OffsetExhausted {
                checkpoint: u64::MAX,
            })
        ),
        "exhausted canonical checkpoint wrapped or contacted the backend"
    );
    evidence.record("checkpoint-s-max", "offset-exhausted-before-poll")?;

    let subscription_id = SubscriptionId::from_bytes([0x68; 16]);
    let SubscriptionCreationOutcome::Created(created) = handle
        .create_subscription(
            subscription_id,
            target,
            fixture.partition_id,
            [0x86; 32],
            InitialPosition::Exact(expected),
        )
        .await?
    else {
        anyhow::bail!("exact subscription was not created")
    };
    let DurablePoll::Delivery(mut delivery) = handle
        .next_delivery(subscription_id, 1_000, DurableDeliveryClock::Trusted)
        .await?
    else {
        anyhow::bail!("public subscriber did not deliver the exact checked record")
    };
    ensure!(
        delivery.handle().offset() == expected
            && delivery.handle().message_id() == prepared.message_id()
            && delivery.handle().envelope_digest() == prepared.envelope_digest()
            && delivery.canonical_bytes() == prepared.canonical_bytes(),
        "public delivery changed the checked record identity"
    );
    ensure!(
        handle.ack(subscription_id, delivery.handle_mut())?
            == CheckpointOutcome::CheckpointCommitted,
        "application ack did not install the canonical checkpoint"
    );

    raw.shutdown(Instant::now() + Duration::from_secs(2))
        .await?;
    let shutdown = handle
        .shutdown(Instant::now() + Duration::from_secs(3))
        .await?;
    ensure!(shutdown.transport_closed() && shutdown.workers_joined());
    drop(handle);
    assert_clean_stop(
        server.stop(Instant::now() + Duration::from_secs(5)).await?,
        "checkpoint-s-plus-one-restart",
    )?;
    server.start(true, None).await?;
    let mut handle = connect_fixture_with_observer(
        &server,
        fixture,
        checkpoint_root.path(),
        source,
        RUNTIME_USERNAME,
        &RuntimeCredentials,
        observer.clone(),
    )
    .await?;
    let SubscriptionCreationOutcome::Created(reopened) = handle
        .reopen_subscription(subscription_id, target, fixture.partition_id, [0x86; 32])
        .await?
    else {
        anyhow::bail!("fresh runtime did not reopen the checkpointed subscription")
    };
    ensure!(
        reopened.owner_epoch() > created.owner_epoch(),
        "fresh runtime reused the stale local owner epoch"
    );
    let mut raw = RealSession::bind(
        server.address(),
        server.certificate_der(),
        RUNTIME_USERNAME,
        RUNTIME_PASSWORD,
        fixture.stream_id,
        fixture.topic_id,
        fixture.partition_id,
        5_000,
    )
    .await?;
    fixture.assert_report(raw.report())?;
    ensure!(
        raw.report().location().resource_epoch() == receipt.resource_epoch(),
        "fresh runtime rebound the checkpoint under a different resource identity"
    );
    ensure!(
        handle
            .next_delivery(subscription_id, 1_001, DurableDeliveryClock::Trusted)
            .await?
            == DurablePoll::Tail,
        "checkpoint S did not make the next inclusive expected offset S+1"
    );
    evidence.record("checkpoint-s+1", "tail-without-offset-mirror")?;

    let malformed_expected = end;
    let mut message_id = [0x55; 16];
    message_id[6] = 0x45;
    message_id[8] = 0x85;
    let mut malformed = vec![
        IggyMessage::builder()
            .id(u128::from_be_bytes(message_id))
            .payload(b"not-a-chirps-envelope".to_vec().into())
            .build()?,
    ];
    let sdk = sdk_client(&server, RUNTIME_USERNAME, RUNTIME_PASSWORD).await?;
    sdk.send_messages(
        &Identifier::numeric(fixture.stream_id)?,
        &Identifier::numeric(fixture.topic_id)?,
        &Partitioning::partition_id(fixture.partition_id),
        &mut malformed,
    )
    .await
    .context("append malformed raw record through the official SDK")?;
    drop(sdk);

    raw.shutdown(Instant::now() + Duration::from_secs(2))
        .await?;
    let shutdown = handle
        .shutdown(Instant::now() + Duration::from_secs(3))
        .await?;
    ensure!(shutdown.transport_closed() && shutdown.workers_joined());
    drop(handle);
    assert_clean_stop(
        server.stop(Instant::now() + Duration::from_secs(5)).await?,
        "malformed-record-restart",
    )?;
    server.start(true, None).await?;
    let mut handle = connect_fixture_with_observer(
        &server,
        fixture,
        checkpoint_root.path(),
        source,
        RUNTIME_USERNAME,
        &RuntimeCredentials,
        observer,
    )
    .await?;
    let mut raw = RealSession::bind(
        server.address(),
        server.certificate_der(),
        RUNTIME_USERNAME,
        RUNTIME_PASSWORD,
        fixture.stream_id,
        fixture.topic_id,
        fixture.partition_id,
        5_000,
    )
    .await?;
    fixture.assert_report(raw.report())?;
    let malformed_observation = raw
        .checked_poll(malformed_expected)
        .await
        .context("poll malformed raw record after the fresh server restart")?;
    ensure!(
        matches!(
            malformed_observation.observe(malformed_expected),
            Ok(PollResolution::Record(record))
                if record.offset() == malformed_expected
                    && record.canonical_bytes() == b"not-a-chirps-envelope"
        ),
        "production checked poll did not expose the exact malformed stored record"
    );

    let malformed_subscription = SubscriptionId::from_bytes([0x69; 16]);
    ensure!(
        matches!(
            handle
                .create_subscription(
                    malformed_subscription,
                    NodeId::new(),
                    fixture.partition_id,
                    [0x96; 32],
                    InitialPosition::Exact(malformed_expected),
                )
                .await?,
            SubscriptionCreationOutcome::Created(_)
        ),
        "malformed-record subscription was not created"
    );
    ensure!(
        matches!(
            handle
                .next_delivery(malformed_subscription, 2_000, DurableDeliveryClock::Trusted,)
                .await,
            Err(DurableSubscriptionError::Unavailable)
        ),
        "malformed canonical bytes escaped as an application delivery"
    );
    ensure!(
        handle
            .health()
            .partitions()
            .iter()
            .any(|partition| matches!(partition.state(), PartitionState::Faulted(_))),
        "malformed canonical bytes did not fail-stop the partition"
    );
    ensure!(
        matches!(
            handle
                .next_delivery(malformed_subscription, 2_001, DurableDeliveryClock::Trusted,)
                .await,
            Err(DurableSubscriptionError::Unavailable)
        ),
        "fail-stopped malformed record was silently skipped on retry"
    );
    evidence.record("malformed-record", "not-delivered-and-fail-stopped")?;

    raw.shutdown(Instant::now() + Duration::from_secs(2))
        .await?;
    let shutdown = handle
        .shutdown(Instant::now() + Duration::from_secs(3))
        .await?;
    ensure!(shutdown.transport_closed() && shutdown.workers_joined());
    assert_clean_stop(
        server.stop(Instant::now() + Duration::from_secs(5)).await?,
        "durable-poll",
    )?;
    Ok(())
}
