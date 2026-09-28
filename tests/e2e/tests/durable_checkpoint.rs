mod durable_checkpoint_recovery;
mod task_6_5_support;

use alopex_chirps::NodeId;
use alopex_chirps_core::durable::{
    CheckpointOutcome, ConfirmationBoundary, DurableSendOutcome, InitialPosition,
    SubscriptionCreationOutcome, SubscriptionId,
};
use anyhow::{Context, Result, ensure};
use chirps_e2e::v07::{
    EvidenceSink, OracleAppendObserver, ServerProcess, VerifiedArtifact,
    connect_fixture_with_observer,
};
use chirps_fault_oracle::OracleStore;
use durable_checkpoint_recovery::{
    JournalBinding, JournalState, read_bound_journal, subscription_directory,
};
use std::sync::Arc;
use std::time::Duration;
use task_6_5_support::{
    RuntimeCredentials, assert_clean_stop, bootstrap_fixture, require_production_artifact,
};
use tokio::time::Instant;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run through scripts/run-v07-lane.sh with an attested production server"]
async fn identity_precedes_delivery_and_checkpoint_survives_fresh_runtime() -> Result<()> {
    let artifact = VerifiedArtifact::from_runner_environment()?;
    require_production_artifact(&artifact)?;
    let mut evidence = EvidenceSink::for_target(artifact.clone(), "durable_checkpoint")?;
    let mut server = ServerProcess::new(artifact.binary.clone())?;
    let fixture = bootstrap_fixture(
        &mut server,
        &artifact,
        "chirps-v07-checkpoint",
        "durable-checkpoint",
        iggy::prelude::IggyExpiry::NeverExpire,
    )
    .await?;
    server.start(true, None).await?;

    let checkpoint_root = tempfile::tempdir()?;
    let oracle_root = tempfile::tempdir()?;
    let observer = Arc::new(OracleAppendObserver::new(OracleStore::new(
        oracle_root.path().join("oracle.log"),
    )));
    let source = NodeId::new();
    let target = NodeId::new();
    let subscription_id = SubscriptionId::from_bytes([0x67; 16]);
    let namespace_digest = [0x76; 32];
    let mut first = connect_fixture_with_observer(
        &server,
        fixture,
        checkpoint_root.path(),
        source,
        chirps_e2e::v07::RUNTIME_USERNAME,
        &RuntimeCredentials,
        observer.clone(),
    )
    .await?;
    let prepared = first.prepare(target, b"checkpoint".to_vec(), b"durable-body")?;
    let sent = first
        .send(&prepared, ConfirmationBoundary::OsSyncedAccepted)
        .await?;
    ensure!(
        sent.outcome() == DurableSendOutcome::OsSyncedAccepted,
        "checkpoint fixture was not strongly accepted"
    );
    ensure!(!observer.failed(), "checkpoint fixture oracle write failed");
    let receipt = sent
        .receipt()
        .context("strong checkpoint fixture omitted its receipt")?;
    ensure!(
        matches!(
            first
                .create_subscription(
                    subscription_id,
                    target,
                    receipt.partition(),
                    namespace_digest,
                    InitialPosition::Exact(receipt.assigned_offset()),
                )
                .await?,
            SubscriptionCreationOutcome::Created(_)
        ),
        "checkpoint subscription was not durably created"
    );
    let delivery = require_delivery(
        first
            .next_delivery(
                subscription_id,
                1_000,
                alopex_chirps::DurableDeliveryClock::Trusted,
            )
            .await?,
        receipt.assigned_offset(),
        prepared.message_id(),
        prepared.envelope_digest(),
    )?;
    ensure!(
        delivery.handle().delivery_attempt() == 1,
        "first delivery did not use attempt one"
    );
    let directory = subscription_directory(checkpoint_root.path(), subscription_id);
    let identity = read_bound_journal(
        &directory.join("checkpoint.journal"),
        JournalBinding::new(
            subscription_id,
            target,
            receipt.partition(),
            receipt.assigned_offset(),
            receipt.resource_epoch(),
        ),
    )?;
    ensure!(
        identity.state == JournalState::Identity
            && identity.message_id == Some(*prepared.message_id().as_bytes())
            && identity.envelope_digest == Some(*prepared.envelope_digest().as_bytes())
            && identity.delivery_attempt == Some(1),
        "public delivery escaped before its exact identity became durable"
    );
    evidence.record("identity-before-first-delivery", "synced-and-exact")?;

    first
        .shutdown(Instant::now() + Duration::from_secs(3))
        .await?;
    drop(delivery);
    drop(first);
    assert_clean_stop(
        server.stop(Instant::now() + Duration::from_secs(5)).await?,
        "uncheckpointed-restart",
    )?;
    server.start(true, None).await?;

    let mut second = connect_fixture_with_observer(
        &server,
        fixture,
        checkpoint_root.path(),
        source,
        chirps_e2e::v07::RUNTIME_USERNAME,
        &RuntimeCredentials,
        observer.clone(),
    )
    .await?;
    let reopened = second
        .reopen_subscription(
            subscription_id,
            target,
            receipt.partition(),
            namespace_digest,
        )
        .await
        .context("reopen uncheckpointed subscription after server restart")?;
    ensure!(
        matches!(reopened, SubscriptionCreationOutcome::Created(_)),
        "fresh runtime did not reopen the uncheckpointed subscription"
    );
    let second_poll = second
        .next_delivery(
            subscription_id,
            9_999,
            alopex_chirps::DurableDeliveryClock::RollbackDetected,
        )
        .await
        .context("poll uncheckpointed subscription after server restart")?;
    let mut redelivery = require_delivery(
        second_poll,
        receipt.assigned_offset(),
        prepared.message_id(),
        prepared.envelope_digest(),
    )?;
    ensure!(
        redelivery.handle().delivery_attempt() == 2,
        "fresh runtime did not preserve identity and increment redelivery attempt"
    );
    ensure!(
        second
            .ack(subscription_id, redelivery.handle_mut())
            .context("checkpoint redelivered record after server restart")?
            == CheckpointOutcome::CheckpointCommitted,
        "ack did not commit the exact checkpoint"
    );
    let committed = read_bound_journal(
        &directory.join("checkpoint.journal"),
        JournalBinding::new(
            subscription_id,
            target,
            receipt.partition(),
            receipt.assigned_offset(),
            receipt.resource_epoch(),
        ),
    )?;
    ensure!(
        committed.state == JournalState::Checkpoint
            && committed.delivery_attempt == Some(2)
            && committed.checkpoint_owner_epoch == Some(redelivery.handle().owner_epoch()),
        "checkpoint and identity were not recovered from one exact committed frame"
    );
    evidence.record("uncheckpointed-fresh-runtime", "redelivered-attempt-two")?;
    evidence.record("checkpoint-install", "identity-and-frontier-committed")?;

    second
        .shutdown(Instant::now() + Duration::from_secs(3))
        .await?;
    drop(second);
    assert_clean_stop(
        server.stop(Instant::now() + Duration::from_secs(5)).await?,
        "checkpointed-restart",
    )?;
    server.start(true, None).await?;

    let mut third = connect_fixture_with_observer(
        &server,
        fixture,
        checkpoint_root.path(),
        source,
        chirps_e2e::v07::RUNTIME_USERNAME,
        &RuntimeCredentials,
        observer,
    )
    .await?;
    let reopened = third
        .reopen_subscription(
            subscription_id,
            target,
            receipt.partition(),
            namespace_digest,
        )
        .await
        .context("reopen checkpointed subscription after server restart")?;
    ensure!(
        matches!(reopened, SubscriptionCreationOutcome::Created(_)),
        "fresh runtime did not reopen the committed subscription"
    );
    let third_poll = third
        .next_delivery(
            subscription_id,
            10_000,
            alopex_chirps::DurableDeliveryClock::Trusted,
        )
        .await
        .context("poll checkpointed subscription after server restart")?;
    ensure!(
        third_poll == alopex_chirps::DurablePoll::Tail,
        "fresh runtime redelivered an already checkpointed record"
    );
    evidence.record("checkpointed-fresh-runtime", "next-offset-tail")?;
    third
        .shutdown(Instant::now() + Duration::from_secs(3))
        .await?;
    assert_clean_stop(
        server.stop(Instant::now() + Duration::from_secs(5)).await?,
        "final",
    )?;
    Ok(())
}

fn require_delivery(
    poll: alopex_chirps::DurablePoll,
    offset: u64,
    message_id: alopex_chirps_core::durable::DurableMessageId,
    envelope_digest: alopex_chirps_core::durable::EnvelopeDigest,
) -> Result<alopex_chirps_core::durable::Delivery> {
    let alopex_chirps::DurablePoll::Delivery(delivery) = poll else {
        anyhow::bail!("expected one exact delivery, got {poll:?}")
    };
    ensure!(
        delivery.handle().offset() == offset
            && delivery.handle().message_id() == message_id
            && delivery.handle().envelope_digest() == envelope_digest,
        "delivery identity differed from the exact stored record"
    );
    Ok(delivery)
}
