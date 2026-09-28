mod durable_redelivery;
mod task_6_5_support;

use alopex_chirps::{
    DurableDeliveryClock, DurableHandle, DurablePoll, DurableSubscriptionError, NodeId,
};
use alopex_chirps_core::durable::{
    CheckpointOperationPhase, CheckpointOutcome, ConfirmationBoundary, Delivery,
    DeliveryHandleState, DurableReceipt, DurableSendOutcome, HandleTransitionError,
    InitialPosition, PreparedDurableSend, SubscriptionCreationOutcome, SubscriptionId,
};
use anyhow::{Context, Result, ensure};
use chirps_e2e::v07::{
    EvidenceSink, FixtureIdentity, OracleAppendObserver, RUNTIME_USERNAME, ServerProcess,
    VerifiedArtifact, connect_fixture_with_observer, read_oracle_append_evidence,
};
use chirps_fault_oracle::OracleStore;
use iggy::prelude::IggyExpiry;
use std::sync::Arc;
use std::time::Duration;
use task_6_5_support::{
    RuntimeCredentials, assert_clean_stop, bootstrap_fixture, require_production_artifact,
};
use tokio::time::Instant;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run through scripts/run-v07-lane.sh with an attested production server"]
async fn ack_release_timeout_and_shutdown_have_one_irrevocable_winner() -> Result<()> {
    let artifact = VerifiedArtifact::from_runner_environment()?;
    require_production_artifact(&artifact)?;
    let mut evidence = EvidenceSink::for_target(artifact.clone(), "durable_delivery")?;
    let mut server = ServerProcess::new(artifact.binary.clone())?;
    let fixture = bootstrap_fixture(
        &mut server,
        &artifact,
        "chirps-v07-delivery",
        "durable-delivery",
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
    let sent = send_records(&mut handle, target, 4).await?;
    ensure!(!observer.failed(), "delivery append oracle callback failed");
    let append_evidence = read_oracle_append_evidence(&oracle)?;
    ensure!(
        append_evidence.len() == sent.len()
            && sent.iter().all(|(prepared, _)| append_evidence
                .iter()
                .any(|entry| entry.matches_prepared(prepared))),
        "delivery append oracle did not bind every exact prepared envelope"
    );

    let ack_subscription = subscription_id(0x70);
    let mut ack_delivery = open_delivery(
        &mut handle,
        fixture,
        ack_subscription,
        target,
        [0x70; 32],
        &sent[0],
    )
    .await?;
    ensure!(
        matches!(
            handle
                .next_delivery(ack_subscription, 1_001, DurableDeliveryClock::Trusted,)
                .await,
            Err(DurableSubscriptionError::Unavailable)
        ),
        "a second message became in-flight before the first handle terminated"
    );
    ensure!(
        handle.ack(ack_subscription, ack_delivery.handle_mut())?
            == CheckpointOutcome::CheckpointCommitted,
        "ack winner did not durably commit its exact checkpoint"
    );
    assert_irrevocable(&mut ack_delivery, DeliveryHandleState::AckTerminalCommitted)?;
    ensure!(
        handle
            .next_delivery(ack_subscription, 1_002, DurableDeliveryClock::Trusted,)
            .await?
            == DurablePoll::Tail,
        "committed ack did not advance exactly to the next frontier"
    );
    evidence.record("terminal-winner-ack", "checkpoint-committed")?;

    let release_subscription = subscription_id(0x71);
    let mut released = open_delivery(
        &mut handle,
        fixture,
        release_subscription,
        target,
        [0x71; 32],
        &sent[1],
    )
    .await?;
    handle.release(release_subscription, released.handle_mut())?;
    assert_irrevocable(&mut released, DeliveryHandleState::Released)?;
    let mut redelivery = require_delivery(
        handle
            .next_delivery(release_subscription, 2_000, DurableDeliveryClock::Trusted)
            .await?,
        &sent[1],
    )?;
    ensure!(
        redelivery.handle().delivery_attempt() > released.handle().delivery_attempt()
            && redelivery.handle().owner_epoch() == released.handle().owner_epoch(),
        "release did not produce a fresh same-owner delivery attempt"
    );
    ensure!(
        matches!(
            handle.ack(release_subscription, released.handle_mut()),
            Err(DurableSubscriptionError::Unavailable)
        ),
        "stale released attempt was allowed to install a checkpoint"
    );
    ensure!(
        handle.ack(release_subscription, redelivery.handle_mut())?
            == CheckpointOutcome::CheckpointCommitted,
        "current redelivery could not commit after stale-attempt rejection"
    );
    evidence.record("terminal-winner-nack-release", "fresh-attempt-stale-fenced")?;

    let timeout_subscription = subscription_id(0x72);
    let mut timed_out = open_delivery(
        &mut handle,
        fixture,
        timeout_subscription,
        target,
        [0x72; 32],
        &sent[2],
    )
    .await?;
    timed_out.handle_mut().timeout()?;
    assert_irrevocable(&mut timed_out, DeliveryHandleState::TimedOut)?;
    ensure!(
        matches!(
            handle.ack(timeout_subscription, timed_out.handle_mut()),
            Err(DurableSubscriptionError::Unavailable)
        ),
        "timed-out handle advanced the canonical checkpoint"
    );
    evidence.record("terminal-winner-timeout", "checkpoint-unchanged")?;

    let shutdown_subscription = subscription_id(0x73);
    let mut shutdown_delivery = open_delivery(
        &mut handle,
        fixture,
        shutdown_subscription,
        target,
        [0x73; 32],
        &sent[3],
    )
    .await?;
    let report = handle
        .shutdown(Instant::now() + Duration::from_secs(3))
        .await?;
    ensure!(
        report.transport_closed()
            && report.workers_joined()
            && report.checkpoint_operations().iter().any(|operation| {
                operation.phase()
                    == CheckpointOperationPhase::Terminal(CheckpointOutcome::CheckpointCommitted)
                    && operation.outcome() == CheckpointOutcome::CheckpointCommitted
            }),
        "shutdown omitted or replaced the already committed checkpoint outcome"
    );
    ensure!(
        ack_delivery.handle().state() == DeliveryHandleState::AckTerminalCommitted,
        "shutdown rewrote the committed delivery handle"
    );
    ensure!(
        matches!(
            handle.ack(shutdown_subscription, shutdown_delivery.handle_mut()),
            Err(DurableSubscriptionError::Unavailable)
        ) && shutdown_delivery.handle().state() == DeliveryHandleState::Open,
        "shutdown-fenced runtime accepted or locally rewrote a stale open handle"
    );
    shutdown_delivery.handle_mut().shutdown_fence()?;
    assert_irrevocable(&mut shutdown_delivery, DeliveryHandleState::ShutdownFenced)?;
    evidence.record(
        "terminal-winner-shutdown",
        "runtime-fenced-no-outcome-rewrite",
    )?;

    assert_clean_stop(
        server.stop(Instant::now() + Duration::from_secs(5)).await?,
        "durable-delivery",
    )?;
    Ok(())
}

async fn send_records(
    handle: &mut DurableHandle,
    target: NodeId,
    count: usize,
) -> Result<Vec<(PreparedDurableSend, DurableReceipt)>> {
    let mut sent = Vec::with_capacity(count);
    for index in 0..count {
        let prepared = handle.prepare(
            target,
            format!("delivery-{index}").into_bytes(),
            format!("payload-{index}").as_bytes(),
        )?;
        let result = handle
            .send(&prepared, ConfirmationBoundary::OsSyncedAccepted)
            .await?;
        ensure!(
            result.outcome() == DurableSendOutcome::OsSyncedAccepted,
            "delivery fixture {index} was not strongly accepted"
        );
        let receipt = result
            .receipt()
            .cloned()
            .with_context(|| format!("delivery fixture {index} omitted its receipt"))?;
        sent.push((prepared, receipt));
    }
    Ok(sent)
}

async fn open_delivery(
    handle: &mut DurableHandle,
    fixture: FixtureIdentity,
    subscription_id: SubscriptionId,
    target: NodeId,
    namespace_digest: [u8; 32],
    sent: &(PreparedDurableSend, DurableReceipt),
) -> Result<Delivery> {
    ensure!(
        matches!(
            handle
                .create_subscription(
                    subscription_id,
                    target,
                    fixture.partition_id,
                    namespace_digest,
                    InitialPosition::Exact(sent.1.assigned_offset()),
                )
                .await?,
            SubscriptionCreationOutcome::Created(_)
        ),
        "delivery subscription was not created"
    );
    require_delivery(
        handle
            .next_delivery(subscription_id, 1_000, DurableDeliveryClock::Trusted)
            .await?,
        sent,
    )
}

fn require_delivery(
    poll: DurablePoll,
    sent: &(PreparedDurableSend, DurableReceipt),
) -> Result<Delivery> {
    let DurablePoll::Delivery(delivery) = poll else {
        anyhow::bail!("expected one delivery")
    };
    ensure!(
        delivery.handle().partition() == sent.1.partition()
            && delivery.handle().offset() == sent.1.assigned_offset()
            && delivery.handle().message_id() == sent.0.message_id()
            && delivery.handle().envelope_digest() == sent.0.envelope_digest()
            && delivery.handle().generation() == sent.0.generation()
            && delivery.handle().target() == sent.0.target()
            && delivery.canonical_bytes() == sent.0.canonical_bytes()
            && delivery.handle().delivery_attempt() > 0
            && delivery.handle().owner_epoch() > 0,
        "delivery handle was not bound to the exact stored envelope and owner"
    );
    Ok(delivery)
}

fn assert_irrevocable(delivery: &mut Delivery, expected: DeliveryHandleState) -> Result<()> {
    ensure!(
        delivery.handle().state() == expected,
        "winner state drifted"
    );
    ensure!(
        matches!(
            delivery.handle_mut().begin_ack(),
            Err(HandleTransitionError::Terminal(actual)) if actual == expected
        ) && matches!(
            delivery.handle_mut().release(),
            Err(HandleTransitionError::Terminal(actual)) if actual == expected
        ) && matches!(
            delivery.handle_mut().timeout(),
            Err(HandleTransitionError::Terminal(actual)) if actual == expected
        ) && matches!(
            delivery.handle_mut().shutdown_fence(),
            Err(HandleTransitionError::Terminal(actual)) if actual == expected
        ) && delivery.handle().state() == expected,
        "a losing terminal event revived or replaced the winner"
    );
    Ok(())
}

const fn subscription_id(fill: u8) -> SubscriptionId {
    SubscriptionId::from_bytes([fill; 16])
}
