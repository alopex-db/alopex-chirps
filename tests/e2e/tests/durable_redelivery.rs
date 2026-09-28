use super::task_6_5_support::{
    RuntimeCredentials, assert_clean_stop, bootstrap_fixture, require_production_artifact,
};
use alopex_chirps::{DurableDeliveryClock, DurablePoll, DurableSubscriptionError, NodeId};
use alopex_chirps_core::durable::{
    CheckpointOutcome, ConfirmationBoundary, DeliveryHandleState, DurableSendOutcome,
    InitialPosition, SubscriptionCreationOutcome, SubscriptionId,
};
use anyhow::{Context, Result, ensure};
use chirps_e2e::v07::{
    EvidenceSink, OracleAppendObserver, RUNTIME_USERNAME, ServerProcess, VerifiedArtifact,
    connect_fixture_with_observer, read_oracle_append_evidence,
};
use chirps_fault_oracle::{
    AckObservation, CheckpointObservation, DeliveryObservation, EffectObservation, ExactLocation,
    ObservationKind, OracleObservation, OracleRecord, OracleStore, RecoveryObservation,
    RecoveryState,
};
use iggy::prelude::IggyExpiry;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run through scripts/run-v07-lane.sh with an attested production server"]
async fn effect_before_checkpoint_restart_redelivers_with_fresh_owner_handle() -> Result<()> {
    let artifact = VerifiedArtifact::from_runner_environment()?;
    require_production_artifact(&artifact)?;
    let mut evidence = EvidenceSink::for_target(artifact.clone(), "durable_redelivery")?;
    let mut server = ServerProcess::new(artifact.binary.clone())?;
    let fixture = bootstrap_fixture(
        &mut server,
        &artifact,
        "chirps-v07-redelivery",
        "durable-redelivery",
        IggyExpiry::NeverExpire,
    )
    .await?;
    server.start(true, None).await?;

    let checkpoint_root = tempfile::tempdir()?;
    let oracle_root = tempfile::tempdir()?;
    let mut oracle = OracleStore::new(oracle_root.path().join("oracle.log"));
    let observer = Arc::new(OracleAppendObserver::new(oracle.clone()));
    let source = NodeId::new();
    let target = NodeId::new();
    let subscription_id = SubscriptionId::from_bytes([0x74; 16]);
    let namespace_digest = [0x47; 32];
    let mut first = connect_fixture_with_observer(
        &server,
        fixture,
        checkpoint_root.path(),
        source,
        RUNTIME_USERNAME,
        &RuntimeCredentials,
        observer.clone(),
    )
    .await?;
    let prepared = first.prepare(target, b"effect-before-checkpoint".to_vec(), b"effect-body")?;
    let sent = first
        .send(&prepared, ConfirmationBoundary::OsSyncedAccepted)
        .await?;
    ensure!(
        sent.outcome() == DurableSendOutcome::OsSyncedAccepted,
        "redelivery fixture was not strongly accepted"
    );
    let receipt = sent
        .receipt()
        .cloned()
        .context("redelivery fixture omitted its exact receipt")?;
    ensure!(
        !observer.failed(),
        "redelivery append oracle callback failed"
    );
    let append_evidence = read_oracle_append_evidence(&oracle)?;
    ensure!(
        append_evidence.len() == 1 && append_evidence[0].matches_prepared(&prepared),
        "redelivery oracle did not bind the exact prepared envelope"
    );
    ensure!(
        matches!(
            first
                .create_subscription(
                    subscription_id,
                    target,
                    fixture.partition_id,
                    namespace_digest,
                    InitialPosition::Exact(receipt.assigned_offset()),
                )
                .await?,
            SubscriptionCreationOutcome::Created(_)
        ),
        "redelivery subscription was not created"
    );
    let DurablePoll::Delivery(mut original) = first
        .next_delivery(subscription_id, 1_000, DurableDeliveryClock::Trusted)
        .await?
    else {
        anyhow::bail!("effect fixture was not delivered")
    };
    assert_exact_delivery(&original, &prepared, &receipt)?;
    ensure!(original.handle().delivery_attempt() == 1);

    let records = oracle.load()?;
    let intent = records
        .iter()
        .find_map(|record| match record {
            OracleRecord::Intent(value) => Some(value.clone()),
            OracleRecord::Observation(_) => None,
        })
        .context("redelivery oracle omitted its intent")?;
    let location = ExactLocation::new(
        receipt.resource_epoch(),
        receipt.partition(),
        receipt.assigned_offset(),
        receipt.assigned_index(),
    );
    oracle.append_observation(&OracleObservation::delivery(
        intent.attempt_id(),
        DeliveryObservation::new(
            intent.message_id(),
            intent.envelope_digest(),
            location,
            false,
        ),
    ))?;
    oracle.append_observation(&OracleObservation::effect(
        intent.attempt_id(),
        EffectObservation::new(
            intent.message_id(),
            intent.envelope_digest(),
            location,
            true,
        ),
    ))?;
    evidence.record(
        "application-effect-before-checkpoint",
        "applied-once-uncheckpointed",
    )?;

    original.handle_mut().timeout()?;
    ensure!(original.handle().state() == DeliveryHandleState::TimedOut);
    let shutdown = first
        .shutdown(Instant::now() + Duration::from_secs(3))
        .await?;
    ensure!(shutdown.transport_closed() && shutdown.workers_joined());
    drop(first);
    assert_clean_stop(
        server.stop(Instant::now() + Duration::from_secs(5)).await?,
        "effect-before-checkpoint-restart",
    )?;
    server.start(true, None).await?;

    let mut second = connect_fixture_with_observer(
        &server,
        fixture,
        checkpoint_root.path(),
        source,
        RUNTIME_USERNAME,
        &RuntimeCredentials,
        observer,
    )
    .await?;
    ensure!(
        matches!(
            second
                .reopen_subscription(
                    subscription_id,
                    target,
                    fixture.partition_id,
                    namespace_digest,
                )
                .await?,
            SubscriptionCreationOutcome::Created(_)
        ),
        "fresh runtime did not reopen the uncheckpointed namespace"
    );
    let DurablePoll::Delivery(mut redelivery) = second
        .next_delivery(
            subscription_id,
            9_000,
            DurableDeliveryClock::RollbackDetected,
        )
        .await?
    else {
        anyhow::bail!("effect-before-checkpoint restart did not redeliver")
    };
    assert_exact_delivery(&redelivery, &prepared, &receipt)?;
    ensure!(
        redelivery.handle().delivery_attempt() > original.handle().delivery_attempt()
            && redelivery.handle().owner_epoch() > original.handle().owner_epoch(),
        "fresh runtime reused the stale owner or delivery attempt"
    );
    oracle.append_observation(&OracleObservation::delivery(
        intent.attempt_id(),
        DeliveryObservation::new(
            intent.message_id(),
            intent.envelope_digest(),
            location,
            true,
        ),
    ))?;
    oracle.append_observation(&OracleObservation::recovery(
        intent.attempt_id(),
        RecoveryObservation::new(RecoveryState::Redelivered, Some(location), None),
    ))?;

    ensure!(
        matches!(
            second.ack(subscription_id, original.handle_mut()),
            Err(DurableSubscriptionError::Unavailable)
        ) && original.handle().state() == DeliveryHandleState::TimedOut
            && redelivery.handle().state() == DeliveryHandleState::Open,
        "stale owner/attempt advanced the checkpoint or disturbed the current handle"
    );
    ensure!(
        second.ack(subscription_id, redelivery.handle_mut())?
            == CheckpointOutcome::CheckpointCommitted,
        "fresh redelivery handle did not commit its checkpoint"
    );
    oracle.append_observation(&OracleObservation::ack(
        intent.attempt_id(),
        AckObservation::new(
            intent.message_id(),
            intent.envelope_digest(),
            location,
            true,
        ),
    ))?;
    oracle.append_observation(&OracleObservation::checkpoint(
        intent.attempt_id(),
        CheckpointObservation::new(
            intent.message_id(),
            intent.envelope_digest(),
            location,
            None,
            Some(receipt.assigned_offset()),
            CheckpointOutcome::CheckpointCommitted,
        ),
    ))?;
    ensure!(
        second
            .next_delivery(subscription_id, 9_001, DurableDeliveryClock::Trusted,)
            .await?
            == DurablePoll::Tail,
        "committed fresh redelivery did not advance to tail"
    );

    let records = oracle.load()?;
    let material: Vec<_> = records
        .iter()
        .filter_map(|record| match record {
            OracleRecord::Observation(value) => Some(value),
            OracleRecord::Intent(_) => None,
        })
        .collect();
    ensure!(
        material
            .iter()
            .any(|value| matches!(value.kind(), ObservationKind::Effect(_)))
            && material
                .iter()
                .filter(|value| matches!(value.kind(), ObservationKind::Delivery(_)))
                .count()
                == 2
            && material
                .iter()
                .any(|value| matches!(value.kind(), ObservationKind::Ack(_)))
            && material
                .iter()
                .any(|value| matches!(value.kind(), ObservationKind::Checkpoint(_))),
        "independent oracle omitted the effect/redelivery/checkpoint chain"
    );
    evidence.record(
        "effect-before-checkpoint-redelivery",
        "same-record-fresh-handle-effect-may-repeat",
    )?;
    evidence.record(
        "stale-owner-attempt",
        "checkpoint-unchanged-until-current-ack",
    )?;

    let shutdown = second
        .shutdown(Instant::now() + Duration::from_secs(3))
        .await?;
    ensure!(shutdown.transport_closed() && shutdown.workers_joined());
    assert_clean_stop(
        server.stop(Instant::now() + Duration::from_secs(5)).await?,
        "durable-redelivery",
    )?;
    Ok(())
}

fn assert_exact_delivery(
    delivery: &alopex_chirps_core::durable::Delivery,
    prepared: &alopex_chirps_core::durable::PreparedDurableSend,
    receipt: &alopex_chirps_core::durable::DurableReceipt,
) -> Result<()> {
    ensure!(
        delivery.handle().partition() == receipt.partition()
            && delivery.handle().offset() == receipt.assigned_offset()
            && delivery.handle().message_id() == prepared.message_id()
            && delivery.handle().envelope_digest() == prepared.envelope_digest()
            && delivery.handle().target() == prepared.target()
            && delivery.handle().generation() == prepared.generation()
            && delivery.canonical_bytes() == prepared.canonical_bytes(),
        "delivery differed from the exact strongly accepted record"
    );
    Ok(())
}
