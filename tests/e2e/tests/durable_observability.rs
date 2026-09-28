mod task_6_5_support;

use alopex_chirps::{DurableDeliveryClock, DurableObservabilityReport, DurablePoll, NodeId};
use alopex_chirps_core::durable::{
    CheckpointOutcome, ConfirmationBoundary, DeliveryHandleState, DurableEventKind,
    DurableSendOutcome, DurableTraceContext, InitialPosition, LifecyclePhase, MetricBoundary,
    MetricFailureStage, MetricOperation, MetricOutcome, PartitionState, Readiness, ResourceEpoch,
    ResourceId, SubscriptionCreationOutcome, SubscriptionId, UnavailableReason,
};
use anyhow::{Context, Result, ensure};
use chirps_e2e::v07::{
    ADMIN_PASSWORD, EvidenceSink, OracleAppendObserver, ROOT_PASSWORD, RUNTIME_PASSWORD,
    RUNTIME_USERNAME, ServerProcess, VerifiedArtifact, connect_fixture_with_observer,
    read_oracle_append_evidence,
};
use chirps_fault_oracle::OracleStore;
use iggy::prelude::IggyExpiry;
use std::sync::Arc;
use std::time::Duration;
use task_6_5_support::{
    RuntimeCredentials, assert_clean_stop, bootstrap_fixture, require_production_artifact,
};
use tokio::time::Instant;

const PAYLOAD_CANARY: &[u8] = b"observability-payload-canary-v07";
const ORDERING_CANARY: &[u8] = b"observability-ordering-canary-v07";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run through scripts/run-v07-lane.sh with an attested production server"]
async fn bounded_observability_correlates_verified_production_transitions() -> Result<()> {
    let artifact = VerifiedArtifact::from_runner_environment()?;
    require_production_artifact(&artifact)?;
    let mut evidence = EvidenceSink::for_target(artifact.clone(), "durable_observability")?;
    let mut server = ServerProcess::new(artifact.binary.clone())?;
    let fixture = bootstrap_fixture(
        &mut server,
        &artifact,
        "chirps-v07-observability",
        "durable-observability",
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
    let subscription_id = SubscriptionId::from_bytes([0x6e; 16]);
    let namespace_digest = [0x6f; 32];
    let resource_epoch = ResourceEpoch::new(
        ResourceId::from_bytes(fixture.resource_id),
        fixture.resource_epoch,
    );
    let mut reports = Vec::new();

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
    assert_active_health(&mut first, fixture.partition_id)?;
    let prepared = first.prepare(target, ORDERING_CANARY.to_vec(), PAYLOAD_CANARY)?;
    let sent = first
        .send(&prepared, ConfirmationBoundary::OsSyncedAccepted)
        .await?;
    ensure!(
        sent.outcome() == DurableSendOutcome::OsSyncedAccepted,
        "production send did not reach its requested boundary"
    );
    let receipt = sent
        .receipt()
        .cloned()
        .context("strong send omitted its exact receipt")?;
    ensure!(
        receipt.resource_epoch() == resource_epoch && receipt.partition() == fixture.partition_id,
        "strong receipt disagreed with the verified resource projection"
    );
    let SubscriptionCreationOutcome::Created(created) = first
        .create_subscription(
            subscription_id,
            target,
            fixture.partition_id,
            namespace_digest,
            InitialPosition::Exact(receipt.assigned_offset()),
        )
        .await?
    else {
        anyhow::bail!("production subscription creation was not committed")
    };
    let DurablePoll::Delivery(mut original) = first
        .next_delivery(subscription_id, 1_000, DurableDeliveryClock::Trusted)
        .await?
    else {
        anyhow::bail!("production record was not delivered")
    };
    ensure!(
        original.handle().owner_epoch() == created.owner_epoch()
            && original.handle().partition() == fixture.partition_id
            && original.handle().offset() == receipt.assigned_offset()
            && original.handle().message_id() == prepared.message_id()
            && original.handle().envelope_digest() == prepared.envelope_digest()
            && original.canonical_bytes() == prepared.canonical_bytes(),
        "initial delivery did not match the production transition"
    );
    let first_owner = original.handle().owner_epoch();
    original.handle_mut().timeout()?;
    ensure!(original.handle().state() == DeliveryHandleState::TimedOut);
    shutdown_handle(&mut first).await?;
    assert_closed_health(&mut first)?;
    reports.push(
        first
            .observability()
            .context("connected Durable handle omitted observability")?,
    );
    drop(first);
    assert_clean_stop(
        server.stop(Instant::now() + Duration::from_secs(5)).await?,
        "observability-fault",
    )?;
    server.start(true, None).await?;

    let mut second = connect_fixture_with_observer(
        &server,
        fixture,
        checkpoint_root.path(),
        source,
        RUNTIME_USERNAME,
        &RuntimeCredentials,
        observer.clone(),
    )
    .await?;
    let SubscriptionCreationOutcome::Created(recovered) = second
        .reopen_subscription(
            subscription_id,
            target,
            fixture.partition_id,
            namespace_digest,
        )
        .await?
    else {
        anyhow::bail!("production subscription did not recover")
    };
    ensure!(recovered.owner_epoch() > first_owner);
    let DurablePoll::Delivery(mut redelivery) = second
        .next_delivery(
            subscription_id,
            2_000,
            DurableDeliveryClock::RollbackDetected,
        )
        .await?
    else {
        anyhow::bail!("uncheckpointed production record was not redelivered")
    };
    ensure!(
        redelivery.handle().owner_epoch() == recovered.owner_epoch()
            && redelivery.handle().delivery_attempt() > original.handle().delivery_attempt()
            && redelivery.handle().offset() == receipt.assigned_offset()
            && redelivery.handle().message_id() == prepared.message_id()
            && redelivery.handle().envelope_digest() == prepared.envelope_digest()
            && redelivery.canonical_bytes() == prepared.canonical_bytes(),
        "redelivery did not use the recovered owner and a fresh attempt"
    );
    ensure!(
        second.ack(subscription_id, redelivery.handle_mut())?
            == CheckpointOutcome::CheckpointCommitted,
        "verified redelivery did not commit its checkpoint"
    );
    ensure!(
        second
            .next_delivery(subscription_id, 2_001, DurableDeliveryClock::Trusted)
            .await?
            == DurablePoll::Tail,
        "committed checkpoint did not advance the production frontier"
    );
    shutdown_handle(&mut second).await?;
    reports.push(
        second
            .observability()
            .context("recovered Durable handle omitted observability")?,
    );
    drop(second);

    let mut rebound = connect_fixture_with_observer(
        &server,
        fixture,
        checkpoint_root.path(),
        source,
        RUNTIME_USERNAME,
        &RuntimeCredentials,
        observer.clone(),
    )
    .await?;
    let SubscriptionCreationOutcome::Created(rebound_binding) = rebound
        .reopen_subscription(
            subscription_id,
            target,
            fixture.partition_id,
            namespace_digest,
        )
        .await?
    else {
        anyhow::bail!("checkpointed production subscription did not rebind")
    };
    ensure!(rebound_binding.owner_epoch() > recovered.owner_epoch());
    ensure!(
        rebound
            .next_delivery(subscription_id, 3_000, DurableDeliveryClock::Trusted)
            .await?
            == DurablePoll::Tail,
        "session rebind did not retain the checkpointed frontier"
    );
    reports.push(
        rebound
            .observability()
            .context("rebound Durable handle omitted observability")?,
    );

    let chain: Vec<_> = reports
        .iter()
        .flat_map(|report| report.partition_events(fixture.partition_id, resource_epoch))
        .map(|event| {
            let DurableTraceContext::Partition { owner_epoch, .. } = event.trace() else {
                unreachable!("partition_events returned an attempt trace")
            };
            (event.kind(), owner_epoch)
        })
        .collect();
    ensure!(
        chain
            == [
                (DurableEventKind::Fault, first_owner),
                (DurableEventKind::Recovery, recovered.owner_epoch()),
                (DurableEventKind::Redelivery, recovered.owner_epoch()),
                (DurableEventKind::Checkpoint, recovered.owner_epoch()),
                (
                    DurableEventKind::SessionRebind,
                    rebound_binding.owner_epoch(),
                ),
            ],
        "bounded events did not reconstruct the verified production chain"
    );
    ensure!(
        reports.iter().all(|report| {
            let snapshot = report.snapshot();
            snapshot.metric_series() <= snapshot.metric_series_limit()
                && snapshot.retained_events() <= snapshot.event_capacity()
                && snapshot.dropped_events() == 0
        }),
        "observability buffers exceeded or lost their bounded evidence"
    );
    for labels in [
        (
            MetricOperation::Send,
            MetricBoundary::OsSyncedAccepted,
            MetricOutcome::Success,
        ),
        (
            MetricOperation::Recovery,
            MetricBoundary::BeforeMutation,
            MetricOutcome::Success,
        ),
        (
            MetricOperation::Poll,
            MetricBoundary::BeforeMutation,
            MetricOutcome::Duplicate,
        ),
        (
            MetricOperation::Checkpoint,
            MetricBoundary::CheckpointInstall,
            MetricOutcome::Success,
        ),
    ] {
        ensure!(
            has_metric(&reports, labels),
            "production metric series was absent"
        );
    }
    assert_no_sensitive_observability(&reports, &prepared, target)?;
    let append_evidence = read_oracle_append_evidence(&oracle)?;
    ensure!(
        !observer.failed()
            && append_evidence.len() == 1
            && append_evidence[0].matches_prepared(&prepared),
        "independent append evidence disagreed with the production transition"
    );
    evidence.record(
        "fault-recovery-redelivery-checkpoint-rebind",
        "production-transitions-correlated",
    )?;
    evidence.record("bounded-secret-negative", "accepted")?;

    shutdown_handle(&mut rebound).await?;
    assert_clean_stop(
        server.stop(Instant::now() + Duration::from_secs(5)).await?,
        "durable-observability",
    )?;
    Ok(())
}

fn has_metric(
    reports: &[DurableObservabilityReport],
    expected: (MetricOperation, MetricBoundary, MetricOutcome),
) -> bool {
    reports.iter().any(|report| {
        report.metric_series().iter().any(|series| {
            let labels = series.labels();
            labels.operation() == expected.0
                && labels.boundary() == expected.1
                && labels.failure_stage() == MetricFailureStage::None
                && labels.outcome() == expected.2
                && series.count() > 0
        })
    })
}

fn assert_active_health(handle: &mut alopex_chirps::DurableHandle, partition: u32) -> Result<()> {
    let health = handle.health();
    ensure!(
        health.lifecycle() == LifecyclePhase::Ready
            && health.readiness() == Readiness::Available
            && health.partitions().len() == 1
            && health.partitions()[0].partition() == partition
            && health.partitions()[0].state() == PartitionState::Active,
        "production Durable health axes were not independently ready"
    );
    Ok(())
}

fn assert_closed_health(handle: &mut alopex_chirps::DurableHandle) -> Result<()> {
    let health = handle.health();
    ensure!(
        health.lifecycle() == LifecyclePhase::Closed
            && health.readiness() == Readiness::Unavailable(UnavailableReason::Shutdown),
        "Durable shutdown was not reflected in bounded health"
    );
    Ok(())
}

fn assert_no_sensitive_observability(
    reports: &[DurableObservabilityReport],
    prepared: &alopex_chirps_core::durable::PreparedDurableSend,
    target: NodeId,
) -> Result<()> {
    let rendered = format!("{reports:?}").to_ascii_lowercase();
    for forbidden in [
        String::from_utf8_lossy(PAYLOAD_CANARY).into_owned(),
        String::from_utf8_lossy(ORDERING_CANARY).into_owned(),
        format!("{:?}", prepared.message_id()).to_ascii_lowercase(),
        format!("{target:?}").to_ascii_lowercase(),
        ROOT_PASSWORD.to_owned(),
        ADMIN_PASSWORD.to_owned(),
        RUNTIME_PASSWORD.to_owned(),
        "permissions".to_owned(),
        "access_token".to_owned(),
        "private_key".to_owned(),
    ] {
        ensure!(
            !rendered.contains(&forbidden),
            "observability output contained a forbidden value class"
        );
    }
    Ok(())
}

async fn shutdown_handle(handle: &mut alopex_chirps::DurableHandle) -> Result<()> {
    let report = handle
        .shutdown(Instant::now() + Duration::from_secs(3))
        .await?;
    ensure!(report.transport_closed() && report.workers_joined());
    Ok(())
}
