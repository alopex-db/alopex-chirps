use super::{TrustedClock, connect_local_state};
use alopex_chirps::{
    DurableCapacityConfig, DurableCapacityLimit, DurableClockReading, DurableClockSource,
    DurableClockTrust, DurableCompactionOutcome, DurableDeliveryClock, DurablePoll,
    DurableStateCategory, DurableSubscriptionError, NodeId,
};
use alopex_chirps_core::durable::{
    CheckpointOutcome, ConfirmationBoundary, DurableSendOutcome, InitialPosition,
    SubscriptionCreationOutcome, SubscriptionId,
};
use anyhow::{Context, Result, ensure};
use chirps_e2e::v07::{EvidenceSink, ServerProcess, VerifiedArtifact};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;
use tokio::time::Instant;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run through scripts/run-v07-lane.sh with an attested production server"]
async fn every_hard_bound_reserve_and_identity_horizon_fails_closed() -> Result<()> {
    let artifact = VerifiedArtifact::from_runner_environment()?;
    super::task_6_5_support::require_production_artifact(&artifact)?;
    let mut evidence = EvidenceSink::for_target(artifact.clone(), "durable_capacity")?;
    let mut server = ServerProcess::new(artifact.binary.clone())?;
    let fixture = super::task_6_5_support::bootstrap_fixture(
        &mut server,
        &artifact,
        "chirps-v07-capacity",
        "durable-capacity",
        iggy::prelude::IggyExpiry::NeverExpire,
    )
    .await?;
    server.start(true, None).await?;

    for (lane, journal_limit, checkpoint_reserve, compaction_reserve) in [
        (
            "reserve-count",
            DurableCapacityLimit::new(1, 1_000_000),
            DurableCapacityLimit::new(1, 1),
            DurableCapacityLimit::new(1, 1),
        ),
        (
            "reserve-bytes",
            DurableCapacityLimit::new(10, 1),
            DurableCapacityLimit::new(1, 1),
            DurableCapacityLimit::new(1, 1),
        ),
    ] {
        let root = tempfile::tempdir()?;
        ensure!(
            connect_local_state(
                &server,
                fixture,
                root.path(),
                NodeId::new(),
                DurableCapacityConfig::uniform(
                    DurableCapacityLimit::new(1_000, 1_000_000),
                    checkpoint_reserve,
                    compaction_reserve,
                )
                .with_limit(DurableStateCategory::CheckpointJournal, journal_limit),
                Arc::new(TrustedClock(10_000)),
            )
            .await
            .is_err(),
            "{lane} admitted a runtime without both startup reserves"
        );
        evidence.record(lane, "startup-unavailable")?;
    }

    let source = NodeId::new();
    let target = NodeId::new();
    let seed_root = tempfile::tempdir()?;
    let mut seed = connect_local_state(
        &server,
        fixture,
        seed_root.path(),
        source,
        generous_capacity(),
        Arc::new(TrustedClock(10_000)),
    )
    .await?;
    let prepared = seed.prepare(target, b"capacity".to_vec(), b"capacity-payload")?;
    let sent = seed
        .send(&prepared, ConfirmationBoundary::OsSyncedAccepted)
        .await?;
    ensure!(sent.outcome() == DurableSendOutcome::OsSyncedAccepted);
    let receipt = sent
        .receipt()
        .cloned()
        .context("capacity receipt missing")?;
    seed.shutdown(Instant::now() + Duration::from_secs(3))
        .await?;

    for (category, limit, operation, should_reject) in [
        (
            DurableStateCategory::Payload,
            DurableCapacityLimit::new(8, 1),
            BoundOperation::Send,
            true,
        ),
        (
            DurableStateCategory::InFlight,
            DurableCapacityLimit::new(1, 1),
            BoundOperation::Poll,
            false,
        ),
        (
            DurableStateCategory::ProcessedIdentity,
            DurableCapacityLimit::new(8, 117),
            BoundOperation::Poll,
            true,
        ),
        (
            DurableStateCategory::Queue,
            DurableCapacityLimit::new(8, 127),
            BoundOperation::Create,
            true,
        ),
        (
            DurableStateCategory::Concurrency,
            DurableCapacityLimit::new(1, 1),
            BoundOperation::Send,
            false,
        ),
    ] {
        let root = tempfile::tempdir()?;
        let mut handle = connect_local_state(
            &server,
            fixture,
            root.path(),
            source,
            generous_capacity().with_limit(category, limit),
            Arc::new(TrustedClock(10_000)),
        )
        .await?;
        let rejected = match operation {
            BoundOperation::Send => handle
                .send(&prepared, ConfirmationBoundary::OsSyncedAccepted)
                .await
                .is_err(),
            BoundOperation::Create => handle
                .create_subscription(
                    subscription_id(category as u8 + 0x80),
                    target,
                    fixture.partition_id,
                    [category as u8; 32],
                    InitialPosition::Exact(receipt.assigned_offset()),
                )
                .await
                .is_err(),
            BoundOperation::Poll => {
                let subscription = subscription_id(category as u8 + 0x80);
                ensure!(matches!(
                    handle
                        .create_subscription(
                            subscription,
                            target,
                            fixture.partition_id,
                            [category as u8; 32],
                            InitialPosition::Exact(receipt.assigned_offset()),
                        )
                        .await?,
                    SubscriptionCreationOutcome::Created(_)
                ));
                match handle
                    .next_delivery(subscription, 1_000, DurableDeliveryClock::Trusted)
                    .await
                {
                    Ok(DurablePoll::Delivery(mut delivery)) => {
                        ensure!(!should_reject, "{category:?} exceeded its hard bound");
                        let usage = handle
                            .local_state_status()?
                            .capacity()
                            .iter()
                            .find(|usage| usage.category() == category)
                            .context("bounded category usage missing")?
                            .count();
                        ensure!(usage == 1, "{category:?} accounting was not exact");
                        if category == DurableStateCategory::InFlight {
                            let saturated = handle.local_state_status()?;
                            ensure!(
                                !saturated.admission_open() && !saturated.poll_open(),
                                "in-flight saturation left admission or polling open"
                            );
                            ensure!(
                                matches!(
                                    handle
                                        .next_delivery(
                                            subscription,
                                            1_000,
                                            DurableDeliveryClock::Trusted,
                                        )
                                        .await,
                                    Err(DurableSubscriptionError::Unavailable)
                                ),
                                "in-flight saturation admitted another poll"
                            );
                        }
                        handle.release(subscription, delivery.handle_mut())?;
                        if category == DurableStateCategory::InFlight {
                            let released = handle.local_state_status()?;
                            ensure!(
                                released.admission_open() && released.poll_open(),
                                "releasing the in-flight token did not reopen capacity"
                            );
                        }
                        false
                    }
                    Err(DurableSubscriptionError::Unavailable) => true,
                    other => anyhow::bail!("unexpected {category:?} bound result: {other:?}"),
                }
            }
        };
        ensure!(
            rejected == should_reject,
            "{category:?} hard-bound acceptance was incorrect"
        );
        let verdict = if should_reject {
            "overflow-rejected-no-live-eviction"
        } else if category == DurableStateCategory::InFlight {
            "exact-bound-saturated-next-poll-rejected"
        } else {
            "exact-bound-admitted-no-overflow"
        };
        evidence.record(category_name(category), verdict)?;
        handle
            .shutdown(Instant::now() + Duration::from_secs(3))
            .await?;
    }

    let checkpoint_root = tempfile::tempdir()?;
    let checkpoint_limit = DurableCapacityLimit::new(3, 131_328);
    let mut checkpointed = connect_local_state(
        &server,
        fixture,
        checkpoint_root.path(),
        source,
        generous_capacity().with_limit(DurableStateCategory::CheckpointJournal, checkpoint_limit),
        Arc::new(TrustedClock(10_000)),
    )
    .await?;
    let checkpoint_subscription = subscription_id(0x90);
    checkpointed
        .create_subscription(
            checkpoint_subscription,
            target,
            fixture.partition_id,
            [0x90; 32],
            InitialPosition::Exact(receipt.assigned_offset()),
        )
        .await?;
    let DurablePoll::Delivery(mut delivery) = checkpointed
        .next_delivery(
            checkpoint_subscription,
            1_000,
            DurableDeliveryClock::Trusted,
        )
        .await?
    else {
        anyhow::bail!("checkpoint capacity fixture was not delivered")
    };
    ensure!(
        checkpointed.ack(checkpoint_subscription, delivery.handle_mut())?
            == CheckpointOutcome::CheckpointCommitted
    );
    let full = checkpointed.local_state_status()?;
    ensure!(
        !full.admission_open() && full.identity_count() == 1,
        "checkpoint saturation evicted live recovery state"
    );
    ensure!(matches!(
        checkpointed.compact_local_state(receipt.assigned_offset() + 1)?,
        DurableCompactionOutcome::Committed { collected: 1, .. }
    ));
    ensure!(checkpointed.local_state_status()?.admission_open());
    evidence.record(
        "checkpoint-journal-bound",
        "closed-until-durable-compaction",
    )?;
    checkpointed
        .shutdown(Instant::now() + Duration::from_secs(3))
        .await?;

    verify_identity_horizon(
        &server,
        fixture,
        source,
        target,
        &prepared,
        receipt.assigned_offset(),
        &mut evidence,
    )
    .await?;

    super::task_6_5_support::assert_clean_stop(
        server.stop(Instant::now() + Duration::from_secs(5)).await?,
        "durable-capacity",
    )?;
    Ok(())
}

async fn verify_identity_horizon(
    server: &ServerProcess,
    fixture: chirps_e2e::v07::FixtureIdentity,
    source: NodeId,
    target: NodeId,
    prepared: &alopex_chirps_core::durable::PreparedDurableSend,
    offset: u64,
    evidence: &mut EvidenceSink,
) -> Result<()> {
    let root = tempfile::tempdir()?;
    let clock = Arc::new(ManualClock::trusted(2_000));
    let mut handle = connect_local_state(
        server,
        fixture,
        root.path(),
        source,
        generous_capacity(),
        clock.clone(),
    )
    .await?;
    let subscription = subscription_id(0x91);
    handle
        .create_subscription(
            subscription,
            target,
            fixture.partition_id,
            [0x91; 32],
            InitialPosition::Exact(offset),
        )
        .await?;
    let DurablePoll::Delivery(mut delivery) = handle
        .next_delivery(subscription, 1_000, DurableDeliveryClock::Trusted)
        .await?
    else {
        anyhow::bail!("identity horizon fixture was not delivered")
    };
    ensure!(delivery.handle().message_id() == prepared.message_id());

    ensure!(matches!(
        handle.compact_local_state(offset + 1)?,
        DurableCompactionOutcome::Committed { collected: 0, .. }
    ));
    ensure!(handle.local_state_status()?.identity_count() == 1);
    evidence.record("identity-horizon-checkpoint", "uncheckpointed-retained")?;

    ensure!(
        handle.ack(subscription, delivery.handle_mut())? == CheckpointOutcome::CheckpointCommitted
    );
    ensure!(matches!(
        handle.compact_local_state(offset)?,
        DurableCompactionOutcome::Committed { collected: 0, .. }
    ));
    ensure!(handle.local_state_status()?.identity_count() == 1);
    evidence.record(
        "identity-horizon-oldest",
        "offset-not-before-oldest-retained",
    )?;

    clock.set(999, DurableClockTrust::Trusted);
    ensure!(matches!(
        handle.compact_local_state(offset + 1)?,
        DurableCompactionOutcome::Committed { collected: 0, .. }
    ));
    ensure!(handle.local_state_status()?.identity_count() == 1);
    evidence.record("identity-horizon-retry-age", "unelapsed-retained")?;

    clock.set(2_000, DurableClockTrust::RollbackDetected);
    ensure!(matches!(
        handle.compact_local_state(offset + 1)?,
        DurableCompactionOutcome::Committed { collected: 0, .. }
    ));
    let rollback = handle.local_state_status()?;
    ensure!(rollback.identity_count() == 1 && !rollback.poll_open());
    evidence.record("identity-horizon-clock", "rollback-retained-poll-stopped")?;

    clock.set(2_000, DurableClockTrust::Trusted);
    ensure!(matches!(
        handle.compact_local_state(offset + 1)?,
        DurableCompactionOutcome::Committed { collected: 1, .. }
    ));
    let collected = handle.local_state_status()?;
    ensure!(collected.identity_count() == 0 && collected.poll_open());
    evidence.record("identity-horizon-all-three", "durably-collected")?;
    evidence.record("post-horizon-dedup", "not-promised")?;
    handle
        .shutdown(Instant::now() + Duration::from_secs(3))
        .await?;
    Ok(())
}

#[derive(Clone, Copy)]
enum BoundOperation {
    Send,
    Create,
    Poll,
}

fn generous_capacity() -> DurableCapacityConfig {
    DurableCapacityConfig::uniform(
        DurableCapacityLimit::new(1_000, 1_000_000),
        DurableCapacityLimit::new(1, 64 * 1024),
        DurableCapacityLimit::new(1, 64 * 1024),
    )
}

fn subscription_id(tag: u8) -> SubscriptionId {
    SubscriptionId::from_bytes([tag; 16])
}

const fn category_name(category: DurableStateCategory) -> &'static str {
    match category {
        DurableStateCategory::Payload => "capacity-payload",
        DurableStateCategory::InFlight => "capacity-in-flight",
        DurableStateCategory::CheckpointJournal => "capacity-checkpoint-journal",
        DurableStateCategory::ProcessedIdentity => "capacity-processed-identity",
        DurableStateCategory::Queue => "capacity-queue",
        DurableStateCategory::Concurrency => "capacity-concurrency",
    }
}

struct ManualClock {
    millis: AtomicU64,
    trust: AtomicU8,
}

impl ManualClock {
    fn trusted(millis: u64) -> Self {
        Self {
            millis: AtomicU64::new(millis),
            trust: AtomicU8::new(0),
        }
    }

    fn set(&self, millis: u64, trust: DurableClockTrust) {
        self.millis.store(millis, Ordering::Release);
        self.trust.store(
            match trust {
                DurableClockTrust::Trusted => 0,
                DurableClockTrust::RollbackDetected => 1,
                DurableClockTrust::Unknown => 2,
            },
            Ordering::Release,
        );
    }
}

impl DurableClockSource for ManualClock {
    fn read(&self) -> DurableClockReading {
        let trust = match self.trust.load(Ordering::Acquire) {
            0 => DurableClockTrust::Trusted,
            1 => DurableClockTrust::RollbackDetected,
            _ => DurableClockTrust::Unknown,
        };
        DurableClockReading::new(self.millis.load(Ordering::Acquire), trust)
    }
}
