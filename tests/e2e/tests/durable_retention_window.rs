use super::task_6_5_support::{
    RuntimeCredentials, assert_clean_stop, bootstrap_fixture, require_production_artifact,
};
use alopex_chirps::NodeId;
use alopex_chirps_core::durable::{ConfirmationBoundary, DurableSendOutcome, ReplayError};
use anyhow::{Context, Result, ensure};
use chirps_e2e::v07::{
    EvidenceSink, OracleAppendObserver, RUNTIME_PASSWORD, RUNTIME_USERNAME, RealSession,
    ServerProcess, VerifiedArtifact, connect_fixture_with_observer, read_oracle_append_evidence,
};
use chirps_fault_oracle::OracleStore;
use iggy::prelude::IggyExpiry;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, sleep};

const MINIMUM_RECEIPT_HORIZON: Duration = Duration::from_secs(3);
const RETENTION_EXPIRY_MICROS: u64 = 3_000_000;

const RETENTION_ENVIRONMENT: &[(&str, &str)] = &[
    ("IGGY_DATA_MAINTENANCE_MESSAGES_CLEANER_ENABLED", "true"),
    ("IGGY_DATA_MAINTENANCE_MESSAGES_INTERVAL", "100ms"),
    ("IGGY_SYSTEM_SEGMENT_SIZE", "512 B"),
];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run through scripts/run-v07-lane.sh with an attested production server"]
async fn strong_receipt_is_pre_horizon_not_a_post_horizon_presence_claim() -> Result<()> {
    let artifact = VerifiedArtifact::from_runner_environment()?;
    require_production_artifact(&artifact)?;
    let mut evidence = EvidenceSink::for_target(artifact.clone(), "durable_retention_window")?;
    let mut server = ServerProcess::new(artifact.binary.clone())?;
    let fixture = bootstrap_fixture(
        &mut server,
        &artifact,
        "chirps-v07-retention-window",
        "durable-retention-window",
        IggyExpiry::from(RETENTION_EXPIRY_MICROS),
    )
    .await?;
    server
        .start_with_environment(true, None, None, RETENTION_ENVIRONMENT)
        .await?;

    let checkpoints = tempfile::tempdir()?;
    let oracle_directory = tempfile::tempdir()?;
    let oracle_store = OracleStore::new(oracle_directory.path().join("oracle.log"));
    let observer = Arc::new(OracleAppendObserver::new(oracle_store.clone()));
    let mut handle = connect_fixture_with_observer(
        &server,
        fixture,
        checkpoints.path(),
        NodeId::new(),
        RUNTIME_USERNAME,
        &RuntimeCredentials,
        observer.clone(),
    )
    .await?;
    let payload = vec![0x5a; 2_048];
    let prepared = handle.prepare(NodeId::new(), b"retention-window".to_vec(), &payload)?;
    let result = handle
        .send(&prepared, ConfirmationBoundary::OsSyncedAccepted)
        .await?;
    ensure!(
        result.outcome() == DurableSendOutcome::OsSyncedAccepted,
        "pre-horizon append did not return the strong boundary"
    );
    let receipt = result
        .receipt()
        .cloned()
        .context("strong result omitted its exact receipt")?;
    let rotator = handle.prepare(NodeId::new(), b"retention-rotator".to_vec(), b"rotate")?;
    ensure!(
        handle
            .send(&rotator, ConfirmationBoundary::OsSyncedAccepted)
            .await?
            .outcome()
            == DurableSendOutcome::OsSyncedAccepted,
        "retention fixture did not seal the receipt segment"
    );
    let receipt_observed_at = Instant::now();
    let minimum_horizon = receipt_observed_at + MINIMUM_RECEIPT_HORIZON;
    ensure!(!observer.failed(), "retention oracle callback failed");
    let oracle = read_oracle_append_evidence(&oracle_store)?;
    ensure!(
        oracle.len() == 2
            && oracle[0].matches_prepared(&prepared)
            && oracle[1].matches_prepared(&rotator),
        "retention oracle did not bind the exact receipt and rotator envelopes"
    );

    ensure!(
        Instant::now() < minimum_horizon,
        "receipt fixture reached retention eligibility before crash injection"
    );
    assert_clean_stop(
        server
            .force_stop(Instant::now() + Duration::from_secs(2))
            .await?,
        "pre-horizon-crash",
    )?;
    let shutdown_deadline = Instant::now() + Duration::from_secs(2);
    let shutdown = handle.shutdown(shutdown_deadline).await?;
    ensure!(
        shutdown.transport_closed() && shutdown.workers_joined(),
        "crashed-server handle did not close and join"
    );
    ensure!(
        Instant::now() < minimum_horizon,
        "fresh-process restart missed the receipt-relative pre-horizon window"
    );
    server
        .start_with_environment(true, None, None, RETENTION_ENVIRONMENT)
        .await?;

    let mut readback = RealSession::bind(
        server.address(),
        server.certificate_der(),
        RUNTIME_USERNAME,
        RUNTIME_PASSWORD,
        fixture.stream_id,
        fixture.topic_id,
        fixture.partition_id,
        30_000,
    )
    .await?;
    fixture.assert_report(readback.report())?;
    let before = readback.checked_poll(receipt.assigned_offset()).await?;
    let stored = before
        .record()
        .context("strong receipt was absent before the retention horizon")?;
    ensure!(
        before.oldest_available() <= receipt.assigned_offset()
            && receipt.assigned_offset() < before.end_exclusive()
            && stored.offset() == receipt.assigned_offset()
            && stored.message_id() == receipt.message_id()
            && stored.envelope_digest() == receipt.envelope_digest()
            && stored.canonical_bytes() == prepared.canonical_bytes(),
        "pre-horizon receipt did not match exact stored-envelope truth"
    );
    evidence.record("pre-horizon-crash-restart-receipt", "present-and-exact")?;

    while Instant::now() < minimum_horizon {
        let observation = readback.checked_poll(receipt.assigned_offset()).await?;
        ensure!(
            observation.oldest_available() <= receipt.assigned_offset()
                && observation.record().is_some(),
            "receipt was evicted before the three-second receipt-relative horizon"
        );
        sleep(Duration::from_millis(50)).await;
    }

    let eviction_deadline = receipt_observed_at + Duration::from_secs(10);
    let after = loop {
        let observation = readback.checked_poll(receipt.assigned_offset()).await?;
        if observation.oldest_available() > receipt.assigned_offset() {
            break observation;
        }
        ensure!(
            Instant::now() < eviction_deadline,
            "production message cleaner did not cross the bounded retention horizon"
        );
        sleep(Duration::from_millis(100)).await;
    };
    ensure!(
        after.resource_epoch() == before.resource_epoch()
            && after.end_exclusive() == before.end_exclusive()
            && after.oldest_available() > receipt.assigned_offset(),
        "normal retention changed the resource epoch/end or did not advance the retained tail"
    );
    ensure!(
        matches!(
            after.observe(receipt.assigned_offset()),
            Err(ReplayError::RetentionGap {
                expected,
                oldest_available,
            }) if expected == receipt.assigned_offset()
                && oldest_available == after.oldest_available()
        ),
        "post-horizon eviction was not classified as a retention gap"
    );
    evidence.record("post-horizon-legal-eviction", "retention-gap")?;

    readback
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await?;
    assert_clean_stop(
        server.stop(Instant::now() + Duration::from_secs(5)).await?,
        "final",
    )?;
    Ok(())
}
