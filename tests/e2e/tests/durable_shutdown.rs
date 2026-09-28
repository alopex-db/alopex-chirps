mod durable_retention_window;
mod task_6_5_support;

use alopex_chirps::NodeId;
use alopex_chirps_core::durable::{
    ConfirmationBoundary, DurableSendOutcome, LifecyclePhase, Readiness, UnavailableReason,
};
use anyhow::{Context, Result, bail, ensure};
use chirps_e2e::v07::{
    EvidenceSink, OracleAppendObserver, ServerProcess, VerifiedArtifact,
    connect_fixture_with_observer, read_oracle_append_evidence,
};
use chirps_fault_oracle::OracleStore;
use iggy::prelude::IggyExpiry;
use std::sync::Arc;
use std::time::Duration;
use task_6_5_support::{
    RuntimeCredentials, assert_clean_stop, bootstrap_fixture, require_production_artifact,
};
use tokio::time::{Instant, timeout_at};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run through scripts/run-v07-lane.sh with an attested production server"]
async fn blocked_response_is_closed_joined_and_reported_once() -> Result<()> {
    let artifact = VerifiedArtifact::from_runner_environment()?;
    require_production_artifact(&artifact)?;
    let mut evidence = EvidenceSink::for_target(artifact.clone(), "durable_shutdown")?;
    let mut server = ServerProcess::new(artifact.binary.clone())?;
    let fixture = bootstrap_fixture(
        &mut server,
        &artifact,
        "chirps-v07-shutdown",
        "durable-shutdown",
        IggyExpiry::NeverExpire,
    )
    .await?;
    server.start(true, None).await?;

    let checkpoints = tempfile::tempdir()?;
    let oracle_directory = tempfile::tempdir()?;
    let oracle_store = OracleStore::new(oracle_directory.path().join("oracle.log"));
    let observer = Arc::new(OracleAppendObserver::new(oracle_store.clone()));
    let mut handle = connect_fixture_with_observer(
        &server,
        fixture,
        checkpoints.path(),
        NodeId::new(),
        chirps_e2e::v07::RUNTIME_USERNAME,
        &RuntimeCredentials,
        observer.clone(),
    )
    .await?;
    let trigger = handle
        .shutdown_trigger()
        .context("connected Durable handle omitted its shutdown trigger")?;
    let prepared = handle.prepare(NodeId::new(), b"blocked-response".to_vec(), b"body")?;

    server.signal("-STOP")?;
    server
        .wait_stopped(Instant::now() + Duration::from_secs(1))
        .await?;
    let close_at = Instant::now() + Duration::from_millis(400);
    let completion_deadline = close_at + Duration::from_secs(2);
    let terminal = {
        let mut send = Box::pin(handle.send(&prepared, ConfirmationBoundary::OsSyncedAccepted));
        if let Ok(early) = timeout_at(close_at, &mut send).await {
            bail!("SIGSTOP response I/O completed before the close deadline: {early:?}");
        }
        let close_observed_at = Instant::now();
        ensure!(
            close_observed_at >= close_at,
            "shutdown trigger fired before the blocked-I/O deadline"
        );
        trigger.request_shutdown()?;
        ensure!(
            trigger.is_shutdown_requested(),
            "close trigger was not sticky"
        );
        timeout_at(completion_deadline, &mut send)
            .await
            .context("close trigger did not release blocked response I/O")??
    };
    ensure!(
        matches!(terminal.outcome(), DurableSendOutcome::Indeterminate(_))
            && terminal.receipt().is_none(),
        "closed response I/O did not retain an explicit ambiguous terminal outcome"
    );
    ensure!(!observer.failed(), "shutdown oracle callback failed");
    let oracle = read_oracle_append_evidence(&oracle_store)?;
    ensure!(
        oracle.len() == 1 && oracle[0].matches_prepared(&prepared),
        "shutdown oracle did not bind the admitted append"
    );

    let draining_health = handle.health();
    ensure!(
        draining_health.lifecycle() == LifecyclePhase::Draining
            && draining_health.readiness() == Readiness::Unavailable(UnavailableReason::Shutdown),
        "shutdown trigger did not close lifecycle admission"
    );
    let report = timeout_at(completion_deadline, handle.shutdown(completion_deadline))
        .await
        .context("Durable shutdown exceeded its deadline")??;
    ensure!(
        report.transport_closed() && report.workers_joined(),
        "shutdown returned before socket close and worker join"
    );
    ensure!(
        report.send_operations().len() == 1
            && report.send_operations()[0].outcome() == terminal.outcome(),
        "shutdown replaced the send's existing terminal outcome"
    );
    let repeated = handle
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await?;
    ensure!(repeated == report, "repeated shutdown changed its report");
    let health = handle.health();
    ensure!(
        health.lifecycle() == LifecyclePhase::Closed
            && health.readiness() == Readiness::Unavailable(UnavailableReason::Shutdown),
        "terminal Durable health did not remain closed/unavailable"
    );
    evidence.record("blocked-response-through-deadline", "indeterminate")?;
    evidence.record("socket-close-and-worker-join", "accepted")?;
    evidence.record("repeated-shutdown-report", "identical")?;

    server.signal("-CONT")?;
    assert_clean_stop(
        server.stop(Instant::now() + Duration::from_secs(5)).await?,
        "final",
    )?;
    Ok(())
}
