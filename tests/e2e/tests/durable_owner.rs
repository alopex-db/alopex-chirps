use super::task_6_5_support::{
    RuntimeCredentials, assert_clean_stop, bootstrap_fixture, require_production_artifact,
};
use super::task_6_6_support::{
    corpus_paths, empty_observation, hex, materialization, recovery_binding,
    subscription_directory, subscription_id,
};
use alopex_chirps::{DurableSubscriptionError, NodeId};
use alopex_chirps_core::durable::{InitialPosition, SubscriptionCreationOutcome};
use anyhow::{Context, Result, anyhow, ensure};
use chirps_e2e::v07::{
    EvidenceSink, OracleAppendObserver, RUNTIME_USERNAME, ServerProcess, VerifiedArtifact,
    connect_fixture_with_observer,
};
use chirps_fault_oracle::{
    CreationState, CreationTransitionObservation, OracleStore, materialize_creation_case,
    read_creation_snapshot, verify_creation_history,
};
use iggy::prelude::IggyExpiry;
use std::fs::OpenOptions;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run through scripts/run-v07-lane.sh with an attested production server"]
async fn owner_lock_chain_and_stale_recovery_are_fenced() -> Result<()> {
    if let Some(path) = std::env::var_os("CHIRPS_OWNER_LOCK_PROBE") {
        let expect_available = std::env::var_os("CHIRPS_OWNER_LOCK_AVAILABLE").is_some();
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let locked = file.try_lock().is_ok();
        ensure!(
            locked == expect_available,
            "cross-process owner lock availability disagreed"
        );
        return Ok(());
    }

    let artifact = VerifiedArtifact::from_runner_environment()?;
    require_production_artifact(&artifact)?;
    let mut evidence = EvidenceSink::for_target(artifact.clone(), "durable_owner")?;
    let mut server = ServerProcess::new(artifact.binary.clone())?;
    let fixture = bootstrap_fixture(
        &mut server,
        &artifact,
        "chirps-v07-owner",
        "durable-owner",
        IggyExpiry::NeverExpire,
    )
    .await?;
    server.start(true, None).await?;
    let observation = empty_observation(&server, fixture).await?;

    let checkpoints = tempfile::tempdir()?;
    let (corpus, requirements, design, source_root) = corpus_paths()?;
    let subscription_id = subscription_id(0x71);
    let target = NodeId::new();
    let namespace_digest = [0x72; 32];
    let selection = InitialPosition::Exact(0);
    let directory = subscription_directory(checkpoints.path(), subscription_id);
    let (inputs, binding, expectation) = materialization(
        &requirements,
        &design,
        &source_root,
        subscription_id,
        target,
        fixture.partition_id,
        namespace_digest,
        selection,
        observation,
    );
    let materialized =
        materialize_creation_case(&corpus, "directory-sync-new", &directory, inputs, binding)?;
    let genesis = read_creation_snapshot(&directory)?;

    let first_root =
        facade_checkpoint_root(checkpoints.path(), "first", subscription_id, &directory)?;
    let mut first = connect(&server, fixture, &first_root, NodeId::new())
        .await
        .context("connect first owner facade")?;
    let recovered = first
        .recover_subscription(
            subscription_id,
            target,
            fixture.partition_id,
            namespace_digest,
            recovery_binding(&directory, subscription_id, selection)?,
        )
        .await?;
    ensure!(
        matches!(recovered, SubscriptionCreationOutcome::Created(binding) if binding.owner_epoch() == 2),
        "first recovery did not install owner epoch two"
    );
    let owner_two = read_creation_snapshot(&directory)?;

    let contender_root =
        facade_checkpoint_root(checkpoints.path(), "contender", subscription_id, &directory)?;
    let mut contender = connect(&server, fixture, &contender_root, NodeId::new())
        .await
        .context("connect contender owner facade")?;
    ensure!(
        matches!(
            contender
                .reopen_subscription(
                    subscription_id,
                    target,
                    fixture.partition_id,
                    namespace_digest,
                )
                .await?,
            SubscriptionCreationOutcome::CreationNotCommitted(
                alopex_chirps_core::durable::CreationFailureKind::OwnerLockUnavailable
            )
        ),
        "a live owner did not fence a second public facade"
    );
    contender
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await?;
    lock_probe(&directory.join(".owner.lock"), false)?;

    first
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await?;
    drop(first);
    lock_probe(&directory.join(".owner.lock"), true)?;

    let reopened_root =
        facade_checkpoint_root(checkpoints.path(), "reopened", subscription_id, &directory)?;
    let mut reopened = connect(&server, fixture, &reopened_root, NodeId::new())
        .await
        .context("connect reopened owner facade")?;
    let outcome = reopened
        .reopen_subscription(
            subscription_id,
            target,
            fixture.partition_id,
            namespace_digest,
        )
        .await?;
    ensure!(
        matches!(outcome, SubscriptionCreationOutcome::Created(binding) if binding.owner_epoch() == 3),
        "fresh facade did not advance the linked owner epoch"
    );
    let owner_three = read_creation_snapshot(&directory)?;
    let verdict = verify_creation_history(&[
        CreationTransitionObservation::new(
            CreationState::Created,
            expectation,
            genesis,
            owner_two.clone(),
        ),
        CreationTransitionObservation::new(
            CreationState::Created,
            expectation,
            owner_two,
            owner_three.clone(),
        ),
    ])
    .map_err(|error| anyhow!("owner oracle rejected linked epochs: {error:?}"))?;
    ensure!(
        verdict.resolved_new() && verdict.final_owner_epoch() == Some(3),
        "independent owner oracle did not prove the linked epoch chain"
    );

    reopened
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await?;
    drop(reopened);
    let owner_two = owner_three
        .current_owner()
        .context("owner three missing")?
        .previous_owner_digest();
    ensure!(
        owner_two != [0; 32],
        "owner chain lost its predecessor digest"
    );

    let stale_root =
        facade_checkpoint_root(checkpoints.path(), "stale", subscription_id, &directory)?;
    let mut stale = connect(&server, fixture, &stale_root, NodeId::new())
        .await
        .context("connect stale owner facade")?;
    let stale_binding = recovery_binding(
        &directory,
        subscription_id,
        InitialPosition::LatestAfterCapturedEnd,
    )?;
    ensure!(
        matches!(
            stale
                .recover_subscription(
                    subscription_id,
                    target,
                    fixture.partition_id,
                    namespace_digest,
                    stale_binding,
                )
                .await,
            Err(DurableSubscriptionError::InvalidState)
        ),
        "stale selection was accepted for the same directory"
    );
    stale
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await?;

    evidence.record(
        &format!(
            "linked-owner:creation={}:journal={}",
            hex(&materialized.creation_sha256()),
            hex(&materialized
                .journal_sha256()
                .context("canonical case omitted journal evidence")?)
        ),
        "owner-epoch-3-stale-fenced",
    )?;
    assert_clean_stop(
        server.stop(Instant::now() + Duration::from_secs(5)).await?,
        "owner",
    )?;
    Ok(())
}

async fn connect(
    server: &ServerProcess,
    fixture: chirps_e2e::v07::FixtureIdentity,
    checkpoint_root: &std::path::Path,
    source: NodeId,
) -> Result<alopex_chirps::DurableHandle> {
    let observer = Arc::new(OracleAppendObserver::new(OracleStore::new(
        checkpoint_root.join("oracle.log"),
    )));
    connect_fixture_with_observer(
        server,
        fixture,
        checkpoint_root,
        source,
        RUNTIME_USERNAME,
        &RuntimeCredentials,
        observer,
    )
    .await
    .map_err(Into::into)
}

fn facade_checkpoint_root(
    root: &std::path::Path,
    name: &str,
    subscription_id: alopex_chirps_core::durable::SubscriptionId,
    directory: &std::path::Path,
) -> Result<std::path::PathBuf> {
    let facade_root = root.join(name);
    std::fs::create_dir(&facade_root)?;
    #[cfg(unix)]
    std::os::unix::fs::symlink(
        directory,
        subscription_directory(&facade_root, subscription_id),
    )?;
    #[cfg(windows)]
    std::os::windows::fs::symlink_dir(
        directory,
        subscription_directory(&facade_root, subscription_id),
    )?;
    Ok(facade_root)
}

fn lock_probe(path: &std::path::Path, available: bool) -> Result<()> {
    // Keep the child harness's result lines out of the parent result stream.
    let output = Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "durable_owner::owner_lock_chain_and_stale_recovery_are_fenced",
            "--ignored",
            "--nocapture",
        ])
        .env("CHIRPS_OWNER_LOCK_PROBE", path)
        .envs(available.then_some(("CHIRPS_OWNER_LOCK_AVAILABLE", "1")))
        .output()
        .context("launch cross-process owner-lock probe")?;
    ensure!(
        output.status.success(),
        "owner-lock probe failed: {}",
        output.status
    );
    Ok(())
}
