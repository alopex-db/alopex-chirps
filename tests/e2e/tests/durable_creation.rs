mod durable_owner;
mod task_6_5_support;
mod task_6_6_support;

use alopex_chirps::{
    DurableDeliveryClock, DurableHandle, DurablePoll, DurableSubscriptionError, NodeId,
};
use alopex_chirps_core::durable::{
    ConfirmationBoundary, CreationFailureKind, DurableSendOutcome, SubscriptionCreationOutcome,
    SubscriptionId,
};
use anyhow::{Context, Result, ensure};
use chirps_e2e::v07::{
    EvidenceSink, OracleAppendObserver, RUNTIME_USERNAME, ServerProcess, VerifiedArtifact,
    connect_fixture_with_observer, read_oracle_append_evidence,
};
use chirps_fault_oracle::{CreationCorpusOracle, OracleStore, materialize_creation_case};
use iggy::prelude::IggyExpiry;
use std::fs;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use task_6_5_support::{
    RuntimeCredentials, assert_clean_stop, bootstrap_fixture, require_production_artifact,
};
use task_6_6_support::{
    captured_observation, corpus_paths, hex, initial_position, materialization, recovery_binding,
    subscription_directory, subscription_id,
};
use tokio::time::Instant;

const CASES: &[&str] = &[
    "write-old",
    "file-sync-old",
    "rename-old-unknown",
    "rename-new-unknown",
    "directory-sync-old-unknown",
    "directory-sync-new-unknown",
    "directory-sync-new",
    "missing-journal",
];
const PREFILL_RECORDS: u64 = 3;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run through scripts/run-v07-lane.sh with an attested production server"]
async fn creation_corpus_replays_in_fresh_processes() -> Result<()> {
    let Some(case_name) = std::env::var_os("CHIRPS_CREATION_CASE") else {
        let executable = std::env::current_exe()?;
        for case_name in CASES {
            let status = Command::new(&executable)
                .args([
                    "--exact",
                    "creation_corpus_replays_in_fresh_processes",
                    "--ignored",
                    "--nocapture",
                ])
                .env("CHIRPS_CREATION_CASE", case_name)
                .status()
                .with_context(|| format!("launch fresh creation process for {case_name}"))?;
            ensure!(status.success(), "creation child failed for {case_name}");
        }
        return Ok(());
    };
    run_case(&case_name.to_string_lossy()).await
}

async fn run_case(case_name: &str) -> Result<()> {
    let artifact = VerifiedArtifact::from_runner_environment()?;
    require_production_artifact(&artifact)?;
    let mut evidence = EvidenceSink::for_target(artifact.clone(), "durable_creation")?;
    let mut server = ServerProcess::new(artifact.binary.clone())?;
    let fixture = bootstrap_fixture(
        &mut server,
        &artifact,
        &format!("chirps-v07-creation-{case_name}"),
        &format!("durable-creation-{case_name}"),
        IggyExpiry::NeverExpire,
    )
    .await
    .context("connect creation prefill facade")?;
    server.start(true, None).await?;

    let oracle_directory = tempfile::tempdir()?;
    let oracle = OracleStore::new(oracle_directory.path().join("oracle.log"));
    let observer = Arc::new(OracleAppendObserver::new(oracle.clone()));
    let source = NodeId::new();
    let target = NodeId::new();
    let prefill_root = tempfile::tempdir()?;
    let mut prefill = connect_fixture_with_observer(
        &server,
        fixture,
        prefill_root.path(),
        source,
        RUNTIME_USERNAME,
        &RuntimeCredentials,
        observer.clone(),
    )
    .await
    .context("connect creation prefill facade")?;
    for index in 0..PREFILL_RECORDS {
        let offset = append_record(&mut prefill, target, case_name, index).await?;
        ensure!(
            offset == index,
            "creation prefill offset was not contiguous"
        );
    }
    shutdown_runtime(&mut prefill).await?;
    ensure!(
        !observer.failed()
            && read_oracle_append_evidence(&oracle)?.len() == PREFILL_RECORDS as usize,
        "creation prefill did not leave exact oracle evidence"
    );
    let observation = captured_observation(&server, fixture, PREFILL_RECORDS).await?;

    let checkpoints = tempfile::tempdir()?;
    let (corpus, requirements, design, source_root) = corpus_paths()?;
    let tag = 0x40
        + CASES
            .iter()
            .position(|value| *value == case_name)
            .context("unknown creation corpus case")? as u8;
    let subscription_id = subscription_id(tag);
    let namespace_digest = [tag; 32];
    let selection = initial_position(case_name);
    let expected_initial = observation.resolve_initial(selection)?;
    let captured_end_exclusive = observation.end_exclusive();
    let directory = subscription_directory(checkpoints.path(), subscription_id);
    let frozen_case = if case_name == "missing-journal" {
        "directory-sync-new"
    } else {
        case_name
    };
    let (inputs, binding, _) = materialization(
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
        materialize_creation_case(&corpus, frozen_case, &directory, inputs, binding)?;
    if case_name == "missing-journal" {
        fs::remove_file(directory.join("checkpoint.journal"))?;
    }

    let mut handle = connect_fixture_with_observer(
        &server,
        fixture,
        checkpoints.path(),
        source,
        RUNTIME_USERNAME,
        &RuntimeCredentials,
        observer.clone(),
    )
    .await
    .context("connect recovered creation facade")?;
    let recovery = recovery_binding(&directory, subscription_id, selection)?;
    let outcome = handle
        .recover_subscription(
            subscription_id,
            target,
            fixture.partition_id,
            namespace_digest,
            recovery,
        )
        .await;

    if case_name == "missing-journal" {
        ensure!(
            matches!(outcome, Err(DurableSubscriptionError::InvalidState)),
            "missing checkpoint journal did not fail closed: {outcome:?}"
        );
        ensure!(
            !directory.join("checkpoint.journal").exists(),
            "recovery silently initialized a missing journal"
        );
        evidence.record("missing-journal", "fail-closed")?;
        shutdown_runtime(&mut handle).await?;
    } else {
        let resolves_new = matches!(
            materialized.oracle(),
            CreationCorpusOracle::UnknownNew | CreationCorpusOracle::New
        );
        if resolves_new {
            let SubscriptionCreationOutcome::Created(created) = outcome? else {
                anyhow::bail!("new creation image did not recover as Created")
            };
            ensure!(
                created.subscription_id() == subscription_id
                    && created.partition() == fixture.partition_id
                    && created.owner_epoch() == 2,
                "created binding disagrees with the materialized namespace"
            );
            assert_saved_initial(
                &mut handle,
                subscription_id,
                expected_initial,
                captured_end_exclusive,
            )
            .await?;
            shutdown_runtime(&mut handle).await?;
            drop(handle);

            let append_root = tempfile::tempdir()?;
            let mut append_after_restart = connect_fixture_with_observer(
                &server,
                fixture,
                append_root.path(),
                source,
                RUNTIME_USERNAME,
                &RuntimeCredentials,
                observer.clone(),
            )
            .await
            .context("connect creation append-restart facade")?;
            ensure!(
                append_record(
                    &mut append_after_restart,
                    target,
                    case_name,
                    PREFILL_RECORDS,
                )
                .await?
                    == PREFILL_RECORDS,
                "post-restart append did not extend the captured frontier once"
            );
            shutdown_runtime(&mut append_after_restart).await?;
            ensure!(
                !observer.failed()
                    && read_oracle_append_evidence(&oracle)?.len()
                        == (PREFILL_RECORDS + 1) as usize,
                "post-restart append did not extend exact oracle evidence once"
            );

            let mut reopened = connect_fixture_with_observer(
                &server,
                fixture,
                checkpoints.path(),
                source,
                RUNTIME_USERNAME,
                &RuntimeCredentials,
                observer.clone(),
            )
            .await
            .context("connect creation reopen facade")?;
            ensure!(
                matches!(
                    reopened
                        .reopen_subscription(
                            subscription_id,
                            target,
                            fixture.partition_id,
                            namespace_digest,
                        )
                        .await?,
                    SubscriptionCreationOutcome::Created(_)
                ),
                "fresh runtime did not reopen the recovered creation"
            );
            let advanced = captured_observation(&server, fixture, PREFILL_RECORDS + 1).await?;
            assert_saved_initial(
                &mut reopened,
                subscription_id,
                expected_initial,
                advanced.end_exclusive(),
            )
            .await?;
            shutdown_runtime(&mut reopened).await?;
        } else {
            ensure!(
                matches!(
                    outcome?,
                    SubscriptionCreationOutcome::CreationNotCommitted(
                        CreationFailureKind::StorageUnavailable
                    )
                ),
                "old creation image did not remain known-old"
            );
            shutdown_runtime(&mut handle).await?;
        }
        let journal = materialized
            .journal_sha256()
            .map(|digest| hex(&digest))
            .unwrap_or_else(|| "absent".to_owned());
        let image = materialized
            .image_sha256()
            .map(|digest| hex(&digest))
            .unwrap_or_else(|| "absent".to_owned());
        evidence.record(
            &format!(
                "{case_name}:creation={}:image={image}:journal={journal}",
                hex(&materialized.creation_sha256())
            ),
            if resolves_new {
                "resolved-new"
            } else {
                "resolved-old"
            },
        )?;
        evidence.record(
            &format!("{case_name}:saved-e0={expected_initial}"),
            match selection {
                alopex_chirps_core::durable::InitialPosition::EarliestRetained => "earliest",
                alopex_chirps_core::durable::InitialPosition::LatestAfterCapturedEnd => "latest",
                alopex_chirps_core::durable::InitialPosition::Exact(_) => "exact",
            },
        )?;
    }

    assert_clean_stop(
        server.stop(Instant::now() + Duration::from_secs(5)).await?,
        case_name,
    )?;
    Ok(())
}

async fn append_record(
    handle: &mut DurableHandle,
    target: NodeId,
    case_name: &str,
    sequence: u64,
) -> Result<u64> {
    let prepared = handle.prepare(
        target,
        format!("creation-{case_name}-{sequence}").into_bytes(),
        format!("creation record {sequence}").as_bytes(),
    )?;
    let result = handle
        .send(&prepared, ConfirmationBoundary::OsSyncedAccepted)
        .await?;
    ensure!(
        result.outcome() == DurableSendOutcome::OsSyncedAccepted,
        "creation fixture record was not strongly accepted"
    );
    Ok(result
        .receipt()
        .context("creation fixture record omitted its strong receipt")?
        .assigned_offset())
}

async fn assert_saved_initial(
    handle: &mut DurableHandle,
    subscription_id: SubscriptionId,
    expected_initial: u64,
    observed_end_exclusive: u64,
) -> Result<()> {
    match handle
        .next_delivery(subscription_id, 10_000, DurableDeliveryClock::Trusted)
        .await?
    {
        DurablePoll::Tail => ensure!(
            expected_initial == observed_end_exclusive,
            "saved initial position was unexpectedly observed as tail"
        ),
        DurablePoll::Delivery(mut delivery) => {
            ensure!(
                delivery.handle().offset() == expected_initial,
                "public delivery reevaluated or changed the saved initial position"
            );
            handle.release(subscription_id, delivery.handle_mut())?;
        }
        DurablePoll::IdentityNotCommitted => {
            anyhow::bail!("saved initial delivery identity did not commit")
        }
    }
    Ok(())
}

async fn shutdown_runtime(handle: &mut DurableHandle) -> Result<()> {
    let report = handle
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await?;
    ensure!(report.transport_closed() && report.workers_joined());
    Ok(())
}
