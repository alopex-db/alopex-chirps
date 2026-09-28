use super::task_6_5_support::{
    RuntimeCredentials, assert_clean_stop, bootstrap_fixture, require_production_artifact,
    sdk_client,
};
use alopex_chirps::{
    DurableBuildError, DurableBuilder, DurableDeliveryClock, DurableSubscriptionError, NodeId,
};
use alopex_chirps_core::durable::{
    InitialPosition, PartitionState, PollResolution, RecoveryReason, ReplayError,
    SubscriptionCreationOutcome, SubscriptionId,
};
use anyhow::{Context, Result, bail, ensure};
use chirps_e2e::v07::{
    EvidenceSink, FixtureIdentity, OracleAppendObserver, ROOT_PASSWORD, ROOT_USERNAME,
    RUNTIME_PASSWORD, RUNTIME_USERNAME, RealSession, ServerProcess, VerifiedArtifact,
    connect_fixture_with_observer,
};
use chirps_fault_oracle::{OracleStore, read_creation_snapshot};
use iggy::prelude::{
    Identifier, IggyExpiry, IggyMessage, MessageClient, Partitioning, TopicClient,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{Instant, sleep};

const RETENTION_EXPIRY_MICROS: u64 = 1_000_000;
const RETENTION_ENVIRONMENT: &[(&str, &str)] = &[
    ("IGGY_DATA_MAINTENANCE_MESSAGES_CLEANER_ENABLED", "true"),
    ("IGGY_DATA_MAINTENANCE_MESSAGES_INTERVAL", "100ms"),
    ("IGGY_SYSTEM_SEGMENT_SIZE", "512 B"),
];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run through scripts/run-v07-lane.sh with an attested production server"]
async fn normal_retention_advances_only_oldest_and_reports_gap() -> Result<()> {
    let artifact = VerifiedArtifact::from_runner_environment()?;
    require_production_artifact(&artifact)?;
    let mut evidence = EvidenceSink::for_target(artifact.clone(), "durable_retention")?;
    let mut server = ServerProcess::new(artifact.binary.clone())?;
    let fixture = bootstrap_fixture(
        &mut server,
        &artifact,
        "chirps-v07-normal-retention",
        "durable-normal-retention",
        IggyExpiry::from(RETENTION_EXPIRY_MICROS),
    )
    .await?;
    server
        .start_with_environment(true, None, None, RETENTION_ENVIRONMENT)
        .await?;

    let source = NodeId::new();
    let target = NodeId::new();
    let checkpoint_root = tempfile::tempdir()?;
    let oracle_root = tempfile::tempdir()?;
    let observer = Arc::new(OracleAppendObserver::new(OracleStore::new(
        oracle_root.path().join("oracle.log"),
    )));
    let subscription_id = SubscriptionId::from_bytes([0x6b; 16]);
    let mut subscriber = connect_fixture_with_observer(
        &server,
        fixture,
        checkpoint_root.path(),
        source,
        RUNTIME_USERNAME,
        &RuntimeCredentials,
        observer.clone(),
    )
    .await?;
    let SubscriptionCreationOutcome::Created(initial_binding) = subscriber
        .create_subscription(
            subscription_id,
            target,
            fixture.partition_id,
            [0xb6; 32],
            InitialPosition::Exact(0),
        )
        .await?
    else {
        anyhow::bail!("retention subscription was not created at the explicit empty tail")
    };

    let preparer = DurableBuilder::new(source)
        .inbox_generation(1)
        .explicit_partitions(1)
        .build()?;
    let prepared = preparer.prepare(target, b"normal-retention".to_vec(), &[0x5a; 2_048])?;
    let rotator = preparer.prepare(NodeId::new(), b"retention-rotator".to_vec(), &[0xa5; 2_048])?;
    let mut messages = vec![
        IggyMessage::builder()
            .id(u128::from_be_bytes(*prepared.message_id().as_bytes()))
            .payload(prepared.canonical_bytes().to_vec().into())
            .build()?,
    ];
    let sdk = sdk_client(&server, RUNTIME_USERNAME, RUNTIME_PASSWORD).await?;
    sdk.send_messages(
        &Identifier::numeric(fixture.stream_id)?,
        &Identifier::numeric(fixture.topic_id)?,
        &Partitioning::partition_id(fixture.partition_id),
        &mut messages,
    )
    .await?;
    let mut rotator_messages = vec![
        IggyMessage::builder()
            .id(u128::from_be_bytes(*rotator.message_id().as_bytes()))
            .payload(rotator.canonical_bytes().to_vec().into())
            .build()?,
    ];
    sdk.send_messages(
        &Identifier::numeric(fixture.stream_id)?,
        &Identifier::numeric(fixture.topic_id)?,
        &Partitioning::partition_id(fixture.partition_id),
        &mut rotator_messages,
    )
    .await?;
    drop(sdk);

    let mut session = RealSession::bind(
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
    fixture.assert_report(session.report())?;
    let before = session
        .checked_poll(0)
        .await
        .context("poll the retained record before cleaner eviction")?;
    let PollResolution::Record(record) = before.observe(0)? else {
        anyhow::bail!("normal-retention fixture did not start with one checked record")
    };
    ensure!(
        record.message_id() == prepared.message_id()
            && record.envelope_digest() == prepared.envelope_digest()
            && record.canonical_bytes() == prepared.canonical_bytes(),
        "retention fixture record did not match its producer-owned envelope"
    );
    let evicted_offset = record.offset();
    let stable_epoch = before.resource_epoch();
    let stable_end = before.end_exclusive();
    let deadline = Instant::now() + Duration::from_secs(10);
    let observed_after_eviction = loop {
        let observation = session
            .checked_poll(evicted_offset)
            .await
            .context("poll while waiting for cleaner eviction")?;
        if observation.oldest_available() > evicted_offset {
            break observation;
        }
        ensure!(
            Instant::now() < deadline,
            "production cleaner did not advance oldest within the retention deadline"
        );
        sleep(Duration::from_millis(100)).await;
    };
    ensure!(
        observed_after_eviction.oldest_available() > evicted_offset
            && observed_after_eviction.resource_epoch() == stable_epoch
            && observed_after_eviction.end_exclusive() == stable_end
            && observed_after_eviction.record().is_none(),
        "retention observation did not cross the expected offset"
    );
    let stable_oldest = observed_after_eviction.oldest_available();
    session
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await?;
    let shutdown = subscriber
        .shutdown(Instant::now() + Duration::from_secs(3))
        .await?;
    ensure!(shutdown.transport_closed() && shutdown.workers_joined());
    drop(subscriber);
    assert_clean_stop(
        server.stop(Instant::now() + Duration::from_secs(5)).await?,
        "normal-retention-restart",
    )?;
    server
        .start_with_environment(true, None, None, RETENTION_ENVIRONMENT)
        .await?;
    let mut subscriber = connect_fixture_with_observer(
        &server,
        fixture,
        checkpoint_root.path(),
        source,
        RUNTIME_USERNAME,
        &RuntimeCredentials,
        observer,
    )
    .await
    .context("reconnect durable subscriber after cleaner restart")?;
    let SubscriptionCreationOutcome::Created(reopened_binding) = subscriber
        .reopen_subscription(subscription_id, target, fixture.partition_id, [0xb6; 32])
        .await
        .context("reopen retention subscription after cleaner restart")?
    else {
        anyhow::bail!("retention subscription did not recover in the fresh process")
    };
    ensure!(
        reopened_binding.owner_epoch() > initial_binding.owner_epoch(),
        "fresh retention runtime reused the stale local owner epoch"
    );
    let mut session = RealSession::bind(
        server.address(),
        server.certificate_der(),
        RUNTIME_USERNAME,
        RUNTIME_PASSWORD,
        fixture.stream_id,
        fixture.topic_id,
        fixture.partition_id,
        5_000,
    )
    .await
    .context("bind retention poll session after cleaner restart")?;
    fixture.assert_report(session.report())?;
    let after = session.checked_poll(evicted_offset).await?;
    ensure!(
        after.resource_epoch() == stable_epoch
            && after.end_exclusive() == stable_end
            && after.oldest_available() == stable_oldest
            && after.record().is_none(),
        "normal retention changed epoch/end or restored a removed segment after restart: expected \
         epoch={stable_epoch:?}, end={stable_end}, oldest={stable_oldest}; observed epoch={:?}, end={}, \
         oldest={}, record={}",
        after.resource_epoch(),
        after.end_exclusive(),
        after.oldest_available(),
        after.record().is_some(),
    );
    ensure!(
        matches!(
            after.observe(evicted_offset),
            Err(ReplayError::RetentionGap {
                expected,
                oldest_available,
            }) if expected == evicted_offset && oldest_available == after.oldest_available()
        ),
        "expected<oldest was inferred as tail, clamped, or otherwise misclassified"
    );
    ensure!(
        matches!(after.observe(stable_end), Ok(PollResolution::Tail)),
        "retained partition did not expose the immutable end as tail"
    );
    evidence.record(
        "normal-retention-oldest-only-fresh-process",
        "retention-gap-same-epoch",
    )?;
    evidence.record("retained-tail-after-eviction", "expected==h-tail")?;
    ensure!(
        matches!(
            subscriber
                .next_delivery(subscription_id, 1_000, DurableDeliveryClock::Trusted)
                .await,
            Err(DurableSubscriptionError::Unavailable)
        ) && subscriber.health().partitions().iter().any(|partition| {
            matches!(
                partition.state(),
                PartitionState::RecoveryRequired(RecoveryReason::RetentionGap)
            )
        }),
        "public subscriber treated a retention gap as tail or silently advanced"
    );
    evidence.record("public-retention-gap", "recovery-required-no-auto-skip")?;

    session
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await?;
    let shutdown = subscriber
        .shutdown(Instant::now() + Duration::from_secs(3))
        .await?;
    ensure!(shutdown.transport_closed() && shutdown.workers_joined());
    assert_clean_stop(
        server.stop(Instant::now() + Duration::from_secs(5)).await?,
        "normal-retention",
    )?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run through scripts/run-v07-lane.sh with an attested production server"]
async fn destructive_replacement_fences_old_namespace_after_fresh_process() -> Result<()> {
    let artifact = VerifiedArtifact::from_runner_environment()?;
    require_production_artifact(&artifact)?;
    let mut evidence = EvidenceSink::for_target(artifact.clone(), "durable_retention")?;
    let mut server = ServerProcess::new(artifact.binary.clone())?;
    let fixture = bootstrap_fixture(
        &mut server,
        &artifact,
        "chirps-v07-replacement",
        "durable-replacement",
        IggyExpiry::NeverExpire,
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
    let subscription_id = SubscriptionId::from_bytes([0x6a; 16]);
    let namespace_digest = [0xa6; 32];
    let mut old = connect_fixture_with_observer(
        &server,
        fixture,
        checkpoint_root.path(),
        source,
        RUNTIME_USERNAME,
        &RuntimeCredentials,
        observer.clone(),
    )
    .await?;
    ensure!(
        matches!(
            old.create_subscription(
                subscription_id,
                target,
                fixture.partition_id,
                namespace_digest,
                InitialPosition::Exact(0),
            )
            .await?,
            SubscriptionCreationOutcome::Created(_)
        ),
        "old resource namespace was not durably created"
    );
    let directory = checkpoint_root.path().join(hex(subscription_id.as_bytes()));
    let before = read_creation_snapshot(&directory)?;
    let manifest = before
        .manifest()
        .context("creation oracle did not find the old resource manifest")?;
    ensure!(
        manifest.captured_resource_epoch().resource_id().as_bytes() == &fixture.resource_id
            && manifest.captured_resource_epoch().epoch() == fixture.resource_epoch,
        "creation oracle did not bind the original resource epoch"
    );
    let shutdown = old
        .shutdown(Instant::now() + Duration::from_secs(3))
        .await?;
    ensure!(shutdown.transport_closed() && shutdown.workers_joined());
    drop(old);

    let root = sdk_client(&server, ROOT_USERNAME, ROOT_PASSWORD).await?;
    root.purge_topic(
        &Identifier::numeric(fixture.stream_id)?,
        &Identifier::numeric(fixture.topic_id)?,
    )
    .await?;
    drop(root);
    assert_clean_stop(
        server.stop(Instant::now() + Duration::from_secs(5)).await?,
        "post-replacement",
    )?;
    server.start(true, None).await?;

    let mut replacement_session = RealSession::bind(
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
    let replacement_report = replacement_session.report();
    let replacement = replacement_report.location().resource_epoch();
    ensure!(
        replacement_report.location().stream_id() == fixture.stream_id
            && replacement_report.location().topic_id() == fixture.topic_id
            && replacement_report.location().partition_id() == fixture.partition_id
            && replacement.resource_id().as_bytes() != &fixture.resource_id
            && replacement.epoch() > fixture.resource_epoch,
        "fresh process did not expose a fresh identity for the same numeric resource"
    );
    let empty = replacement_session.checked_poll(0).await?;
    ensure!(
        empty.resource_epoch() == replacement
            && empty.end_exclusive() == 0
            && empty.oldest_available() == 0
            && matches!(empty.observe(0), Ok(PollResolution::Tail)),
        "destructive replacement did not expose an atomic empty new-epoch observation"
    );
    evidence.record(
        "destructive-replacement-fresh-process",
        "fresh-epoch-same-location",
    )?;
    evidence.record("destructive-replacement-empty", "oldest==h-tail")?;
    replacement_session
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await?;

    let rejected_root = tempfile::tempdir()?;
    let rejected = connect_fixture_with_observer(
        &server,
        fixture,
        rejected_root.path(),
        NodeId::new(),
        RUNTIME_USERNAME,
        &RuntimeCredentials,
        observer.clone(),
    )
    .await;
    match rejected {
        Err(DurableBuildError::BackendUnavailable) => {}
        Err(error) => bail!("old projected resource epoch returned the wrong error: {error:?}"),
        Ok(mut unexpected) => {
            unexpected
                .shutdown(Instant::now() + Duration::from_secs(2))
                .await?;
            bail!("old projected resource epoch was not fenced before handle construction")
        }
    }
    evidence.record(
        "destructive-replacement-old-binding",
        "fenced-before-handle",
    )?;

    let replacement_fixture = FixtureIdentity {
        resource_id: *replacement.resource_id().as_bytes(),
        resource_epoch: replacement.epoch(),
        ..fixture
    };
    replacement_fixture.assert_report(replacement_report)?;
    let mut fresh = connect_fixture_with_observer(
        &server,
        replacement_fixture,
        checkpoint_root.path(),
        source,
        RUNTIME_USERNAME,
        &RuntimeCredentials,
        observer,
    )
    .await?;
    ensure!(
        matches!(
            fresh
                .reopen_subscription(
                    subscription_id,
                    target,
                    fixture.partition_id,
                    namespace_digest,
                )
                .await,
            Err(DurableSubscriptionError::InvalidState)
        ),
        "old local namespace was auto-upgraded or polled under the replacement epoch"
    );
    let after = read_creation_snapshot(&directory)?;
    ensure!(
        after.manifest().is_some_and(
            |value| value.captured_resource_epoch() == manifest.captured_resource_epoch()
        ),
        "replacement attempt rewrote the immutable old resource epoch"
    );
    evidence.record(
        "destructive-replacement-old-manifest",
        "resource-epoch-mismatch",
    )?;

    let shutdown = fresh
        .shutdown(Instant::now() + Duration::from_secs(3))
        .await?;
    ensure!(shutdown.transport_closed() && shutdown.workers_joined());
    assert_clean_stop(
        server.stop(Instant::now() + Duration::from_secs(5)).await?,
        "destructive-replacement",
    )?;
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
