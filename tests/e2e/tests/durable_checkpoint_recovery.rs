use super::task_6_5_support::{
    RuntimeCredentials, assert_clean_stop, bootstrap_fixture, require_production_artifact,
};
use alopex_chirps::{DurableDeliveryClock, DurablePoll, NodeId};
use alopex_chirps_core::durable::{
    ConfirmationBoundary, DurableSendOutcome, InitialPosition, ResourceEpoch, ResourceId,
    SubscriptionCreationOutcome, SubscriptionId,
};
use anyhow::{Context, Result, anyhow, ensure};
use chirps_e2e::v07::{
    EvidenceSink, OracleAppendObserver, ServerProcess, VerifiedArtifact,
    connect_fixture_with_observer,
};
use chirps_fault_oracle::{
    CreationCorpusInputs, CreationExpectation, CreationMaterializationBinding, OracleStore,
    materialize_creation_case,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

const FRAME_MAGIC: &[u8; 8] = b"CHRPST07";
const FRAME_VERSION: u16 = 1;
const FRAME_HEADER_LEN: usize = 16;
const FRAME_CHECKSUM_LEN: usize = 32;
const FRAME_DIGEST_DOMAIN: &[u8] = b"chirps-v0.7-state-frame-sha256\0";
const SOURCE_DIGEST_DOMAIN: &[u8] = b"chirps-v0.7-task-4.2-source-input\0";
const COMMIT_MARKER: &[u8; 8] = b"COMMIT07";
const MAX_MANIFEST_BYTES: u64 = 128 * 1024;
const MAX_JOURNAL_BYTES: u64 = 16 * 1024 * 1024;

const SOURCE_PATHS: &[&str] = &[
    "codec.rs",
    "delivery.rs",
    "lib.rs",
    "lifecycle.rs",
    "message_id.rs",
    "observability.rs",
    "offset_mirror.rs",
    "poll.rs",
    "producer.rs",
    "protocol.rs",
    "routing.rs",
    "runtime.rs",
    "session.rs",
    "state/capacity.rs",
    "state/compaction.rs",
    "state/creation.rs",
    "state/identity.rs",
    "state/journal.rs",
    "state/mod.rs",
    "state/owner.rs",
    "subscriber.rs",
    "transport.rs",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Image {
    NoCanonical,
    PartialPendingHeader,
    FullPendingHeader,
    Header,
    PartialIdentity,
    Identity,
    PartialCheckpoint,
    Checkpoint,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecoveryExpectation {
    Invalid,
    Delivery(u64),
    Tail,
}

#[derive(Clone, Copy)]
struct ExpectedCase {
    name: &'static str,
    stage: &'static str,
    oracle: &'static str,
    file: &'static str,
    image: Image,
    journal: Option<JournalState>,
    recovery: RecoveryExpectation,
}

const CASES: &[ExpectedCase] = &[
    case(
        "initial-write-old",
        "write",
        "old",
        ".checkpoint.journal.pending",
        Image::PartialPendingHeader,
        None,
        RecoveryExpectation::Invalid,
    ),
    case(
        "initial-file-sync-old",
        "file-sync",
        "old",
        ".checkpoint.journal.pending",
        Image::FullPendingHeader,
        None,
        RecoveryExpectation::Invalid,
    ),
    case(
        "initial-rename-unknown-old",
        "rename",
        "unknown-old",
        ".checkpoint.journal.pending",
        Image::FullPendingHeader,
        None,
        RecoveryExpectation::Invalid,
    ),
    case(
        "initial-rename-unknown-new",
        "rename",
        "unknown-new",
        "checkpoint.journal",
        Image::Header,
        Some(JournalState::Header),
        RecoveryExpectation::Delivery(1),
    ),
    case(
        "initial-directory-sync-unknown-old",
        "directory-sync",
        "unknown-old",
        "",
        Image::NoCanonical,
        None,
        RecoveryExpectation::Invalid,
    ),
    case(
        "initial-directory-sync-unknown-new",
        "directory-sync",
        "unknown-new",
        "checkpoint.journal",
        Image::Header,
        Some(JournalState::Header),
        RecoveryExpectation::Delivery(1),
    ),
    case(
        "initial-directory-sync-new",
        "directory-sync",
        "new",
        "checkpoint.journal",
        Image::Header,
        Some(JournalState::Header),
        RecoveryExpectation::Delivery(1),
    ),
    case(
        "identity-write-old",
        "write",
        "old",
        "checkpoint.journal",
        Image::PartialIdentity,
        Some(JournalState::Header),
        RecoveryExpectation::Delivery(1),
    ),
    case(
        "identity-write-unknown-old",
        "write",
        "old",
        "checkpoint.journal",
        Image::Header,
        Some(JournalState::Header),
        RecoveryExpectation::Delivery(1),
    ),
    case(
        "identity-write-unknown-new",
        "write",
        "identity-new",
        "checkpoint.journal",
        Image::Identity,
        Some(JournalState::Identity),
        RecoveryExpectation::Delivery(2),
    ),
    case(
        "identity-file-sync-new",
        "file-sync",
        "identity-new",
        "checkpoint.journal",
        Image::Identity,
        Some(JournalState::Identity),
        RecoveryExpectation::Delivery(2),
    ),
    case(
        "checkpoint-write-old",
        "write",
        "checkpoint-old",
        "checkpoint.journal",
        Image::PartialCheckpoint,
        Some(JournalState::Identity),
        RecoveryExpectation::Delivery(2),
    ),
    case(
        "checkpoint-write-unknown-old",
        "write",
        "checkpoint-old",
        "checkpoint.journal",
        Image::Identity,
        Some(JournalState::Identity),
        RecoveryExpectation::Delivery(2),
    ),
    case(
        "checkpoint-write-unknown-new",
        "write",
        "checkpoint-new",
        "checkpoint.journal",
        Image::Checkpoint,
        Some(JournalState::Checkpoint),
        RecoveryExpectation::Tail,
    ),
    case(
        "checkpoint-file-sync-unknown-new",
        "file-sync",
        "checkpoint-new",
        "checkpoint.journal",
        Image::Checkpoint,
        Some(JournalState::Checkpoint),
        RecoveryExpectation::Tail,
    ),
    case(
        "rotation-rename-unknown-old",
        "rename",
        "checkpoint-old",
        "checkpoint.journal",
        Image::Identity,
        Some(JournalState::Identity),
        RecoveryExpectation::Delivery(2),
    ),
    case(
        "rotation-rename-unknown-new",
        "rename",
        "checkpoint-new",
        "checkpoint.journal",
        Image::Checkpoint,
        Some(JournalState::Checkpoint),
        RecoveryExpectation::Tail,
    ),
    case(
        "rotation-directory-sync-unknown-old",
        "directory-sync",
        "checkpoint-old",
        "checkpoint.journal",
        Image::Identity,
        Some(JournalState::Identity),
        RecoveryExpectation::Delivery(2),
    ),
    case(
        "rotation-directory-sync-unknown-new",
        "directory-sync",
        "checkpoint-new",
        "checkpoint.journal",
        Image::Checkpoint,
        Some(JournalState::Checkpoint),
        RecoveryExpectation::Tail,
    ),
    case(
        "rotation-directory-sync-new",
        "directory-sync",
        "checkpoint-new",
        "checkpoint.journal",
        Image::Checkpoint,
        Some(JournalState::Checkpoint),
        RecoveryExpectation::Tail,
    ),
];

const fn case(
    name: &'static str,
    stage: &'static str,
    oracle: &'static str,
    file: &'static str,
    image: Image,
    journal: Option<JournalState>,
    recovery: RecoveryExpectation,
) -> ExpectedCase {
    ExpectedCase {
        name,
        stage,
        oracle,
        file,
        image,
        journal,
        recovery,
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema_version: u32,
    producer_task: String,
    requirements_sha256: String,
    design_sha256: String,
    source_input_sha256: String,
    sources: Vec<SourceEntry>,
    cases: Vec<CaseEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceEntry {
    path: String,
    sha256: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CaseEntry {
    name: String,
    stage: String,
    oracle: String,
    file: String,
    sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum JournalState {
    Header,
    Identity,
    Checkpoint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct JournalTruth {
    pub(super) state: JournalState,
    pub(super) message_id: Option<[u8; 16]>,
    pub(super) envelope_digest: Option<[u8; 32]>,
    pub(super) delivery_attempt: Option<u64>,
    pub(super) checkpoint_owner_epoch: Option<u64>,
    sequence: u64,
    observation_count: u64,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct JournalBinding {
    subscription_id: SubscriptionId,
    target: NodeId,
    generation: u64,
    partition: u32,
    lifecycle_generation: u64,
    initial_offset: u64,
    resource_epoch: ResourceEpoch,
}

impl JournalBinding {
    pub(super) const fn new(
        subscription_id: SubscriptionId,
        target: NodeId,
        partition: u32,
        initial_offset: u64,
        resource_epoch: ResourceEpoch,
    ) -> Self {
        Self {
            subscription_id,
            target,
            generation: 1,
            partition,
            lifecycle_generation: 1,
            initial_offset,
            resource_epoch,
        }
    }

    const fn frozen(
        subscription_id: SubscriptionId,
        target: NodeId,
        partition: u32,
        initial_offset: u64,
        resource_epoch: ResourceEpoch,
    ) -> Self {
        Self {
            subscription_id,
            target,
            generation: 7,
            partition,
            lifecycle_generation: 9,
            initial_offset,
            resource_epoch,
        }
    }
}

struct LiveRecord {
    offset: u64,
    partition: u32,
    message_id: [u8; 16],
    envelope_digest: [u8; 32],
    resource_epoch: ResourceEpoch,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run through scripts/run-v07-lane.sh with an attested production server"]
async fn frame_stage_corpus_reconciles_exact_old_new_and_unknown_truth() -> Result<()> {
    let Some(case_name) = env::var_os("CHIRPS_CHECKPOINT_RECOVERY_CASE") else {
        let executable = env::current_exe()?;
        for case in CASES {
            let output = Command::new(&executable)
                .args([
                    "--exact",
                    "durable_checkpoint_recovery::frame_stage_corpus_reconciles_exact_old_new_and_unknown_truth",
                    "--ignored",
                    "--nocapture",
                ])
                .env("CHIRPS_CHECKPOINT_RECOVERY_CASE", case.name)
                .output()
                .with_context(|| format!("launch fresh checkpoint process for {}", case.name))?;
            ensure!(
                output.status.success(),
                "checkpoint child failed for {}: stdout={} stderr={}",
                case.name,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
        }
        return Ok(());
    };
    let case_name = case_name.to_string_lossy();
    ensure!(
        CASES.iter().any(|case| case.name == case_name.as_ref()),
        "unknown checkpoint recovery case: {case_name}"
    );
    let artifact = VerifiedArtifact::from_runner_environment()?;
    require_production_artifact(&artifact)?;
    let mut evidence = EvidenceSink::for_target(artifact.clone(), "durable_checkpoint_recovery")?;
    let corpus_root = required_canonical_directory("CHIRPS_LOCAL_CORPUS_ROOT")?;
    let source_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .context("locate Chirps source root")?
        .to_path_buf();
    let (requirements, design) = locate_spec_inputs(&corpus_root)?;
    let journal_root = corpus_root.join("task-4_2");
    validate_corpus(&journal_root, &requirements, &design, &source_root)?;
    validate_rejections(&journal_root, &requirements, &design, &source_root)?;
    evidence.record("corpus-negative-controls", "all-rejected")?;

    let mut server = ServerProcess::new(artifact.binary.clone())?;
    let fixture = bootstrap_fixture(
        &mut server,
        &artifact,
        "chirps-v07-checkpoint-recovery",
        "durable-checkpoint-recovery",
        iggy::prelude::IggyExpiry::NeverExpire,
    )
    .await?;
    server.start(true, None).await?;
    let oracle_root = tempfile::tempdir()?;
    let observer = Arc::new(OracleAppendObserver::new(OracleStore::new(
        oracle_root.path().join("oracle.log"),
    )));
    let source = NodeId::new();
    let target = NodeId::new();
    let producer_root = tempfile::tempdir()?;
    let mut producer = connect_fixture_with_observer(
        &server,
        fixture,
        producer_root.path(),
        source,
        chirps_e2e::v07::RUNTIME_USERNAME,
        &RuntimeCredentials,
        observer.clone(),
    )
    .await?;
    let prepared = producer.prepare(target, b"corpus".to_vec(), b"frame-stage")?;
    let sent = producer
        .send(&prepared, ConfirmationBoundary::OsSyncedAccepted)
        .await?;
    ensure!(
        sent.outcome() == DurableSendOutcome::OsSyncedAccepted,
        "corpus fixture send was not strongly accepted: {sent:?}"
    );
    ensure!(!observer.failed(), "corpus fixture oracle write failed");
    let receipt = sent
        .receipt()
        .context("corpus fixture omitted its strong receipt")?;
    let live = LiveRecord {
        offset: receipt.assigned_offset(),
        partition: receipt.partition(),
        message_id: *receipt.message_id().as_bytes(),
        envelope_digest: *receipt.envelope_digest().as_bytes(),
        resource_epoch: receipt.resource_epoch(),
    };
    producer
        .shutdown(Instant::now() + Duration::from_secs(3))
        .await?;
    drop(producer);
    assert_clean_stop(
        server.stop(Instant::now() + Duration::from_secs(5)).await?,
        "fixture-restart",
    )?;
    server.start(true, None).await?;

    for (index, case) in CASES
        .iter()
        .enumerate()
        .filter(|(_, case)| case.name == case_name.as_ref())
    {
        let checkpoint_root = tempfile::tempdir()?;
        let mut id = [0x67; 16];
        id[15] = u8::try_from(index + 1)?;
        let subscription_id = SubscriptionId::from_bytes(id);
        let namespace_digest = [u8::try_from(index + 1)?; 32];
        let directory = subscription_directory(checkpoint_root.path(), subscription_id);
        let expectation = CreationExpectation::new(
            *subscription_id.as_bytes(),
            *target.as_bytes(),
            1,
            live.partition,
            1,
            namespace_digest,
            InitialPosition::Exact(live.offset),
            live.offset,
            live.resource_epoch,
        );
        materialize_creation_case(
            &corpus_root.join("task-4_1"),
            "directory-sync-new",
            &directory,
            CreationCorpusInputs::new(
                &requirements,
                &design,
                &source_root.join("crates/chirps-backend-iggy/src"),
            ),
            CreationMaterializationBinding::new(
                expectation,
                live.offset.checked_add(1).context("fixture end overflow")?,
                live.offset,
            ),
        )?;
        let binding = JournalBinding::new(
            subscription_id,
            target,
            live.partition,
            live.offset,
            live.resource_epoch,
        );
        install_case(case, &directory, binding, &live)?;
        let before = match case.journal {
            Some(expected) => {
                let truth = read_bound_journal(&directory.join("checkpoint.journal"), binding)?;
                ensure!(
                    truth.state == expected,
                    "{} independent journal verdict drifted",
                    case.name
                );
                Some(truth)
            }
            None => {
                ensure!(
                    !directory.join("checkpoint.journal").exists(),
                    "{} unexpectedly had a canonical journal",
                    case.name
                );
                None
            }
        };

        let mut handle = connect_fixture_with_observer(
            &server,
            fixture,
            checkpoint_root.path(),
            source,
            chirps_e2e::v07::RUNTIME_USERNAME,
            &RuntimeCredentials,
            observer.clone(),
        )
        .await?;
        let reopened = handle
            .reopen_subscription(subscription_id, target, live.partition, namespace_digest)
            .await;
        match case.recovery {
            RecoveryExpectation::Invalid => ensure!(
                reopened == Err(alopex_chirps::DurableSubscriptionError::InvalidState),
                "{} accepted missing/old journal state: {reopened:?}",
                case.name
            ),
            RecoveryExpectation::Delivery(attempt) => {
                ensure!(
                    matches!(reopened?, SubscriptionCreationOutcome::Created(_)),
                    "{} did not reopen",
                    case.name
                );
                let DurablePoll::Delivery(mut delivery) = handle
                    .next_delivery(subscription_id, 1_000, DurableDeliveryClock::Trusted)
                    .await?
                else {
                    return Err(anyhow!("{} did not redeliver its old frontier", case.name));
                };
                ensure!(
                    delivery.handle().offset() == live.offset
                        && delivery.handle().message_id().as_bytes() == &live.message_id
                        && delivery.handle().envelope_digest().as_bytes() == &live.envelope_digest
                        && delivery.handle().delivery_attempt() == attempt,
                    "{} delivered an identity inconsistent with its oracle",
                    case.name
                );
                handle.release(subscription_id, delivery.handle_mut())?;
            }
            RecoveryExpectation::Tail => {
                ensure!(
                    matches!(reopened?, SubscriptionCreationOutcome::Created(_)),
                    "{} did not reopen",
                    case.name
                );
                ensure!(
                    handle
                        .next_delivery(subscription_id, 1_000, DurableDeliveryClock::Trusted)
                        .await?
                        == DurablePoll::Tail,
                    "{} did not preserve its committed checkpoint frontier",
                    case.name
                );
            }
        }
        if let Some(truth) = before {
            if truth.state != JournalState::Header {
                ensure!(
                    truth.message_id == Some(live.message_id)
                        && truth.envelope_digest == Some(live.envelope_digest),
                    "{} materialized the wrong durable identity",
                    case.name
                );
            }
        }
        evidence.record(
            &format!("{}-{}-{}", case.name, case.stage, case.oracle),
            match case.recovery {
                RecoveryExpectation::Invalid => "fail-stop-old",
                RecoveryExpectation::Delivery(1) => "old-frontier-delivery-one",
                RecoveryExpectation::Delivery(2) => "identity-new-checkpoint-old-redelivery",
                RecoveryExpectation::Delivery(_) => unreachable!(),
                RecoveryExpectation::Tail => "checkpoint-new-next-frontier",
            },
        )?;
        if case.name.contains("unknown-old") {
            evidence.record(
                &format!("{}-outcome-preservation", case.name),
                "original-unknown-recovered-old",
            )?;
        } else if case.name.contains("unknown-new") {
            evidence.record(
                &format!("{}-outcome-preservation", case.name),
                "original-unknown-recovered-new",
            )?;
        }
        handle
            .shutdown(Instant::now() + Duration::from_secs(3))
            .await?;
    }
    assert_clean_stop(
        server.stop(Instant::now() + Duration::from_secs(5)).await?,
        "final",
    )?;
    Ok(())
}

pub(super) fn subscription_directory(root: &Path, id: SubscriptionId) -> PathBuf {
    root.join(hex(id.as_bytes()))
}

fn required_canonical_directory(name: &'static str) -> Result<PathBuf> {
    let path = PathBuf::from(env::var_os(name).context("runner omitted local corpus root")?);
    ensure!(
        path.is_absolute() && path.canonicalize()? == path,
        "{name} is not canonical"
    );
    ensure_directory(&path)?;
    Ok(path)
}

fn locate_spec_inputs(corpus_root: &Path) -> Result<(PathBuf, PathBuf)> {
    for ancestor in corpus_root.ancestors() {
        let root = ancestor.join(".spec-workflow/specs/chirps-v0-7-durable-backend");
        let requirements = root.join("requirements.md");
        let design = root.join("design.md");
        if requirements.is_file() && design.is_file() {
            return Ok((requirements, design));
        }
    }
    Err(anyhow!(
        "local corpus root is not under the v0.7 specification root"
    ))
}

fn validate_corpus(
    root: &Path,
    requirements: &Path,
    design: &Path,
    source_root: &Path,
) -> Result<()> {
    ensure_directory(root)?;
    let manifest_bytes = read_regular_bounded(&root.join("manifest.json"), MAX_MANIFEST_BYTES)?;
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes)?;
    ensure!(
        manifest.schema_version == 1 && manifest.producer_task == "4.2",
        "wrong Task 4.2 manifest"
    );
    let requirements_bytes = read_regular_bounded(requirements, MAX_MANIFEST_BYTES)?;
    let design_bytes = read_regular_bounded(design, MAX_MANIFEST_BYTES)?;
    let requirements_digest: [u8; 32] = Sha256::digest(&requirements_bytes).into();
    let design_digest: [u8; 32] = Sha256::digest(&design_bytes).into();
    ensure!(
        manifest.requirements_sha256 == hex(&requirements_digest),
        "stale requirements input"
    );
    ensure!(
        manifest.design_sha256 == hex(&design_digest),
        "stale design input"
    );
    ensure!(
        manifest.sources.len() == SOURCE_PATHS.len(),
        "source inventory length mismatch"
    );
    let source_base = source_root.join("crates/chirps-backend-iggy/src");
    let mut projection = Vec::new();
    for (entry, expected) in manifest.sources.iter().zip(SOURCE_PATHS) {
        ensure!(entry.path == *expected, "source inventory path mismatch");
        let bytes = read_regular_bounded(&source_base.join(expected), MAX_JOURNAL_BYTES)?;
        let digest = Sha256::digest(&bytes);
        ensure!(
            entry.sha256 == hex(&digest),
            "stale source input: {expected}"
        );
        projection.extend_from_slice(&(expected.len() as u32).to_be_bytes());
        projection.extend_from_slice(expected.as_bytes());
        projection.extend_from_slice(&digest);
    }
    let source_digest = digest_bytes(&[SOURCE_DIGEST_DOMAIN, &projection]);
    ensure!(
        manifest.source_input_sha256 == hex(&source_digest),
        "source projection digest mismatch"
    );
    let mut target: [u8; 16] = design_digest[..16].try_into()?;
    target[6] = (target[6] & 0x0f) | 0x40;
    target[8] = (target[8] & 0x3f) | 0x80;
    let mut message_id: [u8; 16] = source_digest[..16].try_into()?;
    message_id[6] = (message_id[6] & 0x0f) | 0x40;
    message_id[8] = (message_id[8] & 0x3f) | 0x80;
    let resource_id: [u8; 16] = requirements_digest[..16].try_into()?;
    let frozen_binding = JournalBinding::frozen(
        SubscriptionId::from_bytes(requirements_digest[..16].try_into()?),
        NodeId::from(target),
        3,
        4,
        ResourceEpoch::new(ResourceId::from_bytes(resource_id), 5),
    );
    let frozen_record = LiveRecord {
        offset: 4,
        partition: 3,
        message_id,
        envelope_digest: design_digest,
        resource_epoch: frozen_binding.resource_epoch,
    };
    ensure!(
        manifest.cases.len() == CASES.len(),
        "case inventory length mismatch"
    );
    let mut expected_root = BTreeSet::from(["manifest.json".to_owned()]);
    for (actual, expected) in manifest.cases.iter().zip(CASES) {
        ensure!(
            actual.name == expected.name
                && actual.stage == expected.stage
                && actual.oracle == expected.oracle
                && actual.file == expected.file,
            "case stage/oracle contract mismatch"
        );
        let directory = root.join(expected.name);
        ensure_directory(&directory)?;
        expected_root.insert(expected.name.to_owned());
        let entries = directory_entries(&directory)?;
        if expected.file.is_empty() {
            ensure!(
                entries.is_empty() && actual.sha256.is_empty(),
                "empty case contains state"
            );
        } else {
            ensure!(
                entries == BTreeSet::from([expected.file.to_owned()]),
                "case file inventory mismatch"
            );
            let bytes = read_regular_bounded(&directory.join(expected.file), MAX_JOURNAL_BYTES)?;
            ensure!(actual.sha256 == sha_hex(&bytes), "case checksum mismatch");
            ensure!(
                case_bytes(expected.image, frozen_binding, &frozen_record)
                    .is_some_and(|value| value == bytes),
                "case bytes differ from the independently reconstructed frame stage"
            );
        }
    }
    ensure!(
        directory_entries(root)? == expected_root,
        "Task 4.2 root inventory mismatch"
    );
    Ok(())
}

fn validate_rejections(
    root: &Path,
    requirements: &Path,
    design: &Path,
    source_root: &Path,
) -> Result<()> {
    reject_mutation(root, requirements, design, source_root, "missing", |copy| {
        fs::remove_file(copy.join("manifest.json"))?;
        Ok(())
    })?;
    reject_mutation(
        root,
        requirements,
        design,
        source_root,
        "missing-case",
        |copy| {
            fs::remove_dir_all(copy.join("initial-write-old"))?;
            Ok(())
        },
    )?;
    reject_manifest_mutation(
        root,
        requirements,
        design,
        source_root,
        "wrong-task",
        |value| {
            value["producer_task"] = serde_json::json!("4.3");
        },
    )?;
    reject_manifest_mutation(
        root,
        requirements,
        design,
        source_root,
        "stale-source",
        |value| {
            value["source_input_sha256"] = serde_json::json!("00");
        },
    )?;
    reject_manifest_mutation(
        root,
        requirements,
        design,
        source_root,
        "oracle-mismatch",
        |value| {
            value["cases"][0]["oracle"] = serde_json::json!("new");
        },
    )?;
    reject_mutation(
        root,
        requirements,
        design,
        source_root,
        "checksum",
        |copy| {
            let path = copy.join("identity-file-sync-new/checkpoint.journal");
            let mut bytes = fs::read(&path)?;
            bytes[0] ^= 1;
            fs::write(path, bytes)?;
            Ok(())
        },
    )
}

fn reject_manifest_mutation(
    root: &Path,
    requirements: &Path,
    design: &Path,
    source_root: &Path,
    label: &str,
    mutate: impl FnOnce(&mut serde_json::Value),
) -> Result<()> {
    reject_mutation(root, requirements, design, source_root, label, |copy| {
        let path = copy.join("manifest.json");
        let mut value: serde_json::Value = serde_json::from_slice(&fs::read(&path)?)?;
        mutate(&mut value);
        fs::write(path, serde_json::to_vec(&value)?)?;
        Ok(())
    })
}

fn reject_mutation(
    root: &Path,
    requirements: &Path,
    design: &Path,
    source_root: &Path,
    label: &str,
    mutate: impl FnOnce(&Path) -> Result<()>,
) -> Result<()> {
    let temp = tempfile::tempdir()?;
    let copy = temp.path().join("task-4_2");
    copy_tree(root, &copy)?;
    mutate(&copy)?;
    ensure!(
        validate_corpus(&copy, requirements, design, source_root).is_err(),
        "negative control was accepted: {label}"
    );
    Ok(())
}

fn copy_tree(source: &Path, destination: &Path) -> Result<()> {
    ensure_directory(source)?;
    fs::create_dir(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        ensure!(!kind.is_symlink(), "corpus contains a symlink");
        let target = destination.join(entry.file_name());
        if kind.is_dir() {
            copy_tree(&entry.path(), &target)?;
        } else {
            ensure!(kind.is_file(), "corpus contains a non-regular entry");
            fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

fn install_case(
    case: &ExpectedCase,
    directory: &Path,
    binding: JournalBinding,
    live: &LiveRecord,
) -> Result<()> {
    let canonical = directory.join("checkpoint.journal");
    if canonical.exists() {
        fs::remove_file(&canonical)?;
    }
    let pending = directory.join(".checkpoint.journal.pending");
    if pending.exists() {
        fs::remove_file(&pending)?;
    }
    if let Some(bytes) = case_bytes(case.image, binding, live) {
        let path = if matches!(
            case.image,
            Image::PartialPendingHeader | Image::FullPendingHeader
        ) {
            &pending
        } else {
            &canonical
        };
        write_synced(path, &bytes)?;
    }
    OpenOptions::new().read(true).open(directory)?.sync_all()?;
    Ok(())
}

fn case_bytes(image: Image, binding: JournalBinding, live: &LiveRecord) -> Option<Vec<u8>> {
    let header = journal_header(binding);
    let identity = identity_frame(live);
    let checkpoint = checkpoint_frame(binding, live);
    Some(match image {
        Image::NoCanonical => return None,
        Image::PartialPendingHeader => header[..header.len() / 2].to_vec(),
        Image::FullPendingHeader | Image::Header => header,
        Image::PartialIdentity => {
            let mut bytes = header;
            bytes.extend_from_slice(&identity[..identity.len() / 2]);
            bytes
        }
        Image::Identity => {
            let mut bytes = header;
            bytes.extend_from_slice(&identity);
            bytes
        }
        Image::PartialCheckpoint => {
            let mut bytes = header;
            bytes.extend_from_slice(&identity);
            bytes.extend_from_slice(&checkpoint[..checkpoint.len() / 2]);
            bytes
        }
        Image::Checkpoint => {
            let mut bytes = header;
            bytes.extend_from_slice(&identity);
            bytes.extend_from_slice(&checkpoint);
            bytes
        }
    })
}

fn journal_header(binding: JournalBinding) -> Vec<u8> {
    let mut body = [0_u8; 100];
    body[0..8].copy_from_slice(&1_u64.to_be_bytes());
    body[8..24].copy_from_slice(binding.subscription_id.as_bytes());
    body[24..40].copy_from_slice(binding.target.as_bytes());
    body[40..48].copy_from_slice(&binding.generation.to_be_bytes());
    body[48..52].copy_from_slice(&binding.partition.to_be_bytes());
    body[52..60].copy_from_slice(&binding.lifecycle_generation.to_be_bytes());
    body[60..68].copy_from_slice(&binding.initial_offset.to_be_bytes());
    body[68..84].copy_from_slice(binding.resource_epoch.resource_id().as_bytes());
    body[84..92].copy_from_slice(&binding.resource_epoch.epoch().to_be_bytes());
    body[92..100].copy_from_slice(COMMIT_MARKER);
    encode_frame(3, &body)
}

fn identity_frame(live: &LiveRecord) -> Vec<u8> {
    let mut record = [0_u8; 118];
    record[0..16].copy_from_slice(&live.message_id);
    record[16..48].copy_from_slice(&live.envelope_digest);
    record[48..64].copy_from_slice(live.resource_epoch.resource_id().as_bytes());
    record[64..72].copy_from_slice(&live.resource_epoch.epoch().to_be_bytes());
    record[72..76].copy_from_slice(&live.partition.to_be_bytes());
    record[76..84].copy_from_slice(&live.offset.to_be_bytes());
    record[84..92].copy_from_slice(&live.offset.to_be_bytes());
    record[92..100].copy_from_slice(&1_u64.to_be_bytes());
    record[100..108].copy_from_slice(&1_u64.to_be_bytes());
    record[108] = 0;
    record[109..117].copy_from_slice(&1_000_u64.to_be_bytes());
    record[117] = 1;
    let mut body = Vec::with_capacity(134);
    body.extend_from_slice(&1_u64.to_be_bytes());
    body.extend_from_slice(&record);
    body.extend_from_slice(COMMIT_MARKER);
    encode_frame(4, &body)
}

fn checkpoint_frame(binding: JournalBinding, live: &LiveRecord) -> Vec<u8> {
    let mut body = [0_u8; 148];
    body[0..8].copy_from_slice(&2_u64.to_be_bytes());
    body[8..24].copy_from_slice(binding.subscription_id.as_bytes());
    body[24..40].copy_from_slice(binding.target.as_bytes());
    body[40..48].copy_from_slice(&binding.generation.to_be_bytes());
    body[48..52].copy_from_slice(&binding.partition.to_be_bytes());
    body[52..60].copy_from_slice(&live.offset.to_be_bytes());
    body[60..76].copy_from_slice(&live.message_id);
    body[76..108].copy_from_slice(&live.envelope_digest);
    body[108..116].copy_from_slice(&1_u64.to_be_bytes());
    body[116..124].copy_from_slice(&1_u64.to_be_bytes());
    body[124..132].copy_from_slice(&binding.lifecycle_generation.to_be_bytes());
    body[132..140].copy_from_slice(&1_u64.to_be_bytes());
    body[140..148].copy_from_slice(COMMIT_MARKER);
    encode_frame(5, &body)
}

fn encode_frame(kind: u8, body: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(FRAME_HEADER_LEN + body.len() + FRAME_CHECKSUM_LEN);
    bytes.extend_from_slice(FRAME_MAGIC);
    bytes.extend_from_slice(&FRAME_VERSION.to_be_bytes());
    bytes.push(kind);
    bytes.push(0);
    bytes.extend_from_slice(&(body.len() as u32).to_be_bytes());
    bytes.extend_from_slice(body);
    let checksum = Sha256::new()
        .chain_update(FRAME_DIGEST_DOMAIN)
        .chain_update(&bytes)
        .finalize();
    bytes.extend_from_slice(&checksum);
    bytes
}

pub(super) fn read_bound_journal(path: &Path, binding: JournalBinding) -> Result<JournalTruth> {
    let bytes = read_regular_bounded(path, MAX_JOURNAL_BYTES)?;
    let mut position = 0;
    let mut truth: Option<JournalTruth> = None;
    while position < bytes.len() {
        let remaining = &bytes[position..];
        if remaining.len() < FRAME_HEADER_LEN {
            break;
        }
        ensure!(
            &remaining[..8] == FRAME_MAGIC,
            "journal frame magic mismatch"
        );
        ensure!(
            u16::from_be_bytes(remaining[8..10].try_into()?) == FRAME_VERSION,
            "journal frame version mismatch"
        );
        ensure!(remaining[11] == 0, "journal frame reserved byte is nonzero");
        let body_len = u32::from_be_bytes(remaining[12..16].try_into()?) as usize;
        let total = FRAME_HEADER_LEN
            .checked_add(body_len)
            .and_then(|value| value.checked_add(FRAME_CHECKSUM_LEN))
            .context("journal frame length overflow")?;
        if remaining.len() < total {
            break;
        }
        let frame = &remaining[..total];
        let checksum_start = total - FRAME_CHECKSUM_LEN;
        ensure!(
            frame[checksum_start..]
                == Sha256::new()
                    .chain_update(FRAME_DIGEST_DOMAIN)
                    .chain_update(&frame[..checksum_start])
                    .finalize()[..],
            "journal frame checksum mismatch"
        );
        let body = &frame[FRAME_HEADER_LEN..checksum_start];
        truth = Some(match (remaining[10], truth) {
            (3, None) => {
                validate_header(body, binding)?;
                JournalTruth {
                    state: JournalState::Header,
                    message_id: None,
                    envelope_digest: None,
                    delivery_attempt: None,
                    checkpoint_owner_epoch: None,
                    sequence: 0,
                    observation_count: 0,
                }
            }
            (4, Some(previous))
                if matches!(
                    previous.state,
                    JournalState::Header | JournalState::Identity
                ) =>
            {
                validate_identity(body, binding, previous)?;
                JournalTruth {
                    state: JournalState::Identity,
                    message_id: Some(body[8..24].try_into()?),
                    envelope_digest: Some(body[24..56].try_into()?),
                    delivery_attempt: Some(u64::from_be_bytes(body[100..108].try_into()?)),
                    checkpoint_owner_epoch: None,
                    sequence: u64::from_be_bytes(body[0..8].try_into()?),
                    observation_count: u64::from_be_bytes(body[108..116].try_into()?),
                }
            }
            (5, Some(previous)) if previous.state == JournalState::Identity => {
                validate_checkpoint(body, binding, previous)?;
                JournalTruth {
                    state: JournalState::Checkpoint,
                    checkpoint_owner_epoch: Some(u64::from_be_bytes(body[116..124].try_into()?)),
                    ..previous
                }
            }
            _ => return Err(anyhow!("journal frame order/kind mismatch")),
        });
        position += total;
    }
    truth.context("journal lacks a complete header")
}

fn validate_header(body: &[u8], binding: JournalBinding) -> Result<()> {
    ensure!(
        body.len() == 100
            && u64::from_be_bytes(body[0..8].try_into()?) == 1
            && &body[8..24] == binding.subscription_id.as_bytes()
            && &body[24..40] == binding.target.as_bytes()
            && u64::from_be_bytes(body[40..48].try_into()?) == binding.generation
            && u32::from_be_bytes(body[48..52].try_into()?) == binding.partition
            && u64::from_be_bytes(body[52..60].try_into()?) == binding.lifecycle_generation
            && u64::from_be_bytes(body[60..68].try_into()?) == binding.initial_offset
            && &body[68..84] == binding.resource_epoch.resource_id().as_bytes()
            && u64::from_be_bytes(body[84..92].try_into()?) == binding.resource_epoch.epoch()
            && &body[92..100] == COMMIT_MARKER,
        "journal namespace header mismatch"
    );
    Ok(())
}

fn validate_identity(body: &[u8], binding: JournalBinding, previous: JournalTruth) -> Result<()> {
    let sequence = u64::from_be_bytes(body.get(0..8).context("short identity")?.try_into()?);
    let message_id: [u8; 16] = body.get(8..24).context("short identity")?.try_into()?;
    let envelope_digest: [u8; 32] = body.get(24..56).context("short identity")?.try_into()?;
    let delivery_attempt =
        u64::from_be_bytes(body.get(100..108).context("short identity")?.try_into()?);
    let observation_count =
        u64::from_be_bytes(body.get(108..116).context("short identity")?.try_into()?);
    ensure!(
        body.len() == 134
            && previous.sequence.checked_add(1) == Some(sequence)
            && &body[56..72] == binding.resource_epoch.resource_id().as_bytes()
            && u64::from_be_bytes(body[72..80].try_into()?) == binding.resource_epoch.epoch()
            && u32::from_be_bytes(body[80..84].try_into()?) == binding.partition
            && u64::from_be_bytes(body[84..92].try_into()?) == binding.initial_offset
            && u64::from_be_bytes(body[92..100].try_into()?) == binding.initial_offset
            && delivery_attempt > 0
            && observation_count > 0
            && body[116] == 0
            && matches!(body[125], 1..=3)
            && &body[126..134] == COMMIT_MARKER,
        "journal identity mismatch"
    );
    match previous.state {
        JournalState::Header => ensure!(
            delivery_attempt == 1 && observation_count == 1,
            "first journal identity did not start at attempt/observation one"
        ),
        JournalState::Identity => ensure!(
            previous.message_id == Some(message_id)
                && previous.envelope_digest == Some(envelope_digest)
                && previous
                    .delivery_attempt
                    .is_some_and(|value| value < delivery_attempt)
                && previous.observation_count.checked_add(1) == Some(observation_count),
            "journal identity successor mismatch"
        ),
        JournalState::Checkpoint => unreachable!(),
    }
    Ok(())
}

fn validate_checkpoint(body: &[u8], binding: JournalBinding, identity: JournalTruth) -> Result<()> {
    ensure!(
        body.len() == 148
            && identity.sequence.checked_add(1) == Some(u64::from_be_bytes(body[0..8].try_into()?))
            && &body[8..24] == binding.subscription_id.as_bytes()
            && &body[24..40] == binding.target.as_bytes()
            && u64::from_be_bytes(body[40..48].try_into()?) == binding.generation
            && u32::from_be_bytes(body[48..52].try_into()?) == binding.partition
            && u64::from_be_bytes(body[52..60].try_into()?) == binding.initial_offset
            && Some(body[60..76].try_into()?) == identity.message_id
            && Some(body[76..108].try_into()?) == identity.envelope_digest
            && Some(u64::from_be_bytes(body[108..116].try_into()?)) == identity.delivery_attempt
            && u64::from_be_bytes(body[116..124].try_into()?) > 0
            && u64::from_be_bytes(body[124..132].try_into()?) == binding.lifecycle_generation
            && u64::from_be_bytes(body[132..140].try_into()?) > 0
            && &body[140..148] == COMMIT_MARKER,
        "journal checkpoint/identity mismatch"
    );
    Ok(())
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn ensure_directory(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "unsafe corpus directory"
    );
    Ok(())
}

fn directory_entries(path: &Path) -> Result<BTreeSet<String>> {
    fs::read_dir(path)?
        .map(|entry| {
            let entry = entry?;
            Ok(entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow!("non-UTF-8 corpus entry"))?)
        })
        .collect()
}

fn read_regular_bounded(path: &Path, maximum: u64) -> Result<Vec<u8>> {
    let before = fs::symlink_metadata(path)?;
    ensure!(
        before.is_file() && !before.file_type().is_symlink() && before.len() <= maximum,
        "unsafe or oversized corpus file"
    );
    let file = File::open(path)?;
    let opened = file.metadata()?;
    #[cfg(unix)]
    ensure!(
        before.dev() == opened.dev() && before.ino() == opened.ino(),
        "corpus file changed during open"
    );
    let mut bytes = Vec::new();
    file.take(maximum + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= maximum,
        "corpus file exceeded its bound"
    );
    Ok(bytes)
}

fn sha_hex(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

fn digest_bytes(parts: &[&[u8]]) -> [u8; 32] {
    let mut digest = Sha256::new();
    for part in parts {
        digest.update(part);
    }
    digest.finalize().into()
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    output
}
