mod durable_capacity;
mod task_6_5_support;

use alopex_chirps::{
    DURABLE_CHECKPOINT_JOURNAL_LIMIT_BYTES, DurableBuilder, DurableCapacityConfig,
    DurableCheckpointConfig, DurableClockSource, DurableCompactionOutcome, DurableConfig,
    DurableExtensionConfig, DurableHandle, DurableLeaseConfig, DurablePartitionProjection,
    DurableProfile, DurableResourceConfig, DurableRoutingConfig, DurableTlsConfig, NodeId,
};
use anyhow::{Context, Result, ensure};
use chirps_e2e::v07::{
    EvidenceSink, FIXTURE_LEASE_MILLIS, FixtureIdentity, RUNTIME_USERNAME, ServerProcess,
    VerifiedArtifact,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use task_6_5_support::{
    RuntimeCredentials, assert_clean_stop, bootstrap_fixture, require_production_artifact,
};
use tokio::time::Instant;

const SOURCE_DOMAIN: &[u8] = b"chirps-v0.7-task-4.3-source-input\0";
const SOURCE_PATHS: [&str; 22] = [
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
const CASES: [(&str, &str, &str, u64); 12] = [
    ("base-write-old", "base-write", "old", 1),
    ("base-file-sync-old", "base-file-sync", "old", 1),
    ("suffix-write-old", "suffix-write", "old", 1),
    ("suffix-file-sync-old", "suffix-file-sync", "old", 1),
    (
        "generation-directory-sync-old",
        "generation-directory-sync",
        "old",
        1,
    ),
    ("root-write-old", "root-write", "old", 1),
    ("root-file-sync-old", "root-file-sync", "old", 1),
    ("root-rename-unknown-old", "root-rename", "unknown-old", 1),
    ("root-rename-unknown-new", "root-rename", "unknown-new", 2),
    (
        "root-directory-sync-unknown-old",
        "root-directory-sync",
        "unknown-old",
        1,
    ),
    (
        "root-directory-sync-unknown-new",
        "root-directory-sync",
        "unknown-new",
        2,
    ),
    ("root-directory-sync-new", "root-directory-sync", "new", 2),
];

#[derive(Debug, Deserialize)]
struct CorpusManifest {
    schema_version: u64,
    producer_task: String,
    requirements_sha256: String,
    design_sha256: String,
    source_input_sha256: String,
    sources: Vec<DigestEntry>,
    cases: Vec<CorpusCase>,
}

#[derive(Debug, Deserialize)]
struct DigestEntry {
    path: String,
    sha256: String,
}

#[derive(Debug, Deserialize)]
struct CorpusCase {
    name: String,
    stage: String,
    oracle: String,
    files: Vec<DigestEntry>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run through scripts/run-v07-lane.sh with an attested production server"]
async fn every_frozen_barrier_image_recovers_through_the_public_facade() -> Result<()> {
    let artifact = VerifiedArtifact::from_runner_environment()?;
    require_production_artifact(&artifact)?;
    let mut evidence = EvidenceSink::for_target(artifact.clone(), "durable_compaction")?;
    let (corpus, requirements, design, source_root) = corpus_paths()?;
    let manifest = validate_corpus(&corpus, &requirements, &design, &source_root)?;
    reject_invalid_corpora(&corpus, &requirements, &design, &source_root)?;

    let mut server = ServerProcess::new(artifact.binary.clone())?;
    let fixture = bootstrap_fixture(
        &mut server,
        &artifact,
        "chirps-v07-compaction",
        "durable-compaction",
        iggy::prelude::IggyExpiry::NeverExpire,
    )
    .await?;
    server.start(true, None).await?;

    for (name, _, oracle, expected_generation) in CASES {
        let checkpoint = tempfile::tempdir()?;
        copy_tree(
            &corpus.join(name),
            &checkpoint.path().join(".chirps-compaction"),
        )?;
        let mut handle = connect_local_state(
            &server,
            fixture,
            checkpoint.path(),
            NodeId::new(),
            DurableCapacityConfig::default(),
            Arc::new(TrustedClock(u64::MAX / 2)),
        )
        .await?;
        let status = handle.local_state_status()?;
        ensure!(
            status.generation() == expected_generation,
            "{name} recovered generation {} instead of oracle {oracle}/{expected_generation}",
            status.generation()
        );
        if expected_generation == 2 {
            ensure!(
                status.applied_through() == 2 && status.suffix_sequences() == [3],
                "{name} lost the committed post-barrier suffix frame"
            );
        } else {
            ensure!(
                status.applied_through() == 1 && status.suffix_sequences().is_empty(),
                "{name} exposed a partial generation instead of the complete old image"
            );
        }
        evidence.record(name, oracle)?;
        let report = handle
            .shutdown(Instant::now() + Duration::from_secs(3))
            .await?;
        ensure!(report.transport_closed() && report.workers_joined());
    }

    let compact_root = tempfile::tempdir()?;
    copy_tree(
        &corpus.join("root-directory-sync-new"),
        &compact_root.path().join(".chirps-compaction"),
    )?;
    let mut compacted = connect_local_state(
        &server,
        fixture,
        compact_root.path(),
        NodeId::new(),
        DurableCapacityConfig::default(),
        Arc::new(TrustedClock(u64::MAX / 2)),
    )
    .await?;
    ensure!(
        compacted.compact_local_state(5)?
            == DurableCompactionOutcome::Committed {
                generation: 3,
                collected: 1,
            },
        "safe public compaction did not install the next complete generation"
    );
    let status = compacted.local_state_status()?;
    ensure!(
        status.generation() == 3
            && status.applied_through() == 3
            && status.suffix_sequences().is_empty()
            && status.identity_count() == 0,
        "compaction lost a committed frame or retained an eligible identity"
    );
    evidence.record(
        "concurrent-frame-cutover",
        "base-through-2-suffix-3-preserved",
    )?;
    evidence.record("post-horizon-dedup", "not-guaranteed-first-seen-permitted")?;
    compacted
        .shutdown(Instant::now() + Duration::from_secs(3))
        .await?;
    assert_clean_stop(
        server.stop(Instant::now() + Duration::from_secs(5)).await?,
        "durable-compaction",
    )?;
    ensure!(manifest.cases.len() == CASES.len());
    Ok(())
}

pub(crate) struct TrustedClock(pub u64);

impl DurableClockSource for TrustedClock {
    fn read(&self) -> alopex_chirps::DurableClockReading {
        alopex_chirps::DurableClockReading::new(self.0, alopex_chirps::DurableClockTrust::Trusted)
    }
}

pub(crate) async fn connect_local_state(
    server: &ServerProcess,
    fixture: FixtureIdentity,
    checkpoint_root: &Path,
    source: NodeId,
    capacity: DurableCapacityConfig,
    clock: Arc<dyn DurableClockSource>,
) -> Result<DurableHandle> {
    let projection = DurablePartitionProjection::new(
        fixture.partition_id,
        fixture.resource_id,
        fixture.resource_epoch,
        fixture.build_sha,
        fixture.retention_bytes,
        fixture.retention_messages,
        fixture.checksum_enabled,
        fixture.configuration_digest,
        fixture.security_digest,
        fixture.capability_digest,
    );
    let config = DurableConfig::new(
        server.address(),
        DurableTlsConfig::new("localhost".to_owned(), vec![server.certificate_der()]),
        RUNTIME_USERNAME.to_owned(),
        DurableProfile::OsSyncedAccepted,
        DurableRoutingConfig::new(1, 1),
        DurableResourceConfig::new(fixture.stream_id, fixture.topic_id, vec![projection]),
        DurableCheckpointConfig::new(
            checkpoint_root.to_path_buf(),
            1,
            DURABLE_CHECKPOINT_JOURNAL_LIMIT_BYTES,
        ),
        DurableLeaseConfig::new(
            FIXTURE_LEASE_MILLIS,
            Duration::from_millis(u64::from(FIXTURE_LEASE_MILLIS) / 4),
        ),
        DurableExtensionConfig::required(1024 * 1024),
    );
    DurableBuilder::new(source)
        .inbox_generation(1)
        .explicit_partitions(1)
        .local_capacity(capacity)
        .durable_clock(clock)
        .connect(
            config,
            &RuntimeCredentials,
            Instant::now() + Duration::from_secs(10),
        )
        .await
        .map_err(Into::into)
}

fn corpus_paths() -> Result<(PathBuf, PathBuf, PathBuf, PathBuf)> {
    let repository = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()?;
    let umbrella = repository
        .ancestors()
        .find(|candidate| candidate.join(".spec-workflow").is_dir())
        .context("locate umbrella spec root")?;
    let spec = umbrella.join(".spec-workflow/specs/chirps-v0-7-durable-backend");
    let corpus = std::env::var_os("CHIRPS_LOCAL_CORPUS_ROOT")
        .map(PathBuf::from)
        .context("CHIRPS_LOCAL_CORPUS_ROOT is required")?
        .join("task-4_3");
    Ok((
        corpus,
        spec.join("requirements.md"),
        spec.join("design.md"),
        repository.join("crates/chirps-backend-iggy/src"),
    ))
}

fn validate_corpus(
    root: &Path,
    requirements: &Path,
    design: &Path,
    source_root: &Path,
) -> Result<CorpusManifest> {
    let manifest_path = root.join("manifest.json");
    let manifest: CorpusManifest = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    ensure!(
        manifest.schema_version == 1,
        "wrong compaction corpus schema"
    );
    ensure!(
        manifest.producer_task == "4.3",
        "wrong corpus producer task"
    );
    ensure!(
        manifest.requirements_sha256 == digest_file(requirements)?,
        "stale requirements input"
    );
    ensure!(
        manifest.design_sha256 == digest_file(design)?,
        "stale design input"
    );
    ensure!(
        manifest
            .sources
            .iter()
            .map(|entry| entry.path.as_str())
            .eq(SOURCE_PATHS),
        "missing or unexpected source input"
    );
    let mut sources = BTreeMap::new();
    for source in &manifest.sources {
        let bytes = fs::read(source_root.join(&source.path))?;
        ensure!(hex(&digest(&bytes)) == source.sha256, "stale source input");
        sources.insert(source.path.as_str(), digest(&bytes));
    }
    let mut projection = Vec::new();
    for (path, sha) in sources {
        projection.extend_from_slice(&(path.len() as u32).to_be_bytes());
        projection.extend_from_slice(path.as_bytes());
        projection.extend_from_slice(&sha);
    }
    let mut source_hasher = Sha256::new();
    source_hasher.update(SOURCE_DOMAIN);
    source_hasher.update(projection);
    ensure!(
        hex(&source_hasher.finalize()) == manifest.source_input_sha256,
        "source-input oracle mismatch"
    );

    let expected: BTreeMap<_, _> = CASES
        .into_iter()
        .map(|(name, stage, oracle, _)| (name, (stage, oracle)))
        .collect();
    let mut inventory = BTreeSet::from([PathBuf::from("manifest.json")]);
    ensure!(
        manifest.cases.len() == expected.len(),
        "missing barrier case"
    );
    for case in &manifest.cases {
        let Some((stage, oracle)) = expected.get(case.name.as_str()) else {
            anyhow::bail!("unknown barrier case")
        };
        ensure!(
            case.stage == *stage && case.oracle == *oracle,
            "barrier oracle mismatch"
        );
        for file in &case.files {
            let relative = PathBuf::from(&case.name).join(&file.path);
            ensure!(
                digest_file(&root.join(&relative))? == file.sha256,
                "corpus file checksum mismatch"
            );
            inventory.insert(relative);
        }
    }
    ensure!(
        inventory == file_inventory(root)?,
        "corpus inventory mismatch"
    );
    Ok(manifest)
}

fn reject_invalid_corpora(
    corpus: &Path,
    requirements: &Path,
    design: &Path,
    source_root: &Path,
) -> Result<()> {
    for mutation in ["missing", "wrong-task", "stale", "checksum", "oracle"] {
        let temp = tempfile::tempdir()?;
        copy_tree(corpus, temp.path())?;
        let manifest_path = temp.path().join("manifest.json");
        let mut manifest: serde_json::Value = serde_json::from_slice(&fs::read(&manifest_path)?)?;
        match mutation {
            "missing" => {
                fs::remove_file(temp.path().join("base-write-old/compaction.root"))?;
            }
            "wrong-task" => manifest["producer_task"] = "4.2".into(),
            "stale" => manifest["source_input_sha256"] = "00".repeat(32).into(),
            "checksum" => {
                let path = temp.path().join("base-write-old/compaction.root");
                let mut bytes = fs::read(&path)?;
                bytes[0] ^= 1;
                fs::write(path, bytes)?;
            }
            "oracle" => manifest["cases"][0]["oracle"] = "new".into(),
            _ => unreachable!(),
        }
        if mutation == "wrong-task" || mutation == "stale" || mutation == "oracle" {
            fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest)?)?;
        }
        ensure!(
            validate_corpus(temp.path(), requirements, design, source_root).is_err(),
            "{mutation} compaction corpus was accepted"
        );
    }
    Ok(())
}

fn copy_tree(source: &Path, destination: &Path) -> Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let path = entry.path();
        let target = destination.join(entry.file_name());
        let file_type = entry.file_type()?;
        ensure!(!file_type.is_symlink(), "corpus contains a symlink");
        if file_type.is_dir() {
            copy_tree(&path, &target)?;
        } else {
            fs::copy(path, target)?;
        }
    }
    Ok(())
}

fn file_inventory(root: &Path) -> Result<BTreeSet<PathBuf>> {
    fn visit(root: &Path, current: &Path, output: &mut BTreeSet<PathBuf>) -> Result<()> {
        for entry in fs::read_dir(current)? {
            let entry = entry?;
            let path = entry.path();
            ensure!(
                !entry.file_type()?.is_symlink(),
                "corpus contains a symlink"
            );
            if path.is_dir() {
                visit(root, &path, output)?;
            } else {
                output.insert(path.strip_prefix(root)?.to_path_buf());
            }
        }
        Ok(())
    }
    let mut result = BTreeSet::new();
    visit(root, root, &mut result)?;
    Ok(result)
}

fn digest_file(path: &Path) -> Result<String> {
    Ok(hex(&digest(&fs::read(path)?)))
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
