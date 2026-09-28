use alopex_chirps::NodeId;
use alopex_chirps_core::durable::{
    CheckpointDirectoryId, CreationRecoveryBinding, InitialPosition, PollObservation,
    ResourceEpoch, ResourceId, SubscriptionId,
};
use anyhow::{Context, Result, ensure};
use chirps_e2e::v07::{
    FixtureIdentity, RUNTIME_PASSWORD, RUNTIME_USERNAME, RealSession, ServerProcess,
};
use chirps_fault_oracle::{
    CreationCorpusInputs, CreationExpectation, CreationMaterializationBinding,
};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::Instant;

const DIRECTORY_ID_DOMAIN: &[u8] = b"chirps-v0.7-checkpoint-directory\0";

pub fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

pub fn corpus_paths() -> Result<(PathBuf, PathBuf, PathBuf, PathBuf)> {
    let root = repository_root();
    let umbrella = root
        .ancestors()
        .find(|candidate| candidate.join(".spec-workflow").is_dir())
        .context("locate umbrella spec root")?;
    let spec = umbrella.join(".spec-workflow/specs/chirps-v0-7-durable-backend");
    let corpus_root = std::env::var_os("CHIRPS_LOCAL_CORPUS_ROOT")
        .map(PathBuf::from)
        .context("CHIRPS_LOCAL_CORPUS_ROOT is required")?;
    ensure!(
        corpus_root.is_absolute() && corpus_root.canonicalize()? == corpus_root,
        "CHIRPS_LOCAL_CORPUS_ROOT must be an absolute canonical directory"
    );
    let corpus = corpus_root.join("task-4_1");
    Ok((
        corpus,
        spec.join("requirements.md"),
        spec.join("design.md"),
        root.join("crates/chirps-backend-iggy/src"),
    ))
}

pub async fn captured_observation(
    server: &ServerProcess,
    fixture: FixtureIdentity,
    expected_end_exclusive: u64,
) -> Result<PollObservation> {
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
    .await?;
    fixture.assert_report(session.report())?;
    let observation = session.checked_poll(0).await?;
    ensure!(
        observation.end_exclusive() == expected_end_exclusive
            && observation.oldest_available() == 0,
        "creation fixture frontier does not match the public prefill"
    );
    session
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await?;
    Ok(observation)
}

pub async fn empty_observation(
    server: &ServerProcess,
    fixture: FixtureIdentity,
) -> Result<PollObservation> {
    captured_observation(server, fixture, 0).await
}

pub fn initial_position(case_name: &str) -> InitialPosition {
    match case_name {
        "write-old" | "rename-new-unknown" => InitialPosition::EarliestRetained,
        "file-sync-old" | "directory-sync-old-unknown" | "directory-sync-new-unknown" => {
            InitialPosition::LatestAfterCapturedEnd
        }
        _ => InitialPosition::Exact(1),
    }
}

pub fn subscription_id(tag: u8) -> SubscriptionId {
    SubscriptionId::from_bytes([tag; 16])
}

pub fn subscription_directory(root: &Path, subscription_id: SubscriptionId) -> PathBuf {
    root.join(hex(subscription_id.as_bytes()))
}

pub fn materialization<'a>(
    requirements: &'a Path,
    design: &'a Path,
    source_root: &'a Path,
    subscription_id: SubscriptionId,
    target: NodeId,
    partition: u32,
    namespace_digest: [u8; 32],
    initial_position: InitialPosition,
    observation: PollObservation,
) -> (
    CreationCorpusInputs<'a>,
    CreationMaterializationBinding,
    CreationExpectation,
) {
    let resolved = observation.resolve_initial(initial_position).unwrap();
    let expectation = CreationExpectation::new(
        *subscription_id.as_bytes(),
        *target.as_bytes(),
        1,
        partition,
        1,
        namespace_digest,
        initial_position,
        resolved,
        ResourceEpoch::new(
            ResourceId::from_bytes(*observation.resource_epoch().resource_id().as_bytes()),
            observation.resource_epoch().epoch(),
        ),
    );
    (
        CreationCorpusInputs::new(requirements, design, source_root),
        CreationMaterializationBinding::new(
            expectation,
            observation.end_exclusive(),
            observation.oldest_available(),
        ),
        expectation,
    )
}

pub fn recovery_binding(
    directory: &Path,
    subscription_id: SubscriptionId,
    initial_position: InitialPosition,
) -> Result<CreationRecoveryBinding> {
    let canonical = directory.canonicalize()?;
    let mut hasher = Sha256::new();
    hasher.update(DIRECTORY_ID_DOMAIN);
    hasher.update(canonical.as_os_str().as_encoded_bytes());
    Ok(CreationRecoveryBinding::new(
        CheckpointDirectoryId::from_bytes(hasher.finalize().into()),
        subscription_id,
        initial_position,
    ))
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
