use anyhow::{Context, Result, anyhow, bail, ensure};
use chirps_e2e::v07::{
    EvidenceSink, ROOT_PASSWORD, ROOT_USERNAME, ServerProcess, VerifiedArtifact,
};
use iggy::prelude::{CompressionAlgorithm, Identifier, IggyExpiry, MaxTopicSize, TopicClient};
use iggy_common::calculate_checksum;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::Instant;

mod task_6_5_support;

use task_6_5_support::{assert_clean_stop, bootstrap_fixture, sdk_client};

const MATRIX: &str = include_str!("../fixtures/metadata_recovery_cases.toml");
const RESOURCE_DIRECTORY: &str = "state/chirps-resource-identity";
const WAL_PREFIX: &str = "resource_identity.wal.";
const SNAPSHOT_PREFIX: &str = "resource_identity.snapshot.";
const WAL_MAGIC: &[u8; 8] = b"CHRWAL01";
const SNAPSHOT_MAGIC: &[u8; 8] = b"CHRSNAP1";
const WAL_VERSION: u16 = 1;
const SNAPSHOT_VERSION: u16 = 1;
const WAL_COMMIT_MARKER: u64 = 0x4348_5257_414c_434d;
const WAL_PREPARED_MARKER: u64 = 0x4348_5257_414c_5052;
const SNAPSHOT_COMMIT_MARKER: u64 = 0x4348_5250_534e_4150;
const WAL_FRAME_SIZE: usize = 78;
const SNAPSHOT_HEADER_SIZE: usize = 70;
const SNAPSHOT_FIXED_SIZE: usize = 86;
const DIGEST_ENTRY_SIZE: usize = 16;
const BASELINE_RETENTION_BYTES: u64 = 3_u64 << 30;
const MUTATED_RETENTION_BYTES: u64 = 4_u64 << 30;
const MAX_ORACLE_FILE_BYTES: u64 = 64 << 20;

#[derive(Debug, Clone, PartialEq, Eq)]
struct MatrixCase {
    name: String,
    action: String,
    expected: String,
}

#[derive(Debug)]
struct RecoveryMatrix {
    snapshot_stages: Vec<MatrixCase>,
    wal_cases: Vec<MatrixCase>,
}

#[derive(Clone, Copy)]
enum MatrixSection {
    Root,
    Snapshot,
    Wal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Identity {
    uuid: [u8; 16],
    resource_epoch: u64,
    lifecycle_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MutationStatus {
    Completed,
    Prepared,
}

#[derive(Debug, Clone)]
struct Frame {
    generation: u64,
    sequence: u64,
    identity: Identity,
    status: MutationStatus,
    digest: u64,
}

#[derive(Debug)]
struct Snapshot {
    applied_through: u64,
    identity: Identity,
    digests: BTreeMap<u64, u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecoveryTruth {
    generation: u64,
    sequence: u64,
    snapshot_watermark: u64,
    identity: Identity,
    fail_stopped: bool,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run through scripts/run-v07-lane.sh with an attested fault server"]
async fn metadata_wal_and_snapshot_recovery_matches_external_oracle() -> Result<()> {
    let artifact = VerifiedArtifact::from_runner_environment()?;
    require_fault_artifact(&artifact)?;
    let matrix = parse_and_validate_matrix(MATRIX)?;
    let matrix_digest = hex(&Sha256::digest(MATRIX.as_bytes()));
    let mut evidence = EvidenceSink::for_target(artifact.clone(), "durable_metadata_recovery")?;
    evidence.record(
        &format!("fault-artifact-{}", &artifact.sha256[..16]),
        "attested-publish-disabled-test",
    )?;
    evidence.record(
        &format!("metadata-recovery-matrix-{matrix_digest}"),
        "validated-external-oracle-v1",
    )?;

    let mut baseline_server = ServerProcess::new(artifact.binary.clone())?;
    let fixture = bootstrap_fixture(
        &mut baseline_server,
        &artifact,
        "chirps-v07-metadata-recovery",
        "durable-metadata-recovery",
        IggyExpiry::NeverExpire,
    )
    .await?;
    baseline_server.start(false, None).await?;
    let root = sdk_client(&baseline_server, ROOT_USERNAME, ROOT_PASSWORD).await?;
    update_retention(
        &root,
        fixture.stream_id,
        fixture.topic_id,
        BASELINE_RETENTION_BYTES,
    )
    .await?;
    drop(root);
    assert_clean_stop(
        baseline_server
            .stop(Instant::now() + Duration::from_secs(5))
            .await?,
        "metadata-baseline",
    )?;

    let baseline_directory = resource_directory(&baseline_server);
    let baseline = recover_resource(&baseline_directory)
        .context("external oracle rejected the complete metadata baseline")?;
    ensure!(
        baseline.generation == 2
            && baseline.sequence == 3
            && baseline.snapshot_watermark == 3
            && !baseline.fail_stopped,
        "normal destructive mutation did not create the expected complete snapshot baseline"
    );

    for case in &matrix.snapshot_stages {
        run_snapshot_stage_case(
            &artifact,
            &baseline_server,
            baseline,
            fixture.stream_id,
            fixture.topic_id,
            case,
            &mut evidence,
        )
        .await?;
    }

    for case in &matrix.wal_cases {
        run_wal_case(
            &artifact,
            &baseline_server,
            baseline,
            fixture.stream_id,
            fixture.topic_id,
            case,
            &mut evidence,
        )
        .await?;
    }

    Ok(())
}

async fn run_snapshot_stage_case(
    artifact: &VerifiedArtifact,
    baseline_server: &ServerProcess,
    baseline: RecoveryTruth,
    stream_id: u32,
    topic_id: u32,
    case: &MatrixCase,
    evidence: &mut EvidenceSink,
) -> Result<()> {
    let mut server = cloned_server(artifact, baseline_server)?;
    server.start(false, Some(case.action.as_str())).await?;
    let root = sdk_client(&server, ROOT_USERNAME, ROOT_PASSWORD).await?;
    ensure!(
        update_retention(&root, stream_id, topic_id, MUTATED_RETENTION_BYTES)
            .await
            .is_err(),
        "{} failpoint allowed the destructive mutation to report success",
        case.name
    );
    drop(root);
    let crash = server
        .force_stop(Instant::now() + Duration::from_secs(3))
        .await?;
    ensure!(
        crash.forced() && !crash.graceful(),
        "{} did not terminate at a crash boundary",
        case.name
    );

    let before_restart = recover_resource(&resource_directory(&server));
    let expected = case.expected.as_str();
    if expected == "fail-stop" {
        ensure!(
            before_restart.is_err()
                || before_restart
                    .as_ref()
                    .is_ok_and(|truth| truth.fail_stopped),
            "{} external oracle accepted a mixed install",
            case.name
        );
        ensure!(
            server.start(false, None).await.is_err(),
            "{} restarted after an oracle fail-stop verdict",
            case.name
        );
        evidence.record(
            &format!("snapshot-{}-{}", case.name, case.action),
            "fail-stop",
        )?;
        return Ok(());
    }

    let recovered = before_restart
        .with_context(|| format!("{} external oracle rejected persisted state", case.name))?;
    let verdict = if recovered.identity == baseline.identity {
        "old"
    } else if recovered.identity.resource_epoch == baseline.identity.resource_epoch + 1
        && recovered.identity.lifecycle_generation == baseline.identity.lifecycle_generation + 1
        && recovered.identity.uuid != baseline.identity.uuid
    {
        "new"
    } else {
        bail!(
            "{} recovered neither the complete old nor new identity",
            case.name
        );
    };
    ensure!(
        verdict == expected && !recovered.fail_stopped,
        "{} external oracle expected {expected}, got {verdict}",
        case.name
    );

    server.start(false, None).await?;
    assert_topic_retention(&server, stream_id, topic_id, MUTATED_RETENTION_BYTES).await?;
    assert_clean_stop(
        server.stop(Instant::now() + Duration::from_secs(5)).await?,
        &format!("snapshot-{}-restart", case.name),
    )?;
    let stable = recover_resource(&resource_directory(&server))?;
    ensure!(
        stable.identity == recovered.identity && !stable.fail_stopped,
        "{} changed identity after fresh-process replay",
        case.name
    );
    evidence.record(
        &format!("snapshot-{}-{}", case.name, case.action),
        &format!("complete-{verdict}"),
    )?;
    Ok(())
}

async fn run_wal_case(
    artifact: &VerifiedArtifact,
    baseline_server: &ServerProcess,
    baseline: RecoveryTruth,
    stream_id: u32,
    topic_id: u32,
    case: &MatrixCase,
    evidence: &mut EvidenceSink,
) -> Result<()> {
    let mut server = cloned_server(artifact, baseline_server)?;
    if case.action == "complete" {
        server.start(false, Some("snapshot-temp-write")).await?;
        let root = sdk_client(&server, ROOT_USERNAME, ROOT_PASSWORD).await?;
        ensure!(
            update_retention(&root, stream_id, topic_id, MUTATED_RETENTION_BYTES)
                .await
                .is_err(),
            "complete WAL fixture unexpectedly installed a snapshot"
        );
        drop(root);
        let crash = server
            .force_stop(Instant::now() + Duration::from_secs(3))
            .await?;
        ensure!(
            crash.forced() && !crash.graceful(),
            "complete WAL fixture did not stop at its crash boundary"
        );
    } else {
        mutate_recovery_input(&resource_directory(&server), baseline, &case.action)?;
    }
    let oracle = recover_resource(&resource_directory(&server));

    match case.expected.as_str() {
        "fail-stop" => {
            ensure!(
                oracle.is_err() || oracle.as_ref().is_ok_and(|truth| truth.fail_stopped),
                "{} external oracle accepted an invalid WAL/snapshot",
                case.name
            );
            ensure!(
                server.start(false, None).await.is_err(),
                "{} fresh process accepted an oracle-rejected WAL/snapshot",
                case.name
            );
            evidence.record(&format!("wal-{}-{}", case.name, case.action), "fail-stop")?;
        }
        "new" | "unchanged" => {
            let expected = oracle
                .with_context(|| format!("{} external oracle rejected valid input", case.name))?;
            if case.expected == "new" {
                ensure!(
                    expected.identity != baseline.identity
                        && expected.identity.resource_epoch == baseline.identity.resource_epoch + 1
                        && expected.identity.lifecycle_generation
                            == baseline.identity.lifecycle_generation + 1
                        && expected.sequence == baseline.sequence + 2
                        && expected.snapshot_watermark == baseline.snapshot_watermark,
                    "{} did not install one complete new identity",
                    case.name
                );
            } else {
                ensure!(
                    expected.identity == baseline.identity
                        && expected.sequence == baseline.sequence,
                    "{} changed state for an exact watermark duplicate",
                    case.name
                );
            }
            ensure!(!expected.fail_stopped, "{} became fail-stopped", case.name);
            server.start(false, None).await?;
            let retention = if case.expected == "new" {
                MUTATED_RETENTION_BYTES
            } else {
                BASELINE_RETENTION_BYTES
            };
            assert_topic_retention(&server, stream_id, topic_id, retention).await?;
            assert_clean_stop(
                server.stop(Instant::now() + Duration::from_secs(5)).await?,
                &format!("wal-{}-restart", case.name),
            )?;
            let observed = recover_resource(&resource_directory(&server))?;
            ensure!(
                observed == expected,
                "{} server replay disagreed with the external oracle",
                case.name
            );
            evidence.record(
                &format!("wal-{}-{}", case.name, case.action),
                if case.expected == "new" {
                    "complete-new"
                } else {
                    "exact-duplicate-harmless"
                },
            )?;
        }
        other => bail!("unsupported matrix expectation {other}"),
    }
    Ok(())
}

fn require_fault_artifact(artifact: &VerifiedArtifact) -> Result<()> {
    ensure!(
        artifact.lane == "fault" && artifact.kind == "publish-disabled-test",
        "Task 6.13 requires the attested publish-disabled fault artifact"
    );
    ensure!(
        artifact.sha256.len() == 64 && artifact.sha256.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "fault artifact digest is not a SHA-256 identity"
    );
    Ok(())
}

fn cloned_server(
    artifact: &VerifiedArtifact,
    baseline_server: &ServerProcess,
) -> Result<ServerProcess> {
    let server = ServerProcess::new(artifact.binary.clone())?;
    copy_tree(&baseline_server.state_path(), &server.state_path())?;
    Ok(server)
}

fn resource_directory(server: &ServerProcess) -> PathBuf {
    server.state_path().join(RESOURCE_DIRECTORY)
}

async fn update_retention(
    client: &iggy::prelude::IggyClient,
    stream_id: u32,
    topic_id: u32,
    retention_bytes: u64,
) -> Result<()> {
    client
        .update_topic(
            &Identifier::numeric(stream_id)?,
            &Identifier::numeric(topic_id)?,
            "durable-metadata-recovery",
            CompressionAlgorithm::None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::from(retention_bytes),
        )
        .await?;
    Ok(())
}

async fn assert_topic_retention(
    server: &ServerProcess,
    stream_id: u32,
    topic_id: u32,
    expected: u64,
) -> Result<()> {
    let root = sdk_client(server, ROOT_USERNAME, ROOT_PASSWORD).await?;
    let topic = root
        .get_topic(
            &Identifier::numeric(stream_id)?,
            &Identifier::numeric(topic_id)?,
        )
        .await?
        .context("recovered topic is absent")?;
    ensure!(
        topic.max_topic_size.as_bytes_u64() == expected,
        "recovered metadata is neither the expected complete old nor new state"
    );
    Ok(())
}

fn mutate_recovery_input(directory: &Path, baseline: RecoveryTruth, mutation: &str) -> Result<()> {
    let wal = directory.join(format!("{WAL_PREFIX}{}", baseline.generation));
    let fresh = next_identity(baseline.identity);
    let complete = encode_frame(
        baseline.generation,
        baseline.sequence + 1,
        fresh,
        MutationStatus::Completed,
    );
    match mutation {
        "partial" => append_synced(&wal, &complete[..13])?,
        "truncated" => append_synced(&wal, &complete[..WAL_FRAME_SIZE - 1])?,
        "checksum" => {
            let mut bytes = complete;
            bytes[62] ^= 0x80;
            append_synced(&wal, &bytes)?;
        }
        "unknown-version" => {
            let mut bytes = complete;
            bytes[8..10].copy_from_slice(&(WAL_VERSION + 1).to_le_bytes());
            append_synced(&wal, &bytes)?;
        }
        "exact-duplicate" => append_synced(
            &wal,
            &encode_frame(
                baseline.generation,
                baseline.snapshot_watermark,
                baseline.identity,
                MutationStatus::Completed,
            ),
        )?,
        "gap" => append_synced(
            &wal,
            &encode_frame(
                baseline.generation,
                baseline.sequence + 2,
                fresh,
                MutationStatus::Completed,
            ),
        )?,
        "duplicate-mismatch" => append_synced(
            &wal,
            &encode_frame(
                baseline.generation,
                baseline.snapshot_watermark,
                fresh,
                MutationStatus::Completed,
            ),
        )?,
        "corrupt-magic" => {
            let mut bytes = complete;
            bytes[0] ^= 0xff;
            append_synced(&wal, &bytes)?;
        }
        "torn-snapshot" => {
            let path = directory.join(format!("{SNAPSHOT_PREFIX}{}", baseline.generation));
            let bytes = read_regular_bounded(&path)?;
            write_synced(&path, &bytes[..bytes.len() / 2])?;
        }
        "mixed-generation" => {
            let source = directory.join(format!("{SNAPSHOT_PREFIX}{}", baseline.generation));
            let destination =
                directory.join(format!("{SNAPSHOT_PREFIX}{}", baseline.generation + 1));
            write_synced(&destination, &read_regular_bounded(&source)?)?;
        }
        other => bail!("unknown recovery-input mutation {other}"),
    }
    File::open(directory)?.sync_all()?;
    Ok(())
}

fn next_identity(previous: Identity) -> Identity {
    let mut uuid = previous.uuid;
    uuid[0] ^= 0xa5;
    if uuid == [0; 16] || uuid == previous.uuid {
        uuid[15] = uuid[15].wrapping_add(1).max(1);
    }
    Identity {
        uuid,
        resource_epoch: previous.resource_epoch + 1,
        lifecycle_generation: previous.lifecycle_generation + 1,
    }
}

fn encode_frame(
    generation: u64,
    sequence: u64,
    identity: Identity,
    status: MutationStatus,
) -> [u8; WAL_FRAME_SIZE] {
    let mut bytes = [0_u8; WAL_FRAME_SIZE];
    bytes[..8].copy_from_slice(WAL_MAGIC);
    bytes[8..10].copy_from_slice(&WAL_VERSION.to_le_bytes());
    bytes[10..18].copy_from_slice(&generation.to_le_bytes());
    bytes[18..26].copy_from_slice(&sequence.to_le_bytes());
    bytes[26..30].copy_from_slice(&32_u32.to_le_bytes());
    bytes[30..46].copy_from_slice(&identity.uuid);
    bytes[46..54].copy_from_slice(&identity.resource_epoch.to_le_bytes());
    bytes[54..62].copy_from_slice(&identity.lifecycle_generation.to_le_bytes());
    let marker = match status {
        MutationStatus::Completed => WAL_COMMIT_MARKER,
        MutationStatus::Prepared => WAL_PREPARED_MARKER,
    };
    let checksum = wal_checksum(&bytes[8..62], status);
    bytes[62..70].copy_from_slice(&checksum.to_le_bytes());
    bytes[70..78].copy_from_slice(&marker.to_le_bytes());
    bytes
}

fn recover_resource(directory: &Path) -> Result<RecoveryTruth> {
    let mut snapshots = BTreeMap::new();
    let mut wals = BTreeMap::new();
    for entry in fs::read_dir(directory)
        .with_context(|| format!("read resource directory {}", directory.display()))?
    {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        ensure!(
            metadata.is_file() && !metadata.file_type().is_symlink(),
            "resource artifact is not a non-symlink regular file"
        );
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| anyhow!("non-UTF-8 resource artifact"))?;
        if let Some(suffix) = name.strip_prefix(SNAPSHOT_PREFIX) {
            if suffix.ends_with(".tmp") {
                continue;
            }
            let generation = parse_generation(suffix, "snapshot")?;
            ensure!(
                snapshots.insert(generation, entry.path()).is_none(),
                "duplicate snapshot generation"
            );
        } else if let Some(suffix) = name.strip_prefix(WAL_PREFIX) {
            let generation = parse_generation(suffix, "WAL")?;
            ensure!(
                wals.insert(generation, entry.path()).is_none(),
                "duplicate WAL generation"
            );
        }
    }

    let mut decoded_snapshots = BTreeMap::new();
    for (generation, path) in &snapshots {
        decoded_snapshots.insert(*generation, decode_snapshot(path, *generation)?);
    }
    for generation in wals.keys() {
        ensure!(
            *generation == 1 || decoded_snapshots.contains_key(generation),
            "orphan WAL generation {generation}"
        );
    }
    let generation = wals
        .keys()
        .rev()
        .copied()
        .find(|generation| *generation == 1 || decoded_snapshots.contains_key(generation))
        .context("no complete WAL/snapshot generation")?;
    let snapshot = decoded_snapshots.remove(&generation);
    ensure!(
        generation == 1 || snapshot.is_some(),
        "snapshot missing for selected generation"
    );
    let snapshot_watermark = snapshot.as_ref().map_or(0, |value| value.applied_through);
    let mut sequence = snapshot_watermark;
    let mut identity = snapshot.as_ref().map(|value| value.identity);
    let mut status = MutationStatus::Completed;
    let digests = snapshot.map_or_else(BTreeMap::new, |value| value.digests);
    let wal = read_regular_bounded(wals.get(&generation).context("selected WAL missing")?)?;
    ensure!(wal.len() % WAL_FRAME_SIZE == 0, "truncated WAL");
    for bytes in wal.as_chunks::<WAL_FRAME_SIZE>().0 {
        let frame = decode_frame(bytes)?;
        ensure!(
            frame.generation == generation,
            "mixed WAL generation: expected {generation}, got {}",
            frame.generation
        );
        if frame.sequence <= snapshot_watermark {
            ensure!(
                digests.get(&frame.sequence) == Some(&frame.digest),
                "watermark duplicate mismatch at sequence {}",
                frame.sequence
            );
            continue;
        }
        ensure!(
            frame.sequence > sequence,
            "duplicate WAL suffix at sequence {}",
            frame.sequence
        );
        ensure!(
            frame.sequence == sequence + 1,
            "WAL sequence gap: expected {}, got {}",
            sequence + 1,
            frame.sequence
        );
        validate_transition(identity, status, frame.identity, frame.status)?;
        sequence = frame.sequence;
        identity = Some(frame.identity);
        status = frame.status;
    }
    Ok(RecoveryTruth {
        generation,
        sequence,
        snapshot_watermark,
        identity: identity.context("resource identity absent")?,
        fail_stopped: status == MutationStatus::Prepared,
    })
}

fn decode_frame(bytes: &[u8]) -> Result<Frame> {
    ensure!(bytes.len() == WAL_FRAME_SIZE, "invalid WAL frame size");
    ensure!(&bytes[..8] == WAL_MAGIC, "corrupt WAL magic");
    let version = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
    ensure!(version == WAL_VERSION, "unknown WAL version {version}");
    let generation = u64::from_le_bytes(bytes[10..18].try_into().unwrap());
    let sequence = u64::from_le_bytes(bytes[18..26].try_into().unwrap());
    ensure!(
        generation != 0
            && sequence != 0
            && u32::from_le_bytes(bytes[26..30].try_into().unwrap()) == 32,
        "invalid WAL identity envelope"
    );
    let status = match u64::from_le_bytes(bytes[70..78].try_into().unwrap()) {
        WAL_COMMIT_MARKER => MutationStatus::Completed,
        WAL_PREPARED_MARKER => MutationStatus::Prepared,
        _ => bail!("corrupt WAL commit marker"),
    };
    let checksum = u64::from_le_bytes(bytes[62..70].try_into().unwrap());
    ensure!(
        checksum == wal_checksum(&bytes[8..62], status),
        "WAL checksum mismatch at sequence {sequence}"
    );
    let identity = Identity {
        uuid: bytes[30..46].try_into().unwrap(),
        resource_epoch: u64::from_le_bytes(bytes[46..54].try_into().unwrap()),
        lifecycle_generation: u64::from_le_bytes(bytes[54..62].try_into().unwrap()),
    };
    ensure!(valid_identity(identity), "invalid WAL identity");
    Ok(Frame {
        generation,
        sequence,
        identity,
        status,
        digest: mutation_digest(sequence, identity, status),
    })
}

fn decode_snapshot(path: &Path, expected_generation: u64) -> Result<Snapshot> {
    let bytes = read_regular_bounded(path)?;
    ensure!(
        bytes.len() >= SNAPSHOT_FIXED_SIZE,
        "truncated metadata snapshot"
    );
    ensure!(&bytes[..8] == SNAPSHOT_MAGIC, "corrupt snapshot magic");
    let version = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
    ensure!(
        version == SNAPSHOT_VERSION,
        "unknown snapshot version {version}"
    );
    let count = u32::from_le_bytes(bytes[66..70].try_into().unwrap()) as usize;
    ensure!(
        count <= 1_000_000,
        "snapshot digest count exceeds its bound"
    );
    let expected_len = SNAPSHOT_FIXED_SIZE
        .checked_add(
            count
                .checked_mul(DIGEST_ENTRY_SIZE)
                .context("snapshot digest length overflow")?,
        )
        .context("snapshot length overflow")?;
    ensure!(bytes.len() == expected_len, "corrupt snapshot length");
    let checksum_offset = expected_len - 16;
    ensure!(
        u64::from_le_bytes(
            bytes[checksum_offset..checksum_offset + 8]
                .try_into()
                .unwrap()
        ) == calculate_checksum(&bytes[8..checksum_offset]),
        "snapshot checksum mismatch"
    );
    ensure!(
        u64::from_le_bytes(bytes[checksum_offset + 8..].try_into().unwrap())
            == SNAPSHOT_COMMIT_MARKER,
        "corrupt snapshot commit marker"
    );
    let generation = u64::from_le_bytes(bytes[10..18].try_into().unwrap());
    let applied_through = u64::from_le_bytes(bytes[18..26].try_into().unwrap());
    let identity = Identity {
        uuid: bytes[26..42].try_into().unwrap(),
        resource_epoch: u64::from_le_bytes(bytes[42..50].try_into().unwrap()),
        lifecycle_generation: u64::from_le_bytes(bytes[50..58].try_into().unwrap()),
    };
    ensure!(
        generation == expected_generation
            && generation != 0
            && applied_through != 0
            && valid_identity(identity),
        "mixed or invalid snapshot identity"
    );
    let state_digest = u64::from_le_bytes(bytes[58..66].try_into().unwrap());
    let mut digests = BTreeMap::new();
    let mut offset = SNAPSHOT_HEADER_SIZE;
    for _ in 0..count {
        let sequence = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        let digest = u64::from_le_bytes(bytes[offset + 8..offset + 16].try_into().unwrap());
        ensure!(
            sequence != 0
                && sequence <= applied_through
                && digests.insert(sequence, digest).is_none(),
            "invalid snapshot digest sequence"
        );
        offset += DIGEST_ENTRY_SIZE;
    }
    ensure!(
        digests.len() as u64 == applied_through
            && digests.get(&applied_through) == Some(&state_digest)
            && state_digest == identity_digest(applied_through, identity),
        "snapshot state is not bound to its watermark"
    );
    Ok(Snapshot {
        applied_through,
        identity,
        digests,
    })
}

fn validate_transition(
    previous: Option<Identity>,
    previous_status: MutationStatus,
    next: Identity,
    next_status: MutationStatus,
) -> Result<()> {
    if previous_status == MutationStatus::Prepared {
        ensure!(
            next_status == MutationStatus::Completed && previous == Some(next),
            "invalid prepared-to-complete transition"
        );
        return Ok(());
    }
    ensure!(valid_identity(next), "invalid resource identity");
    if let Some(previous) = previous {
        ensure!(
            next.uuid != previous.uuid
                && next.resource_epoch > previous.resource_epoch
                && next.lifecycle_generation > previous.lifecycle_generation,
            "resource identity was reused or did not advance"
        );
    }
    Ok(())
}

fn valid_identity(identity: Identity) -> bool {
    identity.uuid != [0; 16] && identity.resource_epoch != 0 && identity.lifecycle_generation != 0
}

fn wal_checksum(body: &[u8], status: MutationStatus) -> u64 {
    if status == MutationStatus::Completed {
        calculate_checksum(body)
    } else {
        let mut bytes = Vec::with_capacity(body.len() + 8);
        bytes.extend_from_slice(body);
        bytes.extend_from_slice(&WAL_PREPARED_MARKER.to_le_bytes());
        calculate_checksum(&bytes)
    }
}

fn mutation_digest(sequence: u64, identity: Identity, status: MutationStatus) -> u64 {
    if status == MutationStatus::Completed {
        return identity_digest(sequence, identity);
    }
    let mut bytes = Vec::with_capacity(48);
    bytes.extend_from_slice(&sequence.to_le_bytes());
    bytes.extend_from_slice(&identity.uuid);
    bytes.extend_from_slice(&identity.resource_epoch.to_le_bytes());
    bytes.extend_from_slice(&identity.lifecycle_generation.to_le_bytes());
    bytes.extend_from_slice(&WAL_PREPARED_MARKER.to_le_bytes());
    calculate_checksum(&bytes)
}

fn identity_digest(sequence: u64, identity: Identity) -> u64 {
    let mut bytes = [0_u8; 40];
    bytes[..8].copy_from_slice(&sequence.to_le_bytes());
    bytes[8..24].copy_from_slice(&identity.uuid);
    bytes[24..32].copy_from_slice(&identity.resource_epoch.to_le_bytes());
    bytes[32..40].copy_from_slice(&identity.lifecycle_generation.to_le_bytes());
    calculate_checksum(&bytes)
}

fn parse_generation(value: &str, kind: &str) -> Result<u64> {
    let generation = value
        .parse::<u64>()
        .with_context(|| format!("invalid {kind} generation"))?;
    ensure!(generation != 0, "zero {kind} generation");
    Ok(generation)
}

fn parse_and_validate_matrix(input: &str) -> Result<RecoveryMatrix> {
    let mut section = MatrixSection::Root;
    let mut root = BTreeMap::new();
    let mut current = BTreeMap::new();
    let mut snapshot_stages = Vec::new();
    let mut wal_cases = Vec::new();
    for raw in input.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let next = match line {
            "[[snapshot_stage]]" => Some(MatrixSection::Snapshot),
            "[[wal_case]]" => Some(MatrixSection::Wal),
            _ => None,
        };
        if let Some(next) = next {
            push_matrix_case(section, &mut current, &mut snapshot_stages, &mut wal_cases)?;
            section = next;
            continue;
        }
        let (key, value) = line
            .split_once('=')
            .context("metadata recovery matrix contains a non-assignment")?;
        let key = key.trim().to_owned();
        let value = value.trim().trim_matches('"').to_owned();
        let target = if matches!(section, MatrixSection::Root) {
            &mut root
        } else {
            &mut current
        };
        ensure!(
            target.insert(key, value).is_none(),
            "duplicate metadata recovery matrix key"
        );
    }
    push_matrix_case(section, &mut current, &mut snapshot_stages, &mut wal_cases)?;
    ensure!(
        root.get("schema_version").map(String::as_str) == Some("1")
            && root.get("producer_task").map(String::as_str) == Some("6.13")
            && root.get("artifact_kind").map(String::as_str) == Some("publish-disabled-test")
            && root.get("oracle").map(String::as_str) == Some("external-resource-store-v1")
            && root.len() == 4,
        "metadata recovery matrix root attestation is invalid"
    );
    let expected_stages = BTreeSet::from([
        "snapshot-temp-write",
        "snapshot-file-sync",
        "snapshot-rename",
        "snapshot-directory-sync",
        "snapshot-install",
        "snapshot-apply",
    ]);
    ensure!(
        snapshot_stages.len() == expected_stages.len()
            && snapshot_stages
                .iter()
                .map(|case| case.action.as_str())
                .collect::<BTreeSet<_>>()
                == expected_stages
            && snapshot_stages
                .iter()
                .all(|case| case.expected == "new" || case.expected == "fail-stop"),
        "snapshot-stage matrix is incomplete or ambiguous"
    );
    let expected_wal_cases = BTreeSet::from([
        "complete",
        "partial",
        "truncated",
        "checksum",
        "unknown-version",
        "exact-duplicate",
        "gap",
        "duplicate-mismatch",
        "corrupt-magic",
        "torn-snapshot",
        "mixed-generation",
    ]);
    ensure!(
        wal_cases.len() == expected_wal_cases.len()
            && wal_cases
                .iter()
                .map(|case| case.action.as_str())
                .collect::<BTreeSet<_>>()
                == expected_wal_cases,
        "WAL/snapshot replay matrix is incomplete"
    );
    Ok(RecoveryMatrix {
        snapshot_stages,
        wal_cases,
    })
}

fn push_matrix_case(
    section: MatrixSection,
    current: &mut BTreeMap<String, String>,
    snapshot_stages: &mut Vec<MatrixCase>,
    wal_cases: &mut Vec<MatrixCase>,
) -> Result<()> {
    let destination = match section {
        MatrixSection::Root => {
            ensure!(current.is_empty(), "matrix root leaked into a case");
            return Ok(());
        }
        MatrixSection::Snapshot => snapshot_stages,
        MatrixSection::Wal => wal_cases,
    };
    ensure!(
        current.len() == 3,
        "matrix case has an unexpected field set"
    );
    let name = current.remove("name").context("matrix case name missing")?;
    let action_key = if matches!(section, MatrixSection::Snapshot) {
        "failpoint"
    } else {
        "mutation"
    };
    let action = current
        .remove(action_key)
        .context("matrix case action missing")?;
    let expected = current
        .remove("expected")
        .context("matrix case expectation missing")?;
    ensure!(current.is_empty(), "matrix case contains unknown fields");
    ensure!(
        !name.is_empty() && !action.is_empty() && !expected.is_empty(),
        "matrix case contains an empty field"
    );
    destination.push(MatrixCase {
        name,
        action,
        expected,
    });
    Ok(())
}

fn append_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new().append(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn read_regular_bounded(path: &Path) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    ensure!(
        metadata.is_file()
            && !metadata.file_type().is_symlink()
            && metadata.len() <= MAX_ORACLE_FILE_BYTES,
        "oracle input is not a bounded non-symlink regular file"
    );
    let mut file = File::open(path)?;
    let opened = file.metadata()?;
    ensure!(
        opened.len() == metadata.len(),
        "oracle input changed while opening"
    );
    let mut bytes = Vec::with_capacity(opened.len() as usize);
    file.read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 == opened.len(),
        "oracle input changed while reading"
    );
    Ok(bytes)
}

fn copy_tree(source: &Path, destination: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(source)?;
    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "fixture source is not a non-symlink directory"
    );
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let from = entry.path();
        let to = destination.join(entry.file_name());
        let metadata = fs::symlink_metadata(&from)?;
        ensure!(
            !metadata.file_type().is_symlink(),
            "fixture source contains a symlink"
        );
        if metadata.is_dir() {
            copy_tree(&from, &to)?;
        } else {
            ensure!(metadata.is_file(), "fixture source has a special file");
            fs::copy(&from, &to)?;
        }
    }
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
