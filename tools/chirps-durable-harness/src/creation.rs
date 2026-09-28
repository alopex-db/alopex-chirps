//! Independent readback and transition oracle for subscription creation state.

use crate::fs::sync_directory;
use crate::oracle::{CreationState, OracleViolation};
use alopex_chirps_core::durable::{InitialPosition, ResourceEpoch, ResourceId};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path};

#[cfg(target_os = "linux")]
use std::os::unix::fs::OpenOptionsExt;

const CREATION_FILE: &str = "creation.unit";
const OWNER_FILE: &str = "owner.unit";
const FRAME_MAGIC: &[u8; 8] = b"CHRPST07";
const FRAME_VERSION: u16 = 1;
const FRAME_HEADER_LEN: usize = 16;
const FRAME_CHECKSUM_LEN: usize = 32;
const MAX_STATE_FRAME_LEN: usize = 64 * 1024;
const FRAME_DIGEST_DOMAIN: &[u8] = b"chirps-v0.7-state-frame-sha256\0";
const DIRECTORY_ID_DOMAIN: &[u8] = b"chirps-v0.7-checkpoint-directory\0";
const CREATION_KIND: u8 = 1;
const OWNER_KIND: u8 = 2;
const CREATION_FIXED_BODY_LEN: usize = 177;
const OWNER_BODY_LEN: usize = 96;
const JOURNAL_KIND: u8 = 3;
const JOURNAL_BODY_LEN: usize = 100;
const JOURNAL_FILE: &str = "checkpoint.journal";
const JOURNAL_COMMIT_MARKER: &[u8; 8] = b"COMMIT07";
const CORPUS_SOURCE_DOMAIN: &[u8] = b"chirps-v0.7-task-4.1-source-input\0";
const MATERIALIZED_OWNER_DOMAIN: &[u8] = b"chirps-v0.7-task-6.6-owner\0";
const MAX_CORPUS_MANIFEST_LEN: u64 = 1024 * 1024;
const MAX_CORPUS_INPUT_LEN: u64 = 16 * 1024 * 1024;
const TEMPLATE_PATH: &str = "directory-sync-new/creation.unit";
const DYNAMIC_FIELDS: &[&str] = &[
    "checkpoint_directory_id",
    "subscription_id",
    "target",
    "generation",
    "partition",
    "lifecycle_generation",
    "namespace_digest",
    "initial_position",
    "resolved_initial_offset",
    "captured_end_exclusive",
    "captured_oldest_available",
    "resource_id",
    "resource_epoch",
    "genesis_owner_id",
];
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
#[cfg(target_os = "linux")]
const O_NOFOLLOW: i32 = 0o400000;
#[cfg(target_os = "linux")]
const ELOOP: i32 = 40;

/// Independently decoded owner record and its exact encoded digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerReadback {
    creation_digest: [u8; 32],
    previous_owner_digest: [u8; 32],
    owner_epoch: u64,
    owner_generation: u64,
    owner_id: [u8; 16],
    record_digest: [u8; 32],
}

impl OwnerReadback {
    #[must_use]
    pub const fn creation_digest(&self) -> [u8; 32] {
        self.creation_digest
    }

    #[must_use]
    pub const fn previous_owner_digest(&self) -> [u8; 32] {
        self.previous_owner_digest
    }

    #[must_use]
    pub const fn owner_epoch(&self) -> u64 {
        self.owner_epoch
    }

    #[must_use]
    pub const fn owner_generation(&self) -> u64 {
        self.owner_generation
    }

    #[must_use]
    pub const fn owner_id(&self) -> [u8; 16] {
        self.owner_id
    }

    #[must_use]
    pub const fn record_digest(&self) -> [u8; 32] {
        self.record_digest
    }
}

/// Complete immutable CreationUnit projection decoded without production state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreationManifestReadback {
    checkpoint_directory_id: [u8; 32],
    subscription_id: [u8; 16],
    target: [u8; 16],
    generation: u64,
    partition: u32,
    lifecycle_generation: u64,
    namespace_digest: [u8; 32],
    initial_position: InitialPosition,
    resolved_initial_offset: u64,
    captured_end_exclusive: u64,
    captured_oldest_available: u64,
    captured_resource_epoch: ResourceEpoch,
    creation_digest: [u8; 32],
    genesis_owner: OwnerReadback,
}

impl CreationManifestReadback {
    #[must_use]
    pub const fn checkpoint_directory_id(&self) -> [u8; 32] {
        self.checkpoint_directory_id
    }

    #[must_use]
    pub const fn subscription_id(&self) -> [u8; 16] {
        self.subscription_id
    }

    #[must_use]
    pub const fn target(&self) -> [u8; 16] {
        self.target
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn partition(&self) -> u32 {
        self.partition
    }

    #[must_use]
    pub const fn lifecycle_generation(&self) -> u64 {
        self.lifecycle_generation
    }

    #[must_use]
    pub const fn namespace_digest(&self) -> [u8; 32] {
        self.namespace_digest
    }

    #[must_use]
    pub const fn initial_position(&self) -> InitialPosition {
        self.initial_position
    }

    #[must_use]
    pub const fn resolved_initial_offset(&self) -> u64 {
        self.resolved_initial_offset
    }

    #[must_use]
    pub const fn captured_end_exclusive(&self) -> u64 {
        self.captured_end_exclusive
    }

    #[must_use]
    pub const fn captured_oldest_available(&self) -> u64 {
        self.captured_oldest_available
    }

    #[must_use]
    pub const fn captured_resource_epoch(&self) -> ResourceEpoch {
        self.captured_resource_epoch
    }

    #[must_use]
    pub const fn creation_digest(&self) -> [u8; 32] {
        self.creation_digest
    }

    #[must_use]
    pub const fn genesis_owner(&self) -> &OwnerReadback {
        &self.genesis_owner
    }
}

/// Canonical manifest and effective current owner observed at one boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreationSnapshot {
    manifest: Option<CreationManifestReadback>,
    current_owner: Option<OwnerReadback>,
    owner_file_present: bool,
}

/// Exact immutable creation fields expected by one public operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreationExpectation {
    subscription_id: [u8; 16],
    target: [u8; 16],
    generation: u64,
    partition: u32,
    lifecycle_generation: u64,
    namespace_digest: [u8; 32],
    initial_position: InitialPosition,
    resolved_initial_offset: u64,
    resource_epoch: ResourceEpoch,
}

impl CreationExpectation {
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        subscription_id: [u8; 16],
        target: [u8; 16],
        generation: u64,
        partition: u32,
        lifecycle_generation: u64,
        namespace_digest: [u8; 32],
        initial_position: InitialPosition,
        resolved_initial_offset: u64,
        resource_epoch: ResourceEpoch,
    ) -> Self {
        Self {
            subscription_id,
            target,
            generation,
            partition,
            lifecycle_generation,
            namespace_digest,
            initial_position,
            resolved_initial_offset,
            resource_epoch,
        }
    }
}

impl CreationSnapshot {
    #[must_use]
    pub const fn manifest(&self) -> Option<&CreationManifestReadback> {
        self.manifest.as_ref()
    }

    #[must_use]
    pub const fn current_owner(&self) -> Option<&OwnerReadback> {
        self.current_owner.as_ref()
    }

    #[must_use]
    pub const fn owner_file_present(&self) -> bool {
        self.owner_file_present
    }
}

/// One public creation/open/recovery outcome bracketed by independent readback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreationTransitionObservation {
    outcome: CreationState,
    expectation: CreationExpectation,
    before: CreationSnapshot,
    after: CreationSnapshot,
}

impl CreationTransitionObservation {
    #[must_use]
    pub const fn new(
        outcome: CreationState,
        expectation: CreationExpectation,
        before: CreationSnapshot,
        after: CreationSnapshot,
    ) -> Self {
        Self {
            outcome,
            expectation,
            before,
            after,
        }
    }

    #[must_use]
    pub const fn outcome(&self) -> CreationState {
        self.outcome
    }

    #[must_use]
    pub const fn before(&self) -> &CreationSnapshot {
        &self.before
    }

    #[must_use]
    pub const fn after(&self) -> &CreationSnapshot {
        &self.after
    }
}

/// Exact terminal facts proved by a standalone creation history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreationVerdict {
    resolved_new: bool,
    final_owner_epoch: Option<u64>,
    creation_digest: Option<[u8; 32]>,
}

impl CreationVerdict {
    #[must_use]
    pub const fn resolved_new(self) -> bool {
        self.resolved_new
    }

    #[must_use]
    pub const fn final_owner_epoch(self) -> Option<u64> {
        self.final_owner_epoch
    }

    #[must_use]
    pub const fn creation_digest(self) -> Option<[u8; 32]> {
        self.creation_digest
    }
}

/// A malformed or unprovable canonical creation snapshot.
#[derive(Debug)]
pub enum CreationReadbackError {
    Io(std::io::Error),
    NotDirectory,
    Symlink,
    InvalidFileType,
    InvalidFrame(&'static str),
    DirectoryDigestMismatch,
    OwnerChain,
}

impl Display for CreationReadbackError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "creation readback I/O failed: {error}"),
            Self::NotDirectory => formatter.write_str("creation readback root is not a directory"),
            Self::Symlink => formatter.write_str("creation readback refuses symlinks"),
            Self::InvalidFileType => {
                formatter.write_str("creation readback state path is not a regular file")
            }
            Self::InvalidFrame(reason) => write!(formatter, "invalid creation frame: {reason}"),
            Self::DirectoryDigestMismatch => {
                formatter.write_str("creation frame belongs to another canonical directory")
            }
            Self::OwnerChain => formatter.write_str("creation owner chain is invalid"),
        }
    }
}

impl Error for CreationReadbackError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for CreationReadbackError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

/// Inputs whose exact bytes bind the frozen Task 4.1 corpus.
#[derive(Debug, Clone, Copy)]
pub struct CreationCorpusInputs<'a> {
    requirements: &'a Path,
    design: &'a Path,
    source_root: &'a Path,
}

impl<'a> CreationCorpusInputs<'a> {
    #[must_use]
    pub const fn new(requirements: &'a Path, design: &'a Path, source_root: &'a Path) -> Self {
        Self {
            requirements,
            design,
            source_root,
        }
    }
}

/// Live values that may replace the explicitly allowlisted creation fields.
#[derive(Debug, Clone, Copy)]
pub struct CreationMaterializationBinding {
    expectation: CreationExpectation,
    captured_end_exclusive: u64,
    captured_oldest_available: u64,
}

impl CreationMaterializationBinding {
    #[must_use]
    pub const fn new(
        expectation: CreationExpectation,
        captured_end_exclusive: u64,
        captured_oldest_available: u64,
    ) -> Self {
        Self {
            expectation,
            captured_end_exclusive,
            captured_oldest_available,
        }
    }
}

/// Frozen crash-stage classification carried into one materialized image.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreationCorpusOracle {
    Old,
    UnknownOld,
    UnknownNew,
    New,
}

/// Digests and oracle identity for one independently materialized case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedCreationCase {
    case_name: String,
    stage: String,
    oracle: CreationCorpusOracle,
    creation_sha256: [u8; 32],
    image_sha256: Option<[u8; 32]>,
    journal_sha256: Option<[u8; 32]>,
}

impl MaterializedCreationCase {
    #[must_use]
    pub fn case_name(&self) -> &str {
        &self.case_name
    }

    #[must_use]
    pub fn stage(&self) -> &str {
        &self.stage
    }

    #[must_use]
    pub const fn oracle(&self) -> CreationCorpusOracle {
        self.oracle
    }

    #[must_use]
    pub const fn creation_sha256(&self) -> [u8; 32] {
        self.creation_sha256
    }

    #[must_use]
    pub const fn image_sha256(&self) -> Option<[u8; 32]> {
        self.image_sha256
    }

    #[must_use]
    pub const fn journal_sha256(&self) -> Option<[u8; 32]> {
        self.journal_sha256
    }
}

/// A malformed, stale, or unsafe frozen creation corpus.
#[derive(Debug)]
pub enum CreationCorpusError {
    Io(std::io::Error),
    InvalidManifest(&'static str),
    InvalidFrame(&'static str),
    DigestMismatch(&'static str),
    UnsafePath,
    DestinationNotEmpty,
}

impl Display for CreationCorpusError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "creation corpus I/O failed: {error}"),
            Self::InvalidManifest(reason) => {
                write!(formatter, "invalid creation corpus manifest: {reason}")
            }
            Self::InvalidFrame(reason) => write!(formatter, "invalid creation template: {reason}"),
            Self::DigestMismatch(input) => write!(formatter, "creation corpus {input} is stale"),
            Self::UnsafePath => formatter.write_str("creation corpus contains an unsafe path"),
            Self::DestinationNotEmpty => {
                formatter.write_str("creation materialization destination is not empty")
            }
        }
    }
}

impl Error for CreationCorpusError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for CreationCorpusError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FrozenCreationCorpus {
    schema_version: u32,
    producer_task: String,
    requirements_sha256: String,
    design_sha256: String,
    source_input_sha256: String,
    sources: Vec<FrozenSource>,
    materialization: FrozenMaterialization,
    cases: Vec<FrozenCase>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FrozenSource {
    path: String,
    sha256: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FrozenMaterialization {
    template: String,
    template_sha256: String,
    dynamic_fields: Vec<String>,
    canonical_journal: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FrozenCase {
    name: String,
    stage: String,
    oracle: String,
    file: String,
    sha256: String,
}

#[derive(Clone, Copy)]
enum MaterializedImage {
    None,
    PartialPending,
    FullPending,
    Canonical,
}

const FROZEN_CASES: &[(&str, &str, &str, &str, MaterializedImage)] = &[
    (
        "write-old",
        "write",
        "old",
        ".creation.unit.pending",
        MaterializedImage::PartialPending,
    ),
    (
        "file-sync-old",
        "file-sync",
        "old",
        ".creation.unit.pending",
        MaterializedImage::FullPending,
    ),
    (
        "rename-old-unknown",
        "rename",
        "unknown-old",
        ".creation.unit.pending",
        MaterializedImage::FullPending,
    ),
    (
        "rename-new-unknown",
        "rename",
        "unknown-new",
        CREATION_FILE,
        MaterializedImage::Canonical,
    ),
    (
        "directory-sync-old-unknown",
        "directory-sync",
        "unknown-old",
        "",
        MaterializedImage::None,
    ),
    (
        "directory-sync-new-unknown",
        "directory-sync",
        "unknown-new",
        CREATION_FILE,
        MaterializedImage::Canonical,
    ),
    (
        "directory-sync-new",
        "directory-sync",
        "new",
        CREATION_FILE,
        MaterializedImage::Canonical,
    ),
];

/// Validates the frozen schema and materializes one case into its final live directory.
///
/// The complete CreationUnit and empty genesis journal are independently framed from the
/// allowlisted live fields. Missing journals are never synthesized by a later open operation.
pub fn materialize_creation_case(
    corpus_root: &Path,
    case_name: &str,
    destination: &Path,
    inputs: CreationCorpusInputs<'_>,
    binding: CreationMaterializationBinding,
) -> Result<MaterializedCreationCase, CreationCorpusError> {
    let manifest = validate_frozen_corpus(corpus_root, inputs)?;
    let (case, image) = manifest
        .cases
        .iter()
        .zip(FROZEN_CASES)
        .find_map(|(case, expected)| (case.name == case_name).then_some((case, expected.4)))
        .ok_or(CreationCorpusError::InvalidManifest("unknown case"))?;
    prepare_destination(destination)?;

    let template =
        read_regular_bounded(&corpus_root.join(TEMPLATE_PATH), MAX_STATE_FRAME_LEN as u64)?;
    let creation = materialize_creation_frame(&template, destination, binding)?;
    let journal = materialize_journal_frame(binding)?;
    let (image_sha256, journal_sha256) = match image {
        MaterializedImage::None => (None, None),
        MaterializedImage::PartialPending => {
            let bytes = &creation[..creation.len() / 2];
            write_synced_new(&destination.join(".creation.unit.pending"), bytes)?;
            (Some(Sha256::digest(bytes).into()), None)
        }
        MaterializedImage::FullPending => {
            write_synced_new(&destination.join(".creation.unit.pending"), &creation)?;
            (Some(Sha256::digest(&creation).into()), None)
        }
        MaterializedImage::Canonical => {
            write_synced_new(&destination.join(CREATION_FILE), &creation)?;
            write_synced_new(&destination.join(JOURNAL_FILE), &journal)?;
            sync_directory(destination)?;
            let snapshot = read_creation_snapshot(destination)
                .map_err(|_| CreationCorpusError::InvalidFrame("materialized readback"))?;
            validate_expectation(&snapshot, binding.expectation)
                .map_err(|_| CreationCorpusError::InvalidFrame("materialized namespace"))?;
            let materialized = snapshot
                .manifest()
                .ok_or(CreationCorpusError::InvalidFrame(
                    "missing materialized creation",
                ))?;
            if materialized.captured_end_exclusive() != binding.captured_end_exclusive
                || materialized.captured_oldest_available() != binding.captured_oldest_available
            {
                return Err(CreationCorpusError::InvalidFrame(
                    "materialized observation mismatch",
                ));
            }
            (
                Some(Sha256::digest(&creation).into()),
                Some(Sha256::digest(&journal).into()),
            )
        }
    };
    sync_directory(destination)?;
    Ok(MaterializedCreationCase {
        case_name: case.name.clone(),
        stage: case.stage.clone(),
        oracle: parse_oracle(&case.oracle)?,
        creation_sha256: Sha256::digest(&creation).into(),
        image_sha256,
        journal_sha256,
    })
}

fn validate_frozen_corpus(
    root: &Path,
    inputs: CreationCorpusInputs<'_>,
) -> Result<FrozenCreationCorpus, CreationCorpusError> {
    ensure_directory(root)?;
    ensure_directory(inputs.source_root)?;
    let manifest_bytes =
        read_regular_bounded(&root.join("manifest.json"), MAX_CORPUS_MANIFEST_LEN)?;
    let manifest: FrozenCreationCorpus = serde_json::from_slice(&manifest_bytes)
        .map_err(|_| CreationCorpusError::InvalidManifest("JSON schema"))?;
    if manifest.schema_version != 2 || manifest.producer_task != "4.1" {
        return Err(CreationCorpusError::InvalidManifest(
            "schema version or producer task",
        ));
    }
    if manifest.materialization.template != TEMPLATE_PATH
        || manifest.materialization.canonical_journal != JOURNAL_FILE
        || manifest.materialization.dynamic_fields
            != DYNAMIC_FIELDS
                .iter()
                .map(|field| (*field).to_owned())
                .collect::<Vec<_>>()
    {
        return Err(CreationCorpusError::InvalidManifest(
            "materialization contract",
        ));
    }
    if manifest.sources.len() != SOURCE_PATHS.len() || manifest.cases.len() != FROZEN_CASES.len() {
        return Err(CreationCorpusError::InvalidManifest("inventory length"));
    }
    let requirements = read_regular_bounded(inputs.requirements, MAX_CORPUS_INPUT_LEN)?;
    let design = read_regular_bounded(inputs.design, MAX_CORPUS_INPUT_LEN)?;
    require_digest(&requirements, &manifest.requirements_sha256, "requirements")?;
    require_digest(&design, &manifest.design_sha256, "design")?;

    let mut source_projection = Vec::new();
    for (source, expected_path) in manifest.sources.iter().zip(SOURCE_PATHS) {
        if source.path != *expected_path || !safe_relative_path(&source.path) {
            return Err(CreationCorpusError::InvalidManifest("source catalog"));
        }
        let bytes =
            read_regular_bounded(&inputs.source_root.join(&source.path), MAX_CORPUS_INPUT_LEN)?;
        require_digest(&bytes, &source.sha256, "source")?;
        source_projection.extend_from_slice(&(source.path.len() as u32).to_be_bytes());
        source_projection.extend_from_slice(source.path.as_bytes());
        source_projection.extend_from_slice(&Sha256::digest(&bytes));
    }
    let source_digest = digest_parts(&[CORPUS_SOURCE_DOMAIN, &source_projection]);
    if parse_digest(&manifest.source_input_sha256)? != source_digest {
        return Err(CreationCorpusError::DigestMismatch("source input"));
    }

    let mut root_entries = BTreeSet::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_type()?.is_symlink() {
            return Err(CreationCorpusError::UnsafePath);
        }
        root_entries.insert(
            entry
                .file_name()
                .to_str()
                .ok_or(CreationCorpusError::UnsafePath)?
                .to_owned(),
        );
    }
    let expected_root_entries = std::iter::once("manifest.json".to_owned())
        .chain(FROZEN_CASES.iter().map(|case| case.0.to_owned()))
        .collect::<BTreeSet<_>>();
    if root_entries != expected_root_entries {
        return Err(CreationCorpusError::InvalidManifest("root inventory"));
    }

    for (case, expected) in manifest.cases.iter().zip(FROZEN_CASES) {
        if case.name != expected.0
            || case.stage != expected.1
            || case.oracle != expected.2
            || case.file != expected.3
            || !safe_relative_path(&case.name)
        {
            return Err(CreationCorpusError::InvalidManifest("case catalog"));
        }
        let directory = root.join(&case.name);
        ensure_directory(&directory)?;
        let entries = fs::read_dir(&directory)?.collect::<Result<Vec<_>, _>>()?;
        if case.file.is_empty() {
            if !entries.is_empty() || !case.sha256.is_empty() {
                return Err(CreationCorpusError::InvalidManifest("empty case"));
            }
        } else {
            if entries.len() != 1
                || entries[0].file_name().to_str() != Some(case.file.as_str())
                || !safe_relative_path(&case.file)
            {
                return Err(CreationCorpusError::InvalidManifest("case inventory"));
            }
            let bytes =
                read_regular_bounded(&directory.join(&case.file), MAX_STATE_FRAME_LEN as u64)?;
            require_digest(&bytes, &case.sha256, "case image")?;
        }
    }
    let template = read_regular_bounded(&root.join(TEMPLATE_PATH), MAX_STATE_FRAME_LEN as u64)?;
    require_digest(
        &template,
        &manifest.materialization.template_sha256,
        "creation template",
    )?;
    decode_creation(&template)
        .map_err(|_| CreationCorpusError::InvalidFrame("template CreationUnit"))?;
    Ok(manifest)
}

fn materialize_creation_frame(
    template: &[u8],
    destination: &Path,
    binding: CreationMaterializationBinding,
) -> Result<Vec<u8>, CreationCorpusError> {
    let template_body = decode_frame(template, CREATION_KIND)
        .map_err(|_| CreationCorpusError::InvalidFrame("template frame"))?;
    if template_body.len() < CREATION_FIXED_BODY_LEN {
        return Err(CreationCorpusError::InvalidFrame("template body"));
    }
    validate_materialization_binding(binding)?;
    let canonical = fs::canonicalize(destination)?;
    let directory_id = digest_parts(&[
        DIRECTORY_ID_DOMAIN,
        canonical.as_os_str().as_encoded_bytes(),
    ]);
    let expectation = binding.expectation;
    let mut body = template_body.to_vec();
    body[0..32].copy_from_slice(&directory_id);
    body[32..48].copy_from_slice(&expectation.subscription_id);
    body[48..64].copy_from_slice(&expectation.target);
    body[64..72].copy_from_slice(&expectation.generation.to_be_bytes());
    body[72..76].copy_from_slice(&expectation.partition.to_be_bytes());
    body[76..84].copy_from_slice(&expectation.lifecycle_generation.to_be_bytes());
    body[84..116].copy_from_slice(&expectation.namespace_digest);
    let (selection, exact) = match expectation.initial_position {
        InitialPosition::EarliestRetained => (1, 0),
        InitialPosition::LatestAfterCapturedEnd => (2, 0),
        InitialPosition::Exact(offset) => (3, offset),
    };
    body[116] = selection;
    body[117..125].copy_from_slice(&exact.to_be_bytes());
    body[125..133].copy_from_slice(&expectation.resolved_initial_offset.to_be_bytes());
    body[133..141].copy_from_slice(&binding.captured_end_exclusive.to_be_bytes());
    body[141..149].copy_from_slice(&binding.captured_oldest_available.to_be_bytes());
    body[149..165].copy_from_slice(expectation.resource_epoch.resource_id().as_bytes());
    body[165..173].copy_from_slice(&expectation.resource_epoch.epoch().to_be_bytes());
    let owner_len = u32::from_be_bytes(
        body[173..177]
            .try_into()
            .map_err(|_| CreationCorpusError::InvalidFrame("owner length"))?,
    ) as usize;
    if body.len() != CREATION_FIXED_BODY_LEN + owner_len {
        return Err(CreationCorpusError::InvalidFrame("owner length"));
    }
    let owner = decode_frame(&body[177..], OWNER_KIND)
        .map_err(|_| CreationCorpusError::InvalidFrame("genesis owner"))?;
    if owner.len() != OWNER_BODY_LEN
        || owner[..64] != [0; 64]
        || owner[64..72] != 1_u64.to_be_bytes()
        || owner[72..80] != 1_u64.to_be_bytes()
    {
        return Err(CreationCorpusError::InvalidFrame("genesis owner"));
    }
    let mut owner_body = owner.to_vec();
    let seed = digest_parts(&[
        MATERIALIZED_OWNER_DOMAIN,
        &directory_id,
        &expectation.subscription_id,
        &expectation.target,
        &expectation.generation.to_be_bytes(),
        &expectation.partition.to_be_bytes(),
        &expectation.lifecycle_generation.to_be_bytes(),
        &expectation.namespace_digest,
    ]);
    owner_body[80..96].copy_from_slice(&seed[..16]);
    owner_body[86] = 0x40 | (owner_body[86] & 0x0f);
    owner_body[88] = 0x80 | (owner_body[88] & 0x3f);
    let owner_frame = encode_frame(OWNER_KIND, &owner_body)?;
    if owner_frame.len() != owner_len {
        return Err(CreationCorpusError::InvalidFrame("genesis owner size"));
    }
    body[177..].copy_from_slice(&owner_frame);
    encode_frame(CREATION_KIND, &body)
}

fn materialize_journal_frame(
    binding: CreationMaterializationBinding,
) -> Result<Vec<u8>, CreationCorpusError> {
    let expectation = binding.expectation;
    let mut body = [0_u8; JOURNAL_BODY_LEN];
    body[0..8].copy_from_slice(&1_u64.to_be_bytes());
    body[8..24].copy_from_slice(&expectation.subscription_id);
    body[24..40].copy_from_slice(&expectation.target);
    body[40..48].copy_from_slice(&expectation.generation.to_be_bytes());
    body[48..52].copy_from_slice(&expectation.partition.to_be_bytes());
    body[52..60].copy_from_slice(&expectation.lifecycle_generation.to_be_bytes());
    body[60..68].copy_from_slice(&expectation.resolved_initial_offset.to_be_bytes());
    body[68..84].copy_from_slice(expectation.resource_epoch.resource_id().as_bytes());
    body[84..92].copy_from_slice(&expectation.resource_epoch.epoch().to_be_bytes());
    body[92..100].copy_from_slice(JOURNAL_COMMIT_MARKER);
    encode_frame(JOURNAL_KIND, &body)
}

fn validate_materialization_binding(
    binding: CreationMaterializationBinding,
) -> Result<(), CreationCorpusError> {
    if binding.captured_oldest_available > binding.captured_end_exclusive
        || binding.expectation.resolved_initial_offset
            != match binding.expectation.initial_position {
                InitialPosition::EarliestRetained => binding.captured_oldest_available,
                InitialPosition::LatestAfterCapturedEnd => binding.captured_end_exclusive,
                InitialPosition::Exact(offset)
                    if binding.captured_oldest_available <= offset
                        && offset <= binding.captured_end_exclusive =>
                {
                    offset
                }
                InitialPosition::Exact(_) => {
                    return Err(CreationCorpusError::InvalidFrame("initial position"));
                }
            }
    {
        return Err(CreationCorpusError::InvalidFrame("initial position"));
    }
    Ok(())
}

fn encode_frame(kind: u8, body: &[u8]) -> Result<Vec<u8>, CreationCorpusError> {
    let body_len = u32::try_from(body.len())
        .map_err(|_| CreationCorpusError::InvalidFrame("frame too large"))?;
    let total = FRAME_HEADER_LEN
        .checked_add(body.len())
        .and_then(|length| length.checked_add(FRAME_CHECKSUM_LEN))
        .ok_or(CreationCorpusError::InvalidFrame("frame too large"))?;
    if total > MAX_STATE_FRAME_LEN {
        return Err(CreationCorpusError::InvalidFrame("frame too large"));
    }
    let mut bytes = Vec::with_capacity(total);
    bytes.extend_from_slice(FRAME_MAGIC);
    bytes.extend_from_slice(&FRAME_VERSION.to_be_bytes());
    bytes.push(kind);
    bytes.push(0);
    bytes.extend_from_slice(&body_len.to_be_bytes());
    bytes.extend_from_slice(body);
    let checksum = digest_parts(&[FRAME_DIGEST_DOMAIN, &bytes]);
    bytes.extend_from_slice(&checksum);
    Ok(bytes)
}

fn parse_oracle(value: &str) -> Result<CreationCorpusOracle, CreationCorpusError> {
    match value {
        "old" => Ok(CreationCorpusOracle::Old),
        "unknown-old" => Ok(CreationCorpusOracle::UnknownOld),
        "unknown-new" => Ok(CreationCorpusOracle::UnknownNew),
        "new" => Ok(CreationCorpusOracle::New),
        _ => Err(CreationCorpusError::InvalidManifest("case oracle")),
    }
}

fn prepare_destination(path: &Path) -> Result<(), CreationCorpusError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(CreationCorpusError::UnsafePath);
            }
            if fs::read_dir(path)?.next().transpose()?.is_some() {
                return Err(CreationCorpusError::DestinationNotEmpty);
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => fs::create_dir(path)?,
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn ensure_directory(path: &Path) -> Result<(), CreationCorpusError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(CreationCorpusError::UnsafePath);
    }
    Ok(())
}

fn safe_relative_path(value: &str) -> bool {
    !value.is_empty()
        && Path::new(value)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn read_regular_bounded(path: &Path, limit: u64) -> Result<Vec<u8>, CreationCorpusError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > limit {
        return Err(CreationCorpusError::UnsafePath);
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(target_os = "linux")]
    options.custom_flags(O_NOFOLLOW);
    let mut file = options.open(path)?;
    let opened = file.metadata()?;
    if !opened.is_file() || opened.len() > limit {
        return Err(CreationCorpusError::UnsafePath);
    }
    let mut bytes = Vec::with_capacity(opened.len() as usize);
    Read::by_ref(&mut file)
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != opened.len() || bytes.len() as u64 > limit {
        return Err(CreationCorpusError::UnsafePath);
    }
    Ok(bytes)
}

fn require_digest(
    bytes: &[u8],
    expected: &str,
    input: &'static str,
) -> Result<(), CreationCorpusError> {
    let actual: [u8; 32] = Sha256::digest(bytes).into();
    if parse_digest(expected)? != actual {
        return Err(CreationCorpusError::DigestMismatch(input));
    }
    Ok(())
}

fn parse_digest(value: &str) -> Result<[u8; 32], CreationCorpusError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(CreationCorpusError::InvalidManifest("SHA-256"));
    }
    let mut digest = [0_u8; 32];
    for (index, pair) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let text = std::str::from_utf8(pair)
            .map_err(|_| CreationCorpusError::InvalidManifest("SHA-256"))?;
        digest[index] = u8::from_str_radix(text, 16)
            .map_err(|_| CreationCorpusError::InvalidManifest("SHA-256"))?;
    }
    Ok(digest)
}

fn write_synced_new(path: &Path, bytes: &[u8]) -> Result<(), CreationCorpusError> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

/// Reads canonical creation and owner files through an independent bounded codec.
pub fn read_creation_snapshot(directory: &Path) -> Result<CreationSnapshot, CreationReadbackError> {
    let directory_metadata = fs::symlink_metadata(directory)?;
    if directory_metadata.file_type().is_symlink() {
        return Err(CreationReadbackError::Symlink);
    }
    if !directory_metadata.is_dir() {
        return Err(CreationReadbackError::NotDirectory);
    }
    let canonical = fs::canonicalize(directory)?;
    let expected_directory_id = digest_parts(&[
        DIRECTORY_ID_DOMAIN,
        canonical.as_os_str().as_encoded_bytes(),
    ]);
    let creation_bytes = read_optional_regular_file(&directory.join(CREATION_FILE))?;
    let owner_bytes = read_optional_regular_file(&directory.join(OWNER_FILE))?;
    let Some(creation_bytes) = creation_bytes else {
        if owner_bytes.is_some() {
            return Err(CreationReadbackError::OwnerChain);
        }
        return Ok(CreationSnapshot {
            manifest: None,
            current_owner: None,
            owner_file_present: false,
        });
    };

    let manifest = decode_creation(&creation_bytes)?;
    if manifest.checkpoint_directory_id != expected_directory_id {
        return Err(CreationReadbackError::DirectoryDigestMismatch);
    }
    let (current_owner, owner_file_present) = if let Some(owner_bytes) = owner_bytes {
        let owner = decode_owner(&owner_bytes)?;
        if owner.owner_epoch <= 1
            || owner.creation_digest != manifest.creation_digest
            || owner.previous_owner_digest == [0; 32]
            || (owner.owner_epoch == 2
                && owner.previous_owner_digest != manifest.genesis_owner.record_digest)
        {
            return Err(CreationReadbackError::OwnerChain);
        }
        (owner, true)
    } else {
        (manifest.genesis_owner.clone(), false)
    };
    Ok(CreationSnapshot {
        manifest: Some(manifest),
        current_owner: Some(current_owner),
        owner_file_present,
    })
}

/// Verifies exact old/new/unknown transitions without an append attempt.
pub fn verify_creation_history(
    transitions: &[CreationTransitionObservation],
) -> Result<CreationVerdict, OracleViolation> {
    let first = transitions
        .first()
        .ok_or(OracleViolation::InvalidCreationRecovery)?;
    let expectation = first.expectation;
    let mut previous_after: Option<&CreationSnapshot> = None;
    let mut resolved_new = false;

    for transition in transitions {
        if transition.expectation != expectation
            || previous_after.is_some_and(|previous| previous != &transition.before)
        {
            return Err(OracleViolation::CreationExpectationMismatch);
        }
        if previous_after.is_none() && !has_anchored_owner_chain(&transition.before) {
            return Err(OracleViolation::InvalidCreationRecovery);
        }
        validate_expectation(&transition.before, expectation)?;
        validate_expectation(&transition.after, expectation)?;
        let unchanged = transition.before == transition.after;
        let advanced = creation_advanced(&transition.before, &transition.after);
        let valid = match transition.outcome {
            CreationState::CreationNotCommitted => unchanged,
            CreationState::CreationUnknown => unchanged || advanced,
            CreationState::Created => advanced,
        };
        if !valid {
            return Err(OracleViolation::InvalidCreationRecovery);
        }
        resolved_new |= advanced;
        previous_after = Some(&transition.after);
    }

    let final_snapshot = &transitions
        .last()
        .ok_or(OracleViolation::InvalidCreationRecovery)?
        .after;
    Ok(CreationVerdict {
        resolved_new,
        final_owner_epoch: final_snapshot
            .current_owner
            .as_ref()
            .map(OwnerReadback::owner_epoch),
        creation_digest: final_snapshot
            .manifest
            .as_ref()
            .map(CreationManifestReadback::creation_digest),
    })
}

fn validate_expectation(
    snapshot: &CreationSnapshot,
    expectation: CreationExpectation,
) -> Result<(), OracleViolation> {
    if snapshot.manifest.as_ref().is_some_and(|manifest| {
        manifest.subscription_id != expectation.subscription_id
            || manifest.target != expectation.target
            || manifest.generation != expectation.generation
            || manifest.partition != expectation.partition
            || manifest.lifecycle_generation != expectation.lifecycle_generation
            || manifest.namespace_digest != expectation.namespace_digest
            || manifest.initial_position != expectation.initial_position
            || manifest.resolved_initial_offset != expectation.resolved_initial_offset
            || manifest.captured_resource_epoch != expectation.resource_epoch
    }) {
        return Err(OracleViolation::CreationExpectationMismatch);
    }
    Ok(())
}

fn has_anchored_owner_chain(snapshot: &CreationSnapshot) -> bool {
    snapshot
        .current_owner
        .as_ref()
        .is_none_or(|owner| owner.owner_epoch <= 2)
}

fn creation_advanced(before: &CreationSnapshot, after: &CreationSnapshot) -> bool {
    match (
        before.manifest.as_ref(),
        before.current_owner.as_ref(),
        after.manifest.as_ref(),
        after.current_owner.as_ref(),
    ) {
        (None, None, Some(manifest), Some(owner)) => {
            !after.owner_file_present && owner == &manifest.genesis_owner
        }
        (Some(before_manifest), Some(before_owner), Some(after_manifest), Some(after_owner)) => {
            before_manifest == after_manifest
                && after.owner_file_present
                && before_owner.owner_epoch.checked_add(1) == Some(after_owner.owner_epoch)
                && before_owner.owner_generation.checked_add(1)
                    == Some(after_owner.owner_generation)
                && after_owner.creation_digest == after_manifest.creation_digest
                && after_owner.previous_owner_digest == before_owner.record_digest
                && after_owner.owner_id != before_owner.owner_id
        }
        _ => false,
    }
}

fn read_optional_regular_file(path: &Path) -> Result<Option<Vec<u8>>, CreationReadbackError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink() {
        return Err(CreationReadbackError::Symlink);
    }
    if !metadata.is_file() {
        return Err(CreationReadbackError::InvalidFileType);
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(target_os = "linux")]
    options.custom_flags(O_NOFOLLOW);
    let mut file = match options.open(path) {
        Ok(file) => file,
        #[cfg(target_os = "linux")]
        Err(error) if error.raw_os_error() == Some(ELOOP) => {
            return Err(CreationReadbackError::Symlink);
        }
        Err(error) => return Err(error.into()),
    };
    let opened_metadata = file.metadata()?;
    if !opened_metadata.is_file() {
        return Err(CreationReadbackError::InvalidFileType);
    }
    if opened_metadata.len() > MAX_STATE_FRAME_LEN as u64 {
        return Err(CreationReadbackError::InvalidFrame("frame too large"));
    }
    let mut bytes = Vec::with_capacity(opened_metadata.len() as usize);
    Read::by_ref(&mut file)
        .take(MAX_STATE_FRAME_LEN as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_STATE_FRAME_LEN {
        return Err(CreationReadbackError::InvalidFrame("frame too large"));
    }
    if bytes.len() as u64 != opened_metadata.len() {
        return Err(CreationReadbackError::InvalidFrame(
            "frame changed while being read",
        ));
    }
    Ok(Some(bytes))
}

fn decode_creation(bytes: &[u8]) -> Result<CreationManifestReadback, CreationReadbackError> {
    let body = decode_frame(bytes, CREATION_KIND)?;
    if body.len() < CREATION_FIXED_BODY_LEN {
        return Err(CreationReadbackError::InvalidFrame("creation body length"));
    }
    let mut cursor = Cursor::new(body);
    let checkpoint_directory_id = cursor.array()?;
    let subscription_id = cursor.array()?;
    let target = cursor.array()?;
    let generation = cursor.u64()?;
    let partition = cursor.u32()?;
    let lifecycle_generation = cursor.u64()?;
    let namespace_digest = cursor.array()?;
    let selection = cursor.u8()?;
    let exact = cursor.u64()?;
    let initial_position = match selection {
        1 if exact == 0 => InitialPosition::EarliestRetained,
        2 if exact == 0 => InitialPosition::LatestAfterCapturedEnd,
        3 => InitialPosition::Exact(exact),
        _ => return Err(CreationReadbackError::InvalidFrame("initial position")),
    };
    let resolved_initial_offset = cursor.u64()?;
    let captured_end_exclusive = cursor.u64()?;
    let captured_oldest_available = cursor.u64()?;
    let resource_id = ResourceId::from_bytes(cursor.array()?);
    let resource_epoch = cursor.u64()?;
    let owner_len = cursor.u32()? as usize;
    let genesis_owner = decode_owner(cursor.take(owner_len)?)?;
    cursor.finish()?;
    if captured_oldest_available > captured_end_exclusive
        || resolved_initial_offset
            != match initial_position {
                InitialPosition::EarliestRetained => captured_oldest_available,
                InitialPosition::LatestAfterCapturedEnd => captured_end_exclusive,
                InitialPosition::Exact(offset)
                    if captured_oldest_available <= offset && offset <= captured_end_exclusive =>
                {
                    offset
                }
                InitialPosition::Exact(_) => {
                    return Err(CreationReadbackError::InvalidFrame(
                        "resolved initial position",
                    ));
                }
            }
        || genesis_owner.owner_epoch != 1
        || genesis_owner.owner_generation != 1
        || genesis_owner.creation_digest != [0; 32]
        || genesis_owner.previous_owner_digest != [0; 32]
    {
        return Err(CreationReadbackError::InvalidFrame("creation invariants"));
    }
    Ok(CreationManifestReadback {
        checkpoint_directory_id,
        subscription_id,
        target,
        generation,
        partition,
        lifecycle_generation,
        namespace_digest,
        initial_position,
        resolved_initial_offset,
        captured_end_exclusive,
        captured_oldest_available,
        captured_resource_epoch: ResourceEpoch::new(resource_id, resource_epoch),
        creation_digest: Sha256::digest(bytes).into(),
        genesis_owner,
    })
}

fn decode_owner(bytes: &[u8]) -> Result<OwnerReadback, CreationReadbackError> {
    let body = decode_frame(bytes, OWNER_KIND)?;
    if body.len() != OWNER_BODY_LEN {
        return Err(CreationReadbackError::InvalidFrame("owner body length"));
    }
    let creation_digest = body[0..32]
        .try_into()
        .map_err(|_| CreationReadbackError::InvalidFrame("owner creation digest"))?;
    let previous_owner_digest = body[32..64]
        .try_into()
        .map_err(|_| CreationReadbackError::InvalidFrame("owner previous digest"))?;
    let owner_epoch = u64::from_be_bytes(
        body[64..72]
            .try_into()
            .map_err(|_| CreationReadbackError::InvalidFrame("owner epoch"))?,
    );
    let owner_generation = u64::from_be_bytes(
        body[72..80]
            .try_into()
            .map_err(|_| CreationReadbackError::InvalidFrame("owner generation"))?,
    );
    let owner_id: [u8; 16] = body[80..96]
        .try_into()
        .map_err(|_| CreationReadbackError::InvalidFrame("owner id"))?;
    if owner_epoch == 0
        || owner_epoch != owner_generation
        || owner_id[6] & 0xf0 != 0x40
        || owner_id[8] & 0xc0 != 0x80
    {
        return Err(CreationReadbackError::InvalidFrame("owner invariants"));
    }
    Ok(OwnerReadback {
        creation_digest,
        previous_owner_digest,
        owner_epoch,
        owner_generation,
        owner_id,
        record_digest: Sha256::digest(bytes).into(),
    })
}

fn decode_frame(bytes: &[u8], expected_kind: u8) -> Result<&[u8], CreationReadbackError> {
    if bytes.len() > MAX_STATE_FRAME_LEN {
        return Err(CreationReadbackError::InvalidFrame("frame too large"));
    }
    if bytes.len() < FRAME_HEADER_LEN + FRAME_CHECKSUM_LEN {
        return Err(CreationReadbackError::InvalidFrame("truncated frame"));
    }
    if &bytes[..8] != FRAME_MAGIC {
        return Err(CreationReadbackError::InvalidFrame("frame magic"));
    }
    if u16::from_be_bytes(
        bytes[8..10]
            .try_into()
            .map_err(|_| CreationReadbackError::InvalidFrame("frame version"))?,
    ) != FRAME_VERSION
    {
        return Err(CreationReadbackError::InvalidFrame("frame version"));
    }
    if bytes[10] != expected_kind {
        return Err(CreationReadbackError::InvalidFrame("record kind"));
    }
    if bytes[11] != 0 {
        return Err(CreationReadbackError::InvalidFrame("reserved header byte"));
    }
    let body_len = u32::from_be_bytes(
        bytes[12..16]
            .try_into()
            .map_err(|_| CreationReadbackError::InvalidFrame("body length"))?,
    ) as usize;
    let checksum_start = FRAME_HEADER_LEN
        .checked_add(body_len)
        .ok_or(CreationReadbackError::InvalidFrame("body length"))?;
    let expected_len = checksum_start
        .checked_add(FRAME_CHECKSUM_LEN)
        .ok_or(CreationReadbackError::InvalidFrame("body length"))?;
    if bytes.len() != expected_len {
        return Err(CreationReadbackError::InvalidFrame(
            "trailing or truncated frame",
        ));
    }
    let checksum = digest_parts(&[FRAME_DIGEST_DOMAIN, &bytes[..checksum_start]]);
    if bytes[checksum_start..] != checksum {
        return Err(CreationReadbackError::InvalidFrame("frame checksum"));
    }
    Ok(&bytes[FRAME_HEADER_LEN..checksum_start])
}

fn digest_parts(parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part);
    }
    hasher.finalize().into()
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], CreationReadbackError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(CreationReadbackError::InvalidFrame("body overflow"))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(CreationReadbackError::InvalidFrame("truncated body"))?;
        self.position = end;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], CreationReadbackError> {
        self.take(N)?
            .try_into()
            .map_err(|_| CreationReadbackError::InvalidFrame("fixed field"))
    }

    fn u8(&mut self) -> Result<u8, CreationReadbackError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, CreationReadbackError> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, CreationReadbackError> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn finish(self) -> Result<(), CreationReadbackError> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(CreationReadbackError::InvalidFrame("trailing body"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CreationCorpusError, CreationCorpusInputs, CreationExpectation,
        CreationMaterializationBinding, CreationReadbackError, CreationTransitionObservation,
        DYNAMIC_FIELDS, FROZEN_CASES, MAX_STATE_FRAME_LEN, MaterializedImage, SOURCE_PATHS,
        materialize_creation_case, read_creation_snapshot, verify_creation_history,
    };
    use crate::CreationState;
    use alopex_chirps_core::durable::{InitialPosition, ResourceEpoch, ResourceId};
    use sha2::{Digest, Sha256};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    const FRAME_MAGIC: &[u8; 8] = b"CHRPST07";
    const FRAME_DIGEST_DOMAIN: &[u8] = b"chirps-v0.7-state-frame-sha256\0";
    const DIRECTORY_ID_DOMAIN: &[u8] = b"chirps-v0.7-checkpoint-directory\0";

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(1);
            let path = std::env::temp_dir().join(format!(
                "chirps-task-6_2-creation-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn digest_parts(parts: &[&[u8]]) -> [u8; 32] {
        let mut hasher = Sha256::new();
        for part in parts {
            hasher.update(part);
        }
        hasher.finalize().into()
    }

    fn encode_frame(kind: u8, body: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(FRAME_MAGIC);
        bytes.extend_from_slice(&1_u16.to_be_bytes());
        bytes.push(kind);
        bytes.push(0);
        bytes.extend_from_slice(&(body.len() as u32).to_be_bytes());
        bytes.extend_from_slice(body);
        bytes.extend_from_slice(&digest_parts(&[FRAME_DIGEST_DOMAIN, &bytes]));
        bytes
    }

    fn owner_body(
        creation_digest: [u8; 32],
        previous_owner_digest: [u8; 32],
        epoch: u64,
        owner_tag: u8,
    ) -> Vec<u8> {
        let mut owner_id = [owner_tag; 16];
        owner_id[6] = 0x40 | (owner_id[6] & 0x0f);
        owner_id[8] = 0x80 | (owner_id[8] & 0x3f);
        let mut body = Vec::new();
        body.extend_from_slice(&creation_digest);
        body.extend_from_slice(&previous_owner_digest);
        body.extend_from_slice(&epoch.to_be_bytes());
        body.extend_from_slice(&epoch.to_be_bytes());
        body.extend_from_slice(&owner_id);
        body
    }

    fn directory_id(directory: &Path) -> [u8; 32] {
        let canonical = fs::canonicalize(directory).unwrap();
        digest_parts(&[
            DIRECTORY_ID_DOMAIN,
            canonical.as_os_str().as_encoded_bytes(),
        ])
    }

    fn creation_bytes(bound_directory: &Path) -> (Vec<u8>, Vec<u8>) {
        let genesis = encode_frame(2, &owner_body([0; 32], [0; 32], 1, 0x61));
        let mut target = [0x21; 16];
        target[6] = 0x41;
        target[8] = 0x81;
        let mut resource = [0x51; 16];
        resource[6] = 0x41;
        resource[8] = 0x91;
        let mut body = Vec::new();
        body.extend_from_slice(&directory_id(bound_directory));
        body.extend_from_slice(&[0x31; 16]);
        body.extend_from_slice(&target);
        body.extend_from_slice(&7_u64.to_be_bytes());
        body.extend_from_slice(&3_u32.to_be_bytes());
        body.extend_from_slice(&11_u64.to_be_bytes());
        body.extend_from_slice(&[0x41; 32]);
        body.push(3);
        body.extend_from_slice(&6_u64.to_be_bytes());
        body.extend_from_slice(&6_u64.to_be_bytes());
        body.extend_from_slice(&10_u64.to_be_bytes());
        body.extend_from_slice(&4_u64.to_be_bytes());
        body.extend_from_slice(&resource);
        body.extend_from_slice(&9_u64.to_be_bytes());
        body.extend_from_slice(&(genesis.len() as u32).to_be_bytes());
        body.extend_from_slice(&genesis);
        (encode_frame(1, &body), genesis)
    }

    fn expectation() -> CreationExpectation {
        let mut target = [0x21; 16];
        target[6] = 0x41;
        target[8] = 0x81;
        let mut resource = [0x51; 16];
        resource[6] = 0x41;
        resource[8] = 0x91;
        CreationExpectation::new(
            [0x31; 16],
            target,
            7,
            3,
            11,
            [0x41; 32],
            InitialPosition::Exact(6),
            6,
            ResourceEpoch::new(ResourceId::from_bytes(resource), 9),
        )
    }

    fn hex(bytes: impl AsRef<[u8]>) -> String {
        bytes
            .as_ref()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    fn frozen_corpus(root: &Path) -> (PathBuf, PathBuf, PathBuf) {
        let corpus = root.join("corpus");
        let sources_root = root.join("sources");
        let requirements = root.join("requirements.md");
        let design = root.join("design.md");
        fs::create_dir(&corpus).unwrap();
        fs::create_dir(&sources_root).unwrap();
        fs::write(&requirements, b"requirements").unwrap();
        fs::write(&design, b"design").unwrap();

        let template_directory = root.join("template");
        fs::create_dir(&template_directory).unwrap();
        let (template, _) = creation_bytes(&template_directory);
        let mut sources = Vec::new();
        let mut source_projection = Vec::new();
        for path in SOURCE_PATHS {
            let bytes = format!("source:{path}").into_bytes();
            let source_path = sources_root.join(path);
            fs::create_dir_all(source_path.parent().unwrap()).unwrap();
            fs::write(&source_path, &bytes).unwrap();
            let digest = Sha256::digest(&bytes);
            source_projection.extend_from_slice(&(path.len() as u32).to_be_bytes());
            source_projection.extend_from_slice(path.as_bytes());
            source_projection.extend_from_slice(&digest);
            sources.push(serde_json::json!({"path": path, "sha256": hex(digest)}));
        }

        let mut cases = Vec::new();
        for (name, stage, oracle, file, image) in FROZEN_CASES {
            let directory = corpus.join(name);
            fs::create_dir(&directory).unwrap();
            let bytes = match image {
                MaterializedImage::None => &[][..],
                MaterializedImage::PartialPending => &template[..template.len() / 2],
                MaterializedImage::FullPending | MaterializedImage::Canonical => &template,
            };
            if !file.is_empty() {
                fs::write(directory.join(file), bytes).unwrap();
            }
            cases.push(serde_json::json!({
                "name": name,
                "stage": stage,
                "oracle": oracle,
                "file": file,
                "sha256": if file.is_empty() { String::new() } else { hex(Sha256::digest(bytes)) },
            }));
        }
        let manifest = serde_json::json!({
            "schema_version": 2,
            "producer_task": "4.1",
            "requirements_sha256": hex(Sha256::digest(b"requirements")),
            "design_sha256": hex(Sha256::digest(b"design")),
            "source_input_sha256": hex(digest_parts(&[
                super::CORPUS_SOURCE_DOMAIN,
                &source_projection,
            ])),
            "sources": sources,
            "materialization": {
                "template": super::TEMPLATE_PATH,
                "template_sha256": hex(Sha256::digest(&template)),
                "dynamic_fields": DYNAMIC_FIELDS,
                "canonical_journal": super::JOURNAL_FILE,
            },
            "cases": cases,
        });
        fs::write(
            corpus.join("manifest.json"),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
        (corpus, requirements, design)
    }

    fn write_creation(directory: &Path) -> (Vec<u8>, Vec<u8>) {
        let (creation, genesis) = creation_bytes(directory);
        fs::write(directory.join("creation.unit"), &creation).unwrap();
        (creation, genesis)
    }

    fn write_owner(directory: &Path, creation: &[u8], previous: &[u8], epoch: u64) -> Vec<u8> {
        let owner = encode_frame(
            2,
            &owner_body(
                Sha256::digest(creation).into(),
                Sha256::digest(previous).into(),
                epoch,
                0x70 + epoch as u8,
            ),
        );
        fs::write(directory.join("owner.unit"), &owner).unwrap();
        owner
    }

    #[test]
    fn materializes_only_allowlisted_fields_and_requires_exact_frozen_inputs() {
        let temp = TestDirectory::new();
        let (corpus, requirements, design) = frozen_corpus(temp.path());
        let destination = temp.path().join("live");
        let evidence = materialize_creation_case(
            &corpus,
            "directory-sync-new",
            &destination,
            CreationCorpusInputs::new(&requirements, &design, &temp.path().join("sources")),
            CreationMaterializationBinding::new(expectation(), 10, 4),
        )
        .unwrap();

        assert_eq!(evidence.case_name(), "directory-sync-new");
        assert_eq!(evidence.creation_sha256(), evidence.image_sha256().unwrap());
        assert_eq!(
            evidence.journal_sha256(),
            Some(Sha256::digest(fs::read(destination.join("checkpoint.journal")).unwrap()).into())
        );
        assert_eq!(
            read_creation_snapshot(&destination)
                .unwrap()
                .manifest()
                .unwrap()
                .checkpoint_directory_id(),
            directory_id(&destination)
        );

        fs::write(temp.path().join("sources/runtime.rs"), b"tampered").unwrap();
        assert!(matches!(
            materialize_creation_case(
                &corpus,
                "write-old",
                &temp.path().join("rejected"),
                CreationCorpusInputs::new(&requirements, &design, &temp.path().join("sources")),
                CreationMaterializationBinding::new(expectation(), 10, 4),
            ),
            Err(CreationCorpusError::DigestMismatch("source"))
        ));

        fs::write(temp.path().join("sources/runtime.rs"), b"source:runtime.rs").unwrap();
        let manifest_path = corpus.join("manifest.json");
        let manifest_bytes = fs::read(&manifest_path).unwrap();
        let mut wrong_task: serde_json::Value = serde_json::from_slice(&manifest_bytes).unwrap();
        wrong_task["producer_task"] = serde_json::Value::String("4.2".to_owned());
        fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&wrong_task).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            materialize_creation_case(
                &corpus,
                "write-old",
                &temp.path().join("wrong-task"),
                CreationCorpusInputs::new(&requirements, &design, &temp.path().join("sources")),
                CreationMaterializationBinding::new(expectation(), 10, 4),
            ),
            Err(CreationCorpusError::InvalidManifest(
                "schema version or producer task"
            ))
        ));

        fs::write(&manifest_path, manifest_bytes).unwrap();
        fs::write(corpus.join("directory-sync-new/creation.unit"), b"tampered").unwrap();
        assert!(matches!(
            materialize_creation_case(
                &corpus,
                "write-old",
                &temp.path().join("tampered"),
                CreationCorpusInputs::new(&requirements, &design, &temp.path().join("sources")),
                CreationMaterializationBinding::new(expectation(), 10, 4),
            ),
            Err(CreationCorpusError::DigestMismatch("case image"))
        ));
        fs::remove_file(manifest_path).unwrap();
        assert!(matches!(
            materialize_creation_case(
                &corpus,
                "write-old",
                &temp.path().join("missing"),
                CreationCorpusInputs::new(&requirements, &design, &temp.path().join("sources")),
                CreationMaterializationBinding::new(expectation(), 10, 4),
            ),
            Err(CreationCorpusError::Io(_))
        ));
    }

    #[test]
    fn reads_exact_creation_and_linked_owner_frames() {
        let temp = TestDirectory::new();
        let subscription = temp.path().join("subscription");
        fs::create_dir(&subscription).unwrap();
        let (creation, genesis) = write_creation(&subscription);
        write_owner(&subscription, &creation, &genesis, 2);

        let snapshot = read_creation_snapshot(&subscription).unwrap();
        let manifest = snapshot.manifest().unwrap();
        assert_eq!(
            manifest.checkpoint_directory_id(),
            directory_id(&subscription)
        );
        assert_eq!(manifest.namespace_digest(), [0x41; 32]);
        assert_eq!(manifest.resolved_initial_offset(), 6);
        assert_eq!(manifest.captured_end_exclusive(), 10);
        assert_eq!(manifest.captured_oldest_available(), 4);
        assert_eq!(snapshot.current_owner().unwrap().owner_epoch(), 2);
        assert!(snapshot.owner_file_present());
    }

    #[test]
    fn rejects_symlink_trailing_wrong_kind_version_checksum_and_directory_digest() {
        let temp = TestDirectory::new();
        let subscription = temp.path().join("subscription");
        fs::create_dir(&subscription).unwrap();
        let (valid, _) = creation_bytes(&subscription);

        let mut cases = Vec::new();
        let mut trailing = valid.clone();
        trailing.push(0);
        cases.push(trailing);
        let mut wrong_kind = valid.clone();
        wrong_kind[10] = 2;
        cases.push(wrong_kind);
        let mut wrong_version = valid.clone();
        wrong_version[9] = 2;
        cases.push(wrong_version);
        let mut wrong_checksum = valid.clone();
        wrong_checksum[20] ^= 0x80;
        cases.push(wrong_checksum);
        for bytes in cases {
            fs::write(subscription.join("creation.unit"), bytes).unwrap();
            assert!(matches!(
                read_creation_snapshot(&subscription),
                Err(CreationReadbackError::InvalidFrame(_))
            ));
        }

        let other = temp.path().join("other");
        fs::create_dir(&other).unwrap();
        let (wrong_directory, _) = creation_bytes(&other);
        fs::write(subscription.join("creation.unit"), wrong_directory).unwrap();
        assert!(matches!(
            read_creation_snapshot(&subscription),
            Err(CreationReadbackError::DirectoryDigestMismatch)
        ));

        fs::write(
            subscription.join("creation.unit"),
            vec![0; MAX_STATE_FRAME_LEN + 1],
        )
        .unwrap();
        assert!(matches!(
            read_creation_snapshot(&subscription),
            Err(CreationReadbackError::InvalidFrame("frame too large"))
        ));

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            fs::remove_file(subscription.join("creation.unit")).unwrap();
            let target = temp.path().join("target");
            fs::write(&target, valid).unwrap();
            symlink(target, subscription.join("creation.unit")).unwrap();
            assert!(matches!(
                read_creation_snapshot(&subscription),
                Err(CreationReadbackError::Symlink)
            ));
        }
    }

    #[test]
    fn rejects_owner_chain_not_linked_to_creation_or_previous_owner() {
        let temp = TestDirectory::new();
        let subscription = temp.path().join("subscription");
        fs::create_dir(&subscription).unwrap();
        let (creation, _) = write_creation(&subscription);
        let bad_owner = encode_frame(2, &owner_body([0x99; 32], [0x88; 32], 2, 0x77));
        fs::write(subscription.join("owner.unit"), bad_owner).unwrap();

        assert!(matches!(
            read_creation_snapshot(&subscription),
            Err(CreationReadbackError::OwnerChain)
        ));

        let bad_previous = encode_frame(
            2,
            &owner_body(Sha256::digest(&creation).into(), [0x88; 32], 2, 0x77),
        );
        fs::write(subscription.join("owner.unit"), bad_previous).unwrap();
        assert!(matches!(
            read_creation_snapshot(&subscription),
            Err(CreationReadbackError::OwnerChain)
        ));
    }

    #[test]
    fn verifies_unknown_new_recovery_and_rejects_owner_fork() {
        let temp = TestDirectory::new();
        let subscription = temp.path().join("subscription");
        fs::create_dir(&subscription).unwrap();
        let absent = read_creation_snapshot(&subscription).unwrap();
        let (creation, genesis) = write_creation(&subscription);
        let candidate = read_creation_snapshot(&subscription).unwrap();
        write_owner(&subscription, &creation, &genesis, 2);
        let recovered = read_creation_snapshot(&subscription).unwrap();

        let transitions = [
            CreationTransitionObservation::new(
                CreationState::CreationUnknown,
                expectation(),
                absent,
                candidate.clone(),
            ),
            CreationTransitionObservation::new(
                CreationState::Created,
                expectation(),
                candidate.clone(),
                recovered.clone(),
            ),
        ];
        let verdict = verify_creation_history(&transitions).unwrap();
        assert!(verdict.resolved_new());
        assert_eq!(verdict.final_owner_epoch(), Some(2));
        assert_eq!(
            verdict.creation_digest(),
            Some(Sha256::digest(&creation).into())
        );

        let mut with_later_unknown = transitions.to_vec();
        with_later_unknown.push(CreationTransitionObservation::new(
            CreationState::CreationUnknown,
            expectation(),
            recovered.clone(),
            recovered.clone(),
        ));
        assert!(
            verify_creation_history(&with_later_unknown)
                .unwrap()
                .resolved_new()
        );

        let discontinuous = CreationTransitionObservation::new(
            CreationState::Created,
            expectation(),
            transitions[0].before().clone(),
            recovered,
        );
        assert!(verify_creation_history(&[transitions[0].clone(), discontinuous]).is_err());
    }

    #[test]
    fn classifies_existing_owner_unknown_old_from_advancement() {
        let temp = TestDirectory::new();
        let subscription = temp.path().join("subscription");
        fs::create_dir(&subscription).unwrap();
        write_creation(&subscription);
        let unchanged = read_creation_snapshot(&subscription).unwrap();

        let verdict = verify_creation_history(&[CreationTransitionObservation::new(
            CreationState::CreationUnknown,
            expectation(),
            unchanged.clone(),
            unchanged,
        )])
        .unwrap();

        assert!(!verdict.resolved_new());
        assert_eq!(verdict.final_owner_epoch(), Some(1));
    }

    #[test]
    fn rejects_mismatched_namespace_selection_and_resource_epoch() {
        let temp = TestDirectory::new();
        let subscription = temp.path().join("subscription");
        fs::create_dir(&subscription).unwrap();
        write_creation(&subscription);
        let snapshot = read_creation_snapshot(&subscription).unwrap();

        let mut mismatches = Vec::new();
        let mut value = expectation();
        value.subscription_id[0] ^= 1;
        mismatches.push(value);
        let mut value = expectation();
        value.target[0] ^= 1;
        mismatches.push(value);
        let mut value = expectation();
        value.generation += 1;
        mismatches.push(value);
        let mut value = expectation();
        value.partition += 1;
        mismatches.push(value);
        let mut value = expectation();
        value.lifecycle_generation += 1;
        mismatches.push(value);
        let mut value = expectation();
        value.namespace_digest[0] ^= 1;
        mismatches.push(value);
        let mut value = expectation();
        value.initial_position = InitialPosition::EarliestRetained;
        mismatches.push(value);
        let mut value = expectation();
        value.resolved_initial_offset += 1;
        mismatches.push(value);
        let mut value = expectation();
        value.resource_epoch = ResourceEpoch::new(value.resource_epoch.resource_id(), 10);
        mismatches.push(value);

        for mismatch in mismatches {
            assert!(
                verify_creation_history(&[CreationTransitionObservation::new(
                    CreationState::CreationNotCommitted,
                    mismatch,
                    snapshot.clone(),
                    snapshot.clone(),
                )])
                .is_err()
            );
        }
    }

    #[test]
    fn requires_observed_predecessor_for_owner_epoch_above_two() {
        let temp = TestDirectory::new();
        let subscription = temp.path().join("subscription");
        fs::create_dir(&subscription).unwrap();
        let (creation, genesis) = write_creation(&subscription);
        let second = write_owner(&subscription, &creation, &genesis, 2);
        let epoch_two = read_creation_snapshot(&subscription).unwrap();
        write_owner(&subscription, &creation, &second, 3);
        let epoch_three = read_creation_snapshot(&subscription).unwrap();

        assert!(
            verify_creation_history(&[CreationTransitionObservation::new(
                CreationState::CreationNotCommitted,
                expectation(),
                epoch_three.clone(),
                epoch_three.clone(),
            )])
            .is_err()
        );

        assert!(
            verify_creation_history(&[CreationTransitionObservation::new(
                CreationState::Created,
                expectation(),
                epoch_two,
                epoch_three,
            )])
            .unwrap()
            .resolved_new()
        );
    }
}
