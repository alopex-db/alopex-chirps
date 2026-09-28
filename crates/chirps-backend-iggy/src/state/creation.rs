//! Atomic subscription creation, exact recovery, and lifetime lock ownership.

#[cfg(test)]
use super::hex;
use super::owner::{OwnerRecord, OwnerRecordError};
use super::{
    InstallError, InstallFault, StateFrameError, StateReadError, StateRecordKind,
    decode_state_frame, digest, digest_parts, durably_install, encode_state_frame, read_state_file,
    sync_directory,
};
use alopex_chirps_core::durable::{
    CheckpointDirectoryId, CreationFailureKind, CreationRecoveryBinding, InitialPosition,
    PollObservation, ResourceEpoch, ResourceId, SubscriptionBinding, SubscriptionCreationOutcome,
    SubscriptionId,
};
use alopex_chirps_wire::node_id::NodeId;
use fs2::FileExt;
#[cfg(test)]
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io;
#[cfg(test)]
use std::io::Write;
use std::path::{Path, PathBuf};
use thiserror::Error;

const CREATION_FILE: &str = "creation.unit";
const OWNER_FILE: &str = "owner.unit";
const LOCK_FILE: &str = ".owner.lock";
const CREATION_FIXED_BODY_LEN: usize = 177;
const DIRECTORY_ID_DOMAIN: &[u8] = b"chirps-v0.7-checkpoint-directory\0";
const CORPUS_SOURCE_DOMAIN: &[u8] = b"chirps-v0.7-task-4.1-source-input\0";
const CORPUS_OWNER_DOMAIN: &[u8] = b"chirps-v0.7-task-4.1-fixture-owner\0";

/// Immutable namespace fields fixed by the subscription manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CreationNamespace {
    subscription_id: SubscriptionId,
    target: NodeId,
    generation: u64,
    partition: u32,
    lifecycle_generation: u64,
    namespace_digest: [u8; 32],
}

impl CreationNamespace {
    pub(crate) const fn new(
        subscription_id: SubscriptionId,
        target: NodeId,
        generation: u64,
        partition: u32,
        lifecycle_generation: u64,
        namespace_digest: [u8; 32],
    ) -> Self {
        Self {
            subscription_id,
            target,
            generation,
            partition,
            lifecycle_generation,
            namespace_digest,
        }
    }

    pub(crate) const fn subscription_id(self) -> SubscriptionId {
        self.subscription_id
    }

    pub(crate) const fn target(self) -> NodeId {
        self.target
    }

    pub(crate) const fn generation(self) -> u64 {
        self.generation
    }

    pub(crate) const fn partition(self) -> u32 {
        self.partition
    }

    pub(crate) const fn lifecycle_generation(self) -> u64 {
        self.lifecycle_generation
    }

    pub(crate) const fn namespace_digest(self) -> [u8; 32] {
        self.namespace_digest
    }
}

/// Explicit create input. There is deliberately no default initial position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CreationRequest {
    namespace: CreationNamespace,
    initial_position: InitialPosition,
}

impl CreationRequest {
    pub(crate) const fn new(
        namespace: CreationNamespace,
        initial_position: InitialPosition,
    ) -> Self {
        Self {
            namespace,
            initial_position,
        }
    }

    pub(crate) const fn namespace(self) -> CreationNamespace {
        self.namespace
    }

    pub(crate) const fn initial_position(self) -> InitialPosition {
        self.initial_position
    }
}

/// Complete immutable manifest, captured initial observation, and genesis owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreationUnit {
    checkpoint_directory_id: CheckpointDirectoryId,
    namespace: CreationNamespace,
    initial_position: InitialPosition,
    resolved_initial: u64,
    captured_end_exclusive: u64,
    captured_oldest_available: u64,
    captured_resource_epoch: ResourceEpoch,
    genesis_owner: OwnerRecord,
}

impl CreationUnit {
    fn new(
        checkpoint_directory_id: CheckpointDirectoryId,
        request: CreationRequest,
        observation: &PollObservation,
    ) -> Result<Self, CreationFailureKind> {
        let resolved_initial = observation
            .resolve_initial(request.initial_position)
            .map_err(|_| CreationFailureKind::InvalidInitialPosition)?;
        Ok(Self {
            checkpoint_directory_id,
            namespace: request.namespace,
            initial_position: request.initial_position,
            resolved_initial,
            captured_end_exclusive: observation.end_exclusive(),
            captured_oldest_available: observation.oldest_available(),
            captured_resource_epoch: observation.resource_epoch(),
            genesis_owner: OwnerRecord::fresh_genesis(),
        })
    }

    fn encode(&self) -> Vec<u8> {
        let owner = self.genesis_owner.encode();
        let owner_len = u32::try_from(owner.len()).expect("bounded owner frame");
        let mut body = Vec::with_capacity(CREATION_FIXED_BODY_LEN + owner.len());
        body.extend_from_slice(self.checkpoint_directory_id.as_bytes());
        body.extend_from_slice(self.namespace.subscription_id.as_bytes());
        body.extend_from_slice(self.namespace.target.as_bytes());
        body.extend_from_slice(&self.namespace.generation.to_be_bytes());
        body.extend_from_slice(&self.namespace.partition.to_be_bytes());
        body.extend_from_slice(&self.namespace.lifecycle_generation.to_be_bytes());
        body.extend_from_slice(&self.namespace.namespace_digest);
        let (selection, exact) = match self.initial_position {
            InitialPosition::EarliestRetained => (1, 0),
            InitialPosition::LatestAfterCapturedEnd => (2, 0),
            InitialPosition::Exact(offset) => (3, offset),
        };
        body.push(selection);
        body.extend_from_slice(&exact.to_be_bytes());
        body.extend_from_slice(&self.resolved_initial.to_be_bytes());
        body.extend_from_slice(&self.captured_end_exclusive.to_be_bytes());
        body.extend_from_slice(&self.captured_oldest_available.to_be_bytes());
        body.extend_from_slice(self.captured_resource_epoch.resource_id().as_bytes());
        body.extend_from_slice(&self.captured_resource_epoch.epoch().to_be_bytes());
        body.extend_from_slice(&owner_len.to_be_bytes());
        body.extend_from_slice(&owner);
        encode_state_frame(StateRecordKind::CreationUnit, &body)
            .expect("fixed creation unit remains below frame bound")
    }

    fn decode(bytes: &[u8]) -> Result<Self, CreationStoreError> {
        let body = decode_state_frame(bytes, StateRecordKind::CreationUnit)?;
        if body.len() < CREATION_FIXED_BODY_LEN {
            return Err(CreationStoreError::CorruptState);
        }
        let mut cursor = BodyCursor::new(body);
        let checkpoint_directory_id = CheckpointDirectoryId::from_bytes(cursor.array::<32>()?);
        let subscription_id = SubscriptionId::from_bytes(cursor.array::<16>()?);
        let target_bytes = cursor.array::<16>()?;
        let target =
            NodeId::from_bytes(&target_bytes).map_err(|_| CreationStoreError::CorruptState)?;
        let generation = cursor.u64()?;
        let partition = cursor.u32()?;
        let lifecycle_generation = cursor.u64()?;
        let namespace_digest = cursor.array::<32>()?;
        let selection = cursor.u8()?;
        let exact = cursor.u64()?;
        let initial_position = match selection {
            1 if exact == 0 => InitialPosition::EarliestRetained,
            2 if exact == 0 => InitialPosition::LatestAfterCapturedEnd,
            3 => InitialPosition::Exact(exact),
            _ => return Err(CreationStoreError::CorruptState),
        };
        let resolved_initial = cursor.u64()?;
        let captured_end_exclusive = cursor.u64()?;
        let captured_oldest_available = cursor.u64()?;
        let resource_id = ResourceId::from_bytes(cursor.array::<16>()?);
        let resource_epoch = cursor.u64()?;
        let owner_len = cursor.u32()? as usize;
        let owner_bytes = cursor.take(owner_len)?;
        cursor.finish()?;
        let genesis_owner = OwnerRecord::decode(owner_bytes)?;
        genesis_owner.validate_genesis()?;
        let captured_resource_epoch = ResourceEpoch::new(resource_id, resource_epoch);
        let bounds = PollObservation::try_new(
            captured_resource_epoch,
            captured_end_exclusive,
            captured_oldest_available,
            None,
        )
        .map_err(|_| CreationStoreError::CorruptState)?;
        if bounds
            .resolve_initial(initial_position)
            .map_err(|_| CreationStoreError::CorruptState)?
            != resolved_initial
        {
            return Err(CreationStoreError::CorruptState);
        }
        Ok(Self {
            checkpoint_directory_id,
            namespace: CreationNamespace::new(
                subscription_id,
                target,
                generation,
                partition,
                lifecycle_generation,
                namespace_digest,
            ),
            initial_position,
            resolved_initial,
            captured_end_exclusive,
            captured_oldest_available,
            captured_resource_epoch,
            genesis_owner,
        })
    }

    fn digest(&self) -> [u8; 32] {
        digest(&self.encode())
    }

    fn recovery_binding(&self) -> CreationRecoveryBinding {
        CreationRecoveryBinding::new(
            self.checkpoint_directory_id,
            self.namespace.subscription_id,
            self.initial_position,
        )
    }

    fn matches_namespace(&self, expected: CreationNamespace) -> bool {
        self.namespace == expected
    }

    fn matches_recovery(&self, binding: CreationRecoveryBinding) -> bool {
        self.checkpoint_directory_id == binding.checkpoint_directory_id()
            && self.namespace.subscription_id == binding.subscription_id()
            && self.initial_position == binding.initial_position()
    }

    fn binding(&self, owner_epoch: u64) -> SubscriptionBinding {
        SubscriptionBinding::new(
            self.namespace.subscription_id,
            self.namespace.target,
            self.namespace.generation,
            self.namespace.partition,
            owner_epoch,
            self.namespace.lifecycle_generation,
        )
    }

    pub(crate) const fn initial_position(&self) -> InitialPosition {
        self.initial_position
    }

    pub(crate) const fn resolved_initial(&self) -> u64 {
        self.resolved_initial
    }

    pub(crate) const fn captured_end_exclusive(&self) -> u64 {
        self.captured_end_exclusive
    }

    pub(crate) const fn captured_oldest_available(&self) -> u64 {
        self.captured_oldest_available
    }

    pub(crate) const fn captured_resource_epoch(&self) -> ResourceEpoch {
        self.captured_resource_epoch
    }
}

/// Active namespace that retains the exact OS lock for its entire lifetime.
#[derive(Debug)]
pub(crate) struct ActiveSubscription {
    directory: PathBuf,
    lock: File,
    creation: CreationUnit,
    owner: OwnerRecord,
    binding: SubscriptionBinding,
}

impl ActiveSubscription {
    fn new(directory: PathBuf, lock: File, creation: CreationUnit, owner: OwnerRecord) -> Self {
        let binding = creation.binding(owner.owner_epoch());
        Self {
            directory,
            lock,
            creation,
            owner,
            binding,
        }
    }

    pub(crate) const fn binding(&self) -> SubscriptionBinding {
        self.binding
    }

    pub(crate) const fn creation(&self) -> &CreationUnit {
        &self.creation
    }

    pub(crate) const fn owner(&self) -> &OwnerRecord {
        &self.owner
    }

    pub(crate) fn directory(&self) -> &Path {
        &self.directory
    }
}

impl Drop for ActiveSubscription {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.lock);
    }
}

/// Store result with the active lock handle retained only by `Created`.
#[derive(Debug)]
pub(crate) enum CreationStoreResult {
    NotCommitted(CreationFailureKind),
    Unknown(CreationRecoveryBinding),
    Created(Box<ActiveSubscription>),
}

impl CreationStoreResult {
    pub(crate) fn public_outcome(&self) -> SubscriptionCreationOutcome {
        match self {
            Self::NotCommitted(kind) => SubscriptionCreationOutcome::CreationNotCommitted(*kind),
            Self::Unknown(binding) => SubscriptionCreationOutcome::CreationUnknown(*binding),
            Self::Created(active) => SubscriptionCreationOutcome::Created(active.binding()),
        }
    }
}

/// Atomic create/open/recover entrypoints for one subscription directory.
#[derive(Debug, Default)]
pub(crate) struct CreationStore;

impl CreationStore {
    pub(crate) fn create(
        &self,
        directory: &Path,
        request: CreationRequest,
        observation: &PollObservation,
    ) -> Result<CreationStoreResult, CreationStoreError> {
        self.create_with_fault(directory, request, observation, InstallFault::None)
    }

    fn create_with_fault(
        &self,
        directory: &Path,
        request: CreationRequest,
        observation: &PollObservation,
        fault: InstallFault,
    ) -> Result<CreationStoreResult, CreationStoreError> {
        let lock = match acquire_operation_lock(directory)? {
            Ok(lock) => lock,
            Err(kind) => return Ok(CreationStoreResult::NotCommitted(kind)),
        };
        let creation_path = directory.join(CREATION_FILE);
        if creation_path.exists() {
            return Err(CreationStoreError::ExistingCreation);
        }
        let directory_id = match checkpoint_directory_id(directory) {
            Ok(directory_id) => directory_id,
            Err(CreationStoreError::Io(_)) => {
                return Ok(CreationStoreResult::NotCommitted(
                    CreationFailureKind::StorageUnavailable,
                ));
            }
            Err(error) => return Err(error),
        };
        let creation = match CreationUnit::new(directory_id, request, observation) {
            Ok(creation) => creation,
            Err(kind) => return Ok(CreationStoreResult::NotCommitted(kind)),
        };
        let recovery = creation.recovery_binding();
        match durably_install(&creation_path, &creation.encode(), fault) {
            Ok(()) => {
                let owner = creation.genesis_owner.clone();
                Ok(CreationStoreResult::Created(Box::new(
                    ActiveSubscription::new(directory.to_path_buf(), lock, creation, owner),
                )))
            }
            Err(InstallError::KnownOld { .. }) => Ok(CreationStoreResult::NotCommitted(
                CreationFailureKind::StorageUnavailable,
            )),
            Err(InstallError::Unknown { .. }) => Ok(CreationStoreResult::Unknown(recovery)),
        }
    }

    pub(crate) fn open(
        &self,
        directory: &Path,
        expected: CreationNamespace,
        current_resource_epoch: ResourceEpoch,
    ) -> Result<CreationStoreResult, CreationStoreError> {
        self.open_with_fault(
            directory,
            expected,
            current_resource_epoch,
            InstallFault::None,
        )
    }

    fn open_with_fault(
        &self,
        directory: &Path,
        expected: CreationNamespace,
        current_resource_epoch: ResourceEpoch,
        fault: InstallFault,
    ) -> Result<CreationStoreResult, CreationStoreError> {
        let lock = match acquire_operation_lock(directory)? {
            Ok(lock) => lock,
            Err(kind) => return Ok(CreationStoreResult::NotCommitted(kind)),
        };
        let creation = load_creation(directory)?;
        if !creation.matches_namespace(expected) {
            return Err(CreationStoreError::ManifestMismatch);
        }
        if creation.captured_resource_epoch != current_resource_epoch {
            return Err(CreationStoreError::ResourceEpochMismatch);
        }
        install_owner(directory, lock, creation, fault)
    }

    pub(crate) fn recover(
        &self,
        directory: &Path,
        binding: CreationRecoveryBinding,
        current_resource_epoch: ResourceEpoch,
    ) -> Result<CreationStoreResult, CreationStoreError> {
        self.recover_with_fault(
            directory,
            binding,
            current_resource_epoch,
            InstallFault::None,
        )
    }

    fn recover_with_fault(
        &self,
        directory: &Path,
        binding: CreationRecoveryBinding,
        current_resource_epoch: ResourceEpoch,
        fault: InstallFault,
    ) -> Result<CreationStoreResult, CreationStoreError> {
        let lock = match acquire_operation_lock(directory)? {
            Ok(lock) => lock,
            Err(kind) => return Ok(CreationStoreResult::NotCommitted(kind)),
        };
        let directory_id = match checkpoint_directory_id(directory) {
            Ok(directory_id) => directory_id,
            Err(CreationStoreError::Io(_)) => {
                return Ok(CreationStoreResult::NotCommitted(
                    CreationFailureKind::StorageUnavailable,
                ));
            }
            Err(error) => return Err(error),
        };
        if directory_id != binding.checkpoint_directory_id() {
            return Err(CreationStoreError::RecoveryBindingMismatch);
        }
        if !directory.join(CREATION_FILE).exists() {
            return Ok(CreationStoreResult::NotCommitted(
                CreationFailureKind::StorageUnavailable,
            ));
        }
        let creation = load_creation(directory)?;
        if !creation.matches_recovery(binding) {
            return Err(CreationStoreError::RecoveryBindingMismatch);
        }
        if creation.captured_resource_epoch != current_resource_epoch {
            return Err(CreationStoreError::ResourceEpochMismatch);
        }
        install_owner(directory, lock, creation, fault)
    }
}

fn install_owner(
    directory: &Path,
    lock: File,
    creation: CreationUnit,
    fault: InstallFault,
) -> Result<CreationStoreResult, CreationStoreError> {
    let owner_path = directory.join(OWNER_FILE);
    let current = if owner_path.exists() {
        let bytes = read_state_file(&owner_path).map_err(map_read_error)?;
        let owner = OwnerRecord::decode(&bytes)?;
        owner.validate_current(creation.digest())?;
        owner
    } else {
        creation.genesis_owner.clone()
    };
    let next = OwnerRecord::next(&current, creation.digest())?;
    let recovery = creation.recovery_binding();
    match durably_install(&owner_path, &next.encode(), fault) {
        Ok(()) => Ok(CreationStoreResult::Created(Box::new(
            ActiveSubscription::new(directory.to_path_buf(), lock, creation, next),
        ))),
        Err(InstallError::KnownOld { .. }) => Ok(CreationStoreResult::NotCommitted(
            CreationFailureKind::StorageUnavailable,
        )),
        Err(InstallError::Unknown { .. }) => Ok(CreationStoreResult::Unknown(recovery)),
    }
}

fn load_creation(directory: &Path) -> Result<CreationUnit, CreationStoreError> {
    let path = directory.join(CREATION_FILE);
    if !path.exists() {
        return Err(CreationStoreError::MissingCreation);
    }
    let bytes = read_state_file(&path).map_err(map_read_error)?;
    let creation = CreationUnit::decode(&bytes)?;
    if creation.checkpoint_directory_id != checkpoint_directory_id(directory)? {
        return Err(CreationStoreError::ManifestMismatch);
    }
    Ok(creation)
}

fn map_read_error(error: StateReadError) -> CreationStoreError {
    match error {
        StateReadError::Io(error) => CreationStoreError::Io(error.kind()),
        StateReadError::Frame(_) => CreationStoreError::CorruptState,
    }
}

enum DirectoryLock {
    Held(File),
    Unavailable,
}

fn acquire_operation_lock(
    directory: &Path,
) -> Result<Result<File, CreationFailureKind>, CreationStoreError> {
    match acquire_directory_lock(directory) {
        Ok(DirectoryLock::Held(lock)) => Ok(Ok(lock)),
        Ok(DirectoryLock::Unavailable) => Ok(Err(CreationFailureKind::OwnerLockUnavailable)),
        Err(CreationStoreError::Io(_)) => Ok(Err(CreationFailureKind::StorageUnavailable)),
        Err(error) => Err(error),
    }
}

fn acquire_directory_lock(directory: &Path) -> Result<DirectoryLock, CreationStoreError> {
    prepare_directory(directory)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(directory.join(LOCK_FILE))
        .map_err(|error| CreationStoreError::Io(error.kind()))?;
    match FileExt::try_lock_exclusive(&lock) {
        Ok(()) => Ok(DirectoryLock::Held(lock)),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(DirectoryLock::Unavailable),
        Err(error) => Err(CreationStoreError::Io(error.kind())),
    }
}

fn prepare_directory(directory: &Path) -> Result<(), CreationStoreError> {
    if directory.exists() {
        if directory.is_dir() {
            return Ok(());
        }
        return Err(CreationStoreError::NotDirectory);
    }
    let parent = directory.parent().ok_or(CreationStoreError::NotDirectory)?;
    if !parent.is_dir() {
        return Err(CreationStoreError::NotDirectory);
    }
    fs::create_dir(directory).map_err(|error| CreationStoreError::Io(error.kind()))?;
    sync_directory(parent).map_err(|error| CreationStoreError::Io(error.kind()))?;
    Ok(())
}

fn checkpoint_directory_id(directory: &Path) -> Result<CheckpointDirectoryId, CreationStoreError> {
    let canonical =
        fs::canonicalize(directory).map_err(|error| CreationStoreError::Io(error.kind()))?;
    let encoded = canonical.as_os_str().as_encoded_bytes();
    Ok(CheckpointDirectoryId::from_bytes(digest_parts(&[
        DIRECTORY_ID_DOMAIN,
        encoded,
    ])))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(crate) enum CreationStoreError {
    #[error("subscription path is not a directory")]
    NotDirectory,
    #[error("a creation unit already exists and cannot be overwritten")]
    ExistingCreation,
    #[error("the subscription creation unit is absent")]
    MissingCreation,
    #[error("the creation manifest does not match the requested namespace or directory")]
    ManifestMismatch,
    #[error("the creation manifest belongs to another resource epoch")]
    ResourceEpochMismatch,
    #[error("creation recovery is not bound to this exact directory, subscription, and selection")]
    RecoveryBindingMismatch,
    #[error("canonical subscription state is corrupt")]
    CorruptState,
    #[error("canonical subscription state I/O failed: {0:?}")]
    Io(io::ErrorKind),
    #[error("canonical subscription frame is invalid: {0}")]
    Frame(#[from] StateFrameError),
    #[error("canonical owner chain is invalid: {0}")]
    Owner(#[from] OwnerRecordError),
}

struct BodyCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> BodyCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], CreationStoreError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(CreationStoreError::CorruptState)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(CreationStoreError::CorruptState)?;
        self.position = end;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], CreationStoreError> {
        self.take(N)?
            .try_into()
            .map_err(|_| CreationStoreError::CorruptState)
    }

    fn u8(&mut self) -> Result<u8, CreationStoreError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, CreationStoreError> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, CreationStoreError> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn finish(self) -> Result<(), CreationStoreError> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(CreationStoreError::CorruptState)
        }
    }
}

#[cfg(test)]
pub(crate) struct CorpusInputs<'a> {
    pub(crate) requirements: &'a [u8],
    pub(crate) design: &'a [u8],
    pub(crate) sources: &'a [(&'a str, &'a [u8])],
}

#[cfg(test)]
#[derive(Clone, Copy)]
struct CorpusCase {
    name: &'static str,
    stage: &'static str,
    oracle: CorpusOracle,
    image: CorpusImage,
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum CorpusImage {
    NoCanonical,
    PartialTemp,
    FullTemp,
    Canonical,
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum CorpusOracle {
    Old,
    UnknownOld,
    UnknownNew,
    New,
}

#[cfg(test)]
impl CorpusOracle {
    const fn name(self) -> &'static str {
        match self {
            Self::Old => "old",
            Self::UnknownOld => "unknown-old",
            Self::UnknownNew => "unknown-new",
            Self::New => "new",
        }
    }

    const fn resolves_new(self) -> bool {
        matches!(self, Self::UnknownNew | Self::New)
    }
}

#[cfg(test)]
const CORPUS_CASES: &[CorpusCase] = &[
    CorpusCase {
        name: "write-old",
        stage: "write",
        oracle: CorpusOracle::Old,
        image: CorpusImage::PartialTemp,
    },
    CorpusCase {
        name: "file-sync-old",
        stage: "file-sync",
        oracle: CorpusOracle::Old,
        image: CorpusImage::FullTemp,
    },
    CorpusCase {
        name: "rename-old-unknown",
        stage: "rename",
        oracle: CorpusOracle::UnknownOld,
        image: CorpusImage::FullTemp,
    },
    CorpusCase {
        name: "rename-new-unknown",
        stage: "rename",
        oracle: CorpusOracle::UnknownNew,
        image: CorpusImage::Canonical,
    },
    CorpusCase {
        name: "directory-sync-old-unknown",
        stage: "directory-sync",
        oracle: CorpusOracle::UnknownOld,
        image: CorpusImage::NoCanonical,
    },
    CorpusCase {
        name: "directory-sync-new-unknown",
        stage: "directory-sync",
        oracle: CorpusOracle::UnknownNew,
        image: CorpusImage::Canonical,
    },
    CorpusCase {
        name: "directory-sync-new",
        stage: "directory-sync",
        oracle: CorpusOracle::New,
        image: CorpusImage::Canonical,
    },
];

#[cfg(test)]
pub(crate) fn generate_creation_corpus(
    output: &Path,
    creation: &CreationUnit,
    inputs: &CorpusInputs<'_>,
) -> Result<(), CreationStoreError> {
    if output.exists() {
        fs::remove_dir_all(output).map_err(|error| CreationStoreError::Io(error.kind()))?;
    }
    fs::create_dir(output).map_err(|error| CreationStoreError::Io(error.kind()))?;
    let mut case_bytes = BTreeMap::new();
    for case in CORPUS_CASES {
        let directory = output.join(case.name);
        fs::create_dir(&directory).map_err(|error| CreationStoreError::Io(error.kind()))?;
        let creation_bytes = corpus_creation_for_directory(creation, &directory, inputs)?.encode();
        match case.image {
            CorpusImage::NoCanonical => {}
            CorpusImage::PartialTemp => {
                let split = creation_bytes.len() / 2;
                write_corpus_file(
                    &directory.join(".creation.unit.pending"),
                    &creation_bytes[..split],
                )?;
            }
            CorpusImage::FullTemp => {
                write_corpus_file(&directory.join(".creation.unit.pending"), &creation_bytes)?;
            }
            CorpusImage::Canonical => {
                write_corpus_file(&directory.join(CREATION_FILE), &creation_bytes)?;
            }
        }
        case_bytes.insert(case.name, creation_bytes);
        sync_directory(&directory).map_err(|error| CreationStoreError::Io(error.kind()))?;
    }
    let manifest = corpus_manifest(&case_bytes, inputs)?;
    write_corpus_file(&output.join("manifest.json"), manifest.as_bytes())?;
    sync_directory(output).map_err(|error| CreationStoreError::Io(error.kind()))?;
    Ok(())
}

#[cfg(test)]
pub(crate) fn verify_creation_corpus(
    output: &Path,
    creation: &CreationUnit,
    inputs: &CorpusInputs<'_>,
) -> Result<(), CreationStoreError> {
    let mut expected_paths = BTreeSet::from([PathBuf::from("manifest.json")]);
    let mut case_bytes = BTreeMap::new();
    for case in CORPUS_CASES {
        let relative_directory = PathBuf::from(case.name);
        let directory = output.join(&relative_directory);
        if !directory.is_dir() {
            return Err(CreationStoreError::CorruptState);
        }
        let case_creation = corpus_creation_for_directory(creation, &directory, inputs)?;
        let creation_bytes = case_creation.encode();
        match case.image {
            CorpusImage::NoCanonical => {}
            CorpusImage::PartialTemp => {
                let relative = relative_directory.join(".creation.unit.pending");
                expected_paths.insert(relative.clone());
                let split = creation_bytes.len() / 2;
                if fs::read(output.join(relative))
                    .map_err(|error| CreationStoreError::Io(error.kind()))?
                    != creation_bytes[..split]
                {
                    return Err(CreationStoreError::CorruptState);
                }
            }
            CorpusImage::FullTemp => {
                let relative = relative_directory.join(".creation.unit.pending");
                expected_paths.insert(relative.clone());
                if fs::read(output.join(relative))
                    .map_err(|error| CreationStoreError::Io(error.kind()))?
                    != creation_bytes
                {
                    return Err(CreationStoreError::CorruptState);
                }
            }
            CorpusImage::Canonical => {
                let relative = relative_directory.join(CREATION_FILE);
                expected_paths.insert(relative.clone());
                if fs::read(output.join(relative))
                    .map_err(|error| CreationStoreError::Io(error.kind()))?
                    != creation_bytes
                {
                    return Err(CreationStoreError::CorruptState);
                }
            }
        }
        verify_corpus_recovery_oracle(&directory, case, &case_creation)?;
        case_bytes.insert(case.name, creation_bytes);
    }
    let actual_paths = corpus_file_inventory(output)?;
    if actual_paths != expected_paths {
        return Err(CreationStoreError::CorruptState);
    }
    let actual_manifest = fs::read(output.join("manifest.json"))
        .map_err(|error| CreationStoreError::Io(error.kind()))?;
    if actual_manifest != corpus_manifest(&case_bytes, inputs)?.as_bytes() {
        return Err(CreationStoreError::CorruptState);
    }
    Ok(())
}

#[cfg(test)]
fn corpus_creation_for_directory(
    template: &CreationUnit,
    directory: &Path,
    inputs: &CorpusInputs<'_>,
) -> Result<CreationUnit, CreationStoreError> {
    let mut creation = template.clone();
    creation.checkpoint_directory_id = checkpoint_directory_id(directory)?;
    let namespace = creation.namespace;
    let mut projection = Vec::new();
    projection.extend_from_slice(creation.checkpoint_directory_id.as_bytes());
    projection.extend_from_slice(namespace.subscription_id.as_bytes());
    projection.extend_from_slice(namespace.target.as_bytes());
    projection.extend_from_slice(&namespace.generation.to_be_bytes());
    projection.extend_from_slice(&namespace.partition.to_be_bytes());
    projection.extend_from_slice(&namespace.lifecycle_generation.to_be_bytes());
    projection.extend_from_slice(&namespace.namespace_digest);
    projection.extend_from_slice(&creation.resolved_initial.to_be_bytes());
    projection.extend_from_slice(&creation.captured_end_exclusive.to_be_bytes());
    projection.extend_from_slice(&creation.captured_oldest_available.to_be_bytes());
    projection.extend_from_slice(creation.captured_resource_epoch.resource_id().as_bytes());
    projection.extend_from_slice(&creation.captured_resource_epoch.epoch().to_be_bytes());
    let seed = digest_parts(&[
        CORPUS_OWNER_DOMAIN,
        &corpus_source_input_digest(inputs),
        &digest(inputs.requirements),
        &digest(inputs.design),
        &projection,
    ]);
    creation.genesis_owner = OwnerRecord::fixture_genesis(
        seed[..16]
            .try_into()
            .expect("SHA-256 prefix is exactly sixteen bytes"),
    );
    Ok(creation)
}

#[cfg(test)]
fn verify_corpus_recovery_oracle(
    directory: &Path,
    case: &CorpusCase,
    creation: &CreationUnit,
) -> Result<(), CreationStoreError> {
    let result = CreationStore.recover(
        directory,
        creation.recovery_binding(),
        creation.captured_resource_epoch(),
    )?;
    let resolves_new = match result {
        CreationStoreResult::Created(active) => {
            let valid = active.creation() == creation && active.binding().owner_epoch() == 2;
            drop(active);
            valid
        }
        CreationStoreResult::NotCommitted(CreationFailureKind::StorageUnavailable) => false,
        CreationStoreResult::NotCommitted(_) | CreationStoreResult::Unknown(_) => {
            return Err(CreationStoreError::CorruptState);
        }
    };
    if resolves_new != case.oracle.resolves_new() {
        return Err(CreationStoreError::CorruptState);
    }

    let owner = directory.join(OWNER_FILE);
    if owner.exists() {
        fs::remove_file(owner).map_err(|error| CreationStoreError::Io(error.kind()))?;
    }
    let lock = directory.join(LOCK_FILE);
    if lock.exists() {
        fs::remove_file(lock).map_err(|error| CreationStoreError::Io(error.kind()))?;
    }
    Ok(())
}

#[cfg(test)]
fn corpus_file_inventory(root: &Path) -> Result<BTreeSet<PathBuf>, CreationStoreError> {
    fn visit(
        root: &Path,
        current: &Path,
        output: &mut BTreeSet<PathBuf>,
    ) -> Result<(), CreationStoreError> {
        for entry in fs::read_dir(current).map_err(|error| CreationStoreError::Io(error.kind()))? {
            let entry = entry.map_err(|error| CreationStoreError::Io(error.kind()))?;
            let path = entry.path();
            if path.is_dir() {
                visit(root, &path, output)?;
            } else {
                output.insert(
                    path.strip_prefix(root)
                        .map_err(|_| CreationStoreError::CorruptState)?
                        .to_path_buf(),
                );
            }
        }
        Ok(())
    }
    let mut output = BTreeSet::new();
    visit(root, root, &mut output)?;
    Ok(output)
}

#[cfg(test)]
fn corpus_manifest(
    case_bytes: &BTreeMap<&'static str, Vec<u8>>,
    inputs: &CorpusInputs<'_>,
) -> Result<String, CreationStoreError> {
    let requirements_digest = digest(inputs.requirements);
    let design_digest = digest(inputs.design);
    let mut sources = inputs.sources.to_vec();
    sources.sort_by_key(|(path, _)| *path);
    let source_input_digest = corpus_source_input_digest(inputs);

    let mut output = String::from("{\n");
    output.push_str("  \"schema_version\": 1,\n");
    output.push_str("  \"producer_task\": \"4.1\",\n");
    output.push_str(&format!(
        "  \"requirements_sha256\": \"{}\",\n",
        hex(&requirements_digest)
    ));
    output.push_str(&format!(
        "  \"design_sha256\": \"{}\",\n",
        hex(&design_digest)
    ));
    output.push_str(&format!(
        "  \"source_input_sha256\": \"{}\",\n",
        hex(&source_input_digest)
    ));
    output.push_str("  \"sources\": [\n");
    for (index, (path, bytes)) in sources.iter().enumerate() {
        output.push_str(&format!(
            "    {{\"path\":\"{}\",\"sha256\":\"{}\"}}{}\n",
            path,
            hex(&digest(bytes)),
            if index + 1 == sources.len() { "" } else { "," }
        ));
    }
    output.push_str("  ],\n  \"cases\": [\n");
    for (index, case) in CORPUS_CASES.iter().enumerate() {
        let creation_bytes = case_bytes
            .get(case.name)
            .ok_or(CreationStoreError::CorruptState)?;
        let (path, bytes): (&str, &[u8]) = match case.image {
            CorpusImage::NoCanonical => ("", &[]),
            CorpusImage::PartialTemp => (
                ".creation.unit.pending",
                &creation_bytes[..creation_bytes.len() / 2],
            ),
            CorpusImage::FullTemp => (".creation.unit.pending", creation_bytes),
            CorpusImage::Canonical => (CREATION_FILE, creation_bytes),
        };
        output.push_str(&format!(
            "    {{\"name\":\"{}\",\"stage\":\"{}\",\"oracle\":\"{}\",\"file\":\"{}\",\"sha256\":\"{}\"}}{}\n",
            case.name,
            case.stage,
            case.oracle.name(),
            path,
            if path.is_empty() { String::new() } else { hex(&digest(bytes)) },
            if index + 1 == CORPUS_CASES.len() { "" } else { "," }
        ));
    }
    output.push_str("  ]\n}\n");
    Ok(output)
}

#[cfg(test)]
fn corpus_source_input_digest(inputs: &CorpusInputs<'_>) -> [u8; 32] {
    let mut sources = inputs.sources.to_vec();
    sources.sort_by_key(|(path, _)| *path);
    let mut source_projection = Vec::new();
    for (path, bytes) in sources {
        source_projection.extend_from_slice(&(path.len() as u32).to_be_bytes());
        source_projection.extend_from_slice(path.as_bytes());
        source_projection.extend_from_slice(&digest(bytes));
    }
    digest_parts(&[CORPUS_SOURCE_DOMAIN, &source_projection])
}

#[cfg(test)]
fn write_corpus_file(path: &Path, bytes: &[u8]) -> Result<(), CreationStoreError> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|error| CreationStoreError::Io(error.kind()))?;
    file.write_all(bytes)
        .map_err(|error| CreationStoreError::Io(error.kind()))?;
    file.sync_all()
        .map_err(|error| CreationStoreError::Io(error.kind()))?;
    Ok(())
}

#[cfg(test)]
fn snapshot_files(root: &Path) -> Result<BTreeMap<PathBuf, Vec<u8>>, CreationStoreError> {
    let mut snapshot = BTreeMap::new();
    for relative in corpus_file_inventory(root)? {
        snapshot.insert(
            relative.clone(),
            fs::read(root.join(relative)).map_err(|error| CreationStoreError::Io(error.kind()))?,
        );
    }
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use super::{
        CORPUS_CASES, CREATION_FILE, CorpusInputs, CreationNamespace, CreationRequest,
        CreationStore, CreationStoreError, CreationStoreResult, OWNER_FILE,
        generate_creation_corpus, snapshot_files, verify_creation_corpus,
    };
    use crate::state::InstallFault;
    use alopex_chirps_core::durable::{
        CreationFailureKind, CreationRecoveryBinding, InitialPosition, PollObservation,
        ResourceEpoch, ResourceId, SubscriptionCreationOutcome, SubscriptionId,
    };
    use alopex_chirps_wire::node_id::NodeId;
    use std::fs;
    use std::path::Path;
    use std::process::Command;
    use std::thread;
    use std::time::{Duration, Instant};
    use tempfile::tempdir;

    const CHILD_PATH_ENV: &str = "CHIRPS_TASK_4_1_CHILD_PATH";
    const CHILD_READY_ENV: &str = "CHIRPS_TASK_4_1_CHILD_READY";
    const CHILD_RELEASE_ENV: &str = "CHIRPS_TASK_4_1_CHILD_RELEASE";

    fn namespace() -> CreationNamespace {
        let mut target = [0x21; 16];
        target[6] = 0x41;
        target[8] = 0x81;
        CreationNamespace::new(
            SubscriptionId::from_bytes([0x31; 16]),
            NodeId::from(target),
            7,
            3,
            11,
            [0x41; 32],
        )
    }

    fn resource_epoch() -> ResourceEpoch {
        let mut id = [0x51; 16];
        id[6] = 0x41;
        id[8] = 0x91;
        ResourceEpoch::new(ResourceId::from_bytes(id), 9)
    }

    fn observation(oldest: u64, end: u64) -> PollObservation {
        PollObservation::try_new(resource_epoch(), end, oldest, None).unwrap()
    }

    fn created(result: CreationStoreResult) -> super::ActiveSubscription {
        let CreationStoreResult::Created(active) = result else {
            panic!("expected active creation")
        };
        *active
    }

    #[test]
    fn v07_task_4_1_initial_selection_is_resolved_once_and_atomically_owned() {
        for (position, expected) in [
            (InitialPosition::EarliestRetained, 4),
            (InitialPosition::LatestAfterCapturedEnd, 10),
            (InitialPosition::Exact(6), 6),
        ] {
            let root = tempdir().unwrap();
            let directory = root.path().join(format!("case-{expected}"));
            let active = created(
                CreationStore
                    .create(
                        &directory,
                        CreationRequest::new(namespace(), position),
                        &observation(4, 10),
                    )
                    .unwrap(),
            );
            assert_eq!(active.binding().owner_epoch(), 1);
            assert_eq!(active.creation().initial_position(), position);
            assert_eq!(active.creation().resolved_initial(), expected);
            assert_eq!(active.creation().captured_oldest_available(), 4);
            assert_eq!(active.creation().captured_end_exclusive(), 10);
            assert_eq!(active.creation().captured_resource_epoch().epoch(), 9);
            assert!(directory.join(CREATION_FILE).is_file());
            assert!(!directory.join(OWNER_FILE).exists());
            assert_eq!(active.directory(), directory);
        }
    }

    #[test]
    fn v07_task_4_1_invalid_exact_is_known_old_without_visible_creation() {
        for exact in [3, 11] {
            let root = tempdir().unwrap();
            let directory = root.path().join(format!("invalid-{exact}"));
            let result = CreationStore
                .create(
                    &directory,
                    CreationRequest::new(namespace(), InitialPosition::Exact(exact)),
                    &observation(4, 10),
                )
                .unwrap();
            assert!(matches!(
                result.public_outcome(),
                SubscriptionCreationOutcome::CreationNotCommitted(
                    CreationFailureKind::InvalidInitialPosition
                )
            ));
            assert!(!directory.join(CREATION_FILE).exists());
            assert!(!directory.join(OWNER_FILE).exists());
        }
    }

    #[test]
    fn v07_task_4_1_creation_stage_failures_are_known_old_or_exact_recovery_only() {
        for fault in [
            InstallFault::BeforeWrite,
            InstallFault::AfterWrite,
            InstallFault::AfterFileSync,
        ] {
            let root = tempdir().unwrap();
            let directory = root.path().join(format!("known-old-{fault:?}"));
            let result = CreationStore
                .create_with_fault(
                    &directory,
                    CreationRequest::new(namespace(), InitialPosition::Exact(6)),
                    &observation(4, 10),
                    fault,
                )
                .unwrap();
            assert!(matches!(
                result,
                CreationStoreResult::NotCommitted(CreationFailureKind::StorageUnavailable)
            ));
            assert!(!directory.join(CREATION_FILE).exists());
        }

        for fault in [InstallFault::AfterRename, InstallFault::AfterDirectorySync] {
            let root = tempdir().unwrap();
            let directory = root.path().join(format!("unknown-{fault:?}"));
            let result = CreationStore
                .create_with_fault(
                    &directory,
                    CreationRequest::new(namespace(), InitialPosition::Exact(6)),
                    &observation(4, 10),
                    fault,
                )
                .unwrap();
            let CreationStoreResult::Unknown(binding) = result else {
                panic!("post-visibility failure must be unknown")
            };
            assert_eq!(binding.initial_position(), InitialPosition::Exact(6));
            assert_eq!(
                CreationStore
                    .create(
                        &directory,
                        CreationRequest::new(namespace(), InitialPosition::Exact(7)),
                        &observation(4, 10),
                    )
                    .unwrap_err(),
                CreationStoreError::ExistingCreation
            );
            let active = created(
                CreationStore
                    .recover(&directory, binding, resource_epoch())
                    .unwrap(),
            );
            assert_eq!(active.binding().owner_epoch(), 2);
            assert_eq!(active.creation().resolved_initial(), 6);
        }
    }

    #[test]
    fn v07_task_4_1_recovery_rejects_another_directory_subscription_or_selection() {
        let root = tempdir().unwrap();
        let directory = root.path().join("original");
        let result = CreationStore
            .create_with_fault(
                &directory,
                CreationRequest::new(namespace(), InitialPosition::Exact(6)),
                &observation(4, 10),
                InstallFault::AfterRename,
            )
            .unwrap();
        let CreationStoreResult::Unknown(binding) = result else {
            panic!("expected unknown")
        };
        let other = root.path().join("other");
        fs::create_dir(&other).unwrap();
        fs::copy(directory.join(CREATION_FILE), other.join(CREATION_FILE)).unwrap();
        assert_eq!(
            CreationStore
                .recover(&other, binding, resource_epoch())
                .unwrap_err(),
            CreationStoreError::RecoveryBindingMismatch
        );
        let wrong = CreationRecoveryBinding::new(
            binding.checkpoint_directory_id(),
            binding.subscription_id(),
            InitialPosition::Exact(7),
        );
        assert_eq!(
            CreationStore
                .recover(&directory, wrong, resource_epoch())
                .unwrap_err(),
            CreationStoreError::RecoveryBindingMismatch
        );
    }

    #[test]
    fn v07_task_4_1_resource_epoch_mismatch_fails_before_owner_install() {
        let root = tempdir().unwrap();
        let directory = root.path().join("epoch-mismatch");
        drop(created(
            CreationStore
                .create(
                    &directory,
                    CreationRequest::new(namespace(), InitialPosition::EarliestRetained),
                    &observation(4, 10),
                )
                .unwrap(),
        ));
        let replacement = ResourceEpoch::new(resource_epoch().resource_id(), 10);
        assert_eq!(
            CreationStore
                .open(&directory, namespace(), replacement)
                .unwrap_err(),
            CreationStoreError::ResourceEpochMismatch
        );
        assert!(!directory.join(OWNER_FILE).exists());
    }

    #[test]
    fn v07_task_4_1_each_open_installs_one_linked_owner_and_corruption_fail_stops() {
        let root = tempdir().unwrap();
        let directory = root.path().join("chain");
        let first = created(
            CreationStore
                .create(
                    &directory,
                    CreationRequest::new(namespace(), InitialPosition::EarliestRetained),
                    &observation(4, 10),
                )
                .unwrap(),
        );
        let genesis_digest = first.owner().digest();
        drop(first);

        let second = created(
            CreationStore
                .open(&directory, namespace(), resource_epoch())
                .unwrap(),
        );
        assert_eq!(second.binding().owner_epoch(), 2);
        assert_eq!(second.creation().resolved_initial(), 4);
        assert_eq!(second.owner().previous_owner_digest(), genesis_digest);
        let second_digest = second.owner().digest();
        let creation_digest = second.creation().digest();
        drop(second);

        let third = created(
            CreationStore
                .open(&directory, namespace(), resource_epoch())
                .unwrap(),
        );
        assert_eq!(third.binding().owner_epoch(), 3);
        assert_eq!(third.creation().resolved_initial(), 4);
        assert_eq!(third.owner().previous_owner_digest(), second_digest);
        assert_eq!(third.owner().creation_digest(), creation_digest);
        drop(third);

        let mut corrupt = fs::read(directory.join(OWNER_FILE)).unwrap();
        corrupt[20] ^= 1;
        fs::write(directory.join(OWNER_FILE), corrupt).unwrap();
        assert!(matches!(
            CreationStore.open(&directory, namespace(), resource_epoch()),
            Err(CreationStoreError::Owner(_) | CreationStoreError::CorruptState)
        ));
    }

    #[test]
    fn v07_task_4_1_owner_install_failure_never_activates_an_uncommitted_candidate() {
        for fault in [InstallFault::AfterWrite, InstallFault::AfterFileSync] {
            let root = tempdir().unwrap();
            let directory = root.path().join(format!("owner-old-{fault:?}"));
            drop(created(
                CreationStore
                    .create(
                        &directory,
                        CreationRequest::new(namespace(), InitialPosition::EarliestRetained),
                        &observation(4, 10),
                    )
                    .unwrap(),
            ));
            assert!(matches!(
                CreationStore
                    .open_with_fault(&directory, namespace(), resource_epoch(), fault)
                    .unwrap(),
                CreationStoreResult::NotCommitted(CreationFailureKind::StorageUnavailable)
            ));
            assert!(!directory.join(OWNER_FILE).exists());
            let active = created(
                CreationStore
                    .open(&directory, namespace(), resource_epoch())
                    .unwrap(),
            );
            assert_eq!(active.binding().owner_epoch(), 2);
        }

        for fault in [InstallFault::AfterRename, InstallFault::AfterDirectorySync] {
            let root = tempdir().unwrap();
            let directory = root.path().join(format!("owner-unknown-{fault:?}"));
            drop(created(
                CreationStore
                    .create(
                        &directory,
                        CreationRequest::new(namespace(), InitialPosition::EarliestRetained),
                        &observation(4, 10),
                    )
                    .unwrap(),
            ));
            assert!(matches!(
                CreationStore
                    .open_with_fault(&directory, namespace(), resource_epoch(), fault)
                    .unwrap(),
                CreationStoreResult::Unknown(_)
            ));
            let active = created(
                CreationStore
                    .open(&directory, namespace(), resource_epoch())
                    .unwrap(),
            );
            assert_eq!(active.binding().owner_epoch(), 3);
        }
    }

    #[test]
    fn v07_task_4_1_owner_record_from_another_creation_cannot_fork_the_chain() {
        let root = tempdir().unwrap();
        let first_directory = root.path().join("first-chain");
        let second_directory = root.path().join("second-chain");
        for directory in [&first_directory, &second_directory] {
            drop(created(
                CreationStore
                    .create(
                        directory,
                        CreationRequest::new(namespace(), InitialPosition::EarliestRetained),
                        &observation(4, 10),
                    )
                    .unwrap(),
            ));
            drop(created(
                CreationStore
                    .open(directory, namespace(), resource_epoch())
                    .unwrap(),
            ));
        }
        fs::copy(
            second_directory.join(OWNER_FILE),
            first_directory.join(OWNER_FILE),
        )
        .unwrap();
        assert!(matches!(
            CreationStore.open(&first_directory, namespace(), resource_epoch()),
            Err(CreationStoreError::Owner(_))
        ));
    }

    #[test]
    fn v07_task_4_1_two_process_owner_lock_is_exclusive() {
        if let Ok(path) = std::env::var(CHILD_PATH_ENV) {
            let active = created(
                CreationStore
                    .open(Path::new(&path), namespace(), resource_epoch())
                    .unwrap(),
            );
            fs::write(std::env::var(CHILD_READY_ENV).unwrap(), b"ready").unwrap();
            let release = std::env::var(CHILD_RELEASE_ENV).unwrap();
            let deadline = Instant::now() + Duration::from_secs(10);
            while !Path::new(&release).exists() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(10));
            }
            assert!(Path::new(&release).exists());
            drop(active);
            return;
        }

        let root = tempdir().unwrap();
        let directory = root.path().join("process-lock");
        drop(created(
            CreationStore
                .create(
                    &directory,
                    CreationRequest::new(namespace(), InitialPosition::EarliestRetained),
                    &observation(4, 10),
                )
                .unwrap(),
        ));
        let ready = root.path().join("ready");
        let release = root.path().join("release");
        let test_name = "state::creation::tests::v07_task_4_1_two_process_owner_lock_is_exclusive";
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(test_name)
            .arg("--nocapture")
            .env(CHILD_PATH_ENV, &directory)
            .env(CHILD_READY_ENV, &ready)
            .env(CHILD_RELEASE_ENV, &release)
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !ready.exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(ready.exists(), "child did not acquire the lifetime lock");
        assert!(matches!(
            CreationStore
                .open(&directory, namespace(), resource_epoch())
                .unwrap(),
            CreationStoreResult::NotCommitted(CreationFailureKind::OwnerLockUnavailable)
        ));
        fs::write(&release, b"release").unwrap();
        assert!(child.wait().unwrap().success());
        let next = created(
            CreationStore
                .open(&directory, namespace(), resource_epoch())
                .unwrap(),
        );
        assert_eq!(next.binding().owner_epoch(), 3);
    }

    #[test]
    fn v07_task_4_1_provisional_corpus_is_complete_digest_bound_and_repeatable() {
        let root = tempdir().unwrap();
        let active = created(
            CreationStore
                .create(
                    &root.path().join("source"),
                    CreationRequest::new(namespace(), InitialPosition::Exact(6)),
                    &observation(4, 10),
                )
                .unwrap(),
        );
        // Generator unit inputs are deliberately synthetic. Release evidence
        // continues to require the authenticated original specification bytes.
        let requirements: &[u8] = b"Synthetic unit-test requirements; not release evidence.\n";
        let design: &[u8] = b"Synthetic unit-test design; not release evidence.\n";
        let sources: &[(&str, &[u8])] = &[
            ("lib.rs", include_bytes!("../lib.rs")),
            ("state/creation.rs", include_bytes!("creation.rs")),
            ("state/mod.rs", include_bytes!("mod.rs")),
            ("state/owner.rs", include_bytes!("owner.rs")),
        ];
        let inputs = CorpusInputs {
            requirements,
            design,
            sources,
        };
        let output = root.path().join("unit-creation-corpus");
        generate_creation_corpus(&output, active.creation(), &inputs).unwrap();
        verify_creation_corpus(&output, active.creation(), &inputs).unwrap();
        let first = snapshot_files(&output).unwrap();
        let mut alternate_template = active.creation().clone();
        alternate_template.genesis_owner = super::OwnerRecord::fresh_genesis();
        assert_ne!(
            alternate_template.genesis_owner.owner_id(),
            active.creation().genesis_owner.owner_id()
        );
        generate_creation_corpus(&output, &alternate_template, &inputs).unwrap();
        verify_creation_corpus(&output, &alternate_template, &inputs).unwrap();
        assert_eq!(snapshot_files(&output).unwrap(), first);
        assert_eq!(CORPUS_CASES.len(), 7);
        let manifest = fs::read_to_string(output.join("manifest.json")).unwrap();
        assert!(
            manifest.contains("85c0d95e5e7ca2f85a4092b51ac17c0bdfabef272fc181e66e6a4e5bc8d6e524")
        );
        assert!(
            manifest.contains("4da71815b5658adacaa3e6ed607c7d22dd32cd1781494d3361bb55fd9e9016f4")
        );
        assert!(manifest.contains("\"producer_task\": \"4.1\""));
        assert!(manifest.contains("unknown-old"));
        assert!(manifest.contains("unknown-new"));
        for (requirements, design) in [
            (b"changed unit requirements".as_slice(), design),
            (requirements, b"changed unit design".as_slice()),
        ] {
            let altered = CorpusInputs {
                requirements,
                design,
                sources,
            };
            assert_eq!(
                verify_creation_corpus(&output, active.creation(), &altered).unwrap_err(),
                CreationStoreError::CorruptState
            );
        }
    }
}
