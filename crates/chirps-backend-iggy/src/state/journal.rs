//! Framed identity and checkpoint journal with contiguous-prefix recovery.

use super::identity::{IdentityCandidate, IdentityError, IdentityRecord};
use super::{
    InstallError, InstallFault, StateFrameError, StateRecordKind, decode_state_frame,
    durably_install, encode_state_frame, state_frame_encoded_len,
};
#[cfg(test)]
use super::{digest, digest_parts, hex, sync_directory};
use alopex_chirps_core::durable::{
    CheckpointInstallBinding, CheckpointOperationPhase, CheckpointOutcome, ResourceEpoch,
    ResourceId, SubscriptionId,
};
use alopex_chirps_wire::node_id::NodeId;
use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;

const JOURNAL_FILE: &str = "checkpoint.journal";
#[cfg(test)]
const JOURNAL_PENDING_FILE: &str = ".checkpoint.journal.pending";
const JOURNAL_GENERATION: u64 = 1;
const HEADER_BODY_LEN: usize = 100;
const CHECKPOINT_BODY_LEN: usize = 148;
pub(crate) const MAX_JOURNAL_LEN: u64 = 16 * 1024 * 1024;
const COMMIT_MARKER: &[u8; 8] = b"COMMIT07";
#[cfg(test)]
const CORPUS_SOURCE_DOMAIN: &[u8] = b"chirps-v0.7-task-4.2-source-input\0";

/// Immutable journal namespace linked to one creation and resource epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct JournalNamespace {
    subscription_id: SubscriptionId,
    target: NodeId,
    generation: u64,
    partition: u32,
    lifecycle_generation: u64,
    resolved_initial: u64,
    resource_epoch: ResourceEpoch,
}

impl JournalNamespace {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        subscription_id: SubscriptionId,
        target: NodeId,
        generation: u64,
        partition: u32,
        lifecycle_generation: u64,
        resolved_initial: u64,
        resource_epoch: ResourceEpoch,
    ) -> Self {
        Self {
            subscription_id,
            target,
            generation,
            partition,
            lifecycle_generation,
            resolved_initial,
            resource_epoch,
        }
    }

    fn encode_body(self) -> [u8; HEADER_BODY_LEN] {
        let mut body = [0_u8; HEADER_BODY_LEN];
        body[0..8].copy_from_slice(&JOURNAL_GENERATION.to_be_bytes());
        body[8..24].copy_from_slice(self.subscription_id.as_bytes());
        body[24..40].copy_from_slice(self.target.as_bytes());
        body[40..48].copy_from_slice(&self.generation.to_be_bytes());
        body[48..52].copy_from_slice(&self.partition.to_be_bytes());
        body[52..60].copy_from_slice(&self.lifecycle_generation.to_be_bytes());
        body[60..68].copy_from_slice(&self.resolved_initial.to_be_bytes());
        body[68..84].copy_from_slice(self.resource_epoch.resource_id().as_bytes());
        body[84..92].copy_from_slice(&self.resource_epoch.epoch().to_be_bytes());
        body[92..100].copy_from_slice(COMMIT_MARKER);
        body
    }

    fn decode_body(body: &[u8]) -> Result<Self, JournalError> {
        if body.len() != HEADER_BODY_LEN
            || u64::from_be_bytes(body[0..8].try_into().expect("fixed header"))
                != JOURNAL_GENERATION
            || &body[92..100] != COMMIT_MARKER
        {
            return Err(JournalError::CorruptJournal);
        }
        let target_bytes: [u8; 16] = body[24..40]
            .try_into()
            .map_err(|_| JournalError::CorruptJournal)?;
        let target = NodeId::from_bytes(&target_bytes).map_err(|_| JournalError::CorruptJournal)?;
        Ok(Self {
            subscription_id: SubscriptionId::from_bytes(
                body[8..24]
                    .try_into()
                    .map_err(|_| JournalError::CorruptJournal)?,
            ),
            target,
            generation: u64::from_be_bytes(
                body[40..48]
                    .try_into()
                    .map_err(|_| JournalError::CorruptJournal)?,
            ),
            partition: u32::from_be_bytes(
                body[48..52]
                    .try_into()
                    .map_err(|_| JournalError::CorruptJournal)?,
            ),
            lifecycle_generation: u64::from_be_bytes(
                body[52..60]
                    .try_into()
                    .map_err(|_| JournalError::CorruptJournal)?,
            ),
            resolved_initial: u64::from_be_bytes(
                body[60..68]
                    .try_into()
                    .map_err(|_| JournalError::CorruptJournal)?,
            ),
            resource_epoch: ResourceEpoch::new(
                ResourceId::from_bytes(
                    body[68..84]
                        .try_into()
                        .map_err(|_| JournalError::CorruptJournal)?,
                ),
                u64::from_be_bytes(
                    body[84..92]
                        .try_into()
                        .map_err(|_| JournalError::CorruptJournal)?,
                ),
            ),
        })
    }
}

/// A complete canonical checkpoint and its exact arbitration identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckpointRecord {
    sequence: u64,
    subscription_id: SubscriptionId,
    target: NodeId,
    generation: u64,
    partition: u32,
    offset: u64,
    message_id: [u8; 16],
    envelope_digest: [u8; 32],
    delivery_attempt: u64,
    owner_epoch: u64,
    lifecycle_generation: u64,
    checkpoint_attempt: u64,
}

impl CheckpointRecord {
    fn from_binding(sequence: u64, binding: CheckpointInstallBinding) -> Self {
        Self {
            sequence,
            subscription_id: binding.subscription_id(),
            target: binding.target(),
            generation: binding.generation(),
            partition: binding.partition(),
            offset: binding.offset(),
            message_id: *binding.message_id().as_bytes(),
            envelope_digest: *binding.envelope_digest().as_bytes(),
            delivery_attempt: binding.delivery_attempt(),
            owner_epoch: binding.owner_epoch(),
            lifecycle_generation: binding.lifecycle_generation(),
            checkpoint_attempt: binding.checkpoint_attempt(),
        }
    }

    fn encode_body(&self) -> [u8; CHECKPOINT_BODY_LEN] {
        let mut body = [0_u8; CHECKPOINT_BODY_LEN];
        body[0..8].copy_from_slice(&self.sequence.to_be_bytes());
        body[8..24].copy_from_slice(self.subscription_id.as_bytes());
        body[24..40].copy_from_slice(self.target.as_bytes());
        body[40..48].copy_from_slice(&self.generation.to_be_bytes());
        body[48..52].copy_from_slice(&self.partition.to_be_bytes());
        body[52..60].copy_from_slice(&self.offset.to_be_bytes());
        body[60..76].copy_from_slice(&self.message_id);
        body[76..108].copy_from_slice(&self.envelope_digest);
        body[108..116].copy_from_slice(&self.delivery_attempt.to_be_bytes());
        body[116..124].copy_from_slice(&self.owner_epoch.to_be_bytes());
        body[124..132].copy_from_slice(&self.lifecycle_generation.to_be_bytes());
        body[132..140].copy_from_slice(&self.checkpoint_attempt.to_be_bytes());
        body[140..148].copy_from_slice(COMMIT_MARKER);
        body
    }

    fn decode_body(body: &[u8]) -> Result<Self, JournalError> {
        if body.len() != CHECKPOINT_BODY_LEN || &body[140..148] != COMMIT_MARKER {
            return Err(JournalError::CorruptJournal);
        }
        let target_bytes: [u8; 16] = body[24..40]
            .try_into()
            .map_err(|_| JournalError::CorruptJournal)?;
        let target = NodeId::from_bytes(&target_bytes).map_err(|_| JournalError::CorruptJournal)?;
        let record = Self {
            sequence: fixed_u64(body, 0)?,
            subscription_id: SubscriptionId::from_bytes(
                body[8..24]
                    .try_into()
                    .map_err(|_| JournalError::CorruptJournal)?,
            ),
            target,
            generation: fixed_u64(body, 40)?,
            partition: u32::from_be_bytes(
                body[48..52]
                    .try_into()
                    .map_err(|_| JournalError::CorruptJournal)?,
            ),
            offset: fixed_u64(body, 52)?,
            message_id: body[60..76]
                .try_into()
                .map_err(|_| JournalError::CorruptJournal)?,
            envelope_digest: body[76..108]
                .try_into()
                .map_err(|_| JournalError::CorruptJournal)?,
            delivery_attempt: fixed_u64(body, 108)?,
            owner_epoch: fixed_u64(body, 116)?,
            lifecycle_generation: fixed_u64(body, 124)?,
            checkpoint_attempt: fixed_u64(body, 132)?,
        };
        if record.delivery_attempt == 0
            || record.owner_epoch == 0
            || record.checkpoint_attempt == 0
            || record.message_id[6] & 0xf0 != 0x40
            || record.message_id[8] & 0xc0 != 0x80
        {
            return Err(JournalError::CorruptJournal);
        }
        Ok(record)
    }

    pub(crate) const fn sequence(&self) -> u64 {
        self.sequence
    }

    pub(crate) const fn offset(&self) -> u64 {
        self.offset
    }

    pub(crate) const fn message_id_bytes(&self) -> [u8; 16] {
        self.message_id
    }

    pub(crate) fn compaction_bytes(&self) -> Vec<u8> {
        self.encode_body().to_vec()
    }
}

/// Test seam for physical append-stage failures; never exported by the facade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppendFault {
    None,
    BeforeWrite,
    AfterWrite,
    AfterFileSync,
}

#[derive(Debug)]
pub(crate) enum JournalInitialization {
    NotCommitted,
    Unknown,
    Ready(Box<JournalStore>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum IdentityPersistOutcome {
    NotCommitted,
    Unknown,
    Committed(IdentityRecord),
}

/// One recovered journal held under the subscription's separately owned lock.
#[derive(Debug)]
pub(crate) struct JournalStore {
    path: PathBuf,
    namespace: JournalNamespace,
    owner_epoch: u64,
    sequence: u64,
    checkpoint: Option<CheckpointRecord>,
    identities: BTreeMap<[u8; 16], IdentityRecord>,
    recovery_required: bool,
}

impl JournalStore {
    pub(crate) fn initialize(
        directory: &Path,
        namespace: JournalNamespace,
        owner_epoch: u64,
    ) -> Result<JournalInitialization, JournalError> {
        Self::initialize_with_fault(directory, namespace, owner_epoch, InstallFault::None)
    }

    fn initialize_with_fault(
        directory: &Path,
        namespace: JournalNamespace,
        owner_epoch: u64,
        fault: InstallFault,
    ) -> Result<JournalInitialization, JournalError> {
        if owner_epoch == 0 {
            return Err(JournalError::InvalidOwnerEpoch);
        }
        let path = directory.join(JOURNAL_FILE);
        if path.exists() {
            return Err(JournalError::ExistingJournal);
        }
        let header = encode_state_frame(StateRecordKind::JournalHeader, &namespace.encode_body())?;
        match durably_install(&path, &header, fault) {
            Ok(()) => Ok(JournalInitialization::Ready(Box::new(Self {
                path,
                namespace,
                owner_epoch,
                sequence: 0,
                checkpoint: None,
                identities: BTreeMap::new(),
                recovery_required: false,
            }))),
            Err(InstallError::KnownOld { .. }) => Ok(JournalInitialization::NotCommitted),
            Err(InstallError::Unknown { .. }) => Ok(JournalInitialization::Unknown),
        }
    }

    pub(crate) fn open(
        directory: &Path,
        expected: JournalNamespace,
        owner_epoch: u64,
    ) -> Result<Self, JournalError> {
        if owner_epoch == 0 {
            return Err(JournalError::InvalidOwnerEpoch);
        }
        let path = directory.join(JOURNAL_FILE);
        let recovered = recover_file(&path)?;
        if recovered.namespace != expected {
            return Err(JournalError::NamespaceMismatch);
        }
        if recovered.valid_length < recovered.file_length {
            let file = OpenOptions::new()
                .write(true)
                .open(&path)
                .map_err(|error| JournalError::Io(error.kind()))?;
            file.set_len(recovered.valid_length)
                .map_err(|error| JournalError::Io(error.kind()))?;
            file.sync_all()
                .map_err(|error| JournalError::Io(error.kind()))?;
        }
        Ok(Self {
            path,
            namespace: recovered.namespace,
            owner_epoch,
            sequence: recovered.sequence,
            checkpoint: recovered.checkpoint,
            identities: recovered.identities,
            recovery_required: false,
        })
    }

    pub(crate) fn persist_identity(
        &mut self,
        candidate: IdentityCandidate,
    ) -> Result<IdentityPersistOutcome, JournalError> {
        self.persist_identity_with_fault(candidate, AppendFault::None)
    }

    fn persist_identity_with_fault(
        &mut self,
        candidate: IdentityCandidate,
        fault: AppendFault,
    ) -> Result<IdentityPersistOutcome, JournalError> {
        self.ensure_ready()?;
        if candidate.partition() != self.namespace.partition
            || candidate.resource_epoch() != self.namespace.resource_epoch
            || candidate.offset() != self.expected_offset()?
        {
            return Err(JournalError::IdentityNamespaceMismatch);
        }
        let key = *candidate.message_id().as_bytes();
        if self.identities.iter().any(|(message_id, identity)| {
            *message_id != key && identity.offset() == candidate.offset()
        }) {
            return Err(JournalError::IdentityOffsetConflict);
        }
        let next = match self.identities.get(&key) {
            Some(current) => current.observe(candidate)?,
            None => IdentityRecord::first(candidate),
        };
        let sequence = self.next_sequence()?;
        let frame = encode_identity_frame(sequence, &next)?;
        match append_frame(&self.path, &frame, fault) {
            AppendResult::KnownOld => Ok(IdentityPersistOutcome::NotCommitted),
            AppendResult::Unknown => {
                self.recovery_required = true;
                Ok(IdentityPersistOutcome::Unknown)
            }
            AppendResult::Committed => {
                self.sequence = sequence;
                self.identities.insert(key, next.clone());
                Ok(IdentityPersistOutcome::Committed(next))
            }
        }
    }

    pub(crate) fn install_checkpoint(
        &mut self,
        binding: CheckpointInstallBinding,
    ) -> Result<CheckpointOutcome, JournalError> {
        self.install_checkpoint_with_phase(binding, |_| true)
    }

    pub(crate) fn install_checkpoint_with_phase(
        &mut self,
        binding: CheckpointInstallBinding,
        transition: impl FnMut(CheckpointOperationPhase) -> bool,
    ) -> Result<CheckpointOutcome, JournalError> {
        self.install_checkpoint_with_fault_and_phase(binding, AppendFault::None, transition)
    }

    fn install_checkpoint_with_fault(
        &mut self,
        binding: CheckpointInstallBinding,
        fault: AppendFault,
    ) -> Result<CheckpointOutcome, JournalError> {
        self.install_checkpoint_with_fault_and_phase(binding, fault, |_| true)
    }

    fn install_checkpoint_with_fault_and_phase(
        &mut self,
        binding: CheckpointInstallBinding,
        fault: AppendFault,
        mut transition: impl FnMut(CheckpointOperationPhase) -> bool,
    ) -> Result<CheckpointOutcome, JournalError> {
        self.ensure_ready()?;
        self.validate_binding(binding)?;
        let key = *binding.message_id().as_bytes();
        let identity = self
            .identities
            .get(&key)
            .ok_or(JournalError::MissingDurableIdentity)?;
        if identity.envelope_digest_bytes() != *binding.envelope_digest().as_bytes()
            || identity.partition() != binding.partition()
            || identity.offset() != binding.offset()
            || identity.last_delivery_attempt() != binding.delivery_attempt()
            || identity.is_checkpointed()
        {
            return Err(JournalError::CheckpointBindingMismatch);
        }
        let sequence = self.next_sequence()?;
        let checkpoint = CheckpointRecord::from_binding(sequence, binding);
        let frame =
            encode_state_frame(StateRecordKind::CheckpointCommit, &checkpoint.encode_body())?;
        match append_checkpoint_frame(&self.path, &frame, fault, &mut transition) {
            AppendResult::KnownOld => Ok(CheckpointOutcome::CheckpointNotCommitted),
            AppendResult::Unknown => {
                self.recovery_required = true;
                Ok(CheckpointOutcome::CheckpointUnknown)
            }
            AppendResult::Committed => {
                let checkpointed = identity.mark_checkpointed()?;
                self.sequence = sequence;
                self.identities.insert(key, checkpointed);
                self.checkpoint = Some(checkpoint);
                Ok(CheckpointOutcome::CheckpointCommitted)
            }
        }
    }

    fn validate_binding(&self, binding: CheckpointInstallBinding) -> Result<(), JournalError> {
        if binding.subscription_id() != self.namespace.subscription_id
            || binding.target() != self.namespace.target
            || binding.generation() != self.namespace.generation
            || binding.partition() != self.namespace.partition
            || binding.owner_epoch() != self.owner_epoch
            || binding.lifecycle_generation() != self.namespace.lifecycle_generation
            || binding.offset() != self.expected_offset()?
            || binding.delivery_attempt() == 0
            || binding.checkpoint_attempt() == 0
        {
            return Err(JournalError::CheckpointBindingMismatch);
        }
        Ok(())
    }

    fn ensure_ready(&self) -> Result<(), JournalError> {
        if self.recovery_required {
            Err(JournalError::RecoveryRequired)
        } else {
            Ok(())
        }
    }

    fn next_sequence(&self) -> Result<u64, JournalError> {
        self.sequence
            .checked_add(1)
            .ok_or(JournalError::SequenceExhausted)
    }

    pub(crate) fn expected_offset(&self) -> Result<u64, JournalError> {
        match &self.checkpoint {
            Some(checkpoint) => checkpoint
                .offset
                .checked_add(1)
                .ok_or(JournalError::OffsetExhausted),
            None => Ok(self.namespace.resolved_initial),
        }
    }

    pub(crate) const fn sequence(&self) -> u64 {
        self.sequence
    }

    pub(crate) const fn checkpoint(&self) -> Option<&CheckpointRecord> {
        self.checkpoint.as_ref()
    }

    pub(crate) fn identity(&self, message_id: [u8; 16]) -> Option<&IdentityRecord> {
        self.identities.get(&message_id)
    }
}

#[derive(Debug)]
struct RecoveredJournal {
    namespace: JournalNamespace,
    sequence: u64,
    checkpoint: Option<CheckpointRecord>,
    identities: BTreeMap<[u8; 16], IdentityRecord>,
    valid_length: u64,
    file_length: u64,
}

fn recover_file(path: &Path) -> Result<RecoveredJournal, JournalError> {
    let metadata = fs::metadata(path).map_err(|error| match error.kind() {
        io::ErrorKind::NotFound => JournalError::MissingJournal,
        kind => JournalError::Io(kind),
    })?;
    if metadata.len() > MAX_JOURNAL_LEN {
        return Err(JournalError::JournalTooLarge);
    }
    let bytes = fs::read(path).map_err(|error| JournalError::Io(error.kind()))?;
    recover_bytes(&bytes)
}

fn recover_bytes(bytes: &[u8]) -> Result<RecoveredJournal, JournalError> {
    let header_len = state_frame_encoded_len(bytes)?.ok_or(JournalError::CorruptJournal)?;
    let namespace = JournalNamespace::decode_body(decode_state_frame(
        &bytes[..header_len],
        StateRecordKind::JournalHeader,
    )?)?;
    let mut recovered = RecoveredJournal {
        namespace,
        sequence: 0,
        checkpoint: None,
        identities: BTreeMap::new(),
        valid_length: header_len as u64,
        file_length: bytes.len() as u64,
    };
    let mut position = header_len;
    while position < bytes.len() {
        let Some(length) = state_frame_encoded_len(&bytes[position..])? else {
            break;
        };
        let frame = &bytes[position..position + length];
        match frame[10] {
            value if value == StateRecordKind::IdentityMutation as u8 => {
                let body = decode_state_frame(frame, StateRecordKind::IdentityMutation)?;
                apply_identity_frame(&mut recovered, body)?;
            }
            value if value == StateRecordKind::CheckpointCommit as u8 => {
                let body = decode_state_frame(frame, StateRecordKind::CheckpointCommit)?;
                apply_checkpoint_frame(&mut recovered, body)?;
            }
            _ => return Err(JournalError::CorruptJournal),
        }
        position += length;
        recovered.valid_length = position as u64;
    }
    Ok(recovered)
}

fn apply_identity_frame(recovered: &mut RecoveredJournal, body: &[u8]) -> Result<(), JournalError> {
    if body.len() < 16 || &body[body.len() - 8..] != COMMIT_MARKER {
        return Err(JournalError::CorruptJournal);
    }
    let sequence = fixed_u64(body, 0)?;
    expect_next_sequence(recovered.sequence, sequence)?;
    let identity = IdentityRecord::decode_body(&body[8..body.len() - 8])?;
    if identity.partition() != recovered.namespace.partition
        || identity.resource_epoch() != recovered.namespace.resource_epoch
        || identity.offset() != expected_offset(recovered.namespace, recovered.checkpoint.as_ref())?
    {
        return Err(JournalError::CorruptJournal);
    }
    let key = identity.message_id_bytes();
    if recovered
        .identities
        .iter()
        .any(|(message_id, current)| *message_id != key && current.offset() == identity.offset())
    {
        return Err(JournalError::IdentityOffsetConflict);
    }
    if let Some(current) = recovered.identities.get(&key) {
        current.validate_successor(&identity)?;
    } else if identity.observation_count() != 1 || identity.is_checkpointed() {
        return Err(JournalError::CorruptJournal);
    }
    recovered.identities.insert(key, identity);
    recovered.sequence = sequence;
    Ok(())
}

fn apply_checkpoint_frame(
    recovered: &mut RecoveredJournal,
    body: &[u8],
) -> Result<(), JournalError> {
    let checkpoint = CheckpointRecord::decode_body(body)?;
    expect_next_sequence(recovered.sequence, checkpoint.sequence)?;
    if checkpoint.subscription_id != recovered.namespace.subscription_id
        || checkpoint.target != recovered.namespace.target
        || checkpoint.generation != recovered.namespace.generation
        || checkpoint.partition != recovered.namespace.partition
        || checkpoint.lifecycle_generation != recovered.namespace.lifecycle_generation
        || checkpoint.offset != expected_offset(recovered.namespace, recovered.checkpoint.as_ref())?
    {
        return Err(JournalError::CorruptJournal);
    }
    let identity = recovered
        .identities
        .get(&checkpoint.message_id)
        .ok_or(JournalError::CorruptJournal)?;
    if identity.envelope_digest_bytes() != checkpoint.envelope_digest
        || identity.partition() != checkpoint.partition
        || identity.offset() != checkpoint.offset
        || identity.last_delivery_attempt() != checkpoint.delivery_attempt
        || identity.is_checkpointed()
    {
        return Err(JournalError::CorruptJournal);
    }
    let checkpointed = identity.mark_checkpointed()?;
    recovered
        .identities
        .insert(checkpoint.message_id, checkpointed);
    recovered.sequence = checkpoint.sequence;
    recovered.checkpoint = Some(checkpoint);
    Ok(())
}

fn expected_offset(
    namespace: JournalNamespace,
    checkpoint: Option<&CheckpointRecord>,
) -> Result<u64, JournalError> {
    match checkpoint {
        Some(record) => record
            .offset
            .checked_add(1)
            .ok_or(JournalError::OffsetExhausted),
        None => Ok(namespace.resolved_initial),
    }
}

fn encode_identity_frame(
    sequence: u64,
    identity: &IdentityRecord,
) -> Result<Vec<u8>, StateFrameError> {
    let identity_body = identity.encode_body();
    let mut body = Vec::with_capacity(8 + identity_body.len() + COMMIT_MARKER.len());
    body.extend_from_slice(&sequence.to_be_bytes());
    body.extend_from_slice(&identity_body);
    body.extend_from_slice(COMMIT_MARKER);
    encode_state_frame(StateRecordKind::IdentityMutation, &body)
}

fn append_frame(path: &Path, frame: &[u8], fault: AppendFault) -> AppendResult {
    if fault == AppendFault::BeforeWrite {
        return AppendResult::KnownOld;
    }
    let mut file = match OpenOptions::new().append(true).open(path) {
        Ok(file) => file,
        Err(_) => return AppendResult::KnownOld,
    };
    if file.write_all(frame).is_err() {
        return AppendResult::Unknown;
    }
    if fault == AppendFault::AfterWrite {
        return AppendResult::Unknown;
    }
    if file.sync_all().is_err() {
        return AppendResult::Unknown;
    }
    if fault == AppendFault::AfterFileSync {
        return AppendResult::Unknown;
    }
    AppendResult::Committed
}

fn append_checkpoint_frame(
    path: &Path,
    frame: &[u8],
    fault: AppendFault,
    transition: &mut dyn FnMut(CheckpointOperationPhase) -> bool,
) -> AppendResult {
    if fault == AppendFault::BeforeWrite {
        transition(CheckpointOperationPhase::KnownOld);
        return AppendResult::KnownOld;
    }
    let mut file = match OpenOptions::new().append(true).open(path) {
        Ok(file) => file,
        Err(_) => {
            transition(CheckpointOperationPhase::KnownOld);
            return AppendResult::KnownOld;
        }
    };
    if !transition(CheckpointOperationPhase::InstallUnknown) {
        return AppendResult::KnownOld;
    }
    if file.write_all(frame).is_err() || fault == AppendFault::AfterWrite {
        return AppendResult::Unknown;
    }
    if file.sync_all().is_err() || fault == AppendFault::AfterFileSync {
        return AppendResult::Unknown;
    }
    transition(CheckpointOperationPhase::Confirmed);
    AppendResult::Committed
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppendResult {
    KnownOld,
    Unknown,
    Committed,
}

fn expect_next_sequence(current: u64, next: u64) -> Result<(), JournalError> {
    if current.checked_add(1) == Some(next) {
        Ok(())
    } else {
        Err(JournalError::NonContiguousSequence)
    }
}

fn fixed_u64(bytes: &[u8], start: usize) -> Result<u64, JournalError> {
    Ok(u64::from_be_bytes(
        bytes
            .get(start..start + 8)
            .ok_or(JournalError::CorruptJournal)?
            .try_into()
            .map_err(|_| JournalError::CorruptJournal)?,
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(crate) enum JournalError {
    #[error("checkpoint journal already exists")]
    ExistingJournal,
    #[error("checkpoint journal is absent")]
    MissingJournal,
    #[error("checkpoint journal namespace does not match")]
    NamespaceMismatch,
    #[error("journal owner epoch must be nonzero")]
    InvalidOwnerEpoch,
    #[error("identity does not belong to the journal namespace/frontier")]
    IdentityNamespaceMismatch,
    #[error("one broker offset cannot be bound to two logical identities")]
    IdentityOffsetConflict,
    #[error("checkpoint does not match the active handle, owner, or frontier")]
    CheckpointBindingMismatch,
    #[error("checkpoint identity was not durably inserted")]
    MissingDurableIdentity,
    #[error("an indeterminate append requires fresh-process recovery")]
    RecoveryRequired,
    #[error("journal sequence is exhausted")]
    SequenceExhausted,
    #[error("checkpoint offset is exhausted")]
    OffsetExhausted,
    #[error("journal sequence has a duplicate or gap")]
    NonContiguousSequence,
    #[error("checkpoint journal exceeds its provisional hard bound")]
    JournalTooLarge,
    #[error("checkpoint journal is corrupt")]
    CorruptJournal,
    #[error("checkpoint journal I/O failed: {0:?}")]
    Io(io::ErrorKind),
    #[error("state frame is invalid: {0}")]
    Frame(#[from] StateFrameError),
    #[error("identity record is invalid: {0}")]
    Identity(#[from] IdentityError),
}

#[cfg(test)]
pub(crate) struct JournalCorpusInputs<'a> {
    pub(crate) requirements: &'a [u8],
    pub(crate) design: &'a [u8],
    pub(crate) sources: &'a [(&'a str, &'a [u8])],
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum CorpusImage {
    NoCanonical,
    PartialPendingHeader,
    FullPendingHeader,
    Header,
    PartialIdentity,
    Identity,
    PartialCheckpoint,
    Checkpoint,
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum CorpusOracle {
    Old,
    UnknownOld,
    UnknownNew,
    New,
    IdentityNew,
    CheckpointOld,
    CheckpointNew,
}

#[cfg(test)]
impl CorpusOracle {
    const fn name(self) -> &'static str {
        match self {
            Self::Old => "old",
            Self::UnknownOld => "unknown-old",
            Self::UnknownNew => "unknown-new",
            Self::New => "new",
            Self::IdentityNew => "identity-new",
            Self::CheckpointOld => "checkpoint-old",
            Self::CheckpointNew => "checkpoint-new",
        }
    }

    const fn expected(self) -> (u64, bool, bool) {
        match self {
            Self::Old | Self::UnknownOld | Self::UnknownNew | Self::New => (0, false, false),
            Self::IdentityNew | Self::CheckpointOld => (1, true, false),
            Self::CheckpointNew => (2, true, true),
        }
    }
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
const CORPUS_CASES: &[CorpusCase] = &[
    CorpusCase {
        name: "initial-write-old",
        stage: "write",
        oracle: CorpusOracle::Old,
        image: CorpusImage::PartialPendingHeader,
    },
    CorpusCase {
        name: "initial-file-sync-old",
        stage: "file-sync",
        oracle: CorpusOracle::Old,
        image: CorpusImage::FullPendingHeader,
    },
    CorpusCase {
        name: "initial-rename-unknown-old",
        stage: "rename",
        oracle: CorpusOracle::UnknownOld,
        image: CorpusImage::FullPendingHeader,
    },
    CorpusCase {
        name: "initial-rename-unknown-new",
        stage: "rename",
        oracle: CorpusOracle::UnknownNew,
        image: CorpusImage::Header,
    },
    CorpusCase {
        name: "initial-directory-sync-unknown-old",
        stage: "directory-sync",
        oracle: CorpusOracle::UnknownOld,
        image: CorpusImage::NoCanonical,
    },
    CorpusCase {
        name: "initial-directory-sync-unknown-new",
        stage: "directory-sync",
        oracle: CorpusOracle::UnknownNew,
        image: CorpusImage::Header,
    },
    CorpusCase {
        name: "initial-directory-sync-new",
        stage: "directory-sync",
        oracle: CorpusOracle::New,
        image: CorpusImage::Header,
    },
    CorpusCase {
        name: "identity-write-old",
        stage: "write",
        oracle: CorpusOracle::Old,
        image: CorpusImage::PartialIdentity,
    },
    CorpusCase {
        name: "identity-write-unknown-old",
        stage: "write",
        oracle: CorpusOracle::Old,
        image: CorpusImage::Header,
    },
    CorpusCase {
        name: "identity-write-unknown-new",
        stage: "write",
        oracle: CorpusOracle::IdentityNew,
        image: CorpusImage::Identity,
    },
    CorpusCase {
        name: "identity-file-sync-new",
        stage: "file-sync",
        oracle: CorpusOracle::IdentityNew,
        image: CorpusImage::Identity,
    },
    CorpusCase {
        name: "checkpoint-write-old",
        stage: "write",
        oracle: CorpusOracle::CheckpointOld,
        image: CorpusImage::PartialCheckpoint,
    },
    CorpusCase {
        name: "checkpoint-write-unknown-old",
        stage: "write",
        oracle: CorpusOracle::CheckpointOld,
        image: CorpusImage::Identity,
    },
    CorpusCase {
        name: "checkpoint-write-unknown-new",
        stage: "write",
        oracle: CorpusOracle::CheckpointNew,
        image: CorpusImage::Checkpoint,
    },
    CorpusCase {
        name: "checkpoint-file-sync-unknown-new",
        stage: "file-sync",
        oracle: CorpusOracle::CheckpointNew,
        image: CorpusImage::Checkpoint,
    },
    CorpusCase {
        name: "rotation-rename-unknown-old",
        stage: "rename",
        oracle: CorpusOracle::CheckpointOld,
        image: CorpusImage::Identity,
    },
    CorpusCase {
        name: "rotation-rename-unknown-new",
        stage: "rename",
        oracle: CorpusOracle::CheckpointNew,
        image: CorpusImage::Checkpoint,
    },
    CorpusCase {
        name: "rotation-directory-sync-unknown-old",
        stage: "directory-sync",
        oracle: CorpusOracle::CheckpointOld,
        image: CorpusImage::Identity,
    },
    CorpusCase {
        name: "rotation-directory-sync-unknown-new",
        stage: "directory-sync",
        oracle: CorpusOracle::CheckpointNew,
        image: CorpusImage::Checkpoint,
    },
    CorpusCase {
        name: "rotation-directory-sync-new",
        stage: "directory-sync",
        oracle: CorpusOracle::CheckpointNew,
        image: CorpusImage::Checkpoint,
    },
];

#[cfg(test)]
struct CorpusFrames {
    header: Vec<u8>,
    identity: Vec<u8>,
    checkpoint: Vec<u8>,
    message_id: [u8; 16],
}

#[cfg(test)]
pub(crate) fn generate_journal_corpus(
    output: &Path,
    inputs: &JournalCorpusInputs<'_>,
) -> Result<(), JournalError> {
    if output.exists() {
        fs::remove_dir_all(output).map_err(|error| JournalError::Io(error.kind()))?;
    }
    fs::create_dir(output).map_err(|error| JournalError::Io(error.kind()))?;
    let frames = corpus_frames(inputs)?;
    let mut case_bytes = BTreeMap::new();
    for case in CORPUS_CASES {
        let directory = output.join(case.name);
        fs::create_dir(&directory).map_err(|error| JournalError::Io(error.kind()))?;
        let (file, bytes) = corpus_image_file(&frames, case.image);
        if !file.is_empty() {
            write_corpus_file(&directory.join(file), &bytes)?;
        }
        sync_directory(&directory).map_err(|error| JournalError::Io(error.kind()))?;
        case_bytes.insert(case.name, bytes);
    }
    let manifest = corpus_manifest(&case_bytes, inputs)?;
    write_corpus_file(&output.join("manifest.json"), manifest.as_bytes())?;
    sync_directory(output).map_err(|error| JournalError::Io(error.kind()))?;
    Ok(())
}

#[cfg(test)]
pub(crate) fn verify_journal_corpus(
    output: &Path,
    inputs: &JournalCorpusInputs<'_>,
) -> Result<(), JournalError> {
    let frames = corpus_frames(inputs)?;
    let mut expected_paths = BTreeSet::from([PathBuf::from("manifest.json")]);
    let mut case_bytes = BTreeMap::new();
    for case in CORPUS_CASES {
        let (file, expected) = corpus_image_file(&frames, case.image);
        if !file.is_empty() {
            let relative = PathBuf::from(case.name).join(file);
            expected_paths.insert(relative.clone());
            let actual =
                fs::read(output.join(&relative)).map_err(|error| JournalError::Io(error.kind()))?;
            if actual != expected {
                return Err(JournalError::CorruptJournal);
            }
        }
        if file == JOURNAL_FILE {
            let recovered = recover_bytes(&expected)?;
            let (sequence, identity_present, checkpoint_present) = case.oracle.expected();
            if recovered.sequence != sequence
                || recovered.identities.contains_key(&frames.message_id) != identity_present
                || recovered.checkpoint.is_some() != checkpoint_present
                || recovered
                    .identities
                    .get(&frames.message_id)
                    .is_some_and(IdentityRecord::is_checkpointed)
                    != checkpoint_present
            {
                return Err(JournalError::CorruptJournal);
            }
        } else if !matches!(case.oracle, CorpusOracle::Old | CorpusOracle::UnknownOld) {
            return Err(JournalError::CorruptJournal);
        }
        case_bytes.insert(case.name, expected);
    }
    if corpus_file_inventory(output)? != expected_paths {
        return Err(JournalError::CorruptJournal);
    }
    let actual_manifest =
        fs::read(output.join("manifest.json")).map_err(|error| JournalError::Io(error.kind()))?;
    if actual_manifest != corpus_manifest(&case_bytes, inputs)?.as_bytes() {
        return Err(JournalError::CorruptJournal);
    }
    Ok(())
}

#[cfg(test)]
fn corpus_frames(inputs: &JournalCorpusInputs<'_>) -> Result<CorpusFrames, JournalError> {
    let source_digest = corpus_source_input_digest(inputs);
    let requirements_digest = digest(inputs.requirements);
    let design_digest = digest(inputs.design);
    let mut target = [0_u8; 16];
    target.copy_from_slice(&design_digest[..16]);
    target[6] = (target[6] & 0x0f) | 0x40;
    target[8] = (target[8] & 0x3f) | 0x80;
    let mut message_id = [0_u8; 16];
    message_id.copy_from_slice(&source_digest[..16]);
    message_id[6] = (message_id[6] & 0x0f) | 0x40;
    message_id[8] = (message_id[8] & 0x3f) | 0x80;
    let mut resource_id = [0_u8; 16];
    resource_id.copy_from_slice(&requirements_digest[..16]);
    let namespace = JournalNamespace::new(
        SubscriptionId::from_bytes(
            requirements_digest[..16]
                .try_into()
                .expect("digest prefix is sixteen bytes"),
        ),
        NodeId::from(target),
        7,
        3,
        9,
        4,
        ResourceEpoch::new(ResourceId::from_bytes(resource_id), 5),
    );
    let identity = IdentityRecord::fixture(message_id, design_digest, resource_id, 5, 3, 4, 1)?;
    let checkpoint = CheckpointRecord {
        sequence: 2,
        subscription_id: namespace.subscription_id,
        target: namespace.target,
        generation: namespace.generation,
        partition: namespace.partition,
        offset: 4,
        message_id,
        envelope_digest: design_digest,
        delivery_attempt: 1,
        owner_epoch: 1,
        lifecycle_generation: namespace.lifecycle_generation,
        checkpoint_attempt: 1,
    };
    Ok(CorpusFrames {
        header: encode_state_frame(StateRecordKind::JournalHeader, &namespace.encode_body())?,
        identity: encode_identity_frame(1, &identity)?,
        checkpoint: encode_state_frame(
            StateRecordKind::CheckpointCommit,
            &checkpoint.encode_body(),
        )?,
        message_id,
    })
}

#[cfg(test)]
fn corpus_image_file(frames: &CorpusFrames, image: CorpusImage) -> (&'static str, Vec<u8>) {
    match image {
        CorpusImage::NoCanonical => return ("", Vec::new()),
        CorpusImage::PartialPendingHeader => {
            return (
                JOURNAL_PENDING_FILE,
                frames.header[..frames.header.len() / 2].to_vec(),
            );
        }
        CorpusImage::FullPendingHeader => {
            return (JOURNAL_PENDING_FILE, frames.header.clone());
        }
        _ => {}
    }
    let mut bytes = frames.header.clone();
    match image {
        CorpusImage::NoCanonical
        | CorpusImage::PartialPendingHeader
        | CorpusImage::FullPendingHeader => unreachable!("handled above"),
        CorpusImage::Header => {}
        CorpusImage::PartialIdentity => {
            bytes.extend_from_slice(&frames.identity[..frames.identity.len() / 2]);
        }
        CorpusImage::Identity => bytes.extend_from_slice(&frames.identity),
        CorpusImage::PartialCheckpoint => {
            bytes.extend_from_slice(&frames.identity);
            bytes.extend_from_slice(&frames.checkpoint[..frames.checkpoint.len() / 2]);
        }
        CorpusImage::Checkpoint => {
            bytes.extend_from_slice(&frames.identity);
            bytes.extend_from_slice(&frames.checkpoint);
        }
    }
    (JOURNAL_FILE, bytes)
}

#[cfg(test)]
fn corpus_manifest(
    case_bytes: &BTreeMap<&'static str, Vec<u8>>,
    inputs: &JournalCorpusInputs<'_>,
) -> Result<String, JournalError> {
    let requirements_digest = digest(inputs.requirements);
    let design_digest = digest(inputs.design);
    let mut sources = inputs.sources.to_vec();
    sources.sort_by_key(|(path, _)| *path);
    let mut output = String::from("{\n");
    output.push_str("  \"schema_version\": 1,\n");
    output.push_str("  \"producer_task\": \"4.2\",\n");
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
        hex(&corpus_source_input_digest(inputs))
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
        let bytes = case_bytes
            .get(case.name)
            .ok_or(JournalError::CorruptJournal)?;
        let (file, _) = corpus_image_file(&corpus_frames(inputs)?, case.image);
        output.push_str(&format!(
            "    {{\"name\":\"{}\",\"stage\":\"{}\",\"oracle\":\"{}\",\"file\":\"{}\",\"sha256\":\"{}\"}}{}\n",
            case.name,
            case.stage,
            case.oracle.name(),
            file,
            if file.is_empty() { String::new() } else { hex(&digest(bytes)) },
            if index + 1 == CORPUS_CASES.len() { "" } else { "," }
        ));
    }
    output.push_str("  ]\n}\n");
    Ok(output)
}

#[cfg(test)]
fn corpus_source_input_digest(inputs: &JournalCorpusInputs<'_>) -> [u8; 32] {
    let mut sources = inputs.sources.to_vec();
    sources.sort_by_key(|(path, _)| *path);
    let mut projection = Vec::new();
    for (path, bytes) in sources {
        projection.extend_from_slice(&(path.len() as u32).to_be_bytes());
        projection.extend_from_slice(path.as_bytes());
        projection.extend_from_slice(&digest(bytes));
    }
    digest_parts(&[CORPUS_SOURCE_DOMAIN, &projection])
}

#[cfg(test)]
fn corpus_file_inventory(root: &Path) -> Result<BTreeSet<PathBuf>, JournalError> {
    fn visit(
        root: &Path,
        current: &Path,
        output: &mut BTreeSet<PathBuf>,
    ) -> Result<(), JournalError> {
        for entry in fs::read_dir(current).map_err(|error| JournalError::Io(error.kind()))? {
            let entry = entry.map_err(|error| JournalError::Io(error.kind()))?;
            let path = entry.path();
            if path.is_dir() {
                visit(root, &path, output)?;
            } else {
                output.insert(
                    path.strip_prefix(root)
                        .map_err(|_| JournalError::CorruptJournal)?
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
fn write_corpus_file(path: &Path, bytes: &[u8]) -> Result<(), JournalError> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|error| JournalError::Io(error.kind()))?;
    file.write_all(bytes)
        .map_err(|error| JournalError::Io(error.kind()))?;
    file.sync_all()
        .map_err(|error| JournalError::Io(error.kind()))?;
    Ok(())
}

#[cfg(test)]
fn snapshot_files(root: &Path) -> Result<BTreeMap<PathBuf, Vec<u8>>, JournalError> {
    let mut snapshot = BTreeMap::new();
    for relative in corpus_file_inventory(root)? {
        snapshot.insert(
            relative.clone(),
            fs::read(root.join(relative)).map_err(|error| JournalError::Io(error.kind()))?,
        );
    }
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use super::{
        AppendFault, CORPUS_CASES, IdentityPersistOutcome, JOURNAL_FILE, JournalCorpusInputs,
        JournalError, JournalInitialization, JournalNamespace, JournalStore,
        generate_journal_corpus, recover_bytes, snapshot_files, verify_journal_corpus,
    };
    use crate::state::InstallFault;
    use crate::state::identity::{ClockProvenance, IdentityCandidate};
    use alopex_chirps_core::durable::{
        CheckedPollRecord, CheckpointOperationPhase, CheckpointOutcome, Delivery, DeliveryContext,
        EnvelopeDigest, ResourceEpoch, ResourceId, SubscriptionBinding, SubscriptionId,
    };
    use alopex_chirps_wire::node_id::NodeId;
    use std::fs;
    use std::io::Write;
    use tempfile::tempdir;

    fn namespace() -> JournalNamespace {
        let mut target = [0x21; 16];
        target[6] = 0x41;
        target[8] = 0x81;
        let mut resource = [0x31; 16];
        resource[6] = 0x41;
        resource[8] = 0x81;
        JournalNamespace::new(
            SubscriptionId::from_bytes([0x11; 16]),
            NodeId::from(target),
            7,
            3,
            9,
            4,
            ResourceEpoch::new(ResourceId::from_bytes(resource), 5),
        )
    }

    fn message_id() -> [u8; 16] {
        let mut id = [0x41; 16];
        id[6] = 0x41;
        id[8] = 0x81;
        id
    }

    fn checked_record(offset: u64, digest: [u8; 32]) -> CheckedPollRecord {
        CheckedPollRecord::try_new(
            offset,
            message_id(),
            EnvelopeDigest::from_bytes(digest),
            b"canonical".to_vec(),
        )
        .unwrap()
    }

    fn identity(attempt: u64, digest: [u8; 32]) -> IdentityCandidate {
        let record = checked_record(4, digest);
        IdentityCandidate::new(
            record.message_id(),
            record.envelope_digest(),
            namespace().resource_epoch,
            3,
            4,
            attempt,
            1_000,
            ClockProvenance::Trusted,
        )
        .unwrap()
    }

    fn checkpoint_binding(
        owner_epoch: u64,
        digest: [u8; 32],
        delivery_attempt: u64,
    ) -> alopex_chirps_core::durable::CheckpointInstallBinding {
        let subscription = SubscriptionBinding::new(
            namespace().subscription_id,
            namespace().target,
            namespace().generation,
            namespace().partition,
            owner_epoch,
            namespace().lifecycle_generation,
        );
        let mut delivery = Delivery::from_checked_record(
            DeliveryContext::new(subscription, delivery_attempt),
            checked_record(4, digest),
        );
        delivery.handle_mut().begin_ack().unwrap()
    }

    fn ready(directory: &std::path::Path) -> JournalStore {
        match JournalStore::initialize(directory, namespace(), 1).unwrap() {
            JournalInitialization::Ready(store) => *store,
            _ => panic!("expected ready journal"),
        }
    }

    #[test]
    fn v07_task_4_2_initial_journal_requires_file_and_directory_durability() {
        let root = tempdir().unwrap();
        for fault in [
            InstallFault::BeforeWrite,
            InstallFault::AfterWrite,
            InstallFault::AfterFileSync,
        ] {
            let old = root.path().join(format!("old-{fault:?}"));
            fs::create_dir(&old).unwrap();
            assert!(matches!(
                JournalStore::initialize_with_fault(&old, namespace(), 1, fault).unwrap(),
                JournalInitialization::NotCommitted
            ));
            assert!(!old.join(JOURNAL_FILE).exists());
        }

        for fault in [InstallFault::AfterRename, InstallFault::AfterDirectorySync] {
            let unknown = root.path().join(format!("unknown-{fault:?}"));
            fs::create_dir(&unknown).unwrap();
            assert!(matches!(
                JournalStore::initialize_with_fault(&unknown, namespace(), 1, fault).unwrap(),
                JournalInitialization::Unknown
            ));
            assert_eq!(
                JournalStore::open(&unknown, namespace(), 2)
                    .unwrap()
                    .sequence(),
                0
            );
        }

        let committed = root.path().join("committed");
        fs::create_dir(&committed).unwrap();
        assert!(matches!(
            JournalStore::initialize(&committed, namespace(), 1).unwrap(),
            JournalInitialization::Ready(_)
        ));
    }

    #[test]
    fn v07_task_4_2_identity_is_synced_before_delivery_and_conflicts_fail_stop() {
        let root = tempdir().unwrap();
        let directory = root.path().join("identity");
        fs::create_dir(&directory).unwrap();
        let mut store = ready(&directory);
        assert_eq!(
            store
                .persist_identity_with_fault(identity(1, [0x51; 32]), AppendFault::BeforeWrite)
                .unwrap(),
            IdentityPersistOutcome::NotCommitted
        );
        assert!(store.identity(message_id()).is_none());
        let first = store.persist_identity(identity(1, [0x51; 32])).unwrap();
        assert!(matches!(first, IdentityPersistOutcome::Committed(_)));
        assert_eq!(store.identity(message_id()).unwrap().observation_count(), 1);
        let second = store.persist_identity(identity(2, [0x51; 32])).unwrap();
        assert!(matches!(second, IdentityPersistOutcome::Committed(_)));
        assert_eq!(store.identity(message_id()).unwrap().observation_count(), 2);
        let mut other_id = [0x42; 16];
        other_id[6] = 0x42;
        other_id[8] = 0x82;
        let other = CheckedPollRecord::try_new(
            4,
            other_id,
            EnvelopeDigest::from_bytes([0x53; 32]),
            b"other".to_vec(),
        )
        .unwrap();
        let other_candidate = IdentityCandidate::new(
            other.message_id(),
            other.envelope_digest(),
            namespace().resource_epoch,
            3,
            4,
            1,
            1_000,
            ClockProvenance::Trusted,
        )
        .unwrap();
        assert_eq!(
            store.persist_identity(other_candidate),
            Err(JournalError::IdentityOffsetConflict)
        );
        assert!(matches!(
            store.persist_identity(identity(3, [0x52; 32])),
            Err(JournalError::Identity(_))
        ));
    }

    #[test]
    fn v07_task_4_2_checkpoint_and_identity_update_share_one_synced_frame() {
        let root = tempdir().unwrap();
        let directory = root.path().join("checkpoint");
        fs::create_dir(&directory).unwrap();
        let mut store = ready(&directory);
        store.persist_identity(identity(1, [0x61; 32])).unwrap();
        let binding = checkpoint_binding(1, [0x61; 32], 1);
        assert_eq!(
            store
                .install_checkpoint_with_fault(binding, AppendFault::BeforeWrite)
                .unwrap(),
            CheckpointOutcome::CheckpointNotCommitted
        );
        assert!(store.checkpoint().is_none());
        assert!(!store.identity(message_id()).unwrap().is_checkpointed());
        assert_eq!(
            store.install_checkpoint(binding).unwrap(),
            CheckpointOutcome::CheckpointCommitted
        );
        assert_eq!(store.checkpoint().unwrap().offset(), 4);
        assert!(store.identity(message_id()).unwrap().is_checkpointed());
        assert_eq!(store.expected_offset().unwrap(), 5);
    }

    #[test]
    fn v07_task_6_5_checkpoint_freeze_before_install_keeps_the_old_journal() {
        let root = tempdir().unwrap();
        let directory = root.path().join("checkpoint-freeze");
        fs::create_dir(&directory).unwrap();
        let mut store = ready(&directory);
        store.persist_identity(identity(1, [0x62; 32])).unwrap();
        let binding = checkpoint_binding(1, [0x62; 32], 1);
        let before = fs::metadata(directory.join(JOURNAL_FILE)).unwrap().len();
        let mut phases = Vec::new();

        let outcome = store
            .install_checkpoint_with_phase(binding, |phase| {
                phases.push(phase);
                phase != CheckpointOperationPhase::InstallUnknown
            })
            .unwrap();

        assert_eq!(outcome, CheckpointOutcome::CheckpointNotCommitted);
        assert_eq!(phases, [CheckpointOperationPhase::InstallUnknown]);
        assert_eq!(
            fs::metadata(directory.join(JOURNAL_FILE)).unwrap().len(),
            before
        );
        assert!(store.checkpoint().is_none());
        assert!(!store.identity(message_id()).unwrap().is_checkpointed());
    }

    #[test]
    fn v07_task_4_2_unknown_is_terminal_until_recovery_which_chooses_exact_new() {
        let root = tempdir().unwrap();
        for fault in [AppendFault::AfterWrite, AppendFault::AfterFileSync] {
            let directory = root.path().join(format!("checkpoint-{fault:?}"));
            fs::create_dir(&directory).unwrap();
            let mut store = ready(&directory);
            store.persist_identity(identity(1, [0x71; 32])).unwrap();
            let binding = checkpoint_binding(1, [0x71; 32], 1);
            assert_eq!(
                store.install_checkpoint_with_fault(binding, fault).unwrap(),
                CheckpointOutcome::CheckpointUnknown
            );
            assert_eq!(
                store.install_checkpoint(binding),
                Err(JournalError::RecoveryRequired)
            );
            let recovered = JournalStore::open(&directory, namespace(), 2).unwrap();
            assert_eq!(recovered.checkpoint().unwrap().offset(), 4);
            assert!(recovered.identity(message_id()).unwrap().is_checkpointed());
        }
    }

    #[test]
    fn v07_task_4_2_identity_append_unknown_requires_recovery_before_delivery() {
        let root = tempdir().unwrap();
        for fault in [AppendFault::AfterWrite, AppendFault::AfterFileSync] {
            let directory = root.path().join(format!("identity-{fault:?}"));
            fs::create_dir(&directory).unwrap();
            let mut store = ready(&directory);
            assert_eq!(
                store
                    .persist_identity_with_fault(identity(1, [0x72; 32]), fault)
                    .unwrap(),
                IdentityPersistOutcome::Unknown
            );
            assert_eq!(
                store.persist_identity(identity(2, [0x72; 32])),
                Err(JournalError::RecoveryRequired)
            );
            let recovered = JournalStore::open(&directory, namespace(), 2).unwrap();
            assert_eq!(recovered.sequence(), 1);
            assert_eq!(
                recovered
                    .identity(message_id())
                    .unwrap()
                    .observation_count(),
                1
            );
            assert!(!recovered.identity(message_id()).unwrap().is_checkpointed());
        }
    }

    #[test]
    fn v07_task_4_2_stale_owner_digest_and_attempt_cannot_advance_checkpoint() {
        let root = tempdir().unwrap();
        let directory = root.path().join("fence");
        fs::create_dir(&directory).unwrap();
        let mut store = ready(&directory);
        store.persist_identity(identity(1, [0x31; 32])).unwrap();
        for binding in [
            checkpoint_binding(2, [0x31; 32], 1),
            checkpoint_binding(1, [0x32; 32], 1),
        ] {
            assert_eq!(
                store.install_checkpoint(binding),
                Err(JournalError::CheckpointBindingMismatch)
            );
        }
        store.persist_identity(identity(2, [0x31; 32])).unwrap();
        assert_eq!(
            store.install_checkpoint(checkpoint_binding(1, [0x31; 32], 1)),
            Err(JournalError::CheckpointBindingMismatch)
        );
        assert_eq!(
            store
                .install_checkpoint(checkpoint_binding(1, [0x31; 32], 2))
                .unwrap(),
            CheckpointOutcome::CheckpointCommitted
        );
        assert_eq!(store.checkpoint().unwrap().offset(), 4);
    }

    #[test]
    fn v07_task_4_2_partial_tail_uses_old_prefix_but_full_corruption_fails() {
        let root = tempdir().unwrap();
        let partial_directory = root.path().join("partial");
        fs::create_dir(&partial_directory).unwrap();
        let mut store = ready(&partial_directory);
        store.persist_identity(identity(1, [0x21; 32])).unwrap();
        let path = partial_directory.join(JOURNAL_FILE);
        let before = fs::read(&path).unwrap();
        let extra = crate::state::encode_state_frame(
            crate::state::StateRecordKind::IdentityMutation,
            b"torn",
        )
        .unwrap();
        let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&extra[..extra.len() / 2]).unwrap();
        drop(file);
        let reopened = JournalStore::open(&partial_directory, namespace(), 2).unwrap();
        assert_eq!(reopened.sequence(), 1);
        assert_eq!(fs::read(&path).unwrap(), before);

        let corrupt_directory = root.path().join("corrupt");
        fs::create_dir(&corrupt_directory).unwrap();
        let mut corrupt_store = ready(&corrupt_directory);
        corrupt_store
            .persist_identity(identity(1, [0x22; 32]))
            .unwrap();
        let corrupt_path = corrupt_directory.join(JOURNAL_FILE);
        let mut corrupt = fs::read(&corrupt_path).unwrap();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 1;
        fs::write(&corrupt_path, corrupt).unwrap();
        assert!(matches!(
            JournalStore::open(&corrupt_directory, namespace(), 2),
            Err(JournalError::Frame(_))
        ));
    }

    #[test]
    fn v07_task_4_2_duplicate_or_gap_sequence_and_invalid_prefix_fail_stop() {
        let root = tempdir().unwrap();
        let directory = root.path().join("sequence");
        fs::create_dir(&directory).unwrap();
        let mut store = ready(&directory);
        store.persist_identity(identity(1, [0x41; 32])).unwrap();
        let bytes = fs::read(directory.join(JOURNAL_FILE)).unwrap();
        let header_len = crate::state::state_frame_encoded_len(&bytes)
            .unwrap()
            .unwrap();
        let identity_frame = bytes[header_len..].to_vec();
        let mut duplicate = bytes.clone();
        duplicate.extend_from_slice(&identity_frame);
        assert_eq!(
            recover_bytes(&duplicate).unwrap_err(),
            JournalError::NonContiguousSequence
        );

        let mut gap_body = crate::state::decode_state_frame(
            &identity_frame,
            crate::state::StateRecordKind::IdentityMutation,
        )
        .unwrap()
        .to_vec();
        gap_body[..8].copy_from_slice(&3_u64.to_be_bytes());
        let gap_frame = crate::state::encode_state_frame(
            crate::state::StateRecordKind::IdentityMutation,
            &gap_body,
        )
        .unwrap();
        let mut gap = bytes.clone();
        gap.truncate(header_len);
        gap.extend_from_slice(&gap_frame);
        assert_eq!(
            recover_bytes(&gap).unwrap_err(),
            JournalError::NonContiguousSequence
        );

        let mut unknown_version = bytes.clone();
        unknown_version[header_len + 9] ^= 1;
        assert!(matches!(
            recover_bytes(&unknown_version),
            Err(JournalError::Frame(
                crate::state::StateFrameError::UnsupportedVersion
            ))
        ));

        let mut marker_body = crate::state::decode_state_frame(
            &identity_frame,
            crate::state::StateRecordKind::IdentityMutation,
        )
        .unwrap()
        .to_vec();
        let marker_end = marker_body.len() - 1;
        marker_body[marker_end] ^= 1;
        let marker_frame = crate::state::encode_state_frame(
            crate::state::StateRecordKind::IdentityMutation,
            &marker_body,
        )
        .unwrap();
        let mut invalid_marker = bytes.clone();
        invalid_marker.truncate(header_len);
        invalid_marker.extend_from_slice(&marker_frame);
        assert_eq!(
            recover_bytes(&invalid_marker).unwrap_err(),
            JournalError::CorruptJournal
        );

        let mut invalid = bytes;
        invalid[0] ^= 1;
        assert!(recover_bytes(&invalid).is_err());
    }

    #[test]
    fn v07_task_4_2_provisional_corpus_is_complete_replayable_and_repeatable() {
        // Generator unit inputs are deliberately synthetic. Release evidence
        // continues to require the authenticated original specification bytes.
        let requirements: &[u8] = b"Synthetic unit-test requirements; not release evidence.\n";
        let design: &[u8] = b"Synthetic unit-test design; not release evidence.\n";
        let sources: &[(&str, &[u8])] = &[
            ("state/identity.rs", include_bytes!("identity.rs")),
            ("state/journal.rs", include_bytes!("journal.rs")),
            ("state/mod.rs", include_bytes!("mod.rs")),
        ];
        let inputs = JournalCorpusInputs {
            requirements,
            design,
            sources,
        };
        let root = tempfile::tempdir().unwrap();
        let output = root.path().join("unit-journal-corpus");
        generate_journal_corpus(&output, &inputs).unwrap();
        verify_journal_corpus(&output, &inputs).unwrap();
        let first = snapshot_files(&output).unwrap();
        generate_journal_corpus(&output, &inputs).unwrap();
        verify_journal_corpus(&output, &inputs).unwrap();
        assert_eq!(snapshot_files(&output).unwrap(), first);
        assert_eq!(CORPUS_CASES.len(), 20);
        let manifest = fs::read_to_string(output.join("manifest.json")).unwrap();
        assert!(manifest.contains("\"producer_task\": \"4.2\""));
        assert!(
            manifest.contains("85c0d95e5e7ca2f85a4092b51ac17c0bdfabef272fc181e66e6a4e5bc8d6e524")
        );
        assert!(
            manifest.contains("4da71815b5658adacaa3e6ed607c7d22dd32cd1781494d3361bb55fd9e9016f4")
        );
        assert!(manifest.contains("unknown-old"));
        assert!(manifest.contains("unknown-new"));
        assert!(manifest.contains("initial-write-old"));
        assert!(manifest.contains(".checkpoint.journal.pending"));
        assert!(output.join("initial-directory-sync-unknown-old").is_dir());
        for (requirements, design) in [
            (b"changed unit requirements".as_slice(), design),
            (requirements, b"changed unit design".as_slice()),
        ] {
            let altered = JournalCorpusInputs {
                requirements,
                design,
                sources,
            };
            assert_eq!(
                verify_journal_corpus(&output, &altered).unwrap_err(),
                JournalError::CorruptJournal
            );
        }
        assert_eq!(
            fs::read_dir(output.join("initial-directory-sync-unknown-old"))
                .unwrap()
                .count(),
            0
        );
    }
}
