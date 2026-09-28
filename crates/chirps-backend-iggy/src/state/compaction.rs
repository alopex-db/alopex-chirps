//! Generation-barrier compaction and three-horizon identity collection.

use super::capacity::{CapacityController, CapacityError, CapacityToken, DurableGcProof};
use super::identity::{ClockProvenance, IdentityError, IdentityRecord};
use super::{
    InstallError, InstallFault, StateFrameError, StateRecordKind, decode_state_frame, digest,
    durably_install, encode_state_frame, set_owner_only_permissions, state_frame_encoded_len,
    sync_directory,
};
#[cfg(test)]
use super::{digest_parts, hex};
use std::collections::BTreeMap;
#[cfg(test)]
use std::collections::BTreeSet;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;

const ROOT_FILE: &str = "compaction.root";
#[cfg(test)]
const ROOT_PENDING_FILE: &str = ".compaction.root.pending";
const BASE_FILE: &str = "base.state";
const SUFFIX_FILE: &str = "suffix.journal";
const FORMAT_VERSION: u16 = 1;
const COMMIT_MARKER: &[u8; 8] = b"CMPCT007";
const MAX_COMPACTION_FILE_LEN: u64 = 16 * 1024 * 1024;
const FRAME_COMMITMENT_DOMAIN: &[u8] = b"chirps-v0.7-compaction-frame-commitment\0";
#[cfg(test)]
const CORPUS_SOURCE_DOMAIN: &[u8] = b"chirps-v0.7-task-4.3-source-input\0";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClockObservation {
    Trusted,
    RollbackDetected,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IdentityGcDecision {
    Eligible,
    Retain,
    RetainAndStopPoll,
}

/// Immutable evidence required before one processed identity can disappear.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IdentityHorizon {
    checkpointed: bool,
    original_offset: u64,
    retry_not_before_unix_ms: u64,
    durable_clock_trusted: bool,
}

impl IdentityHorizon {
    pub(crate) const fn new(
        checkpointed: bool,
        original_offset: u64,
        retry_not_before_unix_ms: u64,
    ) -> Self {
        Self {
            checkpointed,
            original_offset,
            retry_not_before_unix_ms,
            durable_clock_trusted: true,
        }
    }

    pub(crate) const fn evaluate(
        self,
        broker_oldest: u64,
        now_unix_ms: u64,
        clock: ClockObservation,
    ) -> IdentityGcDecision {
        if !self.durable_clock_trusted || !matches!(clock, ClockObservation::Trusted) {
            return IdentityGcDecision::RetainAndStopPoll;
        }
        if self.checkpointed
            && self.original_offset < broker_oldest
            && now_unix_ms >= self.retry_not_before_unix_ms
        {
            IdentityGcDecision::Eligible
        } else {
            IdentityGcDecision::Retain
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompactionIdentity {
    message_id: [u8; 16],
    encoded: Vec<u8>,
    horizon: IdentityHorizon,
}

impl CompactionIdentity {
    pub(crate) fn new(message_id: [u8; 16], encoded: Vec<u8>) -> Result<Self, CompactionError> {
        let record = IdentityRecord::decode_body(&encoded)?;
        if record.message_id_bytes() != message_id {
            return Err(CompactionError::GcPlanMismatch);
        }
        let retry_not_before_unix_ms = u64::from_be_bytes(
            encoded[109..117]
                .try_into()
                .map_err(|_| CompactionError::InvalidBase)?,
        );
        let horizon = IdentityHorizon {
            checkpointed: record.is_checkpointed(),
            original_offset: record.original_offset(),
            retry_not_before_unix_ms,
            durable_clock_trusted: encoded[117] == ClockProvenance::Trusted as u8,
        };
        Ok(Self {
            message_id,
            encoded,
            horizon,
        })
    }
}

fn validate_identity_records(
    identities: &BTreeMap<[u8; 16], Vec<u8>>,
) -> Result<(), CompactionError> {
    for (message_id, encoded) in identities {
        CompactionIdentity::new(*message_id, encoded.clone())?;
    }
    Ok(())
}

pub(crate) fn collect_identity_horizon(
    identities: impl IntoIterator<Item = CompactionIdentity>,
    broker_oldest: u64,
    now_unix_ms: u64,
    clock: ClockObservation,
) -> (BTreeMap<[u8; 16], Vec<u8>>, bool) {
    let mut retained = BTreeMap::new();
    let mut stop_poll = false;
    for identity in identities {
        match identity.horizon.evaluate(broker_oldest, now_unix_ms, clock) {
            IdentityGcDecision::Eligible => {}
            IdentityGcDecision::Retain => {
                retained.insert(identity.message_id, identity.encoded);
            }
            IdentityGcDecision::RetainAndStopPoll => {
                stop_poll = true;
                retained.insert(identity.message_id, identity.encoded);
            }
        }
    }
    (retained, stop_poll)
}

/// Store-derived candidate for one generation cutover. Its private fields prevent
/// callers from supplying an unchecked materialized base or an unbound GC proof.
#[derive(Debug, Clone)]
pub(crate) struct CompactionPlan {
    source_generation: u64,
    source_base_digest: [u8; 32],
    planned_base_digest: [u8; 32],
    base: CompactionBase,
    collected_identities: BTreeMap<[u8; 16], CapacityToken>,
}

/// Fixed-size commitment to every sequenced frame represented by a base.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FrameCommitment {
    applied_through: u64,
    digest: [u8; 32],
}

impl FrameCommitment {
    fn genesis() -> Self {
        Self {
            applied_through: 0,
            digest: digest(FRAME_COMMITMENT_DOMAIN),
        }
    }

    fn from_frames(frames: &BTreeMap<u64, Vec<u8>>) -> Result<Self, CompactionError> {
        let mut commitment = Self::genesis();
        for (sequence, bytes) in frames {
            commitment = commitment.append(*sequence, bytes)?;
        }
        Ok(commitment)
    }

    fn append(self, sequence: u64, bytes: &[u8]) -> Result<Self, CompactionError> {
        if self.applied_through.checked_add(1) != Some(sequence) {
            return Err(CompactionError::SequenceConflict);
        }
        Ok(Self {
            applied_through: sequence,
            digest: super::digest_parts(&[
                FRAME_COMMITMENT_DOMAIN,
                &self.digest,
                &sequence.to_be_bytes(),
                &digest(bytes),
            ]),
        })
    }

    pub(crate) const fn applied_through(self) -> u64 {
        self.applied_through
    }

    pub(crate) const fn digest(self) -> [u8; 32] {
        self.digest
    }
}

/// Materialized state protected by a compaction generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompactionBase {
    generation: u64,
    applied_through: u64,
    immutable_creation: Vec<u8>,
    checkpoint: Option<Vec<u8>>,
    identities: BTreeMap<[u8; 16], Vec<u8>>,
    frame_commitment: FrameCommitment,
}

impl CompactionBase {
    pub(crate) fn new(
        generation: u64,
        applied_through: u64,
        immutable_creation: Vec<u8>,
        checkpoint: Option<Vec<u8>>,
        identities: BTreeMap<[u8; 16], Vec<u8>>,
        committed_frames: &BTreeMap<u64, Vec<u8>>,
    ) -> Result<Self, CompactionError> {
        if generation == 0 || immutable_creation.is_empty() {
            return Err(CompactionError::InvalidBase);
        }
        let frame_commitment = FrameCommitment::from_frames(committed_frames)?;
        Self::with_commitment(
            generation,
            applied_through,
            immutable_creation,
            checkpoint,
            identities,
            frame_commitment,
        )
    }

    pub(crate) fn with_commitment(
        generation: u64,
        applied_through: u64,
        immutable_creation: Vec<u8>,
        checkpoint: Option<Vec<u8>>,
        identities: BTreeMap<[u8; 16], Vec<u8>>,
        frame_commitment: FrameCommitment,
    ) -> Result<Self, CompactionError> {
        if generation == 0
            || immutable_creation.is_empty()
            || frame_commitment.applied_through != applied_through
        {
            return Err(CompactionError::InvalidBase);
        }
        validate_identity_records(&identities)?;
        Ok(Self {
            generation,
            applied_through,
            immutable_creation,
            checkpoint,
            identities,
            frame_commitment,
        })
    }

    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) const fn applied_through(&self) -> u64 {
        self.applied_through
    }

    pub(crate) const fn frame_commitment(&self) -> FrameCommitment {
        self.frame_commitment
    }

    fn encode(&self) -> Result<Vec<u8>, CompactionError> {
        let mut body = Vec::new();
        put_u16(&mut body, FORMAT_VERSION);
        put_u64(&mut body, self.generation);
        put_u64(&mut body, self.applied_through);
        put_bytes(&mut body, &self.immutable_creation)?;
        match &self.checkpoint {
            Some(checkpoint) => {
                body.push(1);
                put_bytes(&mut body, checkpoint)?;
            }
            None => body.push(0),
        }
        put_u32(
            &mut body,
            u32::try_from(self.identities.len()).map_err(|_| CompactionError::InvalidBase)?,
        );
        for (message_id, encoded) in &self.identities {
            body.extend_from_slice(message_id);
            put_bytes(&mut body, encoded)?;
        }
        put_u64(&mut body, self.frame_commitment.applied_through);
        body.extend_from_slice(&self.frame_commitment.digest);
        body.extend_from_slice(COMMIT_MARKER);
        encode_state_frame(StateRecordKind::CompactionBase, &body).map_err(Into::into)
    }

    fn decode(bytes: &[u8]) -> Result<Self, CompactionError> {
        let body = decode_state_frame(bytes, StateRecordKind::CompactionBase)?;
        let mut reader = Reader::new(body);
        if reader.u16()? != FORMAT_VERSION {
            return Err(CompactionError::UnsupportedVersion);
        }
        let generation = reader.u64()?;
        let applied_through = reader.u64()?;
        let immutable_creation = reader.bytes()?;
        let checkpoint = match reader.u8()? {
            0 => None,
            1 => Some(reader.bytes()?),
            _ => return Err(CompactionError::InvalidBase),
        };
        let identity_count = reader.u32()? as usize;
        let mut identities = BTreeMap::new();
        for _ in 0..identity_count {
            let message_id = reader.array::<16>()?;
            if identities.insert(message_id, reader.bytes()?).is_some() {
                return Err(CompactionError::InvalidBase);
            }
        }
        let frame_commitment = FrameCommitment {
            applied_through: reader.u64()?,
            digest: reader.array::<32>()?,
        };
        reader.marker()?;
        reader.finish()?;
        if generation == 0 || immutable_creation.is_empty() {
            return Err(CompactionError::InvalidBase);
        }
        Self::with_commitment(
            generation,
            applied_through,
            immutable_creation,
            checkpoint,
            identities,
            frame_commitment,
        )
    }
}

/// Canonical mutation language consumed by the compaction reducer. Suffixes
/// cannot contain opaque bytes that a later generation would only hash and lose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CompactionMutation {
    InstallCheckpoint(Vec<u8>),
    UpsertIdentity {
        message_id: [u8; 16],
        encoded: Vec<u8>,
    },
}

impl CompactionMutation {
    const CHECKPOINT: u8 = 1;
    const IDENTITY: u8 = 2;

    fn encode(&self, sequence: u64) -> Result<Vec<u8>, CompactionError> {
        let mut body = Vec::new();
        put_u16(&mut body, FORMAT_VERSION);
        put_u64(&mut body, sequence);
        match self {
            Self::InstallCheckpoint(checkpoint) => {
                if checkpoint.is_empty() {
                    return Err(CompactionError::InvalidMutation);
                }
                body.push(Self::CHECKPOINT);
                put_bytes(&mut body, checkpoint)?;
            }
            Self::UpsertIdentity {
                message_id,
                encoded,
            } => {
                if encoded.is_empty() {
                    return Err(CompactionError::InvalidMutation);
                }
                CompactionIdentity::new(*message_id, encoded.clone())?;
                body.push(Self::IDENTITY);
                body.extend_from_slice(message_id);
                put_bytes(&mut body, encoded)?;
            }
        }
        body.extend_from_slice(COMMIT_MARKER);
        encode_state_frame(StateRecordKind::CompactionMutation, &body).map_err(Into::into)
    }

    fn decode(expected_sequence: u64, bytes: &[u8]) -> Result<Self, CompactionError> {
        let body = decode_state_frame(bytes, StateRecordKind::CompactionMutation)?;
        let mut reader = Reader::new(body);
        if reader.u16()? != FORMAT_VERSION || reader.u64()? != expected_sequence {
            return Err(CompactionError::InvalidMutation);
        }
        let mutation = match reader.u8()? {
            Self::CHECKPOINT => Self::InstallCheckpoint(reader.bytes()?),
            Self::IDENTITY => Self::UpsertIdentity {
                message_id: reader.array::<16>()?,
                encoded: reader.bytes()?,
            },
            _ => return Err(CompactionError::InvalidMutation),
        };
        reader.marker()?;
        reader.finish()?;
        match &mutation {
            Self::InstallCheckpoint(bytes) | Self::UpsertIdentity { encoded: bytes, .. }
                if bytes.is_empty() =>
            {
                Err(CompactionError::InvalidMutation)
            }
            Self::UpsertIdentity {
                message_id,
                encoded,
            } => {
                CompactionIdentity::new(*message_id, encoded.clone())?;
                Ok(mutation)
            }
            _ => Ok(mutation),
        }
    }

    fn apply(self, checkpoint: &mut Option<Vec<u8>>, identities: &mut BTreeMap<[u8; 16], Vec<u8>>) {
        match self {
            Self::InstallCheckpoint(next) => *checkpoint = Some(next),
            Self::UpsertIdentity {
                message_id,
                encoded,
            } => {
                identities.insert(message_id, encoded);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MutationRoute {
    CurrentGeneration,
    WaitingForCutover,
}

/// Store-global generation barrier with explicit pre-barrier drain and suffix queue.
#[derive(Debug)]
pub(crate) struct GenerationBarrier {
    generation: u64,
    compacted_prefix: FrameCommitment,
    committed: BTreeMap<u64, Vec<u8>>,
    pre_barrier_pending: BTreeMap<u64, Vec<u8>>,
    waiting: BTreeMap<u64, Vec<u8>>,
    acquired: bool,
    reserve_held: bool,
}

impl GenerationBarrier {
    pub(crate) fn new(
        generation: u64,
        committed: BTreeMap<u64, Vec<u8>>,
    ) -> Result<Self, CompactionError> {
        if generation == 0 {
            return Err(CompactionError::InvalidGeneration);
        }
        let last = committed.keys().next_back().copied().unwrap_or(0);
        validate_contiguous(committed.keys().copied(), 1, last)?;
        for (sequence, bytes) in &committed {
            CompactionMutation::decode(*sequence, bytes)?;
        }
        Ok(Self {
            generation,
            compacted_prefix: FrameCommitment::genesis(),
            committed,
            pre_barrier_pending: BTreeMap::new(),
            waiting: BTreeMap::new(),
            acquired: false,
            reserve_held: false,
        })
    }

    pub(crate) fn from_recovered(recovered: &RecoveredCompaction) -> Result<Self, CompactionError> {
        let first = recovered
            .base
            .applied_through
            .checked_add(1)
            .ok_or(CompactionError::SequenceConflict)?;
        let last = recovered
            .suffix
            .keys()
            .next_back()
            .copied()
            .unwrap_or(recovered.base.applied_through);
        validate_contiguous(recovered.suffix.keys().copied(), first, last)?;
        Ok(Self {
            generation: recovered.generation(),
            compacted_prefix: recovered.base.frame_commitment,
            committed: recovered.suffix.clone(),
            pre_barrier_pending: BTreeMap::new(),
            waiting: BTreeMap::new(),
            acquired: false,
            reserve_held: false,
        })
    }

    pub(crate) fn register_mutation(
        &mut self,
        sequence: u64,
        mutation: CompactionMutation,
    ) -> Result<MutationRoute, CompactionError> {
        if self.committed.contains_key(&sequence)
            || self.pre_barrier_pending.contains_key(&sequence)
            || self.waiting.contains_key(&sequence)
        {
            return Err(CompactionError::SequenceConflict);
        }
        let bytes = mutation.encode(sequence)?;
        if self.acquired {
            self.waiting.insert(sequence, bytes);
            Ok(MutationRoute::WaitingForCutover)
        } else {
            self.pre_barrier_pending.insert(sequence, bytes);
            Ok(MutationRoute::CurrentGeneration)
        }
    }

    fn pending_pre_barrier(&self, sequence: u64) -> Result<&[u8], CompactionError> {
        self.pre_barrier_pending
            .get(&sequence)
            .map(Vec::as_slice)
            .ok_or(CompactionError::UnknownMutation)
    }

    fn expected_pre_sequence(&self) -> Result<u64, CompactionError> {
        self.committed
            .keys()
            .next_back()
            .copied()
            .unwrap_or(self.compacted_prefix.applied_through)
            .checked_add(1)
            .ok_or(CompactionError::SequenceConflict)
    }

    fn mark_pre_barrier_committed(&mut self, sequence: u64) -> Result<(), CompactionError> {
        let expected = self.expected_pre_sequence()?;
        if sequence != expected {
            return Err(CompactionError::SequenceConflict);
        }
        let bytes = self
            .pre_barrier_pending
            .remove(&sequence)
            .ok_or(CompactionError::UnknownMutation)?;
        self.committed.insert(sequence, bytes);
        Ok(())
    }

    pub(crate) fn acquire(
        &mut self,
        capacity: &mut CapacityController,
    ) -> Result<(), CompactionError> {
        if self.acquired {
            return Err(CompactionError::BarrierAlreadyHeld);
        }
        capacity.begin_compaction()?;
        self.acquired = true;
        self.reserve_held = true;
        Ok(())
    }

    pub(crate) fn is_drained(&self) -> bool {
        self.pre_barrier_pending.is_empty()
    }

    pub(crate) fn cutoff(&self) -> Result<u64, CompactionError> {
        if !self.acquired || !self.is_drained() || !self.reserve_held {
            return Err(CompactionError::BarrierNotReady);
        }
        Ok(self
            .committed
            .keys()
            .next_back()
            .copied()
            .unwrap_or(self.compacted_prefix.applied_through))
    }

    pub(crate) fn committed(&self) -> &BTreeMap<u64, Vec<u8>> {
        &self.committed
    }

    pub(crate) fn cutoff_commitment(&self) -> Result<FrameCommitment, CompactionError> {
        self.cutoff()?;
        let mut commitment = self.compacted_prefix;
        for (sequence, bytes) in &self.committed {
            commitment = commitment.append(*sequence, bytes)?;
        }
        Ok(commitment)
    }

    fn suffix(&self) -> Result<&BTreeMap<u64, Vec<u8>>, CompactionError> {
        let cutoff = self.cutoff()?;
        let last = self.waiting.keys().next_back().copied().unwrap_or(cutoff);
        validate_contiguous(self.waiting.keys().copied(), cutoff.saturating_add(1), last)?;
        Ok(&self.waiting)
    }

    fn release_after_commit(
        &mut self,
        capacity: &mut CapacityController,
    ) -> Result<(), CompactionError> {
        capacity.finish_compaction()?;
        self.compacted_prefix = self.cutoff_commitment()?;
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or(CompactionError::InvalidGeneration)?;
        self.committed = std::mem::take(&mut self.waiting);
        self.acquired = false;
        self.reserve_held = false;
        Ok(())
    }

    pub(crate) const fn reserve_held(&self) -> bool {
        self.reserve_held
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompactionFault {
    None,
    AfterBaseWrite,
    AfterBaseFileSync,
    AfterSuffixWrite,
    AfterSuffixFileSync,
    AfterGenerationDirectorySync,
    AfterRootWrite,
    AfterRootFileSync,
    AfterRootRename,
    AfterRootDirectorySync,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CompactionOutcome {
    KeptOld,
    Unknown,
    Committed(DurableGcProof),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecoveredCompaction {
    base: CompactionBase,
    suffix: BTreeMap<u64, Vec<u8>>,
}

type MaterializedCompactionState = (Option<Vec<u8>>, BTreeMap<[u8; 16], Vec<u8>>);

impl RecoveredCompaction {
    pub(crate) const fn generation(&self) -> u64 {
        self.base.generation
    }

    pub(crate) const fn applied_through(&self) -> u64 {
        self.base.applied_through
    }

    pub(crate) fn suffix(&self) -> &BTreeMap<u64, Vec<u8>> {
        &self.suffix
    }

    pub(crate) fn materialized_state(
        &self,
    ) -> Result<MaterializedCompactionState, CompactionError> {
        let mut checkpoint = self.base.checkpoint.clone();
        let mut identities = self.base.identities.clone();
        for (sequence, bytes) in &self.suffix {
            CompactionMutation::decode(*sequence, bytes)?.apply(&mut checkpoint, &mut identities);
        }
        Ok((checkpoint, identities))
    }

    pub(crate) fn frame_commitment(&self) -> Result<FrameCommitment, CompactionError> {
        let mut commitment = self.base.frame_commitment;
        for (sequence, bytes) in &self.suffix {
            commitment = commitment.append(*sequence, bytes)?;
        }
        Ok(commitment)
    }
}

#[derive(Debug)]
pub(crate) struct CompactionStore {
    root: PathBuf,
    active: RecoveredCompaction,
    recovery_required: bool,
}

impl CompactionStore {
    pub(crate) fn initialize(root: &Path, base: CompactionBase) -> Result<Self, CompactionError> {
        if base.generation != 1 || root.join(ROOT_FILE).exists() {
            return Err(CompactionError::InvalidGeneration);
        }
        fs::create_dir_all(root).map_err(io_error)?;
        let directory = generation_directory(root, base.generation);
        fs::create_dir(&directory).map_err(io_error)?;
        write_synced_new(&directory.join(BASE_FILE), &base.encode()?)?;
        let suffix_bytes = encode_suffix(base.generation, base.applied_through, &BTreeMap::new())?;
        write_synced_new(&directory.join(SUFFIX_FILE), &suffix_bytes)?;
        sync_directory(&directory).map_err(io_error)?;
        let root_bytes = encode_root(&base, &suffix_bytes)?;
        durably_install(&root.join(ROOT_FILE), &root_bytes, InstallFault::None)?;
        Self::recover(root)
    }

    pub(crate) fn recover(root: &Path) -> Result<Self, CompactionError> {
        let active = recover_generation(root)?;
        Ok(Self {
            root: root.to_path_buf(),
            active,
            recovery_required: false,
        })
    }

    /// Builds the only materialization accepted by [`Self::compact`]. Immutable
    /// creation and checkpoint state are carried forward exactly; processed
    /// identities may disappear only when the active entry has all three GC
    /// horizons and an exact live capacity charge.
    pub(crate) fn plan_compaction(
        &self,
        barrier: &GenerationBarrier,
        identities: impl IntoIterator<Item = CompactionIdentity>,
        broker_oldest: u64,
        now_unix_ms: u64,
        clock: ClockObservation,
        capacity: &mut CapacityController,
    ) -> Result<CompactionPlan, CompactionError> {
        if self.recovery_required {
            return Err(CompactionError::RecoveryRequired);
        }
        let cutoff = barrier.cutoff()?;
        if barrier.generation != self.active.generation()
            || self.active.frame_commitment()? != barrier.cutoff_commitment()?
        {
            return Err(CompactionError::BarrierSnapshotMismatch);
        }
        if !matches!(clock, ClockObservation::Trusted) {
            capacity.stop_for_clock_uncertainty();
        }

        let mut candidates = BTreeMap::new();
        for identity in identities {
            let message_id = identity.message_id;
            let canonical = CompactionIdentity::new(message_id, identity.encoded.clone())?;
            if canonical != identity || candidates.insert(message_id, canonical).is_some() {
                return Err(CompactionError::GcPlanMismatch);
            }
        }
        let mut checkpoint = self.active.base.checkpoint.clone();
        let mut materialized_identities = self.active.base.identities.clone();
        for (sequence, bytes) in barrier.committed() {
            CompactionMutation::decode(*sequence, bytes)?
                .apply(&mut checkpoint, &mut materialized_identities);
        }
        if candidates.len() != materialized_identities.len() {
            return Err(CompactionError::GcPlanMismatch);
        }

        let mut retained = BTreeMap::new();
        let mut collected_identities = BTreeMap::new();
        for (message_id, active_bytes) in &materialized_identities {
            let candidate = candidates
                .remove(message_id)
                .ok_or(CompactionError::GcPlanMismatch)?;
            if candidate.encoded != *active_bytes {
                return Err(CompactionError::GcPlanMismatch);
            }
            match candidate
                .horizon
                .evaluate(broker_oldest, now_unix_ms, clock)
            {
                IdentityGcDecision::Eligible => {
                    if !barrier.waiting.is_empty() {
                        return Err(CompactionError::GcDeferredByWaitingMutation);
                    }
                    collected_identities
                        .insert(*message_id, capacity.identity_gc_token(*message_id)?);
                }
                IdentityGcDecision::Retain | IdentityGcDecision::RetainAndStopPoll => {
                    retained.insert(*message_id, active_bytes.clone());
                }
            }
        }
        if !candidates.is_empty() {
            return Err(CompactionError::GcPlanMismatch);
        }

        let source_base_digest = digest(&self.active.base.encode()?);
        let base = CompactionBase::with_commitment(
            self.active
                .generation()
                .checked_add(1)
                .ok_or(CompactionError::InvalidGeneration)?,
            cutoff,
            self.active.base.immutable_creation.clone(),
            checkpoint,
            retained,
            barrier.cutoff_commitment()?,
        )?;
        let planned_base_digest = digest(&base.encode()?);
        Ok(CompactionPlan {
            source_generation: self.active.generation(),
            source_base_digest,
            planned_base_digest,
            base,
            collected_identities,
        })
    }

    pub(crate) fn compact(
        &mut self,
        plan: CompactionPlan,
        barrier: &mut GenerationBarrier,
        capacity: &mut CapacityController,
        fault: CompactionFault,
    ) -> Result<CompactionOutcome, CompactionError> {
        if self.recovery_required {
            return Err(CompactionError::RecoveryRequired);
        }
        let cutoff = barrier.cutoff()?;
        let CompactionPlan {
            source_generation,
            source_base_digest,
            planned_base_digest,
            base,
            collected_identities,
        } = plan;
        let next_generation = self
            .active
            .generation()
            .checked_add(1)
            .ok_or(CompactionError::InvalidGeneration)?;
        let mut expected_checkpoint = self.active.base.checkpoint.clone();
        let mut expected_identities = self.active.base.identities.clone();
        for (sequence, bytes) in barrier.committed() {
            CompactionMutation::decode(*sequence, bytes)?
                .apply(&mut expected_checkpoint, &mut expected_identities);
        }
        for (message_id, token) in &collected_identities {
            if expected_identities.remove(message_id).is_none()
                || capacity.identity_gc_token(*message_id) != Ok(*token)
            {
                return Err(CompactionError::GcPlanMismatch);
            }
        }
        if source_generation != self.active.generation()
            || source_base_digest != digest(&self.active.base.encode()?)
            || planned_base_digest != digest(&base.encode()?)
            || base.immutable_creation != self.active.base.immutable_creation
            || base.checkpoint != expected_checkpoint
            || base.identities != expected_identities
            || base.generation != next_generation
            || base.applied_through != cutoff
            || base.frame_commitment != barrier.cutoff_commitment()?
            || self.active.frame_commitment()? != barrier.cutoff_commitment()?
        {
            return Err(CompactionError::CompactionPlanMismatch);
        }
        if !collected_identities.is_empty() && !barrier.waiting.is_empty() {
            return Err(CompactionError::GcPlanMismatch);
        }
        let suffix = barrier.suffix()?;
        if barrier.generation != self.active.generation() {
            return Err(CompactionError::BarrierSnapshotMismatch);
        }
        let directory = generation_directory(&self.root, next_generation);
        if directory.exists() {
            return Err(CompactionError::ExistingGeneration);
        }
        let base_bytes = base.encode()?;
        let suffix_bytes = encode_suffix(base.generation, base.applied_through, suffix)?;
        let root_bytes = encode_root(&base, &suffix_bytes)?;
        if let Err(error) = fs::create_dir(&directory).map_err(io_error) {
            self.recovery_required = true;
            return Err(error);
        }
        let prepared = (|| -> Result<bool, CompactionError> {
            write_new(&directory.join(BASE_FILE), &base_bytes)?;
            if fault == CompactionFault::AfterBaseWrite {
                return Ok(false);
            }
            sync_file(&directory.join(BASE_FILE))?;
            if fault == CompactionFault::AfterBaseFileSync {
                return Ok(false);
            }
            write_new(&directory.join(SUFFIX_FILE), &suffix_bytes)?;
            if fault == CompactionFault::AfterSuffixWrite {
                return Ok(false);
            }
            sync_file(&directory.join(SUFFIX_FILE))?;
            if fault == CompactionFault::AfterSuffixFileSync {
                return Ok(false);
            }
            sync_directory(&directory).map_err(io_error)?;
            sync_directory(&self.root).map_err(io_error)?;
            Ok(fault != CompactionFault::AfterGenerationDirectorySync)
        })();
        match prepared {
            Ok(true) => {}
            Ok(false) => {
                self.recovery_required = true;
                return Ok(CompactionOutcome::KeptOld);
            }
            Err(error) => {
                self.recovery_required = true;
                return Err(error);
            }
        }
        let install_fault = match fault {
            CompactionFault::AfterRootWrite => InstallFault::AfterWrite,
            CompactionFault::AfterRootFileSync => InstallFault::AfterFileSync,
            CompactionFault::AfterRootRename => InstallFault::AfterRename,
            CompactionFault::AfterRootDirectorySync => InstallFault::AfterDirectorySync,
            _ => InstallFault::None,
        };
        match durably_install(&self.root.join(ROOT_FILE), &root_bytes, install_fault) {
            Ok(()) => {
                self.active = match recover_generation(&self.root) {
                    Ok(active) => active,
                    Err(error) => {
                        self.recovery_required = true;
                        return Err(error);
                    }
                };
                if let Err(error) = barrier.release_after_commit(capacity) {
                    self.recovery_required = true;
                    return Err(error);
                }
                Ok(CompactionOutcome::Committed(
                    DurableGcProof::after_root_sync(next_generation, collected_identities)?,
                ))
            }
            Err(InstallError::KnownOld { .. }) => {
                self.recovery_required = true;
                Ok(CompactionOutcome::KeptOld)
            }
            Err(InstallError::Unknown { .. }) => {
                self.recovery_required = true;
                Ok(CompactionOutcome::Unknown)
            }
        }
    }

    pub(crate) fn reclaim_generation(&self, generation: u64) -> Result<(), CompactionError> {
        if self.recovery_required || generation == self.active.generation() {
            return Err(CompactionError::CannotReclaimActiveGeneration);
        }
        let directory = generation_directory(&self.root, generation);
        if directory.exists() {
            fs::remove_dir_all(&directory).map_err(io_error)?;
            sync_directory(&self.root).map_err(io_error)?;
        }
        Ok(())
    }

    pub(crate) const fn active(&self) -> &RecoveredCompaction {
        &self.active
    }

    pub(crate) const fn recovery_required(&self) -> bool {
        self.recovery_required
    }

    /// Completes a mutation registered before the barrier only after its old-generation
    /// suffix frame is file-synced. This is the no-loss bridge into the new base.
    pub(crate) fn commit_registered(
        &mut self,
        barrier: &mut GenerationBarrier,
        sequence: u64,
    ) -> Result<(), CompactionError> {
        if self.recovery_required {
            return Err(CompactionError::RecoveryRequired);
        }
        let expected = self
            .active
            .base
            .applied_through
            .checked_add(self.active.suffix.len() as u64)
            .and_then(|value| value.checked_add(1))
            .ok_or(CompactionError::SequenceConflict)?;
        if barrier.generation != self.active.generation() || sequence != expected {
            return Err(CompactionError::SequenceConflict);
        }
        let payload = barrier.pending_pre_barrier(sequence)?.to_vec();
        if barrier.expected_pre_sequence()? != sequence {
            return Err(CompactionError::SequenceConflict);
        }
        let frame = encode_suffix_frame(sequence, &payload)?;
        let path = generation_directory(&self.root, self.active.generation()).join(SUFFIX_FILE);
        let append_result = (|| {
            let mut file = OpenOptions::new()
                .append(true)
                .open(path)
                .map_err(io_error)?;
            file.write_all(&frame).map_err(io_error)?;
            file.sync_all().map_err(io_error)
        })();
        if let Err(error) = append_result {
            self.recovery_required = true;
            return Err(error);
        }
        self.active.suffix.insert(sequence, payload);
        barrier.mark_pre_barrier_committed(sequence)
    }
}

#[cfg(test)]
pub(crate) struct CompactionCorpusInputs<'a> {
    pub(crate) requirements: &'a [u8],
    pub(crate) design: &'a [u8],
    pub(crate) sources: &'a [(&'a str, &'a [u8])],
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

    const fn generation(self) -> u64 {
        match self {
            Self::Old | Self::UnknownOld => 1,
            Self::UnknownNew | Self::New => 2,
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum CorpusImage {
    PartialBase,
    CompleteBase,
    PartialSuffix,
    CompleteGenerationOldRoot,
    PartialPendingRoot,
    CompletePendingRoot,
    OldRoot,
    NewRoot,
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
        name: "base-write-old",
        stage: "base-write",
        oracle: CorpusOracle::Old,
        image: CorpusImage::PartialBase,
    },
    CorpusCase {
        name: "base-file-sync-old",
        stage: "base-file-sync",
        oracle: CorpusOracle::Old,
        image: CorpusImage::CompleteBase,
    },
    CorpusCase {
        name: "suffix-write-old",
        stage: "suffix-write",
        oracle: CorpusOracle::Old,
        image: CorpusImage::PartialSuffix,
    },
    CorpusCase {
        name: "suffix-file-sync-old",
        stage: "suffix-file-sync",
        oracle: CorpusOracle::Old,
        image: CorpusImage::CompleteGenerationOldRoot,
    },
    CorpusCase {
        name: "generation-directory-sync-old",
        stage: "generation-directory-sync",
        oracle: CorpusOracle::Old,
        image: CorpusImage::CompleteGenerationOldRoot,
    },
    CorpusCase {
        name: "root-write-old",
        stage: "root-write",
        oracle: CorpusOracle::Old,
        image: CorpusImage::PartialPendingRoot,
    },
    CorpusCase {
        name: "root-file-sync-old",
        stage: "root-file-sync",
        oracle: CorpusOracle::Old,
        image: CorpusImage::CompletePendingRoot,
    },
    CorpusCase {
        name: "root-rename-unknown-old",
        stage: "root-rename",
        oracle: CorpusOracle::UnknownOld,
        image: CorpusImage::OldRoot,
    },
    CorpusCase {
        name: "root-rename-unknown-new",
        stage: "root-rename",
        oracle: CorpusOracle::UnknownNew,
        image: CorpusImage::NewRoot,
    },
    CorpusCase {
        name: "root-directory-sync-unknown-old",
        stage: "root-directory-sync",
        oracle: CorpusOracle::UnknownOld,
        image: CorpusImage::OldRoot,
    },
    CorpusCase {
        name: "root-directory-sync-unknown-new",
        stage: "root-directory-sync",
        oracle: CorpusOracle::UnknownNew,
        image: CorpusImage::NewRoot,
    },
    CorpusCase {
        name: "root-directory-sync-new",
        stage: "root-directory-sync",
        oracle: CorpusOracle::New,
        image: CorpusImage::NewRoot,
    },
];

#[cfg(test)]
struct CorpusFixtures {
    old_base: Vec<u8>,
    old_suffix: Vec<u8>,
    new_base: Vec<u8>,
    new_suffix: Vec<u8>,
    old_root: Vec<u8>,
    new_root: Vec<u8>,
}

#[cfg(test)]
pub(crate) fn generate_compaction_corpus(
    output: &Path,
    inputs: &CompactionCorpusInputs<'_>,
) -> Result<(), CompactionError> {
    if output.exists() {
        fs::remove_dir_all(output).map_err(io_error)?;
    }
    fs::create_dir(output).map_err(io_error)?;
    let fixtures = corpus_fixtures(inputs)?;
    let mut artifacts = BTreeMap::new();
    for case in CORPUS_CASES {
        let files = corpus_case_files(case.image, &fixtures);
        for (relative, bytes) in &files {
            let path = output.join(case.name).join(relative);
            fs::create_dir_all(path.parent().ok_or(CompactionError::InvalidBase)?)
                .map_err(io_error)?;
            write_synced_new(&path, bytes)?;
        }
        sync_corpus_directories(&output.join(case.name))?;
        artifacts.insert(case.name, files);
    }
    let manifest = corpus_manifest(&artifacts, inputs)?;
    write_synced_new(&output.join("manifest.json"), manifest.as_bytes())?;
    sync_directory(output).map_err(io_error)?;
    Ok(())
}

#[cfg(test)]
pub(crate) fn verify_compaction_corpus(
    output: &Path,
    inputs: &CompactionCorpusInputs<'_>,
) -> Result<(), CompactionError> {
    let fixtures = corpus_fixtures(inputs)?;
    let mut expected_inventory = BTreeSet::from([PathBuf::from("manifest.json")]);
    let mut artifacts = BTreeMap::new();
    for case in CORPUS_CASES {
        let files = corpus_case_files(case.image, &fixtures);
        for (relative, expected) in &files {
            let inventory_path = PathBuf::from(case.name).join(relative);
            expected_inventory.insert(inventory_path.clone());
            if fs::read(output.join(&inventory_path)).map_err(io_error)? != *expected {
                return Err(CompactionError::CorpusMismatch);
            }
        }
        let recovered = recover_generation(&output.join(case.name))?;
        if recovered.generation() != case.oracle.generation() {
            return Err(CompactionError::CorpusMismatch);
        }
        if recovered.generation() == 2
            && (recovered.applied_through() != 2
                || recovered.suffix().keys().copied().collect::<Vec<_>>() != [3])
        {
            return Err(CompactionError::CorpusMismatch);
        }
        artifacts.insert(case.name, files);
    }
    if corpus_inventory(output)? != expected_inventory {
        return Err(CompactionError::CorpusMismatch);
    }
    let manifest = corpus_manifest(&artifacts, inputs)?;
    if fs::read(output.join("manifest.json")).map_err(io_error)? != manifest.as_bytes() {
        return Err(CompactionError::CorpusMismatch);
    }
    Ok(())
}

#[cfg(test)]
fn corpus_fixtures(inputs: &CompactionCorpusInputs<'_>) -> Result<CorpusFixtures, CompactionError> {
    let source = corpus_source_input_digest(inputs);
    let requirements = digest(inputs.requirements);
    let design = digest(inputs.design);
    let mut corpus_message_id: [u8; 16] = source[..16].try_into().expect("digest prefix");
    corpus_message_id[6] = (corpus_message_id[6] & 0x0f) | 0x40;
    corpus_message_id[8] = (corpus_message_id[8] & 0x3f) | 0x80;
    let corpus_identity = IdentityRecord::fixture(
        corpus_message_id,
        source,
        requirements[..16].try_into().expect("digest prefix"),
        1,
        0,
        4,
        1,
    )?
    .mark_checkpointed()?
    .encode_body()
    .to_vec();
    let old_frames = BTreeMap::from([(
        1,
        CompactionMutation::InstallCheckpoint(design.to_vec()).encode(1)?,
    )]);
    let new_frames = BTreeMap::from([
        (
            1,
            CompactionMutation::InstallCheckpoint(design.to_vec()).encode(1)?,
        ),
        (
            2,
            CompactionMutation::InstallCheckpoint(source.to_vec()).encode(2)?,
        ),
    ]);
    let suffix = BTreeMap::from([(
        3,
        CompactionMutation::InstallCheckpoint(requirements.to_vec()).encode(3)?,
    )]);
    let old_base_model = CompactionBase::new(
        1,
        1,
        requirements.to_vec(),
        Some(design.to_vec()),
        BTreeMap::from([(corpus_message_id, corpus_identity.clone())]),
        &old_frames,
    )?;
    let new_base_model = CompactionBase::new(
        2,
        2,
        requirements.to_vec(),
        Some(source.to_vec()),
        BTreeMap::from([(corpus_message_id, corpus_identity)]),
        &new_frames,
    )?;
    let old_base = old_base_model.encode()?;
    let new_base = new_base_model.encode()?;
    let old_suffix = encode_suffix(1, 1, &BTreeMap::new())?;
    let new_suffix = encode_suffix(2, 2, &suffix)?;
    let old_root = encode_root(&old_base_model, &old_suffix)?;
    let new_root = encode_root(&new_base_model, &new_suffix)?;
    Ok(CorpusFixtures {
        old_base,
        old_suffix,
        new_base,
        new_suffix,
        old_root,
        new_root,
    })
}

#[cfg(test)]
fn corpus_case_files(image: CorpusImage, fixtures: &CorpusFixtures) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut files = BTreeMap::from([
        (
            PathBuf::from("generation-00000000000000000001").join(BASE_FILE),
            fixtures.old_base.clone(),
        ),
        (
            PathBuf::from("generation-00000000000000000001").join(SUFFIX_FILE),
            fixtures.old_suffix.clone(),
        ),
        (PathBuf::from(ROOT_FILE), fixtures.old_root.clone()),
    ]);
    let new_directory = PathBuf::from("generation-00000000000000000002");
    match image {
        CorpusImage::PartialBase => {
            files.insert(
                new_directory.join(BASE_FILE),
                fixtures.new_base[..fixtures.new_base.len() / 2].to_vec(),
            );
        }
        CorpusImage::CompleteBase => {
            files.insert(new_directory.join(BASE_FILE), fixtures.new_base.clone());
        }
        CorpusImage::PartialSuffix => {
            files.insert(new_directory.join(BASE_FILE), fixtures.new_base.clone());
            files.insert(
                new_directory.join(SUFFIX_FILE),
                fixtures.new_suffix[..fixtures.new_suffix.len() / 2].to_vec(),
            );
        }
        CorpusImage::CompleteGenerationOldRoot
        | CorpusImage::PartialPendingRoot
        | CorpusImage::CompletePendingRoot
        | CorpusImage::OldRoot
        | CorpusImage::NewRoot => {
            files.insert(new_directory.join(BASE_FILE), fixtures.new_base.clone());
            files.insert(new_directory.join(SUFFIX_FILE), fixtures.new_suffix.clone());
        }
    }
    match image {
        CorpusImage::PartialPendingRoot => {
            files.insert(
                PathBuf::from(ROOT_PENDING_FILE),
                fixtures.new_root[..fixtures.new_root.len() / 2].to_vec(),
            );
        }
        CorpusImage::CompletePendingRoot => {
            files.insert(PathBuf::from(ROOT_PENDING_FILE), fixtures.new_root.clone());
        }
        CorpusImage::NewRoot => {
            files.insert(PathBuf::from(ROOT_FILE), fixtures.new_root.clone());
        }
        _ => {}
    }
    files
}

#[cfg(test)]
fn corpus_manifest(
    artifacts: &BTreeMap<&'static str, BTreeMap<PathBuf, Vec<u8>>>,
    inputs: &CompactionCorpusInputs<'_>,
) -> Result<String, CompactionError> {
    let mut sources = inputs.sources.to_vec();
    sources.sort_by_key(|(path, _)| *path);
    let mut output = String::from("{\n");
    output.push_str("  \"schema_version\": 1,\n");
    output.push_str("  \"producer_task\": \"4.3\",\n");
    output.push_str(&format!(
        "  \"requirements_sha256\": \"{}\",\n",
        hex(&digest(inputs.requirements))
    ));
    output.push_str(&format!(
        "  \"design_sha256\": \"{}\",\n",
        hex(&digest(inputs.design))
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
        let files = artifacts
            .get(case.name)
            .ok_or(CompactionError::CorpusMismatch)?;
        output.push_str(&format!(
            "    {{\"name\":\"{}\",\"stage\":\"{}\",\"oracle\":\"{}\",\"files\":[",
            case.name,
            case.stage,
            case.oracle.name()
        ));
        for (file_index, (path, bytes)) in files.iter().enumerate() {
            output.push_str(&format!(
                "{{\"path\":\"{}\",\"sha256\":\"{}\"}}{}",
                path.display(),
                hex(&digest(bytes)),
                if file_index + 1 == files.len() {
                    ""
                } else {
                    ","
                }
            ));
        }
        output.push_str(&format!(
            "]}}{}\n",
            if index + 1 == CORPUS_CASES.len() {
                ""
            } else {
                ","
            }
        ));
    }
    output.push_str("  ]\n}\n");
    Ok(output)
}

#[cfg(test)]
fn corpus_source_input_digest(inputs: &CompactionCorpusInputs<'_>) -> [u8; 32] {
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
fn corpus_inventory(root: &Path) -> Result<BTreeSet<PathBuf>, CompactionError> {
    fn visit(
        root: &Path,
        current: &Path,
        output: &mut BTreeSet<PathBuf>,
    ) -> Result<(), CompactionError> {
        for entry in fs::read_dir(current).map_err(io_error)? {
            let path = entry.map_err(io_error)?.path();
            if path.is_dir() {
                visit(root, &path, output)?;
            } else {
                output.insert(
                    path.strip_prefix(root)
                        .map_err(|_| CompactionError::CorpusMismatch)?
                        .to_path_buf(),
                );
            }
        }
        Ok(())
    }
    let mut result = BTreeSet::new();
    visit(root, root, &mut result)?;
    Ok(result)
}

#[cfg(test)]
fn sync_corpus_directories(root: &Path) -> Result<(), CompactionError> {
    let mut directories = vec![root.to_path_buf()];
    for entry in fs::read_dir(root).map_err(io_error)? {
        let path = entry.map_err(io_error)?.path();
        if path.is_dir() {
            directories.push(path);
        }
    }
    for directory in directories.into_iter().rev() {
        sync_directory(&directory).map_err(io_error)?;
    }
    Ok(())
}

fn generation_directory(root: &Path, generation: u64) -> PathBuf {
    root.join(format!("generation-{generation:020}"))
}

fn encode_suffix(
    generation: u64,
    applied_through: u64,
    frames: &BTreeMap<u64, Vec<u8>>,
) -> Result<Vec<u8>, CompactionError> {
    let mut output = encode_suffix_header(generation, applied_through)?;
    for (sequence, bytes) in frames {
        CompactionMutation::decode(*sequence, bytes)?;
        output.extend_from_slice(&encode_suffix_frame(*sequence, bytes)?);
    }
    if output.len() as u64 > MAX_COMPACTION_FILE_LEN {
        return Err(CompactionError::FileTooLarge);
    }
    Ok(output)
}

fn encode_suffix_header(generation: u64, applied_through: u64) -> Result<Vec<u8>, CompactionError> {
    let mut body = Vec::new();
    put_u16(&mut body, FORMAT_VERSION);
    put_u64(&mut body, generation);
    put_u64(&mut body, applied_through);
    body.extend_from_slice(COMMIT_MARKER);
    encode_state_frame(StateRecordKind::CompactionSuffixHeader, &body).map_err(Into::into)
}

fn encode_suffix_frame(sequence: u64, bytes: &[u8]) -> Result<Vec<u8>, CompactionError> {
    let mut body = Vec::new();
    put_u64(&mut body, sequence);
    put_bytes(&mut body, bytes)?;
    body.extend_from_slice(COMMIT_MARKER);
    encode_state_frame(StateRecordKind::CompactionSuffix, &body).map_err(Into::into)
}

fn suffix_header_length(bytes: &[u8]) -> Result<usize, CompactionError> {
    state_frame_encoded_len(bytes)?.ok_or(CompactionError::IncompleteGeneration)
}

fn decode_suffix(
    bytes: &[u8],
    generation: u64,
    applied_through: u64,
) -> Result<(BTreeMap<u64, Vec<u8>>, usize), CompactionError> {
    let mut output = BTreeMap::new();
    let mut position = suffix_header_length(bytes)?;
    let header = decode_state_frame(&bytes[..position], StateRecordKind::CompactionSuffixHeader)?;
    let mut header_reader = Reader::new(header);
    if header_reader.u16()? != FORMAT_VERSION
        || header_reader.u64()? != generation
        || header_reader.u64()? != applied_through
    {
        return Err(CompactionError::MixedGeneration);
    }
    header_reader.marker()?;
    header_reader.finish()?;
    let mut expected = applied_through
        .checked_add(1)
        .ok_or(CompactionError::SequenceConflict)?;
    while position < bytes.len() {
        let Some(length) = state_frame_encoded_len(&bytes[position..])? else {
            break;
        };
        let body = decode_state_frame(
            &bytes[position..position + length],
            StateRecordKind::CompactionSuffix,
        )?;
        let mut reader = Reader::new(body);
        let sequence = reader.u64()?;
        let payload = reader.bytes()?;
        reader.marker()?;
        reader.finish()?;
        if sequence != expected || output.insert(sequence, payload).is_some() {
            return Err(CompactionError::SequenceConflict);
        }
        CompactionMutation::decode(
            sequence,
            output
                .get(&sequence)
                .ok_or(CompactionError::InvalidMutation)?,
        )?;
        expected = expected
            .checked_add(1)
            .ok_or(CompactionError::SequenceConflict)?;
        position += length;
    }
    Ok((output, position))
}

fn encode_root(base: &CompactionBase, suffix_bytes: &[u8]) -> Result<Vec<u8>, CompactionError> {
    let base_bytes = base.encode()?;
    let header_length = suffix_header_length(suffix_bytes)?;
    let mut body = Vec::new();
    put_u16(&mut body, FORMAT_VERSION);
    put_u64(&mut body, base.generation);
    put_u64(&mut body, base.applied_through);
    body.extend_from_slice(&digest(&base_bytes));
    body.extend_from_slice(&digest(&suffix_bytes[..header_length]));
    body.extend_from_slice(COMMIT_MARKER);
    encode_state_frame(StateRecordKind::CompactionRoot, &body).map_err(Into::into)
}

fn decode_root(bytes: &[u8]) -> Result<(u64, u64, [u8; 32], [u8; 32]), CompactionError> {
    let body = decode_state_frame(bytes, StateRecordKind::CompactionRoot)?;
    let mut reader = Reader::new(body);
    if reader.u16()? != FORMAT_VERSION {
        return Err(CompactionError::UnsupportedVersion);
    }
    let generation = reader.u64()?;
    let applied_through = reader.u64()?;
    let base_digest = reader.array::<32>()?;
    let suffix_digest = reader.array::<32>()?;
    reader.marker()?;
    reader.finish()?;
    if generation == 0 {
        return Err(CompactionError::InvalidGeneration);
    }
    Ok((generation, applied_through, base_digest, suffix_digest))
}

fn recover_generation(root: &Path) -> Result<RecoveredCompaction, CompactionError> {
    let root_bytes = read_bounded(&root.join(ROOT_FILE))?;
    let (generation, applied_through, expected_base, expected_suffix) = decode_root(&root_bytes)?;
    let directory = generation_directory(root, generation);
    let base_bytes = read_bounded(&directory.join(BASE_FILE))?;
    let suffix_bytes = read_bounded(&directory.join(SUFFIX_FILE))?;
    let suffix_header_len = suffix_header_length(&suffix_bytes)?;
    if digest(&base_bytes) != expected_base
        || digest(&suffix_bytes[..suffix_header_len]) != expected_suffix
    {
        return Err(CompactionError::MixedGeneration);
    }
    let base = CompactionBase::decode(&base_bytes)?;
    if base.generation != generation || base.applied_through != applied_through {
        return Err(CompactionError::MixedGeneration);
    }
    let (suffix, valid_suffix_length) = decode_suffix(&suffix_bytes, generation, applied_through)?;
    if valid_suffix_length < suffix_bytes.len() {
        let suffix_path = directory.join(SUFFIX_FILE);
        let file = OpenOptions::new()
            .write(true)
            .open(&suffix_path)
            .map_err(io_error)?;
        file.set_len(valid_suffix_length as u64).map_err(io_error)?;
        file.sync_all().map_err(io_error)?;
    }
    Ok(RecoveredCompaction { base, suffix })
}

fn validate_contiguous(
    sequences: impl IntoIterator<Item = u64>,
    first: u64,
    last: u64,
) -> Result<(), CompactionError> {
    let mut expected = first;
    let mut saw = false;
    for sequence in sequences {
        saw = true;
        if sequence != expected {
            return Err(CompactionError::SequenceConflict);
        }
        expected = expected
            .checked_add(1)
            .ok_or(CompactionError::SequenceConflict)?;
    }
    if (last < first && saw) || (last >= first && expected != last.saturating_add(1)) {
        return Err(CompactionError::SequenceConflict);
    }
    Ok(())
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<(), CompactionError> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(io_error)?;
    set_owner_only_permissions(&file).map_err(io_error)?;
    file.write_all(bytes).map_err(io_error)
}

fn sync_file(path: &Path) -> Result<(), CompactionError> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(io_error)?
        .sync_all()
        .map_err(io_error)
}

fn write_synced_new(path: &Path, bytes: &[u8]) -> Result<(), CompactionError> {
    write_new(path, bytes)?;
    sync_file(path)
}

fn read_bounded(path: &Path) -> Result<Vec<u8>, CompactionError> {
    let metadata = fs::metadata(path).map_err(io_error)?;
    if metadata.len() > MAX_COMPACTION_FILE_LEN {
        return Err(CompactionError::FileTooLarge);
    }
    fs::read(path).map_err(io_error)
}

fn put_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn put_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn put_bytes(output: &mut Vec<u8>, bytes: &[u8]) -> Result<(), CompactionError> {
    put_u32(
        output,
        u32::try_from(bytes.len()).map_err(|_| CompactionError::InvalidBase)?,
    );
    output.extend_from_slice(bytes);
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], CompactionError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(CompactionError::InvalidBase)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(CompactionError::InvalidBase)?;
        self.position = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, CompactionError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, CompactionError> {
        Ok(u16::from_be_bytes(self.array()?))
    }

    fn u32(&mut self) -> Result<u32, CompactionError> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, CompactionError> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], CompactionError> {
        self.take(N)?
            .try_into()
            .map_err(|_| CompactionError::InvalidBase)
    }

    fn bytes(&mut self) -> Result<Vec<u8>, CompactionError> {
        let length = self.u32()? as usize;
        Ok(self.take(length)?.to_vec())
    }

    fn marker(&mut self) -> Result<(), CompactionError> {
        if self.take(COMMIT_MARKER.len())? != COMMIT_MARKER {
            return Err(CompactionError::InvalidBase);
        }
        Ok(())
    }

    fn finish(self) -> Result<(), CompactionError> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(CompactionError::InvalidBase)
        }
    }
}

fn io_error(error: io::Error) -> CompactionError {
    CompactionError::Io(error.kind())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(crate) enum CompactionError {
    #[error("compaction generation is invalid")]
    InvalidGeneration,
    #[error("compaction base is invalid")]
    InvalidBase,
    #[error("compaction suffix mutation is invalid")]
    InvalidMutation,
    #[error("compaction format version is unsupported")]
    UnsupportedVersion,
    #[error("compaction sequence has a duplicate or gap")]
    SequenceConflict,
    #[error("compaction mutation is not registered before the barrier")]
    UnknownMutation,
    #[error("compaction barrier is already held")]
    BarrierAlreadyHeld,
    #[error("compaction barrier has not drained or lost its reserve")]
    BarrierNotReady,
    #[error("compaction base does not match the frozen barrier")]
    BarrierSnapshotMismatch,
    #[error("compaction plan does not match the active base or frozen barrier")]
    CompactionPlanMismatch,
    #[error("identity GC plan does not exactly cover the active identity set")]
    GcPlanMismatch,
    #[error("identity GC is deferred while post-barrier mutations are waiting")]
    GcDeferredByWaitingMutation,
    #[error("compaction generation already exists")]
    ExistingGeneration,
    #[error("compaction outcome is unknown and requires fresh recovery")]
    RecoveryRequired,
    #[error("compaction selected a mixed or incomplete generation")]
    MixedGeneration,
    #[error("compaction generation is incomplete")]
    IncompleteGeneration,
    #[error("active or uncertain compaction generation cannot be reclaimed")]
    CannotReclaimActiveGeneration,
    #[error("compaction file exceeds the hard bound")]
    FileTooLarge,
    #[error("compaction corpus does not match its bound inputs or oracle")]
    CorpusMismatch,
    #[error("compaction capacity contract failed: {0}")]
    Capacity(#[from] CapacityError),
    #[error("compaction identity record is invalid: {0}")]
    Identity(#[from] IdentityError),
    #[error("compaction state frame is invalid: {0}")]
    Frame(#[from] StateFrameError),
    #[error("compaction durable install failed: {0}")]
    Install(#[from] InstallError),
    #[error("compaction I/O failed: {0:?}")]
    Io(io::ErrorKind),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::capacity::{
        CapacityFootprint, CapacityLimit, CapacityLimits, StartupReserves,
    };
    use std::fs;
    use tempfile::tempdir;

    fn controller() -> CapacityController {
        CapacityController::start(
            CapacityLimits::uniform(CapacityLimit::new(16, 4096).unwrap()),
            StartupReserves::new(
                CapacityFootprint::new(1, 64).unwrap(),
                CapacityFootprint::new(1, 512).unwrap(),
            ),
        )
        .unwrap()
    }

    fn frames(last: u64) -> BTreeMap<u64, Vec<u8>> {
        (1..=last)
            .map(|sequence| {
                (
                    sequence,
                    CompactionMutation::InstallCheckpoint(
                        format!("checkpoint-{sequence}").into_bytes(),
                    )
                    .encode(sequence)
                    .unwrap(),
                )
            })
            .collect()
    }

    fn message_id(fill: u8) -> [u8; 16] {
        let mut bytes = [fill; 16];
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        bytes
    }

    fn canonical_identity(message_id: [u8; 16], checkpointed: bool, offset: u64) -> Vec<u8> {
        let record =
            IdentityRecord::fixture(message_id, [0x22; 32], [0x33; 16], 1, 0, offset, 1).unwrap();
        let record = if checkpointed {
            record.mark_checkpointed().unwrap()
        } else {
            record
        };
        record.encode_body().to_vec()
    }

    fn base(generation: u64, last: u64, committed: &BTreeMap<u64, Vec<u8>>) -> CompactionBase {
        CompactionBase::new(
            generation,
            last,
            b"immutable-creation".to_vec(),
            Some(format!("checkpoint-{last}").into_bytes()),
            BTreeMap::from([(
                message_id(0x41),
                canonical_identity(message_id(0x41), true, 4),
            )]),
            committed,
        )
        .unwrap()
    }

    fn initialized(root: &Path) -> CompactionStore {
        CompactionStore::initialize(root, base(1, 1, &frames(1))).unwrap()
    }

    fn prepared_barrier(
        store: &mut CompactionStore,
        capacity: &mut CapacityController,
    ) -> GenerationBarrier {
        let mut barrier = GenerationBarrier::new(1, frames(1)).unwrap();
        assert_eq!(
            barrier
                .register_mutation(
                    2,
                    CompactionMutation::InstallCheckpoint(b"checkpoint-2".to_vec()),
                )
                .unwrap(),
            MutationRoute::CurrentGeneration
        );
        barrier.acquire(capacity).unwrap();
        assert_eq!(
            barrier
                .register_mutation(
                    3,
                    CompactionMutation::InstallCheckpoint(b"checkpoint-3".to_vec()),
                )
                .unwrap(),
            MutationRoute::WaitingForCutover
        );
        assert_eq!(barrier.cutoff(), Err(CompactionError::BarrierNotReady));
        store.commit_registered(&mut barrier, 2).unwrap();
        barrier
    }

    fn plan_with_observation(
        store: &CompactionStore,
        barrier: &GenerationBarrier,
        capacity: &mut CapacityController,
        broker_oldest: u64,
        now_unix_ms: u64,
        clock: ClockObservation,
    ) -> Result<CompactionPlan, CompactionError> {
        let identities = store
            .active
            .base
            .identities
            .iter()
            .map(|(message_id, encoded)| {
                CompactionIdentity::new(*message_id, encoded.clone()).unwrap()
            });
        store.plan_compaction(
            barrier,
            identities,
            broker_oldest,
            now_unix_ms,
            clock,
            capacity,
        )
    }

    fn retained_plan(
        store: &CompactionStore,
        barrier: &GenerationBarrier,
        capacity: &mut CapacityController,
    ) -> CompactionPlan {
        plan_with_observation(
            store,
            barrier,
            capacity,
            4,
            1_000,
            ClockObservation::Trusted,
        )
        .unwrap()
    }

    #[test]
    fn v07_task_4_3_identity_gc_requires_all_horizons_and_trusted_clock() {
        for (horizon, oldest, now, clock, expected) in [
            (
                IdentityHorizon::new(true, 4, 1_000),
                5,
                1_000,
                ClockObservation::Trusted,
                IdentityGcDecision::Eligible,
            ),
            (
                IdentityHorizon::new(false, 4, 1_000),
                5,
                1_000,
                ClockObservation::Trusted,
                IdentityGcDecision::Retain,
            ),
            (
                IdentityHorizon::new(true, 5, 1_000),
                5,
                1_000,
                ClockObservation::Trusted,
                IdentityGcDecision::Retain,
            ),
            (
                IdentityHorizon::new(true, 4, 1_001),
                5,
                1_000,
                ClockObservation::Trusted,
                IdentityGcDecision::Retain,
            ),
            (
                IdentityHorizon::new(true, 4, 1_000),
                5,
                1_000,
                ClockObservation::RollbackDetected,
                IdentityGcDecision::RetainAndStopPoll,
            ),
            (
                IdentityHorizon::new(true, 4, 1_000),
                5,
                1_000,
                ClockObservation::Unknown,
                IdentityGcDecision::RetainAndStopPoll,
            ),
        ] {
            assert_eq!(horizon.evaluate(oldest, now, clock), expected);
        }
        let identities = [
            CompactionIdentity::new(message_id(1), canonical_identity(message_id(1), true, 4))
                .unwrap(),
            CompactionIdentity::new(message_id(2), canonical_identity(message_id(2), true, 5))
                .unwrap(),
        ];
        let (retained, stop_poll) =
            collect_identity_horizon(identities.clone(), 5, 1_000, ClockObservation::Trusted);
        assert_eq!(
            retained.keys().copied().collect::<Vec<_>>(),
            [message_id(2)]
        );
        assert!(!stop_poll);
        let (retained, stop_poll) =
            collect_identity_horizon(identities, 5, 1_000, ClockObservation::RollbackDetected);
        assert_eq!(retained.len(), 2);
        assert!(stop_poll);
    }

    #[test]
    fn v07_task_4_3_compaction_plan_rejects_unproved_removal_and_materialization() {
        let root = tempdir().unwrap();
        let mut store = initialized(root.path());
        let mut capacity = controller();
        let mut barrier = GenerationBarrier::from_recovered(store.active()).unwrap();
        barrier.acquire(&mut capacity).unwrap();

        assert_eq!(
            store
                .plan_compaction(
                    &barrier,
                    std::iter::empty(),
                    5,
                    1_000,
                    ClockObservation::Trusted,
                    &mut capacity,
                )
                .unwrap_err(),
            CompactionError::GcPlanMismatch
        );
        let retained = plan_with_observation(
            &store,
            &barrier,
            &mut capacity,
            4,
            1_000,
            ClockObservation::Trusted,
        )
        .unwrap();
        assert_eq!(retained.base.identities, store.active.base.identities);

        let active_id = message_id(0x41);
        let forged = CompactionIdentity {
            message_id: active_id,
            encoded: store.active.base.identities[&active_id].clone(),
            horizon: IdentityHorizon::new(true, 0, 0),
        };
        assert_eq!(
            store
                .plan_compaction(
                    &barrier,
                    [forged],
                    1,
                    1_000,
                    ClockObservation::Trusted,
                    &mut capacity,
                )
                .unwrap_err(),
            CompactionError::GcPlanMismatch
        );

        let rollback = plan_with_observation(
            &store,
            &barrier,
            &mut capacity,
            5,
            1_000,
            ClockObservation::RollbackDetected,
        )
        .unwrap();
        assert_eq!(rollback.base.identities, store.active.base.identities);
        assert!(!capacity.status().poll_open());

        capacity.restore_trusted_clock();
        let mut tampered = retained;
        tampered.base.immutable_creation = b"different-creation".to_vec();
        tampered.base.checkpoint = Some(b"different-checkpoint".to_vec());
        tampered.planned_base_digest = digest(&tampered.base.encode().unwrap());
        assert_eq!(
            store.compact(tampered, &mut barrier, &mut capacity, CompactionFault::None,),
            Err(CompactionError::CompactionPlanMismatch)
        );

        let root = tempdir().unwrap();
        let mut store = initialized(root.path());
        let mut capacity = controller();
        capacity
            .try_admit_identity(message_id(0x41), CapacityFootprint::new(1, 32).unwrap())
            .unwrap();
        let barrier = prepared_barrier(&mut store, &mut capacity);
        assert_eq!(
            plan_with_observation(
                &store,
                &barrier,
                &mut capacity,
                5,
                1_000,
                ClockObservation::Trusted,
            )
            .unwrap_err(),
            CompactionError::GcDeferredByWaitingMutation
        );
        assert!(barrier.reserve_held());
    }

    #[test]
    fn v07_task_4_3_reducer_materializes_every_frozen_mutation_into_the_base() {
        let root = tempdir().unwrap();
        let mut store = initialized(root.path());
        let mut capacity = controller();
        let mut barrier = GenerationBarrier::new(1, frames(1)).unwrap();
        let second_id = message_id(0x42);
        let second_identity = canonical_identity(second_id, false, 5);
        barrier
            .register_mutation(
                2,
                CompactionMutation::UpsertIdentity {
                    message_id: second_id,
                    encoded: second_identity.clone(),
                },
            )
            .unwrap();
        barrier.acquire(&mut capacity).unwrap();
        store.commit_registered(&mut barrier, 2).unwrap();
        let identities = [
            CompactionIdentity::new(
                message_id(0x41),
                canonical_identity(message_id(0x41), true, 4),
            )
            .unwrap(),
            CompactionIdentity::new(second_id, second_identity.clone()).unwrap(),
        ];
        let plan = store
            .plan_compaction(
                &barrier,
                identities,
                4,
                1_000,
                ClockObservation::Trusted,
                &mut capacity,
            )
            .unwrap();
        assert_eq!(plan.base.identities.len(), 2);
        store
            .compact(plan, &mut barrier, &mut capacity, CompactionFault::None)
            .unwrap();
        assert_eq!(
            store.active.base.identities.get(&second_id).unwrap(),
            &second_identity
        );
        assert!(store.active.suffix().is_empty());
    }

    #[test]
    fn v07_task_4_3_barrier_drains_prefreeze_and_routes_waiter_to_suffix() {
        let root = tempdir().unwrap();
        let mut store = initialized(root.path());
        let mut capacity = controller();
        let mut barrier = prepared_barrier(&mut store, &mut capacity);
        let plan = retained_plan(&store, &barrier, &mut capacity);
        assert!(barrier.reserve_held());
        let CompactionOutcome::Committed(gc_proof) = store
            .compact(plan, &mut barrier, &mut capacity, CompactionFault::None)
            .unwrap()
        else {
            panic!("expected committed compaction")
        };
        assert_eq!(gc_proof.generation(), 2);
        assert!(!barrier.reserve_held());
        assert_eq!(store.active().generation(), 2);
        assert_eq!(
            store.active().suffix().get(&3).unwrap(),
            frames(3).get(&3).unwrap()
        );
        assert_eq!(
            store.active.base.checkpoint.as_deref(),
            Some(b"checkpoint-2".as_slice())
        );
        assert_eq!(
            store.active().frame_commitment().unwrap(),
            FrameCommitment::from_frames(&frames(3)).unwrap()
        );

        let mut barrier = GenerationBarrier::from_recovered(store.active()).unwrap();
        barrier
            .register_mutation(
                4,
                CompactionMutation::InstallCheckpoint(b"checkpoint-4".to_vec()),
            )
            .unwrap();
        barrier.acquire(&mut capacity).unwrap();
        barrier
            .register_mutation(
                5,
                CompactionMutation::InstallCheckpoint(b"checkpoint-5".to_vec()),
            )
            .unwrap();
        store.commit_registered(&mut barrier, 4).unwrap();
        let plan = retained_plan(&store, &barrier, &mut capacity);
        store
            .compact(plan, &mut barrier, &mut capacity, CompactionFault::None)
            .unwrap();
        assert_eq!(store.active().generation(), 3);
        assert_eq!(
            store.active.base.checkpoint.as_deref(),
            Some(b"checkpoint-4".as_slice())
        );
        assert_eq!(
            store.active().suffix().keys().copied().collect::<Vec<_>>(),
            [5]
        );
        assert_eq!(
            store.active().frame_commitment().unwrap(),
            FrameCommitment::from_frames(&frames(5)).unwrap()
        );
        assert_eq!(
            store.reclaim_generation(3),
            Err(CompactionError::CannotReclaimActiveGeneration)
        );
        store.reclaim_generation(1).unwrap();
        store.reclaim_generation(2).unwrap();
        assert!(!generation_directory(root.path(), 1).exists());
    }

    #[test]
    fn v07_task_4_3_every_cutover_fault_recovers_one_complete_generation() {
        for fault in [
            CompactionFault::AfterBaseWrite,
            CompactionFault::AfterBaseFileSync,
            CompactionFault::AfterSuffixWrite,
            CompactionFault::AfterSuffixFileSync,
            CompactionFault::AfterGenerationDirectorySync,
            CompactionFault::AfterRootWrite,
            CompactionFault::AfterRootFileSync,
            CompactionFault::AfterRootRename,
            CompactionFault::AfterRootDirectorySync,
        ] {
            let root = tempdir().unwrap();
            let mut store = initialized(root.path());
            let mut capacity = controller();
            let mut barrier = prepared_barrier(&mut store, &mut capacity);
            let plan = retained_plan(&store, &barrier, &mut capacity);
            let retry_plan = plan.clone();
            let outcome = store
                .compact(plan, &mut barrier, &mut capacity, fault)
                .unwrap();
            let recovered = CompactionStore::recover(root.path()).unwrap();
            let new_visible = matches!(
                fault,
                CompactionFault::AfterRootRename | CompactionFault::AfterRootDirectorySync
            );
            assert_eq!(
                outcome,
                if new_visible {
                    CompactionOutcome::Unknown
                } else {
                    CompactionOutcome::KeptOld
                }
            );
            assert_eq!(
                recovered.active().generation(),
                if new_visible { 2 } else { 1 }
            );
            assert_eq!(
                recovered.active().frame_commitment().unwrap(),
                FrameCommitment::from_frames(&frames(if new_visible { 3 } else { 2 })).unwrap()
            );
            assert!(barrier.reserve_held());
            assert_eq!(
                store.reclaim_generation(1),
                Err(CompactionError::CannotReclaimActiveGeneration)
            );
            assert_eq!(
                store.compact(
                    retry_plan,
                    &mut barrier,
                    &mut capacity,
                    CompactionFault::None,
                ),
                Err(CompactionError::RecoveryRequired)
            );
        }
    }

    #[test]
    fn v07_task_4_3_identity_removal_is_visible_only_with_durable_new_root() {
        for fault in [
            CompactionFault::AfterBaseWrite,
            CompactionFault::AfterBaseFileSync,
            CompactionFault::AfterSuffixWrite,
            CompactionFault::AfterSuffixFileSync,
            CompactionFault::AfterGenerationDirectorySync,
            CompactionFault::AfterRootWrite,
            CompactionFault::AfterRootFileSync,
            CompactionFault::AfterRootRename,
            CompactionFault::AfterRootDirectorySync,
        ] {
            let root = tempdir().unwrap();
            let mut store = initialized(root.path());
            let mut capacity = controller();
            capacity
                .try_admit_identity(message_id(0x41), CapacityFootprint::new(1, 32).unwrap())
                .unwrap();
            let mut barrier = GenerationBarrier::from_recovered(store.active()).unwrap();
            barrier.acquire(&mut capacity).unwrap();
            let plan = plan_with_observation(
                &store,
                &barrier,
                &mut capacity,
                5,
                1_000,
                ClockObservation::Trusted,
            )
            .unwrap();
            store
                .compact(plan, &mut barrier, &mut capacity, fault)
                .unwrap();
            let recovered = CompactionStore::recover(root.path()).unwrap();
            let new_visible = matches!(
                fault,
                CompactionFault::AfterRootRename | CompactionFault::AfterRootDirectorySync
            );
            assert_eq!(recovered.active().base.identities.is_empty(), new_visible);
        }

        let root = tempdir().unwrap();
        let mut store = initialized(root.path());
        let mut capacity = controller();
        let identity_token = capacity
            .try_admit_identity(message_id(0x41), CapacityFootprint::new(1, 32).unwrap())
            .unwrap();
        let mut barrier = GenerationBarrier::from_recovered(store.active()).unwrap();
        barrier.acquire(&mut capacity).unwrap();
        let plan = plan_with_observation(
            &store,
            &barrier,
            &mut capacity,
            5,
            1_000,
            ClockObservation::Trusted,
        )
        .unwrap();
        let CompactionOutcome::Committed(proof) = store
            .compact(plan, &mut barrier, &mut capacity, CompactionFault::None)
            .unwrap()
        else {
            panic!("expected durable GC commit")
        };
        assert!(proof.contains(message_id(0x41)));
        capacity
            .release_identity_after_gc(identity_token, message_id(0x41), &proof)
            .unwrap();
    }

    #[test]
    fn v07_task_4_3_mixed_generation_and_suffix_gap_fail_stop() {
        let root = tempdir().unwrap();
        let store = initialized(root.path());
        let suffix_path = generation_directory(root.path(), 1).join(SUFFIX_FILE);
        let before = fs::read(&suffix_path).unwrap();
        let frame = encode_suffix_frame(2, b"uncommitted-tail").unwrap();
        let mut file = OpenOptions::new().append(true).open(&suffix_path).unwrap();
        file.write_all(&frame[..frame.len() / 2]).unwrap();
        drop(file);
        let recovered = CompactionStore::recover(root.path()).unwrap();
        assert_eq!(
            recovered.active().frame_commitment().unwrap(),
            base(1, 1, &frames(1)).frame_commitment()
        );
        assert_eq!(fs::read(&suffix_path).unwrap(), before);
        drop(store);

        let root = tempdir().unwrap();
        let _store = initialized(root.path());
        let suffix_path = generation_directory(root.path(), 1).join(SUFFIX_FILE);
        let opaque_frame = encode_suffix_frame(2, b"opaque-unreducible-state").unwrap();
        let mut file = OpenOptions::new().append(true).open(&suffix_path).unwrap();
        file.write_all(&opaque_frame).unwrap();
        drop(file);
        assert!(matches!(
            CompactionStore::recover(root.path()),
            Err(CompactionError::Frame(_))
        ));

        let root = tempdir().unwrap();
        let _store = initialized(root.path());
        let suffix_path = generation_directory(root.path(), 1).join(SUFFIX_FILE);
        let mut corrupt_frame = encode_suffix_frame(2, b"corrupt-complete-frame").unwrap();
        let last = corrupt_frame.len() - 1;
        corrupt_frame[last] ^= 1;
        let mut file = OpenOptions::new().append(true).open(&suffix_path).unwrap();
        file.write_all(&corrupt_frame).unwrap();
        drop(file);
        assert!(matches!(
            CompactionStore::recover(root.path()),
            Err(CompactionError::Frame(StateFrameError::ChecksumMismatch))
        ));

        let root = tempdir().unwrap();
        let _store = initialized(root.path());
        let root_path = root.path().join(ROOT_FILE);
        let mut corrupt_root = fs::read(&root_path).unwrap();
        let last = corrupt_root.len() - 1;
        corrupt_root[last] ^= 1;
        fs::write(&root_path, corrupt_root).unwrap();
        assert!(matches!(
            CompactionStore::recover(root.path()),
            Err(CompactionError::Frame(StateFrameError::ChecksumMismatch))
        ));

        let root = tempdir().unwrap();
        let mut store = initialized(root.path());
        let mut capacity = controller();
        let mut barrier = prepared_barrier(&mut store, &mut capacity);
        let plan = retained_plan(&store, &barrier, &mut capacity);
        store
            .compact(plan, &mut barrier, &mut capacity, CompactionFault::None)
            .unwrap();
        let generation_two = generation_directory(root.path(), 2);
        fs::copy(
            generation_directory(root.path(), 1).join(BASE_FILE),
            generation_two.join(BASE_FILE),
        )
        .unwrap();
        assert_eq!(
            CompactionStore::recover(root.path()).unwrap_err(),
            CompactionError::MixedGeneration
        );

        let root = tempdir().unwrap();
        let mut store = initialized(root.path());
        let mut capacity = controller();
        let mut barrier = prepared_barrier(&mut store, &mut capacity);
        let plan = retained_plan(&store, &barrier, &mut capacity);
        store
            .compact(plan, &mut barrier, &mut capacity, CompactionFault::None)
            .unwrap();
        let next_base = store.active.base.clone();
        let gap_suffix = encode_suffix(
            2,
            2,
            &BTreeMap::from([(
                4,
                CompactionMutation::InstallCheckpoint(b"checkpoint-4".to_vec())
                    .encode(4)
                    .unwrap(),
            )]),
        )
        .unwrap();
        fs::write(
            generation_directory(root.path(), 2).join(SUFFIX_FILE),
            &gap_suffix,
        )
        .unwrap();
        fs::write(
            root.path().join(ROOT_FILE),
            encode_root(&next_base, &gap_suffix).unwrap(),
        )
        .unwrap();
        assert_eq!(
            CompactionStore::recover(root.path()).unwrap_err(),
            CompactionError::SequenceConflict
        );
    }

    #[test]
    fn v07_task_4_3_provisional_corpus_is_complete_bound_and_repeatable() {
        let requirements = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../../.spec-workflow/specs/chirps-v0-7-durable-backend/requirements.md"
        ));
        let design = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../../.spec-workflow/specs/chirps-v0-7-durable-backend/design.md"
        ));
        let sources: &[(&str, &[u8])] = &[
            ("state/capacity.rs", include_bytes!("capacity.rs")),
            ("state/compaction.rs", include_bytes!("compaction.rs")),
            ("state/mod.rs", include_bytes!("mod.rs")),
        ];
        let inputs = CompactionCorpusInputs {
            requirements,
            design,
            sources,
        };
        let output = PathBuf::from(std::env::var("CARGO_TARGET_DIR").unwrap())
            .join("task-4_3-provisional-corpus");
        generate_compaction_corpus(&output, &inputs).unwrap();
        verify_compaction_corpus(&output, &inputs).unwrap();
        let first = corpus_inventory(&output)
            .unwrap()
            .into_iter()
            .map(|path| {
                let bytes = fs::read(output.join(&path)).unwrap();
                (path, bytes)
            })
            .collect::<BTreeMap<_, _>>();
        generate_compaction_corpus(&output, &inputs).unwrap();
        verify_compaction_corpus(&output, &inputs).unwrap();
        let second = corpus_inventory(&output)
            .unwrap()
            .into_iter()
            .map(|path| {
                let bytes = fs::read(output.join(&path)).unwrap();
                (path, bytes)
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(first, second);
        assert_eq!(CORPUS_CASES.len(), 12);
        let manifest = fs::read_to_string(output.join("manifest.json")).unwrap();
        assert!(manifest.contains("\"producer_task\": \"4.3\""));
        assert!(manifest.contains("root-directory-sync-unknown-old"));
        assert!(manifest.contains("root-directory-sync-unknown-new"));
        assert!(manifest.contains(ROOT_PENDING_FILE));
        assert!(
            manifest.contains("6fc671aed8f10a7c664ad27d2d6342a57f228375f944942f877b4f2be166aec6")
        );
        assert!(
            manifest.contains("797fe316e18c7145fb3aa1243afc1279df5942d57219ede034c85caf89be7e71")
        );
    }
}
