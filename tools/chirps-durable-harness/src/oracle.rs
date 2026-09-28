//! Independent, append-only Durable fault-oracle records.

use alopex_chirps_core::durable::{
    AttemptBinding, AttemptFailureKind, AttemptPhase, CheckpointOutcome, ConfirmationBoundary,
    DurableSendOutcome, DurableSendResult, EnvelopeDigest, PayloadDigest, PreparedDurableSend,
    ResourceEpoch, ResourceId, SessionFingerprint,
};
use fs2::FileExt;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::ffi::OsString;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};

const RECORD_MAGIC: [u8; 4] = *b"CDOR";
const RECORD_VERSION: u16 = 5;
const RECORD_HEADER_LEN: usize = 27;
const HEAD_MAGIC: [u8; 4] = *b"CDOH";
const HEAD_LEN: usize = 22;
const CANONICAL_CODEC_VERSION: u16 = 1;
const MAX_CANONICAL_ENVELOPE_LEN: usize = 64_000_000;
const MAX_ORDERING_KEY_LEN: usize = 255;
const CANONICAL_FIXED_LEN: usize = 2 + 16 + 16 + 16 + 8 + 4 + 4 + 8 + 32 + 32;
const MAX_CANONICAL_PAYLOAD_LEN: usize = MAX_CANONICAL_ENVELOPE_LEN - CANONICAL_FIXED_LEN;
const ENVELOPE_DOMAIN: &[u8] = b"ALOPEX-CHIRPS-DURABLE-ENVELOPE\0";

/// Frozen Task 5.1 private command code for `AppendOneSynced`.
pub const APPEND_ONE_SYNCED_CODE: u32 = 0x8000_0703;

/// Durable evidence representation of a core-generated message identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OracleMessageId([u8; 16]);

impl OracleMessageId {
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 16] {
        self.0
    }
}

/// Durable evidence representation of a core-generated attempt identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OracleAttemptId([u8; 16]);

impl OracleAttemptId {
    #[must_use]
    pub const fn as_bytes(self) -> [u8; 16] {
        self.0
    }
}

/// Intent that must reach stable storage before the wrapped append is invoked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleIntent {
    message_id: OracleMessageId,
    attempt_id: OracleAttemptId,
    envelope_digest: EnvelopeDigest,
    codec_version: u16,
    source: [u8; 16],
    target: [u8; 16],
    generation: u64,
    partition: u32,
    ordering_key: Vec<u8>,
    routing_map_version: u32,
    payload: Vec<u8>,
    payload_digest: PayloadDigest,
    requested_boundary: ConfirmationBoundary,
    session_fingerprint: SessionFingerprint,
}

impl OracleIntent {
    /// Captures an exact core attempt before it crosses the append boundary.
    pub fn from_started_attempt(
        prepared: &PreparedDurableSend,
        binding: &AttemptBinding,
    ) -> Result<Self, OracleError> {
        if binding.phase() != AttemptPhase::Started {
            return Err(OracleError::AttemptAlreadyInvoked);
        }
        DurableSendResult::not_submitted(prepared, binding.clone(), AttemptFailureKind::Protocol)
            .map_err(|_| OracleError::AttemptBindingMismatch)?;
        let attempt_id = binding.attempt_id().ok_or(OracleError::AttemptNotStarted)?;
        let requested_boundary = binding
            .requested_boundary()
            .ok_or(OracleError::AttemptNotStarted)?;
        let session_fingerprint = binding
            .session_fingerprint()
            .ok_or(OracleError::AttemptNotStarted)?;
        let decoded = decode_canonical_envelope(prepared.canonical_bytes())
            .map_err(|_| OracleError::InvalidRecord("prepared canonical envelope"))?;
        if decoded.message_id != *prepared.message_id().as_bytes()
            || decoded.codec_version != prepared.codec_version()
            || decoded.source != *prepared.source().as_bytes()
            || decoded.target != *prepared.target().as_bytes()
            || decoded.generation != prepared.generation()
            || decoded.partition != prepared.partition()
            || decoded.ordering_key != prepared.ordering_key()
            || decoded.payload_digest != prepared.payload_digest()
            || decoded.envelope_digest != prepared.envelope_digest()
        {
            return Err(OracleError::InvalidRecord(
                "prepared fields differ from canonical envelope",
            ));
        }
        Ok(Self {
            message_id: OracleMessageId(*prepared.message_id().as_bytes()),
            attempt_id: OracleAttemptId(*attempt_id.as_bytes()),
            envelope_digest: prepared.envelope_digest(),
            codec_version: prepared.codec_version(),
            source: *prepared.source().as_bytes(),
            target: *prepared.target().as_bytes(),
            generation: prepared.generation(),
            partition: prepared.partition(),
            ordering_key: prepared.ordering_key().to_vec(),
            routing_map_version: prepared.routing_map_version(),
            payload: decoded.payload,
            payload_digest: prepared.payload_digest(),
            requested_boundary,
            session_fingerprint,
        })
    }

    #[must_use]
    pub const fn message_id(&self) -> OracleMessageId {
        self.message_id
    }

    #[must_use]
    pub const fn attempt_id(&self) -> OracleAttemptId {
        self.attempt_id
    }

    #[must_use]
    pub const fn envelope_digest(&self) -> EnvelopeDigest {
        self.envelope_digest
    }

    #[must_use]
    pub const fn codec_version(&self) -> u16 {
        self.codec_version
    }

    #[must_use]
    pub const fn source(&self) -> [u8; 16] {
        self.source
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
    pub fn ordering_key(&self) -> &[u8] {
        &self.ordering_key
    }

    #[must_use]
    pub const fn routing_map_version(&self) -> u32 {
        self.routing_map_version
    }

    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    #[must_use]
    pub const fn payload_digest(&self) -> PayloadDigest {
        self.payload_digest
    }

    #[must_use]
    pub const fn requested_boundary(&self) -> ConfirmationBoundary {
        self.requested_boundary
    }

    #[must_use]
    pub const fn session_fingerprint(&self) -> SessionFingerprint {
        self.session_fingerprint
    }
}

/// Exact broker record location observed independently of production state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ExactLocation {
    resource_epoch: ResourceEpoch,
    partition: u32,
    offset: u64,
    index: u64,
}

impl ExactLocation {
    #[must_use]
    pub const fn new(
        resource_epoch: ResourceEpoch,
        partition: u32,
        offset: u64,
        index: u64,
    ) -> Self {
        Self {
            resource_epoch,
            partition,
            offset,
            index,
        }
    }

    #[must_use]
    pub const fn resource_epoch(self) -> ResourceEpoch {
        self.resource_epoch
    }

    #[must_use]
    pub const fn partition(self) -> u32 {
        self.partition
    }

    #[must_use]
    pub const fn offset(self) -> u64 {
        self.offset
    }

    #[must_use]
    pub const fn index(self) -> u64 {
        self.index
    }
}

/// Physical append stages observed by the independent proxy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireStage {
    AppendInvocation,
    WireWrite,
    JournalFlush,
    MessageSync,
    IndexSync,
    Response,
}

/// Counts for one command at one physical stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireObservation {
    command_code: u32,
    stage: WireStage,
    wire_count: u32,
    command_count: u32,
    record_count: u32,
    payload_count: u32,
}

impl WireObservation {
    #[must_use]
    pub const fn new(
        command_code: u32,
        stage: WireStage,
        wire_count: u32,
        command_count: u32,
        record_count: u32,
        payload_count: u32,
    ) -> Self {
        Self {
            command_code,
            stage,
            wire_count,
            command_count,
            record_count,
            payload_count,
        }
    }
}

/// Independently decoded stored envelope plus physical sync evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredReadback {
    canonical_envelope: Vec<u8>,
    location: ExactLocation,
    message_synced: bool,
    index_synced: bool,
}

impl StoredReadback {
    #[must_use]
    pub fn new(
        canonical_envelope: Vec<u8>,
        location: ExactLocation,
        message_synced: bool,
        index_synced: bool,
    ) -> Self {
        Self {
            canonical_envelope,
            location,
            message_synced,
            index_synced,
        }
    }

    #[must_use]
    pub fn canonical_envelope(&self) -> &[u8] {
        &self.canonical_envelope
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedCanonicalEnvelope {
    codec_version: u16,
    message_id: [u8; 16],
    source: [u8; 16],
    target: [u8; 16],
    generation: u64,
    partition: u32,
    ordering_key: Vec<u8>,
    payload: Vec<u8>,
    payload_digest: PayloadDigest,
    envelope_digest: EnvelopeDigest,
}

impl DecodedCanonicalEnvelope {
    fn matches_intent(&self, intent: &OracleIntent) -> bool {
        self.codec_version == intent.codec_version
            && self.message_id == intent.message_id.0
            && self.source == intent.source
            && self.target == intent.target
            && self.generation == intent.generation
            && self.partition == intent.partition
            && self.ordering_key == intent.ordering_key
            && self.payload == intent.payload
            && self.payload_digest == intent.payload_digest
            && self.envelope_digest == intent.envelope_digest
    }
}

fn decode_canonical_envelope(bytes: &[u8]) -> Result<DecodedCanonicalEnvelope, OracleError> {
    if bytes.len() > MAX_CANONICAL_ENVELOPE_LEN {
        return Err(OracleError::InvalidRecord("canonical envelope too large"));
    }
    let mut cursor = Cursor::new(bytes);
    let codec_version = cursor.u16()?;
    if codec_version != CANONICAL_CODEC_VERSION {
        return Err(OracleError::InvalidRecord("canonical codec version"));
    }
    let message_id = cursor.array()?;
    if message_id[6] & 0xf0 != 0x40 || message_id[8] & 0xc0 != 0x80 {
        return Err(OracleError::InvalidRecord("canonical message id"));
    }
    let source = cursor.array()?;
    let target = cursor.array()?;
    let generation = cursor.u64()?;
    let partition = cursor.u32()?;
    let ordering_key_len = cursor.u32()? as usize;
    if ordering_key_len > MAX_ORDERING_KEY_LEN {
        return Err(OracleError::InvalidRecord(
            "canonical ordering key too large",
        ));
    }
    let ordering_key = cursor.take(ordering_key_len)?.to_vec();
    let payload_len_u64 = cursor.u64()?;
    if payload_len_u64 > MAX_CANONICAL_PAYLOAD_LEN as u64 {
        return Err(OracleError::InvalidRecord("canonical payload too large"));
    }
    let payload_len = usize::try_from(payload_len_u64)
        .map_err(|_| OracleError::InvalidRecord("canonical payload length"))?;
    let payload = cursor.take(payload_len)?.to_vec();
    let claimed_payload_digest: [u8; 32] = cursor.array()?;
    let claimed_envelope_digest: [u8; 32] = cursor.array()?;
    cursor.finish()?;

    let recomputed_payload_digest: [u8; 32] = Sha256::digest(&payload).into();
    if claimed_payload_digest != recomputed_payload_digest {
        return Err(OracleError::InvalidRecord("canonical payload digest"));
    }
    let mut hasher = Sha256::new();
    hasher.update(ENVELOPE_DOMAIN);
    hasher.update(codec_version.to_be_bytes());
    hasher.update(message_id);
    hasher.update(source);
    hasher.update(target);
    hasher.update(generation.to_be_bytes());
    hasher.update(partition.to_be_bytes());
    hasher.update((ordering_key_len as u32).to_be_bytes());
    hasher.update(&ordering_key);
    hasher.update(payload_len_u64.to_be_bytes());
    hasher.update(&payload);
    let recomputed_envelope_digest: [u8; 32] = hasher.finalize().into();
    if claimed_envelope_digest != recomputed_envelope_digest {
        return Err(OracleError::InvalidRecord("canonical envelope digest"));
    }

    Ok(DecodedCanonicalEnvelope {
        codec_version,
        message_id,
        source,
        target,
        generation,
        partition,
        ordering_key,
        payload,
        payload_digest: PayloadDigest::from_bytes(claimed_payload_digest),
        envelope_digest: EnvelopeDigest::from_bytes(claimed_envelope_digest),
    })
}

/// Exact strong receipt fields captured from the authenticated command trace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiptObservation {
    message_id: OracleMessageId,
    envelope_digest: EnvelopeDigest,
    attempt_id: OracleAttemptId,
    boundary: ConfirmationBoundary,
    session_fingerprint: SessionFingerprint,
    location: ExactLocation,
}

impl ReceiptObservation {
    #[must_use]
    pub const fn new(
        message_id: OracleMessageId,
        envelope_digest: EnvelopeDigest,
        attempt_id: OracleAttemptId,
        boundary: ConfirmationBoundary,
        session_fingerprint: SessionFingerprint,
        location: ExactLocation,
    ) -> Self {
        Self {
            message_id,
            envelope_digest,
            attempt_id,
            boundary,
            session_fingerprint,
            location,
        }
    }
}

/// Public send outcome and its optional exact receipt observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseObservation {
    outcome: DurableSendOutcome,
    receipt: Option<ReceiptObservation>,
}

impl ResponseObservation {
    #[must_use]
    pub const fn new(outcome: DurableSendOutcome, receipt: Option<ReceiptObservation>) -> Self {
        Self { outcome, receipt }
    }
}

/// Independently observed delivery or redelivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryObservation {
    message_id: OracleMessageId,
    envelope_digest: EnvelopeDigest,
    location: ExactLocation,
    redelivery: bool,
}

impl DeliveryObservation {
    #[must_use]
    pub const fn new(
        message_id: OracleMessageId,
        envelope_digest: EnvelopeDigest,
        location: ExactLocation,
        redelivery: bool,
    ) -> Self {
        Self {
            message_id,
            envelope_digest,
            location,
            redelivery,
        }
    }
}

/// Independently observed application effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EffectObservation {
    message_id: OracleMessageId,
    envelope_digest: EnvelopeDigest,
    location: ExactLocation,
    applied: bool,
}

/// Application acknowledgement observed for one exact delivered record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckObservation {
    message_id: OracleMessageId,
    envelope_digest: EnvelopeDigest,
    location: ExactLocation,
    accepted: bool,
}

impl AckObservation {
    #[must_use]
    pub const fn new(
        message_id: OracleMessageId,
        envelope_digest: EnvelopeDigest,
        location: ExactLocation,
        accepted: bool,
    ) -> Self {
        Self {
            message_id,
            envelope_digest,
            location,
            accepted,
        }
    }
}

impl EffectObservation {
    #[must_use]
    pub const fn new(
        message_id: OracleMessageId,
        envelope_digest: EnvelopeDigest,
        location: ExactLocation,
        applied: bool,
    ) -> Self {
        Self {
            message_id,
            envelope_digest,
            location,
            applied,
        }
    }
}

/// Old/candidate/restart checkpoint evidence for one installation result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointObservation {
    message_id: OracleMessageId,
    envelope_digest: EnvelopeDigest,
    location: ExactLocation,
    previous_offset: Option<u64>,
    recovered_offset: Option<u64>,
    outcome: CheckpointOutcome,
}

impl CheckpointObservation {
    #[must_use]
    pub const fn new(
        message_id: OracleMessageId,
        envelope_digest: EnvelopeDigest,
        location: ExactLocation,
        previous_offset: Option<u64>,
        recovered_offset: Option<u64>,
        outcome: CheckpointOutcome,
    ) -> Self {
        Self {
            message_id,
            envelope_digest,
            location,
            previous_offset,
            recovered_offset,
            outcome,
        }
    }
}

/// Restart recovery table required for delivery/checkpoint evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryState {
    Undelivered,
    DeliveredUncheckpointed,
    CheckpointOld,
    CheckpointNew,
    Committed,
    Redelivered,
    PresentAfterRestart,
    EvictedAfterSync,
}

/// Fresh-process recovery evidence for an exact record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryObservation {
    state: RecoveryState,
    location: Option<ExactLocation>,
    oldest_after_restart: Option<u64>,
}

impl RecoveryObservation {
    #[must_use]
    pub const fn new(
        state: RecoveryState,
        location: Option<ExactLocation>,
        oldest_after_restart: Option<u64>,
    ) -> Self {
        Self {
            state,
            location,
            oldest_after_restart,
        }
    }

    #[must_use]
    pub const fn evicted_after_sync(location: ExactLocation, oldest_after_restart: u64) -> Self {
        Self::new(
            RecoveryState::EvictedAfterSync,
            Some(location),
            Some(oldest_after_restart),
        )
    }
}

/// Creation recovery classification observed independently after restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreationState {
    CreationNotCommitted,
    CreationUnknown,
    Created,
}

/// Actual durable manifest fields read after creation recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreationManifestObservation {
    namespace_digest: [u8; 32],
    resolved_initial_offset: u64,
    owner_epoch: u64,
}

impl CreationManifestObservation {
    #[must_use]
    pub const fn new(
        namespace_digest: [u8; 32],
        resolved_initial_offset: u64,
        owner_epoch: u64,
    ) -> Self {
        Self {
            namespace_digest,
            resolved_initial_offset,
            owner_epoch,
        }
    }
}

/// Expected namespace plus actual manifest/owner recovery evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreationObservation {
    state: CreationState,
    expected_namespace_digest: [u8; 32],
    expected_resolved_initial_offset: u64,
    manifest: Option<CreationManifestObservation>,
    owner_epoch_before: Option<u64>,
    owner_epoch_after: Option<u64>,
}

impl CreationObservation {
    #[must_use]
    pub const fn new(
        state: CreationState,
        expected_namespace_digest: [u8; 32],
        expected_resolved_initial_offset: u64,
        manifest: Option<CreationManifestObservation>,
        owner_epoch_before: Option<u64>,
        owner_epoch_after: Option<u64>,
    ) -> Self {
        Self {
            state,
            expected_namespace_digest,
            expected_resolved_initial_offset,
            manifest,
            owner_epoch_before,
            owner_epoch_after,
        }
    }
}

/// One append-only observation correlated to one exact attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleObservation {
    attempt_id: OracleAttemptId,
    kind: ObservationKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObservationKind {
    Wire(WireObservation),
    StoredReadback(StoredReadback),
    Response(ResponseObservation),
    Delivery(DeliveryObservation),
    Effect(EffectObservation),
    Ack(AckObservation),
    Checkpoint(CheckpointObservation),
    ResourceEpoch(ResourceEpoch),
    Recovery(RecoveryObservation),
    Creation(CreationObservation),
}

impl OracleObservation {
    const fn new(attempt_id: OracleAttemptId, kind: ObservationKind) -> Self {
        Self { attempt_id, kind }
    }

    #[must_use]
    pub const fn wire(attempt_id: OracleAttemptId, value: WireObservation) -> Self {
        Self::new(attempt_id, ObservationKind::Wire(value))
    }

    #[must_use]
    pub const fn stored_readback(attempt_id: OracleAttemptId, value: StoredReadback) -> Self {
        Self::new(attempt_id, ObservationKind::StoredReadback(value))
    }

    #[must_use]
    pub const fn response(attempt_id: OracleAttemptId, value: ResponseObservation) -> Self {
        Self::new(attempt_id, ObservationKind::Response(value))
    }

    #[must_use]
    pub const fn delivery(attempt_id: OracleAttemptId, value: DeliveryObservation) -> Self {
        Self::new(attempt_id, ObservationKind::Delivery(value))
    }

    #[must_use]
    pub const fn effect(attempt_id: OracleAttemptId, value: EffectObservation) -> Self {
        Self::new(attempt_id, ObservationKind::Effect(value))
    }

    #[must_use]
    pub const fn ack(attempt_id: OracleAttemptId, value: AckObservation) -> Self {
        Self::new(attempt_id, ObservationKind::Ack(value))
    }

    #[must_use]
    pub const fn checkpoint(attempt_id: OracleAttemptId, value: CheckpointObservation) -> Self {
        Self::new(attempt_id, ObservationKind::Checkpoint(value))
    }

    #[must_use]
    pub const fn resource_epoch(attempt_id: OracleAttemptId, value: ResourceEpoch) -> Self {
        Self::new(attempt_id, ObservationKind::ResourceEpoch(value))
    }

    #[must_use]
    pub const fn recovery(attempt_id: OracleAttemptId, value: RecoveryObservation) -> Self {
        Self::new(attempt_id, ObservationKind::Recovery(value))
    }

    #[must_use]
    pub const fn creation(attempt_id: OracleAttemptId, value: CreationObservation) -> Self {
        Self::new(attempt_id, ObservationKind::Creation(value))
    }

    #[must_use]
    pub const fn attempt_id(&self) -> OracleAttemptId {
        self.attempt_id
    }

    #[must_use]
    pub const fn kind(&self) -> &ObservationKind {
        &self.kind
    }
}

/// One persisted append-only oracle record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OracleRecord {
    Intent(OracleIntent),
    Observation(OracleObservation),
}

/// Small exact verdict used by later process-level fixtures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OracleVerdict {
    append_count: u32,
    exact_record_proven: bool,
}

impl OracleVerdict {
    #[must_use]
    pub const fn append_count(self) -> u32 {
        self.append_count
    }

    #[must_use]
    pub const fn exact_record_proven(self) -> bool {
        self.exact_record_proven
    }
}

/// Rejects contradictory evidence without consulting production state.
pub fn verify_attempt(
    intent: &OracleIntent,
    observations: &[OracleObservation],
) -> Result<OracleVerdict, OracleViolation> {
    let mut append_count = 0_u32;
    let mut seen_wire_stages = [false; 6];
    let mut next_wire_stage = 0;
    let mut wire_counts = None;
    let mut stored = None;
    let mut response = None;
    let mut receipt = None;
    let mut response_observed = false;
    let mut deliveries = Vec::new();
    let mut effect = None;
    let mut effect_before_delivery = false;
    let mut ack = None;
    let mut checkpoint_frontier = None;
    let mut checkpoint_observed = false;
    let mut checkpoint_evidence = None;
    let mut checkpoint_location = None;
    let mut resource_epoch = None;
    let mut recoveries = Vec::new();
    let mut seen_recovery_states = [false; 8];
    let mut creation_history = None;
    let mut append_invoked = false;

    for observation in observations {
        if observation.attempt_id != intent.attempt_id {
            return Err(OracleViolation::AttemptCorrelationMismatch);
        }
        match &observation.kind {
            ObservationKind::Wire(value) => {
                let value = *value;
                if response_observed {
                    return Err(OracleViolation::WireAfterResponse);
                }
                if value.command_code != APPEND_ONE_SYNCED_CODE {
                    return Err(OracleViolation::WrongCommandCode);
                }
                if value.wire_count > 1
                    || value.command_count > 1
                    || value.record_count > 1
                    || value.payload_count > 1
                {
                    return Err(OracleViolation::MultipleAppends);
                }
                if value.wire_count != value.command_count
                    || value.command_count != value.record_count
                    || value.record_count != value.payload_count
                {
                    return Err(OracleViolation::InconsistentAppendCounts);
                }
                let stage = wire_stage_index(value.stage);
                if seen_wire_stages[stage] {
                    return Err(OracleViolation::DuplicateWireStage);
                }
                if stage < next_wire_stage {
                    return Err(OracleViolation::WireStageRegression);
                }
                if stage > next_wire_stage {
                    return Err(OracleViolation::MissingWireStage);
                }
                let counts = [
                    value.wire_count,
                    value.command_count,
                    value.record_count,
                    value.payload_count,
                ];
                if wire_counts.is_some_and(|expected| expected != counts) {
                    return Err(OracleViolation::WireCountsChanged);
                }
                if counts == [0, 0, 0, 0] {
                    return Err(OracleViolation::ZeroWireCounts);
                }
                wire_counts = Some(counts);
                seen_wire_stages[stage] = true;
                next_wire_stage += 1;
                if value.stage == WireStage::AppendInvocation {
                    append_count = value.command_count;
                    append_invoked = true;
                }
            }
            ObservationKind::StoredReadback(value) => {
                require_append_invocation(append_invoked)?;
                if value.index_synced && !value.message_synced {
                    return Err(OracleViolation::InvalidStoredSyncEvidence);
                }
                if (value.message_synced
                    && !seen_wire_stages[wire_stage_index(WireStage::MessageSync)])
                    || (value.index_synced
                        && !seen_wire_stages[wire_stage_index(WireStage::IndexSync)])
                {
                    return Err(OracleViolation::StoredReadbackBeforeSync);
                }
                if stored.replace(value).is_some() {
                    return Err(OracleViolation::MultipleStoredRecords);
                }
                let decoded = decode_canonical_envelope(&value.canonical_envelope)
                    .map_err(|_| OracleViolation::StoredEnvelopeMismatch)?;
                if !decoded.matches_intent(intent) || value.location.partition != intent.partition {
                    return Err(OracleViolation::StoredEnvelopeMismatch);
                }
            }
            ObservationKind::Response(value) => {
                let value = *value;
                if response.replace(value).is_some() {
                    return Err(OracleViolation::MultipleResponses);
                }
                if value.outcome == DurableSendOutcome::OsSyncedAccepted
                    && let Some(value) = value.receipt
                {
                    verify_receipt(intent, value)?;
                    receipt = Some(value);
                }
                response_observed = true;
            }
            ObservationKind::Delivery(value) => {
                require_append_invocation(append_invoked)?;
                let value = *value;
                if value.message_id != intent.message_id
                    || value.envelope_digest != intent.envelope_digest
                    || value.location.partition != intent.partition
                {
                    return Err(OracleViolation::DeliveryMismatch);
                }
                if value.redelivery
                    && matches!(
                        checkpoint_evidence,
                        Some(CheckpointEvidence::New | CheckpointEvidence::Committed)
                    )
                {
                    return Err(OracleViolation::InvalidRecoveryEvidence);
                }
                if deliveries.is_empty() == value.redelivery {
                    return Err(OracleViolation::InvalidDeliverySequence);
                }
                deliveries.push(value);
            }
            ObservationKind::Effect(value) => {
                require_append_invocation(append_invoked)?;
                let value = *value;
                if value.message_id != intent.message_id
                    || value.envelope_digest != intent.envelope_digest
                    || value.location.partition != intent.partition
                {
                    return Err(OracleViolation::EffectMismatch);
                }
                effect_before_delivery = !deliveries
                    .iter()
                    .any(|delivery| delivery.location == value.location);
                if effect.replace(value).is_some() {
                    return Err(OracleViolation::EffectMismatch);
                }
            }
            ObservationKind::Ack(value) => {
                require_append_invocation(append_invoked)?;
                let value = *value;
                if value.message_id != intent.message_id
                    || value.envelope_digest != intent.envelope_digest
                    || value.location.partition != intent.partition
                    || ack.replace(value).is_some()
                {
                    return Err(OracleViolation::AckMismatch);
                }
                if !deliveries
                    .iter()
                    .any(|delivery| delivery.location == value.location)
                {
                    return Err(OracleViolation::AckWithoutDelivery);
                }
            }
            ObservationKind::Checkpoint(value) => {
                require_append_invocation(append_invoked)?;
                let value = *value;
                if value.message_id != intent.message_id
                    || value.envelope_digest != intent.envelope_digest
                    || value.location.partition != intent.partition
                    || checkpoint_location.is_some_and(|location| location != value.location)
                {
                    return Err(OracleViolation::CheckpointRecordMismatch);
                }
                match ack {
                    Some(ack) if ack.accepted && ack.location == value.location => {}
                    Some(ack) if ack.accepted => {
                        return Err(OracleViolation::CheckpointRecordMismatch);
                    }
                    _ => return Err(OracleViolation::CheckpointWithoutAck),
                }
                let evidence =
                    verify_checkpoint(value, &mut checkpoint_frontier, checkpoint_evidence)?;
                checkpoint_observed = true;
                checkpoint_evidence = Some(evidence);
                checkpoint_location = Some(value.location);
            }
            ObservationKind::ResourceEpoch(value) => {
                if resource_epoch.replace(*value).is_some() {
                    return Err(OracleViolation::ResourceEpochMismatch);
                }
            }
            ObservationKind::Recovery(value) => {
                require_append_invocation(append_invoked)?;
                let value = *value;
                let state = recovery_state_index(value.state);
                if seen_recovery_states[state] {
                    return Err(OracleViolation::InvalidRecoveryEvidence);
                }
                seen_recovery_states[state] = true;
                verify_recovery(
                    value,
                    stored,
                    &deliveries,
                    effect,
                    ack,
                    checkpoint_evidence,
                    checkpoint_location,
                    stored.is_some_and(|value| value.message_synced && value.index_synced),
                    receipt,
                )?;
                recoveries.push(value);
            }
            ObservationKind::Creation(value) => {
                verify_creation(*value, &mut creation_history)?;
            }
        }
    }

    if let (Some(stored), Some(resource_epoch)) = (stored, resource_epoch)
        && resource_epoch != stored.location.resource_epoch
    {
        return Err(OracleViolation::ResourceEpochMismatch);
    }

    let response = response.ok_or(OracleViolation::MissingResponse)?;
    let material_observed = stored.is_some()
        || !deliveries.is_empty()
        || effect.is_some_and(|value| value.applied)
        || ack.is_some_and(|value| value.accepted)
        || checkpoint_observed
        || recoveries
            .iter()
            .any(|value| value.state != RecoveryState::Undelivered);
    match response.outcome {
        DurableSendOutcome::NotSubmitted(_) => {
            if append_count != 0 {
                return Err(OracleViolation::NotSubmittedAfterAppend);
            }
            if material_observed {
                return Err(OracleViolation::ObservationWithoutAppend);
            }
        }
        DurableSendOutcome::Indeterminate(_) => {
            if append_count == 0 && material_observed {
                return Err(OracleViolation::ObservationWithoutAppend);
            }
        }
        DurableSendOutcome::BrokerAccepted => {
            if append_count != 1 {
                return Err(OracleViolation::AcceptedWithoutAppend);
            }
            if intent.requested_boundary != ConfirmationBoundary::BrokerAccepted {
                return Err(OracleViolation::BoundaryOutcomeMismatch);
            }
        }
        DurableSendOutcome::OsSyncedAccepted if append_count != 1 => {
            return Err(OracleViolation::AcceptedWithoutAppend);
        }
        DurableSendOutcome::OsSyncedAccepted => {}
    }
    if response.outcome == DurableSendOutcome::OsSyncedAccepted
        && !seen_wire_stages.iter().all(|seen| *seen)
    {
        return Err(OracleViolation::MissingWireStage);
    }
    if response.outcome != DurableSendOutcome::OsSyncedAccepted && response.receipt.is_some() {
        return Err(OracleViolation::UnexpectedExactReceipt);
    }
    if material_observed && append_count != 1 {
        return Err(OracleViolation::ObservationWithoutAppend);
    }
    if effect_before_delivery {
        return Err(OracleViolation::EffectWithoutDelivery);
    }

    for delivery in &deliveries {
        if stored.is_none_or(|value| value.location != delivery.location) {
            return Err(OracleViolation::DeliveryWithoutStoredRecord);
        }
    }
    if effect.is_some_and(|value| value.applied) && deliveries.is_empty() {
        return Err(OracleViolation::EffectWithoutDelivery);
    }
    if let Some(ack) = ack
        && !deliveries
            .iter()
            .any(|value| value.location == ack.location)
    {
        return Err(OracleViolation::AckWithoutDelivery);
    }
    if checkpoint_observed && !ack.is_some_and(|value| value.accepted) {
        return Err(OracleViolation::CheckpointWithoutAck);
    }
    if let (Some(checkpoint_location), Some(ack)) = (checkpoint_location, ack)
        && checkpoint_location != ack.location
    {
        return Err(OracleViolation::CheckpointRecordMismatch);
    }

    let mut exact_record_proven = false;
    if response.outcome == DurableSendOutcome::OsSyncedAccepted {
        let receipt = receipt.ok_or(OracleViolation::ExactReceiptMismatch)?;
        exact_record_proven = stored.is_some_and(|value| {
            value.location == receipt.location && value.message_synced && value.index_synced
        });
        if !exact_record_proven {
            return Err(OracleViolation::ExactReceiptWithoutStoredRecord);
        }
    }

    Ok(OracleVerdict {
        append_count,
        exact_record_proven,
    })
}

/// Verifies every attempt and then enforces cross-attempt identity/location invariants.
pub fn verify_log(records: &[OracleRecord]) -> Result<Vec<OracleVerdict>, OracleViolation> {
    let mut attempts: HashMap<OracleAttemptId, (OracleIntent, Vec<OracleObservation>)> =
        HashMap::new();
    let mut order = Vec::new();
    for record in records {
        match record {
            OracleRecord::Intent(intent) => {
                if attempts
                    .insert(intent.attempt_id, (intent.clone(), Vec::new()))
                    .is_some()
                {
                    return Err(OracleViolation::DuplicateAttempt);
                }
                order.push(intent.attempt_id);
            }
            OracleRecord::Observation(observation) => {
                let (_, observations) = attempts
                    .get_mut(&observation.attempt_id)
                    .ok_or(OracleViolation::MissingIntent)?;
                observations.push(observation.clone());
            }
        }
    }

    let mut message_digests = HashMap::new();
    let mut append_offsets = HashMap::new();
    let mut append_indexes = HashMap::new();
    let mut verdicts = Vec::with_capacity(order.len());
    for attempt_id in order {
        let (intent, observations) = attempts
            .get(&attempt_id)
            .ok_or(OracleViolation::MissingIntent)?;
        if message_digests
            .insert(intent.message_id, intent.envelope_digest)
            .is_some_and(|digest| digest != intent.envelope_digest)
        {
            return Err(OracleViolation::SameMessageDifferentEnvelope);
        }
        let verdict = verify_attempt(intent, observations)?;
        if verdict.append_count != 0 {
            for observation in observations {
                if let ObservationKind::StoredReadback(stored) = &observation.kind {
                    let location = stored.location;
                    let offset_key = (location.resource_epoch, location.partition, location.offset);
                    let index_key = (location.resource_epoch, location.partition, location.index);
                    if append_offsets
                        .insert(offset_key, attempt_id)
                        .is_some_and(|owner| owner != attempt_id)
                        || append_indexes
                            .insert(index_key, attempt_id)
                            .is_some_and(|owner| owner != attempt_id)
                    {
                        return Err(OracleViolation::DuplicateAppendLocation);
                    }
                }
            }
        }
        verdicts.push(verdict);
    }
    Ok(verdicts)
}

fn require_append_invocation(append_invoked: bool) -> Result<(), OracleViolation> {
    if append_invoked {
        Ok(())
    } else {
        Err(OracleViolation::ObservationBeforeAppendInvocation)
    }
}

const fn wire_stage_index(value: WireStage) -> usize {
    match value {
        WireStage::AppendInvocation => 0,
        WireStage::WireWrite => 1,
        WireStage::JournalFlush => 2,
        WireStage::MessageSync => 3,
        WireStage::IndexSync => 4,
        WireStage::Response => 5,
    }
}

const fn recovery_state_index(value: RecoveryState) -> usize {
    match value {
        RecoveryState::Undelivered => 0,
        RecoveryState::DeliveredUncheckpointed => 1,
        RecoveryState::CheckpointOld => 2,
        RecoveryState::CheckpointNew => 3,
        RecoveryState::Committed => 4,
        RecoveryState::Redelivered => 5,
        RecoveryState::PresentAfterRestart => 6,
        RecoveryState::EvictedAfterSync => 7,
    }
}

fn verify_receipt(
    intent: &OracleIntent,
    receipt: ReceiptObservation,
) -> Result<(), OracleViolation> {
    if receipt.message_id != intent.message_id
        || receipt.envelope_digest != intent.envelope_digest
        || receipt.attempt_id != intent.attempt_id
        || receipt.boundary != intent.requested_boundary
        || receipt.session_fingerprint != intent.session_fingerprint
        || receipt.location.partition != intent.partition
        || receipt.boundary != ConfirmationBoundary::OsSyncedAccepted
    {
        return Err(OracleViolation::ExactReceiptMismatch);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CheckpointEvidence {
    Old,
    New,
    Committed,
}

fn verify_checkpoint(
    value: CheckpointObservation,
    frontier: &mut Option<Option<u64>>,
    previous_evidence: Option<CheckpointEvidence>,
) -> Result<CheckpointEvidence, OracleViolation> {
    let candidate_offset = value.location.offset;
    if previous_evidence == Some(CheckpointEvidence::New)
        && value.previous_offset == Some(candidate_offset)
        && value.recovered_offset == Some(candidate_offset)
        && value.outcome == CheckpointOutcome::CheckpointCommitted
    {
        *frontier = Some(Some(candidate_offset));
        return Ok(CheckpointEvidence::Committed);
    }
    if let Some(expected) = *frontier {
        if value.previous_offset != expected {
            return Err(OracleViolation::CheckpointFrontierRollback);
        }
    } else {
        *frontier = Some(value.previous_offset);
    }
    if value
        .previous_offset
        .is_some_and(|previous| previous.checked_add(1) != Some(candidate_offset))
    {
        return Err(OracleViolation::InvalidCheckpointRecovery);
    }
    let evidence = match value.outcome {
        CheckpointOutcome::CheckpointNotCommitted => {
            if value.recovered_offset != value.previous_offset {
                return Err(OracleViolation::InvalidCheckpointRecovery);
            }
            CheckpointEvidence::Old
        }
        CheckpointOutcome::CheckpointUnknown => {
            if value.recovered_offset == value.previous_offset {
                CheckpointEvidence::Old
            } else if value.recovered_offset == Some(candidate_offset) {
                CheckpointEvidence::New
            } else {
                return Err(OracleViolation::InvalidCheckpointRecovery);
            }
        }
        CheckpointOutcome::CheckpointCommitted => {
            if value.recovered_offset != Some(candidate_offset) {
                return Err(OracleViolation::InvalidCheckpointRecovery);
            }
            CheckpointEvidence::Committed
        }
    };
    if evidence != CheckpointEvidence::Old {
        *frontier = Some(Some(candidate_offset));
    }
    Ok(evidence)
}

#[allow(clippy::too_many_arguments)]
fn verify_recovery(
    value: RecoveryObservation,
    stored: Option<&StoredReadback>,
    deliveries: &[DeliveryObservation],
    effect: Option<EffectObservation>,
    ack: Option<AckObservation>,
    checkpoint_evidence: Option<CheckpointEvidence>,
    checkpoint_location: Option<ExactLocation>,
    stored_synced: bool,
    receipt: Option<ReceiptObservation>,
) -> Result<(), OracleViolation> {
    let valid = match value.state {
        RecoveryState::Undelivered => {
            deliveries.is_empty()
                && effect.is_none_or(|effect| !effect.applied)
                && ack.is_none_or(|ack| !ack.accepted)
                && checkpoint_evidence != Some(CheckpointEvidence::New)
                && checkpoint_evidence != Some(CheckpointEvidence::Committed)
                && value
                    .location
                    .is_none_or(|location| stored.is_some_and(|stored| stored.location == location))
        }
        RecoveryState::DeliveredUncheckpointed => {
            !deliveries.is_empty()
                && checkpoint_evidence != Some(CheckpointEvidence::New)
                && checkpoint_evidence != Some(CheckpointEvidence::Committed)
                && value.location.is_some_and(|location| {
                    deliveries
                        .iter()
                        .any(|delivery| delivery.location == location)
                })
        }
        RecoveryState::CheckpointOld => {
            checkpoint_evidence == Some(CheckpointEvidence::Old)
                && ack.is_some_and(|ack| ack.accepted)
                && value.location == checkpoint_location
        }
        RecoveryState::CheckpointNew => {
            checkpoint_evidence == Some(CheckpointEvidence::New)
                && ack.is_some_and(|ack| ack.accepted)
                && value.location == checkpoint_location
        }
        RecoveryState::Committed => {
            checkpoint_evidence == Some(CheckpointEvidence::Committed)
                && ack.is_some_and(|ack| ack.accepted)
                && value.location == checkpoint_location
        }
        RecoveryState::Redelivered => {
            checkpoint_evidence != Some(CheckpointEvidence::New)
                && checkpoint_evidence != Some(CheckpointEvidence::Committed)
                && value.location.is_some_and(|location| {
                    deliveries
                        .iter()
                        .any(|delivery| delivery.redelivery && delivery.location == location)
                })
        }
        RecoveryState::PresentAfterRestart => value
            .location
            .is_some_and(|location| stored.is_some_and(|stored| stored.location == location)),
        RecoveryState::EvictedAfterSync => receipt.is_some_and(|receipt| {
            stored_synced
                && stored.is_some_and(|stored| stored.location == receipt.location)
                && value.location == Some(receipt.location)
                && value
                    .oldest_after_restart
                    .is_some_and(|oldest| oldest > receipt.location.offset)
        }),
    };
    if valid {
        Ok(())
    } else {
        Err(OracleViolation::InvalidRecoveryEvidence)
    }
}

#[derive(Debug, Clone, Copy)]
struct CreationHistory {
    expected_namespace_digest: [u8; 32],
    expected_resolved_initial_offset: u64,
    owner_epoch_before: Option<u64>,
    owner_epoch_after: Option<u64>,
    manifest: Option<CreationManifestObservation>,
    terminal: bool,
}

fn verify_creation(
    value: CreationObservation,
    history: &mut Option<CreationHistory>,
) -> Result<(), OracleViolation> {
    let owner_incremented = match (value.owner_epoch_before, value.owner_epoch_after) {
        (None, Some(1)) => true,
        (Some(before), Some(after)) => before.checked_add(1) == Some(after),
        _ => false,
    };
    let manifest_matches = value.manifest.is_some_and(|manifest| {
        manifest.namespace_digest == value.expected_namespace_digest
            && manifest.resolved_initial_offset == value.expected_resolved_initial_offset
            && Some(manifest.owner_epoch) == value.owner_epoch_after
    });
    let valid = match value.state {
        CreationState::CreationNotCommitted => {
            value.manifest.is_none() && value.owner_epoch_after == value.owner_epoch_before
        }
        CreationState::CreationUnknown => {
            (value.manifest.is_none() && value.owner_epoch_after == value.owner_epoch_before)
                || (manifest_matches && owner_incremented)
        }
        CreationState::Created => manifest_matches && owner_incremented,
    };
    if !valid {
        return Err(OracleViolation::InvalidCreationRecovery);
    }

    let Some(previous) = history.as_mut() else {
        *history = Some(CreationHistory {
            expected_namespace_digest: value.expected_namespace_digest,
            expected_resolved_initial_offset: value.expected_resolved_initial_offset,
            owner_epoch_before: value.owner_epoch_before,
            owner_epoch_after: value.owner_epoch_after,
            manifest: value.manifest,
            terminal: value.state != CreationState::CreationUnknown,
        });
        return Ok(());
    };
    let advances_owner = previous.owner_epoch_after == previous.owner_epoch_before
        && owner_epoch_increment(previous.owner_epoch_before) == value.owner_epoch_after;
    if previous.terminal
        || value.expected_namespace_digest != previous.expected_namespace_digest
        || value.expected_resolved_initial_offset != previous.expected_resolved_initial_offset
        || value.owner_epoch_before != previous.owner_epoch_before
        || (value.owner_epoch_after != previous.owner_epoch_after && !advances_owner)
        || previous
            .manifest
            .is_some_and(|manifest| value.manifest != Some(manifest))
    {
        return Err(OracleViolation::CreationExpectationMismatch);
    }
    previous.owner_epoch_after = value.owner_epoch_after;
    if value.manifest.is_some() {
        previous.manifest = value.manifest;
    }
    previous.terminal = value.state != CreationState::CreationUnknown;
    Ok(())
}

fn owner_epoch_increment(value: Option<u64>) -> Option<u64> {
    match value {
        None => Some(1),
        Some(value) => value.checked_add(1),
    }
}

/// Exact contradiction found while correlating one attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OracleViolation {
    AttemptCorrelationMismatch,
    MultipleAppends,
    InconsistentAppendCounts,
    WrongCommandCode,
    ZeroWireCounts,
    DuplicateWireStage,
    WireStageRegression,
    MissingWireStage,
    WireCountsChanged,
    WireAfterResponse,
    ObservationBeforeAppendInvocation,
    StoredReadbackBeforeSync,
    InvalidStoredSyncEvidence,
    MultipleStoredRecords,
    MultipleResponses,
    MissingResponse,
    StoredEnvelopeMismatch,
    DeliveryMismatch,
    InvalidDeliverySequence,
    DeliveryWithoutStoredRecord,
    EffectMismatch,
    EffectWithoutDelivery,
    AckMismatch,
    AckWithoutDelivery,
    CheckpointWithoutAck,
    CheckpointRecordMismatch,
    ResourceEpochMismatch,
    NotSubmittedAfterAppend,
    ObservationWithoutAppend,
    AcceptedWithoutAppend,
    UnexpectedExactReceipt,
    BoundaryOutcomeMismatch,
    ExactReceiptMismatch,
    ExactReceiptWithoutStoredRecord,
    InvalidCheckpointRecovery,
    CheckpointFrontierRollback,
    InvalidCreationRecovery,
    CreationExpectationMismatch,
    InvalidRecoveryEvidence,
    DuplicateAttempt,
    MissingIntent,
    SameMessageDifferentEnvelope,
    DuplicateAppendLocation,
}

/// Append-only filesystem store used only by the verification harness.
#[derive(Debug, Clone)]
pub struct OracleStore {
    path: PathBuf,
    transaction_lock: Arc<Mutex<()>>,
}

impl OracleStore {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        let path = normalized_lock_path(&path.into());
        let transaction_lock = shared_path_lock(&path);
        Self {
            path,
            transaction_lock,
        }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Appends and syncs an intent, including the parent directory on creation.
    pub fn persist_intent(&mut self, intent: &OracleIntent) -> Result<(), OracleError> {
        let _guard = self.lock_transaction()?;
        let history = self.load_chain_unlocked()?;
        if history.records.iter().any(
            |record| matches!(record, OracleRecord::Intent(existing) if existing.attempt_id == intent.attempt_id),
        ) {
            return Err(OracleError::DuplicateIntent);
        }
        self.append_unlocked(&OracleRecord::Intent(intent.clone()), &history)
    }

    /// Appends and syncs one observation only after its intent exists.
    pub fn append_observation(
        &mut self,
        observation: &OracleObservation,
    ) -> Result<(), OracleError> {
        if let ObservationKind::StoredReadback(stored) = &observation.kind
            && stored.canonical_envelope.len() > MAX_CANONICAL_ENVELOPE_LEN
        {
            return Err(OracleError::InvalidRecord("stored envelope too large"));
        }
        let _guard = self.lock_transaction()?;
        let history = self.load_chain_unlocked()?;
        if !history.records.iter().any(
            |record| matches!(record, OracleRecord::Intent(intent) if intent.attempt_id == observation.attempt_id),
        ) {
            return Err(OracleError::MissingIntent);
        }
        self.append_unlocked(&OracleRecord::Observation(observation.clone()), &history)
    }

    /// Strictly reloads every complete checksummed record in append order.
    pub fn load(&self) -> Result<Vec<OracleRecord>, OracleError> {
        let _guard = self.lock_transaction()?;
        self.load_chain_unlocked().map(|history| history.records)
    }

    fn lock_transaction(&self) -> Result<TransactionGuard<'_>, OracleError> {
        let process_guard = self
            .transaction_lock
            .lock()
            .map_err(|_| OracleError::InvalidRecord("path lock poisoned"))?;
        let lock_path = suffixed_path(&self.path, ".lock");
        let lock_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)?;
        FileExt::lock_exclusive(&lock_file)?;
        Ok(TransactionGuard {
            _process_guard: process_guard,
            _lock_file: lock_file,
        })
    }

    fn load_chain_unlocked(&self) -> Result<DecodedHistory, OracleError> {
        let mut file = match File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let history = DecodedHistory::default();
                self.verify_head(&history)?;
                return Ok(history);
            }
            Err(error) => return Err(error.into()),
        };
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let history = decode_records(&bytes)?;
        self.verify_head(&history)?;
        Ok(history)
    }

    fn append_unlocked(
        &self,
        record: &OracleRecord,
        history: &DecodedHistory,
    ) -> Result<(), OracleError> {
        let sequence = history
            .last_sequence
            .checked_add(1)
            .ok_or(OracleError::InvalidRecord("sequence overflow"))?;
        let (bytes, chain_checksum) = encode_record(record, sequence, history.last_chain_checksum);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        self.write_head(sequence, chain_checksum)?;
        Ok(())
    }

    fn head_path(&self) -> PathBuf {
        suffixed_path(&self.path, ".head")
    }

    fn verify_head(&self, history: &DecodedHistory) -> Result<(), OracleError> {
        let path = self.head_path();
        let mut file = match File::open(path) {
            Ok(file) => file,
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound && history.last_sequence == 0 =>
            {
                return Ok(());
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(OracleError::InvalidRecord("missing head"));
            }
            Err(error) => return Err(error.into()),
        };
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let (sequence, chain_checksum) = decode_head(&bytes)?;
        if sequence != history.last_sequence || chain_checksum != history.last_chain_checksum {
            return Err(OracleError::InvalidRecord("head mismatch"));
        }
        Ok(())
    }

    fn write_head(&self, sequence: u64, chain_checksum: u32) -> Result<(), OracleError> {
        let head_path = self.head_path();
        let temporary_path = suffixed_path(&self.path, ".head.tmp");
        let bytes = encode_head(sequence, chain_checksum);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary_path, &head_path)?;
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        File::open(parent)?.sync_all()?;
        Ok(())
    }
}

struct TransactionGuard<'a> {
    _process_guard: MutexGuard<'a, ()>,
    _lock_file: File,
}

fn shared_path_lock(path: &Path) -> Arc<Mutex<()>> {
    static PATH_LOCKS: OnceLock<Mutex<HashMap<PathBuf, Weak<Mutex<()>>>>> = OnceLock::new();

    let key = path.to_path_buf();
    let mut locks = PATH_LOCKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
        return lock;
    }
    locks.retain(|_, lock| lock.strong_count() != 0);
    let lock = Arc::new(Mutex::new(()));
    locks.insert(key, Arc::downgrade(&lock));
    lock
}

fn normalized_lock_path(path: &Path) -> PathBuf {
    if let Ok(path) = fs::canonicalize(path) {
        return path;
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if let (Ok(parent), Some(file_name)) = (fs::canonicalize(parent), path.file_name()) {
        return parent.join(file_name);
    }
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|directory| directory.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    }
}

/// Filesystem, framing, or ordering failure in the independent oracle.
#[derive(Debug)]
pub enum OracleError {
    Io(std::io::Error),
    InvalidRecord(&'static str),
    DuplicateIntent,
    MissingIntent,
    AttemptNotStarted,
    AttemptAlreadyInvoked,
    AttemptBindingMismatch,
}

impl Display for OracleError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "oracle storage failed: {error}"),
            Self::InvalidRecord(reason) => write!(formatter, "invalid oracle record: {reason}"),
            Self::DuplicateIntent => formatter.write_str("oracle intent already exists"),
            Self::MissingIntent => formatter.write_str("oracle observation has no intent"),
            Self::AttemptNotStarted => formatter.write_str("oracle attempt was not started"),
            Self::AttemptAlreadyInvoked => {
                formatter.write_str("oracle intent was captured after append invocation")
            }
            Self::AttemptBindingMismatch => {
                formatter.write_str("oracle attempt does not bind the prepared request")
            }
        }
    }
}

impl Error for OracleError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for OracleError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

fn encode_record(
    record: &OracleRecord,
    sequence: u64,
    previous_chain_checksum: u32,
) -> (Vec<u8>, u32) {
    let (kind, payload) = match record {
        OracleRecord::Intent(intent) => (1, encode_intent(intent)),
        OracleRecord::Observation(observation) => (2, encode_observation(observation)),
    };
    let chain_checksum = record_checksum(kind, sequence, previous_chain_checksum, &payload);
    let mut bytes = Vec::with_capacity(RECORD_HEADER_LEN + payload.len());
    bytes.extend_from_slice(&RECORD_MAGIC);
    put_u16(&mut bytes, RECORD_VERSION);
    bytes.push(kind);
    put_u64(&mut bytes, sequence);
    put_u32(&mut bytes, previous_chain_checksum);
    put_u32(&mut bytes, payload.len() as u32);
    put_u32(&mut bytes, chain_checksum);
    bytes.extend_from_slice(&payload);
    (bytes, chain_checksum)
}

#[derive(Default)]
struct DecodedHistory {
    records: Vec<OracleRecord>,
    last_sequence: u64,
    last_chain_checksum: u32,
}

fn decode_records(bytes: &[u8]) -> Result<DecodedHistory, OracleError> {
    let mut records = Vec::new();
    let mut cursor = Cursor::new(bytes);
    let mut expected_sequence = 1_u64;
    let mut previous_chain_checksum = 0_u32;
    let mut intents = HashSet::new();
    while cursor.remaining() != 0 {
        if cursor.remaining() < RECORD_HEADER_LEN {
            return Err(OracleError::InvalidRecord("truncated header"));
        }
        if cursor.take(4)? != RECORD_MAGIC {
            return Err(OracleError::InvalidRecord("magic"));
        }
        if cursor.u16()? != RECORD_VERSION {
            return Err(OracleError::InvalidRecord("version"));
        }
        let kind = cursor.u8()?;
        let sequence = cursor.u64()?;
        let observed_previous = cursor.u32()?;
        let length = cursor.u32()? as usize;
        let expected_checksum = cursor.u32()?;
        let payload = cursor.take(length)?;
        if sequence != expected_sequence || observed_previous != previous_chain_checksum {
            return Err(OracleError::InvalidRecord("history chain"));
        }
        if record_checksum(kind, sequence, observed_previous, payload) != expected_checksum {
            return Err(OracleError::InvalidRecord("checksum"));
        }
        let record = match kind {
            1 => OracleRecord::Intent(decode_intent(payload)?),
            2 => OracleRecord::Observation(decode_observation(payload)?),
            _ => return Err(OracleError::InvalidRecord("kind")),
        };
        match &record {
            OracleRecord::Intent(intent) => {
                if !intents.insert(intent.attempt_id) {
                    return Err(OracleError::InvalidRecord("duplicate intent"));
                }
            }
            OracleRecord::Observation(observation)
                if !intents.contains(&observation.attempt_id) =>
            {
                return Err(OracleError::InvalidRecord("observation before intent"));
            }
            OracleRecord::Observation(_) => {}
        }
        records.push(record);
        expected_sequence = expected_sequence
            .checked_add(1)
            .ok_or(OracleError::InvalidRecord("sequence overflow"))?;
        previous_chain_checksum = expected_checksum;
    }
    Ok(DecodedHistory {
        records,
        last_sequence: expected_sequence - 1,
        last_chain_checksum: previous_chain_checksum,
    })
}

fn suffixed_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value: OsString = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}

fn encode_head(sequence: u64, chain_checksum: u32) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(HEAD_LEN);
    bytes.extend_from_slice(&HEAD_MAGIC);
    put_u16(&mut bytes, RECORD_VERSION);
    put_u64(&mut bytes, sequence);
    put_u32(&mut bytes, chain_checksum);
    let head_checksum = checksum(0, &bytes);
    put_u32(&mut bytes, head_checksum);
    bytes
}

fn decode_head(bytes: &[u8]) -> Result<(u64, u32), OracleError> {
    if bytes.len() != HEAD_LEN {
        return Err(OracleError::InvalidRecord("head length"));
    }
    let mut cursor = Cursor::new(bytes);
    if cursor.take(4)? != HEAD_MAGIC {
        return Err(OracleError::InvalidRecord("head magic"));
    }
    if cursor.u16()? != RECORD_VERSION {
        return Err(OracleError::InvalidRecord("head version"));
    }
    let sequence = cursor.u64()?;
    let chain_checksum = cursor.u32()?;
    let expected_checksum = cursor.u32()?;
    if checksum(0, &bytes[..HEAD_LEN - 4]) != expected_checksum {
        return Err(OracleError::InvalidRecord("head checksum"));
    }
    Ok((sequence, chain_checksum))
}

fn record_checksum(kind: u8, sequence: u64, previous_chain_checksum: u32, payload: &[u8]) -> u32 {
    let mut bytes = Vec::with_capacity(12 + payload.len());
    put_u64(&mut bytes, sequence);
    put_u32(&mut bytes, previous_chain_checksum);
    bytes.extend_from_slice(payload);
    checksum(kind, &bytes)
}

fn encode_intent(value: &OracleIntent) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(190 + value.ordering_key.len() + value.payload.len());
    bytes.extend_from_slice(&value.message_id.0);
    bytes.extend_from_slice(&value.attempt_id.0);
    bytes.extend_from_slice(value.envelope_digest.as_bytes());
    put_u16(&mut bytes, value.codec_version);
    bytes.extend_from_slice(&value.source);
    bytes.extend_from_slice(&value.target);
    put_u64(&mut bytes, value.generation);
    put_u32(&mut bytes, value.partition);
    put_u32(&mut bytes, value.ordering_key.len() as u32);
    bytes.extend_from_slice(&value.ordering_key);
    put_u32(&mut bytes, value.routing_map_version);
    put_u64(&mut bytes, value.payload.len() as u64);
    bytes.extend_from_slice(&value.payload);
    bytes.extend_from_slice(value.payload_digest.as_bytes());
    bytes.push(encode_boundary(value.requested_boundary));
    bytes.extend_from_slice(value.session_fingerprint.as_bytes());
    bytes
}

fn decode_intent(bytes: &[u8]) -> Result<OracleIntent, OracleError> {
    let mut cursor = Cursor::new(bytes);
    let message_id = OracleMessageId(cursor.array()?);
    let attempt_id = OracleAttemptId(cursor.array()?);
    let envelope_digest = EnvelopeDigest::from_bytes(cursor.array()?);
    let codec_version = cursor.u16()?;
    let source = cursor.array()?;
    let target = cursor.array()?;
    let generation = cursor.u64()?;
    let partition = cursor.u32()?;
    let ordering_key_len = cursor.u32()? as usize;
    if ordering_key_len > MAX_ORDERING_KEY_LEN {
        return Err(OracleError::InvalidRecord("intent ordering key too large"));
    }
    let ordering_key = cursor.take(ordering_key_len)?.to_vec();
    let routing_map_version = cursor.u32()?;
    let payload_len_u64 = cursor.u64()?;
    if payload_len_u64 > MAX_CANONICAL_PAYLOAD_LEN as u64 {
        return Err(OracleError::InvalidRecord("intent payload too large"));
    }
    let payload_len = usize::try_from(payload_len_u64)
        .map_err(|_| OracleError::InvalidRecord("intent payload length"))?;
    let payload = cursor.take(payload_len)?.to_vec();
    let payload_digest = PayloadDigest::from_bytes(cursor.array()?);
    let value = OracleIntent {
        message_id,
        attempt_id,
        envelope_digest,
        codec_version,
        source,
        target,
        generation,
        partition,
        ordering_key,
        routing_map_version,
        payload,
        payload_digest,
        requested_boundary: decode_boundary(cursor.u8()?)?,
        session_fingerprint: SessionFingerprint::from_bytes(cursor.array()?),
    };
    cursor.finish()?;
    validate_decoded_intent(&value)?;
    Ok(value)
}

fn validate_decoded_intent(value: &OracleIntent) -> Result<(), OracleError> {
    if value.codec_version != CANONICAL_CODEC_VERSION {
        return Err(OracleError::InvalidRecord("intent codec version"));
    }
    if value.message_id.0[6] & 0xf0 != 0x40 || value.message_id.0[8] & 0xc0 != 0x80 {
        return Err(OracleError::InvalidRecord("intent message id"));
    }
    let payload_digest: [u8; 32] = Sha256::digest(&value.payload).into();
    if payload_digest != *value.payload_digest.as_bytes() {
        return Err(OracleError::InvalidRecord("intent payload digest"));
    }
    let mut hasher = Sha256::new();
    hasher.update(ENVELOPE_DOMAIN);
    hasher.update(value.codec_version.to_be_bytes());
    hasher.update(value.message_id.0);
    hasher.update(value.source);
    hasher.update(value.target);
    hasher.update(value.generation.to_be_bytes());
    hasher.update(value.partition.to_be_bytes());
    hasher.update((value.ordering_key.len() as u32).to_be_bytes());
    hasher.update(&value.ordering_key);
    hasher.update((value.payload.len() as u64).to_be_bytes());
    hasher.update(&value.payload);
    let envelope_digest: [u8; 32] = hasher.finalize().into();
    if envelope_digest != *value.envelope_digest.as_bytes() {
        return Err(OracleError::InvalidRecord("intent envelope digest"));
    }
    Ok(())
}

fn encode_observation(value: &OracleObservation) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&value.attempt_id.0);
    match &value.kind {
        ObservationKind::Wire(value) => {
            let value = *value;
            bytes.push(1);
            put_u32(&mut bytes, value.command_code);
            bytes.push(encode_stage(value.stage));
            put_u32(&mut bytes, value.wire_count);
            put_u32(&mut bytes, value.command_count);
            put_u32(&mut bytes, value.record_count);
            put_u32(&mut bytes, value.payload_count);
        }
        ObservationKind::StoredReadback(value) => {
            bytes.push(2);
            put_u32(&mut bytes, value.canonical_envelope.len() as u32);
            bytes.extend_from_slice(&value.canonical_envelope);
            encode_location(&mut bytes, value.location);
            put_bool(&mut bytes, value.message_synced);
            put_bool(&mut bytes, value.index_synced);
        }
        ObservationKind::Response(value) => {
            let value = *value;
            bytes.push(3);
            encode_outcome(&mut bytes, value.outcome);
            put_bool(&mut bytes, value.receipt.is_some());
            if let Some(receipt) = value.receipt {
                encode_receipt(&mut bytes, receipt);
            }
        }
        ObservationKind::Delivery(value) => {
            let value = *value;
            bytes.push(4);
            bytes.extend_from_slice(&value.message_id.0);
            bytes.extend_from_slice(value.envelope_digest.as_bytes());
            encode_location(&mut bytes, value.location);
            put_bool(&mut bytes, value.redelivery);
        }
        ObservationKind::Effect(value) => {
            let value = *value;
            bytes.push(5);
            bytes.extend_from_slice(&value.message_id.0);
            bytes.extend_from_slice(value.envelope_digest.as_bytes());
            encode_location(&mut bytes, value.location);
            put_bool(&mut bytes, value.applied);
        }
        ObservationKind::Ack(value) => {
            let value = *value;
            bytes.push(10);
            bytes.extend_from_slice(&value.message_id.0);
            bytes.extend_from_slice(value.envelope_digest.as_bytes());
            encode_location(&mut bytes, value.location);
            put_bool(&mut bytes, value.accepted);
        }
        ObservationKind::Checkpoint(value) => {
            let value = *value;
            bytes.push(6);
            bytes.extend_from_slice(&value.message_id.0);
            bytes.extend_from_slice(value.envelope_digest.as_bytes());
            encode_location(&mut bytes, value.location);
            put_option_u64(&mut bytes, value.previous_offset);
            put_option_u64(&mut bytes, value.recovered_offset);
            bytes.push(encode_checkpoint(value.outcome));
        }
        ObservationKind::ResourceEpoch(value) => {
            bytes.push(7);
            encode_epoch(&mut bytes, *value);
        }
        ObservationKind::Recovery(value) => {
            let value = *value;
            bytes.push(8);
            bytes.push(encode_recovery(value.state));
            put_bool(&mut bytes, value.location.is_some());
            if let Some(location) = value.location {
                encode_location(&mut bytes, location);
            }
            put_option_u64(&mut bytes, value.oldest_after_restart);
        }
        ObservationKind::Creation(value) => {
            let value = *value;
            bytes.push(9);
            bytes.push(encode_creation(value.state));
            bytes.extend_from_slice(&value.expected_namespace_digest);
            put_u64(&mut bytes, value.expected_resolved_initial_offset);
            put_bool(&mut bytes, value.manifest.is_some());
            if let Some(manifest) = value.manifest {
                bytes.extend_from_slice(&manifest.namespace_digest);
                put_u64(&mut bytes, manifest.resolved_initial_offset);
                put_u64(&mut bytes, manifest.owner_epoch);
            }
            put_option_u64(&mut bytes, value.owner_epoch_before);
            put_option_u64(&mut bytes, value.owner_epoch_after);
        }
    }
    bytes
}

fn decode_observation(bytes: &[u8]) -> Result<OracleObservation, OracleError> {
    let mut cursor = Cursor::new(bytes);
    let attempt_id = OracleAttemptId(cursor.array()?);
    let kind = match cursor.u8()? {
        1 => ObservationKind::Wire(WireObservation::new(
            cursor.u32()?,
            decode_stage(cursor.u8()?)?,
            cursor.u32()?,
            cursor.u32()?,
            cursor.u32()?,
            cursor.u32()?,
        )),
        2 => {
            let envelope_len = cursor.u32()? as usize;
            if envelope_len > MAX_CANONICAL_ENVELOPE_LEN {
                return Err(OracleError::InvalidRecord("stored envelope too large"));
            }
            ObservationKind::StoredReadback(StoredReadback::new(
                cursor.take(envelope_len)?.to_vec(),
                decode_location(&mut cursor)?,
                cursor.boolean()?,
                cursor.boolean()?,
            ))
        }
        3 => {
            let outcome = decode_outcome(&mut cursor)?;
            let receipt = if cursor.boolean()? {
                Some(decode_receipt(&mut cursor)?)
            } else {
                None
            };
            ObservationKind::Response(ResponseObservation::new(outcome, receipt))
        }
        4 => ObservationKind::Delivery(DeliveryObservation::new(
            OracleMessageId(cursor.array()?),
            EnvelopeDigest::from_bytes(cursor.array()?),
            decode_location(&mut cursor)?,
            cursor.boolean()?,
        )),
        5 => ObservationKind::Effect(EffectObservation::new(
            OracleMessageId(cursor.array()?),
            EnvelopeDigest::from_bytes(cursor.array()?),
            decode_location(&mut cursor)?,
            cursor.boolean()?,
        )),
        10 => ObservationKind::Ack(AckObservation::new(
            OracleMessageId(cursor.array()?),
            EnvelopeDigest::from_bytes(cursor.array()?),
            decode_location(&mut cursor)?,
            cursor.boolean()?,
        )),
        6 => ObservationKind::Checkpoint(CheckpointObservation::new(
            OracleMessageId(cursor.array()?),
            EnvelopeDigest::from_bytes(cursor.array()?),
            decode_location(&mut cursor)?,
            cursor.option_u64()?,
            cursor.option_u64()?,
            decode_checkpoint(cursor.u8()?)?,
        )),
        7 => ObservationKind::ResourceEpoch(decode_epoch(&mut cursor)?),
        8 => {
            let state = decode_recovery(cursor.u8()?)?;
            let location = if cursor.boolean()? {
                Some(decode_location(&mut cursor)?)
            } else {
                None
            };
            ObservationKind::Recovery(RecoveryObservation::new(
                state,
                location,
                cursor.option_u64()?,
            ))
        }
        9 => {
            let state = decode_creation(cursor.u8()?)?;
            let expected_namespace_digest = cursor.array()?;
            let expected_resolved_initial_offset = cursor.u64()?;
            let manifest = if cursor.boolean()? {
                Some(CreationManifestObservation::new(
                    cursor.array()?,
                    cursor.u64()?,
                    cursor.u64()?,
                ))
            } else {
                None
            };
            ObservationKind::Creation(CreationObservation::new(
                state,
                expected_namespace_digest,
                expected_resolved_initial_offset,
                manifest,
                cursor.option_u64()?,
                cursor.option_u64()?,
            ))
        }
        _ => return Err(OracleError::InvalidRecord("observation kind")),
    };
    cursor.finish()?;
    Ok(OracleObservation::new(attempt_id, kind))
}

fn encode_location(bytes: &mut Vec<u8>, value: ExactLocation) {
    encode_epoch(bytes, value.resource_epoch);
    put_u32(bytes, value.partition);
    put_u64(bytes, value.offset);
    put_u64(bytes, value.index);
}

fn decode_location(cursor: &mut Cursor<'_>) -> Result<ExactLocation, OracleError> {
    Ok(ExactLocation::new(
        decode_epoch(cursor)?,
        cursor.u32()?,
        cursor.u64()?,
        cursor.u64()?,
    ))
}

fn encode_epoch(bytes: &mut Vec<u8>, value: ResourceEpoch) {
    bytes.extend_from_slice(value.resource_id().as_bytes());
    put_u64(bytes, value.epoch());
}

fn decode_epoch(cursor: &mut Cursor<'_>) -> Result<ResourceEpoch, OracleError> {
    Ok(ResourceEpoch::new(
        ResourceId::from_bytes(cursor.array()?),
        cursor.u64()?,
    ))
}

fn encode_receipt(bytes: &mut Vec<u8>, value: ReceiptObservation) {
    bytes.extend_from_slice(&value.message_id.0);
    bytes.extend_from_slice(value.envelope_digest.as_bytes());
    bytes.extend_from_slice(&value.attempt_id.0);
    bytes.push(encode_boundary(value.boundary));
    bytes.extend_from_slice(value.session_fingerprint.as_bytes());
    encode_location(bytes, value.location);
}

fn decode_receipt(cursor: &mut Cursor<'_>) -> Result<ReceiptObservation, OracleError> {
    Ok(ReceiptObservation::new(
        OracleMessageId(cursor.array()?),
        EnvelopeDigest::from_bytes(cursor.array()?),
        OracleAttemptId(cursor.array()?),
        decode_boundary(cursor.u8()?)?,
        SessionFingerprint::from_bytes(cursor.array()?),
        decode_location(cursor)?,
    ))
}

fn encode_outcome(bytes: &mut Vec<u8>, value: DurableSendOutcome) {
    match value {
        DurableSendOutcome::NotSubmitted(reason) => {
            bytes.push(1);
            bytes.push(encode_failure(reason));
        }
        DurableSendOutcome::BrokerAccepted => bytes.push(2),
        DurableSendOutcome::OsSyncedAccepted => bytes.push(3),
        DurableSendOutcome::Indeterminate(reason) => {
            bytes.push(4);
            bytes.push(encode_failure(reason));
        }
    }
}

fn decode_outcome(cursor: &mut Cursor<'_>) -> Result<DurableSendOutcome, OracleError> {
    match cursor.u8()? {
        1 => Ok(DurableSendOutcome::NotSubmitted(decode_failure(
            cursor.u8()?,
        )?)),
        2 => Ok(DurableSendOutcome::BrokerAccepted),
        3 => Ok(DurableSendOutcome::OsSyncedAccepted),
        4 => Ok(DurableSendOutcome::Indeterminate(decode_failure(
            cursor.u8()?,
        )?)),
        _ => Err(OracleError::InvalidRecord("send outcome")),
    }
}

fn encode_failure(value: AttemptFailureKind) -> u8 {
    match value {
        AttemptFailureKind::Unavailable => 1,
        AttemptFailureKind::Transport => 2,
        AttemptFailureKind::Protocol => 3,
        AttemptFailureKind::Cancelled => 4,
        AttemptFailureKind::Shutdown => 5,
    }
}

fn decode_failure(value: u8) -> Result<AttemptFailureKind, OracleError> {
    match value {
        1 => Ok(AttemptFailureKind::Unavailable),
        2 => Ok(AttemptFailureKind::Transport),
        3 => Ok(AttemptFailureKind::Protocol),
        4 => Ok(AttemptFailureKind::Cancelled),
        5 => Ok(AttemptFailureKind::Shutdown),
        _ => Err(OracleError::InvalidRecord("attempt failure")),
    }
}

fn encode_boundary(value: ConfirmationBoundary) -> u8 {
    match value {
        ConfirmationBoundary::BrokerAccepted => 1,
        ConfirmationBoundary::OsSyncedAccepted => 2,
    }
}

fn decode_boundary(value: u8) -> Result<ConfirmationBoundary, OracleError> {
    match value {
        1 => Ok(ConfirmationBoundary::BrokerAccepted),
        2 => Ok(ConfirmationBoundary::OsSyncedAccepted),
        _ => Err(OracleError::InvalidRecord("confirmation boundary")),
    }
}

fn encode_stage(value: WireStage) -> u8 {
    match value {
        WireStage::AppendInvocation => 1,
        WireStage::WireWrite => 2,
        WireStage::JournalFlush => 3,
        WireStage::MessageSync => 4,
        WireStage::IndexSync => 5,
        WireStage::Response => 6,
    }
}

fn decode_stage(value: u8) -> Result<WireStage, OracleError> {
    match value {
        1 => Ok(WireStage::AppendInvocation),
        2 => Ok(WireStage::WireWrite),
        3 => Ok(WireStage::JournalFlush),
        4 => Ok(WireStage::MessageSync),
        5 => Ok(WireStage::IndexSync),
        6 => Ok(WireStage::Response),
        _ => Err(OracleError::InvalidRecord("wire stage")),
    }
}

fn encode_checkpoint(value: CheckpointOutcome) -> u8 {
    match value {
        CheckpointOutcome::CheckpointCommitted => 1,
        CheckpointOutcome::CheckpointNotCommitted => 2,
        CheckpointOutcome::CheckpointUnknown => 3,
    }
}

fn decode_checkpoint(value: u8) -> Result<CheckpointOutcome, OracleError> {
    match value {
        1 => Ok(CheckpointOutcome::CheckpointCommitted),
        2 => Ok(CheckpointOutcome::CheckpointNotCommitted),
        3 => Ok(CheckpointOutcome::CheckpointUnknown),
        _ => Err(OracleError::InvalidRecord("checkpoint outcome")),
    }
}

fn encode_recovery(value: RecoveryState) -> u8 {
    match value {
        RecoveryState::Undelivered => 1,
        RecoveryState::DeliveredUncheckpointed => 2,
        RecoveryState::CheckpointOld => 3,
        RecoveryState::CheckpointNew => 4,
        RecoveryState::Committed => 5,
        RecoveryState::Redelivered => 6,
        RecoveryState::PresentAfterRestart => 7,
        RecoveryState::EvictedAfterSync => 8,
    }
}

fn decode_recovery(value: u8) -> Result<RecoveryState, OracleError> {
    match value {
        1 => Ok(RecoveryState::Undelivered),
        2 => Ok(RecoveryState::DeliveredUncheckpointed),
        3 => Ok(RecoveryState::CheckpointOld),
        4 => Ok(RecoveryState::CheckpointNew),
        5 => Ok(RecoveryState::Committed),
        6 => Ok(RecoveryState::Redelivered),
        7 => Ok(RecoveryState::PresentAfterRestart),
        8 => Ok(RecoveryState::EvictedAfterSync),
        _ => Err(OracleError::InvalidRecord("recovery state")),
    }
}

fn encode_creation(value: CreationState) -> u8 {
    match value {
        CreationState::CreationNotCommitted => 1,
        CreationState::CreationUnknown => 2,
        CreationState::Created => 3,
    }
}

fn decode_creation(value: u8) -> Result<CreationState, OracleError> {
    match value {
        1 => Ok(CreationState::CreationNotCommitted),
        2 => Ok(CreationState::CreationUnknown),
        3 => Ok(CreationState::Created),
        _ => Err(OracleError::InvalidRecord("creation state")),
    }
}

fn checksum(kind: u8, bytes: &[u8]) -> u32 {
    let mut value = 0xffff_ffff_u32 ^ u32::from(kind);
    for byte in bytes {
        value ^= u32::from(*byte);
        for _ in 0..8 {
            value = (value >> 1) ^ (0xedb8_8320_u32 & (0_u32.wrapping_sub(value & 1)));
        }
    }
    !value
}

fn put_bool(bytes: &mut Vec<u8>, value: bool) {
    bytes.push(u8::from(value));
}

fn put_u16(bytes: &mut Vec<u8>, value: u16) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn put_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn put_option_u64(bytes: &mut Vec<u8>, value: Option<u64>) {
    put_bool(bytes, value.is_some());
    if let Some(value) = value {
        put_u64(bytes, value);
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], OracleError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(OracleError::InvalidRecord("length overflow"))?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or(OracleError::InvalidRecord("truncated payload"))?;
        self.position = end;
        Ok(bytes)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], OracleError> {
        self.take(N)?
            .try_into()
            .map_err(|_| OracleError::InvalidRecord("fixed field"))
    }

    fn u8(&mut self) -> Result<u8, OracleError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, OracleError> {
        Ok(u16::from_be_bytes(self.array()?))
    }

    fn u32(&mut self) -> Result<u32, OracleError> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, OracleError> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn boolean(&mut self) -> Result<bool, OracleError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(OracleError::InvalidRecord("boolean")),
        }
    }

    fn option_u64(&mut self) -> Result<Option<u64>, OracleError> {
        if self.boolean()? {
            Ok(Some(self.u64()?))
        } else {
            Ok(None)
        }
    }

    fn finish(self) -> Result<(), OracleError> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            Err(OracleError::InvalidRecord("trailing bytes"))
        }
    }
}

#[cfg(test)]
pub(crate) fn test_intent() -> OracleIntent {
    tests::intent()
}

#[cfg(test)]
mod tests {
    use super::*;
    use alopex_chirps_core::durable::{
        AttemptBinding, CanonicalEnvelope, ConfirmationBoundary, DurableMessageRoute,
        EnvelopeDigest, PayloadDigest, PreparedDurableSend, SessionFingerprint,
    };
    use std::fs;
    use std::path::PathBuf;
    use std::process::{Command, Stdio};
    use std::sync::Barrier;
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    pub(super) fn intent() -> OracleIntent {
        let route = DurableMessageRoute::new(
            [0x11; 16].into(),
            [0x22; 16].into(),
            7,
            3,
            b"order-7".to_vec(),
            1,
        );
        let prepared = PreparedDurableSend::prepare(route, |message_id| {
            let payload = b"payload-7";
            let payload_digest = PayloadDigest::from_bytes(Sha256::digest(payload).into());
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&CANONICAL_CODEC_VERSION.to_be_bytes());
            bytes.extend_from_slice(message_id.as_bytes());
            bytes.extend_from_slice(&[0x11; 16]);
            bytes.extend_from_slice(&[0x22; 16]);
            bytes.extend_from_slice(&7_u64.to_be_bytes());
            bytes.extend_from_slice(&3_u32.to_be_bytes());
            bytes.extend_from_slice(&7_u32.to_be_bytes());
            bytes.extend_from_slice(b"order-7");
            bytes.extend_from_slice(&(payload.len() as u64).to_be_bytes());
            bytes.extend_from_slice(payload);
            let mut hasher = Sha256::new();
            hasher.update(ENVELOPE_DOMAIN);
            hasher.update(&bytes);
            let envelope_digest = EnvelopeDigest::from_bytes(hasher.finalize().into());
            bytes.extend_from_slice(payload_digest.as_bytes());
            bytes.extend_from_slice(envelope_digest.as_bytes());
            CanonicalEnvelope::try_new(1, bytes, payload_digest, envelope_digest)
        })
        .unwrap();
        let binding = AttemptBinding::start(
            &prepared,
            SessionFingerprint::from_bytes([0x55; 32]),
            ConfirmationBoundary::OsSyncedAccepted,
        )
        .unwrap();
        OracleIntent::from_started_attempt(&prepared, &binding).unwrap()
    }

    fn canonical_envelope(intent: &OracleIntent) -> Vec<u8> {
        canonical_envelope_with(
            intent,
            intent.codec_version,
            intent.message_id.0,
            intent.source,
            intent.target,
            intent.generation,
            intent.partition,
            &intent.ordering_key,
            &intent.payload,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn canonical_envelope_with(
        _intent: &OracleIntent,
        codec_version: u16,
        message_id: [u8; 16],
        source: [u8; 16],
        target: [u8; 16],
        generation: u64,
        partition: u32,
        ordering_key: &[u8],
        payload: &[u8],
    ) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&codec_version.to_be_bytes());
        bytes.extend_from_slice(&message_id);
        bytes.extend_from_slice(&source);
        bytes.extend_from_slice(&target);
        bytes.extend_from_slice(&generation.to_be_bytes());
        bytes.extend_from_slice(&partition.to_be_bytes());
        bytes.extend_from_slice(&(ordering_key.len() as u32).to_be_bytes());
        bytes.extend_from_slice(ordering_key);
        bytes.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        bytes.extend_from_slice(payload);
        let payload_digest: [u8; 32] = Sha256::digest(payload).into();
        let mut hasher = Sha256::new();
        hasher.update(ENVELOPE_DOMAIN);
        hasher.update(&bytes);
        let envelope_digest: [u8; 32] = hasher.finalize().into();
        bytes.extend_from_slice(&payload_digest);
        bytes.extend_from_slice(&envelope_digest);
        bytes
    }

    fn location() -> ExactLocation {
        ExactLocation::new(
            ResourceEpoch::new(ResourceId::from_bytes([0x66; 16]), 9),
            3,
            41,
            42,
        )
    }

    fn other_location() -> ExactLocation {
        ExactLocation::new(
            ResourceEpoch::new(ResourceId::from_bytes([0x66; 16]), 9),
            3,
            43,
            44,
        )
    }

    const WIRE_STAGES: [WireStage; 6] = [
        WireStage::AppendInvocation,
        WireStage::WireWrite,
        WireStage::JournalFlush,
        WireStage::MessageSync,
        WireStage::IndexSync,
        WireStage::Response,
    ];

    fn complete_wire_observations(intent: &OracleIntent) -> Vec<OracleObservation> {
        WIRE_STAGES
            .into_iter()
            .map(|stage| {
                OracleObservation::wire(
                    intent.attempt_id(),
                    WireObservation::new(APPEND_ONE_SYNCED_CODE, stage, 1, 1, 1, 1),
                )
            })
            .collect()
    }

    fn strong_observations(intent: &OracleIntent) -> Vec<OracleObservation> {
        strong_observations_at(intent, location())
    }

    fn strong_observations_at(
        intent: &OracleIntent,
        exact_location: ExactLocation,
    ) -> Vec<OracleObservation> {
        let mut observations = complete_wire_observations(intent);
        observations.extend([
            OracleObservation::stored_readback(
                intent.attempt_id(),
                StoredReadback::new(canonical_envelope(intent), exact_location, true, true),
            ),
            OracleObservation::response(
                intent.attempt_id(),
                ResponseObservation::new(
                    DurableSendOutcome::OsSyncedAccepted,
                    Some(ReceiptObservation::new(
                        intent.message_id(),
                        intent.envelope_digest(),
                        intent.attempt_id(),
                        intent.requested_boundary(),
                        intent.session_fingerprint(),
                        exact_location,
                    )),
                ),
            ),
        ]);
        observations
    }

    fn delivered_observations(intent: &OracleIntent) -> Vec<OracleObservation> {
        let mut observations = strong_observations(intent);
        observations.push(OracleObservation::delivery(
            intent.attempt_id(),
            DeliveryObservation::new(
                intent.message_id(),
                intent.envelope_digest(),
                location(),
                false,
            ),
        ));
        observations
    }

    fn acked_observations(intent: &OracleIntent) -> Vec<OracleObservation> {
        let mut observations = delivered_observations(intent);
        observations.push(OracleObservation::ack(
            intent.attempt_id(),
            AckObservation::new(
                intent.message_id(),
                intent.envelope_digest(),
                location(),
                true,
            ),
        ));
        observations
    }

    fn append_wire(intent: &OracleIntent, count: u32) -> OracleObservation {
        OracleObservation::wire(
            intent.attempt_id(),
            WireObservation::new(
                APPEND_ONE_SYNCED_CODE,
                WireStage::AppendInvocation,
                count,
                count,
                count,
                count,
            ),
        )
    }

    fn response(intent: &OracleIntent, outcome: DurableSendOutcome) -> OracleObservation {
        OracleObservation::response(intent.attempt_id(), ResponseObservation::new(outcome, None))
    }

    fn checkpoint(
        intent: &OracleIntent,
        previous_offset: Option<u64>,
        recovered_offset: Option<u64>,
        outcome: CheckpointOutcome,
    ) -> CheckpointObservation {
        CheckpointObservation::new(
            intent.message_id(),
            intent.envelope_digest(),
            location(),
            previous_offset,
            recovered_offset,
            outcome,
        )
    }

    fn unique_directory(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "chirps-oracle-records-{name}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    #[test]
    fn v07_task_6_2_rejects_more_than_one_append_for_one_attempt() {
        let intent = intent();
        let observations = vec![OracleObservation::wire(
            intent.attempt_id(),
            WireObservation::new(
                APPEND_ONE_SYNCED_CODE,
                WireStage::AppendInvocation,
                2,
                2,
                2,
                2,
            ),
        )];

        assert_eq!(
            verify_attempt(&intent, &observations),
            Err(OracleViolation::MultipleAppends)
        );
    }

    #[test]
    fn v07_task_6_2_requires_complete_wire_prefix_for_strong_success() {
        let intent = intent();
        let mut observations = strong_observations(&intent);
        observations.remove(4);

        assert_eq!(
            verify_attempt(&intent, &observations),
            Err(OracleViolation::MissingWireStage)
        );
    }

    #[test]
    fn v07_task_6_2_rejects_stored_envelope_substitution() {
        let intent = intent();
        let mut observations = strong_observations(&intent);
        observations[WIRE_STAGES.len()] = OracleObservation::stored_readback(
            intent.attempt_id(),
            StoredReadback::new(
                {
                    let mut bytes = canonical_envelope(&intent);
                    let last = bytes.len() - 1;
                    bytes[last] ^= 1;
                    bytes
                },
                location(),
                true,
                true,
            ),
        );

        assert_eq!(
            verify_attempt(&intent, &observations),
            Err(OracleViolation::StoredEnvelopeMismatch)
        );
    }

    #[test]
    fn v07_task_6_2_rejects_false_exact_receipt_without_readback() {
        let intent = intent();
        let mut observations = strong_observations(&intent);
        observations.remove(WIRE_STAGES.len());

        assert_eq!(
            verify_attempt(&intent, &observations),
            Err(OracleViolation::ExactReceiptWithoutStoredRecord)
        );
    }

    #[test]
    fn v07_task_6_2_checkpoint_unknown_accepts_only_old_or_new_frontier() {
        let intent = intent();
        let mut valid = acked_observations(&intent);
        valid.push(OracleObservation::checkpoint(
            intent.attempt_id(),
            checkpoint(
                &intent,
                Some(40),
                Some(40),
                CheckpointOutcome::CheckpointUnknown,
            ),
        ));
        assert!(verify_attempt(&intent, &valid).is_ok());

        let mut skipped = acked_observations(&intent);
        skipped.push(OracleObservation::checkpoint(
            intent.attempt_id(),
            checkpoint(
                &intent,
                Some(40),
                Some(42),
                CheckpointOutcome::CheckpointUnknown,
            ),
        ));
        assert_eq!(
            verify_attempt(&intent, &skipped),
            Err(OracleViolation::InvalidCheckpointRecovery)
        );
    }

    #[test]
    fn v07_task_6_2_exact_receipt_allows_only_proven_post_sync_eviction() {
        let intent = intent();
        let mut observations = strong_observations(&intent);
        observations.push(OracleObservation::recovery(
            intent.attempt_id(),
            RecoveryObservation::evicted_after_sync(location(), 42),
        ));
        assert!(verify_attempt(&intent, &observations).is_ok());

        observations[WIRE_STAGES.len()] = OracleObservation::stored_readback(
            intent.attempt_id(),
            StoredReadback::new(canonical_envelope(&intent), location(), true, false),
        );
        assert_eq!(
            verify_attempt(&intent, &observations),
            Err(OracleViolation::InvalidRecoveryEvidence),
        );
    }

    #[test]
    fn v07_task_6_2_rejects_material_effects_without_append_or_delivery() {
        let intent = intent();
        let applied = OracleObservation::effect(
            intent.attempt_id(),
            EffectObservation::new(
                intent.message_id(),
                intent.envelope_digest(),
                location(),
                true,
            ),
        );

        assert_eq!(
            verify_attempt(
                &intent,
                &[
                    append_wire(&intent, 1),
                    response(
                        &intent,
                        DurableSendOutcome::Indeterminate(AttemptFailureKind::Transport),
                    ),
                    applied.clone(),
                ],
            ),
            Err(OracleViolation::EffectWithoutDelivery)
        );
        assert_eq!(
            verify_attempt(
                &intent,
                &[
                    response(
                        &intent,
                        DurableSendOutcome::NotSubmitted(AttemptFailureKind::Unavailable),
                    ),
                    applied,
                ],
            ),
            Err(OracleViolation::ObservationBeforeAppendInvocation)
        );
    }

    #[test]
    fn v07_task_6_2_rejects_every_material_observation_before_append_invocation() {
        let intent = intent();
        let before_append = [
            OracleObservation::stored_readback(
                intent.attempt_id(),
                StoredReadback::new(canonical_envelope(&intent), location(), true, true),
            ),
            OracleObservation::delivery(
                intent.attempt_id(),
                DeliveryObservation::new(
                    intent.message_id(),
                    intent.envelope_digest(),
                    location(),
                    false,
                ),
            ),
            OracleObservation::effect(
                intent.attempt_id(),
                EffectObservation::new(
                    intent.message_id(),
                    intent.envelope_digest(),
                    location(),
                    true,
                ),
            ),
            OracleObservation::ack(
                intent.attempt_id(),
                AckObservation::new(
                    intent.message_id(),
                    intent.envelope_digest(),
                    location(),
                    true,
                ),
            ),
            OracleObservation::checkpoint(
                intent.attempt_id(),
                checkpoint(
                    &intent,
                    Some(40),
                    Some(41),
                    CheckpointOutcome::CheckpointCommitted,
                ),
            ),
            OracleObservation::recovery(
                intent.attempt_id(),
                RecoveryObservation::new(
                    RecoveryState::PresentAfterRestart,
                    Some(location()),
                    None,
                ),
            ),
        ];
        for observation in before_append {
            assert_eq!(
                verify_attempt(&intent, &[observation]),
                Err(OracleViolation::ObservationBeforeAppendInvocation)
            );
        }
    }

    #[test]
    fn v07_task_6_2_recomputes_and_compares_every_stored_envelope_field() {
        let intent = intent();
        let mut other_message_id = intent.message_id.0;
        other_message_id[15] ^= 1;
        let substitutions = [
            canonical_envelope_with(
                &intent,
                2,
                intent.message_id.0,
                intent.source,
                intent.target,
                intent.generation,
                intent.partition,
                &intent.ordering_key,
                &intent.payload,
            ),
            canonical_envelope_with(
                &intent,
                intent.codec_version,
                other_message_id,
                intent.source,
                intent.target,
                intent.generation,
                intent.partition,
                &intent.ordering_key,
                &intent.payload,
            ),
            canonical_envelope_with(
                &intent,
                intent.codec_version,
                intent.message_id.0,
                [0x31; 16],
                intent.target,
                intent.generation,
                intent.partition,
                &intent.ordering_key,
                &intent.payload,
            ),
            canonical_envelope_with(
                &intent,
                intent.codec_version,
                intent.message_id.0,
                intent.source,
                [0x32; 16],
                intent.generation,
                intent.partition,
                &intent.ordering_key,
                &intent.payload,
            ),
            canonical_envelope_with(
                &intent,
                intent.codec_version,
                intent.message_id.0,
                intent.source,
                intent.target,
                intent.generation + 1,
                intent.partition,
                &intent.ordering_key,
                &intent.payload,
            ),
            canonical_envelope_with(
                &intent,
                intent.codec_version,
                intent.message_id.0,
                intent.source,
                intent.target,
                intent.generation,
                intent.partition + 1,
                &intent.ordering_key,
                &intent.payload,
            ),
            canonical_envelope_with(
                &intent,
                intent.codec_version,
                intent.message_id.0,
                intent.source,
                intent.target,
                intent.generation,
                intent.partition,
                b"other-key",
                &intent.payload,
            ),
            canonical_envelope_with(
                &intent,
                intent.codec_version,
                intent.message_id.0,
                intent.source,
                intent.target,
                intent.generation,
                intent.partition,
                &intent.ordering_key,
                b"other-payload",
            ),
        ];
        for canonical in substitutions {
            let mut observations = strong_observations(&intent);
            observations[WIRE_STAGES.len()] = OracleObservation::stored_readback(
                intent.attempt_id(),
                StoredReadback::new(canonical, location(), true, true),
            );
            assert_eq!(
                verify_attempt(&intent, &observations),
                Err(OracleViolation::StoredEnvelopeMismatch)
            );
        }
    }

    #[test]
    fn v07_task_6_2_rejects_synced_readback_before_matching_wire_sync() {
        let intent = intent();
        let mut before_message_sync = strong_observations(&intent);
        let stored = before_message_sync.remove(WIRE_STAGES.len());
        before_message_sync.insert(1, stored);
        assert_eq!(
            verify_attempt(&intent, &before_message_sync),
            Err(OracleViolation::StoredReadbackBeforeSync)
        );

        let mut impossible_flags = strong_observations(&intent);
        impossible_flags[WIRE_STAGES.len()] = OracleObservation::stored_readback(
            intent.attempt_id(),
            StoredReadback::new(canonical_envelope(&intent), location(), false, true),
        );
        assert_eq!(
            verify_attempt(&intent, &impossible_flags),
            Err(OracleViolation::InvalidStoredSyncEvidence)
        );
    }

    #[test]
    fn v07_task_6_2_accepts_unknown_new_then_idempotent_committed_confirmation() {
        let intent = intent();
        let mut observations = acked_observations(&intent);
        observations.extend([
            OracleObservation::checkpoint(
                intent.attempt_id(),
                checkpoint(
                    &intent,
                    Some(40),
                    Some(41),
                    CheckpointOutcome::CheckpointUnknown,
                ),
            ),
            OracleObservation::recovery(
                intent.attempt_id(),
                RecoveryObservation::new(RecoveryState::CheckpointNew, Some(location()), None),
            ),
            OracleObservation::checkpoint(
                intent.attempt_id(),
                checkpoint(
                    &intent,
                    Some(41),
                    Some(41),
                    CheckpointOutcome::CheckpointCommitted,
                ),
            ),
            OracleObservation::recovery(
                intent.attempt_id(),
                RecoveryObservation::new(RecoveryState::Committed, Some(location()), None),
            ),
        ]);
        assert!(verify_attempt(&intent, &observations).is_ok());
    }

    #[test]
    fn v07_task_6_2_verify_log_accepts_legal_retry_and_rejects_cross_attempt_aliases() {
        let first = intent();
        let mut retry = first.clone();
        retry.attempt_id = OracleAttemptId([0x77; 16]);
        let mut records = vec![OracleRecord::Intent(first.clone())];
        records.extend(
            strong_observations_at(&first, location())
                .into_iter()
                .map(OracleRecord::Observation),
        );
        records.push(OracleRecord::Intent(retry.clone()));
        records.extend(
            strong_observations_at(&retry, other_location())
                .into_iter()
                .map(OracleRecord::Observation),
        );
        assert_eq!(verify_log(&records).unwrap().len(), 2);

        let mut different_digest = retry.clone();
        different_digest.attempt_id = OracleAttemptId([0x78; 16]);
        different_digest.envelope_digest = EnvelopeDigest::from_bytes([0x99; 32]);
        let mut conflicting = records.clone();
        conflicting.push(OracleRecord::Intent(different_digest));
        assert_eq!(
            verify_log(&conflicting),
            Err(OracleViolation::SameMessageDifferentEnvelope)
        );

        for aliased_location in [
            ExactLocation::new(
                ResourceEpoch::new(ResourceId::from_bytes([0x66; 16]), 9),
                3,
                41,
                99,
            ),
            ExactLocation::new(
                ResourceEpoch::new(ResourceId::from_bytes([0x66; 16]), 9),
                3,
                99,
                42,
            ),
        ] {
            let other = intent();
            let mut duplicate_location = vec![OracleRecord::Intent(first.clone())];
            duplicate_location.extend(
                strong_observations(&first)
                    .into_iter()
                    .map(OracleRecord::Observation),
            );
            duplicate_location.push(OracleRecord::Intent(other.clone()));
            duplicate_location.extend(
                strong_observations_at(&other, aliased_location)
                    .into_iter()
                    .map(OracleRecord::Observation),
            );
            assert_eq!(
                verify_log(&duplicate_location),
                Err(OracleViolation::DuplicateAppendLocation)
            );
        }

        let orphan = OracleObservation::wire(
            OracleAttemptId([0x91; 16]),
            WireObservation::new(
                APPEND_ONE_SYNCED_CODE,
                WireStage::AppendInvocation,
                1,
                1,
                1,
                1,
            ),
        );
        assert_eq!(
            verify_log(&[OracleRecord::Observation(orphan)]),
            Err(OracleViolation::MissingIntent)
        );
    }

    #[test]
    fn v07_task_6_2_process_writer_helper() {
        let Ok(path) = std::env::var("CHIRPS_ORACLE_PROCESS_PATH") else {
            return;
        };
        let start = PathBuf::from(std::env::var("CHIRPS_ORACLE_PROCESS_START").unwrap());
        while !start.exists() {
            thread::sleep(Duration::from_millis(1));
        }
        let count: usize = std::env::var("CHIRPS_ORACLE_PROCESS_COUNT")
            .unwrap()
            .parse()
            .unwrap();
        let mut store = OracleStore::new(path);
        for _ in 0..count {
            store.persist_intent(&intent()).unwrap();
        }
    }

    #[test]
    fn v07_task_6_2_serializes_append_transactions_across_processes() {
        const CHILDREN: usize = 2;
        const RECORDS_PER_CHILD: usize = 2;
        let directory = unique_directory("process-lock");
        fs::create_dir(&directory).unwrap();
        let path = directory.join("oracle.log");
        let start = directory.join("start");
        let executable = std::env::current_exe().unwrap();
        let mut children = Vec::new();
        for _ in 0..CHILDREN {
            children.push(
                Command::new(&executable)
                    .args([
                        "--exact",
                        "oracle::tests::v07_task_6_2_process_writer_helper",
                    ])
                    .env("CHIRPS_ORACLE_PROCESS_PATH", &path)
                    .env("CHIRPS_ORACLE_PROCESS_START", &start)
                    .env("CHIRPS_ORACLE_PROCESS_COUNT", RECORDS_PER_CHILD.to_string())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()
                    .unwrap(),
            );
        }
        fs::write(&start, b"go").unwrap();
        for child in &mut children {
            assert!(child.wait().unwrap().success());
        }
        let records = OracleStore::new(&path).load().unwrap();
        assert_eq!(records.len(), CHILDREN * RECORDS_PER_CHILD);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn v07_task_6_2_rejects_causally_reordered_side_effects() {
        let intent = intent();
        let delivery = OracleObservation::delivery(
            intent.attempt_id(),
            DeliveryObservation::new(
                intent.message_id(),
                intent.envelope_digest(),
                location(),
                false,
            ),
        );

        let mut effect_before_delivery = strong_observations(&intent);
        effect_before_delivery.push(OracleObservation::effect(
            intent.attempt_id(),
            EffectObservation::new(
                intent.message_id(),
                intent.envelope_digest(),
                location(),
                true,
            ),
        ));
        effect_before_delivery.push(delivery.clone());
        assert_eq!(
            verify_attempt(&intent, &effect_before_delivery),
            Err(OracleViolation::EffectWithoutDelivery)
        );

        let mut ack_before_delivery = strong_observations(&intent);
        ack_before_delivery.push(OracleObservation::ack(
            intent.attempt_id(),
            AckObservation::new(
                intent.message_id(),
                intent.envelope_digest(),
                location(),
                true,
            ),
        ));
        ack_before_delivery.push(delivery);
        assert_eq!(
            verify_attempt(&intent, &ack_before_delivery),
            Err(OracleViolation::AckWithoutDelivery)
        );

        let mut checkpoint_before_ack = delivered_observations(&intent);
        checkpoint_before_ack.push(OracleObservation::checkpoint(
            intent.attempt_id(),
            checkpoint(
                &intent,
                Some(40),
                Some(41),
                CheckpointOutcome::CheckpointCommitted,
            ),
        ));
        checkpoint_before_ack.push(OracleObservation::ack(
            intent.attempt_id(),
            AckObservation::new(
                intent.message_id(),
                intent.envelope_digest(),
                location(),
                true,
            ),
        ));
        assert_eq!(
            verify_attempt(&intent, &checkpoint_before_ack),
            Err(OracleViolation::CheckpointWithoutAck)
        );
    }

    #[test]
    fn v07_task_6_2_rejects_effect_for_a_different_exact_location() {
        let intent = intent();
        let mut observations = delivered_observations(&intent);
        observations.push(OracleObservation::effect(
            intent.attempt_id(),
            EffectObservation::new(
                intent.message_id(),
                intent.envelope_digest(),
                ExactLocation::new(location().resource_epoch(), 3, 42, 43),
                true,
            ),
        ));

        assert_eq!(
            verify_attempt(&intent, &observations),
            Err(OracleViolation::EffectWithoutDelivery)
        );
    }

    #[test]
    fn v07_task_6_2_binds_recovery_to_the_checkpoint_state_at_observation_time() {
        let intent = intent();
        let mut observations = acked_observations(&intent);
        observations.extend([
            OracleObservation::checkpoint(
                intent.attempt_id(),
                checkpoint(
                    &intent,
                    Some(40),
                    Some(40),
                    CheckpointOutcome::CheckpointNotCommitted,
                ),
            ),
            OracleObservation::recovery(
                intent.attempt_id(),
                RecoveryObservation::new(RecoveryState::CheckpointOld, Some(location()), None),
            ),
            OracleObservation::checkpoint(
                intent.attempt_id(),
                checkpoint(
                    &intent,
                    Some(40),
                    Some(41),
                    CheckpointOutcome::CheckpointCommitted,
                ),
            ),
            OracleObservation::recovery(
                intent.attempt_id(),
                RecoveryObservation::new(RecoveryState::Committed, Some(location()), None),
            ),
        ]);

        assert!(verify_attempt(&intent, &observations).is_ok());
    }

    #[test]
    fn v07_task_6_2_rejects_redelivery_after_committed_checkpoint() {
        let intent = intent();
        let mut observations = acked_observations(&intent);
        observations.extend([
            OracleObservation::checkpoint(
                intent.attempt_id(),
                checkpoint(
                    &intent,
                    Some(40),
                    Some(41),
                    CheckpointOutcome::CheckpointCommitted,
                ),
            ),
            OracleObservation::delivery(
                intent.attempt_id(),
                DeliveryObservation::new(
                    intent.message_id(),
                    intent.envelope_digest(),
                    location(),
                    true,
                ),
            ),
            OracleObservation::recovery(
                intent.attempt_id(),
                RecoveryObservation::new(RecoveryState::Redelivered, Some(location()), None),
            ),
        ]);

        assert_eq!(
            verify_attempt(&intent, &observations),
            Err(OracleViolation::InvalidRecoveryEvidence)
        );

        let mut recovery_rollback = delivered_observations(&intent);
        recovery_rollback.extend([
            OracleObservation::delivery(
                intent.attempt_id(),
                DeliveryObservation::new(
                    intent.message_id(),
                    intent.envelope_digest(),
                    location(),
                    true,
                ),
            ),
            OracleObservation::ack(
                intent.attempt_id(),
                AckObservation::new(
                    intent.message_id(),
                    intent.envelope_digest(),
                    location(),
                    true,
                ),
            ),
            OracleObservation::checkpoint(
                intent.attempt_id(),
                checkpoint(
                    &intent,
                    Some(40),
                    Some(41),
                    CheckpointOutcome::CheckpointCommitted,
                ),
            ),
            OracleObservation::recovery(
                intent.attempt_id(),
                RecoveryObservation::new(RecoveryState::Redelivered, Some(location()), None),
            ),
        ]);
        assert_eq!(
            verify_attempt(&intent, &recovery_rollback),
            Err(OracleViolation::InvalidRecoveryEvidence)
        );
    }

    #[test]
    fn v07_task_6_2_validates_every_wire_stage_command_and_count() {
        let intent = intent();
        let wrong_command = OracleObservation::wire(
            intent.attempt_id(),
            WireObservation::new(
                APPEND_ONE_SYNCED_CODE + 1,
                WireStage::MessageSync,
                1,
                1,
                1,
                1,
            ),
        );
        assert_eq!(
            verify_attempt(&intent, &[wrong_command]),
            Err(OracleViolation::WrongCommandCode)
        );

        let duplicate_write = OracleObservation::wire(
            intent.attempt_id(),
            WireObservation::new(APPEND_ONE_SYNCED_CODE, WireStage::WireWrite, 2, 2, 2, 2),
        );
        assert_eq!(
            verify_attempt(&intent, &[duplicate_write]),
            Err(OracleViolation::MultipleAppends)
        );
    }

    #[test]
    fn v07_task_6_2_rejects_every_wire_counter_mutation_missing_stage_and_reorder() {
        let intent = intent();

        for stage in 0..WIRE_STAGES.len() {
            for counter in 0..4 {
                let mut observations = strong_observations(&intent);
                let ObservationKind::Wire(value) = &mut observations[stage].kind else {
                    unreachable!()
                };
                match counter {
                    0 => value.wire_count = 0,
                    1 => value.command_count = 0,
                    2 => value.record_count = 0,
                    3 => value.payload_count = 0,
                    _ => unreachable!(),
                }
                assert_eq!(
                    verify_attempt(&intent, &observations),
                    Err(OracleViolation::InconsistentAppendCounts),
                    "stage {stage}, counter {counter}"
                );
            }

            let mut changed_counts = strong_observations(&intent);
            let ObservationKind::Wire(value) = &mut changed_counts[stage].kind else {
                unreachable!()
            };
            value.wire_count = 0;
            value.command_count = 0;
            value.record_count = 0;
            value.payload_count = 0;
            assert_eq!(
                verify_attempt(&intent, &changed_counts),
                Err(if stage == 0 {
                    OracleViolation::ZeroWireCounts
                } else {
                    OracleViolation::WireCountsChanged
                }),
                "cross-stage counts at stage {stage}"
            );

            let mut missing = strong_observations(&intent);
            missing.remove(stage);
            assert_eq!(
                verify_attempt(&intent, &missing),
                Err(OracleViolation::MissingWireStage),
                "missing stage {stage}"
            );
        }

        for stage in 0..WIRE_STAGES.len() - 1 {
            let mut reordered = strong_observations(&intent);
            reordered.swap(stage, stage + 1);
            assert_eq!(
                verify_attempt(&intent, &reordered),
                Err(OracleViolation::MissingWireStage),
                "reordered stages {stage} and {}",
                stage + 1
            );
        }

        let mut response_before_wire_response = strong_observations(&intent);
        let response = response_before_wire_response.pop().unwrap();
        response_before_wire_response.insert(5, response);
        assert_eq!(
            verify_attempt(&intent, &response_before_wire_response),
            Err(OracleViolation::WireAfterResponse)
        );
    }

    #[test]
    fn v07_task_6_2_rejects_zero_count_stage_and_wire_after_public_response() {
        let intent = intent();
        let not_submitted = response(
            &intent,
            DurableSendOutcome::NotSubmitted(AttemptFailureKind::Transport),
        );
        assert!(
            verify_attempt(&intent, &[append_wire(&intent, 0), not_submitted]).is_err(),
            "an observed invocation cannot carry zero counters or be NotSubmitted"
        );

        let indeterminate = response(
            &intent,
            DurableSendOutcome::Indeterminate(AttemptFailureKind::Transport),
        );
        let wire_write = OracleObservation::wire(
            intent.attempt_id(),
            WireObservation::new(APPEND_ONE_SYNCED_CODE, WireStage::WireWrite, 1, 1, 1, 1),
        );
        assert!(
            verify_attempt(
                &intent,
                &[append_wire(&intent, 1), indeterminate, wire_write],
            )
            .is_err(),
            "wire evidence after a public outcome must fail closed"
        );
    }

    #[test]
    fn v07_task_6_2_rejects_checkpoint_frontier_rollback_and_missing_ack() {
        let intent = intent();
        let mut rollback = acked_observations(&intent);
        rollback.push(OracleObservation::checkpoint(
            intent.attempt_id(),
            checkpoint(
                &intent,
                Some(40),
                Some(41),
                CheckpointOutcome::CheckpointCommitted,
            ),
        ));
        rollback.push(OracleObservation::checkpoint(
            intent.attempt_id(),
            checkpoint(
                &intent,
                Some(40),
                Some(40),
                CheckpointOutcome::CheckpointNotCommitted,
            ),
        ));
        assert_eq!(
            verify_attempt(&intent, &rollback),
            Err(OracleViolation::CheckpointFrontierRollback)
        );

        let mut missing_ack = delivered_observations(&intent);
        missing_ack.push(OracleObservation::checkpoint(
            intent.attempt_id(),
            checkpoint(
                &intent,
                Some(40),
                Some(41),
                CheckpointOutcome::CheckpointCommitted,
            ),
        ));
        assert_eq!(
            verify_attempt(&intent, &missing_ack),
            Err(OracleViolation::CheckpointWithoutAck)
        );

        let wrong_location = ExactLocation::new(location().resource_epoch(), 3, 42, 43);
        let mut wrong_record = acked_observations(&intent);
        wrong_record.push(OracleObservation::checkpoint(
            intent.attempt_id(),
            CheckpointObservation::new(
                intent.message_id(),
                intent.envelope_digest(),
                wrong_location,
                Some(41),
                Some(42),
                CheckpointOutcome::CheckpointCommitted,
            ),
        ));
        assert_eq!(
            verify_attempt(&intent, &wrong_record),
            Err(OracleViolation::CheckpointRecordMismatch)
        );

        let mut wrong_digest = checkpoint(
            &intent,
            Some(40),
            Some(41),
            CheckpointOutcome::CheckpointCommitted,
        );
        wrong_digest.envelope_digest = EnvelopeDigest::from_bytes([0x91; 32]);
        let mut wrong_digest_observations = acked_observations(&intent);
        wrong_digest_observations.push(OracleObservation::checkpoint(
            intent.attempt_id(),
            wrong_digest,
        ));
        assert_eq!(
            verify_attempt(&intent, &wrong_digest_observations),
            Err(OracleViolation::CheckpointRecordMismatch)
        );

        let mut skipped_frontier = acked_observations(&intent);
        skipped_frontier.push(OracleObservation::checkpoint(
            intent.attempt_id(),
            checkpoint(
                &intent,
                Some(39),
                Some(41),
                CheckpointOutcome::CheckpointCommitted,
            ),
        ));
        assert_eq!(
            verify_attempt(&intent, &skipped_frontier),
            Err(OracleViolation::InvalidCheckpointRecovery)
        );
    }

    #[test]
    fn v07_task_6_2_enforces_outcome_append_count_matrix_and_attempt_binding() {
        let intent = intent();
        let not_submitted = DurableSendOutcome::NotSubmitted(AttemptFailureKind::Transport);
        let indeterminate = DurableSendOutcome::Indeterminate(AttemptFailureKind::Transport);

        assert!(verify_attempt(&intent, &[response(&intent, not_submitted)]).is_ok());
        assert_eq!(
            verify_attempt(
                &intent,
                &[append_wire(&intent, 1), response(&intent, not_submitted)],
            ),
            Err(OracleViolation::NotSubmittedAfterAppend)
        );
        assert!(verify_attempt(&intent, &[response(&intent, indeterminate)]).is_ok());
        assert!(
            verify_attempt(
                &intent,
                &[append_wire(&intent, 1), response(&intent, indeterminate)],
            )
            .is_ok()
        );

        assert_eq!(
            verify_attempt(
                &intent,
                &[OracleObservation::response(
                    OracleAttemptId([0x99; 16]),
                    ResponseObservation::new(indeterminate, None),
                )],
            ),
            Err(OracleViolation::AttemptCorrelationMismatch)
        );
    }

    #[test]
    fn v07_task_6_2_accepts_only_correlated_recovery_states() {
        let intent = intent();

        let undelivered = vec![
            append_wire(&intent, 1),
            response(
                &intent,
                DurableSendOutcome::Indeterminate(AttemptFailureKind::Transport),
            ),
            OracleObservation::recovery(
                intent.attempt_id(),
                RecoveryObservation::new(RecoveryState::Undelivered, None, None),
            ),
        ];
        assert!(verify_attempt(&intent, &undelivered).is_ok());

        let mut delivered = delivered_observations(&intent);
        delivered.push(OracleObservation::recovery(
            intent.attempt_id(),
            RecoveryObservation::new(
                RecoveryState::DeliveredUncheckpointed,
                Some(location()),
                None,
            ),
        ));
        assert!(verify_attempt(&intent, &delivered).is_ok());

        let mut wrong_delivery_recovery = delivered_observations(&intent);
        wrong_delivery_recovery.push(OracleObservation::recovery(
            intent.attempt_id(),
            RecoveryObservation::new(
                RecoveryState::DeliveredUncheckpointed,
                Some(ExactLocation::new(location().resource_epoch(), 3, 99, 100)),
                None,
            ),
        ));
        assert_eq!(
            verify_attempt(&intent, &wrong_delivery_recovery),
            Err(OracleViolation::InvalidRecoveryEvidence)
        );

        for (state, outcome, recovered) in [
            (
                RecoveryState::CheckpointOld,
                CheckpointOutcome::CheckpointNotCommitted,
                Some(40),
            ),
            (
                RecoveryState::CheckpointNew,
                CheckpointOutcome::CheckpointUnknown,
                Some(41),
            ),
            (
                RecoveryState::Committed,
                CheckpointOutcome::CheckpointCommitted,
                Some(41),
            ),
        ] {
            let mut observations = acked_observations(&intent);
            observations.push(OracleObservation::checkpoint(
                intent.attempt_id(),
                checkpoint(&intent, Some(40), recovered, outcome),
            ));
            observations.push(OracleObservation::recovery(
                intent.attempt_id(),
                RecoveryObservation::new(state, Some(location()), None),
            ));
            assert!(verify_attempt(&intent, &observations).is_ok(), "{state:?}");
        }

        let mut redelivered = delivered_observations(&intent);
        redelivered.push(OracleObservation::delivery(
            intent.attempt_id(),
            DeliveryObservation::new(
                intent.message_id(),
                intent.envelope_digest(),
                location(),
                true,
            ),
        ));
        redelivered.push(OracleObservation::recovery(
            intent.attempt_id(),
            RecoveryObservation::new(RecoveryState::Redelivered, Some(location()), None),
        ));
        assert!(verify_attempt(&intent, &redelivered).is_ok());

        let mut present = strong_observations(&intent);
        present.push(OracleObservation::recovery(
            intent.attempt_id(),
            RecoveryObservation::new(RecoveryState::PresentAfterRestart, Some(location()), None),
        ));
        assert!(verify_attempt(&intent, &present).is_ok());

        let mut evicted = strong_observations(&intent);
        evicted.push(OracleObservation::recovery(
            intent.attempt_id(),
            RecoveryObservation::evicted_after_sync(location(), 43),
        ));
        assert!(verify_attempt(&intent, &evicted).is_ok());

        let mut false_committed = acked_observations(&intent);
        false_committed.push(OracleObservation::recovery(
            intent.attempt_id(),
            RecoveryObservation::new(RecoveryState::Committed, Some(location()), None),
        ));
        assert_eq!(
            verify_attempt(&intent, &false_committed),
            Err(OracleViolation::InvalidRecoveryEvidence)
        );

        let mut wrong_checkpoint_state = acked_observations(&intent);
        wrong_checkpoint_state.push(OracleObservation::checkpoint(
            intent.attempt_id(),
            checkpoint(
                &intent,
                Some(40),
                Some(41),
                CheckpointOutcome::CheckpointCommitted,
            ),
        ));
        wrong_checkpoint_state.push(OracleObservation::recovery(
            intent.attempt_id(),
            RecoveryObservation::new(RecoveryState::CheckpointNew, Some(location()), None),
        ));
        assert_eq!(
            verify_attempt(&intent, &wrong_checkpoint_state),
            Err(OracleViolation::InvalidRecoveryEvidence)
        );
    }

    #[test]
    fn v07_task_6_2_rejects_ack_without_the_exact_delivery() {
        let intent = intent();
        let mut observations = strong_observations(&intent);
        observations.push(OracleObservation::ack(
            intent.attempt_id(),
            AckObservation::new(
                intent.message_id(),
                intent.envelope_digest(),
                location(),
                false,
            ),
        ));

        assert_eq!(
            verify_attempt(&intent, &observations),
            Err(OracleViolation::AckWithoutDelivery)
        );
    }

    #[test]
    fn v07_task_6_2_validates_every_creation_recovery_classification() {
        let intent = intent();
        let expected_namespace = [0xa5; 32];
        let expected_initial = 9;
        let complete_manifest =
            CreationManifestObservation::new(expected_namespace, expected_initial, 4);
        let base = response(
            &intent,
            DurableSendOutcome::NotSubmitted(AttemptFailureKind::Unavailable),
        );
        for creation in [
            CreationObservation::new(
                CreationState::CreationNotCommitted,
                expected_namespace,
                expected_initial,
                None,
                Some(3),
                Some(3),
            ),
            CreationObservation::new(
                CreationState::CreationUnknown,
                expected_namespace,
                expected_initial,
                None,
                Some(3),
                Some(3),
            ),
            CreationObservation::new(
                CreationState::CreationUnknown,
                expected_namespace,
                expected_initial,
                Some(complete_manifest),
                Some(3),
                Some(4),
            ),
            CreationObservation::new(
                CreationState::Created,
                expected_namespace,
                expected_initial,
                Some(complete_manifest),
                Some(3),
                Some(4),
            ),
        ] {
            assert!(
                verify_attempt(
                    &intent,
                    &[
                        base.clone(),
                        OracleObservation::creation(intent.attempt_id(), creation),
                    ],
                )
                .is_ok()
            );
        }

        for invalid_manifest in [
            CreationManifestObservation::new([0xb6; 32], expected_initial, 4),
            CreationManifestObservation::new(expected_namespace, expected_initial + 1, 4),
            CreationManifestObservation::new(expected_namespace, expected_initial, 3),
        ] {
            assert_eq!(
                verify_attempt(
                    &intent,
                    &[
                        base.clone(),
                        OracleObservation::creation(
                            intent.attempt_id(),
                            CreationObservation::new(
                                CreationState::Created,
                                expected_namespace,
                                expected_initial,
                                Some(invalid_manifest),
                                Some(3),
                                Some(4),
                            ),
                        ),
                    ],
                ),
                Err(OracleViolation::InvalidCreationRecovery)
            );
        }
    }

    #[test]
    fn v07_task_6_2_creation_history_rejects_selection_and_owner_chain_overwrite() {
        let intent = intent();
        let base = response(
            &intent,
            DurableSendOutcome::NotSubmitted(AttemptFailureKind::Unavailable),
        );
        let initial = CreationObservation::new(
            CreationState::CreationUnknown,
            [0xa5; 32],
            9,
            None,
            Some(3),
            Some(3),
        );
        let resolved = CreationObservation::new(
            CreationState::Created,
            [0xa5; 32],
            9,
            Some(CreationManifestObservation::new([0xa5; 32], 9, 4)),
            Some(3),
            Some(4),
        );
        assert!(
            verify_attempt(
                &intent,
                &[
                    base.clone(),
                    OracleObservation::creation(intent.attempt_id(), initial),
                    OracleObservation::creation(intent.attempt_id(), resolved),
                ],
            )
            .is_ok()
        );

        for overwritten in [
            CreationObservation::new(
                CreationState::Created,
                [0xb6; 32],
                9,
                Some(CreationManifestObservation::new([0xb6; 32], 9, 4)),
                Some(3),
                Some(4),
            ),
            CreationObservation::new(
                CreationState::Created,
                [0xa5; 32],
                10,
                Some(CreationManifestObservation::new([0xa5; 32], 10, 4)),
                Some(3),
                Some(4),
            ),
            CreationObservation::new(
                CreationState::Created,
                [0xa5; 32],
                9,
                Some(CreationManifestObservation::new([0xa5; 32], 9, 6)),
                Some(5),
                Some(6),
            ),
        ] {
            assert!(
                verify_attempt(
                    &intent,
                    &[
                        base.clone(),
                        OracleObservation::creation(intent.attempt_id(), initial),
                        OracleObservation::creation(intent.attempt_id(), overwritten),
                    ],
                )
                .is_err()
            );
        }
    }

    #[test]
    fn v07_task_6_2_rejects_truncated_corrupt_and_wrong_version_records() {
        let intent = intent();
        let (record, _) = encode_record(&OracleRecord::Intent(intent), 1, 0);

        let mut truncated = record.clone();
        truncated.pop();
        assert!(decode_records(&truncated).is_err());

        let mut corrupt = record.clone();
        *corrupt.last_mut().unwrap() ^= 0x01;
        assert!(decode_records(&corrupt).is_err());

        let mut wrong_version = record;
        wrong_version[5] ^= 0x01;
        assert!(decode_records(&wrong_version).is_err());
    }

    #[test]
    fn v07_task_6_2_schema_v5_round_trips_every_observation_kind() {
        let intent = intent();
        let expected_namespace = [0xa5; 32];
        let mut records = vec![
            OracleRecord::Intent(intent.clone()),
            OracleRecord::Observation(OracleObservation::wire(
                intent.attempt_id(),
                WireObservation::new(
                    APPEND_ONE_SYNCED_CODE,
                    WireStage::AppendInvocation,
                    11,
                    12,
                    13,
                    14,
                ),
            )),
            OracleRecord::Observation(OracleObservation::stored_readback(
                intent.attempt_id(),
                StoredReadback::new(canonical_envelope(&intent), location(), true, true),
            )),
            OracleRecord::Observation(OracleObservation::response(
                intent.attempt_id(),
                ResponseObservation::new(
                    DurableSendOutcome::OsSyncedAccepted,
                    Some(ReceiptObservation::new(
                        intent.message_id(),
                        intent.envelope_digest(),
                        intent.attempt_id(),
                        intent.requested_boundary(),
                        intent.session_fingerprint(),
                        location(),
                    )),
                ),
            )),
            OracleRecord::Observation(OracleObservation::delivery(
                intent.attempt_id(),
                DeliveryObservation::new(
                    intent.message_id(),
                    intent.envelope_digest(),
                    location(),
                    false,
                ),
            )),
            OracleRecord::Observation(OracleObservation::effect(
                intent.attempt_id(),
                EffectObservation::new(
                    intent.message_id(),
                    intent.envelope_digest(),
                    location(),
                    true,
                ),
            )),
            OracleRecord::Observation(OracleObservation::ack(
                intent.attempt_id(),
                AckObservation::new(
                    intent.message_id(),
                    intent.envelope_digest(),
                    location(),
                    true,
                ),
            )),
            OracleRecord::Observation(OracleObservation::checkpoint(
                intent.attempt_id(),
                checkpoint(
                    &intent,
                    Some(40),
                    Some(41),
                    CheckpointOutcome::CheckpointCommitted,
                ),
            )),
            OracleRecord::Observation(OracleObservation::creation(
                intent.attempt_id(),
                CreationObservation::new(
                    CreationState::Created,
                    expected_namespace,
                    9,
                    Some(CreationManifestObservation::new([0xb6; 32], 10, 5)),
                    Some(3),
                    Some(4),
                ),
            )),
            OracleRecord::Observation(OracleObservation::resource_epoch(
                intent.attempt_id(),
                location().resource_epoch(),
            )),
            OracleRecord::Observation(OracleObservation::recovery(
                intent.attempt_id(),
                RecoveryObservation::new(
                    RecoveryState::PresentAfterRestart,
                    Some(location()),
                    None,
                ),
            )),
        ];
        for (index, stage) in WIRE_STAGES.into_iter().enumerate().skip(1) {
            records.push(OracleRecord::Observation(OracleObservation::wire(
                intent.attempt_id(),
                WireObservation::new(
                    APPEND_ONE_SYNCED_CODE + index as u32,
                    stage,
                    index as u32 + 20,
                    index as u32 + 30,
                    index as u32 + 40,
                    index as u32 + 50,
                ),
            )));
        }
        for failure in [
            AttemptFailureKind::Unavailable,
            AttemptFailureKind::Transport,
            AttemptFailureKind::Protocol,
            AttemptFailureKind::Cancelled,
            AttemptFailureKind::Shutdown,
        ] {
            records.push(OracleRecord::Observation(OracleObservation::response(
                intent.attempt_id(),
                ResponseObservation::new(DurableSendOutcome::NotSubmitted(failure), None),
            )));
        }
        records.extend([
            OracleRecord::Observation(OracleObservation::response(
                intent.attempt_id(),
                ResponseObservation::new(
                    DurableSendOutcome::Indeterminate(AttemptFailureKind::Protocol),
                    None,
                ),
            )),
            OracleRecord::Observation(OracleObservation::response(
                intent.attempt_id(),
                ResponseObservation::new(DurableSendOutcome::BrokerAccepted, None),
            )),
            OracleRecord::Observation(OracleObservation::response(
                intent.attempt_id(),
                ResponseObservation::new(
                    DurableSendOutcome::OsSyncedAccepted,
                    Some(ReceiptObservation::new(
                        intent.message_id(),
                        intent.envelope_digest(),
                        intent.attempt_id(),
                        ConfirmationBoundary::BrokerAccepted,
                        intent.session_fingerprint(),
                        other_location(),
                    )),
                ),
            )),
            OracleRecord::Observation(OracleObservation::stored_readback(
                intent.attempt_id(),
                StoredReadback::new(canonical_envelope(&intent), other_location(), true, false),
            )),
            OracleRecord::Observation(OracleObservation::stored_readback(
                intent.attempt_id(),
                StoredReadback::new(canonical_envelope(&intent), other_location(), false, false),
            )),
            OracleRecord::Observation(OracleObservation::stored_readback(
                intent.attempt_id(),
                StoredReadback::new(canonical_envelope(&intent), other_location(), false, true),
            )),
            OracleRecord::Observation(OracleObservation::delivery(
                intent.attempt_id(),
                DeliveryObservation::new(
                    intent.message_id(),
                    intent.envelope_digest(),
                    other_location(),
                    true,
                ),
            )),
            OracleRecord::Observation(OracleObservation::effect(
                intent.attempt_id(),
                EffectObservation::new(
                    intent.message_id(),
                    intent.envelope_digest(),
                    other_location(),
                    false,
                ),
            )),
            OracleRecord::Observation(OracleObservation::ack(
                intent.attempt_id(),
                AckObservation::new(
                    intent.message_id(),
                    intent.envelope_digest(),
                    other_location(),
                    false,
                ),
            )),
            OracleRecord::Observation(OracleObservation::checkpoint(
                intent.attempt_id(),
                checkpoint(
                    &intent,
                    None,
                    None,
                    CheckpointOutcome::CheckpointNotCommitted,
                ),
            )),
            OracleRecord::Observation(OracleObservation::checkpoint(
                intent.attempt_id(),
                checkpoint(
                    &intent,
                    Some(40),
                    Some(40),
                    CheckpointOutcome::CheckpointUnknown,
                ),
            )),
            OracleRecord::Observation(OracleObservation::creation(
                intent.attempt_id(),
                CreationObservation::new(
                    CreationState::CreationNotCommitted,
                    [0xc7; 32],
                    17,
                    None,
                    None,
                    None,
                ),
            )),
            OracleRecord::Observation(OracleObservation::creation(
                intent.attempt_id(),
                CreationObservation::new(
                    CreationState::CreationUnknown,
                    [0xd8; 32],
                    18,
                    None,
                    Some(6),
                    Some(6),
                ),
            )),
        ]);
        for (index, state) in [
            RecoveryState::Undelivered,
            RecoveryState::DeliveredUncheckpointed,
            RecoveryState::CheckpointOld,
            RecoveryState::CheckpointNew,
            RecoveryState::Committed,
            RecoveryState::Redelivered,
            RecoveryState::EvictedAfterSync,
        ]
        .into_iter()
        .enumerate()
        {
            records.push(OracleRecord::Observation(OracleObservation::recovery(
                intent.attempt_id(),
                RecoveryObservation::new(
                    state,
                    (index % 2 == 0).then_some(other_location()),
                    (index % 3 == 0).then_some(100 + index as u64),
                ),
            )));
        }
        let mut bytes = Vec::new();
        let mut previous_checksum = 0;
        for (index, record) in records.iter().enumerate() {
            let (encoded, checksum) = encode_record(record, index as u64 + 1, previous_checksum);
            bytes.extend_from_slice(&encoded);
            previous_checksum = checksum;
        }

        assert_eq!(decode_records(&bytes).unwrap().records, records);
    }

    #[test]
    fn v07_task_6_2_load_rejects_suffix_deletion_reordering_and_observation_before_intent() {
        let directory = unique_directory("append-only");
        fs::create_dir(&directory).unwrap();
        let intent = intent();

        let suffix_path = directory.join("suffix.log");
        let mut suffix_store = OracleStore::new(&suffix_path);
        suffix_store.persist_intent(&intent).unwrap();
        suffix_store
            .append_observation(&response(
                &intent,
                DurableSendOutcome::Indeterminate(AttemptFailureKind::Transport),
            ))
            .unwrap();
        let (first_record, _) = encode_record(&OracleRecord::Intent(intent.clone()), 1, 0);
        fs::OpenOptions::new()
            .write(true)
            .open(&suffix_path)
            .unwrap()
            .set_len(first_record.len() as u64)
            .unwrap();
        assert!(suffix_store.load().is_err());

        let observation_one = response(
            &intent,
            DurableSendOutcome::Indeterminate(AttemptFailureKind::Transport),
        );
        let observation_two = append_wire(&intent, 1);
        let (first, first_checksum) = encode_record(&OracleRecord::Intent(intent.clone()), 1, 0);
        let (second, second_checksum) = encode_record(
            &OracleRecord::Observation(observation_one.clone()),
            2,
            first_checksum,
        );
        let (third, third_checksum) = encode_record(
            &OracleRecord::Observation(observation_two.clone()),
            3,
            second_checksum,
        );
        let reorder_path = directory.join("reorder.log");
        let reorder_store = OracleStore::new(&reorder_path);
        let reordered = [first.as_slice(), third.as_slice(), second.as_slice()].concat();
        fs::write(&reorder_path, reordered).unwrap();
        reorder_store.write_head(3, third_checksum).unwrap();
        assert!(reorder_store.load().is_err());

        let before_path = directory.join("before.log");
        let before_store = OracleStore::new(&before_path);
        let (observation, observation_checksum) =
            encode_record(&OracleRecord::Observation(observation_two), 1, 0);
        let (late_intent, late_checksum) =
            encode_record(&OracleRecord::Intent(intent), 2, observation_checksum);
        fs::write(
            &before_path,
            [observation.as_slice(), late_intent.as_slice()].concat(),
        )
        .unwrap();
        before_store.write_head(2, late_checksum).unwrap();
        assert!(before_store.load().is_err());

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn v07_task_6_2_serializes_concurrent_store_clones_on_one_path() {
        const ATTEMPTS: usize = 12;

        let directory = unique_directory("concurrent");
        fs::create_dir(&directory).unwrap();
        let path = directory.join("oracle.log");
        let store = OracleStore::new(&path);
        let barrier = Arc::new(Barrier::new(ATTEMPTS));
        let mut workers = Vec::new();

        for worker in 1..=ATTEMPTS {
            let mut store = store.clone();
            let barrier = barrier.clone();
            let mut intent = intent();
            intent.attempt_id = OracleAttemptId([worker as u8; 16]);
            let observation = response(
                &intent,
                DurableSendOutcome::Indeterminate(AttemptFailureKind::Transport),
            );
            workers.push(thread::spawn(move || {
                barrier.wait();
                store.persist_intent(&intent).unwrap();
                store.append_observation(&observation).unwrap();
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }

        assert_eq!(store.load().unwrap().len(), ATTEMPTS * 2);
        fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn v07_task_6_2_final_file_symlink_uses_one_canonical_storage_base() {
        use std::os::unix::fs::symlink;

        let directory = unique_directory("file-symlink");
        fs::create_dir_all(&directory).unwrap();
        let direct_path = directory.join("oracle.log");
        let alias_path = directory.join("oracle-alias.log");
        fs::write(&direct_path, []).unwrap();
        symlink(&direct_path, &alias_path).unwrap();

        let mut direct = OracleStore::new(&direct_path);
        let mut alias = OracleStore::new(&alias_path);
        assert_eq!(direct.path(), alias.path());
        assert!(Arc::ptr_eq(
            &direct.transaction_lock,
            &alias.transaction_lock
        ));

        let intent = intent();
        let observation = response(
            &intent,
            DurableSendOutcome::Indeterminate(AttemptFailureKind::Transport),
        );
        direct.persist_intent(&intent).unwrap();
        alias.append_observation(&observation).unwrap();
        assert_eq!(direct.load().unwrap().len(), 2);
        assert_eq!(alias.load().unwrap().len(), 2);
        assert!(suffixed_path(&direct_path, ".head").exists());
        assert!(!suffixed_path(&alias_path, ".head").exists());

        fs::remove_dir_all(directory).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn v07_task_6_2_path_lock_unifies_dot_parent_and_symlink_aliases() {
        use std::os::unix::fs::symlink;

        let directory = unique_directory("path-alias");
        let real = directory.join("real");
        let child = real.join("child");
        let alias = directory.join("alias");
        fs::create_dir_all(&child).unwrap();
        symlink(&real, &alias).unwrap();

        let direct = OracleStore::new(real.join("oracle.log"));
        let dotted = OracleStore::new(real.join(".").join("oracle.log"));
        let parent = OracleStore::new(child.join("..").join("oracle.log"));
        let symlinked = OracleStore::new(alias.join("oracle.log"));
        assert!(Arc::ptr_eq(
            &direct.transaction_lock,
            &dotted.transaction_lock
        ));
        assert!(Arc::ptr_eq(
            &direct.transaction_lock,
            &parent.transaction_lock
        ));
        assert!(Arc::ptr_eq(
            &direct.transaction_lock,
            &symlinked.transaction_lock
        ));

        let stores = [
            direct.clone(),
            dotted.clone(),
            parent.clone(),
            symlinked.clone(),
            direct.clone(),
            dotted,
            parent,
            symlinked,
        ];
        let barrier = Arc::new(Barrier::new(stores.len()));
        let workers: Vec<_> = stores
            .into_iter()
            .enumerate()
            .map(|(worker, mut store)| {
                let barrier = barrier.clone();
                let mut intent = intent();
                intent.attempt_id = OracleAttemptId([worker as u8 + 1; 16]);
                thread::spawn(move || {
                    barrier.wait();
                    store.persist_intent(&intent).unwrap();
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
        let existing_alias = OracleStore::new(alias.join("oracle.log"));
        assert!(Arc::ptr_eq(
            &direct.transaction_lock,
            &existing_alias.transaction_lock
        ));
        assert_eq!(direct.load().unwrap().len(), 8);

        fs::remove_dir_all(directory).unwrap();
    }
}
