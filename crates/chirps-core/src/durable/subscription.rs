use super::message::{DurableMessageId, EnvelopeDigest, ResourceEpoch};
use alopex_chirps_wire::node_id::NodeId;
use thiserror::Error;

/// The caller's mandatory selection for a new subscription frontier.
///
/// No default exists: callers must explicitly choose retained history, the
/// captured end, or an exact inclusive offset. `Exact(0)` is a valid offset and
/// is never used to represent an absent checkpoint.
///
/// ```compile_fail
/// use alopex_chirps_core::durable::InitialPosition;
/// let _: InitialPosition = Default::default();
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InitialPosition {
    /// Resolve once to the oldest retained offset in the creation observation.
    EarliestRetained,
    /// Resolve once to the captured end-exclusive offset, after existing data.
    LatestAfterCapturedEnd,
    /// Resolve once to this exact inclusive offset.
    Exact(u64),
}

macro_rules! fixed_bytes {
    ($name:ident, $length:expr, $summary:literal) => {
        #[doc = $summary]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name([u8; $length]);

        impl $name {
            /// Wraps an identity produced by the responsible manifest or store.
            #[must_use]
            pub const fn from_bytes(bytes: [u8; $length]) -> Self {
                Self(bytes)
            }

            /// Returns the fixed-width identity.
            #[must_use]
            pub const fn as_bytes(&self) -> &[u8; $length] {
                &self.0
            }
        }
    };
}

fixed_bytes!(
    SubscriptionId,
    16,
    "The provider-neutral identity of one immutable subscription namespace."
);
fixed_bytes!(
    CheckpointDirectoryId,
    32,
    "A stable digest identifying the one checkpoint directory allowed to recover an unknown creation."
);

/// Immutable namespace and owner binding of a successfully opened subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionBinding {
    subscription_id: SubscriptionId,
    target: NodeId,
    generation: u64,
    partition: u32,
    owner_epoch: u64,
    lifecycle_generation: u64,
}

impl SubscriptionBinding {
    /// Creates a binding after the manifest and current owner record are
    /// durably verified by the checkpoint store.
    #[must_use]
    pub const fn new(
        subscription_id: SubscriptionId,
        target: NodeId,
        generation: u64,
        partition: u32,
        owner_epoch: u64,
        lifecycle_generation: u64,
    ) -> Self {
        Self {
            subscription_id,
            target,
            generation,
            partition,
            owner_epoch,
            lifecycle_generation,
        }
    }

    /// Returns the immutable subscription namespace identity.
    #[must_use]
    pub const fn subscription_id(self) -> SubscriptionId {
        self.subscription_id
    }

    /// Returns the target inbox identity.
    #[must_use]
    pub const fn target(self) -> NodeId {
        self.target
    }

    /// Returns the inbox generation.
    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }

    /// Returns the explicit partition.
    #[must_use]
    pub const fn partition(self) -> u32 {
        self.partition
    }

    /// Returns the durable local owner epoch.
    #[must_use]
    pub const fn owner_epoch(self) -> u64 {
        self.owner_epoch
    }

    /// Returns the lifecycle generation that admitted this subscription.
    #[must_use]
    pub const fn lifecycle_generation(self) -> u64 {
        self.lifecycle_generation
    }
}

/// Immutable authority for the only operation allowed after creation becomes
/// ambiguous: recover or reopen the same checkpoint directory and selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CreationRecoveryBinding {
    checkpoint_directory_id: CheckpointDirectoryId,
    subscription_id: SubscriptionId,
    initial_position: InitialPosition,
}

impl CreationRecoveryBinding {
    /// Binds an unknown creation to its original directory, namespace, and
    /// requested initial selection.
    #[must_use]
    pub const fn new(
        checkpoint_directory_id: CheckpointDirectoryId,
        subscription_id: SubscriptionId,
        initial_position: InitialPosition,
    ) -> Self {
        Self {
            checkpoint_directory_id,
            subscription_id,
            initial_position,
        }
    }

    /// Returns the stable checkpoint-directory identity.
    #[must_use]
    pub const fn checkpoint_directory_id(self) -> CheckpointDirectoryId {
        self.checkpoint_directory_id
    }

    /// Returns the original subscription identity.
    #[must_use]
    pub const fn subscription_id(self) -> SubscriptionId {
        self.subscription_id
    }

    /// Returns the original explicit initial selection.
    #[must_use]
    pub const fn initial_position(self) -> InitialPosition {
        self.initial_position
    }
}

/// A bounded known-old creation failure, before an ambiguous candidate write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum CreationFailureKind {
    /// The checkpoint directory is already owned by another local process.
    #[error("checkpoint directory is already owned")]
    OwnerLockUnavailable,
    /// Durable local storage failed before the candidate could become ambiguous.
    #[error("checkpoint storage is unavailable")]
    StorageUnavailable,
    /// Capacity admission rejected the creation before mutation.
    #[error("checkpoint capacity is unavailable")]
    CapacityUnavailable,
    /// The requested selection conflicts with the atomic creation observation.
    #[error("initial subscription position is invalid")]
    InvalidInitialPosition,
}

/// Public result of installing or reopening a subscription namespace.
///
/// `CreationNotCommitted` is known-old and may be retried as a fresh creation.
/// `CreationUnknown` permits only recovery with its exact binding. `Created`
/// is returned only after manifest and owner durability is proven.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscriptionCreationOutcome {
    /// No creation candidate could have become durable.
    CreationNotCommitted(CreationFailureKind),
    /// Candidate reachability is unknown; exact-directory recovery is required.
    CreationUnknown(CreationRecoveryBinding),
    /// The namespace and owner epoch are durably installed.
    Created(SubscriptionBinding),
}

/// One record returned by a bounded, provider-neutral poll decoder.
///
/// Construction validates the received logical identity as UUIDv4 and rejects
/// an empty canonical envelope. Provider framing, count, length, and trailing
/// bytes must already have been checked before calling this constructor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckedPollRecord {
    offset: u64,
    message_id: DurableMessageId,
    envelope_digest: EnvelopeDigest,
    canonical_bytes: Vec<u8>,
}

impl CheckedPollRecord {
    /// Creates the sole record allowed in one checked poll observation.
    pub fn try_new(
        offset: u64,
        message_id_bytes: [u8; 16],
        envelope_digest: EnvelopeDigest,
        canonical_bytes: Vec<u8>,
    ) -> Result<Self, PollRecordError> {
        let message_id = DurableMessageId::try_from_wire_bytes(message_id_bytes)
            .map_err(|_| PollRecordError::InvalidMessageId)?;
        if canonical_bytes.is_empty() {
            return Err(PollRecordError::EmptyCanonicalEnvelope);
        }
        Ok(Self {
            offset,
            message_id,
            envelope_digest,
            canonical_bytes,
        })
    }

    /// Returns the broker offset of this record.
    #[must_use]
    pub const fn offset(&self) -> u64 {
        self.offset
    }

    /// Returns the checked logical message identity.
    #[must_use]
    pub const fn message_id(&self) -> DurableMessageId {
        self.message_id
    }

    /// Returns the verified canonical-envelope digest.
    #[must_use]
    pub const fn envelope_digest(&self) -> EnvelopeDigest {
        self.envelope_digest
    }

    /// Returns the bounded canonical envelope bytes.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }
}

/// A rejected record from the checked poll decoder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum PollRecordError {
    /// The decoded logical identity was not an RFC 4122 UUIDv4 value.
    #[error("poll record message identity is not UUIDv4")]
    InvalidMessageId,
    /// A canonical Durable envelope cannot be empty.
    #[error("poll record canonical envelope is empty")]
    EmptyCanonicalEnvelope,
}

/// One atomic provider-neutral observation used for replay decisions.
///
/// `end_exclusive` is the captured next-append offset `H`; `oldest_available`
/// is the retained lower bound and equals `H` for an empty partition. The
/// optional record enforces a maximum count of one. Normal ordering is only
/// within one target/generation/partition by offset; no cross-target,
/// cross-partition, or global order is promised.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollObservation {
    resource_epoch: ResourceEpoch,
    end_exclusive: u64,
    oldest_available: u64,
    record: Option<CheckedPollRecord>,
}

impl PollObservation {
    /// Validates the atomic bounds and the optional record's retained range.
    pub fn try_new(
        resource_epoch: ResourceEpoch,
        end_exclusive: u64,
        oldest_available: u64,
        record: Option<CheckedPollRecord>,
    ) -> Result<Self, PollObservationError> {
        if oldest_available > end_exclusive {
            return Err(PollObservationError::OldestPastEnd {
                oldest_available,
                end_exclusive,
            });
        }
        if let Some(record) = &record {
            if record.offset < oldest_available {
                return Err(PollObservationError::RecordBeforeOldest {
                    offset: record.offset,
                    oldest_available,
                });
            }
            if record.offset >= end_exclusive {
                return Err(PollObservationError::RecordAtOrBeyondEnd {
                    offset: record.offset,
                    end_exclusive,
                });
            }
        }
        Ok(Self {
            resource_epoch,
            end_exclusive,
            oldest_available,
            record,
        })
    }

    /// Returns the observed immutable resource incarnation.
    #[must_use]
    pub const fn resource_epoch(&self) -> ResourceEpoch {
        self.resource_epoch
    }

    /// Returns the next-append, end-exclusive offset `H`.
    #[must_use]
    pub const fn end_exclusive(&self) -> u64 {
        self.end_exclusive
    }

    /// Returns the oldest currently retained offset.
    #[must_use]
    pub const fn oldest_available(&self) -> u64 {
        self.oldest_available
    }

    /// Returns the zero-or-one checked record.
    #[must_use]
    pub const fn record(&self) -> Option<&CheckedPollRecord> {
        self.record.as_ref()
    }

    /// Resolves the explicit initial selection exactly once against this
    /// observation. Exact `H` is valid and represents the current tail.
    pub fn resolve_initial(&self, initial_position: InitialPosition) -> Result<u64, ReplayError> {
        match initial_position {
            InitialPosition::EarliestRetained => Ok(self.oldest_available),
            InitialPosition::LatestAfterCapturedEnd => Ok(self.end_exclusive),
            InitialPosition::Exact(expected) => self.check_bounds(expected).map(|()| expected),
        }
    }

    /// Applies the complete checked replay truth table to one inclusive
    /// expected offset.
    pub fn observe(&self, expected: u64) -> Result<PollResolution<'_>, ReplayError> {
        self.check_bounds(expected)?;
        if expected == self.end_exclusive {
            return match &self.record {
                None => Ok(PollResolution::Tail),
                Some(record) => Err(ReplayError::UnexpectedRecordOffset {
                    expected,
                    actual: record.offset,
                }),
            };
        }
        match &self.record {
            Some(record) if record.offset == expected => Ok(PollResolution::Record(record)),
            Some(record) => Err(ReplayError::UnexpectedRecordOffset {
                expected,
                actual: record.offset,
            }),
            None => Err(ReplayError::MissingExpectedRecord {
                expected,
                end_exclusive: self.end_exclusive,
            }),
        }
    }

    fn check_bounds(&self, expected: u64) -> Result<(), ReplayError> {
        if expected < self.oldest_available {
            return Err(ReplayError::RetentionGap {
                expected,
                oldest_available: self.oldest_available,
            });
        }
        if expected > self.end_exclusive {
            return Err(ReplayError::CheckpointConflict {
                expected,
                end_exclusive: self.end_exclusive,
            });
        }
        Ok(())
    }
}

/// A malformed atomic poll observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum PollObservationError {
    /// The retained lower bound cannot be after the captured end.
    #[error("oldest available offset {oldest_available} is after end {end_exclusive}")]
    OldestPastEnd {
        /// Reported retained lower bound.
        oldest_available: u64,
        /// Captured next-append offset.
        end_exclusive: u64,
    },
    /// A returned record is already below the retained range.
    #[error("record offset {offset} is before oldest available {oldest_available}")]
    RecordBeforeOldest {
        /// Returned record offset.
        offset: u64,
        /// Reported retained lower bound.
        oldest_available: u64,
    },
    /// A returned record must be strictly before the captured end.
    #[error("record offset {offset} is at or beyond end {end_exclusive}")]
    RecordAtOrBeyondEnd {
        /// Returned record offset.
        offset: u64,
        /// Captured next-append offset.
        end_exclusive: u64,
    },
}

/// Result of applying the replay truth table to a checked observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollResolution<'a> {
    /// The exact inclusive expected record is present.
    Record(&'a CheckedPollRecord),
    /// `expected == H` and no record is present: normal current tail.
    Tail,
}

/// Computes the next inclusive expected offset from canonical checkpoint state.
///
/// `None` selects the manifest's already-resolved initial frontier. A present
/// checkpoint at offset zero advances to one; absence is never encoded as zero.
pub const fn expected_offset(
    checkpoint: Option<u64>,
    resolved_initial: u64,
) -> Result<u64, ReplayError> {
    match checkpoint {
        None => Ok(resolved_initial),
        Some(value) => match value.checked_add(1) {
            Some(expected) => Ok(expected),
            None => Err(ReplayError::OffsetExhausted { checkpoint: value }),
        },
    }
}

/// A fail-stop replay decision error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ReplayError {
    /// The committed checkpoint cannot be advanced without overflow.
    #[error("checkpoint offset {checkpoint} is exhausted")]
    OffsetExhausted {
        /// The canonical committed checkpoint.
        checkpoint: u64,
    },
    /// The inclusive expected offset has already been evicted by retention.
    #[error("expected offset {expected} is before oldest available {oldest_available}")]
    RetentionGap {
        /// Canonical inclusive expected offset.
        expected: u64,
        /// Current retained lower bound.
        oldest_available: u64,
    },
    /// Canonical state is ahead of the broker's captured end.
    #[error("expected offset {expected} is after captured end {end_exclusive}")]
    CheckpointConflict {
        /// Canonical inclusive expected offset.
        expected: u64,
        /// Captured next-append offset.
        end_exclusive: u64,
    },
    /// A record was required inside the retained interval but was absent.
    #[error("expected record {expected} is missing before end {end_exclusive}")]
    MissingExpectedRecord {
        /// Canonical inclusive expected offset.
        expected: u64,
        /// Captured next-append offset.
        end_exclusive: u64,
    },
    /// The decoder returned a different record than the inclusive expectation.
    #[error("expected record {expected}, but observed {actual}")]
    UnexpectedRecordOffset {
        /// Canonical inclusive expected offset.
        expected: u64,
        /// Returned record offset.
        actual: u64,
    },
}

/// Immutable context assigned when a checked record wins delivery admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DeliveryContext {
    subscription: SubscriptionBinding,
    delivery_attempt: u64,
}

impl DeliveryContext {
    /// Binds a new attempt number to the current verified subscription owner.
    #[must_use]
    pub const fn new(subscription: SubscriptionBinding, delivery_attempt: u64) -> Self {
        Self {
            subscription,
            delivery_attempt,
        }
    }
}

/// One application delivery and its uniquely bound arbitration handle.
///
/// Delivery ordering is only by offset within this handle's exact
/// target/generation/partition. No order is guaranteed between targets,
/// partitions, or subscriptions.
#[derive(Debug, PartialEq, Eq)]
pub struct Delivery {
    canonical_bytes: Vec<u8>,
    handle: DeliveryHandle,
}

impl Delivery {
    /// Consumes a checked record and binds every identity used by ack,
    /// release, timeout, owner fencing, and shutdown arbitration.
    #[must_use]
    pub fn from_checked_record(context: DeliveryContext, record: CheckedPollRecord) -> Self {
        let subscription = context.subscription;
        Self {
            canonical_bytes: record.canonical_bytes,
            handle: DeliveryHandle {
                subscription_id: subscription.subscription_id,
                target: subscription.target,
                generation: subscription.generation,
                partition: subscription.partition,
                offset: record.offset,
                message_id: record.message_id,
                envelope_digest: record.envelope_digest,
                delivery_attempt: context.delivery_attempt,
                owner_epoch: subscription.owner_epoch,
                lifecycle_generation: subscription.lifecycle_generation,
                checkpoint_attempt: 0,
                state: DeliveryHandleState::Open,
            },
        }
    }

    /// Returns the immutable canonical envelope bytes.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    /// Returns the fully bound delivery handle.
    #[must_use]
    pub const fn handle(&self) -> &DeliveryHandle {
        &self.handle
    }

    /// Returns exclusive access to arbitrate this handle's next transition.
    #[must_use]
    pub fn handle_mut(&mut self) -> &mut DeliveryHandle {
        &mut self.handle
    }
}

/// The local arbitration state of one fully bound delivery handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeliveryHandleState {
    /// Ack, release, timeout, or shutdown may win next.
    Open,
    /// Checkpoint installation is running and is not terminal.
    AckInstalling,
    /// A known-old checkpoint failure permits ack retry on this same handle.
    AckRetryable,
    /// The canonical checkpoint was committed.
    AckTerminalCommitted,
    /// Checkpoint reachability became unknown; recovery is required.
    AckTerminalUnknown,
    /// Application release/nack won arbitration.
    Released,
    /// Delivery timeout won arbitration.
    TimedOut,
    /// Shutdown fenced this handle before checkpoint installation began.
    ShutdownFenced,
}

impl DeliveryHandleState {
    /// Returns whether no transition may ever revive this handle.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::AckTerminalCommitted
                | Self::AckTerminalUnknown
                | Self::Released
                | Self::TimedOut
                | Self::ShutdownFenced
        )
    }
}

/// A delivery handle bound to every identity participating in arbitration.
#[derive(Debug, PartialEq, Eq)]
pub struct DeliveryHandle {
    subscription_id: SubscriptionId,
    target: NodeId,
    generation: u64,
    partition: u32,
    offset: u64,
    message_id: DurableMessageId,
    envelope_digest: EnvelopeDigest,
    delivery_attempt: u64,
    owner_epoch: u64,
    lifecycle_generation: u64,
    checkpoint_attempt: u64,
    state: DeliveryHandleState,
}

/// Opaque proof that checkpoint installation started for one exact handle.
///
/// The binding carries every arbitration identity. A checkpoint outcome from a
/// stale, different-owner, different-attempt, or different-message handle
/// cannot finish another handle's installation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CheckpointInstallBinding {
    subscription_id: SubscriptionId,
    target: NodeId,
    generation: u64,
    partition: u32,
    offset: u64,
    message_id: DurableMessageId,
    envelope_digest: EnvelopeDigest,
    delivery_attempt: u64,
    owner_epoch: u64,
    lifecycle_generation: u64,
    checkpoint_attempt: u64,
}

impl CheckpointInstallBinding {
    fn from_handle(handle: &DeliveryHandle) -> Self {
        Self {
            subscription_id: handle.subscription_id,
            target: handle.target,
            generation: handle.generation,
            partition: handle.partition,
            offset: handle.offset,
            message_id: handle.message_id,
            envelope_digest: handle.envelope_digest,
            delivery_attempt: handle.delivery_attempt,
            owner_epoch: handle.owner_epoch,
            lifecycle_generation: handle.lifecycle_generation,
            checkpoint_attempt: handle.checkpoint_attempt,
        }
    }

    fn matches(&self, handle: &DeliveryHandle) -> bool {
        self.subscription_id == handle.subscription_id
            && self.target == handle.target
            && self.generation == handle.generation
            && self.partition == handle.partition
            && self.offset == handle.offset
            && self.message_id == handle.message_id
            && self.envelope_digest == handle.envelope_digest
            && self.delivery_attempt == handle.delivery_attempt
            && self.owner_epoch == handle.owner_epoch
            && self.lifecycle_generation == handle.lifecycle_generation
            && self.checkpoint_attempt == handle.checkpoint_attempt
    }

    /// Returns the immutable subscription identity.
    #[must_use]
    pub const fn subscription_id(self) -> SubscriptionId {
        self.subscription_id
    }

    /// Returns the target inbox.
    #[must_use]
    pub const fn target(self) -> NodeId {
        self.target
    }

    /// Returns the inbox generation.
    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }

    /// Returns the explicit partition.
    #[must_use]
    pub const fn partition(self) -> u32 {
        self.partition
    }

    /// Returns the delivered broker offset.
    #[must_use]
    pub const fn offset(self) -> u64 {
        self.offset
    }

    /// Returns the checked logical message identity.
    #[must_use]
    pub const fn message_id(self) -> DurableMessageId {
        self.message_id
    }

    /// Returns the canonical-envelope digest.
    #[must_use]
    pub const fn envelope_digest(self) -> EnvelopeDigest {
        self.envelope_digest
    }

    /// Returns the bound delivery-attempt number.
    #[must_use]
    pub const fn delivery_attempt(self) -> u64 {
        self.delivery_attempt
    }

    /// Returns the bound durable owner epoch.
    #[must_use]
    pub const fn owner_epoch(self) -> u64 {
        self.owner_epoch
    }

    /// Returns the bound lifecycle generation.
    #[must_use]
    pub const fn lifecycle_generation(self) -> u64 {
        self.lifecycle_generation
    }

    /// Returns the handle-local checkpoint-install attempt generation.
    #[must_use]
    pub const fn checkpoint_attempt(self) -> u64 {
        self.checkpoint_attempt
    }
}

impl DeliveryHandle {
    /// Returns the immutable subscription identity.
    #[must_use]
    pub const fn subscription_id(&self) -> SubscriptionId {
        self.subscription_id
    }

    /// Returns the target inbox.
    #[must_use]
    pub const fn target(&self) -> NodeId {
        self.target
    }

    /// Returns the inbox generation.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the explicit partition.
    #[must_use]
    pub const fn partition(&self) -> u32 {
        self.partition
    }

    /// Returns the delivered broker offset.
    #[must_use]
    pub const fn offset(&self) -> u64 {
        self.offset
    }

    /// Returns the checked logical message identity.
    #[must_use]
    pub const fn message_id(&self) -> DurableMessageId {
        self.message_id
    }

    /// Returns the canonical-envelope digest.
    #[must_use]
    pub const fn envelope_digest(&self) -> EnvelopeDigest {
        self.envelope_digest
    }

    /// Returns the monotonically assigned delivery-attempt number.
    #[must_use]
    pub const fn delivery_attempt(&self) -> u64 {
        self.delivery_attempt
    }

    /// Returns the local owner epoch that admitted this delivery.
    #[must_use]
    pub const fn owner_epoch(&self) -> u64 {
        self.owner_epoch
    }

    /// Returns the lifecycle generation that admitted this delivery.
    #[must_use]
    pub const fn lifecycle_generation(&self) -> u64 {
        self.lifecycle_generation
    }

    /// Returns the current arbitration state.
    #[must_use]
    pub const fn state(&self) -> DeliveryHandleState {
        self.state
    }

    /// Starts checkpoint installation from `Open` or known-old retry state.
    pub fn begin_ack(&mut self) -> Result<CheckpointInstallBinding, HandleTransitionError> {
        match self.state {
            DeliveryHandleState::Open | DeliveryHandleState::AckRetryable => {
                self.checkpoint_attempt = self
                    .checkpoint_attempt
                    .checked_add(1)
                    .ok_or(HandleTransitionError::CheckpointAttemptExhausted)?;
                self.state = DeliveryHandleState::AckInstalling;
                Ok(CheckpointInstallBinding::from_handle(self))
            }
            DeliveryHandleState::AckInstalling => Err(HandleTransitionError::AckInstallInProgress),
            terminal => Err(HandleTransitionError::Terminal(terminal)),
        }
    }

    /// Finishes the active checkpoint installation without rewriting its
    /// phase-specific outcome. Only known-old returns to retryable state.
    pub fn finish_ack(
        &mut self,
        binding: &CheckpointInstallBinding,
        outcome: CheckpointOutcome,
    ) -> Result<(), HandleTransitionError> {
        if self.state != DeliveryHandleState::AckInstalling {
            return if self.state.is_terminal() {
                Err(HandleTransitionError::Terminal(self.state))
            } else {
                Err(HandleTransitionError::AckNotInstalling)
            };
        }
        if !binding.matches(self) {
            return Err(HandleTransitionError::BindingMismatch);
        }
        self.state = match outcome {
            CheckpointOutcome::CheckpointCommitted => DeliveryHandleState::AckTerminalCommitted,
            CheckpointOutcome::CheckpointNotCommitted => DeliveryHandleState::AckRetryable,
            CheckpointOutcome::CheckpointUnknown => DeliveryHandleState::AckTerminalUnknown,
        };
        Ok(())
    }

    /// Records application release or nack without advancing the checkpoint.
    pub fn release(&mut self) -> Result<(), HandleTransitionError> {
        self.finish_terminal(DeliveryHandleState::Released)
    }

    /// Records timeout without advancing the checkpoint.
    pub fn timeout(&mut self) -> Result<(), HandleTransitionError> {
        self.finish_terminal(DeliveryHandleState::TimedOut)
    }

    /// Fences an open/retryable handle when shutdown admission closes.
    pub fn shutdown_fence(&mut self) -> Result<(), HandleTransitionError> {
        self.finish_terminal(DeliveryHandleState::ShutdownFenced)
    }

    fn finish_terminal(
        &mut self,
        terminal: DeliveryHandleState,
    ) -> Result<(), HandleTransitionError> {
        match self.state {
            DeliveryHandleState::Open | DeliveryHandleState::AckRetryable => {
                self.state = terminal;
                Ok(())
            }
            DeliveryHandleState::AckInstalling => Err(HandleTransitionError::AckInstallInProgress),
            state => Err(HandleTransitionError::Terminal(state)),
        }
    }
}

/// Phase-specific result of one canonical checkpoint installation attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CheckpointOutcome {
    /// The checksummed checkpoint record reached its required sync boundary.
    CheckpointCommitted,
    /// A known-old failure occurred before candidate reachability was ambiguous.
    CheckpointNotCommitted,
    /// Candidate reachability is unknown and partition recovery is required.
    CheckpointUnknown,
}

/// A rejected delivery-handle arbitration transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum HandleTransitionError {
    /// A checkpoint install is already running and is deliberately nonterminal.
    #[error("checkpoint installation is already in progress")]
    AckInstallInProgress,
    /// A checkpoint outcome was supplied without an active installation.
    #[error("checkpoint installation has not started")]
    AckNotInstalling,
    /// The outcome belongs to a different immutable delivery identity.
    #[error("checkpoint installation binding does not match this delivery handle")]
    BindingMismatch,
    /// The per-handle checkpoint-install attempt generation was exhausted.
    #[error("checkpoint installation attempt generation is exhausted")]
    CheckpointAttemptExhausted,
    /// Terminal handles are permanently fenced and cannot be revived.
    #[error("delivery handle is terminal in state {0:?}")]
    Terminal(DeliveryHandleState),
}

#[cfg(test)]
mod v07_task_2_2 {
    use super::*;
    use crate::durable::{DurableMessageId, EnvelopeDigest, ResourceEpoch, ResourceId};
    use alopex_chirps_wire::node_id::NodeId;

    fn epoch() -> ResourceEpoch {
        ResourceEpoch::new(ResourceId::from_bytes([0x11; 16]), 7)
    }

    fn valid_message_id_bytes() -> [u8; 16] {
        let mut bytes = [0x22; 16];
        bytes[6] = 0x42;
        bytes[8] = 0x82;
        bytes
    }

    fn record(offset: u64) -> CheckedPollRecord {
        CheckedPollRecord::try_new(
            offset,
            valid_message_id_bytes(),
            EnvelopeDigest::from_bytes([0x33; 32]),
            b"canonical-envelope".to_vec(),
        )
        .expect("checked poll record")
    }

    fn subscription_binding() -> SubscriptionBinding {
        SubscriptionBinding::new(
            SubscriptionId::from_bytes([0x44; 16]),
            NodeId::new(),
            5,
            3,
            9,
            2,
        )
    }

    fn delivery() -> Delivery {
        Delivery::from_checked_record(DeliveryContext::new(subscription_binding(), 12), record(41))
    }

    #[test]
    fn initial_position_is_explicit_and_exact_zero_is_not_absence() {
        assert_ne!(InitialPosition::EarliestRetained, InitialPosition::Exact(0));
        assert_ne!(
            InitialPosition::LatestAfterCapturedEnd,
            InitialPosition::Exact(0)
        );
    }

    #[test]
    fn creation_unknown_preserves_the_only_valid_recovery_binding() {
        let recovery = CreationRecoveryBinding::new(
            CheckpointDirectoryId::from_bytes([0x55; 32]),
            SubscriptionId::from_bytes([0x44; 16]),
            InitialPosition::Exact(17),
        );
        let outcome = SubscriptionCreationOutcome::CreationUnknown(recovery);
        let SubscriptionCreationOutcome::CreationUnknown(actual) = outcome else {
            panic!("creation must remain unknown");
        };

        assert_eq!(
            actual.checkpoint_directory_id(),
            CheckpointDirectoryId::from_bytes([0x55; 32])
        );
        assert_eq!(
            actual.subscription_id(),
            SubscriptionId::from_bytes([0x44; 16])
        );
        assert_eq!(actual.initial_position(), InitialPosition::Exact(17));
    }

    #[test]
    fn creation_outcomes_keep_known_old_unknown_and_committed_distinct() {
        let known_old = SubscriptionCreationOutcome::CreationNotCommitted(
            CreationFailureKind::StorageUnavailable,
        );
        let created = SubscriptionCreationOutcome::Created(subscription_binding());

        assert!(matches!(
            known_old,
            SubscriptionCreationOutcome::CreationNotCommitted(
                CreationFailureKind::StorageUnavailable
            )
        ));
        assert!(matches!(created, SubscriptionCreationOutcome::Created(_)));
    }

    #[test]
    fn checked_poll_observation_rejects_impossible_bounds_and_records() {
        assert_eq!(
            PollObservation::try_new(epoch(), 10, 11, None),
            Err(PollObservationError::OldestPastEnd {
                oldest_available: 11,
                end_exclusive: 10,
            })
        );
        assert_eq!(
            PollObservation::try_new(epoch(), 10, 4, Some(record(3))),
            Err(PollObservationError::RecordBeforeOldest {
                offset: 3,
                oldest_available: 4,
            })
        );
        assert_eq!(
            PollObservation::try_new(epoch(), 10, 4, Some(record(10))),
            Err(PollObservationError::RecordAtOrBeyondEnd {
                offset: 10,
                end_exclusive: 10,
            })
        );
    }

    #[test]
    fn initial_selection_resolves_once_against_atomic_bounds() {
        let observation =
            PollObservation::try_new(epoch(), 20, 7, None).expect("valid atomic observation");

        assert_eq!(
            observation.resolve_initial(InitialPosition::EarliestRetained),
            Ok(7)
        );
        assert_eq!(
            observation.resolve_initial(InitialPosition::LatestAfterCapturedEnd),
            Ok(20)
        );
        assert_eq!(
            observation.resolve_initial(InitialPosition::Exact(13)),
            Ok(13)
        );
        assert_eq!(
            observation.resolve_initial(InitialPosition::Exact(6)),
            Err(ReplayError::RetentionGap {
                expected: 6,
                oldest_available: 7,
            })
        );
        assert_eq!(
            observation.resolve_initial(InitialPosition::Exact(21)),
            Err(ReplayError::CheckpointConflict {
                expected: 21,
                end_exclusive: 20,
            })
        );
    }

    #[test]
    fn checkpoint_zero_advances_to_one_and_max_is_typed_exhaustion() {
        assert_eq!(expected_offset(None, 37), Ok(37));
        assert_eq!(expected_offset(Some(0), 37), Ok(1));
        assert_eq!(
            expected_offset(Some(u64::MAX), 37),
            Err(ReplayError::OffsetExhausted {
                checkpoint: u64::MAX,
            })
        );
    }

    #[test]
    fn poll_truth_table_distinguishes_record_tail_gap_and_conflict() {
        let with_record = PollObservation::try_new(epoch(), 20, 7, Some(record(13)))
            .expect("valid record observation");
        assert!(matches!(
            with_record.observe(13),
            Ok(PollResolution::Record(actual)) if actual.offset() == 13
        ));
        assert_eq!(
            with_record.observe(12),
            Err(ReplayError::UnexpectedRecordOffset {
                expected: 12,
                actual: 13,
            })
        );

        let tail = PollObservation::try_new(epoch(), 20, 7, None).expect("valid empty observation");
        assert_eq!(tail.observe(20), Ok(PollResolution::Tail));
        assert_eq!(
            tail.observe(13),
            Err(ReplayError::MissingExpectedRecord {
                expected: 13,
                end_exclusive: 20,
            })
        );
        assert_eq!(
            tail.observe(6),
            Err(ReplayError::RetentionGap {
                expected: 6,
                oldest_available: 7,
            })
        );
        assert_eq!(
            tail.observe(21),
            Err(ReplayError::CheckpointConflict {
                expected: 21,
                end_exclusive: 20,
            })
        );
    }

    #[test]
    fn checked_record_rejects_non_uuid_v4_and_empty_envelope() {
        assert_eq!(
            CheckedPollRecord::try_new(
                1,
                [0_u8; 16],
                EnvelopeDigest::from_bytes([0x33; 32]),
                b"canonical-envelope".to_vec(),
            ),
            Err(PollRecordError::InvalidMessageId)
        );
        assert_eq!(
            CheckedPollRecord::try_new(
                1,
                valid_message_id_bytes(),
                EnvelopeDigest::from_bytes([0x33; 32]),
                Vec::new(),
            ),
            Err(PollRecordError::EmptyCanonicalEnvelope)
        );
    }

    #[test]
    fn delivery_handle_is_bound_to_every_arbitration_identity() {
        let binding = subscription_binding();
        let delivery = Delivery::from_checked_record(DeliveryContext::new(binding, 12), record(41));
        let handle = delivery.handle();

        assert_eq!(handle.subscription_id(), binding.subscription_id());
        assert_eq!(handle.target(), binding.target());
        assert_eq!(handle.generation(), 5);
        assert_eq!(handle.partition(), 3);
        assert_eq!(handle.offset(), 41);
        assert_eq!(handle.message_id().as_bytes(), &valid_message_id_bytes());
        assert_eq!(
            handle.envelope_digest(),
            EnvelopeDigest::from_bytes([0x33; 32])
        );
        assert_eq!(handle.delivery_attempt(), 12);
        assert_eq!(handle.owner_epoch(), 9);
        assert_eq!(handle.lifecycle_generation(), 2);
        assert_eq!(delivery.canonical_bytes(), b"canonical-envelope");
    }

    #[test]
    fn known_old_checkpoint_is_retryable_only_through_the_same_handle() {
        let mut delivery = delivery();
        let handle = delivery.handle_mut();

        let first_install = handle.begin_ack().expect("open handle begins ack");
        assert_eq!(handle.state(), DeliveryHandleState::AckInstalling);
        assert!(!handle.state().is_terminal());
        handle
            .finish_ack(&first_install, CheckpointOutcome::CheckpointNotCommitted)
            .expect("known-old remains retryable");
        assert_eq!(handle.state(), DeliveryHandleState::AckRetryable);
        let second_install = handle.begin_ack().expect("same handle retries ack");
        assert_eq!(handle.state(), DeliveryHandleState::AckInstalling);
        assert_ne!(
            first_install.checkpoint_attempt(),
            second_install.checkpoint_attempt()
        );
        assert_eq!(
            handle.finish_ack(&first_install, CheckpointOutcome::CheckpointCommitted),
            Err(HandleTransitionError::BindingMismatch)
        );
        assert_eq!(handle.state(), DeliveryHandleState::AckInstalling);
        handle
            .finish_ack(&second_install, CheckpointOutcome::CheckpointCommitted)
            .expect("current retry binding commits");
    }

    #[test]
    fn checkpoint_result_from_a_different_handle_cannot_advance_state() {
        let mut first = delivery();
        let mut second = delivery();
        let first_install = first.handle_mut().begin_ack().expect("first ack");
        let second_install = second.handle_mut().begin_ack().expect("second ack");

        assert_eq!(
            first
                .handle_mut()
                .finish_ack(&second_install, CheckpointOutcome::CheckpointCommitted,),
            Err(HandleTransitionError::BindingMismatch)
        );
        assert_eq!(first.handle().state(), DeliveryHandleState::AckInstalling);
        first
            .handle_mut()
            .finish_ack(&first_install, CheckpointOutcome::CheckpointCommitted)
            .expect("exact binding commits");
    }

    #[test]
    fn committed_and_unknown_checkpoint_results_are_terminal() {
        let mut committed = delivery();
        let committed_install = committed.handle_mut().begin_ack().expect("begin ack");
        committed
            .handle_mut()
            .finish_ack(&committed_install, CheckpointOutcome::CheckpointCommitted)
            .expect("commit checkpoint");
        assert_eq!(
            committed.handle().state(),
            DeliveryHandleState::AckTerminalCommitted
        );
        assert_eq!(
            committed.handle_mut().begin_ack(),
            Err(HandleTransitionError::Terminal(
                DeliveryHandleState::AckTerminalCommitted
            ))
        );

        let mut unknown = delivery();
        let unknown_install = unknown.handle_mut().begin_ack().expect("begin ack");
        unknown
            .handle_mut()
            .finish_ack(&unknown_install, CheckpointOutcome::CheckpointUnknown)
            .expect("record unknown checkpoint");
        assert_eq!(
            unknown.handle().state(),
            DeliveryHandleState::AckTerminalUnknown
        );
        assert_eq!(
            unknown.handle_mut().release(),
            Err(HandleTransitionError::Terminal(
                DeliveryHandleState::AckTerminalUnknown
            ))
        );
    }

    #[test]
    fn release_timeout_and_shutdown_fences_never_revive() {
        for (terminal, transition) in [
            (
                DeliveryHandleState::Released,
                DeliveryHandle::release as fn(&mut DeliveryHandle) -> _,
            ),
            (DeliveryHandleState::TimedOut, DeliveryHandle::timeout),
            (
                DeliveryHandleState::ShutdownFenced,
                DeliveryHandle::shutdown_fence,
            ),
        ] {
            let mut delivery = delivery();
            transition(delivery.handle_mut()).expect("open transition wins");
            assert_eq!(delivery.handle().state(), terminal);
            assert_eq!(
                delivery.handle_mut().begin_ack(),
                Err(HandleTransitionError::Terminal(terminal))
            );
        }
    }

    #[test]
    fn ack_installing_rejects_competing_terminal_events() {
        let mut delivery = delivery();
        let _install = delivery.handle_mut().begin_ack().expect("begin ack");

        assert_eq!(
            delivery.handle_mut().timeout(),
            Err(HandleTransitionError::AckInstallInProgress)
        );
        assert_eq!(
            delivery.handle().state(),
            DeliveryHandleState::AckInstalling
        );
    }

    #[test]
    fn received_message_id_is_the_checked_uuid_v4_value() {
        let record = record(9);
        let checked = DurableMessageId::try_from_wire_bytes(valid_message_id_bytes())
            .expect("valid received identity");
        assert_eq!(record.message_id(), checked);
    }
}
