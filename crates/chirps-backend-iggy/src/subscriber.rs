//! Single-partition checked delivery coordination.
//!
//! Application effects and the local checkpoint are not one transaction. A
//! crash after an application effect but before checkpoint commit can therefore
//! redeliver the same logical message with a new delivery attempt.

use crate::codec::{EnvelopeDecodeError, decode};
use crate::delivery::{
    AdmittedDelivery, DeliveryArbiter, DeliveryEvent, DeliveryEventError, DeliveryToken,
};
use crate::poll::{CheckedPollCoordinator, CheckedPollError, CheckedPollKind, CheckedPollPort};
use crate::state::identity::{ClockProvenance, IdentityCandidate, IdentityError};
use crate::state::journal::{IdentityPersistOutcome, JournalError, JournalStore};
use alopex_chirps_core::durable::{
    CheckpointInstallBinding, CheckpointOutcome, DeliveryHandleState, ReplayError,
    SubscriptionBinding,
};
use std::sync::Arc;
use thiserror::Error;

/// Runtime state of one explicit subscription partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionStatus {
    /// Polling may run when no delivery is in flight.
    Active,
    /// One exact delivery handle fences the next poll.
    InFlight,
    /// A retention gap requires explicit operator action.
    RetentionGap,
    /// Local reachability is unknown and fresh recovery is mandatory.
    RecoveryRequired,
    /// Corrupt, mismatched, or contradictory state stopped the partition.
    Faulted,
    /// Shutdown fenced the partition permanently.
    Closed,
}

/// Durable clock provenance copied into an identity horizon record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryClock {
    /// Wall clock is trusted for retry-age comparison.
    Trusted,
    /// A rollback was detected.
    RollbackDetected,
    /// Clock provenance cannot currently be established.
    Unknown,
}

impl From<DeliveryClock> for ClockProvenance {
    fn from(value: DeliveryClock) -> Self {
        match value {
            DeliveryClock::Trusted => Self::Trusted,
            DeliveryClock::RollbackDetected => Self::RollbackDetected,
            DeliveryClock::Unknown => Self::Unknown,
        }
    }
}

/// Accepted result of one bounded poll attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NextDelivery {
    /// The expected offset equals the atomic captured end.
    Tail,
    /// Identity append was known-old; the same expected record may be polled again.
    IdentityNotCommitted,
    /// Identity reached durability and application bytes may now be observed.
    Delivery(AdmittedDelivery),
}

/// Coordinates checked poll, canonical decode, durable identity, and handle CAS.
pub struct SubscriberCoordinator<P> {
    binding: SubscriptionBinding,
    poll: CheckedPollCoordinator<P>,
    journal: Box<dyn SubscriberJournal>,
    delivery: DeliveryArbiter,
    status: PartitionStatus,
}

trait SubscriberJournal: Send {
    fn expected_offset(&self) -> Result<u64, JournalError>;
    fn next_identity_attempt(
        &self,
        message_id: [u8; 16],
        first_retry_not_before_unix_ms: u64,
        first_clock_provenance: ClockProvenance,
    ) -> Result<IdentityAttempt, JournalError>;
    fn persist_identity(
        &mut self,
        candidate: IdentityCandidate,
    ) -> Result<IdentityPersistOutcome, JournalError>;
    fn install_checkpoint(
        &mut self,
        binding: CheckpointInstallBinding,
    ) -> Result<CheckpointOutcome, JournalError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct IdentityAttempt {
    delivery_attempt: u64,
    retry_not_before_unix_ms: u64,
    clock_provenance: ClockProvenance,
}

impl SubscriberJournal for JournalStore {
    fn expected_offset(&self) -> Result<u64, JournalError> {
        self.expected_offset()
    }

    fn next_identity_attempt(
        &self,
        message_id: [u8; 16],
        first_retry_not_before_unix_ms: u64,
        first_clock_provenance: ClockProvenance,
    ) -> Result<IdentityAttempt, JournalError> {
        match self.identity(message_id) {
            Some(identity) => Ok(IdentityAttempt {
                delivery_attempt: identity
                    .last_delivery_attempt()
                    .checked_add(1)
                    .ok_or(JournalError::SequenceExhausted)?,
                retry_not_before_unix_ms: identity.retry_not_before_unix_ms(),
                clock_provenance: identity.clock_provenance(),
            }),
            None => Ok(IdentityAttempt {
                delivery_attempt: 1,
                retry_not_before_unix_ms: first_retry_not_before_unix_ms,
                clock_provenance: first_clock_provenance,
            }),
        }
    }

    fn persist_identity(
        &mut self,
        candidate: IdentityCandidate,
    ) -> Result<IdentityPersistOutcome, JournalError> {
        self.persist_identity(candidate)
    }

    fn install_checkpoint(
        &mut self,
        binding: CheckpointInstallBinding,
    ) -> Result<CheckpointOutcome, JournalError> {
        self.install_checkpoint(binding)
    }
}

impl<P> SubscriberCoordinator<P>
where
    P: CheckedPollPort,
{
    #[allow(dead_code)] // Task 4.5 composes this coordinator into the lifecycle facade.
    pub(crate) fn new(
        binding: SubscriptionBinding,
        resource_epoch: alopex_chirps_core::durable::ResourceEpoch,
        port: Arc<P>,
        journal: JournalStore,
    ) -> Self {
        Self {
            binding,
            poll: CheckedPollCoordinator::new(port, resource_epoch),
            journal: Box::new(journal),
            delivery: DeliveryArbiter::new(binding),
            status: PartitionStatus::Active,
        }
    }

    #[cfg(test)]
    fn new_with_journal(
        binding: SubscriptionBinding,
        resource_epoch: alopex_chirps_core::durable::ResourceEpoch,
        port: Arc<P>,
        journal: Box<dyn SubscriberJournal>,
    ) -> Self {
        Self {
            binding,
            poll: CheckedPollCoordinator::new(port, resource_epoch),
            journal,
            delivery: DeliveryArbiter::new(binding),
            status: PartitionStatus::Active,
        }
    }

    /// Returns the current fail-stop/in-flight state.
    #[must_use]
    pub const fn status(&self) -> PartitionStatus {
        self.status
    }

    /// Returns whether one application handle currently fences polling.
    #[must_use]
    pub const fn has_in_flight(&self) -> bool {
        self.delivery.has_in_flight()
    }

    /// Performs at most one checked poll and releases bytes only after identity sync.
    pub async fn next_delivery(
        &mut self,
        retry_not_before_unix_ms: u64,
        clock: DeliveryClock,
    ) -> Result<NextDelivery, SubscriberError> {
        self.require_status(PartitionStatus::Active)?;
        if self.delivery.has_in_flight() {
            self.status = PartitionStatus::InFlight;
            return Err(SubscriberError::Delivery(
                DeliveryEventError::AlreadyInFlight,
            ));
        }
        let expected = self.journal.expected_offset().map_err(|error| {
            self.latch_journal_error(error);
            SubscriberError::State(error.into())
        })?;
        let success = match self.poll.poll_next(None, expected).await {
            Ok(success) => success,
            Err(error) => {
                if !matches!(error, CheckedPollError::ReadFailure(_)) {
                    self.status = match error {
                        CheckedPollError::Replay(ReplayError::RetentionGap { .. }) => {
                            PartitionStatus::RetentionGap
                        }
                        _ => PartitionStatus::Faulted,
                    };
                }
                return Err(SubscriberError::Poll(error));
            }
        };
        if success.kind() == CheckedPollKind::Tail {
            return Ok(NextDelivery::Tail);
        }

        let observation = success.into_observation();
        let record = observation
            .record()
            .cloned()
            .ok_or_else(|| self.fault(SubscriberError::MissingCheckedRecord))?;
        let decoded = decode(record.canonical_bytes())
            .map_err(|error| self.fault(SubscriberError::Envelope(error)))?;
        if decoded.message_id_bytes() != record.message_id().as_bytes()
            || decoded.envelope_digest() != record.envelope_digest()
            || decoded.target() != self.binding.target()
            || decoded.generation() != self.binding.generation()
            || decoded.partition() != self.binding.partition()
        {
            return Err(self.fault(SubscriberError::EnvelopeBindingMismatch));
        }

        let key = *record.message_id().as_bytes();
        let identity_attempt = self
            .journal
            .next_identity_attempt(key, retry_not_before_unix_ms, clock.into())
            .map_err(|error| {
                self.latch_journal_error(error);
                SubscriberError::State(error.into())
            })?;
        let candidate = IdentityCandidate::new(
            record.message_id(),
            record.envelope_digest(),
            observation.resource_epoch(),
            self.binding.partition(),
            record.offset(),
            identity_attempt.delivery_attempt,
            identity_attempt.retry_not_before_unix_ms,
            identity_attempt.clock_provenance,
        )
        .map_err(|error| {
            self.fault(SubscriberError::State(JournalError::Identity(error).into()))
        })?;
        match self.journal.persist_identity(candidate) {
            Ok(IdentityPersistOutcome::NotCommitted) => Ok(NextDelivery::IdentityNotCommitted),
            Ok(IdentityPersistOutcome::Unknown) => {
                self.status = PartitionStatus::RecoveryRequired;
                Err(SubscriberError::IdentityUnknown)
            }
            Ok(IdentityPersistOutcome::Committed(_)) => {
                let admitted = self
                    .delivery
                    .admit_persisted(record, identity_attempt.delivery_attempt)
                    .map_err(|error| self.fault(SubscriberError::Delivery(error)))?;
                self.status = PartitionStatus::InFlight;
                Ok(NextDelivery::Delivery(admitted))
            }
            Err(error) => {
                self.latch_journal_error(error);
                Err(SubscriberError::State(error.into()))
            }
        }
    }

    /// Installs one exact canonical checkpoint and preserves its phase outcome.
    pub fn ack(&mut self, token: DeliveryToken) -> Result<CheckpointOutcome, SubscriberError> {
        self.require_status(PartitionStatus::InFlight)?;
        let install = self.delivery.begin_ack(token)?;
        let outcome = match self.journal.install_checkpoint(install) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.latch_journal_error(error);
                return Err(SubscriberError::State(error.into()));
            }
        };
        let state = self.delivery.finish_ack(token, &install, outcome)?;
        self.status = match state {
            DeliveryHandleState::AckRetryable => PartitionStatus::InFlight,
            DeliveryHandleState::AckTerminalCommitted => PartitionStatus::Active,
            DeliveryHandleState::AckTerminalUnknown => PartitionStatus::RecoveryRequired,
            _ => return Err(self.fault(SubscriberError::UnexpectedHandleState(state))),
        };
        Ok(outcome)
    }

    /// Nack/releases the exact current handle without advancing checkpoint state.
    pub fn release(&mut self, token: DeliveryToken) -> Result<(), SubscriberError> {
        self.finish_non_ack(token, DeliveryEvent::Release)
    }

    /// Times out the exact current handle without advancing checkpoint state.
    pub fn timeout(&mut self, token: DeliveryToken) -> Result<(), SubscriberError> {
        self.finish_non_ack(token, DeliveryEvent::Timeout)
    }

    /// Fences an optional open handle and closes this coordinator.
    pub fn close(&mut self, token: Option<DeliveryToken>) -> Result<(), SubscriberError> {
        if self.status == PartitionStatus::Closed {
            return Ok(());
        }
        if let Some(token) = token {
            self.delivery.finish(token, DeliveryEvent::ShutdownFence)?;
        } else if self.delivery.has_in_flight() {
            return Err(SubscriberError::MissingShutdownToken);
        }
        self.status = PartitionStatus::Closed;
        Ok(())
    }

    fn finish_non_ack(
        &mut self,
        token: DeliveryToken,
        event: DeliveryEvent,
    ) -> Result<(), SubscriberError> {
        self.require_status(PartitionStatus::InFlight)?;
        self.delivery.finish(token, event)?;
        self.status = PartitionStatus::Active;
        Ok(())
    }

    fn require_status(&self, expected: PartitionStatus) -> Result<(), SubscriberError> {
        if self.status == expected {
            Ok(())
        } else {
            Err(SubscriberError::PartitionUnavailable(self.status))
        }
    }

    fn fault(&mut self, error: SubscriberError) -> SubscriberError {
        self.status = PartitionStatus::Faulted;
        error
    }

    fn latch_journal_error(&mut self, error: JournalError) {
        self.status = if error == JournalError::RecoveryRequired {
            PartitionStatus::RecoveryRequired
        } else {
            PartitionStatus::Faulted
        };
    }
}

/// Stable public classification of private local-state failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SubscriberStateError {
    /// A prior write may have reached durable storage; reopening is required.
    #[error("local state requires fresh-process recovery")]
    RecoveryRequired,
    /// The bounded local-state capacity was exhausted.
    #[error("local state capacity is exhausted")]
    CapacityExhausted,
    /// The committed checkpoint has no representable successor offset.
    #[error("checkpoint offset is exhausted")]
    OffsetExhausted,
    /// One offset or logical message identity has contradictory durable bindings.
    #[error("durable message identity is contradictory")]
    IdentityConflict,
    /// The local filesystem could not complete an operation.
    #[error("local state storage is unavailable")]
    StorageUnavailable,
    /// Local state was invalid, corrupt, or inconsistent with this namespace.
    #[error("local state is invalid or inconsistent")]
    InvalidState,
}

impl From<JournalError> for SubscriberStateError {
    fn from(error: JournalError) -> Self {
        match error {
            JournalError::RecoveryRequired => Self::RecoveryRequired,
            JournalError::JournalTooLarge => Self::CapacityExhausted,
            JournalError::OffsetExhausted => Self::OffsetExhausted,
            JournalError::IdentityOffsetConflict
            | JournalError::Identity(IdentityError::IdentityConflict)
            | JournalError::Identity(IdentityError::MetadataConflict) => Self::IdentityConflict,
            JournalError::Io(_) => Self::StorageUnavailable,
            _ => Self::InvalidState,
        }
    }
}

/// Typed subscriber failure. Retryability is determined by partition status.
#[derive(Debug, Error)]
pub enum SubscriberError {
    /// This operation is not legal in the current partition state.
    #[error("partition is unavailable in state {0:?}")]
    PartitionUnavailable(PartitionStatus),
    /// Checked polling failed before delivery admission.
    #[error("checked poll failed: {0}")]
    Poll(#[from] CheckedPollError),
    /// A validated record was unexpectedly absent.
    #[error("record-shaped checked poll omitted its record")]
    MissingCheckedRecord,
    /// Canonical envelope decoding failed.
    #[error("canonical envelope failed validation: {0}")]
    Envelope(EnvelopeDecodeError),
    /// Envelope route or identity differs from the checked poll record/namespace.
    #[error("canonical envelope does not match the checked record or subscription")]
    EnvelopeBindingMismatch,
    /// Identity/checkpoint durable state failed.
    #[error("checkpoint journal failed: {0}")]
    State(SubscriberStateError),
    /// Identity append reachability is unknown.
    #[error("identity persistence is unknown and requires recovery")]
    IdentityUnknown,
    /// Delivery arbitration rejected the event.
    #[error("delivery arbitration failed: {0}")]
    Delivery(#[from] DeliveryEventError),
    /// A handle reached a state outside the checkpoint outcome mapping.
    #[error("checkpoint produced unexpected handle state {0:?}")]
    UnexpectedHandleState(DeliveryHandleState),
    /// Closing with an in-flight handle requires its exact token.
    #[error("shutdown token is required while a delivery is in flight")]
    MissingShutdownToken,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::poll::CheckedPollPortError;
    use crate::producer::prepare;
    use crate::routing::{PartitionRouter, ValidatedRoutingConfiguration};
    use crate::state::identity::IdentityRecord;
    use crate::state::journal::{JournalInitialization, JournalNamespace};
    use alopex_chirps_core::durable::{
        CheckedPollRecord, PollObservation, ResourceEpoch, ResourceId, SubscriptionId,
    };
    use alopex_chirps_wire::node_id::NodeId;
    use async_trait::async_trait;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Debug)]
    struct ScriptedPort {
        results: Mutex<VecDeque<Result<PollObservation, CheckedPollPortError>>>,
        expected: Mutex<Vec<u64>>,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl CheckedPollPort for ScriptedPort {
        async fn checked_poll_once(
            &self,
            expected_offset: u64,
        ) -> Result<PollObservation, CheckedPollPortError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.expected.lock().unwrap().push(expected_offset);
            self.results.lock().unwrap().pop_front().unwrap()
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum IdentityResult {
        NotCommitted,
        Unknown,
        Committed,
    }

    #[derive(Debug)]
    struct ScriptedJournal {
        expected_offset: u64,
        attempts: VecDeque<u64>,
        identities: VecDeque<Result<IdentityResult, JournalError>>,
        checkpoints: VecDeque<Result<CheckpointOutcome, JournalError>>,
    }

    impl SubscriberJournal for ScriptedJournal {
        fn expected_offset(&self) -> Result<u64, JournalError> {
            Ok(self.expected_offset)
        }

        fn next_identity_attempt(
            &self,
            _message_id: [u8; 16],
            first_retry_not_before_unix_ms: u64,
            first_clock_provenance: ClockProvenance,
        ) -> Result<IdentityAttempt, JournalError> {
            let delivery_attempt = self
                .attempts
                .front()
                .copied()
                .ok_or(JournalError::SequenceExhausted)?;
            Ok(IdentityAttempt {
                delivery_attempt,
                retry_not_before_unix_ms: first_retry_not_before_unix_ms,
                clock_provenance: first_clock_provenance,
            })
        }

        fn persist_identity(
            &mut self,
            candidate: IdentityCandidate,
        ) -> Result<IdentityPersistOutcome, JournalError> {
            self.attempts.pop_front();
            match self.identities.pop_front().unwrap()? {
                IdentityResult::NotCommitted => Ok(IdentityPersistOutcome::NotCommitted),
                IdentityResult::Unknown => Ok(IdentityPersistOutcome::Unknown),
                IdentityResult::Committed => Ok(IdentityPersistOutcome::Committed(
                    IdentityRecord::first(candidate),
                )),
            }
        }

        fn install_checkpoint(
            &mut self,
            _binding: CheckpointInstallBinding,
        ) -> Result<CheckpointOutcome, JournalError> {
            self.checkpoints.pop_front().unwrap()
        }
    }

    fn node(fill: u8) -> NodeId {
        NodeId::from_bytes(&[fill; 16]).unwrap()
    }

    fn epoch() -> ResourceEpoch {
        ResourceEpoch::new(ResourceId::from_bytes([0x33; 16]), 4)
    }

    fn binding() -> SubscriptionBinding {
        SubscriptionBinding::new(
            SubscriptionId::from_bytes([0x11; 16]),
            node(0x22),
            7,
            0,
            3,
            5,
        )
    }

    fn record(offset: u64, target: NodeId) -> CheckedPollRecord {
        let router = PartitionRouter::from_validated_configuration(ValidatedRoutingConfiguration {
            source: node(0x44),
            generation: 7,
            partition_count: 1,
            mapping_version: 1,
        })
        .unwrap();
        let prepared = prepare(&router, target, b"key".to_vec(), b"payload").unwrap();
        CheckedPollRecord::try_new(
            offset,
            *prepared.message_id().as_bytes(),
            prepared.envelope_digest(),
            prepared.canonical_bytes().to_vec(),
        )
        .unwrap()
    }

    fn observation(record: Option<CheckedPollRecord>, end: u64, oldest: u64) -> PollObservation {
        PollObservation::try_new(epoch(), end, oldest, record).unwrap()
    }

    fn coordinator(
        polls: impl IntoIterator<Item = Result<PollObservation, CheckedPollPortError>>,
        attempts: impl IntoIterator<Item = u64>,
        identities: impl IntoIterator<Item = Result<IdentityResult, JournalError>>,
        checkpoints: impl IntoIterator<Item = Result<CheckpointOutcome, JournalError>>,
    ) -> (SubscriberCoordinator<ScriptedPort>, Arc<ScriptedPort>) {
        let port = Arc::new(ScriptedPort {
            results: Mutex::new(polls.into_iter().collect()),
            expected: Mutex::new(Vec::new()),
            calls: AtomicUsize::new(0),
        });
        let journal = ScriptedJournal {
            expected_offset: 9,
            attempts: attempts.into_iter().collect(),
            identities: identities.into_iter().collect(),
            checkpoints: checkpoints.into_iter().collect(),
        };
        (
            SubscriberCoordinator::new_with_journal(
                binding(),
                epoch(),
                Arc::clone(&port),
                Box::new(journal),
            ),
            port,
        )
    }

    #[tokio::test]
    async fn v07_task_4_4_single_in_flight_redelivery_uses_new_attempt_and_fences_old() {
        let first_record = record(9, binding().target());
        let (mut coordinator, port) = coordinator(
            [
                Ok(observation(Some(first_record.clone()), 10, 0)),
                Ok(observation(Some(first_record), 10, 0)),
            ],
            [1, 2],
            [Ok(IdentityResult::Committed), Ok(IdentityResult::Committed)],
            [],
        );
        let NextDelivery::Delivery(first) = coordinator
            .next_delivery(1_000, DeliveryClock::Trusted)
            .await
            .unwrap()
        else {
            panic!("expected first delivery")
        };
        assert_eq!(coordinator.status(), PartitionStatus::InFlight);
        assert_eq!(
            coordinator
                .next_delivery(1_000, DeliveryClock::Trusted)
                .await
                .unwrap_err()
                .to_string(),
            SubscriberError::PartitionUnavailable(PartitionStatus::InFlight).to_string()
        );
        assert_eq!(port.calls.load(Ordering::SeqCst), 1);
        coordinator.release(first.token()).unwrap();

        let NextDelivery::Delivery(second) = coordinator
            .next_delivery(1_000, DeliveryClock::Trusted)
            .await
            .unwrap()
        else {
            panic!("expected redelivery")
        };
        assert_eq!(second.token().delivery_attempt(), 2);
        assert!(matches!(
            coordinator.ack(first.token()),
            Err(SubscriberError::Delivery(DeliveryEventError::StaleToken))
        ));
        coordinator.timeout(second.token()).unwrap();
        assert_eq!(coordinator.status(), PartitionStatus::Active);
        assert_eq!(*port.expected.lock().unwrap(), [9, 9]);
    }

    #[tokio::test]
    async fn v07_task_4_4_real_journal_commits_identity_before_delivery_and_checkpoint_before_poll()
    {
        let directory = tempfile::tempdir().unwrap();
        let binding = binding();
        let namespace = JournalNamespace::new(
            binding.subscription_id(),
            binding.target(),
            binding.generation(),
            binding.partition(),
            binding.lifecycle_generation(),
            9,
            epoch(),
        );
        let JournalInitialization::Ready(journal) =
            JournalStore::initialize(directory.path(), namespace, binding.owner_epoch()).unwrap()
        else {
            panic!("journal initialization must commit")
        };
        let first = record(9, binding.target());
        let duplicate = CheckedPollRecord::try_new(
            10,
            *first.message_id().as_bytes(),
            first.envelope_digest(),
            first.canonical_bytes().to_vec(),
        )
        .unwrap();
        let second = record(11, binding.target());
        assert_ne!(first.message_id(), second.message_id());
        let port = Arc::new(ScriptedPort {
            results: Mutex::new(
                [
                    Ok(observation(Some(first.clone()), 11, 0)),
                    Ok(observation(Some(first), 12, 0)),
                    Ok(observation(Some(duplicate), 12, 0)),
                    Ok(observation(Some(second), 12, 0)),
                ]
                .into_iter()
                .collect(),
            ),
            expected: Mutex::new(Vec::new()),
            calls: AtomicUsize::new(0),
        });
        let mut coordinator =
            SubscriberCoordinator::new(binding, epoch(), Arc::clone(&port), *journal);

        let NextDelivery::Delivery(delivery) = coordinator
            .next_delivery(1_000, DeliveryClock::Trusted)
            .await
            .unwrap()
        else {
            panic!("identity commit must release the delivery")
        };
        assert_eq!(coordinator.status(), PartitionStatus::InFlight);
        coordinator.release(delivery.token()).unwrap();
        assert_eq!(coordinator.status(), PartitionStatus::Active);

        let NextDelivery::Delivery(redelivery) = coordinator
            .next_delivery(9_999, DeliveryClock::Unknown)
            .await
            .unwrap()
        else {
            panic!("the persisted identity horizon must be reused for redelivery")
        };
        assert_eq!(redelivery.token().delivery_attempt(), 2);
        assert_eq!(
            coordinator.ack(redelivery.token()).unwrap(),
            CheckpointOutcome::CheckpointCommitted
        );

        let NextDelivery::Delivery(duplicate) = coordinator
            .next_delivery(12_000, DeliveryClock::RollbackDetected)
            .await
            .unwrap()
        else {
            panic!("same-ID same-digest append must remain a visible duplicate attempt")
        };
        assert_eq!(duplicate.token().delivery_attempt(), 3);
        assert_eq!(
            coordinator.ack(duplicate.token()).unwrap(),
            CheckpointOutcome::CheckpointCommitted
        );

        let NextDelivery::Delivery(second) = coordinator
            .next_delivery(2_000, DeliveryClock::Trusted)
            .await
            .unwrap()
        else {
            panic!("the next identity must have its own first attempt")
        };
        assert_eq!(second.token().delivery_attempt(), 1);
        assert_eq!(
            coordinator.ack(second.token()).unwrap(),
            CheckpointOutcome::CheckpointCommitted
        );
        assert_eq!(*port.expected.lock().unwrap(), [9, 9, 10, 11]);
    }

    #[tokio::test]
    async fn v07_task_4_4_ack_known_old_retries_but_committed_and_unknown_are_terminal() {
        for (outcomes, expected_status) in [
            (
                vec![
                    Ok(CheckpointOutcome::CheckpointNotCommitted),
                    Ok(CheckpointOutcome::CheckpointCommitted),
                ],
                PartitionStatus::Active,
            ),
            (
                vec![Ok(CheckpointOutcome::CheckpointUnknown)],
                PartitionStatus::RecoveryRequired,
            ),
        ] {
            let (mut coordinator, _) = coordinator(
                [Ok(observation(Some(record(9, binding().target())), 10, 0))],
                [1],
                [Ok(IdentityResult::Committed)],
                outcomes,
            );
            let NextDelivery::Delivery(delivery) = coordinator
                .next_delivery(1_000, DeliveryClock::Trusted)
                .await
                .unwrap()
            else {
                panic!("expected delivery")
            };
            let first = coordinator.ack(delivery.token()).unwrap();
            if first == CheckpointOutcome::CheckpointNotCommitted {
                assert_eq!(coordinator.status(), PartitionStatus::InFlight);
                assert_eq!(
                    coordinator.ack(delivery.token()).unwrap(),
                    CheckpointOutcome::CheckpointCommitted
                );
            }
            assert_eq!(coordinator.status(), expected_status);
            assert!(!coordinator.has_in_flight());
        }
    }

    #[tokio::test]
    async fn v07_task_4_4_identity_must_commit_before_delivery_and_unknown_fail_stops() {
        for (identity, expected_status) in [
            (IdentityResult::NotCommitted, PartitionStatus::Active),
            (IdentityResult::Unknown, PartitionStatus::RecoveryRequired),
        ] {
            let (mut coordinator, _) = coordinator(
                [Ok(observation(Some(record(9, binding().target())), 10, 0))],
                [1],
                [Ok(identity)],
                [],
            );
            let result = coordinator
                .next_delivery(1_000, DeliveryClock::Trusted)
                .await;
            match identity {
                IdentityResult::NotCommitted => {
                    assert_eq!(result.unwrap(), NextDelivery::IdentityNotCommitted)
                }
                IdentityResult::Unknown => {
                    assert!(matches!(result, Err(SubscriberError::IdentityUnknown)))
                }
                IdentityResult::Committed => unreachable!(),
            }
            assert!(!coordinator.has_in_flight());
            assert_eq!(coordinator.status(), expected_status);
        }
    }

    #[tokio::test]
    async fn v07_task_4_4_misroute_gap_and_contradiction_fail_stop_but_read_failure_retries() {
        let cases = [
            (
                Ok(observation(Some(record(9, node(0x55))), 10, 0)),
                PartitionStatus::Faulted,
            ),
            (Ok(observation(None, 10, 10)), PartitionStatus::RetentionGap),
        ];
        for (poll, status) in cases {
            let (mut coordinator, _) = coordinator([poll], [1], [], []);
            assert!(
                coordinator
                    .next_delivery(1_000, DeliveryClock::Trusted)
                    .await
                    .is_err()
            );
            assert_eq!(coordinator.status(), status);
            assert!(!coordinator.has_in_flight());
        }

        let (mut coordinator, _) = coordinator(
            [Err(CheckedPollPortError::Transport)],
            std::iter::empty(),
            std::iter::empty(),
            std::iter::empty(),
        );
        assert!(
            coordinator
                .next_delivery(1_000, DeliveryClock::Trusted)
                .await
                .is_err()
        );
        assert_eq!(coordinator.status(), PartitionStatus::Active);
    }
}
