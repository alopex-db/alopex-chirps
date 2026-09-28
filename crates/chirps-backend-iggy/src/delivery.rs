//! Owner-scoped delivery-handle arbitration.

use alopex_chirps_core::durable::{
    CheckedPollRecord, CheckpointInstallBinding, CheckpointOutcome, Delivery, DeliveryContext,
    DeliveryHandle, DeliveryHandleState, DurableMessageId, EnvelopeDigest, HandleTransitionError,
    SubscriptionBinding, SubscriptionId,
};
use alopex_chirps_wire::node_id::NodeId;
use thiserror::Error;

/// Immutable identity of one admitted delivery attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct DeliveryToken {
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
}

impl DeliveryToken {
    pub(crate) fn from_handle(handle: &DeliveryHandle) -> Self {
        Self {
            subscription_id: handle.subscription_id(),
            target: handle.target(),
            generation: handle.generation(),
            partition: handle.partition(),
            offset: handle.offset(),
            message_id: handle.message_id(),
            envelope_digest: handle.envelope_digest(),
            delivery_attempt: handle.delivery_attempt(),
            owner_epoch: handle.owner_epoch(),
            lifecycle_generation: handle.lifecycle_generation(),
        }
    }
    fn matches(self, handle: &DeliveryHandle) -> bool {
        self == Self::from_handle(handle)
    }
}

/// Non-ack terminal event competing at the one owner-scoped boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeliveryEvent {
    /// Application nack/release; the same offset may be redelivered.
    Release,
    /// Delivery deadline expired; the same offset may be redelivered.
    Timeout,
}

/// Serial owner of the sole in-flight handle for one partition.
#[derive(Debug)]
pub(crate) struct DeliveryArbiter {
    binding: SubscriptionBinding,
    current: Option<DeliveryToken>,
}

impl DeliveryArbiter {
    /// Creates an empty arbiter tied to one durable owner epoch.
    #[must_use]
    #[allow(dead_code)] // Task 4.5 composes the subscriber into the lifecycle facade.
    pub(crate) const fn new(binding: SubscriptionBinding) -> Self {
        Self {
            binding,
            current: None,
        }
    }

    /// Returns whether polling is fenced by one admitted delivery.
    #[must_use]
    pub(crate) const fn has_in_flight(&self) -> bool {
        self.current.is_some()
    }

    /// Admits a record only after its exact attempt was durably persisted.
    pub(crate) fn admit_persisted(
        &mut self,
        record: CheckedPollRecord,
        delivery_attempt: u64,
    ) -> Result<Delivery, DeliveryEventError> {
        if self.current.is_some() {
            return Err(DeliveryEventError::AlreadyInFlight);
        }
        if delivery_attempt == 0 {
            return Err(DeliveryEventError::StaleAttempt);
        }
        let delivery = Delivery::from_checked_record(
            DeliveryContext::new(self.binding, delivery_attempt),
            record,
        );
        self.current = Some(DeliveryToken::from_handle(delivery.handle()));
        Ok(delivery)
    }

    /// Begins checkpoint installation for the exact current token.
    pub(crate) fn begin_ack(
        &mut self,
        handle: &mut DeliveryHandle,
    ) -> Result<CheckpointInstallBinding, DeliveryEventError> {
        self.require_current(handle)?;
        handle.begin_ack().map_err(Into::into)
    }

    /// Applies one exact checkpoint result and releases only terminal handles.
    pub(crate) fn finish_ack(
        &mut self,
        handle: &mut DeliveryHandle,
        binding: &CheckpointInstallBinding,
        outcome: CheckpointOutcome,
    ) -> Result<DeliveryHandleState, DeliveryEventError> {
        self.require_current(handle)?;
        handle.finish_ack(binding, outcome)?;
        let state = handle.state();
        if state.is_terminal() {
            self.current = None;
        }
        Ok(state)
    }

    /// Linearizes one release, timeout, or shutdown event.
    pub(crate) fn finish(
        &mut self,
        handle: &mut DeliveryHandle,
        event: DeliveryEvent,
    ) -> Result<DeliveryHandleState, DeliveryEventError> {
        self.require_current(handle)?;
        match event {
            DeliveryEvent::Release => handle.release(),
            DeliveryEvent::Timeout => handle.timeout(),
        }?;
        let state = handle.state();
        self.current = None;
        Ok(state)
    }

    pub(crate) fn fence(&mut self) {
        self.current = None;
    }

    fn require_current(&self, handle: &DeliveryHandle) -> Result<(), DeliveryEventError> {
        let token = self.current.ok_or(DeliveryEventError::NoInFlight)?;
        if !token.matches(handle) {
            return Err(DeliveryEventError::StaleToken);
        }
        Ok(())
    }
}

/// Rejected event at the partition's single arbitration boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum DeliveryEventError {
    /// A delivery is already admitted, so polling/admission remains fenced.
    #[error("one delivery is already in flight")]
    AlreadyInFlight,
    /// No delivery can accept this event.
    #[error("no delivery is in flight")]
    NoInFlight,
    /// The token belongs to another owner, message, offset, or attempt.
    #[error("delivery token is stale or does not identify the current handle")]
    StaleToken,
    /// The supplied durable attempt is invalid.
    #[error("delivery attempt must be nonzero")]
    StaleAttempt,
    /// Ack installation won and competing terminal events cannot preempt it.
    #[error("checkpoint installation is already in progress")]
    AckInstallInProgress,
    /// The provider-neutral handle rejected this transition.
    #[error("delivery handle transition failed: {0}")]
    Handle(HandleTransitionError),
}

impl From<HandleTransitionError> for DeliveryEventError {
    fn from(error: HandleTransitionError) -> Self {
        match error {
            HandleTransitionError::AckInstallInProgress => Self::AckInstallInProgress,
            other => Self::Handle(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DeliveryArbiter, DeliveryEvent, DeliveryEventError};
    use alopex_chirps_core::durable::{
        CheckedPollRecord, CheckpointOutcome, DeliveryHandleState, EnvelopeDigest,
        SubscriptionBinding, SubscriptionId,
    };
    use alopex_chirps_wire::node_id::NodeId;

    fn binding(owner_epoch: u64) -> SubscriptionBinding {
        SubscriptionBinding::new(
            SubscriptionId::from_bytes([0x11; 16]),
            NodeId::from_bytes(&[0x22; 16]).unwrap(),
            7,
            2,
            owner_epoch,
            3,
        )
    }

    fn record(offset: u64, fill: u8) -> CheckedPollRecord {
        let mut message_id = [fill; 16];
        message_id[6] = 0x44;
        message_id[8] = 0x84;
        CheckedPollRecord::try_new(
            offset,
            message_id,
            EnvelopeDigest::from_bytes([fill; 32]),
            vec![fill],
        )
        .unwrap()
    }

    #[test]
    fn v07_task_4_4_only_one_terminal_event_wins_and_old_token_stays_fenced() {
        for winner in [DeliveryEvent::Release, DeliveryEvent::Timeout] {
            let mut arbiter = DeliveryArbiter::new(binding(1));
            let mut first = arbiter.admit_persisted(record(9, 0x41), 1).unwrap();
            arbiter.finish(first.handle_mut(), winner).unwrap();
            assert!(!arbiter.has_in_flight());

            let second = arbiter.admit_persisted(record(9, 0x41), 2).unwrap();
            assert!(second.handle().delivery_attempt() > first.handle().delivery_attempt());
            assert_eq!(
                arbiter.begin_ack(first.handle_mut()),
                Err(DeliveryEventError::StaleToken)
            );
            assert_eq!(second.handle().state(), DeliveryHandleState::Open);
        }
    }

    #[test]
    fn v07_task_4_4_ack_install_is_nonterminal_and_only_known_old_retries() {
        let mut arbiter = DeliveryArbiter::new(binding(1));
        let mut delivery = arbiter.admit_persisted(record(9, 0x41), 1).unwrap();
        let install = arbiter.begin_ack(delivery.handle_mut()).unwrap();
        assert_eq!(
            arbiter.finish(delivery.handle_mut(), DeliveryEvent::Release),
            Err(DeliveryEventError::AckInstallInProgress)
        );
        arbiter
            .finish_ack(
                delivery.handle_mut(),
                &install,
                CheckpointOutcome::CheckpointNotCommitted,
            )
            .unwrap();
        assert_eq!(delivery.handle().state(), DeliveryHandleState::AckRetryable);

        let retry = arbiter.begin_ack(delivery.handle_mut()).unwrap();
        assert_ne!(install.checkpoint_attempt(), retry.checkpoint_attempt());
        arbiter
            .finish_ack(
                delivery.handle_mut(),
                &retry,
                CheckpointOutcome::CheckpointCommitted,
            )
            .unwrap();
        assert!(!arbiter.has_in_flight());
        assert_eq!(
            arbiter.finish_ack(
                delivery.handle_mut(),
                &retry,
                CheckpointOutcome::CheckpointCommitted,
            ),
            Err(DeliveryEventError::NoInFlight)
        );
    }
}
