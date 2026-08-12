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
pub struct DeliveryToken {
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
    fn from_handle(handle: &DeliveryHandle) -> Self {
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

    /// Returns the monotonically increasing attempt bound to this token.
    #[must_use]
    pub const fn delivery_attempt(self) -> u64 {
        self.delivery_attempt
    }

    /// Returns the delivered inclusive broker offset.
    #[must_use]
    pub const fn offset(self) -> u64 {
        self.offset
    }

    /// Returns the canonical logical message identity.
    #[must_use]
    pub const fn message_id(self) -> DurableMessageId {
        self.message_id
    }
}

/// Application-visible bytes released only after durable identity insertion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmittedDelivery {
    canonical_bytes: Vec<u8>,
    token: DeliveryToken,
}

impl AdmittedDelivery {
    /// Returns the immutable canonical envelope bytes.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    /// Returns the exact owner/message/attempt arbitration token.
    #[must_use]
    pub const fn token(&self) -> DeliveryToken {
        self.token
    }
}

/// Non-ack terminal event competing at the one owner-scoped boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeliveryEvent {
    /// Application nack/release; the same offset may be redelivered.
    Release,
    /// Delivery deadline expired; the same offset may be redelivered.
    Timeout,
    /// Lifecycle shutdown fenced this handle.
    ShutdownFence,
}

/// Serial owner of the sole in-flight handle for one partition.
#[derive(Debug)]
pub(crate) struct DeliveryArbiter {
    binding: SubscriptionBinding,
    current: Option<Delivery>,
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

    /// Returns the current handle state without releasing the handle.
    #[must_use]
    #[cfg(test)]
    fn current_state(&self) -> Option<DeliveryHandleState> {
        self.current
            .as_ref()
            .map(|delivery| delivery.handle().state())
    }

    /// Admits a record only after its exact attempt was durably persisted.
    pub(crate) fn admit_persisted(
        &mut self,
        record: CheckedPollRecord,
        delivery_attempt: u64,
    ) -> Result<AdmittedDelivery, DeliveryEventError> {
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
        let admission = AdmittedDelivery {
            canonical_bytes: delivery.canonical_bytes().to_vec(),
            token: DeliveryToken::from_handle(delivery.handle()),
        };
        self.current = Some(delivery);
        Ok(admission)
    }

    /// Begins checkpoint installation for the exact current token.
    pub(crate) fn begin_ack(
        &mut self,
        token: DeliveryToken,
    ) -> Result<CheckpointInstallBinding, DeliveryEventError> {
        self.current_handle_mut(token)?
            .begin_ack()
            .map_err(Into::into)
    }

    /// Applies one exact checkpoint result and releases only terminal handles.
    pub(crate) fn finish_ack(
        &mut self,
        token: DeliveryToken,
        binding: &CheckpointInstallBinding,
        outcome: CheckpointOutcome,
    ) -> Result<DeliveryHandleState, DeliveryEventError> {
        let state = {
            let handle = self.current_handle_mut(token)?;
            handle.finish_ack(binding, outcome)?;
            handle.state()
        };
        if state.is_terminal() {
            self.current = None;
        }
        Ok(state)
    }

    /// Linearizes one release, timeout, or shutdown event.
    pub(crate) fn finish(
        &mut self,
        token: DeliveryToken,
        event: DeliveryEvent,
    ) -> Result<DeliveryHandleState, DeliveryEventError> {
        let state = {
            let handle = self.current_handle_mut(token)?;
            match event {
                DeliveryEvent::Release => handle.release(),
                DeliveryEvent::Timeout => handle.timeout(),
                DeliveryEvent::ShutdownFence => handle.shutdown_fence(),
            }?;
            handle.state()
        };
        self.current = None;
        Ok(state)
    }

    fn current_handle_mut(
        &mut self,
        token: DeliveryToken,
    ) -> Result<&mut DeliveryHandle, DeliveryEventError> {
        let delivery = self
            .current
            .as_mut()
            .ok_or(DeliveryEventError::NoInFlight)?;
        if !token.matches(delivery.handle()) {
            return Err(DeliveryEventError::StaleToken);
        }
        Ok(delivery.handle_mut())
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
        for winner in [
            DeliveryEvent::Release,
            DeliveryEvent::Timeout,
            DeliveryEvent::ShutdownFence,
        ] {
            let mut arbiter = DeliveryArbiter::new(binding(1));
            let first = arbiter.admit_persisted(record(9, 0x41), 1).unwrap();
            arbiter.finish(first.token(), winner).unwrap();
            assert!(!arbiter.has_in_flight());

            let second = arbiter.admit_persisted(record(9, 0x41), 2).unwrap();
            assert!(second.token().delivery_attempt() > first.token().delivery_attempt());
            assert_eq!(
                arbiter.begin_ack(first.token()),
                Err(DeliveryEventError::StaleToken)
            );
            assert_eq!(arbiter.current_state(), Some(DeliveryHandleState::Open));
        }
    }

    #[test]
    fn v07_task_4_4_ack_install_is_nonterminal_and_only_known_old_retries() {
        let mut arbiter = DeliveryArbiter::new(binding(1));
        let delivery = arbiter.admit_persisted(record(9, 0x41), 1).unwrap();
        let install = arbiter.begin_ack(delivery.token()).unwrap();
        assert_eq!(
            arbiter.finish(delivery.token(), DeliveryEvent::Release),
            Err(DeliveryEventError::AckInstallInProgress)
        );
        arbiter
            .finish_ack(
                delivery.token(),
                &install,
                CheckpointOutcome::CheckpointNotCommitted,
            )
            .unwrap();
        assert_eq!(
            arbiter.current_state(),
            Some(DeliveryHandleState::AckRetryable)
        );

        let retry = arbiter.begin_ack(delivery.token()).unwrap();
        assert_ne!(install.checkpoint_attempt(), retry.checkpoint_attempt());
        arbiter
            .finish_ack(
                delivery.token(),
                &retry,
                CheckpointOutcome::CheckpointCommitted,
            )
            .unwrap();
        assert!(!arbiter.has_in_flight());
        assert_eq!(
            arbiter.finish_ack(
                delivery.token(),
                &retry,
                CheckpointOutcome::CheckpointCommitted,
            ),
            Err(DeliveryEventError::NoInFlight)
        );
    }
}
