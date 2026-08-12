//! Checked single-record poll coordination.
//!
//! The coordinator computes one inclusive expected offset from Chirps-owned
//! checkpoint state, performs exactly one private `CheckedPoll`, and validates
//! the complete replay truth table before advancing its observed broker
//! frontier. The private request already fixes one numeric stream/topic,
//! explicit partition and exact offset; it has no consumer-group balancing,
//! auto-commit, or unchecked high-level decoder path.

use crate::session::{BoundSession, SessionError, SessionFenceReason, SessionInvocationError};
use crate::transport::InvocationError;
use alopex_chirps_core::durable::{
    PollObservation, PollResolution, ReplayError, ResourceEpoch, expected_offset,
};
use async_trait::async_trait;
use std::sync::Arc;
use thiserror::Error;

/// Bounded result of one adapter-owned checked-poll invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckedPollPortError {
    /// No usable authenticated session is currently available.
    Unavailable,
    /// The one transport invocation did not yield a valid response.
    Transport,
    /// Request construction or exact response correlation failed.
    Protocol,
}

/// Narrow dependency-injection boundary for one checked poll.
#[async_trait]
pub trait CheckedPollPort: Send + Sync {
    /// Performs exactly one checked poll for the inclusive expected offset.
    async fn checked_poll_once(
        &self,
        expected_offset: u64,
    ) -> Result<PollObservation, CheckedPollPortError>;
}

/// A retryable read failure that did not alter canonical checkpoint state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum PollReadFailure {
    /// No usable authenticated session is currently available.
    #[error("checked poll session is unavailable")]
    Unavailable,
    /// The one transport exchange failed or became indeterminate.
    #[error("checked poll transport failed")]
    Transport,
}

/// A checked-poll failure classified before any delivery is admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum CheckedPollError {
    /// A retryable read failure; the same expected offset remains canonical.
    #[error("checked poll read failed: {0}")]
    ReadFailure(PollReadFailure),
    /// The private request or response violated its exact protocol contract.
    #[error("checked poll protocol validation failed")]
    Protocol,
    /// The observation belongs to another resource incarnation.
    #[error("checked poll resource epoch differs from the bound namespace")]
    ResourceEpochMismatch {
        /// Resource incarnation fixed by the subscription namespace.
        expected: ResourceEpoch,
        /// Resource incarnation returned atomically by the broker.
        actual: ResourceEpoch,
    },
    /// The captured end-exclusive offset regressed within one resource epoch.
    #[error("checked poll end regressed from {previous} to {actual}")]
    EndRegressed {
        /// Last fully validated end-exclusive frontier.
        previous: u64,
        /// Newly returned lower end-exclusive frontier.
        actual: u64,
    },
    /// The provider-neutral replay truth table rejected the observation.
    #[error("checked poll replay validation failed: {0}")]
    Replay(#[from] ReplayError),
}

/// Shape of one fully validated checked-poll success.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckedPollKind {
    /// The exact expected record is present.
    Record,
    /// The expected offset equals the captured end and no record is present.
    Tail,
}

/// One atomic observation accepted by every Task 3.8 invariant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckedPollSuccess {
    expected_offset: u64,
    kind: CheckedPollKind,
    observation: PollObservation,
}

impl CheckedPollSuccess {
    /// Returns the inclusive offset computed from canonical Chirps state.
    #[must_use]
    pub const fn expected_offset(&self) -> u64 {
        self.expected_offset
    }

    /// Returns whether the validated result is a record or the current tail.
    #[must_use]
    pub const fn kind(&self) -> CheckedPollKind {
        self.kind
    }

    /// Borrows the correlated, bounded atomic observation.
    #[must_use]
    pub const fn observation(&self) -> &PollObservation {
        &self.observation
    }

    /// Consumes this success and releases the provider-neutral observation.
    #[must_use]
    pub fn into_observation(self) -> PollObservation {
        self.observation
    }
}

/// Stateful validator for a single immutable subscription resource epoch.
#[derive(Debug)]
pub struct CheckedPollCoordinator<P> {
    port: Arc<P>,
    resource_epoch: ResourceEpoch,
    last_end_exclusive: Option<u64>,
}

impl<P> CheckedPollCoordinator<P>
where
    P: CheckedPollPort,
{
    /// Fixes the port and immutable resource incarnation for this namespace.
    #[must_use]
    pub const fn new(port: Arc<P>, resource_epoch: ResourceEpoch) -> Self {
        Self {
            port,
            resource_epoch,
            last_end_exclusive: None,
        }
    }

    /// Returns the last fully validated broker end, never a failed candidate.
    #[must_use]
    pub const fn last_end_exclusive(&self) -> Option<u64> {
        self.last_end_exclusive
    }

    /// Computes the expected offset and performs at most one checked poll.
    ///
    /// `checkpoint` is the Chirps-owned committed inclusive offset. `None`
    /// selects the already-resolved immutable initial frontier. No result here
    /// writes a checkpoint or treats an Iggy consumer offset as canonical.
    pub async fn poll_next(
        &mut self,
        checkpoint: Option<u64>,
        resolved_initial: u64,
    ) -> Result<CheckedPollSuccess, CheckedPollError> {
        let expected = expected_offset(checkpoint, resolved_initial)?;
        let observation = self
            .port
            .checked_poll_once(expected)
            .await
            .map_err(map_port_error)?;

        if observation.resource_epoch() != self.resource_epoch {
            return Err(CheckedPollError::ResourceEpochMismatch {
                expected: self.resource_epoch,
                actual: observation.resource_epoch(),
            });
        }
        if let Some(previous) = self.last_end_exclusive
            && observation.end_exclusive() < previous
        {
            return Err(CheckedPollError::EndRegressed {
                previous,
                actual: observation.end_exclusive(),
            });
        }

        let kind = match observation.observe(expected)? {
            PollResolution::Record(_) => CheckedPollKind::Record,
            PollResolution::Tail => CheckedPollKind::Tail,
        };
        self.last_end_exclusive = Some(observation.end_exclusive());
        Ok(CheckedPollSuccess {
            expected_offset: expected,
            kind,
            observation,
        })
    }
}

const fn map_port_error(error: CheckedPollPortError) -> CheckedPollError {
    match error {
        CheckedPollPortError::Unavailable => {
            CheckedPollError::ReadFailure(PollReadFailure::Unavailable)
        }
        CheckedPollPortError::Transport => {
            CheckedPollError::ReadFailure(PollReadFailure::Transport)
        }
        CheckedPollPortError::Protocol => CheckedPollError::Protocol,
    }
}

#[async_trait]
impl CheckedPollPort for BoundSession {
    async fn checked_poll_once(
        &self,
        expected_offset: u64,
    ) -> Result<PollObservation, CheckedPollPortError> {
        match self.checked_poll(expected_offset).await {
            Ok(response) => Ok(response.into_observation()),
            Err(SessionInvocationError::Session(SessionError::Fenced(
                SessionFenceReason::ResponseMismatch,
            )))
            | Err(SessionInvocationError::InvalidRequest)
            | Err(SessionInvocationError::InvalidResponse) => Err(CheckedPollPortError::Protocol),
            Err(SessionInvocationError::Session(_)) => Err(CheckedPollPortError::Unavailable),
            Err(SessionInvocationError::Invocation(
                InvocationError::NotInvoked(_) | InvocationError::Indeterminate(_),
            )) => Err(CheckedPollPortError::Transport),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CheckedPollCoordinator, CheckedPollError, CheckedPollKind, CheckedPollPort,
        CheckedPollPortError, PollReadFailure,
    };
    use alopex_chirps_core::durable::{
        CheckedPollRecord, EnvelopeDigest, PollObservation, ReplayError, ResourceEpoch, ResourceId,
    };
    use async_trait::async_trait;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Debug)]
    struct ScriptedPort {
        calls: AtomicUsize,
        expected: Mutex<Vec<u64>>,
        results: Mutex<VecDeque<Result<PollObservation, CheckedPollPortError>>>,
    }

    impl ScriptedPort {
        fn new(
            results: impl IntoIterator<Item = Result<PollObservation, CheckedPollPortError>>,
        ) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                expected: Mutex::new(Vec::new()),
                results: Mutex::new(results.into_iter().collect()),
            }
        }
    }

    #[async_trait]
    impl CheckedPollPort for ScriptedPort {
        async fn checked_poll_once(
            &self,
            expected_offset: u64,
        ) -> Result<PollObservation, CheckedPollPortError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.expected.lock().unwrap().push(expected_offset);
            self.results
                .lock()
                .unwrap()
                .pop_front()
                .expect("scripted checked poll result")
        }
    }

    fn epoch(number: u64) -> ResourceEpoch {
        let mut bytes = [0x31; 16];
        bytes[6] = 0x41;
        bytes[8] = 0x91;
        ResourceEpoch::new(ResourceId::from_bytes(bytes), number)
    }

    fn record(offset: u64) -> CheckedPollRecord {
        let mut id = [0x42; 16];
        id[6] = 0x44;
        id[8] = 0x82;
        CheckedPollRecord::try_new(
            offset,
            id,
            EnvelopeDigest::from_bytes([0x73; 32]),
            b"canonical envelope".to_vec(),
        )
        .unwrap()
    }

    fn observation(
        resource_epoch: ResourceEpoch,
        end: u64,
        oldest: u64,
        record: Option<CheckedPollRecord>,
    ) -> PollObservation {
        PollObservation::try_new(resource_epoch, end, oldest, record).unwrap()
    }

    #[tokio::test]
    async fn v07_task_3_8_exact_record_and_tail_use_one_explicit_expected_offset_each() {
        let port = Arc::new(ScriptedPort::new([
            Ok(observation(epoch(7), 12, 4, Some(record(8)))),
            Ok(observation(epoch(7), 12, 4, None)),
        ]));
        let mut coordinator = CheckedPollCoordinator::new(Arc::clone(&port), epoch(7));

        let first = coordinator.poll_next(None, 8).await.unwrap();
        assert_eq!(first.expected_offset(), 8);
        assert_eq!(first.kind(), CheckedPollKind::Record);
        assert_eq!(first.observation().record().unwrap().offset(), 8);

        let second = coordinator.poll_next(Some(11), 99).await.unwrap();
        assert_eq!(second.expected_offset(), 12);
        assert_eq!(second.kind(), CheckedPollKind::Tail);
        assert_eq!(*port.expected.lock().unwrap(), [8, 12]);
        assert_eq!(port.calls.load(Ordering::SeqCst), 2);
        assert_eq!(coordinator.last_end_exclusive(), Some(12));
    }

    #[tokio::test]
    async fn v07_task_3_8_exhausted_checkpoint_fails_before_backend_contact() {
        let port = Arc::new(ScriptedPort::new([]));
        let mut coordinator = CheckedPollCoordinator::new(Arc::clone(&port), epoch(7));

        assert_eq!(
            coordinator.poll_next(Some(u64::MAX), 0).await,
            Err(CheckedPollError::Replay(ReplayError::OffsetExhausted {
                checkpoint: u64::MAX,
            }))
        );
        assert_eq!(port.calls.load(Ordering::SeqCst), 0);
        assert_eq!(coordinator.last_end_exclusive(), None);
    }

    #[tokio::test]
    async fn v07_task_3_8_epoch_mismatch_and_end_regression_are_fail_stop() {
        let port = Arc::new(ScriptedPort::new([
            Ok(observation(epoch(7), 20, 5, None)),
            Ok(observation(epoch(8), 20, 5, None)),
            Ok(observation(epoch(7), 19, 5, None)),
        ]));
        let mut coordinator = CheckedPollCoordinator::new(Arc::clone(&port), epoch(7));

        assert!(coordinator.poll_next(None, 20).await.is_ok());
        assert!(matches!(
            coordinator.poll_next(None, 20).await,
            Err(CheckedPollError::ResourceEpochMismatch { .. })
        ));
        assert_eq!(coordinator.last_end_exclusive(), Some(20));
        assert_eq!(
            coordinator.poll_next(None, 19).await,
            Err(CheckedPollError::EndRegressed {
                previous: 20,
                actual: 19,
            })
        );
        assert_eq!(coordinator.last_end_exclusive(), Some(20));
    }

    #[tokio::test]
    async fn v07_task_3_8_replay_truth_table_rejects_gap_conflict_missing_and_wrong_record() {
        let cases = [
            (
                observation(epoch(7), 20, 10, None),
                9,
                ReplayError::RetentionGap {
                    expected: 9,
                    oldest_available: 10,
                },
            ),
            (
                observation(epoch(7), 20, 10, None),
                21,
                ReplayError::CheckpointConflict {
                    expected: 21,
                    end_exclusive: 20,
                },
            ),
            (
                observation(epoch(7), 20, 10, None),
                15,
                ReplayError::MissingExpectedRecord {
                    expected: 15,
                    end_exclusive: 20,
                },
            ),
            (
                observation(epoch(7), 20, 10, Some(record(16))),
                15,
                ReplayError::UnexpectedRecordOffset {
                    expected: 15,
                    actual: 16,
                },
            ),
        ];

        for (candidate, expected, error) in cases {
            let port = Arc::new(ScriptedPort::new([Ok(candidate)]));
            let mut coordinator = CheckedPollCoordinator::new(port, epoch(7));
            assert_eq!(
                coordinator.poll_next(None, expected).await,
                Err(CheckedPollError::Replay(error))
            );
            assert_eq!(coordinator.last_end_exclusive(), None);
        }
    }

    #[tokio::test]
    async fn v07_task_3_8_failed_candidate_never_advances_end_frontier() {
        let port = Arc::new(ScriptedPort::new([
            Ok(observation(epoch(7), 10, 2, None)),
            Ok(observation(epoch(7), 30, 2, None)),
            Ok(observation(epoch(7), 15, 2, None)),
        ]));
        let mut coordinator = CheckedPollCoordinator::new(port, epoch(7));

        assert!(coordinator.poll_next(None, 10).await.is_ok());
        assert!(matches!(
            coordinator.poll_next(None, 9).await,
            Err(CheckedPollError::Replay(
                ReplayError::MissingExpectedRecord { .. }
            ))
        ));
        assert_eq!(coordinator.last_end_exclusive(), Some(10));
        assert!(coordinator.poll_next(None, 15).await.is_ok());
        assert_eq!(coordinator.last_end_exclusive(), Some(15));
    }

    #[tokio::test]
    async fn v07_task_3_8_read_failures_are_retryable_and_protocol_is_fail_stop() {
        let port = Arc::new(ScriptedPort::new([
            Err(CheckedPollPortError::Unavailable),
            Err(CheckedPollPortError::Transport),
            Err(CheckedPollPortError::Protocol),
        ]));
        let mut coordinator = CheckedPollCoordinator::new(port, epoch(7));

        assert_eq!(
            coordinator.poll_next(None, 3).await,
            Err(CheckedPollError::ReadFailure(PollReadFailure::Unavailable))
        );
        assert_eq!(
            coordinator.poll_next(None, 3).await,
            Err(CheckedPollError::ReadFailure(PollReadFailure::Transport))
        );
        assert_eq!(
            coordinator.poll_next(None, 3).await,
            Err(CheckedPollError::Protocol)
        );
        assert_eq!(coordinator.last_end_exclusive(), None);
    }
}
