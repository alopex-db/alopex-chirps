//! Adapter-owned lifecycle and shutdown coordination.

use crate::observability::BoundedObservability;
use crate::session::BoundSession;
use alopex_chirps_core::durable::{
    AdmissionTicket, CheckpointOperationPhase, CheckpointOperationReport, ChirpsPlaneHealth,
    ControlPlaneHealth, DurableHealth, LifecycleGate, LifecyclePhase, LifecycleTransitionError,
    OperationReportError, PartitionHealth, PartitionState, Readiness, RegisteredOperation,
    RegisteredOperationKind, SendOperationPhase, SendOperationReport, ShutdownFreeze,
    ShutdownFreezeError, ShutdownReport, ShutdownReportError, UnavailableReason,
};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

/// Minimal close/join projection used by the lifecycle coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShutdownTransportReport {
    closed: bool,
    joined: bool,
}

impl ShutdownTransportReport {
    /// Creates exact terminal transport evidence.
    #[must_use]
    pub const fn new(closed: bool, joined: bool) -> Self {
        Self { closed, joined }
    }
}

/// Adapter-owned transport boundary that can unblock I/O before being joined.
#[async_trait]
pub trait ShutdownTransport: Send {
    /// Requests idempotent close without waiting for worker termination.
    fn request_close(&self);

    /// Consumes and joins the owned transport by the supplied deadline.
    async fn shutdown(self: Box<Self>, deadline: Instant) -> ShutdownTransportReport;
}

#[async_trait]
impl ShutdownTransport for BoundSession {
    fn request_close(&self) {
        self.close_handle().close();
    }

    async fn shutdown(self: Box<Self>, deadline: Instant) -> ShutdownTransportReport {
        let report = BoundSession::shutdown(*self, deadline).await;
        ShutdownTransportReport::new(report.socket_close_requested(), report.all_workers_joined())
    }
}

/// One adapter delivery owner that must be fenced before shutdown freezes work.
pub trait DeliveryFence: Send {
    /// Invalidates every pre-drain delivery handle owned by this fence.
    fn fence(&mut self);
}

#[derive(Debug)]
struct SendState {
    phase: SendOperationPhase,
    frozen: bool,
}

/// Shared phase handle for one lifecycle-registered send.
#[derive(Debug, Clone)]
pub struct SendOperationHandle {
    state: Arc<Mutex<SendState>>,
}

impl SendOperationHandle {
    /// Advances the send phase without permitting regression or terminal
    /// outcome replacement.
    pub fn transition(&self, next: SendOperationPhase) -> Result<(), LifecycleCoordinatorError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| LifecycleCoordinatorError::OperationStatePoisoned)?;
        if state.frozen {
            return Err(LifecycleCoordinatorError::OperationFrozen);
        }
        if !valid_send_transition(state.phase, next) {
            return Err(LifecycleCoordinatorError::InvalidSendTransition);
        }
        state.phase = next;
        Ok(())
    }
}

fn valid_send_transition(current: SendOperationPhase, next: SendOperationPhase) -> bool {
    current == next
        || matches!(
            (current, next),
            (
                SendOperationPhase::Prepared,
                SendOperationPhase::AppendInvoked | SendOperationPhase::Terminal(_)
            ) | (
                SendOperationPhase::AppendInvoked,
                SendOperationPhase::Terminal(_)
            )
        )
}

#[derive(Debug)]
struct CheckpointState {
    phase: CheckpointOperationPhase,
    frozen: bool,
}

/// Shared phase handle for one lifecycle-registered checkpoint install.
#[derive(Debug, Clone)]
pub struct CheckpointOperationHandle {
    state: Arc<Mutex<CheckpointState>>,
}

impl CheckpointOperationHandle {
    /// Advances the checkpoint phase without permitting regression or a
    /// terminal outcome replacement.
    pub fn transition(
        &self,
        next: CheckpointOperationPhase,
    ) -> Result<(), LifecycleCoordinatorError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| LifecycleCoordinatorError::OperationStatePoisoned)?;
        if state.frozen {
            return Err(LifecycleCoordinatorError::OperationFrozen);
        }
        if !valid_checkpoint_transition(state.phase, next) {
            return Err(LifecycleCoordinatorError::InvalidCheckpointTransition);
        }
        state.phase = next;
        Ok(())
    }
}

fn valid_checkpoint_transition(
    current: CheckpointOperationPhase,
    next: CheckpointOperationPhase,
) -> bool {
    current == next
        || matches!(
            (current, next),
            (
                CheckpointOperationPhase::Prewrite,
                CheckpointOperationPhase::KnownOld
                    | CheckpointOperationPhase::InstallUnknown
                    | CheckpointOperationPhase::Confirmed
                    | CheckpointOperationPhase::Terminal(_)
            ) | (
                CheckpointOperationPhase::KnownOld,
                CheckpointOperationPhase::InstallUnknown
                    | CheckpointOperationPhase::Confirmed
                    | CheckpointOperationPhase::Terminal(_)
            )
        )
}

struct RegisteredWorker {
    cancel: watch::Sender<bool>,
    task: JoinHandle<()>,
}

/// Owns Durable admission, phase freezing, transport close, and worker joins.
pub struct LifecycleCoordinator {
    gate: LifecycleGate,
    readiness: Readiness,
    partitions: BTreeMap<u32, PartitionState>,
    started_operations: Vec<RegisteredOperation>,
    send_states: Vec<Arc<Mutex<SendState>>>,
    checkpoint_states: Vec<Arc<Mutex<CheckpointState>>>,
    workers: Vec<RegisteredWorker>,
    delivery_fences: Vec<Box<dyn DeliveryFence>>,
    transport: Option<Box<dyn ShutdownTransport>>,
    shutdown_freeze: Option<ShutdownFreeze>,
    observability: BoundedObservability,
}

impl LifecycleCoordinator {
    /// Creates a starting coordinator around one owned Durable transport.
    #[must_use]
    pub fn new(transport: Box<dyn ShutdownTransport>, observability: BoundedObservability) -> Self {
        Self {
            gate: LifecycleGate::new(),
            readiness: Readiness::Unavailable(UnavailableReason::Connectivity),
            partitions: BTreeMap::new(),
            started_operations: Vec::new(),
            send_states: Vec::new(),
            checkpoint_states: Vec::new(),
            workers: Vec::new(),
            delivery_fences: Vec::new(),
            transport: Some(transport),
            shutdown_freeze: None,
            observability,
        }
    }

    /// Opens admission after startup validation and sets its independent
    /// readiness projection.
    pub fn mark_ready(&mut self, readiness: Readiness) -> Result<(), LifecycleCoordinatorError> {
        self.gate.mark_ready()?;
        self.readiness = readiness;
        Ok(())
    }

    /// Returns the current lifecycle axis.
    #[must_use]
    pub const fn phase(&self) -> LifecyclePhase {
        self.gate.phase()
    }

    /// Captures the current admission generation.
    pub fn admission_ticket(&self) -> Result<AdmissionTicket, LifecycleCoordinatorError> {
        if self.readiness != Readiness::Available {
            return Err(LifecycleCoordinatorError::ReadinessUnavailable(
                self.readiness,
            ));
        }
        Ok(self.gate.admission_ticket()?)
    }

    /// Registers a started send and returns its phase transition handle.
    pub fn register_send(
        &mut self,
        ticket: AdmissionTicket,
        phase: SendOperationPhase,
    ) -> Result<SendOperationHandle, LifecycleCoordinatorError> {
        self.ensure_admission_ready()?;
        if phase == SendOperationPhase::Idle {
            return Err(LifecycleCoordinatorError::InvalidSendInitialPhase);
        }
        let operation = self.gate.register(ticket, RegisteredOperationKind::Send)?;
        let state = Arc::new(Mutex::new(SendState {
            phase,
            frozen: false,
        }));
        self.started_operations.push(operation);
        self.send_states.push(Arc::clone(&state));
        Ok(SendOperationHandle { state })
    }

    /// Registers a started checkpoint and returns its phase transition handle.
    pub fn register_checkpoint(
        &mut self,
        ticket: AdmissionTicket,
        phase: CheckpointOperationPhase,
    ) -> Result<CheckpointOperationHandle, LifecycleCoordinatorError> {
        self.ensure_admission_ready()?;
        if phase == CheckpointOperationPhase::Idle {
            return Err(LifecycleCoordinatorError::InvalidCheckpointInitialPhase);
        }
        let operation = self
            .gate
            .register(ticket, RegisteredOperationKind::Checkpoint)?;
        let state = Arc::new(Mutex::new(CheckpointState {
            phase,
            frozen: false,
        }));
        self.started_operations.push(operation);
        self.checkpoint_states.push(Arc::clone(&state));
        Ok(CheckpointOperationHandle { state })
    }

    /// Registers and starts one owned worker under lifecycle cancellation.
    pub fn register_worker<F, Fut>(
        &mut self,
        ticket: AdmissionTicket,
        worker: F,
    ) -> Result<(), LifecycleCoordinatorError>
    where
        F: FnOnce(watch::Receiver<bool>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.ensure_admission_ready()?;
        let operation = self
            .gate
            .register(ticket, RegisteredOperationKind::Worker)?;
        let (cancel, cancelled) = watch::channel(false);
        self.started_operations.push(operation);
        self.workers.push(RegisteredWorker {
            cancel,
            task: tokio::spawn(worker(cancelled)),
        });
        Ok(())
    }

    /// Registers a delivery owner in the current generation so it cannot be
    /// added after drain begins or omitted from the frozen started set.
    pub fn add_delivery_fence(
        &mut self,
        ticket: AdmissionTicket,
        fence: Box<dyn DeliveryFence>,
    ) -> Result<(), LifecycleCoordinatorError> {
        self.ensure_admission_ready()?;
        let operation = self
            .gate
            .register(ticket, RegisteredOperationKind::Worker)?;
        self.started_operations.push(operation);
        self.delivery_fences.push(fence);
        Ok(())
    }

    /// Replaces only the Durable readiness axis.
    pub const fn set_readiness(&mut self, readiness: Readiness) {
        self.readiness = readiness;
    }

    /// Replaces one partition axis without coupling Control or readiness.
    pub fn set_partition_state(&mut self, partition: u32, state: PartitionState) {
        self.partitions.insert(partition, state);
    }

    /// Returns a deterministic, provider-neutral health snapshot.
    #[must_use]
    pub fn health_snapshot(&self, control: ControlPlaneHealth) -> ChirpsPlaneHealth {
        let partitions = self
            .partitions
            .iter()
            .map(|(&partition, &state)| PartitionHealth::new(partition, state))
            .collect();
        ChirpsPlaneHealth::new(
            control,
            DurableHealth::new(self.gate.phase(), self.readiness, partitions),
        )
    }

    /// Returns the bounded observability sink owned by this coordinator.
    #[must_use]
    pub const fn observability(&self) -> &BoundedObservability {
        &self.observability
    }

    /// Returns mutable access to the bounded observability sink.
    pub const fn observability_mut(&mut self) -> &mut BoundedObservability {
        &mut self.observability
    }

    /// Closes admission, fences deliveries, freezes outcomes, closes transport,
    /// joins every owned worker, and memoizes the exact terminal report.
    pub async fn shutdown(
        &mut self,
        deadline: Instant,
    ) -> Result<ShutdownReport, LifecycleCoordinatorError> {
        if let Some(report) = self.gate.cached_shutdown_report() {
            return Ok(report.clone());
        }

        let generation = self.gate.begin_shutdown()?;
        for fence in &mut self.delivery_fences {
            fence.fence();
        }
        let freeze = ShutdownFreeze::try_new(generation, self.started_operations.clone(), true)?;
        self.freeze_operation_phases()?;
        self.shutdown_freeze = Some(freeze);
        self.readiness = Readiness::Unavailable(UnavailableReason::Shutdown);

        self.transport
            .as_ref()
            .ok_or(LifecycleCoordinatorError::TransportUnavailable)?
            .request_close();

        for worker in &self.workers {
            worker.cancel.send_replace(true);
        }
        for mut worker in self.workers.drain(..) {
            if tokio::time::timeout_at(deadline, &mut worker.task)
                .await
                .is_err()
            {
                worker.task.abort();
                let _ = worker.task.await;
            }
        }

        let transport = self
            .transport
            .take()
            .ok_or(LifecycleCoordinatorError::TransportUnavailable)?;
        let transport_report = transport.shutdown(deadline).await;
        let send_operations = self.frozen_send_reports()?;
        let checkpoint_operations = self.frozen_checkpoint_reports()?;
        let report = ShutdownReport::try_new(
            generation,
            transport_report.closed,
            transport_report.joined,
            send_operations,
            checkpoint_operations,
        )?;
        Ok(self.gate.complete_shutdown(report)?.clone())
    }

    /// Returns the immutable started-operation snapshot installed at drain.
    #[must_use]
    pub const fn shutdown_freeze(&self) -> Option<&ShutdownFreeze> {
        self.shutdown_freeze.as_ref()
    }

    /// Returns zero before terminal shutdown and one after report installation.
    #[must_use]
    pub const fn shutdown_install_count(&self) -> u8 {
        self.gate.shutdown_install_count()
    }

    fn ensure_admission_ready(&self) -> Result<(), LifecycleCoordinatorError> {
        if self.readiness != Readiness::Available {
            return Err(LifecycleCoordinatorError::ReadinessUnavailable(
                self.readiness,
            ));
        }
        Ok(())
    }

    fn freeze_operation_phases(&self) -> Result<(), LifecycleCoordinatorError> {
        for state in &self.send_states {
            state
                .lock()
                .map_err(|_| LifecycleCoordinatorError::OperationStatePoisoned)?
                .frozen = true;
        }
        for state in &self.checkpoint_states {
            state
                .lock()
                .map_err(|_| LifecycleCoordinatorError::OperationStatePoisoned)?
                .frozen = true;
        }
        Ok(())
    }

    fn frozen_send_reports(&self) -> Result<Vec<SendOperationReport>, LifecycleCoordinatorError> {
        self.send_states
            .iter()
            .map(|state| {
                let state = state
                    .lock()
                    .map_err(|_| LifecycleCoordinatorError::OperationStatePoisoned)?;
                Ok(SendOperationReport::at_shutdown(state.phase)?)
            })
            .collect()
    }

    fn frozen_checkpoint_reports(
        &self,
    ) -> Result<Vec<CheckpointOperationReport>, LifecycleCoordinatorError> {
        self.checkpoint_states
            .iter()
            .map(|state| {
                let state = state
                    .lock()
                    .map_err(|_| LifecycleCoordinatorError::OperationStatePoisoned)?;
                Ok(CheckpointOperationReport::at_shutdown(state.phase)?)
            })
            .collect()
    }
}

/// A rejected adapter lifecycle action.
#[derive(Debug, Error)]
pub enum LifecycleCoordinatorError {
    /// Core lifecycle generation or admission rejected the action.
    #[error(transparent)]
    Lifecycle(#[from] LifecycleTransitionError),
    /// The shutdown snapshot did not prove exact prior-generation ownership.
    #[error(transparent)]
    Freeze(#[from] ShutdownFreezeError),
    /// A non-started operation reached shutdown classification.
    #[error(transparent)]
    OperationReport(#[from] OperationReportError),
    /// Transport or worker evidence did not establish terminal shutdown.
    #[error(transparent)]
    ShutdownReport(#[from] ShutdownReportError),
    /// A send was registered without a started phase.
    #[error("send registration requires a started phase")]
    InvalidSendInitialPhase,
    /// A checkpoint was registered without a started phase.
    #[error("checkpoint registration requires a started phase")]
    InvalidCheckpointInitialPhase,
    /// The independent readiness axis does not permit new admission.
    #[error("durable readiness does not allow admission: {0:?}")]
    ReadinessUnavailable(Readiness),
    /// A send phase regressed or attempted to replace a terminal outcome.
    #[error("invalid send operation phase transition")]
    InvalidSendTransition,
    /// A checkpoint phase regressed or attempted to replace a terminal outcome.
    #[error("invalid checkpoint operation phase transition")]
    InvalidCheckpointTransition,
    /// Shutdown already froze this operation's exact phase.
    #[error("operation phase is frozen by shutdown")]
    OperationFrozen,
    /// A prior panic poisoned shared operation state.
    #[error("operation phase state is poisoned")]
    OperationStatePoisoned,
    /// The owned transport was unavailable before terminal shutdown evidence.
    #[error("owned transport is unavailable")]
    TransportUnavailable,
}

#[cfg(test)]
mod tests {
    use super::{DeliveryFence, LifecycleCoordinator, ShutdownTransport, ShutdownTransportReport};
    use crate::observability::{BoundedObservability, ObservabilityLimits};
    use alopex_chirps_core::durable::{
        AttemptFailureKind, CheckpointOperationPhase, CheckpointOutcome, ControlPlaneHealth,
        DurableSendOutcome, LifecyclePhase, PartitionFaultReason, PartitionState, Readiness,
        RecoveryReason, SendOperationPhase, UnavailableReason,
    };
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::time::{Duration, Instant};

    #[derive(Debug, Default)]
    struct Calls {
        close: AtomicUsize,
        shutdown: AtomicUsize,
        fence: AtomicUsize,
        worker_cancelled: AtomicUsize,
        order: Mutex<Vec<&'static str>>,
    }

    struct ScriptedTransport {
        calls: Arc<Calls>,
    }

    #[async_trait]
    impl ShutdownTransport for ScriptedTransport {
        fn request_close(&self) {
            self.calls.close.fetch_add(1, Ordering::SeqCst);
            self.calls.order.lock().unwrap().push("transport-close");
        }

        async fn shutdown(self: Box<Self>, _deadline: Instant) -> ShutdownTransportReport {
            self.calls.shutdown.fetch_add(1, Ordering::SeqCst);
            self.calls.order.lock().unwrap().push("transport-join");
            ShutdownTransportReport::new(true, true)
        }
    }

    struct ScriptedFence {
        calls: Arc<Calls>,
    }

    impl DeliveryFence for ScriptedFence {
        fn fence(&mut self) {
            self.calls.fence.fetch_add(1, Ordering::SeqCst);
            self.calls.order.lock().unwrap().push("delivery-fence");
        }
    }

    fn coordinator(calls: &Arc<Calls>) -> LifecycleCoordinator {
        let observability = BoundedObservability::new(ObservabilityLimits::new(16, 16).unwrap());
        LifecycleCoordinator::new(
            Box::new(ScriptedTransport {
                calls: Arc::clone(calls),
            }),
            observability,
        )
    }

    #[tokio::test]
    async fn v07_task_4_5_generation_freeze_close_cancel_join_and_repeat_are_one_shot() {
        let calls = Arc::new(Calls::default());
        let mut coordinator = coordinator(&calls);
        coordinator.mark_ready(Readiness::Available).unwrap();
        let ticket = coordinator.admission_ticket().unwrap();
        coordinator
            .add_delivery_fence(
                ticket,
                Box::new(ScriptedFence {
                    calls: Arc::clone(&calls),
                }),
            )
            .unwrap();
        let send = coordinator
            .register_send(ticket, SendOperationPhase::Prepared)
            .unwrap();
        let checkpoint = coordinator
            .register_checkpoint(ticket, CheckpointOperationPhase::Prewrite)
            .unwrap();
        let worker_calls = Arc::clone(&calls);
        coordinator
            .register_worker(ticket, move |mut cancelled| async move {
                while !*cancelled.borrow() {
                    if cancelled.changed().await.is_err() {
                        return;
                    }
                }
                worker_calls.worker_cancelled.fetch_add(1, Ordering::SeqCst);
                worker_calls.order.lock().unwrap().push("worker-join");
            })
            .unwrap();

        send.transition(SendOperationPhase::AppendInvoked).unwrap();
        checkpoint
            .transition(CheckpointOperationPhase::InstallUnknown)
            .unwrap();
        let report = coordinator
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();

        assert_eq!(
            report.send_operations()[0].outcome(),
            DurableSendOutcome::Indeterminate(AttemptFailureKind::Shutdown)
        );
        assert_eq!(
            report.checkpoint_operations()[0].outcome(),
            CheckpointOutcome::CheckpointUnknown
        );
        assert!(report.transport_closed());
        assert!(report.workers_joined());
        assert_eq!(coordinator.phase(), LifecyclePhase::Closed);
        assert_eq!(
            coordinator
                .shutdown_freeze()
                .unwrap()
                .started_operations()
                .len(),
            4
        );
        assert_eq!(coordinator.shutdown_install_count(), 1);
        assert_eq!(calls.close.load(Ordering::SeqCst), 1);
        assert_eq!(calls.shutdown.load(Ordering::SeqCst), 1);
        assert_eq!(calls.fence.load(Ordering::SeqCst), 1);
        assert_eq!(calls.worker_cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(
            *calls.order.lock().unwrap(),
            [
                "delivery-fence",
                "transport-close",
                "worker-join",
                "transport-join"
            ]
        );

        let repeated = coordinator
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(repeated, report);
        assert_eq!(calls.close.load(Ordering::SeqCst), 1);
        assert_eq!(calls.shutdown.load(Ordering::SeqCst), 1);
        assert_eq!(coordinator.shutdown_install_count(), 1);
        assert!(
            send.transition(SendOperationPhase::Terminal(
                DurableSendOutcome::BrokerAccepted
            ))
            .is_err()
        );
    }

    #[tokio::test]
    async fn v07_task_4_5_every_deadline_phase_preserves_its_exact_outcome() {
        let send_cases = [
            (
                SendOperationPhase::Prepared,
                DurableSendOutcome::NotSubmitted(AttemptFailureKind::Shutdown),
            ),
            (
                SendOperationPhase::AppendInvoked,
                DurableSendOutcome::Indeterminate(AttemptFailureKind::Shutdown),
            ),
            (
                SendOperationPhase::Terminal(DurableSendOutcome::OsSyncedAccepted),
                DurableSendOutcome::OsSyncedAccepted,
            ),
        ];
        let checkpoint_cases = [
            (
                CheckpointOperationPhase::Prewrite,
                CheckpointOutcome::CheckpointNotCommitted,
            ),
            (
                CheckpointOperationPhase::KnownOld,
                CheckpointOutcome::CheckpointNotCommitted,
            ),
            (
                CheckpointOperationPhase::InstallUnknown,
                CheckpointOutcome::CheckpointUnknown,
            ),
            (
                CheckpointOperationPhase::Confirmed,
                CheckpointOutcome::CheckpointCommitted,
            ),
            (
                CheckpointOperationPhase::Terminal(CheckpointOutcome::CheckpointCommitted),
                CheckpointOutcome::CheckpointCommitted,
            ),
        ];

        for (send_phase, send_outcome) in send_cases {
            for (checkpoint_phase, checkpoint_outcome) in checkpoint_cases {
                let calls = Arc::new(Calls::default());
                let mut coordinator = coordinator(&calls);
                coordinator.mark_ready(Readiness::Available).unwrap();
                let ticket = coordinator.admission_ticket().unwrap();
                coordinator.register_send(ticket, send_phase).unwrap();
                coordinator
                    .register_checkpoint(ticket, checkpoint_phase)
                    .unwrap();

                let report = coordinator
                    .shutdown(Instant::now() + Duration::from_secs(1))
                    .await
                    .unwrap();
                assert_eq!(report.send_operations()[0].outcome(), send_outcome);
                assert_eq!(
                    report.checkpoint_operations()[0].outcome(),
                    checkpoint_outcome
                );
            }
        }
    }

    #[tokio::test]
    async fn v07_task_4_5_drain_fences_stale_registration_and_control_health_stays_separate() {
        let calls = Arc::new(Calls::default());
        let mut coordinator = coordinator(&calls);
        coordinator.mark_ready(Readiness::Available).unwrap();
        let stale = coordinator.admission_ticket().unwrap();
        coordinator.set_readiness(Readiness::Unavailable(UnavailableReason::Connectivity));
        coordinator.set_partition_state(
            7,
            PartitionState::RecoveryRequired(RecoveryReason::CheckpointIndeterminate),
        );
        let before = coordinator.health_snapshot(ControlPlaneHealth::Available);
        assert_eq!(before.control(), ControlPlaneHealth::Available);
        assert_eq!(before.durable().lifecycle(), LifecyclePhase::Ready);
        assert_eq!(
            before.durable().readiness(),
            Readiness::Unavailable(UnavailableReason::Connectivity)
        );
        assert!(coordinator.admission_ticket().is_err());
        assert!(
            coordinator
                .register_send(stale, SendOperationPhase::Prepared)
                .is_err()
        );
        assert!(
            coordinator
                .register_checkpoint(stale, CheckpointOperationPhase::Prewrite)
                .is_err()
        );
        assert!(coordinator.register_worker(stale, |_| async {}).is_err());
        assert!(
            coordinator
                .add_delivery_fence(
                    stale,
                    Box::new(ScriptedFence {
                        calls: Arc::clone(&calls),
                    }),
                )
                .is_err()
        );
        assert_eq!(calls.fence.load(Ordering::SeqCst), 0);

        coordinator
            .shutdown(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(
            coordinator
                .register_send(stale, SendOperationPhase::Prepared)
                .is_err()
        );
        coordinator.set_partition_state(
            7,
            PartitionState::Faulted(PartitionFaultReason::InvalidEnvelope),
        );
        let after = coordinator.health_snapshot(ControlPlaneHealth::Available);
        assert_eq!(after.control(), ControlPlaneHealth::Available);
        assert_eq!(after.durable().lifecycle(), LifecyclePhase::Closed);
        assert_eq!(
            after.durable().readiness(),
            Readiness::Unavailable(UnavailableReason::Shutdown)
        );
        assert_eq!(
            after.durable().partitions()[0].state(),
            PartitionState::Faulted(PartitionFaultReason::InvalidEnvelope)
        );
        coordinator.set_readiness(Readiness::Available);
        assert!(coordinator.admission_ticket().is_err());
    }

    #[tokio::test]
    async fn v07_task_4_5_deadline_aborts_and_joins_a_worker_that_ignores_cancellation() {
        let calls = Arc::new(Calls::default());
        let mut coordinator = coordinator(&calls);
        coordinator.mark_ready(Readiness::Available).unwrap();
        let ticket = coordinator.admission_ticket().unwrap();
        coordinator
            .register_worker(ticket, |_| std::future::pending::<()>())
            .unwrap();

        let report = coordinator.shutdown(Instant::now()).await.unwrap();

        assert!(report.workers_joined());
        assert!(report.transport_closed());
        assert_eq!(coordinator.phase(), LifecyclePhase::Closed);
    }
}
