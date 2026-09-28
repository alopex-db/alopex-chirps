//! Hard local-state capacity and startup reserve contracts.

use std::collections::BTreeMap;
use thiserror::Error;

const CATEGORY_COUNT: usize = 6;

/// Independently bounded local-state categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum StateCategory {
    Payload = 0,
    InFlight = 1,
    CheckpointJournal = 2,
    ProcessedIdentity = 3,
    Queue = 4,
    Concurrency = 5,
}

impl StateCategory {
    pub(crate) const ALL: [Self; CATEGORY_COUNT] = [
        Self::Payload,
        Self::InFlight,
        Self::CheckpointJournal,
        Self::ProcessedIdentity,
        Self::Queue,
        Self::Concurrency,
    ];

    const fn index(self) -> usize {
        self as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct CapacityFootprint {
    count: u64,
    bytes: u64,
}

impl CapacityFootprint {
    pub(crate) fn new(count: u64, bytes: u64) -> Result<Self, CapacityError> {
        if count == 0 && bytes == 0 {
            return Err(CapacityError::EmptyFootprint);
        }
        Ok(Self { count, bytes })
    }

    fn checked_add(self, other: Self) -> Result<Self, CapacityError> {
        Ok(Self {
            count: self
                .count
                .checked_add(other.count)
                .ok_or(CapacityError::ArithmeticOverflow)?,
            bytes: self
                .bytes
                .checked_add(other.bytes)
                .ok_or(CapacityError::ArithmeticOverflow)?,
        })
    }

    fn checked_sub(self, other: Self) -> Result<Self, CapacityError> {
        Ok(Self {
            count: self
                .count
                .checked_sub(other.count)
                .ok_or(CapacityError::AccountingMismatch)?,
            bytes: self
                .bytes
                .checked_sub(other.bytes)
                .ok_or(CapacityError::AccountingMismatch)?,
        })
    }

    pub(crate) const fn count(self) -> u64 {
        self.count
    }

    pub(crate) const fn bytes(self) -> u64 {
        self.bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CapacityLimit {
    count: u64,
    bytes: u64,
}

impl CapacityLimit {
    pub(crate) fn new(count: u64, bytes: u64) -> Result<Self, CapacityError> {
        if count == 0 || bytes == 0 {
            return Err(CapacityError::ZeroLimit);
        }
        Ok(Self { count, bytes })
    }

    fn contains(self, usage: CapacityFootprint) -> bool {
        usage.count <= self.count && usage.bytes <= self.bytes
    }

    fn saturated_by(self, usage: CapacityFootprint) -> bool {
        usage.count == self.count || usage.bytes == self.bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CapacityLimits([CapacityLimit; CATEGORY_COUNT]);

impl CapacityLimits {
    pub(crate) const fn uniform(limit: CapacityLimit) -> Self {
        Self([limit; CATEGORY_COUNT])
    }

    pub(crate) fn with_limit(mut self, category: StateCategory, limit: CapacityLimit) -> Self {
        self.0[category.index()] = limit;
        self
    }

    const fn get(self, category: StateCategory) -> CapacityLimit {
        self.0[category.index()]
    }
}

/// Space that remains owned by the checkpoint and compaction protocols.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StartupReserves {
    checkpoint_install: CapacityFootprint,
    compaction: CapacityFootprint,
}

impl StartupReserves {
    pub(crate) const fn new(
        checkpoint_install: CapacityFootprint,
        compaction: CapacityFootprint,
    ) -> Self {
        Self {
            checkpoint_install,
            compaction,
        }
    }

    #[cfg(test)]
    const fn none() -> Self {
        Self {
            checkpoint_install: CapacityFootprint { count: 0, bytes: 0 },
            compaction: CapacityFootprint { count: 0, bytes: 0 },
        }
    }

    fn validate(self) -> Result<CapacityFootprint, CapacityError> {
        if self.checkpoint_install.count == 0
            || self.checkpoint_install.bytes == 0
            || self.compaction.count == 0
            || self.compaction.bytes == 0
        {
            return Err(CapacityError::StartupReserveUnavailable);
        }
        self.checkpoint_install.checked_add(self.compaction)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Retention {
    LiveRecovery,
    Terminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CapacityToken(u64);

/// Proof that a compaction root selecting a GC-filtered base reached directory durability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DurableGcProof {
    generation: u64,
    collected_identities: BTreeMap<[u8; 16], CapacityToken>,
}

impl DurableGcProof {
    pub(super) fn after_root_sync(
        generation: u64,
        collected_identities: BTreeMap<[u8; 16], CapacityToken>,
    ) -> Result<Self, CapacityError> {
        if generation == 0 {
            return Err(CapacityError::InvalidGcProof);
        }
        Ok(Self {
            generation,
            collected_identities,
        })
    }

    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn contains(&self, message_id: [u8; 16]) -> bool {
        self.collected_identities.contains_key(&message_id)
    }

    fn authorizes(&self, message_id: [u8; 16], token: CapacityToken) -> bool {
        self.collected_identities.get(&message_id) == Some(&token)
    }
}

#[derive(Debug, Clone, Copy)]
struct Charge {
    category: StateCategory,
    footprint: CapacityFootprint,
    retention: Retention,
    identity: Option<[u8; 16]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CapacityStatus {
    admission_open: bool,
    poll_open: bool,
}

impl CapacityStatus {
    pub(crate) const fn admission_open(self) -> bool {
        self.admission_open
    }

    pub(crate) const fn poll_open(self) -> bool {
        self.poll_open
    }
}

/// Exact accounting owner. Live-recovery charges cannot be released by pressure.
#[derive(Debug)]
pub(crate) struct CapacityController {
    limits: CapacityLimits,
    usage: [CapacityFootprint; CATEGORY_COUNT],
    charges: BTreeMap<CapacityToken, Charge>,
    identity_charges: BTreeMap<[u8; 16], CapacityToken>,
    next_token: u64,
    capacity_rejected: bool,
    clock_uncertain: bool,
    compaction_active: bool,
    reserves: StartupReserves,
}

impl CapacityController {
    pub(crate) fn start(
        limits: CapacityLimits,
        reserves: StartupReserves,
    ) -> Result<Self, CapacityError> {
        let reserved = reserves.validate()?;
        if !limits
            .get(StateCategory::CheckpointJournal)
            .contains(reserved)
        {
            return Err(CapacityError::StartupReserveUnavailable);
        }
        let mut usage = [CapacityFootprint::default(); CATEGORY_COUNT];
        usage[StateCategory::CheckpointJournal.index()] = reserved;
        let capacity_rejected = limits
            .get(StateCategory::CheckpointJournal)
            .saturated_by(reserved);
        Ok(Self {
            limits,
            usage,
            charges: BTreeMap::new(),
            identity_charges: BTreeMap::new(),
            next_token: 1,
            capacity_rejected,
            clock_uncertain: false,
            compaction_active: false,
            reserves,
        })
    }

    pub(crate) fn try_admit(
        &mut self,
        category: StateCategory,
        footprint: CapacityFootprint,
        retention: Retention,
    ) -> Result<CapacityToken, CapacityError> {
        self.try_admit_owned(category, footprint, retention, None, true)
    }

    /// Adds a later charge that belongs to an operation already admitted by
    /// [`Self::try_admit`]. Hard bounds still apply, but saturation caused by
    /// the operation's first charge does not reject its remaining accounting.
    pub(crate) fn try_admit_continuation(
        &mut self,
        category: StateCategory,
        footprint: CapacityFootprint,
        retention: Retention,
    ) -> Result<CapacityToken, CapacityError> {
        self.try_admit_owned(category, footprint, retention, None, false)
    }

    pub(crate) fn try_admit_identity(
        &mut self,
        message_id: [u8; 16],
        footprint: CapacityFootprint,
    ) -> Result<CapacityToken, CapacityError> {
        if self.identity_charges.contains_key(&message_id) {
            return Err(CapacityError::DuplicateIdentityCharge);
        }
        let token = self.try_admit_owned(
            StateCategory::ProcessedIdentity,
            footprint,
            Retention::LiveRecovery,
            Some(message_id),
            true,
        )?;
        self.identity_charges.insert(message_id, token);
        Ok(token)
    }

    fn try_admit_owned(
        &mut self,
        category: StateCategory,
        footprint: CapacityFootprint,
        retention: Retention,
        identity: Option<[u8; 16]>,
        require_open: bool,
    ) -> Result<CapacityToken, CapacityError> {
        if require_open && !self.status().admission_open {
            return Err(CapacityError::AdmissionStopped);
        }
        let next = self.usage[category.index()].checked_add(footprint)?;
        let limit = self.limits.get(category);
        if !limit.contains(next) {
            self.capacity_rejected = true;
            return Err(CapacityError::Exhausted { category });
        }
        let token = CapacityToken(self.next_token);
        self.next_token = self
            .next_token
            .checked_add(1)
            .ok_or(CapacityError::ArithmeticOverflow)?;
        self.usage[category.index()] = next;
        self.charges.insert(
            token,
            Charge {
                category,
                footprint,
                retention,
                identity,
            },
        );
        if limit.saturated_by(next) {
            self.capacity_rejected = true;
        }
        Ok(token)
    }

    /// Converts a successfully persisted preview charge into live identity
    /// ownership without opening a second admission window.
    pub(crate) fn bind_identity(
        &mut self,
        token: CapacityToken,
        message_id: [u8; 16],
    ) -> Result<(), CapacityError> {
        if self.identity_charges.contains_key(&message_id) {
            return Err(CapacityError::DuplicateIdentityCharge);
        }
        let charge = self
            .charges
            .get_mut(&token)
            .ok_or(CapacityError::UnknownToken)?;
        if charge.category != StateCategory::ProcessedIdentity
            || charge.retention != Retention::Terminal
            || charge.identity.is_some()
        {
            return Err(CapacityError::NotLiveRecoveryState);
        }
        charge.retention = Retention::LiveRecovery;
        charge.identity = Some(message_id);
        self.identity_charges.insert(message_id, token);
        Ok(())
    }

    pub(crate) fn release_terminal(&mut self, token: CapacityToken) -> Result<(), CapacityError> {
        let charge = *self
            .charges
            .get(&token)
            .ok_or(CapacityError::UnknownToken)?;
        if charge.retention == Retention::LiveRecovery {
            return Err(CapacityError::LiveStateCannotBeEvicted);
        }
        self.charges.remove(&token);
        self.usage[charge.category.index()] =
            self.usage[charge.category.index()].checked_sub(charge.footprint)?;
        self.recompute_exhaustion();
        Ok(())
    }

    pub(crate) fn release_identity_after_gc(
        &mut self,
        token: CapacityToken,
        message_id: [u8; 16],
        proof: &DurableGcProof,
    ) -> Result<(), CapacityError> {
        if proof.generation == 0 || !proof.authorizes(message_id, token) {
            return Err(CapacityError::InvalidGcProof);
        }
        let charge = *self
            .charges
            .get(&token)
            .ok_or(CapacityError::UnknownToken)?;
        if charge.retention != Retention::LiveRecovery
            || charge.category != StateCategory::ProcessedIdentity
            || charge.identity != Some(message_id)
        {
            return Err(CapacityError::NotLiveRecoveryState);
        }
        if self.identity_charges.get(&message_id) != Some(&token) {
            return Err(CapacityError::InvalidGcProof);
        }
        self.charges.remove(&token);
        self.identity_charges.remove(&message_id);
        self.usage[charge.category.index()] =
            self.usage[charge.category.index()].checked_sub(charge.footprint)?;
        self.recompute_exhaustion();
        Ok(())
    }

    pub(crate) fn identity_gc_token(
        &self,
        message_id: [u8; 16],
    ) -> Result<CapacityToken, CapacityError> {
        self.identity_charges
            .get(&message_id)
            .copied()
            .ok_or(CapacityError::InvalidGcProof)
    }

    pub(crate) fn contains_identity(&self, message_id: [u8; 16]) -> bool {
        self.identity_charges.contains_key(&message_id)
    }

    pub(crate) fn begin_compaction(&mut self) -> Result<(), CapacityError> {
        if self.compaction_active {
            return Err(CapacityError::CompactionAlreadyActive);
        }
        if self.reserves.compaction.count == 0 || self.reserves.compaction.bytes == 0 {
            return Err(CapacityError::StartupReserveUnavailable);
        }
        self.compaction_active = true;
        Ok(())
    }

    pub(crate) fn finish_compaction(&mut self) -> Result<(), CapacityError> {
        if !self.compaction_active {
            return Err(CapacityError::CompactionNotActive);
        }
        self.compaction_active = false;
        Ok(())
    }

    pub(crate) fn stop_for_clock_uncertainty(&mut self) {
        self.clock_uncertain = true;
    }

    pub(crate) fn restore_trusted_clock(&mut self) {
        self.clock_uncertain = false;
        self.recompute_exhaustion();
    }

    pub(crate) fn status(&self) -> CapacityStatus {
        let open = !self.capacity_rejected && !self.clock_uncertain;
        CapacityStatus {
            admission_open: open,
            poll_open: open,
        }
    }

    pub(crate) const fn usage(&self, category: StateCategory) -> CapacityFootprint {
        self.usage[category.index()]
    }

    fn recompute_exhaustion(&mut self) {
        self.capacity_rejected = StateCategory::ALL.into_iter().any(|category| {
            self.limits
                .get(category)
                .saturated_by(self.usage[category.index()])
        });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(crate) enum CapacityError {
    #[error("capacity count and byte limits must both be nonzero")]
    ZeroLimit,
    #[error("capacity footprint must consume a count or byte")]
    EmptyFootprint,
    #[error("capacity accounting overflowed")]
    ArithmeticOverflow,
    #[error("capacity accounting does not match its token")]
    AccountingMismatch,
    #[error("capacity accounting state is poisoned")]
    StatePoisoned,
    #[error("checkpoint-install and compaction reserves are unavailable")]
    StartupReserveUnavailable,
    #[error("capacity is exhausted for {category:?}")]
    Exhausted { category: StateCategory },
    #[error("new admission and polling are stopped")]
    AdmissionStopped,
    #[error("capacity token is unknown")]
    UnknownToken,
    #[error("live recovery state cannot be evicted")]
    LiveStateCannotBeEvicted,
    #[error("capacity token is not live recovery state")]
    NotLiveRecoveryState,
    #[error("processed identity already has a live capacity charge")]
    DuplicateIdentityCharge,
    #[error("durable identity-GC proof is invalid")]
    InvalidGcProof,
    #[error("compaction already owns its reserve")]
    CompactionAlreadyActive,
    #[error("compaction does not own its reserve")]
    CompactionNotActive,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reserves() -> StartupReserves {
        StartupReserves::new(
            CapacityFootprint::new(1, 2).unwrap(),
            CapacityFootprint::new(1, 2).unwrap(),
        )
    }

    #[test]
    fn v07_task_4_3_every_category_enforces_count_and_byte_limits() {
        for category in StateCategory::ALL {
            for count_lane in [true, false] {
                let base = CapacityLimit::new(8, 32).unwrap();
                let mut limits = CapacityLimits::uniform(base).with_limit(
                    StateCategory::CheckpointJournal,
                    CapacityLimit::new(8, 32).unwrap(),
                );
                limits = limits.with_limit(
                    category,
                    match (category == StateCategory::CheckpointJournal, count_lane) {
                        (true, true) => CapacityLimit::new(4, 32).unwrap(),
                        (true, false) => CapacityLimit::new(8, 12).unwrap(),
                        (false, true) => CapacityLimit::new(2, 32).unwrap(),
                        (false, false) => CapacityLimit::new(8, 8).unwrap(),
                    },
                );
                let footprint = if count_lane {
                    CapacityFootprint::new(1, 1).unwrap()
                } else {
                    CapacityFootprint::new(1, 4).unwrap()
                };
                let mut controller = CapacityController::start(limits, reserves()).unwrap();
                let first = controller
                    .try_admit(category, footprint, Retention::Terminal)
                    .unwrap();
                assert!(controller.status().admission_open());
                let second = controller
                    .try_admit(category, footprint, Retention::Terminal)
                    .unwrap();
                assert!(!controller.status().admission_open());
                assert!(!controller.status().poll_open());
                controller.release_terminal(first).unwrap();
                assert!(controller.status().admission_open());
                controller.release_terminal(second).unwrap();
            }
        }
    }

    #[test]
    fn v07_task_4_3_rejection_never_evicts_live_recovery_state() {
        let mut controller = CapacityController::start(
            CapacityLimits::uniform(CapacityLimit::new(4, 16).unwrap()),
            reserves(),
        )
        .unwrap();
        let message_id = [0x41; 16];
        let live = controller
            .try_admit_identity(message_id, CapacityFootprint::new(4, 16).unwrap())
            .unwrap();
        assert_eq!(
            controller.release_terminal(live),
            Err(CapacityError::LiveStateCannotBeEvicted)
        );
        assert_eq!(
            controller.usage(StateCategory::ProcessedIdentity),
            CapacityFootprint::new(4, 16).unwrap()
        );
        assert!(!controller.status().poll_open());
        assert_eq!(
            controller.try_admit_identity(message_id, CapacityFootprint::new(1, 1).unwrap()),
            Err(CapacityError::DuplicateIdentityCharge)
        );
        let proof =
            DurableGcProof::after_root_sync(2, BTreeMap::from([(message_id, live)])).unwrap();
        assert_eq!(proof.generation(), 2);
        controller
            .release_identity_after_gc(live, message_id, &proof)
            .unwrap();
        assert_eq!(
            controller.usage(StateCategory::ProcessedIdentity),
            CapacityFootprint::default()
        );
        assert!(controller.status().poll_open());

        let replacement = controller
            .try_admit_identity(message_id, CapacityFootprint::new(1, 1).unwrap())
            .unwrap();
        assert_eq!(
            controller.release_identity_after_gc(replacement, message_id, &proof),
            Err(CapacityError::InvalidGcProof)
        );
    }

    #[test]
    fn v07_task_4_3_startup_requires_both_reserves_and_holds_compaction_reserve() {
        let limits = CapacityLimits::uniform(CapacityLimit::new(4, 16).unwrap());
        assert_eq!(
            CapacityController::start(limits, StartupReserves::none()).unwrap_err(),
            CapacityError::StartupReserveUnavailable
        );
        let mut controller = CapacityController::start(limits, reserves()).unwrap();
        controller.begin_compaction().unwrap();
        assert_eq!(
            controller.begin_compaction(),
            Err(CapacityError::CompactionAlreadyActive)
        );
        controller.finish_compaction().unwrap();
    }

    #[test]
    fn v07_task_4_3_clock_uncertainty_stops_poll_until_trust_is_restored() {
        let mut controller = CapacityController::start(
            CapacityLimits::uniform(CapacityLimit::new(4, 16).unwrap()),
            reserves(),
        )
        .unwrap();
        controller.stop_for_clock_uncertainty();
        assert!(!controller.status().admission_open());
        assert!(!controller.status().poll_open());
        controller.restore_trusted_clock();
        assert!(controller.status().poll_open());
    }
}
