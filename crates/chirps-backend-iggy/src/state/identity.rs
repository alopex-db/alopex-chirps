//! Durable identity-horizon records owned by the local checkpoint journal.

use alopex_chirps_core::durable::{DurableMessageId, EnvelopeDigest, ResourceEpoch, ResourceId};
use thiserror::Error;

const IDENTITY_BODY_LEN: usize = 110;

/// Provenance attached to the durable retry-not-before wall-clock value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClockProvenance {
    Trusted = 1,
    RollbackDetected = 2,
    Unknown = 3,
}

impl ClockProvenance {
    fn decode(value: u8) -> Result<Self, IdentityError> {
        match value {
            1 => Ok(Self::Trusted),
            2 => Ok(Self::RollbackDetected),
            3 => Ok(Self::Unknown),
            _ => Err(IdentityError::InvalidEncoding),
        }
    }
}

/// One checked identity observation that must become durable before delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IdentityCandidate {
    message_id: DurableMessageId,
    envelope_digest: EnvelopeDigest,
    resource_epoch: ResourceEpoch,
    partition: u32,
    offset: u64,
    delivery_attempt: u64,
    retry_not_before_unix_ms: u64,
    clock_provenance: ClockProvenance,
}

impl IdentityCandidate {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        message_id: DurableMessageId,
        envelope_digest: EnvelopeDigest,
        resource_epoch: ResourceEpoch,
        partition: u32,
        offset: u64,
        delivery_attempt: u64,
        retry_not_before_unix_ms: u64,
        clock_provenance: ClockProvenance,
    ) -> Result<Self, IdentityError> {
        if delivery_attempt == 0 {
            return Err(IdentityError::InvalidDeliveryAttempt);
        }
        Ok(Self {
            message_id,
            envelope_digest,
            resource_epoch,
            partition,
            offset,
            delivery_attempt,
            retry_not_before_unix_ms,
            clock_provenance,
        })
    }

    pub(crate) const fn message_id(self) -> DurableMessageId {
        self.message_id
    }

    pub(crate) const fn envelope_digest(self) -> EnvelopeDigest {
        self.envelope_digest
    }

    pub(crate) const fn resource_epoch(self) -> ResourceEpoch {
        self.resource_epoch
    }

    pub(crate) const fn partition(self) -> u32 {
        self.partition
    }

    pub(crate) const fn offset(self) -> u64 {
        self.offset
    }

    pub(crate) const fn delivery_attempt(self) -> u64 {
        self.delivery_attempt
    }
}

/// Canonical horizon entry reconstructed from the contiguous journal prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IdentityRecord {
    message_id: [u8; 16],
    envelope_digest: [u8; 32],
    resource_id: [u8; 16],
    resource_epoch: u64,
    partition: u32,
    offset: u64,
    last_delivery_attempt: u64,
    observation_count: u64,
    checkpointed: bool,
    retry_not_before_unix_ms: u64,
    clock_provenance: ClockProvenance,
}

impl IdentityRecord {
    pub(crate) fn first(candidate: IdentityCandidate) -> Self {
        Self {
            message_id: *candidate.message_id.as_bytes(),
            envelope_digest: *candidate.envelope_digest.as_bytes(),
            resource_id: *candidate.resource_epoch.resource_id().as_bytes(),
            resource_epoch: candidate.resource_epoch.epoch(),
            partition: candidate.partition,
            offset: candidate.offset,
            last_delivery_attempt: candidate.delivery_attempt,
            observation_count: 1,
            checkpointed: false,
            retry_not_before_unix_ms: candidate.retry_not_before_unix_ms,
            clock_provenance: candidate.clock_provenance,
        }
    }

    pub(crate) fn observe(&self, candidate: IdentityCandidate) -> Result<Self, IdentityError> {
        if self.message_id != *candidate.message_id.as_bytes()
            || self.envelope_digest != *candidate.envelope_digest.as_bytes()
        {
            return Err(IdentityError::IdentityConflict);
        }
        if self.resource_id != *candidate.resource_epoch.resource_id().as_bytes()
            || self.resource_epoch != candidate.resource_epoch.epoch()
            || self.partition != candidate.partition
            || self.offset != candidate.offset
            || self.retry_not_before_unix_ms != candidate.retry_not_before_unix_ms
            || self.clock_provenance != candidate.clock_provenance
        {
            return Err(IdentityError::MetadataConflict);
        }
        if self.checkpointed {
            return Err(IdentityError::AlreadyCheckpointed);
        }
        if candidate.delivery_attempt <= self.last_delivery_attempt {
            return Err(IdentityError::StaleDeliveryAttempt);
        }
        let mut next = self.clone();
        next.last_delivery_attempt = candidate.delivery_attempt;
        next.observation_count = next
            .observation_count
            .checked_add(1)
            .ok_or(IdentityError::ObservationCountExhausted)?;
        Ok(next)
    }

    pub(crate) fn mark_checkpointed(&self) -> Result<Self, IdentityError> {
        if self.checkpointed {
            return Err(IdentityError::AlreadyCheckpointed);
        }
        let mut next = self.clone();
        next.checkpointed = true;
        Ok(next)
    }

    pub(crate) fn validate_successor(&self, next: &Self) -> Result<(), IdentityError> {
        if self.message_id != next.message_id || self.envelope_digest != next.envelope_digest {
            return Err(IdentityError::IdentityConflict);
        }
        if self.resource_id != next.resource_id
            || self.resource_epoch != next.resource_epoch
            || self.partition != next.partition
            || self.offset != next.offset
            || self.retry_not_before_unix_ms != next.retry_not_before_unix_ms
            || self.clock_provenance != next.clock_provenance
        {
            return Err(IdentityError::MetadataConflict);
        }
        if self.checkpointed
            || next.checkpointed
            || next.last_delivery_attempt <= self.last_delivery_attempt
            || self.observation_count.checked_add(1) != Some(next.observation_count)
        {
            return Err(IdentityError::StaleDeliveryAttempt);
        }
        Ok(())
    }

    pub(crate) fn encode_body(&self) -> [u8; IDENTITY_BODY_LEN] {
        let mut body = [0_u8; IDENTITY_BODY_LEN];
        let mut position = 0;
        for bytes in [
            self.message_id.as_slice(),
            self.envelope_digest.as_slice(),
            self.resource_id.as_slice(),
            self.resource_epoch.to_be_bytes().as_slice(),
            self.partition.to_be_bytes().as_slice(),
            self.offset.to_be_bytes().as_slice(),
            self.last_delivery_attempt.to_be_bytes().as_slice(),
            self.observation_count.to_be_bytes().as_slice(),
        ] {
            body[position..position + bytes.len()].copy_from_slice(bytes);
            position += bytes.len();
        }
        body[position] = u8::from(self.checkpointed);
        position += 1;
        body[position..position + 8].copy_from_slice(&self.retry_not_before_unix_ms.to_be_bytes());
        position += 8;
        body[position] = self.clock_provenance as u8;
        body
    }

    pub(crate) fn decode_body(body: &[u8]) -> Result<Self, IdentityError> {
        if body.len() != IDENTITY_BODY_LEN {
            return Err(IdentityError::InvalidEncoding);
        }
        let message_id: [u8; 16] = body[0..16]
            .try_into()
            .map_err(|_| IdentityError::InvalidEncoding)?;
        if message_id[6] & 0xf0 != 0x40 || message_id[8] & 0xc0 != 0x80 {
            return Err(IdentityError::InvalidMessageId);
        }
        let checkpointed = match body[100] {
            0 => false,
            1 => true,
            _ => return Err(IdentityError::InvalidEncoding),
        };
        let record = Self {
            message_id,
            envelope_digest: body[16..48]
                .try_into()
                .map_err(|_| IdentityError::InvalidEncoding)?,
            resource_id: body[48..64]
                .try_into()
                .map_err(|_| IdentityError::InvalidEncoding)?,
            resource_epoch: u64::from_be_bytes(
                body[64..72]
                    .try_into()
                    .map_err(|_| IdentityError::InvalidEncoding)?,
            ),
            partition: u32::from_be_bytes(
                body[72..76]
                    .try_into()
                    .map_err(|_| IdentityError::InvalidEncoding)?,
            ),
            offset: u64::from_be_bytes(
                body[76..84]
                    .try_into()
                    .map_err(|_| IdentityError::InvalidEncoding)?,
            ),
            last_delivery_attempt: u64::from_be_bytes(
                body[84..92]
                    .try_into()
                    .map_err(|_| IdentityError::InvalidEncoding)?,
            ),
            observation_count: u64::from_be_bytes(
                body[92..100]
                    .try_into()
                    .map_err(|_| IdentityError::InvalidEncoding)?,
            ),
            checkpointed,
            retry_not_before_unix_ms: u64::from_be_bytes(
                body[101..109]
                    .try_into()
                    .map_err(|_| IdentityError::InvalidEncoding)?,
            ),
            clock_provenance: ClockProvenance::decode(body[109])?,
        };
        if record.last_delivery_attempt == 0 || record.observation_count == 0 {
            return Err(IdentityError::InvalidEncoding);
        }
        Ok(record)
    }

    #[cfg(test)]
    pub(crate) fn fixture(
        message_id: [u8; 16],
        envelope_digest: [u8; 32],
        resource_id: [u8; 16],
        resource_epoch: u64,
        partition: u32,
        offset: u64,
        delivery_attempt: u64,
    ) -> Result<Self, IdentityError> {
        let mut body = [0_u8; IDENTITY_BODY_LEN];
        body[0..16].copy_from_slice(&message_id);
        body[16..48].copy_from_slice(&envelope_digest);
        body[48..64].copy_from_slice(&resource_id);
        body[64..72].copy_from_slice(&resource_epoch.to_be_bytes());
        body[72..76].copy_from_slice(&partition.to_be_bytes());
        body[76..84].copy_from_slice(&offset.to_be_bytes());
        body[84..92].copy_from_slice(&delivery_attempt.to_be_bytes());
        body[92..100].copy_from_slice(&1_u64.to_be_bytes());
        body[100] = 0;
        body[101..109].copy_from_slice(&1_000_u64.to_be_bytes());
        body[109] = ClockProvenance::Trusted as u8;
        Self::decode_body(&body)
    }

    pub(crate) const fn message_id_bytes(&self) -> [u8; 16] {
        self.message_id
    }

    pub(crate) const fn envelope_digest_bytes(&self) -> [u8; 32] {
        self.envelope_digest
    }

    pub(crate) const fn resource_epoch(&self) -> ResourceEpoch {
        ResourceEpoch::new(
            ResourceId::from_bytes(self.resource_id),
            self.resource_epoch,
        )
    }

    pub(crate) const fn partition(&self) -> u32 {
        self.partition
    }

    pub(crate) const fn offset(&self) -> u64 {
        self.offset
    }

    pub(crate) const fn last_delivery_attempt(&self) -> u64 {
        self.last_delivery_attempt
    }

    pub(crate) const fn observation_count(&self) -> u64 {
        self.observation_count
    }

    pub(crate) const fn is_checkpointed(&self) -> bool {
        self.checkpointed
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(crate) enum IdentityError {
    #[error("identity delivery attempt must be nonzero")]
    InvalidDeliveryAttempt,
    #[error("identity frame contains invalid encoding")]
    InvalidEncoding,
    #[error("identity frame message ID is not UUIDv4")]
    InvalidMessageId,
    #[error("the same message ID was observed with a different envelope digest")]
    IdentityConflict,
    #[error("the same identity was observed with conflicting immutable metadata")]
    MetadataConflict,
    #[error("a stale or duplicate delivery attempt cannot be recorded")]
    StaleDeliveryAttempt,
    #[error("a checkpointed identity cannot be observed as a new attempt")]
    AlreadyCheckpointed,
    #[error("identity observation count is exhausted")]
    ObservationCountExhausted,
}
