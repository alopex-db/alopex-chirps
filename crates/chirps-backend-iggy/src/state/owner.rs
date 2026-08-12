//! Checksummed linked owner records for one canonical subscription directory.

use super::{StateFrameError, StateRecordKind, decode_state_frame, digest, encode_state_frame};
use thiserror::Error;
use uuid::Uuid;

const OWNER_BODY_LEN: usize = 32 + 32 + 8 + 8 + 16;

/// One canonical owner-chain record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OwnerRecord {
    creation_digest: [u8; 32],
    previous_owner_digest: [u8; 32],
    owner_epoch: u64,
    owner_generation: u64,
    owner_id: [u8; 16],
}

impl OwnerRecord {
    pub(crate) fn fresh_genesis() -> Self {
        Self {
            creation_digest: [0; 32],
            previous_owner_digest: [0; 32],
            owner_epoch: 1,
            owner_generation: 1,
            owner_id: *Uuid::new_v4().as_bytes(),
        }
    }

    #[cfg(test)]
    pub(crate) fn fixture_genesis(mut owner_id: [u8; 16]) -> Self {
        owner_id[6] = (owner_id[6] & 0x0f) | 0x40;
        owner_id[8] = (owner_id[8] & 0x3f) | 0x80;
        Self {
            creation_digest: [0; 32],
            previous_owner_digest: [0; 32],
            owner_epoch: 1,
            owner_generation: 1,
            owner_id,
        }
    }

    pub(crate) fn next(
        current: &Self,
        creation_digest: [u8; 32],
    ) -> Result<Self, OwnerRecordError> {
        let owner_epoch = current
            .owner_epoch
            .checked_add(1)
            .ok_or(OwnerRecordError::OwnerEpochExhausted)?;
        let owner_generation = current
            .owner_generation
            .checked_add(1)
            .ok_or(OwnerRecordError::OwnerGenerationExhausted)?;
        Ok(Self {
            creation_digest,
            previous_owner_digest: current.digest(),
            owner_epoch,
            owner_generation,
            owner_id: *Uuid::new_v4().as_bytes(),
        })
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, OwnerRecordError> {
        let body = decode_state_frame(bytes, StateRecordKind::OwnerRecord)?;
        if body.len() != OWNER_BODY_LEN {
            return Err(OwnerRecordError::InvalidBody);
        }
        let mut creation_digest = [0; 32];
        creation_digest.copy_from_slice(&body[..32]);
        let mut previous_owner_digest = [0; 32];
        previous_owner_digest.copy_from_slice(&body[32..64]);
        let owner_epoch = u64::from_be_bytes(body[64..72].try_into().expect("fixed owner body"));
        let owner_generation =
            u64::from_be_bytes(body[72..80].try_into().expect("fixed owner body"));
        let mut owner_id = [0; 16];
        owner_id.copy_from_slice(&body[80..96]);
        let record = Self {
            creation_digest,
            previous_owner_digest,
            owner_epoch,
            owner_generation,
            owner_id,
        };
        record.validate_identity()?;
        if owner_epoch == 0 || owner_generation == 0 || owner_epoch != owner_generation {
            return Err(OwnerRecordError::InvalidBody);
        }
        Ok(record)
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut body = Vec::with_capacity(OWNER_BODY_LEN);
        body.extend_from_slice(&self.creation_digest);
        body.extend_from_slice(&self.previous_owner_digest);
        body.extend_from_slice(&self.owner_epoch.to_be_bytes());
        body.extend_from_slice(&self.owner_generation.to_be_bytes());
        body.extend_from_slice(&self.owner_id);
        encode_state_frame(StateRecordKind::OwnerRecord, &body)
            .expect("fixed owner record remains below frame bound")
    }

    pub(crate) fn digest(&self) -> [u8; 32] {
        digest(&self.encode())
    }

    pub(crate) const fn owner_epoch(&self) -> u64 {
        self.owner_epoch
    }

    pub(crate) const fn owner_generation(&self) -> u64 {
        self.owner_generation
    }

    pub(crate) const fn owner_id(&self) -> [u8; 16] {
        self.owner_id
    }

    pub(crate) const fn creation_digest(&self) -> [u8; 32] {
        self.creation_digest
    }

    pub(crate) const fn previous_owner_digest(&self) -> [u8; 32] {
        self.previous_owner_digest
    }

    pub(crate) fn validate_genesis(&self) -> Result<(), OwnerRecordError> {
        if self.owner_epoch != 1
            || self.owner_generation != 1
            || self.creation_digest != [0; 32]
            || self.previous_owner_digest != [0; 32]
        {
            return Err(OwnerRecordError::InvalidGenesis);
        }
        self.validate_identity()
    }

    pub(crate) fn validate_current(
        &self,
        expected_creation_digest: [u8; 32],
    ) -> Result<(), OwnerRecordError> {
        self.validate_identity()?;
        if self.owner_epoch <= 1
            || self.owner_generation != self.owner_epoch
            || self.creation_digest != expected_creation_digest
            || self.previous_owner_digest == [0; 32]
        {
            return Err(OwnerRecordError::InvalidLink);
        }
        Ok(())
    }

    fn validate_identity(&self) -> Result<(), OwnerRecordError> {
        if self.owner_id[6] & 0xf0 != 0x40 || self.owner_id[8] & 0xc0 != 0x80 {
            return Err(OwnerRecordError::InvalidOwnerId);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub(crate) enum OwnerRecordError {
    #[error("owner record frame is invalid: {0}")]
    Frame(#[from] StateFrameError),
    #[error("owner record body is invalid")]
    InvalidBody,
    #[error("owner identity is not UUIDv4")]
    InvalidOwnerId,
    #[error("embedded genesis owner is invalid")]
    InvalidGenesis,
    #[error("owner record does not link to the canonical creation chain")]
    InvalidLink,
    #[error("owner epoch is exhausted")]
    OwnerEpochExhausted,
    #[error("owner generation is exhausted")]
    OwnerGenerationExhausted,
}

#[cfg(test)]
mod tests {
    use super::{OwnerRecord, OwnerRecordError};

    #[test]
    fn v07_task_4_1_owner_chain_binds_creation_previous_digest_and_monotonic_epoch() {
        let genesis = OwnerRecord::fresh_genesis();
        genesis.validate_genesis().unwrap();
        let creation_digest = [0x41; 32];
        let second = OwnerRecord::next(&genesis, creation_digest).unwrap();
        assert_eq!(second.owner_epoch(), 2);
        assert_eq!(second.owner_generation(), 2);
        assert_eq!(second.creation_digest(), creation_digest);
        assert_eq!(second.previous_owner_digest(), genesis.digest());
        assert_eq!(OwnerRecord::decode(&second.encode()).unwrap(), second);
        second.validate_current(creation_digest).unwrap();

        let third = OwnerRecord::next(&second, creation_digest).unwrap();
        assert_eq!(third.owner_epoch(), 3);
        assert_eq!(third.previous_owner_digest(), second.digest());
    }

    #[test]
    fn v07_task_4_1_owner_substitution_and_corruption_fail_closed() {
        let genesis = OwnerRecord::fresh_genesis();
        let current = OwnerRecord::next(&genesis, [0x21; 32]).unwrap();
        assert_eq!(
            current.validate_current([0x22; 32]),
            Err(OwnerRecordError::InvalidLink)
        );

        let mut corrupt = current.encode();
        corrupt[20] ^= 1;
        assert!(OwnerRecord::decode(&corrupt).is_err());
        let mut trailing = current.encode();
        trailing.push(0);
        assert!(OwnerRecord::decode(&trailing).is_err());
    }
}
