//! Versioned canonical Durable envelope encoding and bounded verification.
//!
//! Version 1 fixes this network-byte-order layout:
//! `version | message_id | source | target | generation | partition |
//! ordering_key_len | ordering_key | payload_len | payload | payload_sha256 |
//! envelope_sha256`. The envelope digest independently hashes the domain
//! separator followed by every field through the payload, including both
//! variable-width length prefixes. Attempt and session metadata never enter
//! either the canonical bytes or digest.

use crate::message_id::is_uuid_v4;
use alopex_chirps_core::durable::{
    CanonicalEnvelope, DurableMessageId, EnvelopeDigest, PayloadDigest,
};
use alopex_chirps_wire::node_id::NodeId;
use sha2::{Digest, Sha256};
use std::ops::Range;
use thiserror::Error;

/// The only canonical Durable envelope version supported by v0.7.
pub const CODEC_VERSION: u16 = 1;

/// Hash-domain separator for canonical Durable envelopes.
pub const DOMAIN_SEPARATOR: &[u8] = b"ALOPEX-CHIRPS-DURABLE-ENVELOPE\0";

/// Maximum encoded envelope size accepted by the pinned Iggy 0.10.0 payload
/// model. The bound is checked before encoder or decoder allocation.
pub const MAX_CANONICAL_ENVELOPE_LEN: usize = 64_000_000;

/// Maximum ordering-key length supported by the pinned Iggy 0.10.0 binary
/// partitioning model.
pub const MAX_ORDERING_KEY_LEN: usize = 255;

const VERSION_LEN: usize = 2;
const UUID_LEN: usize = 16;
const GENERATION_LEN: usize = 8;
const PARTITION_LEN: usize = 4;
const ORDERING_KEY_LENGTH_LEN: usize = 4;
const PAYLOAD_LENGTH_LEN: usize = 8;
const DIGEST_LEN: usize = 32;
const ORDERING_KEY_LENGTH_OFFSET: usize =
    VERSION_LEN + UUID_LEN + UUID_LEN + UUID_LEN + GENERATION_LEN + PARTITION_LEN;
const FIXED_WIRE_LEN: usize = ORDERING_KEY_LENGTH_OFFSET
    + ORDERING_KEY_LENGTH_LEN
    + PAYLOAD_LENGTH_LEN
    + DIGEST_LEN
    + DIGEST_LEN;

/// Largest application payload when the ordering key is empty. A non-empty
/// ordering key consumes the same bounded canonical-envelope budget.
pub const MAX_APPLICATION_PAYLOAD_LEN: usize = MAX_CANONICAL_ENVELOPE_LEN - FIXED_WIRE_LEN;

/// Borrowed fields bound into one version-1 canonical envelope.
#[derive(Debug, Clone, Copy)]
pub struct EnvelopeFields<'a> {
    source: NodeId,
    target: NodeId,
    generation: u64,
    partition: u32,
    ordering_key: &'a [u8],
    payload: &'a [u8],
}

impl<'a> EnvelopeFields<'a> {
    /// Creates the exact state-free fields fixed before a send attempt.
    #[must_use]
    pub const fn new(
        source: NodeId,
        target: NodeId,
        generation: u64,
        partition: u32,
        ordering_key: &'a [u8],
        payload: &'a [u8],
    ) -> Self {
        Self {
            source,
            target,
            generation,
            partition,
            ordering_key,
            payload,
        }
    }
}

/// Produces immutable canonical bytes and both SHA-256 digests for a generated
/// message identity.
pub fn encode(
    message_id: &DurableMessageId,
    fields: EnvelopeFields<'_>,
) -> Result<CanonicalEnvelope, EnvelopeEncodeError> {
    let parts = encode_parts(message_id.as_bytes(), fields)?;
    CanonicalEnvelope::try_new(
        CODEC_VERSION,
        parts.bytes,
        parts.payload_digest,
        parts.envelope_digest,
    )
    .map_err(|_| EnvelopeEncodeError::InvariantViolation)
}

#[derive(Debug)]
struct EncodedParts {
    bytes: Vec<u8>,
    payload_digest: PayloadDigest,
    envelope_digest: EnvelopeDigest,
}

fn encode_parts(
    message_id: &[u8; UUID_LEN],
    fields: EnvelopeFields<'_>,
) -> Result<EncodedParts, EnvelopeEncodeError> {
    if !is_uuid_v4(message_id) {
        return Err(EnvelopeEncodeError::InvalidMessageId);
    }
    if fields.ordering_key.len() > MAX_ORDERING_KEY_LEN {
        return Err(EnvelopeEncodeError::OrderingKeyTooLarge {
            actual: fields.ordering_key.len(),
            maximum: MAX_ORDERING_KEY_LEN,
        });
    }
    if fields.payload.len() > MAX_APPLICATION_PAYLOAD_LEN {
        return Err(EnvelopeEncodeError::PayloadTooLarge {
            actual: fields.payload.len(),
            maximum: MAX_APPLICATION_PAYLOAD_LEN,
        });
    }

    let encoded_len = FIXED_WIRE_LEN
        .checked_add(fields.ordering_key.len())
        .and_then(|length| length.checked_add(fields.payload.len()))
        .ok_or(EnvelopeEncodeError::LengthOverflow)?;
    if encoded_len > MAX_CANONICAL_ENVELOPE_LEN {
        return Err(EnvelopeEncodeError::EnvelopeTooLarge {
            actual: encoded_len,
            maximum: MAX_CANONICAL_ENVELOPE_LEN,
        });
    }

    let ordering_key_len = u32::try_from(fields.ordering_key.len())
        .map_err(|_| EnvelopeEncodeError::LengthOverflow)?;
    let payload_len =
        u64::try_from(fields.payload.len()).map_err(|_| EnvelopeEncodeError::LengthOverflow)?;
    let payload_digest = PayloadDigest::from_bytes(Sha256::digest(fields.payload).into());
    let envelope_digest = compute_envelope_digest(message_id, fields);

    let mut bytes = Vec::with_capacity(encoded_len);
    bytes.extend_from_slice(&CODEC_VERSION.to_be_bytes());
    bytes.extend_from_slice(message_id);
    bytes.extend_from_slice(fields.source.as_bytes());
    bytes.extend_from_slice(fields.target.as_bytes());
    bytes.extend_from_slice(&fields.generation.to_be_bytes());
    bytes.extend_from_slice(&fields.partition.to_be_bytes());
    bytes.extend_from_slice(&ordering_key_len.to_be_bytes());
    bytes.extend_from_slice(fields.ordering_key);
    bytes.extend_from_slice(&payload_len.to_be_bytes());
    bytes.extend_from_slice(fields.payload);
    bytes.extend_from_slice(payload_digest.as_bytes());
    bytes.extend_from_slice(envelope_digest.as_bytes());
    debug_assert_eq!(bytes.len(), encoded_len);

    Ok(EncodedParts {
        bytes,
        payload_digest,
        envelope_digest,
    })
}

fn compute_envelope_digest(
    message_id: &[u8; UUID_LEN],
    fields: EnvelopeFields<'_>,
) -> EnvelopeDigest {
    let ordering_key_len = fields.ordering_key.len() as u32;
    let payload_len = fields.payload.len() as u64;
    let mut hasher = Sha256::new();
    hasher.update(DOMAIN_SEPARATOR);
    hasher.update(CODEC_VERSION.to_be_bytes());
    hasher.update(message_id);
    hasher.update(fields.source.as_bytes());
    hasher.update(fields.target.as_bytes());
    hasher.update(fields.generation.to_be_bytes());
    hasher.update(fields.partition.to_be_bytes());
    hasher.update(ordering_key_len.to_be_bytes());
    hasher.update(fields.ordering_key);
    hasher.update(payload_len.to_be_bytes());
    hasher.update(fields.payload);
    EnvelopeDigest::from_bytes(hasher.finalize().into())
}

/// A state-free canonical envelope encoding failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EnvelopeEncodeError {
    /// A wire identity supplied by an internal checked path was not UUIDv4.
    #[error("canonical envelope message identity is not UUIDv4")]
    InvalidMessageId,
    /// The ordering key exceeds the pinned provider bound.
    #[error("ordering key length {actual} exceeds maximum {maximum}")]
    OrderingKeyTooLarge { actual: usize, maximum: usize },
    /// The application payload exceeds the bounded envelope budget.
    #[error("payload length {actual} exceeds maximum {maximum}")]
    PayloadTooLarge { actual: usize, maximum: usize },
    /// The complete canonical envelope exceeds the provider payload bound.
    #[error("canonical envelope length {actual} exceeds maximum {maximum}")]
    EnvelopeTooLarge { actual: usize, maximum: usize },
    /// Length arithmetic or its fixed-width representation overflowed.
    #[error("canonical envelope length overflow")]
    LengthOverflow,
    /// The provider-neutral canonical envelope rejected an encoder invariant.
    #[error("canonical envelope invariant violation")]
    InvariantViolation,
}

/// One fully verified canonical envelope. The input is copied exactly once,
/// after all bounds, framing, identity, digest, and trailing-byte checks pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedEnvelope {
    codec_version: u16,
    message_id: [u8; UUID_LEN],
    source: NodeId,
    target: NodeId,
    generation: u64,
    partition: u32,
    ordering_key_range: Range<usize>,
    payload_range: Range<usize>,
    payload_digest: PayloadDigest,
    envelope_digest: EnvelopeDigest,
    canonical_bytes: Vec<u8>,
}

impl DecodedEnvelope {
    /// Returns the checked codec version.
    #[must_use]
    pub const fn codec_version(&self) -> u16 {
        self.codec_version
    }

    /// Returns the checked UUIDv4 bytes received from the wire.
    #[must_use]
    pub const fn message_id_bytes(&self) -> &[u8; UUID_LEN] {
        &self.message_id
    }

    /// Returns the source node bound by the canonical digest.
    #[must_use]
    pub const fn source(&self) -> NodeId {
        self.source
    }

    /// Returns the target node bound by the canonical digest.
    #[must_use]
    pub const fn target(&self) -> NodeId {
        self.target
    }

    /// Returns the inbox generation bound by the canonical digest.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the explicit partition bound by the canonical digest.
    #[must_use]
    pub const fn partition(&self) -> u32 {
        self.partition
    }

    /// Returns the checked ordering-key slice.
    #[must_use]
    pub fn ordering_key(&self) -> &[u8] {
        &self.canonical_bytes[self.ordering_key_range.clone()]
    }

    /// Returns the checked application payload slice.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.canonical_bytes[self.payload_range.clone()]
    }

    /// Returns the verified payload SHA-256 digest.
    #[must_use]
    pub const fn payload_digest(&self) -> PayloadDigest {
        self.payload_digest
    }

    /// Returns the verified canonical-envelope SHA-256 digest.
    #[must_use]
    pub const fn envelope_digest(&self) -> EnvelopeDigest {
        self.envelope_digest
    }

    /// Returns the exact checked canonical bytes.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    /// Verifies the identity-horizon rule for two checked observations.
    pub fn ensure_same_identity(&self, other: &Self) -> Result<(), EnvelopeIdentityError> {
        if self.message_id != other.message_id {
            return Err(EnvelopeIdentityError::DifferentMessageId);
        }
        if self.envelope_digest != other.envelope_digest {
            return Err(EnvelopeIdentityError::SameMessageIdDifferentEnvelope);
        }
        if self.canonical_bytes != other.canonical_bytes {
            return Err(EnvelopeIdentityError::DigestCollisionOrCorruption);
        }
        Ok(())
    }
}

/// Decodes and verifies one canonical envelope without allocating from any
/// untrusted length. Unknown versions are critical and fail closed.
pub fn decode(bytes: &[u8]) -> Result<DecodedEnvelope, EnvelopeDecodeError> {
    if bytes.len() > MAX_CANONICAL_ENVELOPE_LEN {
        return Err(EnvelopeDecodeError::EnvelopeTooLarge {
            actual: bytes.len(),
            maximum: MAX_CANONICAL_ENVELOPE_LEN,
        });
    }

    let mut reader = CheckedReader::new(bytes);
    let codec_version = reader.read_u16()?;
    if codec_version != CODEC_VERSION {
        return Err(EnvelopeDecodeError::UnsupportedVersion(codec_version));
    }

    let message_id = reader.read_array::<UUID_LEN>()?;
    if !is_uuid_v4(&message_id) {
        return Err(EnvelopeDecodeError::InvalidMessageId);
    }
    let source = NodeId::from(reader.read_array::<UUID_LEN>()?);
    let target = NodeId::from(reader.read_array::<UUID_LEN>()?);
    let generation = reader.read_u64()?;
    let partition = reader.read_u32()?;

    let ordering_key_len = reader.read_u32()? as usize;
    if ordering_key_len > MAX_ORDERING_KEY_LEN {
        return Err(EnvelopeDecodeError::OrderingKeyTooLarge {
            actual: ordering_key_len,
            maximum: MAX_ORDERING_KEY_LEN,
        });
    }
    let ordering_key_start = reader.position();
    let ordering_key = reader.take(ordering_key_len)?;
    let ordering_key_range = ordering_key_start..reader.position();

    let payload_len_u64 = reader.read_u64()?;
    if payload_len_u64 > MAX_APPLICATION_PAYLOAD_LEN as u64 {
        return Err(EnvelopeDecodeError::PayloadTooLarge {
            actual: payload_len_u64,
            maximum: MAX_APPLICATION_PAYLOAD_LEN as u64,
        });
    }
    let payload_len =
        usize::try_from(payload_len_u64).map_err(|_| EnvelopeDecodeError::LengthOverflow)?;
    let payload_start = reader.position();
    let payload = reader.take(payload_len)?;
    let payload_range = payload_start..reader.position();

    let claimed_payload_digest = reader.read_array::<DIGEST_LEN>()?;
    let claimed_envelope_digest = reader.read_array::<DIGEST_LEN>()?;
    if reader.remaining() != 0 {
        return Err(EnvelopeDecodeError::TrailingBytes);
    }

    let recomputed_payload_digest: [u8; DIGEST_LEN] = Sha256::digest(payload).into();
    if claimed_payload_digest != recomputed_payload_digest {
        return Err(EnvelopeDecodeError::PayloadDigestMismatch);
    }
    let fields = EnvelopeFields::new(source, target, generation, partition, ordering_key, payload);
    let recomputed_envelope_digest = compute_envelope_digest(&message_id, fields);
    if claimed_envelope_digest != *recomputed_envelope_digest.as_bytes() {
        return Err(EnvelopeDecodeError::EnvelopeDigestMismatch);
    }

    Ok(DecodedEnvelope {
        codec_version,
        message_id,
        source,
        target,
        generation,
        partition,
        ordering_key_range,
        payload_range,
        payload_digest: PayloadDigest::from_bytes(claimed_payload_digest),
        envelope_digest: EnvelopeDigest::from_bytes(claimed_envelope_digest),
        canonical_bytes: bytes.to_vec(),
    })
}

/// A rejected canonical envelope received from an untrusted source.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EnvelopeDecodeError {
    /// The complete wire value exceeds the fixed allocation bound.
    #[error("canonical envelope length {actual} exceeds maximum {maximum}")]
    EnvelopeTooLarge { actual: usize, maximum: usize },
    /// A fixed-width field or declared byte range was truncated.
    #[error("canonical envelope is truncated")]
    Truncated,
    /// The decoded fixed-width length cannot be represented safely.
    #[error("canonical envelope length overflow")]
    LengthOverflow,
    /// The version is unknown and therefore treated as critical.
    #[error("unsupported canonical envelope version {0}")]
    UnsupportedVersion(u16),
    /// The received logical identity is not an RFC 4122 UUIDv4 value.
    #[error("canonical envelope message identity is not UUIDv4")]
    InvalidMessageId,
    /// The ordering key exceeds the fixed provider limit.
    #[error("ordering key length {actual} exceeds maximum {maximum}")]
    OrderingKeyTooLarge { actual: usize, maximum: usize },
    /// The payload declaration exceeds the fixed envelope budget.
    #[error("payload length {actual} exceeds maximum {maximum}")]
    PayloadTooLarge { actual: u64, maximum: u64 },
    /// The payload does not match its claimed SHA-256 digest.
    #[error("canonical envelope payload digest mismatch")]
    PayloadDigestMismatch,
    /// Decoded canonical fields do not match the claimed envelope digest.
    #[error("canonical envelope digest mismatch")]
    EnvelopeDigestMismatch,
    /// Bytes remain after the one complete versioned envelope.
    #[error("canonical envelope has trailing bytes")]
    TrailingBytes,
}

/// A conflict between two individually checked envelope observations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum EnvelopeIdentityError {
    /// The observations refer to different logical messages.
    #[error("canonical envelopes have different message identities")]
    DifferentMessageId,
    /// One logical message ID was bound to two envelope digests.
    #[error("the same message identity is bound to different envelopes")]
    SameMessageIdDifferentEnvelope,
    /// Equal identity/digest metadata accompanied unequal canonical bytes.
    #[error("equal canonical digest accompanied different envelope bytes")]
    DigestCollisionOrCorruption,
}

struct CheckedReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> CheckedReader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    const fn position(&self) -> usize {
        self.position
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], EnvelopeDecodeError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(EnvelopeDecodeError::LengthOverflow)?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(EnvelopeDecodeError::Truncated)?;
        self.position = end;
        Ok(value)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], EnvelopeDecodeError> {
        let mut value = [0; N];
        value.copy_from_slice(self.take(N)?);
        Ok(value)
    }

    fn read_u16(&mut self) -> Result<u16, EnvelopeDecodeError> {
        Ok(u16::from_be_bytes(self.read_array()?))
    }

    fn read_u32(&mut self) -> Result<u32, EnvelopeDecodeError> {
        Ok(u32::from_be_bytes(self.read_array()?))
    }

    fn read_u64(&mut self) -> Result<u64, EnvelopeDecodeError> {
        Ok(u64::from_be_bytes(self.read_array()?))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CODEC_VERSION, DOMAIN_SEPARATOR, EnvelopeDecodeError, EnvelopeFields,
        EnvelopeIdentityError, MAX_CANONICAL_ENVELOPE_LEN, MAX_ORDERING_KEY_LEN,
        ORDERING_KEY_LENGTH_OFFSET, decode, encode, encode_parts,
    };
    use alopex_chirps_core::durable::{DurableMessageRoute, PrepareFailure, PreparedDurableSend};
    use alopex_chirps_wire::node_id::NodeId;
    use proptest::prelude::*;

    const GOLDEN_MESSAGE_ID: [u8; 16] = [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x46, 0x07, 0x88, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f,
    ];
    const GOLDEN_PAYLOAD_DIGEST: [u8; 32] = [
        0x2c, 0xf2, 0x4d, 0xba, 0x5f, 0xb0, 0xa3, 0x0e, 0x26, 0xe8, 0x3b, 0x2a, 0xc5, 0xb9, 0xe2,
        0x9e, 0x1b, 0x16, 0x1e, 0x5c, 0x1f, 0xa7, 0x42, 0x5e, 0x73, 0x04, 0x33, 0x62, 0x93, 0x8b,
        0x98, 0x24,
    ];
    const GOLDEN_ENVELOPE_DIGEST: [u8; 32] = [
        0xb5, 0x52, 0xf9, 0x14, 0x62, 0x9a, 0xa6, 0x93, 0xdf, 0x5d, 0x35, 0x6f, 0x66, 0x54, 0xff,
        0x3c, 0xc6, 0x37, 0xe7, 0xd1, 0x9e, 0xd0, 0xad, 0xc3, 0xca, 0xd8, 0x6b, 0x13, 0x36, 0x4f,
        0x2c, 0x17,
    ];

    fn golden_parts() -> super::EncodedParts {
        encode_parts(
            &GOLDEN_MESSAGE_ID,
            EnvelopeFields::new(
                NodeId::from([0x11; 16]),
                NodeId::from([0x22; 16]),
                0x0102_0304_0506_0708,
                0x0a0b_0c0d,
                b"key",
                b"hello",
            ),
        )
        .expect("golden envelope must encode")
    }

    fn to_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn v07_task_3_2_golden_vector_freezes_v1_encoding_and_digests() {
        let parts = golden_parts();
        assert_eq!(CODEC_VERSION, 1);
        assert_eq!(DOMAIN_SEPARATOR, b"ALOPEX-CHIRPS-DURABLE-ENVELOPE\0");
        assert_eq!(parts.payload_digest.as_bytes(), &GOLDEN_PAYLOAD_DIGEST);
        assert_eq!(parts.envelope_digest.as_bytes(), &GOLDEN_ENVELOPE_DIGEST);
        assert_eq!(
            to_hex(&parts.bytes),
            "0001000102030405460788090a0b0c0d0e0f111111111111111111111111111111112222222222222222222222222222222201020304050607080a0b0c0d000000036b6579000000000000000568656c6c6f2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824b552f914629aa693df5d356f6654ff3cc637e7d19ed0adc3cad86b13364f2c17"
        );
    }

    #[test]
    fn v07_task_3_2_round_trip_preserves_every_canonical_field() {
        let parts = golden_parts();
        let decoded = decode(&parts.bytes).expect("golden envelope must decode");

        assert_eq!(decoded.codec_version(), CODEC_VERSION);
        assert_eq!(decoded.message_id_bytes(), &GOLDEN_MESSAGE_ID);
        assert_eq!(decoded.source(), NodeId::from([0x11; 16]));
        assert_eq!(decoded.target(), NodeId::from([0x22; 16]));
        assert_eq!(decoded.generation(), 0x0102_0304_0506_0708);
        assert_eq!(decoded.partition(), 0x0a0b_0c0d);
        assert_eq!(decoded.ordering_key(), b"key");
        assert_eq!(decoded.payload(), b"hello");
        assert_eq!(decoded.payload_digest(), parts.payload_digest);
        assert_eq!(decoded.envelope_digest(), parts.envelope_digest);
        assert_eq!(decoded.canonical_bytes(), parts.bytes);
    }

    #[test]
    fn v07_task_3_2_public_encoder_freezes_a_prepared_send_before_backend_contact() {
        let source = NodeId::from([0x51; 16]);
        let target = NodeId::from([0x62; 16]);
        let route = DurableMessageRoute::new(source, target, 7, 3, b"order".to_vec(), 1);
        let prepared = PreparedDurableSend::prepare(route, |message_id| {
            encode(
                message_id,
                EnvelopeFields::new(source, target, 7, 3, b"order", b"payload"),
            )
            .map_err(|_| PrepareFailure::CanonicalEncoding)
        })
        .expect("bounded canonical preparation must succeed");
        let decoded = decode(prepared.canonical_bytes()).expect("prepared bytes must verify");

        assert_eq!(decoded.message_id_bytes(), prepared.message_id().as_bytes());
        assert_eq!(decoded.source(), prepared.source());
        assert_eq!(decoded.target(), prepared.target());
        assert_eq!(decoded.generation(), prepared.generation());
        assert_eq!(decoded.partition(), prepared.partition());
        assert_eq!(decoded.ordering_key(), prepared.ordering_key());
        assert_eq!(decoded.payload_digest(), prepared.payload_digest());
        assert_eq!(decoded.envelope_digest(), prepared.envelope_digest());
    }

    #[test]
    fn v07_task_3_2_corruption_substitution_and_trailing_bytes_are_rejected() {
        let parts = golden_parts();
        let decoded = decode(&parts.bytes).expect("locate checked payload");

        let mut payload_corruption = parts.bytes.clone();
        payload_corruption[decoded.payload_range.start] ^= 0x01;
        assert_eq!(
            decode(&payload_corruption),
            Err(EnvelopeDecodeError::PayloadDigestMismatch)
        );

        let mut route_substitution = parts.bytes.clone();
        route_substitution[2 + 16] ^= 0x01;
        assert_eq!(
            decode(&route_substitution),
            Err(EnvelopeDecodeError::EnvelopeDigestMismatch)
        );

        let mut payload_digest_substitution = parts.bytes.clone();
        let payload_digest_offset = payload_digest_substitution.len() - 64;
        payload_digest_substitution[payload_digest_offset] ^= 0x01;
        assert_eq!(
            decode(&payload_digest_substitution),
            Err(EnvelopeDecodeError::PayloadDigestMismatch)
        );

        let mut envelope_digest_substitution = parts.bytes.clone();
        let last = envelope_digest_substitution.len() - 1;
        envelope_digest_substitution[last] ^= 0x01;
        assert_eq!(
            decode(&envelope_digest_substitution),
            Err(EnvelopeDecodeError::EnvelopeDigestMismatch)
        );

        let mut trailing = parts.bytes;
        trailing.push(0);
        assert_eq!(decode(&trailing), Err(EnvelopeDecodeError::TrailingBytes));
    }

    #[test]
    fn v07_task_3_2_bounds_versions_uuid_and_all_truncations_fail_closed() {
        let parts = golden_parts();

        for len in 0..parts.bytes.len() {
            assert!(
                decode(&parts.bytes[..len]).is_err(),
                "truncation at {len} unexpectedly decoded"
            );
        }

        let mut unknown_version = parts.bytes.clone();
        unknown_version[..2].copy_from_slice(&2_u16.to_be_bytes());
        assert_eq!(
            decode(&unknown_version),
            Err(EnvelopeDecodeError::UnsupportedVersion(2))
        );

        let mut invalid_uuid = parts.bytes.clone();
        invalid_uuid[2 + 6] &= 0x0f;
        assert_eq!(
            decode(&invalid_uuid),
            Err(EnvelopeDecodeError::InvalidMessageId)
        );

        let mut oversized_key = parts.bytes.clone();
        oversized_key[ORDERING_KEY_LENGTH_OFFSET..ORDERING_KEY_LENGTH_OFFSET + 4]
            .copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(matches!(
            decode(&oversized_key),
            Err(EnvelopeDecodeError::OrderingKeyTooLarge { .. })
        ));

        let mut oversized_payload = parts.bytes.clone();
        let payload_length_offset = ORDERING_KEY_LENGTH_OFFSET + 4 + b"key".len();
        oversized_payload[payload_length_offset..payload_length_offset + 8]
            .copy_from_slice(&u64::MAX.to_be_bytes());
        assert!(matches!(
            decode(&oversized_payload),
            Err(EnvelopeDecodeError::PayloadTooLarge { .. })
        ));

        let oversized_bytes = vec![0; MAX_CANONICAL_ENVELOPE_LEN + 1];
        assert!(matches!(
            decode(&oversized_bytes),
            Err(EnvelopeDecodeError::EnvelopeTooLarge { .. })
        ));
        assert_eq!(MAX_ORDERING_KEY_LEN, 255);

        let oversized_ordering_key = vec![0; MAX_ORDERING_KEY_LEN + 1];
        assert!(matches!(
            encode_parts(
                &GOLDEN_MESSAGE_ID,
                EnvelopeFields::new(
                    NodeId::from([0x11; 16]),
                    NodeId::from([0x22; 16]),
                    1,
                    0,
                    &oversized_ordering_key,
                    b"payload",
                ),
            ),
            Err(super::EnvelopeEncodeError::OrderingKeyTooLarge { .. })
        ));
    }

    #[test]
    fn v07_task_3_2_same_id_with_different_envelope_is_typed_corruption() {
        let first = golden_parts();
        let second = encode_parts(
            &GOLDEN_MESSAGE_ID,
            EnvelopeFields::new(
                NodeId::from([0x11; 16]),
                NodeId::from([0x22; 16]),
                0x0102_0304_0506_0708,
                0x0a0b_0c0d,
                b"key",
                b"different",
            ),
        )
        .expect("second envelope must encode");
        let first = decode(&first.bytes).expect("first envelope must decode");
        let second = decode(&second.bytes).expect("second envelope must decode");

        assert_eq!(
            first.ensure_same_identity(&second),
            Err(EnvelopeIdentityError::SameMessageIdDifferentEnvelope)
        );
    }

    proptest! {
        #[test]
        fn v07_task_3_2_property_round_trip(
            ordering_key in proptest::collection::vec(any::<u8>(), 0..=64),
            payload in proptest::collection::vec(any::<u8>(), 0..=4096),
            generation in any::<u64>(),
            partition in any::<u32>(),
        ) {
            let parts = encode_parts(
                &GOLDEN_MESSAGE_ID,
                EnvelopeFields::new(
                    NodeId::from([0x31; 16]),
                    NodeId::from([0x42; 16]),
                    generation,
                    partition,
                    &ordering_key,
                    &payload,
                ),
            ).expect("bounded property case must encode");
            let decoded = decode(&parts.bytes).expect("encoded property case must decode");

            prop_assert_eq!(decoded.message_id_bytes(), &GOLDEN_MESSAGE_ID);
            prop_assert_eq!(decoded.ordering_key(), ordering_key.as_slice());
            prop_assert_eq!(decoded.payload(), payload.as_slice());
            prop_assert_eq!(decoded.generation(), generation);
            prop_assert_eq!(decoded.partition(), partition);
        }
    }
}
