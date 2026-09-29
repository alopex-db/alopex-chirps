//! Canonical bounded wire codec for the Chirps-owned Iggy private extension.
//!
//! The outer request/response frames are the official Iggy 0.10.0 Sans-I/O
//! framing types. The payload is not official protocol 0.10.0: it is a
//! separately versioned Chirps extension with its own command registry,
//! permission tag, schema digest, body bound, and integrity digest.

use crate::codec::{MAX_CANONICAL_ENVELOPE_LEN, decode as decode_envelope};
use alopex_chirps_core::durable::{
    CheckedPollRecord, EnvelopeDigest, PollObservation, ResourceEpoch, ResourceId,
    SessionFingerprint,
};
use bytes::{BufMut, Bytes, BytesMut};
use iggy_binary_protocol::{RequestFrame, ResponseFrame, STATUS_OK};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Version of the Chirps-owned private extension payload.
pub const PRIVATE_EXTENSION_VERSION: u16 = 1;
/// Private command code for capability binding.
pub const CAPABILITY_BIND_CODE: u32 = 0x8000_0701;
/// Private command code for lease renewal.
pub const LEASE_RENEW_CODE: u32 = 0x8000_0702;
/// Private command code for one exact OS-synced append.
pub const APPEND_ONE_SYNCED_CODE: u32 = 0x8000_0703;
/// Private command code for one atomic bounded poll observation.
pub const CHECKED_POLL_CODE: u32 = 0x8000_0704;

/// Canonical complete wire-contract description whose digest is carried by
/// every frame. Numeric registries, outer framing, integrity domains, bounds,
/// and all command bodies are part of the immutable schema identity.
pub const EXTENSION_SCHEMA_DESCRIPTOR: &[u8] = concat!(
    "chirps-iggy-private-extension/v1;",
    "official_outer=iggy_binary_protocol@0.10.0;",
    "outer.request=length:u32le=4+payload_n,code:u32le,payload[payload_n];",
    "outer.response=status:u32le=0,length:u32le=payload_n,payload[payload_n];outer.nonzero_status=rejected;",
    "magic=0x4348525007000000;version:u16le=1;",
    "codes=CapabilityBind:0x80000701,LeaseRenew:0x80000702,AppendOneSynced:0x80000703,CheckedPoll:0x80000704;",
    "directions=Request:1,Response:2;permissions=Bind:1,Renew:2,Append:3,Poll:4;",
    "schema_digest=SHA-256(descriptor_bytes);",
    "frame_digest=SHA-256(0x414c4f5045582d4348495250532d494747592d505249564154452d4652414d4500||inner_header_and_body);",
    "endian=le;inner=magic[8],version:u16,command:u32,direction:u8,permission:u8,body_len:u32,schema_digest[32],body,frame_digest[32];",
    "tags=checksum.Disabled:0,checksum.Enabled:1,strong_boundary.OsSyncedAccepted:2;",
    "constraints=lease_ms>0,expiry_ms>0,resource_id:UUIDv4,attempt_id:UUIDv4,message_id:UUIDv4;",
    "bounds=ordering_key<=255,envelope<=64000000,body<=64000512,complete_frame<=64000604,append_count=1,poll_max_count=1,poll_response_count=0|1;",
    "embedded_envelope=version:u16be=1,message_id[16],source[16],target[16],generation:u64be,partition:u32be,ordering_key_len:u32be,ordering_key,payload_len:u64be,payload,payload_sha256[32],envelope_sha256[32];",
    "embedded_payload_digest=SHA-256(payload);embedded_envelope_digest=SHA-256(0x414c4f5045582d4348495250532d44555241424c452d454e56454c4f504500||embedded_fields_through_payload);",
    "CapabilityBind.req=stream:u32,topic:u32,partition:u32,lease_ms:u32;",
    "CapabilityBind.res=build_sha[20],boot_id[16],resource_id[16],resource_epoch:u64,stream:u32,topic:u32,partition:u32,retention_bytes:u64,retention_messages:u64,checksum:u8,config_digest[32],security_digest[32],capability_digest[32],session_id[16],fingerprint[32],expiry_ms:u64;",
    "LeaseRenew.req=session_id[16],boot_id[16],fingerprint[32],lease_ms:u32;",
    "LeaseRenew.res=session_id[16],boot_id[16],fingerprint[32],expiry_ms:u64;",
    "AppendOneSynced.req=session_id[16],boot_id[16],fingerprint[32],attempt_id[16],boundary:u8=2,resource_id[16],resource_epoch:u64,stream:u32,topic:u32,partition:u32,count:u8=1,message_id[16],envelope_digest[32],envelope_len:u32,envelope;",
    "AppendOneSynced.res=session_id[16],boot_id[16],fingerprint[32],attempt_id[16],boundary:u8=2,resource_id[16],resource_epoch:u64,stream:u32,topic:u32,partition:u32,count:u8=1,offset:u64,index:u64,message_id[16],envelope_digest[32];",
    "CheckedPoll.req=session_id[16],boot_id[16],fingerprint[32],resource_id[16],resource_epoch:u64,stream:u32,topic:u32,partition:u32,expected_offset:u64,max_count:u8=1;",
    "CheckedPoll.res=session_id[16],boot_id[16],fingerprint[32],resource_id[16],resource_epoch:u64,stream:u32,topic:u32,partition:u32,expected_offset:u64,end_exclusive:u64,oldest:u64,count:u8=0|1,record=(offset:u64,message_id[16],envelope_digest[32],envelope_len:u32,envelope)"
)
.as_bytes();

/// SHA-256 of [`EXTENSION_SCHEMA_DESCRIPTOR`].
pub const EXTENSION_SCHEMA_DIGEST: [u8; 32] = [
    0x71, 0xe4, 0x62, 0x39, 0x56, 0x75, 0x01, 0x7a, 0x7f, 0xd5, 0xbe, 0x74, 0xd2, 0xf7, 0x1b, 0x0a,
    0x24, 0x88, 0xda, 0x88, 0x2a, 0xca, 0x52, 0xd2, 0x7b, 0xd1, 0x91, 0x6d, 0x93, 0xc3, 0xd6, 0xa2,
];

const PRIVATE_MAGIC: [u8; 8] = *b"CHRP\x07\0\0\0";
const FRAME_DIGEST_DOMAIN: &[u8] = b"ALOPEX-CHIRPS-IGGY-PRIVATE-FRAME\0";
const INNER_HEADER_LEN: usize = 52;
const FRAME_DIGEST_LEN: usize = 32;
const OUTER_FRAME_LEN: usize = 8;
const MAX_PRIVATE_OVERHEAD: usize = 512;

/// Maximum decoded private body length, including one maximum envelope.
pub const MAX_PRIVATE_BODY_LEN: usize = MAX_CANONICAL_ENVELOPE_LEN + MAX_PRIVATE_OVERHEAD;
/// Maximum complete official frame accepted by this codec.
pub const MAX_PRIVATE_FRAME_LEN: usize =
    OUTER_FRAME_LEN + INNER_HEADER_LEN + MAX_PRIVATE_BODY_LEN + FRAME_DIGEST_LEN;

/// Immutable digest manifest for one client-owned canonical golden frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientGoldenVector {
    /// Stable vector name used by the compatible-server cross-match.
    pub name: &'static str,
    /// Private command encoded by the vector.
    pub command: PrivateCommand,
    /// True for an official success-response frame; false for a request frame.
    pub is_response: bool,
    /// Exact complete-frame byte length.
    pub frame_len: usize,
    /// SHA-256 of the complete official frame and private payload.
    pub frame_sha256: [u8; 32],
}

/// Client-owned v1 golden manifest. Task 5.2 must produce byte-identical
/// server vectors and match all eight lengths and digests.
pub const CLIENT_GOLDEN_VECTORS: [ClientGoldenVector; 8] = [
    ClientGoldenVector {
        name: "capability-bind-request",
        command: PrivateCommand::CapabilityBind,
        is_response: false,
        frame_len: 108,
        frame_sha256: [
            200, 174, 159, 235, 75, 75, 146, 9, 167, 239, 163, 167, 240, 85, 147, 127, 118, 81,
            189, 198, 70, 184, 8, 55, 113, 172, 31, 19, 7, 84, 188, 23,
        ],
    },
    ClientGoldenVector {
        name: "capability-bind-response",
        command: PrivateCommand::CapabilityBind,
        is_response: true,
        frame_len: 333,
        frame_sha256: [
            127, 213, 11, 38, 99, 93, 41, 186, 90, 117, 70, 52, 229, 93, 2, 137, 158, 149, 15, 113,
            200, 83, 164, 160, 4, 68, 227, 129, 99, 95, 238, 44,
        ],
    },
    ClientGoldenVector {
        name: "lease-renew-request",
        command: PrivateCommand::LeaseRenew,
        is_response: false,
        frame_len: 160,
        frame_sha256: [
            180, 191, 217, 253, 61, 250, 47, 96, 74, 33, 58, 86, 199, 2, 231, 7, 151, 153, 68, 172,
            77, 83, 116, 116, 203, 39, 250, 126, 250, 109, 31, 217,
        ],
    },
    ClientGoldenVector {
        name: "lease-renew-response",
        command: PrivateCommand::LeaseRenew,
        is_response: true,
        frame_len: 164,
        frame_sha256: [
            123, 250, 86, 204, 238, 62, 139, 204, 135, 167, 72, 248, 125, 131, 224, 64, 232, 178,
            187, 216, 102, 170, 217, 87, 3, 184, 174, 63, 125, 97, 55, 65,
        ],
    },
    ClientGoldenVector {
        name: "append-one-synced-request",
        command: PrivateCommand::AppendOneSynced,
        is_response: false,
        frame_len: 408,
        frame_sha256: [
            8, 116, 214, 64, 94, 77, 137, 80, 39, 27, 241, 148, 83, 73, 235, 123, 16, 47, 46, 103,
            150, 84, 119, 240, 174, 125, 241, 223, 41, 6, 91, 220,
        ],
    },
    ClientGoldenVector {
        name: "append-one-synced-response",
        command: PrivateCommand::AppendOneSynced,
        is_response: true,
        frame_len: 274,
        frame_sha256: [
            174, 96, 31, 173, 1, 138, 94, 56, 104, 1, 53, 7, 202, 44, 108, 198, 243, 41, 243, 245,
            241, 232, 14, 250, 88, 89, 126, 2, 214, 151, 136, 186,
        ],
    },
    ClientGoldenVector {
        name: "checked-poll-request",
        command: PrivateCommand::CheckedPoll,
        is_response: false,
        frame_len: 201,
        frame_sha256: [
            34, 28, 140, 16, 27, 83, 210, 66, 190, 117, 188, 209, 253, 226, 167, 90, 244, 216, 38,
            165, 139, 66, 46, 253, 46, 243, 142, 180, 57, 25, 67, 228,
        ],
    },
    ClientGoldenVector {
        name: "checked-poll-response",
        command: PrivateCommand::CheckedPoll,
        is_response: true,
        frame_len: 423,
        frame_sha256: [
            45, 198, 11, 111, 71, 34, 131, 88, 238, 6, 143, 174, 184, 221, 143, 158, 28, 155, 54,
            162, 194, 35, 135, 173, 15, 6, 60, 164, 174, 0, 162, 234,
        ],
    },
];

/// The four command identities owned by the v0.7 private extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PrivateCommand {
    /// Authenticate and bind a capability/resource/session projection.
    CapabilityBind,
    /// Renew one connection-bound lease.
    LeaseRenew,
    /// Append exactly one canonical envelope and return an exact sync receipt.
    AppendOneSynced,
    /// Observe bounds and at most one record atomically.
    CheckedPoll,
}

impl PrivateCommand {
    /// Returns the private command code carried in both outer and inner frames.
    #[must_use]
    pub const fn code(self) -> u32 {
        match self {
            Self::CapabilityBind => CAPABILITY_BIND_CODE,
            Self::LeaseRenew => LEASE_RENEW_CODE,
            Self::AppendOneSynced => APPEND_ONE_SYNCED_CODE,
            Self::CheckedPoll => CHECKED_POLL_CODE,
        }
    }

    const fn permission(self) -> PrivatePermission {
        match self {
            Self::CapabilityBind => PrivatePermission::Bind,
            Self::LeaseRenew => PrivatePermission::Renew,
            Self::AppendOneSynced => PrivatePermission::Append,
            Self::CheckedPoll => PrivatePermission::Poll,
        }
    }

    fn from_code(code: u32) -> Result<Self, PrivateProtocolError> {
        match code {
            CAPABILITY_BIND_CODE => Ok(Self::CapabilityBind),
            LEASE_RENEW_CODE => Ok(Self::LeaseRenew),
            APPEND_ONE_SYNCED_CODE => Ok(Self::AppendOneSynced),
            CHECKED_POLL_CODE => Ok(Self::CheckedPoll),
            _ => Err(PrivateProtocolError::UnknownCommand(code)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum PrivatePermission {
    Bind = 1,
    Renew = 2,
    Append = 3,
    Poll = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Direction {
    Request = 1,
    Response = 2,
}

/// Exact binding of a protected operation to one authenticated connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionBinding {
    session_id: [u8; 16],
    boot_id: [u8; 16],
    fingerprint: SessionFingerprint,
}

impl SessionBinding {
    /// Creates a binding from values verified during capability bind.
    #[must_use]
    pub const fn new(
        session_id: [u8; 16],
        boot_id: [u8; 16],
        fingerprint: SessionFingerprint,
    ) -> Self {
        Self {
            session_id,
            boot_id,
            fingerprint,
        }
    }

    /// Returns the server session identity.
    #[must_use]
    pub const fn session_id(&self) -> &[u8; 16] {
        &self.session_id
    }

    /// Returns the server boot identity.
    #[must_use]
    pub const fn boot_id(&self) -> &[u8; 16] {
        &self.boot_id
    }

    /// Returns the authenticated projection fingerprint.
    #[must_use]
    pub const fn fingerprint(&self) -> SessionFingerprint {
        self.fingerprint
    }
}

/// Exact resource incarnation and explicit Iggy location.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ResourceLocation {
    resource_id: ResourceId,
    resource_epoch: u64,
    stream_id: u32,
    topic_id: u32,
    partition_id: u32,
}

impl ResourceLocation {
    /// Creates a location from a verified capability report.
    pub fn new(
        resource_id: ResourceId,
        resource_epoch: u64,
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
    ) -> Result<Self, PrivateProtocolError> {
        validate_uuid_v4(resource_id.as_bytes(), "resource ID")?;
        Ok(Self {
            resource_id,
            resource_epoch,
            stream_id,
            topic_id,
            partition_id,
        })
    }

    /// Returns the provider-neutral resource UUID/epoch pair.
    #[must_use]
    pub const fn resource_epoch(self) -> ResourceEpoch {
        ResourceEpoch::new(self.resource_id, self.resource_epoch)
    }

    /// Returns the explicit stream identifier.
    #[must_use]
    pub const fn stream_id(self) -> u32 {
        self.stream_id
    }

    /// Returns the explicit topic identifier.
    #[must_use]
    pub const fn topic_id(self) -> u32 {
        self.topic_id
    }

    /// Returns the explicit zero-based partition identifier.
    #[must_use]
    pub const fn partition_id(self) -> u32 {
        self.partition_id
    }
}

/// CapabilityBind request for one pre-provisioned explicit location.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapabilityBindRequest {
    stream_id: u32,
    topic_id: u32,
    partition_id: u32,
    requested_lease_millis: u32,
}

impl CapabilityBindRequest {
    /// Creates a bounded bind request.
    pub fn new(
        stream_id: u32,
        topic_id: u32,
        partition_id: u32,
        requested_lease_millis: u32,
    ) -> Result<Self, PrivateProtocolError> {
        validate_lease_duration(requested_lease_millis)?;
        Ok(Self {
            stream_id,
            topic_id,
            partition_id,
            requested_lease_millis,
        })
    }
}

/// Checksum state reported in the bound capability projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ChecksumMode {
    /// Partition checksum validation is disabled.
    Disabled = 0,
    /// Partition checksum validation is enabled.
    Enabled = 1,
}

/// Canonical capability/resource projection reported by the compatible server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapabilityReport {
    build_sha: [u8; 20],
    boot_id: [u8; 16],
    location: ResourceLocation,
    retention_bytes: u64,
    retention_messages: u64,
    checksum_mode: ChecksumMode,
    configuration_digest: [u8; 32],
    security_digest: [u8; 32],
    capability_digest: [u8; 32],
}

impl CapabilityReport {
    /// Creates the exact projection returned by a compatible server.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub const fn new(
        build_sha: [u8; 20],
        boot_id: [u8; 16],
        location: ResourceLocation,
        retention_bytes: u64,
        retention_messages: u64,
        checksum_mode: ChecksumMode,
        configuration_digest: [u8; 32],
        security_digest: [u8; 32],
        capability_digest: [u8; 32],
    ) -> Self {
        Self {
            build_sha,
            boot_id,
            location,
            retention_bytes,
            retention_messages,
            checksum_mode,
            configuration_digest,
            security_digest,
            capability_digest,
        }
    }

    /// Returns the exact resource/location projection.
    #[must_use]
    pub const fn location(self) -> ResourceLocation {
        self.location
    }

    /// Returns the pinned compatible-server build SHA bytes.
    #[must_use]
    pub const fn build_sha(&self) -> &[u8; 20] {
        &self.build_sha
    }

    /// Returns the server boot identity.
    #[must_use]
    pub const fn boot_id(&self) -> &[u8; 16] {
        &self.boot_id
    }

    /// Returns the bound retention byte projection.
    #[must_use]
    pub const fn retention_bytes(self) -> u64 {
        self.retention_bytes
    }

    /// Returns the bound retention message-count projection.
    #[must_use]
    pub const fn retention_messages(self) -> u64 {
        self.retention_messages
    }

    /// Returns the bound checksum mode.
    #[must_use]
    pub const fn checksum_mode(self) -> ChecksumMode {
        self.checksum_mode
    }

    /// Returns the exact configuration projection digest.
    #[must_use]
    pub const fn configuration_digest(&self) -> &[u8; 32] {
        &self.configuration_digest
    }

    /// Returns the exact security projection digest.
    #[must_use]
    pub const fn security_digest(&self) -> &[u8; 32] {
        &self.security_digest
    }

    /// Returns the exact capability projection digest.
    #[must_use]
    pub const fn capability_digest(&self) -> &[u8; 32] {
        &self.capability_digest
    }
}

/// CapabilityBind success response with the first active lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapabilityBindResponse {
    report: CapabilityReport,
    session_id: [u8; 16],
    session_fingerprint: SessionFingerprint,
    expires_at_monotonic_millis: u64,
}

impl CapabilityBindResponse {
    /// Creates a bind response; expiry zero is never an active lease.
    pub(crate) fn new(
        report: CapabilityReport,
        session_id: [u8; 16],
        session_fingerprint: SessionFingerprint,
        expires_at_monotonic_millis: u64,
    ) -> Result<Self, PrivateProtocolError> {
        validate_expiry(expires_at_monotonic_millis)?;
        Ok(Self {
            report,
            session_id,
            session_fingerprint,
            expires_at_monotonic_millis,
        })
    }
}

/// LeaseRenew request bound to the same authenticated connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseRenewRequest {
    binding: SessionBinding,
    requested_lease_millis: u32,
}

impl LeaseRenewRequest {
    /// Creates an explicit renewal request.
    pub fn new(
        binding: SessionBinding,
        requested_lease_millis: u32,
    ) -> Result<Self, PrivateProtocolError> {
        validate_lease_duration(requested_lease_millis)?;
        Ok(Self {
            binding,
            requested_lease_millis,
        })
    }
}

/// LeaseRenew success response with the new server-monotonic expiry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseRenewResponse {
    binding: SessionBinding,
    expires_at_monotonic_millis: u64,
}

impl LeaseRenewResponse {
    /// Creates a successful renewal response.
    pub(crate) fn new(
        binding: SessionBinding,
        expires_at_monotonic_millis: u64,
    ) -> Result<Self, PrivateProtocolError> {
        validate_expiry(expires_at_monotonic_millis)?;
        Ok(Self {
            binding,
            expires_at_monotonic_millis,
        })
    }
}

/// The sole confirmation boundary valid for AppendOneSynced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum StrongConfirmationBoundary {
    /// Message and index have reached the server's OS-sync boundary.
    OsSyncedAccepted = 2,
}

/// AppendOneSynced request for exactly one immutable canonical envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendOneSyncedRequest {
    binding: SessionBinding,
    attempt_id: [u8; 16],
    boundary: StrongConfirmationBoundary,
    location: ResourceLocation,
    message_id: [u8; 16],
    envelope_digest: EnvelopeDigest,
    canonical_envelope: Vec<u8>,
}

impl AppendOneSyncedRequest {
    /// Creates a one-record strong append request.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        binding: SessionBinding,
        attempt_id: [u8; 16],
        location: ResourceLocation,
        message_id: [u8; 16],
        envelope_digest: EnvelopeDigest,
        canonical_envelope: Vec<u8>,
    ) -> Result<Self, PrivateProtocolError> {
        validate_uuid_v4(&attempt_id, "attempt ID")?;
        validate_envelope_binding(
            &canonical_envelope,
            &message_id,
            envelope_digest,
            location.partition_id,
        )?;
        Ok(Self {
            binding,
            attempt_id,
            boundary: StrongConfirmationBoundary::OsSyncedAccepted,
            location,
            message_id,
            envelope_digest,
            canonical_envelope,
        })
    }

    /// Returns the immutable canonical envelope bytes.
    #[must_use]
    pub fn canonical_envelope(&self) -> &[u8] {
        &self.canonical_envelope
    }
}

/// AppendOneSynced exact success response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendOneSyncedResponse {
    binding: SessionBinding,
    attempt_id: [u8; 16],
    boundary: StrongConfirmationBoundary,
    location: ResourceLocation,
    assigned_offset: u64,
    assigned_index: u64,
    message_id: [u8; 16],
    envelope_digest: EnvelopeDigest,
}

impl AppendOneSyncedResponse {
    /// Creates the exact location-bound strong response.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        binding: SessionBinding,
        attempt_id: [u8; 16],
        location: ResourceLocation,
        assigned_offset: u64,
        assigned_index: u64,
        message_id: [u8; 16],
        envelope_digest: EnvelopeDigest,
    ) -> Result<Self, PrivateProtocolError> {
        validate_uuid_v4(&attempt_id, "attempt ID")?;
        validate_uuid_v4(&message_id, "message ID")?;
        Ok(Self {
            binding,
            attempt_id,
            boundary: StrongConfirmationBoundary::OsSyncedAccepted,
            location,
            assigned_offset,
            assigned_index,
            message_id,
            envelope_digest,
        })
    }
}

/// CheckedPoll request for one inclusive expected offset and at most one record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckedPollRequest {
    binding: SessionBinding,
    location: ResourceLocation,
    expected_offset: u64,
}

impl CheckedPollRequest {
    /// Creates a count-one checked poll request.
    #[must_use]
    pub const fn new(
        binding: SessionBinding,
        location: ResourceLocation,
        expected_offset: u64,
    ) -> Self {
        Self {
            binding,
            location,
            expected_offset,
        }
    }
}

/// CheckedPoll response from one atomic server observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckedPollResponse {
    binding: SessionBinding,
    location: ResourceLocation,
    expected_offset: u64,
    end_exclusive: u64,
    oldest_available: u64,
    record: Option<CheckedPollRecord>,
}

impl CheckedPollResponse {
    /// Creates an atomic observation; the subscriber owns canonical-envelope decoding.
    pub(crate) fn new(
        binding: SessionBinding,
        location: ResourceLocation,
        expected_offset: u64,
        end_exclusive: u64,
        oldest_available: u64,
        record: Option<CheckedPollRecord>,
    ) -> Result<Self, PrivateProtocolError> {
        PollObservation::try_new(
            location.resource_epoch(),
            end_exclusive,
            oldest_available,
            record.clone(),
        )
        .map_err(|_| PrivateProtocolError::InvalidPollObservation)?;
        Ok(Self {
            binding,
            location,
            expected_offset,
            end_exclusive,
            oldest_available,
            record,
        })
    }

    fn into_observation(self) -> Result<PollObservation, PrivateProtocolError> {
        PollObservation::try_new(
            self.location.resource_epoch(),
            self.end_exclusive,
            self.oldest_available,
            self.record,
        )
        .map_err(|_| PrivateProtocolError::InvalidPollObservation)
    }
}

/// Correlation-verified CapabilityBind output. No lease or capability data is
/// exposed from a decoded response before this type is constructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerifiedCapabilityBindResponse {
    report: CapabilityReport,
    binding: SessionBinding,
    expires_at_monotonic_millis: u64,
}

impl VerifiedCapabilityBindResponse {
    /// Returns the complete verified capability projection.
    #[must_use]
    pub const fn report(self) -> CapabilityReport {
        self.report
    }

    /// Returns the verified connection/session binding.
    #[must_use]
    pub const fn binding(self) -> SessionBinding {
        self.binding
    }

    /// Returns the active server-monotonic lease expiry.
    #[must_use]
    pub const fn expires_at_monotonic_millis(self) -> u64 {
        self.expires_at_monotonic_millis
    }
}

/// Correlation-verified LeaseRenew output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerifiedLeaseRenewResponse {
    binding: SessionBinding,
    expires_at_monotonic_millis: u64,
}

impl VerifiedLeaseRenewResponse {
    /// Returns the exact renewed connection/session binding.
    #[must_use]
    pub const fn binding(self) -> SessionBinding {
        self.binding
    }

    /// Returns the new server-monotonic expiry.
    #[must_use]
    pub const fn expires_at_monotonic_millis(self) -> u64 {
        self.expires_at_monotonic_millis
    }
}

/// Correlation-verified AppendOneSynced output. These are the only protocol
/// values from which the adapter may construct a strong receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VerifiedAppendOneSyncedResponse {
    location: ResourceLocation,
    assigned_offset: u64,
    assigned_index: u64,
}

impl VerifiedAppendOneSyncedResponse {
    /// Returns the exact verified resource/location projection.
    #[must_use]
    pub const fn location(self) -> ResourceLocation {
        self.location
    }

    /// Returns the assigned broker offset.
    #[must_use]
    pub const fn assigned_offset(self) -> u64 {
        self.assigned_offset
    }

    /// Returns the assigned broker index.
    #[must_use]
    pub const fn assigned_index(self) -> u64 {
        self.assigned_index
    }
}

/// Correlation-verified CheckedPoll output. Delivery-bearing observation data
/// is only available through this verified wrapper.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedCheckedPollResponse {
    observation: PollObservation,
}

impl VerifiedCheckedPollResponse {
    /// Borrows the provider-neutral atomic observation.
    #[must_use]
    pub const fn observation(&self) -> &PollObservation {
        &self.observation
    }

    /// Consumes the verified wrapper into the provider-neutral observation.
    #[must_use]
    pub fn into_observation(self) -> PollObservation {
        self.observation
    }
}

/// A response whose command and every safety-bearing echo were atomically
/// correlated with the initiating request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifiedPrivateResponse {
    /// Verified CapabilityBind response.
    CapabilityBind(VerifiedCapabilityBindResponse),
    /// Verified LeaseRenew response.
    LeaseRenew(VerifiedLeaseRenewResponse),
    /// Verified AppendOneSynced response.
    AppendOneSynced(VerifiedAppendOneSyncedResponse),
    /// Verified CheckedPoll response.
    CheckedPoll(VerifiedCheckedPollResponse),
}

impl VerifiedPrivateResponse {
    /// Returns the correlated private command identity.
    #[must_use]
    pub const fn command(&self) -> PrivateCommand {
        match self {
            Self::CapabilityBind(_) => PrivateCommand::CapabilityBind,
            Self::LeaseRenew(_) => PrivateCommand::LeaseRenew,
            Self::AppendOneSynced(_) => PrivateCommand::AppendOneSynced,
            Self::CheckedPoll(_) => PrivateCommand::CheckedPoll,
        }
    }
}

/// A canonical private request payload wrapped in official Iggy framing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrivateRequest {
    /// CapabilityBind request.
    CapabilityBind(CapabilityBindRequest),
    /// LeaseRenew request.
    LeaseRenew(LeaseRenewRequest),
    /// AppendOneSynced request.
    AppendOneSynced(AppendOneSyncedRequest),
    /// CheckedPoll request.
    CheckedPoll(CheckedPollRequest),
}

impl PrivateRequest {
    /// Returns the exact private command identity.
    #[must_use]
    pub const fn command(&self) -> PrivateCommand {
        match self {
            Self::CapabilityBind(_) => PrivateCommand::CapabilityBind,
            Self::LeaseRenew(_) => PrivateCommand::LeaseRenew,
            Self::AppendOneSynced(_) => PrivateCommand::AppendOneSynced,
            Self::CheckedPoll(_) => PrivateCommand::CheckedPoll,
        }
    }

    /// Encodes an exact official request frame with a canonical private payload.
    pub fn encode(&self) -> Result<Bytes, PrivateProtocolError> {
        let command = self.command();
        let body = encode_request_body(self)?;
        let payload = encode_inner(command, Direction::Request, &body)?;
        let mut frame = BytesMut::with_capacity(OUTER_FRAME_LEN + payload.len());
        RequestFrame::encode(command.code(), &payload, &mut frame)
            .map_err(|_| PrivateProtocolError::OuterFraming)?;
        Ok(frame.freeze())
    }

    /// Decodes one complete request and rejects unknown/trailing private data.
    pub fn decode(bytes: &[u8]) -> Result<Self, PrivateProtocolError> {
        validate_complete_frame_bound(bytes)?;
        let (frame, consumed) =
            RequestFrame::decode(bytes).map_err(|_| PrivateProtocolError::OuterFraming)?;
        if consumed != bytes.len() {
            return Err(PrivateProtocolError::TrailingBytes);
        }
        let outer_command = PrivateCommand::from_code(frame.code)?;
        let (inner_command, body) = decode_inner(frame.payload, Direction::Request)?;
        if outer_command != inner_command {
            return Err(PrivateProtocolError::CommandMismatch);
        }
        decode_request_body(inner_command, body)
    }
}

/// A canonical private success response wrapped in official Iggy framing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrivateResponse {
    /// CapabilityBind response.
    CapabilityBind(CapabilityBindResponse),
    /// LeaseRenew response.
    LeaseRenew(LeaseRenewResponse),
    /// AppendOneSynced response.
    AppendOneSynced(AppendOneSyncedResponse),
    /// CheckedPoll response.
    CheckedPoll(CheckedPollResponse),
}

impl PrivateResponse {
    /// Returns the exact private command identity echoed by the response.
    #[must_use]
    pub const fn command(&self) -> PrivateCommand {
        match self {
            Self::CapabilityBind(_) => PrivateCommand::CapabilityBind,
            Self::LeaseRenew(_) => PrivateCommand::LeaseRenew,
            Self::AppendOneSynced(_) => PrivateCommand::AppendOneSynced,
            Self::CheckedPoll(_) => PrivateCommand::CheckedPoll,
        }
    }

    /// Encodes an official success response with a canonical private payload.
    pub fn encode(&self) -> Result<Bytes, PrivateProtocolError> {
        let command = self.command();
        let body = encode_response_body(self)?;
        let payload = encode_inner(command, Direction::Response, &body)?;
        let mut frame = BytesMut::with_capacity(OUTER_FRAME_LEN + payload.len());
        ResponseFrame::encode_ok(&payload, &mut frame)
            .map_err(|_| PrivateProtocolError::OuterFraming)?;
        Ok(frame.freeze())
    }

    /// Decodes and atomically correlates one complete successful response.
    pub fn decode_for_request(
        bytes: &[u8],
        request: &PrivateRequest,
    ) -> Result<VerifiedPrivateResponse, PrivateProtocolError> {
        Self::decode(bytes)?.verify_request(request)
    }

    fn decode(bytes: &[u8]) -> Result<Self, PrivateProtocolError> {
        validate_complete_frame_bound(bytes)?;
        let (frame, consumed) =
            ResponseFrame::decode(bytes).map_err(|_| PrivateProtocolError::OuterFraming)?;
        if consumed != bytes.len() {
            return Err(PrivateProtocolError::TrailingBytes);
        }
        if frame.status != STATUS_OK {
            return Err(PrivateProtocolError::RemoteStatus(frame.status));
        }
        let (command, body) = decode_inner(frame.payload, Direction::Response)?;
        decode_response_body(command, body)
    }

    /// Consumes the raw response and verifies every echoed
    /// command/session/location/operation field before releasing any output
    /// from which a receipt, lease, or delivery may be constructed.
    fn verify_request(
        self,
        request: &PrivateRequest,
    ) -> Result<VerifiedPrivateResponse, PrivateProtocolError> {
        match (self, request) {
            (Self::CapabilityBind(response), PrivateRequest::CapabilityBind(request)) => {
                let location = response.report.location;
                if location.stream_id != request.stream_id
                    || location.topic_id != request.topic_id
                    || location.partition_id != request.partition_id
                {
                    return Err(PrivateProtocolError::ResponseBindingMismatch("location"));
                }
                Ok(VerifiedPrivateResponse::CapabilityBind(
                    VerifiedCapabilityBindResponse {
                        report: response.report,
                        binding: SessionBinding::new(
                            response.session_id,
                            response.report.boot_id,
                            response.session_fingerprint,
                        ),
                        expires_at_monotonic_millis: response.expires_at_monotonic_millis,
                    },
                ))
            }
            (Self::LeaseRenew(response), PrivateRequest::LeaseRenew(request)) => {
                ensure_equal(response.binding, request.binding, "session binding")?;
                Ok(VerifiedPrivateResponse::LeaseRenew(
                    VerifiedLeaseRenewResponse {
                        binding: response.binding,
                        expires_at_monotonic_millis: response.expires_at_monotonic_millis,
                    },
                ))
            }
            (Self::AppendOneSynced(response), PrivateRequest::AppendOneSynced(request)) => {
                ensure_equal(response.binding, request.binding, "session binding")?;
                ensure_equal(response.attempt_id, request.attempt_id, "attempt ID")?;
                ensure_equal(response.boundary, request.boundary, "confirmation boundary")?;
                ensure_equal(response.location, request.location, "location")?;
                ensure_equal(response.message_id, request.message_id, "message ID")?;
                ensure_equal(
                    response.envelope_digest,
                    request.envelope_digest,
                    "envelope digest",
                )?;
                Ok(VerifiedPrivateResponse::AppendOneSynced(
                    VerifiedAppendOneSyncedResponse {
                        location: response.location,
                        assigned_offset: response.assigned_offset,
                        assigned_index: response.assigned_index,
                    },
                ))
            }
            (Self::CheckedPoll(response), PrivateRequest::CheckedPoll(request)) => {
                ensure_equal(response.binding, request.binding, "session binding")?;
                ensure_equal(response.location, request.location, "location")?;
                ensure_equal(
                    response.expected_offset,
                    request.expected_offset,
                    "expected offset",
                )?;
                Ok(VerifiedPrivateResponse::CheckedPoll(
                    VerifiedCheckedPollResponse {
                        observation: response.into_observation()?,
                    },
                ))
            }
            _ => Err(PrivateProtocolError::CommandMismatch),
        }
    }
}

fn encode_inner(
    command: PrivateCommand,
    direction: Direction,
    body: &[u8],
) -> Result<Bytes, PrivateProtocolError> {
    if body.len() > MAX_PRIVATE_BODY_LEN {
        return Err(PrivateProtocolError::BodyTooLarge {
            actual: body.len(),
            maximum: MAX_PRIVATE_BODY_LEN,
        });
    }
    let body_len = u32::try_from(body.len()).map_err(|_| PrivateProtocolError::BodyTooLarge {
        actual: body.len(),
        maximum: MAX_PRIVATE_BODY_LEN,
    })?;
    let mut payload = BytesMut::with_capacity(INNER_HEADER_LEN + body.len() + FRAME_DIGEST_LEN);
    payload.put_slice(&PRIVATE_MAGIC);
    payload.put_u16_le(PRIVATE_EXTENSION_VERSION);
    payload.put_u32_le(command.code());
    payload.put_u8(direction as u8);
    payload.put_u8(command.permission() as u8);
    payload.put_u32_le(body_len);
    payload.put_slice(&EXTENSION_SCHEMA_DIGEST);
    payload.put_slice(body);
    let digest = private_frame_digest(&payload);
    payload.put_slice(&digest);
    Ok(payload.freeze())
}

fn decode_inner(
    payload: &[u8],
    expected_direction: Direction,
) -> Result<(PrivateCommand, &[u8]), PrivateProtocolError> {
    if payload.len() < INNER_HEADER_LEN + FRAME_DIGEST_LEN {
        return Err(PrivateProtocolError::Truncated);
    }
    let mut cursor = Cursor::new(payload);
    if cursor.read_array::<8>()? != PRIVATE_MAGIC {
        return Err(PrivateProtocolError::InvalidMagic);
    }
    let version = cursor.read_u16()?;
    if version != PRIVATE_EXTENSION_VERSION {
        return Err(PrivateProtocolError::UnsupportedVersion(version));
    }
    let command = PrivateCommand::from_code(cursor.read_u32()?)?;
    let direction = cursor.read_u8()?;
    if direction != expected_direction as u8 {
        return Err(PrivateProtocolError::WrongDirection(direction));
    }
    let permission = cursor.read_u8()?;
    if permission != command.permission() as u8 {
        return Err(PrivateProtocolError::PermissionMismatch);
    }
    let body_len = cursor.read_u32()? as usize;
    if body_len > MAX_PRIVATE_BODY_LEN {
        return Err(PrivateProtocolError::BodyTooLarge {
            actual: body_len,
            maximum: MAX_PRIVATE_BODY_LEN,
        });
    }
    if cursor.read_array::<32>()? != EXTENSION_SCHEMA_DIGEST {
        return Err(PrivateProtocolError::SchemaDigestMismatch);
    }
    let expected_len = INNER_HEADER_LEN
        .checked_add(body_len)
        .and_then(|value| value.checked_add(FRAME_DIGEST_LEN))
        .ok_or(PrivateProtocolError::BodyTooLarge {
            actual: body_len,
            maximum: MAX_PRIVATE_BODY_LEN,
        })?;
    if payload.len() < expected_len {
        return Err(PrivateProtocolError::Truncated);
    }
    if payload.len() > expected_len {
        return Err(PrivateProtocolError::TrailingBytes);
    }
    let body_start = INNER_HEADER_LEN;
    let body_end = body_start + body_len;
    let received_digest: [u8; 32] = payload[body_end..]
        .try_into()
        .map_err(|_| PrivateProtocolError::Truncated)?;
    if private_frame_digest(&payload[..body_end]) != received_digest {
        return Err(PrivateProtocolError::FrameDigestMismatch);
    }
    Ok((command, &payload[body_start..body_end]))
}

fn private_frame_digest(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(FRAME_DIGEST_DOMAIN);
    hasher.update(bytes);
    hasher.finalize().into()
}

fn encode_request_body(request: &PrivateRequest) -> Result<Bytes, PrivateProtocolError> {
    let mut body = BytesMut::new();
    match request {
        PrivateRequest::CapabilityBind(value) => {
            body.put_u32_le(value.stream_id);
            body.put_u32_le(value.topic_id);
            body.put_u32_le(value.partition_id);
            body.put_u32_le(value.requested_lease_millis);
        }
        PrivateRequest::LeaseRenew(value) => {
            encode_session(&mut body, value.binding);
            body.put_u32_le(value.requested_lease_millis);
        }
        PrivateRequest::AppendOneSynced(value) => {
            validate_canonical_envelope(&value.canonical_envelope)?;
            encode_session(&mut body, value.binding);
            body.put_slice(&value.attempt_id);
            body.put_u8(value.boundary as u8);
            encode_location(&mut body, value.location);
            body.put_u8(1);
            body.put_slice(&value.message_id);
            body.put_slice(value.envelope_digest.as_bytes());
            put_len_prefixed(&mut body, &value.canonical_envelope)?;
        }
        PrivateRequest::CheckedPoll(value) => {
            encode_session(&mut body, value.binding);
            encode_location(&mut body, value.location);
            body.put_u64_le(value.expected_offset);
            body.put_u8(1);
        }
    }
    Ok(body.freeze())
}

fn encode_response_body(response: &PrivateResponse) -> Result<Bytes, PrivateProtocolError> {
    let mut body = BytesMut::new();
    match response {
        PrivateResponse::CapabilityBind(value) => {
            encode_capability_report(&mut body, value.report);
            body.put_slice(&value.session_id);
            body.put_slice(value.session_fingerprint.as_bytes());
            body.put_u64_le(value.expires_at_monotonic_millis);
        }
        PrivateResponse::LeaseRenew(value) => {
            encode_session(&mut body, value.binding);
            body.put_u64_le(value.expires_at_monotonic_millis);
        }
        PrivateResponse::AppendOneSynced(value) => {
            encode_session(&mut body, value.binding);
            body.put_slice(&value.attempt_id);
            body.put_u8(value.boundary as u8);
            encode_location(&mut body, value.location);
            body.put_u8(1);
            body.put_u64_le(value.assigned_offset);
            body.put_u64_le(value.assigned_index);
            body.put_slice(&value.message_id);
            body.put_slice(value.envelope_digest.as_bytes());
        }
        PrivateResponse::CheckedPoll(value) => {
            encode_session(&mut body, value.binding);
            encode_location(&mut body, value.location);
            body.put_u64_le(value.expected_offset);
            body.put_u64_le(value.end_exclusive);
            body.put_u64_le(value.oldest_available);
            match &value.record {
                None => body.put_u8(0),
                Some(record) => {
                    body.put_u8(1);
                    body.put_u64_le(record.offset());
                    body.put_slice(record.message_id().as_bytes());
                    body.put_slice(record.envelope_digest().as_bytes());
                    put_len_prefixed(&mut body, record.canonical_bytes())?;
                }
            }
        }
    }
    Ok(body.freeze())
}

fn decode_request_body(
    command: PrivateCommand,
    body: &[u8],
) -> Result<PrivateRequest, PrivateProtocolError> {
    let mut cursor = Cursor::new(body);
    let request = match command {
        PrivateCommand::CapabilityBind => {
            let value = CapabilityBindRequest::new(
                cursor.read_u32()?,
                cursor.read_u32()?,
                cursor.read_u32()?,
                cursor.read_u32()?,
            )?;
            PrivateRequest::CapabilityBind(value)
        }
        PrivateCommand::LeaseRenew => {
            let value = LeaseRenewRequest::new(decode_session(&mut cursor)?, cursor.read_u32()?)?;
            PrivateRequest::LeaseRenew(value)
        }
        PrivateCommand::AppendOneSynced => {
            let binding = decode_session(&mut cursor)?;
            let attempt_id = cursor.read_array()?;
            decode_strong_boundary(cursor.read_u8()?)?;
            let location = decode_location(&mut cursor)?;
            validate_count(cursor.read_u8()?, 1)?;
            let message_id = cursor.read_array()?;
            let envelope_digest = EnvelopeDigest::from_bytes(cursor.read_array()?);
            let canonical_envelope = cursor.read_len_prefixed(MAX_CANONICAL_ENVELOPE_LEN)?;
            PrivateRequest::AppendOneSynced(AppendOneSyncedRequest::new(
                binding,
                attempt_id,
                location,
                message_id,
                envelope_digest,
                canonical_envelope,
            )?)
        }
        PrivateCommand::CheckedPoll => {
            let binding = decode_session(&mut cursor)?;
            let location = decode_location(&mut cursor)?;
            let expected_offset = cursor.read_u64()?;
            validate_count(cursor.read_u8()?, 1)?;
            PrivateRequest::CheckedPoll(CheckedPollRequest::new(binding, location, expected_offset))
        }
    };
    cursor.finish()?;
    Ok(request)
}

fn decode_response_body(
    command: PrivateCommand,
    body: &[u8],
) -> Result<PrivateResponse, PrivateProtocolError> {
    let mut cursor = Cursor::new(body);
    let response = match command {
        PrivateCommand::CapabilityBind => {
            let report = decode_capability_report(&mut cursor)?;
            let session_id = cursor.read_array()?;
            let fingerprint = SessionFingerprint::from_bytes(cursor.read_array()?);
            let expiry = cursor.read_u64()?;
            PrivateResponse::CapabilityBind(CapabilityBindResponse::new(
                report,
                session_id,
                fingerprint,
                expiry,
            )?)
        }
        PrivateCommand::LeaseRenew => {
            let value = LeaseRenewResponse::new(decode_session(&mut cursor)?, cursor.read_u64()?)?;
            PrivateResponse::LeaseRenew(value)
        }
        PrivateCommand::AppendOneSynced => {
            let binding = decode_session(&mut cursor)?;
            let attempt_id = cursor.read_array()?;
            decode_strong_boundary(cursor.read_u8()?)?;
            let location = decode_location(&mut cursor)?;
            validate_count(cursor.read_u8()?, 1)?;
            let assigned_offset = cursor.read_u64()?;
            let assigned_index = cursor.read_u64()?;
            let message_id = cursor.read_array()?;
            let envelope_digest = EnvelopeDigest::from_bytes(cursor.read_array()?);
            PrivateResponse::AppendOneSynced(AppendOneSyncedResponse::new(
                binding,
                attempt_id,
                location,
                assigned_offset,
                assigned_index,
                message_id,
                envelope_digest,
            )?)
        }
        PrivateCommand::CheckedPoll => {
            let binding = decode_session(&mut cursor)?;
            let location = decode_location(&mut cursor)?;
            let expected_offset = cursor.read_u64()?;
            let end_exclusive = cursor.read_u64()?;
            let oldest_available = cursor.read_u64()?;
            let count = cursor.read_u8()?;
            let record = match count {
                0 => None,
                1 => {
                    let offset = cursor.read_u64()?;
                    let message_id = cursor.read_array()?;
                    let envelope_digest = EnvelopeDigest::from_bytes(cursor.read_array()?);
                    let canonical = cursor.read_len_prefixed(MAX_CANONICAL_ENVELOPE_LEN)?;
                    Some(
                        CheckedPollRecord::try_new(offset, message_id, envelope_digest, canonical)
                            .map_err(|_| PrivateProtocolError::InvalidPollRecord)?,
                    )
                }
                actual => {
                    return Err(PrivateProtocolError::InvalidCount {
                        expected_maximum: 1,
                        actual,
                    });
                }
            };
            PrivateResponse::CheckedPoll(CheckedPollResponse::new(
                binding,
                location,
                expected_offset,
                end_exclusive,
                oldest_available,
                record,
            )?)
        }
    };
    cursor.finish()?;
    Ok(response)
}

fn encode_session(body: &mut BytesMut, binding: SessionBinding) {
    body.put_slice(&binding.session_id);
    body.put_slice(&binding.boot_id);
    body.put_slice(binding.fingerprint.as_bytes());
}

fn decode_session(cursor: &mut Cursor<'_>) -> Result<SessionBinding, PrivateProtocolError> {
    Ok(SessionBinding::new(
        cursor.read_array()?,
        cursor.read_array()?,
        SessionFingerprint::from_bytes(cursor.read_array()?),
    ))
}

fn encode_location(body: &mut BytesMut, location: ResourceLocation) {
    body.put_slice(location.resource_id.as_bytes());
    body.put_u64_le(location.resource_epoch);
    body.put_u32_le(location.stream_id);
    body.put_u32_le(location.topic_id);
    body.put_u32_le(location.partition_id);
}

fn decode_location(cursor: &mut Cursor<'_>) -> Result<ResourceLocation, PrivateProtocolError> {
    let resource_id = ResourceId::from_bytes(cursor.read_array()?);
    ResourceLocation::new(
        resource_id,
        cursor.read_u64()?,
        cursor.read_u32()?,
        cursor.read_u32()?,
        cursor.read_u32()?,
    )
}

fn encode_capability_report(body: &mut BytesMut, report: CapabilityReport) {
    body.put_slice(&report.build_sha);
    body.put_slice(&report.boot_id);
    encode_location(body, report.location);
    body.put_u64_le(report.retention_bytes);
    body.put_u64_le(report.retention_messages);
    body.put_u8(report.checksum_mode as u8);
    body.put_slice(&report.configuration_digest);
    body.put_slice(&report.security_digest);
    body.put_slice(&report.capability_digest);
}

fn decode_capability_report(
    cursor: &mut Cursor<'_>,
) -> Result<CapabilityReport, PrivateProtocolError> {
    let build_sha = cursor.read_array()?;
    let boot_id = cursor.read_array()?;
    let location = decode_location(cursor)?;
    let retention_bytes = cursor.read_u64()?;
    let retention_messages = cursor.read_u64()?;
    let checksum_mode = match cursor.read_u8()? {
        0 => ChecksumMode::Disabled,
        1 => ChecksumMode::Enabled,
        actual => return Err(PrivateProtocolError::InvalidChecksumMode(actual)),
    };
    Ok(CapabilityReport::new(
        build_sha,
        boot_id,
        location,
        retention_bytes,
        retention_messages,
        checksum_mode,
        cursor.read_array()?,
        cursor.read_array()?,
        cursor.read_array()?,
    ))
}

fn put_len_prefixed(body: &mut BytesMut, bytes: &[u8]) -> Result<(), PrivateProtocolError> {
    if bytes.len() > MAX_CANONICAL_ENVELOPE_LEN {
        return Err(PrivateProtocolError::EnvelopeTooLarge {
            actual: bytes.len(),
            maximum: MAX_CANONICAL_ENVELOPE_LEN,
        });
    }
    let len = u32::try_from(bytes.len()).map_err(|_| PrivateProtocolError::EnvelopeTooLarge {
        actual: bytes.len(),
        maximum: MAX_CANONICAL_ENVELOPE_LEN,
    })?;
    body.put_u32_le(len);
    body.put_slice(bytes);
    Ok(())
}

fn validate_complete_frame_bound(bytes: &[u8]) -> Result<(), PrivateProtocolError> {
    if bytes.len() > MAX_PRIVATE_FRAME_LEN {
        return Err(PrivateProtocolError::FrameTooLarge {
            actual: bytes.len(),
            maximum: MAX_PRIVATE_FRAME_LEN,
        });
    }
    Ok(())
}

fn validate_lease_duration(value: u32) -> Result<(), PrivateProtocolError> {
    if value == 0 {
        return Err(PrivateProtocolError::ZeroLeaseDuration);
    }
    Ok(())
}

fn validate_expiry(value: u64) -> Result<(), PrivateProtocolError> {
    if value == 0 {
        return Err(PrivateProtocolError::ZeroLeaseExpiry);
    }
    Ok(())
}

fn validate_canonical_envelope(bytes: &[u8]) -> Result<(), PrivateProtocolError> {
    if bytes.is_empty() {
        return Err(PrivateProtocolError::EmptyEnvelope);
    }
    if bytes.len() > MAX_CANONICAL_ENVELOPE_LEN {
        return Err(PrivateProtocolError::EnvelopeTooLarge {
            actual: bytes.len(),
            maximum: MAX_CANONICAL_ENVELOPE_LEN,
        });
    }
    Ok(())
}

fn validate_envelope_binding(
    bytes: &[u8],
    message_id: &[u8; 16],
    envelope_digest: EnvelopeDigest,
    partition: u32,
) -> Result<(), PrivateProtocolError> {
    validate_canonical_envelope(bytes)?;
    let envelope =
        decode_envelope(bytes).map_err(|_| PrivateProtocolError::InvalidCanonicalEnvelope)?;
    if envelope.message_id_bytes() != message_id
        || envelope.envelope_digest() != envelope_digest
        || envelope.partition() != partition
    {
        return Err(PrivateProtocolError::EnvelopeBindingMismatch);
    }
    Ok(())
}

fn validate_uuid_v4(bytes: &[u8; 16], field: &'static str) -> Result<(), PrivateProtocolError> {
    if bytes[6] & 0xf0 != 0x40 || bytes[8] & 0xc0 != 0x80 {
        return Err(PrivateProtocolError::InvalidUuidV4(field));
    }
    Ok(())
}

fn decode_strong_boundary(value: u8) -> Result<StrongConfirmationBoundary, PrivateProtocolError> {
    match value {
        2 => Ok(StrongConfirmationBoundary::OsSyncedAccepted),
        actual => Err(PrivateProtocolError::InvalidBoundary(actual)),
    }
}

fn validate_count(actual: u8, expected: u8) -> Result<(), PrivateProtocolError> {
    if actual != expected {
        return Err(PrivateProtocolError::InvalidCount {
            expected_maximum: expected,
            actual,
        });
    }
    Ok(())
}

fn ensure_equal<T: PartialEq>(
    actual: T,
    expected: T,
    field: &'static str,
) -> Result<(), PrivateProtocolError> {
    if actual != expected {
        return Err(PrivateProtocolError::ResponseBindingMismatch(field));
    }
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn read_u8(&mut self) -> Result<u8, PrivateProtocolError> {
        Ok(self.read_array::<1>()?[0])
    }

    fn read_u16(&mut self) -> Result<u16, PrivateProtocolError> {
        Ok(u16::from_le_bytes(self.read_array()?))
    }

    fn read_u32(&mut self) -> Result<u32, PrivateProtocolError> {
        Ok(u32::from_le_bytes(self.read_array()?))
    }

    fn read_u64(&mut self) -> Result<u64, PrivateProtocolError> {
        Ok(u64::from_le_bytes(self.read_array()?))
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], PrivateProtocolError> {
        let end = self
            .position
            .checked_add(N)
            .ok_or(PrivateProtocolError::Truncated)?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or(PrivateProtocolError::Truncated)?;
        self.position = end;
        bytes
            .try_into()
            .map_err(|_| PrivateProtocolError::Truncated)
    }

    fn read_len_prefixed(&mut self, maximum: usize) -> Result<Vec<u8>, PrivateProtocolError> {
        let length = self.read_u32()? as usize;
        if length > maximum {
            return Err(PrivateProtocolError::EnvelopeTooLarge {
                actual: length,
                maximum,
            });
        }
        let end = self
            .position
            .checked_add(length)
            .ok_or(PrivateProtocolError::Truncated)?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or(PrivateProtocolError::Truncated)?;
        self.position = end;
        Ok(bytes.to_vec())
    }

    fn finish(&self) -> Result<(), PrivateProtocolError> {
        if self.position != self.bytes.len() {
            return Err(PrivateProtocolError::TrailingBytes);
        }
        Ok(())
    }
}

/// A fail-closed private-extension codec or response-binding failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PrivateProtocolError {
    /// The complete outer frame exceeds the allocation guard.
    #[error("private frame length {actual} exceeds maximum {maximum}")]
    FrameTooLarge { actual: usize, maximum: usize },
    /// The declared or encoded private body exceeds its bound.
    #[error("private body length {actual} exceeds maximum {maximum}")]
    BodyTooLarge { actual: usize, maximum: usize },
    /// A canonical envelope exceeds the envelope codec bound.
    #[error("canonical envelope length {actual} exceeds maximum {maximum}")]
    EnvelopeTooLarge { actual: usize, maximum: usize },
    /// Official Iggy framing was malformed.
    #[error("official Iggy frame is malformed")]
    OuterFraming,
    /// The private payload is truncated.
    #[error("private payload is truncated")]
    Truncated,
    /// Exact decoding never permits trailing bytes.
    #[error("private frame contains trailing bytes")]
    TrailingBytes,
    /// The private magic does not identify Chirps v0.7.
    #[error("private extension magic mismatch")]
    InvalidMagic,
    /// This binary does not implement the received private version.
    #[error("unsupported private extension version {0}")]
    UnsupportedVersion(u16),
    /// The command code is outside the frozen private registry.
    #[error("unknown private command code {0}")]
    UnknownCommand(u32),
    /// Outer and inner request command identities differ.
    #[error("private command identity mismatch")]
    CommandMismatch,
    /// The request/response direction tag is wrong.
    #[error("invalid private direction tag {0}")]
    WrongDirection(u8),
    /// The command-specific permission tag is wrong.
    #[error("private command permission mismatch")]
    PermissionMismatch,
    /// The peer does not implement the frozen schema projection.
    #[error("private extension schema digest mismatch")]
    SchemaDigestMismatch,
    /// Payload bytes do not match the encoded integrity digest.
    #[error("private frame digest mismatch")]
    FrameDigestMismatch,
    /// A non-success official response cannot be decoded as success.
    #[error("private command returned remote status {0}")]
    RemoteStatus(u32),
    /// Zero cannot represent a requested active lease duration.
    #[error("lease duration must be non-zero")]
    ZeroLeaseDuration,
    /// Zero cannot represent an active server-monotonic expiry.
    #[error("lease expiry must be non-zero")]
    ZeroLeaseExpiry,
    /// AppendOneSynced always carries the strong boundary tag.
    #[error("invalid strong confirmation boundary {0}")]
    InvalidBoundary(u8),
    /// Append and poll command counts are fixed at one.
    #[error("private record count {actual} does not match expected maximum {expected_maximum}")]
    InvalidCount { expected_maximum: u8, actual: u8 },
    /// The capability checksum tag is unknown.
    #[error("invalid checksum mode {0}")]
    InvalidChecksumMode(u8),
    /// An identifier required to be generated as UUIDv4 is malformed.
    #[error("{0} is not UUIDv4")]
    InvalidUuidV4(&'static str),
    /// A strong append cannot carry an empty envelope.
    #[error("canonical envelope is empty")]
    EmptyEnvelope,
    /// The canonical envelope codec rejected the received bytes.
    #[error("canonical envelope failed validation")]
    InvalidCanonicalEnvelope,
    /// Envelope identity/digest/partition differs from the private command.
    #[error("canonical envelope does not match private command binding")]
    EnvelopeBindingMismatch,
    /// A checked poll record failed provider-neutral identity/shape checks.
    #[error("checked poll record is invalid")]
    InvalidPollRecord,
    /// Atomic poll bounds or record position are inconsistent.
    #[error("checked poll observation is invalid")]
    InvalidPollObservation,
    /// A decoded response does not echo the exact request binding.
    #[error("private response does not match request field {0}")]
    ResponseBindingMismatch(&'static str),
}

#[cfg(test)]
mod tests {
    use super::{
        APPEND_ONE_SYNCED_CODE, AppendOneSyncedRequest, AppendOneSyncedResponse,
        CAPABILITY_BIND_CODE, CHECKED_POLL_CODE, CLIENT_GOLDEN_VECTORS, CapabilityBindRequest,
        CapabilityBindResponse, CapabilityReport, CheckedPollRequest, CheckedPollResponse,
        ChecksumMode, ClientGoldenVector, EXTENSION_SCHEMA_DESCRIPTOR, EXTENSION_SCHEMA_DIGEST,
        FRAME_DIGEST_LEN, INNER_HEADER_LEN, LEASE_RENEW_CODE, LeaseRenewRequest,
        LeaseRenewResponse, MAX_PRIVATE_BODY_LEN, OUTER_FRAME_LEN, PRIVATE_EXTENSION_VERSION,
        PrivateCommand, PrivateProtocolError, PrivateRequest, PrivateResponse, ResourceLocation,
        SessionBinding, VerifiedPrivateResponse, private_frame_digest,
    };
    use alopex_chirps_core::durable::{
        CheckedPollRecord, EnvelopeDigest, ResourceId, SessionFingerprint,
    };
    use iggy_binary_protocol::codes::command_name;
    use sha2::{Digest, Sha256};

    const GOLDEN_MESSAGE_ID: [u8; 16] = [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x46, 0x07, 0x88, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f,
    ];
    const GOLDEN_ENVELOPE_DIGEST: [u8; 32] = [
        0xb5, 0x52, 0xf9, 0x14, 0x62, 0x9a, 0xa6, 0x93, 0xdf, 0x5d, 0x35, 0x6f, 0x66, 0x54, 0xff,
        0x3c, 0xc6, 0x37, 0xe7, 0xd1, 0x9e, 0xd0, 0xad, 0xc3, 0xca, 0xd8, 0x6b, 0x13, 0x36, 0x4f,
        0x2c, 0x17,
    ];
    const GOLDEN_ENVELOPE_HEX: &str = "0001000102030405460788090a0b0c0d0e0f111111111111111111111111111111112222222222222222222222222222222201020304050607080a0b0c0d000000036b6579000000000000000568656c6c6f2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824b552f914629aa693df5d356f6654ff3cc637e7d19ed0adc3cad86b13364f2c17";

    fn uuid_v4(byte: u8) -> [u8; 16] {
        let mut value = [byte; 16];
        value[6] = (byte & 0x0f) | 0x40;
        value[8] = (byte & 0x3f) | 0x80;
        value
    }

    fn from_hex(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| {
                let text = std::str::from_utf8(pair).unwrap();
                u8::from_str_radix(text, 16).unwrap()
            })
            .collect()
    }

    fn sample_binding() -> SessionBinding {
        SessionBinding::new(
            uuid_v4(0x31),
            uuid_v4(0x32),
            SessionFingerprint::from_bytes([0x33; 32]),
        )
    }

    fn sample_location() -> ResourceLocation {
        ResourceLocation::new(
            ResourceId::from_bytes(uuid_v4(0x41)),
            0x0102_0304_0506_0708,
            0x1112_1314,
            0x2122_2324,
            0x0a0b_0c0d,
        )
        .unwrap()
    }

    fn sample_pairs() -> Vec<(PrivateRequest, PrivateResponse)> {
        let binding = sample_binding();
        let location = sample_location();
        let envelope_digest = EnvelopeDigest::from_bytes(GOLDEN_ENVELOPE_DIGEST);
        let envelope = from_hex(GOLDEN_ENVELOPE_HEX);
        let bind_request = CapabilityBindRequest::new(
            location.stream_id(),
            location.topic_id(),
            location.partition_id(),
            30_000,
        )
        .unwrap();
        let report = CapabilityReport::new(
            [0x51; 20],
            *binding.boot_id(),
            location,
            1_048_576,
            1_024,
            ChecksumMode::Enabled,
            [0x52; 32],
            [0x53; 32],
            [0x54; 32],
        );
        let bind_response = CapabilityBindResponse::new(
            report,
            *binding.session_id(),
            binding.fingerprint(),
            90_000,
        )
        .unwrap();
        let lease_request = LeaseRenewRequest::new(binding, 30_000).unwrap();
        let lease_response = LeaseRenewResponse::new(binding, 120_000).unwrap();
        let attempt_id = uuid_v4(0x61);
        let append_request = AppendOneSyncedRequest::new(
            binding,
            attempt_id,
            location,
            GOLDEN_MESSAGE_ID,
            envelope_digest,
            envelope.clone(),
        )
        .unwrap();
        let append_response = AppendOneSyncedResponse::new(
            binding,
            attempt_id,
            location,
            91,
            92,
            GOLDEN_MESSAGE_ID,
            envelope_digest,
        )
        .unwrap();
        let poll_request = CheckedPollRequest::new(binding, location, 91);
        let record =
            CheckedPollRecord::try_new(91, GOLDEN_MESSAGE_ID, envelope_digest, envelope).unwrap();
        let poll_response =
            CheckedPollResponse::new(binding, location, 91, 92, 7, Some(record)).unwrap();
        vec![
            (
                PrivateRequest::CapabilityBind(bind_request),
                PrivateResponse::CapabilityBind(bind_response),
            ),
            (
                PrivateRequest::LeaseRenew(lease_request),
                PrivateResponse::LeaseRenew(lease_response),
            ),
            (
                PrivateRequest::AppendOneSynced(append_request),
                PrivateResponse::AppendOneSynced(append_response),
            ),
            (
                PrivateRequest::CheckedPoll(poll_request),
                PrivateResponse::CheckedPoll(poll_response),
            ),
        ]
    }

    fn actual_golden_vectors() -> Vec<ClientGoldenVector> {
        let mut vectors = Vec::with_capacity(8);
        for (request, response) in sample_pairs() {
            let request_bytes = request.encode().unwrap();
            let response_bytes = response.encode().unwrap();
            let request_name = match request.command() {
                PrivateCommand::CapabilityBind => "capability-bind-request",
                PrivateCommand::LeaseRenew => "lease-renew-request",
                PrivateCommand::AppendOneSynced => "append-one-synced-request",
                PrivateCommand::CheckedPoll => "checked-poll-request",
            };
            let response_name = match response.command() {
                PrivateCommand::CapabilityBind => "capability-bind-response",
                PrivateCommand::LeaseRenew => "lease-renew-response",
                PrivateCommand::AppendOneSynced => "append-one-synced-response",
                PrivateCommand::CheckedPoll => "checked-poll-response",
            };
            vectors.push(ClientGoldenVector {
                name: request_name,
                command: request.command(),
                is_response: false,
                frame_len: request_bytes.len(),
                frame_sha256: Sha256::digest(&request_bytes).into(),
            });
            vectors.push(ClientGoldenVector {
                name: response_name,
                command: response.command(),
                is_response: true,
                frame_len: response_bytes.len(),
                frame_sha256: Sha256::digest(&response_bytes).into(),
            });
        }
        vectors
    }

    fn resign(frame: &mut [u8]) {
        let digest_start = frame.len() - FRAME_DIGEST_LEN;
        let digest = private_frame_digest(&frame[OUTER_FRAME_LEN..digest_start]);
        frame[digest_start..].copy_from_slice(&digest);
    }

    #[test]
    fn v07_task_3_4_private_command_registry_is_versioned_and_separate() {
        assert_eq!(PRIVATE_EXTENSION_VERSION, 1);
        assert_eq!(PrivateCommand::CapabilityBind.code(), CAPABILITY_BIND_CODE);
        assert_eq!(PrivateCommand::LeaseRenew.code(), LEASE_RENEW_CODE);
        assert_eq!(
            PrivateCommand::AppendOneSynced.code(),
            APPEND_ONE_SYNCED_CODE
        );
        assert_eq!(PrivateCommand::CheckedPoll.code(), CHECKED_POLL_CODE);
        for code in [
            CAPABILITY_BIND_CODE,
            LEASE_RENEW_CODE,
            APPEND_ONE_SYNCED_CODE,
            CHECKED_POLL_CODE,
        ] {
            assert!(
                command_name(code).is_err(),
                "private code must not be official"
            );
        }
        let digest: [u8; 32] = Sha256::digest(EXTENSION_SCHEMA_DESCRIPTOR).into();
        assert_eq!(digest, EXTENSION_SCHEMA_DIGEST);
    }

    #[test]
    fn v07_task_3_4_all_requests_and_responses_round_trip_and_bind() {
        let binding = sample_binding();
        let location = sample_location();
        for (index, (request, response)) in sample_pairs().into_iter().enumerate() {
            let request_bytes = request.encode().unwrap();
            let response_bytes = response.encode().unwrap();
            assert_eq!(PrivateRequest::decode(&request_bytes), Ok(request.clone()));
            assert_eq!(
                PrivateResponse::decode(&response_bytes),
                Ok(response.clone())
            );
            let verified = PrivateResponse::decode_for_request(&response_bytes, &request).unwrap();
            assert_eq!(verified.command(), request.command());
            match (index, verified) {
                (0, VerifiedPrivateResponse::CapabilityBind(value)) => {
                    assert_eq!(value.report().location(), location);
                    assert_eq!(value.binding(), binding);
                    assert_eq!(value.expires_at_monotonic_millis(), 90_000);
                }
                (1, VerifiedPrivateResponse::LeaseRenew(value)) => {
                    assert_eq!(value.binding(), binding);
                    assert_eq!(value.expires_at_monotonic_millis(), 120_000);
                }
                (2, VerifiedPrivateResponse::AppendOneSynced(value)) => {
                    assert_eq!(value.location(), location);
                    assert_eq!(value.assigned_offset(), 91);
                    assert_eq!(value.assigned_index(), 92);
                }
                (3, VerifiedPrivateResponse::CheckedPoll(value)) => {
                    assert_eq!(value.observation().end_exclusive(), 92);
                    assert_eq!(value.observation().oldest_available(), 7);
                }
                _ => panic!("verified response variant must match its request"),
            }
        }
    }

    #[test]
    fn v07_task_3_4_checked_poll_empty_partition_is_one_atomic_empty_observation() {
        let binding = sample_binding();
        let location = sample_location();
        let request = PrivateRequest::CheckedPoll(CheckedPollRequest::new(binding, location, 41));
        let response = PrivateResponse::CheckedPoll(
            CheckedPollResponse::new(binding, location, 41, 41, 41, None).unwrap(),
        );
        let decoded = PrivateResponse::decode_for_request(&response.encode().unwrap(), &request)
            .expect("correlated response must verify");
        let VerifiedPrivateResponse::CheckedPoll(decoded) = decoded else {
            panic!("checked-poll command must retain its verified response type");
        };
        let observation = decoded.into_observation();
        assert_eq!(observation.end_exclusive(), 41);
        assert_eq!(observation.oldest_available(), 41);
        assert!(observation.record().is_none());
        assert!(matches!(
            CheckedPollResponse::new(binding, location, 41, 40, 41, None),
            Err(PrivateProtocolError::InvalidPollObservation)
        ));
    }

    #[test]
    fn v07_task_3_4_client_golden_vector_manifest_is_immutable() {
        assert_eq!(actual_golden_vectors(), CLIENT_GOLDEN_VECTORS);
    }

    #[test]
    fn v07_task_3_4_unknown_version_code_permission_schema_and_corruption_fail_closed() {
        let (request, _) = sample_pairs().remove(0);
        let encoded = request.encode().unwrap();

        let mut unknown_outer = encoded.to_vec();
        unknown_outer[4..8].copy_from_slice(&0xdead_beef_u32.to_le_bytes());
        assert_eq!(
            PrivateRequest::decode(&unknown_outer),
            Err(PrivateProtocolError::UnknownCommand(0xdead_beef))
        );

        let mut unsupported_version = encoded.to_vec();
        unsupported_version[OUTER_FRAME_LEN + 8..OUTER_FRAME_LEN + 10]
            .copy_from_slice(&2_u16.to_le_bytes());
        assert_eq!(
            PrivateRequest::decode(&unsupported_version),
            Err(PrivateProtocolError::UnsupportedVersion(2))
        );

        let mut unknown_inner = encoded.to_vec();
        unknown_inner[OUTER_FRAME_LEN + 10..OUTER_FRAME_LEN + 14]
            .copy_from_slice(&0xdead_beef_u32.to_le_bytes());
        assert_eq!(
            PrivateRequest::decode(&unknown_inner),
            Err(PrivateProtocolError::UnknownCommand(0xdead_beef))
        );

        let mut wrong_permission = encoded.to_vec();
        wrong_permission[OUTER_FRAME_LEN + 15] ^= 1;
        assert_eq!(
            PrivateRequest::decode(&wrong_permission),
            Err(PrivateProtocolError::PermissionMismatch)
        );

        let mut wrong_schema = encoded.to_vec();
        wrong_schema[OUTER_FRAME_LEN + 20] ^= 1;
        assert_eq!(
            PrivateRequest::decode(&wrong_schema),
            Err(PrivateProtocolError::SchemaDigestMismatch)
        );

        let mut corrupted = encoded.to_vec();
        corrupted[OUTER_FRAME_LEN + INNER_HEADER_LEN] ^= 1;
        assert_eq!(
            PrivateRequest::decode(&corrupted),
            Err(PrivateProtocolError::FrameDigestMismatch)
        );
    }

    #[test]
    fn v07_task_3_4_oversize_truncation_trailing_boundary_and_count_fail_closed() {
        for (request, response) in sample_pairs() {
            let request_bytes = request.encode().unwrap();
            for cut in 0..request_bytes.len() {
                assert!(PrivateRequest::decode(&request_bytes[..cut]).is_err());
            }
            let response_bytes = response.encode().unwrap();
            for cut in 0..response_bytes.len() {
                assert!(PrivateResponse::decode(&response_bytes[..cut]).is_err());
            }
        }

        let (request, response) = sample_pairs().remove(0);
        let mut trailing_request = request.encode().unwrap().to_vec();
        trailing_request.push(0);
        assert_eq!(
            PrivateRequest::decode(&trailing_request),
            Err(PrivateProtocolError::TrailingBytes)
        );
        let mut trailing_response = response.encode().unwrap().to_vec();
        trailing_response.push(0);
        assert_eq!(
            PrivateResponse::decode(&trailing_response),
            Err(PrivateProtocolError::TrailingBytes)
        );

        let mut declared_oversize = request.encode().unwrap().to_vec();
        declared_oversize[OUTER_FRAME_LEN + 16..OUTER_FRAME_LEN + 20]
            .copy_from_slice(&((MAX_PRIVATE_BODY_LEN as u32) + 1).to_le_bytes());
        assert!(matches!(
            PrivateRequest::decode(&declared_oversize),
            Err(PrivateProtocolError::BodyTooLarge { .. })
        ));

        let (append, _) = sample_pairs().remove(2);
        let mut weak_boundary = append.encode().unwrap().to_vec();
        weak_boundary[OUTER_FRAME_LEN + INNER_HEADER_LEN + 64 + 16] = 1;
        resign(&mut weak_boundary);
        assert_eq!(
            PrivateRequest::decode(&weak_boundary),
            Err(PrivateProtocolError::InvalidBoundary(1))
        );

        let mut append_count = append.encode().unwrap().to_vec();
        append_count[OUTER_FRAME_LEN + INNER_HEADER_LEN + 64 + 16 + 1 + 36] = 2;
        resign(&mut append_count);
        assert_eq!(
            PrivateRequest::decode(&append_count),
            Err(PrivateProtocolError::InvalidCount {
                expected_maximum: 1,
                actual: 2,
            })
        );

        let (poll, poll_response) = sample_pairs().remove(3);
        let mut request_count = poll.encode().unwrap().to_vec();
        request_count[OUTER_FRAME_LEN + INNER_HEADER_LEN + 64 + 36 + 8] = 2;
        resign(&mut request_count);
        assert_eq!(
            PrivateRequest::decode(&request_count),
            Err(PrivateProtocolError::InvalidCount {
                expected_maximum: 1,
                actual: 2,
            })
        );
        let mut response_count = poll_response.encode().unwrap().to_vec();
        response_count[OUTER_FRAME_LEN + INNER_HEADER_LEN + 64 + 36 + 8 + 8 + 8] = 2;
        resign(&mut response_count);
        assert_eq!(
            PrivateResponse::decode(&response_count),
            Err(PrivateProtocolError::InvalidCount {
                expected_maximum: 1,
                actual: 2,
            })
        );
    }

    #[test]
    fn v07_task_3_4_substituted_response_bindings_fail_before_receipt_or_delivery() {
        let pairs = sample_pairs();
        for (index, (request, response)) in pairs.iter().enumerate() {
            let mut substituted = response.clone();
            match &mut substituted {
                PrivateResponse::CapabilityBind(value) => value.report.location.topic_id ^= 1,
                PrivateResponse::LeaseRenew(value) => value.binding.session_id[0] ^= 1,
                PrivateResponse::AppendOneSynced(value) => value.attempt_id[0] ^= 1,
                PrivateResponse::CheckedPoll(value) => value.expected_offset ^= 1,
            }
            let substituted = PrivateResponse::decode(&substituted.encode().unwrap()).unwrap();
            assert!(
                matches!(
                    substituted.verify_request(request),
                    Err(PrivateProtocolError::ResponseBindingMismatch(_))
                ),
                "pair {index} must reject substitution"
            );
        }
        assert_eq!(
            pairs[0].1.clone().verify_request(&pairs[1].0),
            Err(PrivateProtocolError::CommandMismatch)
        );
    }

    #[test]
    fn v07_task_3_4_invalid_envelope_attempt_poll_and_remote_status_fail_closed() {
        let binding = sample_binding();
        let location = sample_location();
        let digest = EnvelopeDigest::from_bytes(GOLDEN_ENVELOPE_DIGEST);
        assert_eq!(
            ResourceLocation::new(ResourceId::from_bytes([0; 16]), 1, 2, 3, 4),
            Err(PrivateProtocolError::InvalidUuidV4("resource ID"))
        );
        assert_eq!(
            AppendOneSyncedRequest::new(
                binding,
                [0; 16],
                location,
                GOLDEN_MESSAGE_ID,
                digest,
                from_hex(GOLDEN_ENVELOPE_HEX),
            ),
            Err(PrivateProtocolError::InvalidUuidV4("attempt ID"))
        );
        assert_eq!(
            AppendOneSyncedRequest::new(
                binding,
                uuid_v4(7),
                location,
                GOLDEN_MESSAGE_ID,
                digest,
                vec![1],
            ),
            Err(PrivateProtocolError::InvalidCanonicalEnvelope)
        );
        assert_eq!(
            AppendOneSyncedRequest::new(
                binding,
                uuid_v4(7),
                location,
                uuid_v4(8),
                digest,
                from_hex(GOLDEN_ENVELOPE_HEX),
            ),
            Err(PrivateProtocolError::EnvelopeBindingMismatch)
        );
        assert_eq!(
            AppendOneSyncedResponse::new(
                binding,
                [0; 16],
                location,
                1,
                2,
                GOLDEN_MESSAGE_ID,
                digest,
            ),
            Err(PrivateProtocolError::InvalidUuidV4("attempt ID"))
        );
        assert_eq!(
            AppendOneSyncedResponse::new(binding, uuid_v4(7), location, 1, 2, [0; 16], digest,),
            Err(PrivateProtocolError::InvalidUuidV4("message ID"))
        );

        let invalid_poll_record =
            CheckedPollRecord::try_new(91, uuid_v4(9), digest, vec![1]).unwrap();
        assert!(
            CheckedPollResponse::new(binding, location, 91, 92, 7, Some(invalid_poll_record))
                .is_ok()
        );

        let (_, response) = sample_pairs().remove(0);
        let mut remote_error = response.encode().unwrap().to_vec();
        remote_error[..4].copy_from_slice(&17_u32.to_le_bytes());
        assert_eq!(
            PrivateResponse::decode(&remote_error),
            Err(PrivateProtocolError::RemoteStatus(17))
        );
    }
}
