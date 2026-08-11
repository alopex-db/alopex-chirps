use alopex_chirps_core::durable::{DurableMessageId, MessageIdError};

/// Number of random bits carried by an RFC 4122 UUIDv4 identity.
pub const UUID_V4_RANDOM_BITS: u32 = 122;

/// Largest aggregate generation count covered by the documented deployment
/// collision assumption.
pub const MAX_ASSUMED_AGGREGATE_IDS: u64 = 1_u64 << 32;

/// Generates a logical Durable message identity from the platform CSPRNG.
///
/// This is the adapter's only identity-generation entry point. It delegates to
/// the provider-neutral type, which exposes no caller-selected byte or string
/// constructor. UUIDv4 supplies 122 random bits; for aggregate generation count
/// `N`, the collision upper bound is `N(N-1)/2^123`. At
/// `N <= 2^32`, that bound is below `2^-59`. This is a deployment assumption,
/// not a runtime counter or cap.
pub fn generate() -> Result<DurableMessageId, MessageIdError> {
    DurableMessageId::generate()
}

pub(crate) const fn is_uuid_v4(bytes: &[u8; 16]) -> bool {
    bytes[6] & 0xf0 == 0x40 && bytes[8] & 0xc0 == 0x80
}

#[cfg(test)]
mod tests {
    use super::{MAX_ASSUMED_AGGREGATE_IDS, UUID_V4_RANDOM_BITS, generate};
    use std::collections::HashSet;

    #[test]
    fn v07_task_3_2_generator_is_uuidv4_only_and_documents_collision_assumption() {
        let mut generated = HashSet::new();
        for _ in 0..1_024 {
            let id = generate().expect("platform CSPRNG must be available for the test");
            let bytes = id.as_bytes();
            assert_eq!(bytes[6] & 0xf0, 0x40, "UUID version must be four");
            assert_eq!(bytes[8] & 0xc0, 0x80, "UUID variant must be RFC 4122");
            assert!(generated.insert(*bytes), "generated UUIDv4 collision");
        }

        assert_eq!(UUID_V4_RANDOM_BITS, 122);
        assert_eq!(MAX_ASSUMED_AGGREGATE_IDS, 1_u64 << 32);
    }
}
