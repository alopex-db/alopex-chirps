//! Versioned deterministic mapping to explicit zero-based Iggy partitions.
//!
//! Routing-map version 1 hashes only the target, inbox generation, and bounded
//! ordering key under a fixed domain. Source identity, message identity,
//! payload, attempt, and session state cannot influence the selected partition.

use crate::codec::MAX_ORDERING_KEY_LEN;
use alopex_chirps_core::durable::DurableMessageRoute;
use alopex_chirps_wire::node_id::NodeId;
use sha2::{Digest, Sha256};
use thiserror::Error;

/// The sole routing-map algorithm implemented by the v0.7 adapter.
pub const ROUTING_MAP_VERSION: u32 = 1;

/// Hash-domain separator for deterministic Durable routing.
pub const MAPPING_DOMAIN_SEPARATOR: &[u8] = b"ALOPEX-CHIRPS-DURABLE-ROUTING\0";

/// Routing metadata accepted by the adapter's durable-manifest validation
/// boundary.
///
/// The fields are crate-private: external callers cannot manufacture a second
/// configuration for an existing generation. The manifest loader owns
/// construction; [`PartitionRouter::transition`] owns live changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedRoutingConfiguration {
    pub(crate) source: NodeId,
    pub(crate) generation: u64,
    pub(crate) partition_count: u32,
    pub(crate) mapping_version: u32,
}

/// Immutable routing configuration for one inbox generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionRouter {
    source: NodeId,
    generation: u64,
    partition_count: u32,
    mapping_version: u32,
}

impl PartitionRouter {
    /// Creates the first immutable routing configuration for one explicit
    /// inbox generation. Later configuration changes must use
    /// [`Self::transition`] so they cannot reuse the current generation.
    pub(crate) fn new(
        source: NodeId,
        generation: u64,
        partition_count: u32,
    ) -> Result<Self, RoutingError> {
        Self::from_validated_configuration(ValidatedRoutingConfiguration {
            source,
            generation,
            partition_count,
            mapping_version: ROUTING_MAP_VERSION,
        })
    }

    /// Reconstructs a router only from metadata accepted by the adapter's
    /// durable-manifest validation boundary.
    pub fn from_validated_configuration(
        configuration: ValidatedRoutingConfiguration,
    ) -> Result<Self, RoutingError> {
        validate_configuration(configuration.partition_count, configuration.mapping_version)?;
        Ok(Self {
            source: configuration.source,
            generation: configuration.generation,
            partition_count: configuration.partition_count,
            mapping_version: configuration.mapping_version,
        })
    }

    /// Creates the next immutable configuration while forbidding config changes
    /// inside the current generation.
    #[allow(dead_code)] // retained for manifest transition validation
    pub fn transition(
        &self,
        proposed_generation: u64,
        proposed_partition_count: u32,
        proposed_mapping_version: u32,
    ) -> Result<Self, RoutingError> {
        validate_configuration(proposed_partition_count, proposed_mapping_version)?;
        if proposed_generation < self.generation {
            return Err(RoutingError::GenerationRegression {
                current: self.generation,
                proposed: proposed_generation,
            });
        }
        let configuration_changed = proposed_partition_count != self.partition_count
            || proposed_mapping_version != self.mapping_version;
        if configuration_changed && proposed_generation == self.generation {
            return Err(RoutingError::GenerationRequired);
        }
        Ok(Self {
            source: self.source,
            generation: proposed_generation,
            partition_count: proposed_partition_count,
            mapping_version: proposed_mapping_version,
        })
    }

    /// Returns the local source stored in every produced route.
    #[must_use]
    pub const fn source(&self) -> NodeId {
        self.source
    }

    /// Returns the immutable inbox generation.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the non-zero number of zero-based explicit partitions.
    #[must_use]
    #[allow(dead_code)] // retained for manifest transition validation
    pub const fn partition_count(&self) -> u32 {
        self.partition_count
    }

    /// Returns the fixed mapping algorithm version.
    #[must_use]
    #[allow(dead_code)] // retained for manifest transition validation
    pub const fn mapping_version(&self) -> u32 {
        self.mapping_version
    }

    /// Deterministically resolves a target and ordering key to an explicit
    /// zero-based partition.
    pub fn partition_for(&self, target: NodeId, ordering_key: &[u8]) -> Result<u32, RoutingError> {
        validate_ordering_key(ordering_key)?;
        let digest = route_hash(self.generation, target, ordering_key);
        let mut prefix = [0; 8];
        prefix.copy_from_slice(&digest[..8]);
        let hash_value = u64::from_be_bytes(prefix);
        Ok((hash_value % u64::from(self.partition_count)) as u32)
    }

    /// Freezes the resolved partition and all routing inputs in a provider-
    /// neutral prepared-route value. No balanced provider routing is exposed.
    pub fn route(
        &self,
        target: NodeId,
        ordering_key: Vec<u8>,
    ) -> Result<DurableMessageRoute, RoutingError> {
        let partition = self.partition_for(target, &ordering_key)?;
        Ok(DurableMessageRoute::new(
            self.source,
            target,
            self.generation,
            partition,
            ordering_key,
            self.mapping_version,
        ))
    }

    /// Rejects any observed partition that does not equal the canonical mapping
    /// before a caller can deliver or acknowledge the record.
    pub fn validate_partition(
        &self,
        target: NodeId,
        ordering_key: &[u8],
        actual: u32,
    ) -> Result<(), RoutingError> {
        if actual >= self.partition_count {
            return Err(RoutingError::PartitionOutOfRange {
                actual,
                partition_count: self.partition_count,
            });
        }
        let expected = self.partition_for(target, ordering_key)?;
        if actual != expected {
            return Err(RoutingError::Misrouted { expected, actual });
        }
        Ok(())
    }
}

fn validate_configuration(partition_count: u32, mapping_version: u32) -> Result<(), RoutingError> {
    if partition_count == 0 {
        return Err(RoutingError::EmptyPartitionSet);
    }
    if mapping_version != ROUTING_MAP_VERSION {
        return Err(RoutingError::UnsupportedMappingVersion(mapping_version));
    }
    Ok(())
}

fn validate_ordering_key(ordering_key: &[u8]) -> Result<(), RoutingError> {
    if ordering_key.len() > MAX_ORDERING_KEY_LEN {
        return Err(RoutingError::OrderingKeyTooLarge {
            actual: ordering_key.len(),
            maximum: MAX_ORDERING_KEY_LEN,
        });
    }
    Ok(())
}

fn route_hash(generation: u64, target: NodeId, ordering_key: &[u8]) -> [u8; 32] {
    let ordering_key_len = ordering_key.len() as u32;
    let mut hasher = Sha256::new();
    hasher.update(MAPPING_DOMAIN_SEPARATOR);
    hasher.update(ROUTING_MAP_VERSION.to_be_bytes());
    hasher.update(target.as_bytes());
    hasher.update(generation.to_be_bytes());
    hasher.update(ordering_key_len.to_be_bytes());
    hasher.update(ordering_key);
    hasher.finalize().into()
}

/// A deterministic routing or routing-config validation failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RoutingError {
    /// An explicit routing map cannot contain zero partitions.
    #[error("deterministic routing requires at least one partition")]
    EmptyPartitionSet,
    /// This binary does not implement the requested mapping algorithm.
    #[error("unsupported routing-map version {0}")]
    UnsupportedMappingVersion(u32),
    /// A configuration change attempted to reuse the current generation.
    #[error("partition-count or mapping-version change requires a new generation")]
    GenerationRequired,
    /// Inbox generations are monotonic and cannot regress.
    #[error("routing generation regressed from {current} to {proposed}")]
    GenerationRegression { current: u64, proposed: u64 },
    /// The ordering key exceeds the canonical codec/provider bound.
    #[error("ordering key length {actual} exceeds maximum {maximum}")]
    OrderingKeyTooLarge { actual: usize, maximum: usize },
    /// An observed partition is outside the configured zero-based range.
    #[error("partition {actual} is outside configured count {partition_count}")]
    PartitionOutOfRange { actual: u32, partition_count: u32 },
    /// An observed record is validly encoded but mapped to the wrong partition.
    #[error("record is misrouted: expected partition {expected}, observed {actual}")]
    Misrouted { expected: u32, actual: u32 },
}

#[cfg(test)]
mod tests {
    use super::{
        MAPPING_DOMAIN_SEPARATOR, PartitionRouter, ROUTING_MAP_VERSION, RoutingError,
        ValidatedRoutingConfiguration, route_hash,
    };
    use crate::codec::{EnvelopeFields, MAX_ORDERING_KEY_LEN, encode};
    use alopex_chirps_core::durable::{PrepareFailure, PreparedDurableSend};
    use alopex_chirps_wire::node_id::NodeId;
    use proptest::prelude::*;

    fn validated_router(
        source: NodeId,
        generation: u64,
        partition_count: u32,
        mapping_version: u32,
    ) -> Result<PartitionRouter, RoutingError> {
        PartitionRouter::from_validated_configuration(ValidatedRoutingConfiguration {
            source,
            generation,
            partition_count,
            mapping_version,
        })
    }

    #[test]
    fn v07_task_3_3_golden_mapping_is_stable_and_zero_based() {
        let target = NodeId::from([0x42; 16]);
        let router = validated_router(
            NodeId::from([0x11; 16]),
            0x0102_0304_0506_0708,
            17,
            ROUTING_MAP_VERSION,
        )
        .expect("valid router");

        assert_eq!(ROUTING_MAP_VERSION, 1);
        assert_eq!(MAPPING_DOMAIN_SEPARATOR, b"ALOPEX-CHIRPS-DURABLE-ROUTING\0");
        assert_eq!(
            route_hash(router.generation(), target, b"customer-42"),
            [
                0xab, 0xaa, 0x18, 0xe5, 0x83, 0x4e, 0xc5, 0x72, 0x6b, 0xfa, 0x04, 0x4d, 0x95, 0x4b,
                0x69, 0x00, 0xa7, 0x9a, 0xc3, 0x96, 0xfd, 0xa2, 0x08, 0x3a, 0x24, 0x8a, 0x31, 0xf8,
                0x64, 0x79, 0xa4, 0x39,
            ]
        );
        assert_eq!(
            router.partition_for(target, b"customer-42"),
            Ok(9),
            "golden partition must remain stable"
        );
    }

    #[test]
    fn v07_task_3_3_route_freezes_explicit_partition_in_prepared_request() {
        let source = NodeId::from([0x11; 16]);
        let target = NodeId::from([0x22; 16]);
        let router = validated_router(source, 9, 8, ROUTING_MAP_VERSION).expect("valid router");
        let route = router
            .route(target, b"order-7".to_vec())
            .expect("route must resolve");
        let prepared = PreparedDurableSend::prepare(route, |message_id| {
            encode(
                message_id,
                EnvelopeFields::new(
                    source,
                    target,
                    9,
                    router.partition_for(target, b"order-7").unwrap(),
                    b"order-7",
                    b"payload",
                ),
            )
            .map_err(|_| PrepareFailure::CanonicalEncoding)
        })
        .expect("prepare must freeze route");
        let retry = prepared.clone();

        assert_eq!(prepared.source(), source);
        assert_eq!(prepared.target(), target);
        assert_eq!(prepared.generation(), 9);
        assert_eq!(
            prepared.partition(),
            router.partition_for(target, b"order-7").unwrap(),
            "prepared route must freeze the canonical partition"
        );
        assert_eq!(prepared.partition(), retry.partition());
        assert_eq!(prepared.ordering_key(), retry.ordering_key());
        assert_eq!(prepared.routing_map_version(), ROUTING_MAP_VERSION);
        assert!(prepared.partition() < router.partition_count());
    }

    #[test]
    fn v07_task_3_3_partition_or_mapping_change_requires_new_generation() {
        let source = NodeId::from([0x11; 16]);
        let router = validated_router(source, 7, 4, ROUTING_MAP_VERSION).expect("valid router");

        assert!(router.transition(7, 4, ROUTING_MAP_VERSION).is_ok());
        assert_eq!(
            router.transition(7, 8, ROUTING_MAP_VERSION),
            Err(RoutingError::GenerationRequired)
        );
        assert_eq!(
            router.transition(6, 4, ROUTING_MAP_VERSION),
            Err(RoutingError::GenerationRegression {
                current: 7,
                proposed: 6,
            })
        );

        let next = router
            .transition(8, 8, ROUTING_MAP_VERSION)
            .expect("new generation may change partition count");
        assert_eq!(next.generation(), 8);
        assert_eq!(next.partition_count(), 8);
    }

    #[test]
    fn v07_task_3_3_invalid_configuration_key_and_misroute_fail_typed() {
        let source = NodeId::from([0x11; 16]);
        assert_eq!(
            validated_router(source, 1, 0, ROUTING_MAP_VERSION),
            Err(RoutingError::EmptyPartitionSet)
        );
        assert_eq!(
            validated_router(source, 1, 4, ROUTING_MAP_VERSION + 1),
            Err(RoutingError::UnsupportedMappingVersion(2))
        );

        let router = validated_router(source, 1, 4, ROUTING_MAP_VERSION).expect("valid router");
        let oversized = vec![0; MAX_ORDERING_KEY_LEN + 1];
        assert!(matches!(
            router.partition_for(NodeId::from([0x22; 16]), &oversized),
            Err(RoutingError::OrderingKeyTooLarge { .. })
        ));

        let target = NodeId::from([0x22; 16]);
        let expected = router.partition_for(target, b"key").unwrap();
        let wrong = (expected + 1) % router.partition_count();
        assert_eq!(
            router.validate_partition(target, b"key", wrong),
            Err(RoutingError::Misrouted {
                expected,
                actual: wrong,
            })
        );
        assert_eq!(
            router.validate_partition(target, b"key", router.partition_count()),
            Err(RoutingError::PartitionOutOfRange {
                actual: router.partition_count(),
                partition_count: router.partition_count(),
            })
        );
    }

    #[test]
    fn v07_task_3_3_source_message_and_payload_do_not_change_partition_mapping() {
        let target = NodeId::from([0x77; 16]);
        let left = validated_router(NodeId::from([0x11; 16]), 12, 13, ROUTING_MAP_VERSION).unwrap();
        let right =
            validated_router(NodeId::from([0x22; 16]), 12, 13, ROUTING_MAP_VERSION).unwrap();

        assert_eq!(
            left.partition_for(target, b"stable"),
            right.partition_for(target, b"stable")
        );
    }

    proptest! {
        #[test]
        fn v07_task_3_3_property_is_deterministic_and_in_range(
            target_bytes in any::<[u8; 16]>(),
            generation in any::<u64>(),
            partition_count in 1_u32..=1024,
            key in proptest::collection::vec(any::<u8>(), 0..=MAX_ORDERING_KEY_LEN),
        ) {
            let target = NodeId::from(target_bytes);
            let router = validated_router(
                NodeId::from([0x11; 16]),
                generation,
                partition_count,
                ROUTING_MAP_VERSION,
            ).unwrap();
            let first = router.partition_for(target, &key).unwrap();
            let second = router.partition_for(target, &key).unwrap();

            prop_assert_eq!(first, second);
            prop_assert!(first < partition_count);
            prop_assert_eq!(router.validate_partition(target, &key, first), Ok(()));
        }
    }
}
