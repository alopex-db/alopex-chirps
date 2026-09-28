//! Independent expectations and exact broker bytes for performance auditing.
use crate::{evidence::Arm, sha256};
use alopex_chirps_backend_iggy::codec;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExpectedMessage {
    pub sequence: u64,
    /// Captured before send; retries may have multiple concrete attempts.
    pub attempt_ids: Vec<[u8; 16]>,
    pub confirmed: bool,
    pub source: [u8; 16],
    pub target: [u8; 16],
    pub generation: u64,
    pub partition: u32,
    pub ordering_key: Vec<u8>,
    pub payload_sha256: String,
    pub payload_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BrokerMessage {
    pub id: [u8; 16],
    pub offset: u64,
    /// Actual broker payload, including the Full canonical envelope overhead.
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReadbackMetrics {
    pub unexpected_duplicates: u64,
    pub wrong_identities: u64,
    pub wrong_digests: u64,
    pub confirmed_messages_not_observed: u64,
    pub observed_broker_bytes: u64,
    pub observed_application_bytes: u64,
}

pub fn audit(
    arm: Arm,
    expected: &[ExpectedMessage],
    observed: &[BrokerMessage],
) -> Result<ReadbackMetrics> {
    let mut attempts = BTreeMap::new();
    let mut sequences = BTreeSet::new();
    for message in expected {
        anyhow::ensure!(
            sequences.insert(message.sequence),
            "duplicate expected logical sequence"
        );
        anyhow::ensure!(
            !message.attempt_ids.is_empty(),
            "expected message has no send attempts"
        );
        for id in &message.attempt_ids {
            anyhow::ensure!(
                attempts.insert(id, message).is_none(),
                "send attempt ID was reused"
            );
        }
    }
    let mut seen = BTreeSet::new();
    let mut offsets = BTreeSet::new();
    let mut metrics = ReadbackMetrics::default();
    for message in observed {
        anyhow::ensure!(
            offsets.insert(message.offset),
            "audit reader repeated a broker offset"
        );
        metrics.observed_broker_bytes = metrics
            .observed_broker_bytes
            .checked_add(message.bytes.len().try_into()?)
            .context("broker byte count overflow")?;
        let Some(expected) = attempts.get(&message.id) else {
            metrics.wrong_identities += 1;
            continue;
        };
        if !seen.insert(expected.sequence) {
            metrics.unexpected_duplicates += 1;
        }
        match arm {
            Arm::Direct => {
                metrics.observed_application_bytes += message.bytes.len() as u64;
                if message.bytes.len() as u64 != expected.payload_bytes
                    || sha256(&message.bytes) != expected.payload_sha256
                {
                    metrics.wrong_digests += 1;
                }
            }
            Arm::Full => match codec::decode(&message.bytes) {
                Ok(envelope) => {
                    metrics.observed_application_bytes += envelope.payload().len() as u64;
                    if envelope.message_id_bytes() != &message.id
                        || envelope.source().as_bytes() != &expected.source
                        || envelope.target().as_bytes() != &expected.target
                        || envelope.generation() != expected.generation
                        || envelope.partition() != expected.partition
                        || envelope.ordering_key() != expected.ordering_key
                    {
                        metrics.wrong_identities += 1;
                    }
                    if envelope.payload().len() as u64 != expected.payload_bytes
                        || sha256(envelope.payload()) != expected.payload_sha256
                    {
                        metrics.wrong_digests += 1;
                    }
                }
                Err(_) => metrics.wrong_digests += 1,
            },
        }
    }
    metrics.confirmed_messages_not_observed = expected
        .iter()
        .filter(|message| message.confirmed && !seen.contains(&message.sequence))
        .count()
        .try_into()?;
    Ok(metrics)
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    pub schema: String,
    pub arm: Arm,
    pub expected: Vec<ExpectedMessage>,
    pub observed: Vec<BrokerMessage>,
    pub metrics: ReadbackMetrics,
    pub backend_queue_after_drain: Option<u64>,
}

pub struct Ledger {
    template: ExpectedMessage,
    expected: BTreeMap<u64, ExpectedMessage>,
    first_offset: u64,
    stream: iggy::prelude::Identifier,
    topic: iggy::prelude::Identifier,
    operation_timeout: std::time::Duration,
}

impl Ledger {
    pub async fn new(
        workload: &crate::Workload,
        client: &iggy::prelude::IggyClient,
    ) -> Result<Self> {
        use iggy::prelude::*;
        let stream = Identifier::numeric(workload.stream_id)?;
        let topic = Identifier::numeric(workload.topic_id)?;
        let operation_timeout = std::time::Duration::from_millis(workload.operation_timeout_millis);
        let last = tokio::time::timeout(
            operation_timeout,
            client.poll_messages(
                &stream,
                &topic,
                Some(workload.partition_id),
                &Consumer::new(Identifier::numeric(1)?),
                &PollingStrategy::last(),
                1,
                false,
            ),
        )
        .await??;
        let first_offset = last.messages.last().map_or(Ok(0), |message| {
            message
                .header
                .offset
                .checked_add(1)
                .context("broker offset exhausted")
        })?;
        Ok(Self {
            template: ExpectedMessage {
                sequence: 0,
                attempt_ids: Vec::new(),
                confirmed: false,
                source: crate::decode_hex(&workload.source_node_id_hex, "source")?,
                target: crate::decode_hex(&workload.target_node_id_hex, "target")?,
                generation: workload.inbox_generation,
                partition: workload.partition_id,
                ordering_key: crate::decode_hex_bytes(&workload.ordering_key_hex, "ordering key")?,
                payload_sha256: workload.payload_sha256.clone(),
                payload_bytes: 0,
            },
            expected: BTreeMap::new(),
            first_offset,
            stream,
            topic,
            operation_timeout,
        })
    }

    pub fn attempt(&mut self, sequence: u64, id: [u8; 16], original_payload: &[u8]) -> Result<()> {
        anyhow::ensure!(
            sha256(original_payload) == self.template.payload_sha256,
            "expected ledger payload differs from candidate"
        );
        let expected = self.expected.entry(sequence).or_insert_with(|| {
            let mut expected = self.template.clone();
            expected.sequence = sequence;
            expected
                .ordering_key
                .extend_from_slice(&sequence.to_be_bytes());
            expected.payload_bytes = original_payload.len() as u64;
            expected
        });
        anyhow::ensure!(
            !expected.attempt_ids.contains(&id),
            "send attempt ID was reused"
        );
        expected.attempt_ids.push(id);
        Ok(())
    }

    pub fn confirm(&mut self, sequence: u64) -> Result<()> {
        self.expected
            .get_mut(&sequence)
            .context("confirmed unknown send sequence")?
            .confirmed = true;
        Ok(())
    }

    pub async fn read(
        &self,
        client: &iggy::prelude::IggyClient,
        arm: Arm,
        retain_one: bool,
    ) -> Result<Artifact> {
        use iggy::prelude::*;
        let mut observed = Vec::new();
        let mut next = self.first_offset;
        let budget = self
            .expected
            .values()
            .map(|value| value.attempt_ids.len())
            .sum::<usize>()
            .checked_add(1)
            .context("readback budget overflow")?;
        loop {
            let response = tokio::time::timeout(
                self.operation_timeout,
                client.poll_messages(
                    &self.stream,
                    &self.topic,
                    Some(self.template.partition),
                    &Consumer::new(Identifier::numeric(1)?),
                    &PollingStrategy::offset(next),
                    1024,
                    false,
                ),
            )
            .await??;
            anyhow::ensure!(
                response.count as usize == response.messages.len(),
                "broker poll count differs"
            );
            if response.messages.is_empty() {
                break;
            }
            anyhow::ensure!(
                response.partition_id == self.template.partition,
                "broker poll partition differs"
            );
            for message in response.messages {
                anyhow::ensure!(
                    message.header.offset == next,
                    "broker offsets are not contiguous"
                );
                next = next.checked_add(1).context("broker offset exhausted")?;
                observed.push(BrokerMessage {
                    id: message.header.id.to_be_bytes(),
                    offset: message.header.offset,
                    bytes: message.payload.to_vec(),
                });
                anyhow::ensure!(
                    observed.len() <= budget,
                    "broker readback exceeds expected attempts; partition is not isolated"
                );
            }
            if next > response.current_offset {
                break;
            }
        }
        let expected: Vec<_> = self.expected.values().cloned().collect();
        if retain_one {
            anyhow::ensure!(
                !observed.is_empty(),
                "lag control requires an actual broker message"
            );
            observed.pop();
        }
        let metrics = audit(arm, &expected, &observed)?;
        Ok(Artifact {
            schema: "chirps.durable-perf-readback/v1".into(),
            arm,
            expected,
            observed,
            metrics,
            backend_queue_after_drain: None,
        })
    }
}

#[cfg(test)]
pub(crate) fn synthetic_fixture(
    arm: Arm,
    control: Option<crate::evidence::SafetyControl>,
) -> Artifact {
    use crate::evidence::SafetyControl;
    use alopex_chirps::NodeId;
    use alopex_chirps_core::durable::{
        DurableMessageId, DurableMessageRoute, PrepareFailure, PreparedDurableSend,
    };
    let mut expected = Vec::new();
    let mut observed = Vec::new();
    for sequence in 0..4u64 {
        let mut item = ExpectedMessage {
            sequence,
            attempt_ids: Vec::new(),
            confirmed: true,
            source: [1; 16],
            target: [2; 16],
            generation: 1,
            partition: 0,
            ordering_key: sequence.to_be_bytes().to_vec(),
            payload_sha256: sha256(&[1]),
            payload_bytes: 1,
        };
        let count = if sequence == 0 && control == Some(SafetyControl::UnexpectedDuplicate) {
            2
        } else {
            1
        };
        for _ in 0..count {
            let id = DurableMessageId::generate().unwrap();

            let mut source = item.source;
            if sequence == 0 && control == Some(SafetyControl::WrongIdentity) {
                source[0] ^= 1;
            }
            let payload = if sequence == 0 && control == Some(SafetyControl::WrongDigest) {
                vec![2]
            } else {
                vec![1]
            };
            let (id, bytes) = match arm {
                Arm::Direct => (*id.as_bytes(), payload),
                Arm::Full => {
                    let route = DurableMessageRoute::new(
                        NodeId::from(source),
                        NodeId::from(item.target),
                        item.generation,
                        item.partition,
                        item.ordering_key.clone(),
                        1,
                    );
                    let prepared = PreparedDurableSend::prepare(route, |id| {
                        codec::encode(
                            id,
                            codec::EnvelopeFields::new(
                                NodeId::from(source),
                                NodeId::from(item.target),
                                item.generation,
                                item.partition,
                                &item.ordering_key,
                                &payload,
                            ),
                        )
                        .map_err(|_| PrepareFailure::CanonicalEncoding)
                    })
                    .unwrap();
                    (
                        *prepared.message_id().as_bytes(),
                        prepared.canonical_bytes().to_vec(),
                    )
                }
            };
            item.attempt_ids.push(id);
            observed.push(BrokerMessage {
                id,
                offset: observed.len() as u64,
                bytes,
            });
        }
        expected.push(item);
    }
    if control == Some(SafetyControl::UndrainedLag) {
        observed.pop();
    }
    let metrics = audit(arm, &expected, &observed).unwrap();
    Artifact {
        schema: "chirps.durable-perf-readback/v1".into(),
        arm,
        expected,
        observed,
        metrics,
        backend_queue_after_drain: (arm == Arm::Full).then_some(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evidence::SafetyControl;

    #[test]
    fn actual_full_envelopes_preserve_application_size_and_expose_wire_overhead() {
        for arm in [Arm::Direct, Arm::Full] {
            let artifact = synthetic_fixture(arm, None);
            assert_eq!(artifact.metrics.observed_application_bytes, 4);
            assert_eq!(artifact.metrics.unexpected_duplicates, 0);
            assert_eq!(artifact.metrics.wrong_identities, 0);
            assert_eq!(artifact.metrics.wrong_digests, 0);
            assert_eq!(artifact.metrics.confirmed_messages_not_observed, 0);
            if arm == Arm::Full {
                assert!(artifact.metrics.observed_broker_bytes > 4);
            } else {
                assert_eq!(artifact.metrics.observed_broker_bytes, 4);
            }
        }
    }

    #[test]
    fn detects_isolated_controls_from_actual_bytes_and_logical_ledger() {
        for control in [
            SafetyControl::UnexpectedDuplicate,
            SafetyControl::WrongIdentity,
            SafetyControl::WrongDigest,
            SafetyControl::UndrainedLag,
        ] {
            let artifact = synthetic_fixture(Arm::Full, Some(control));
            assert_eq!(
                artifact.metrics.unexpected_duplicates,
                u64::from(control == SafetyControl::UnexpectedDuplicate)
            );
            assert_eq!(
                artifact.metrics.wrong_identities,
                u64::from(control == SafetyControl::WrongIdentity)
            );
            assert_eq!(
                artifact.metrics.wrong_digests,
                u64::from(control == SafetyControl::WrongDigest)
            );
            assert_eq!(
                artifact.metrics.confirmed_messages_not_observed,
                u64::from(control == SafetyControl::UndrainedLag)
            );
        }
    }

    #[test]
    fn rejects_audit_reader_replays_and_expected_ledger_aliases() {
        let mut artifact = synthetic_fixture(Arm::Full, None);
        artifact.observed.push(artifact.observed[0].clone());
        assert!(audit(Arm::Full, &artifact.expected, &artifact.observed).is_err());
        artifact.observed.pop();
        artifact.expected.push(artifact.expected[0].clone());
        assert!(audit(Arm::Full, &artifact.expected, &artifact.observed).is_err());
        artifact.expected.pop();
        artifact.expected[1].attempt_ids = artifact.expected[0].attempt_ids.clone();
        assert!(audit(Arm::Full, &artifact.expected, &artifact.observed).is_err());
    }

    #[test]
    fn each_independent_expected_identity_field_is_checked() {
        let base = synthetic_fixture(Arm::Full, None);
        for field in 0..5 {
            let mut artifact = base.clone();
            let expected = &mut artifact.expected[0];
            match field {
                0 => expected.source[0] ^= 1,
                1 => expected.target[0] ^= 1,
                2 => expected.generation += 1,
                3 => expected.partition += 1,
                _ => expected.ordering_key.push(1),
            }
            assert_eq!(
                audit(Arm::Full, &artifact.expected, &artifact.observed)
                    .unwrap()
                    .wrong_identities,
                1
            );
        }
    }

    #[test]
    fn direct_digest_and_length_are_independently_checked() {
        let base = synthetic_fixture(Arm::Direct, None);
        let mut changed = base.clone();
        changed.observed[0].bytes[0] ^= 1;
        assert_eq!(
            audit(Arm::Direct, &changed.expected, &changed.observed)
                .unwrap()
                .wrong_digests,
            1
        );
        let mut changed = base;
        changed.expected[0].payload_bytes += 1;
        assert_eq!(
            audit(Arm::Direct, &changed.expected, &changed.observed)
                .unwrap()
                .wrong_digests,
            1
        );
    }

    #[test]
    fn every_truncated_envelope_is_reported_as_bad_digest() {
        let mut artifact = synthetic_fixture(Arm::Full, None);
        let bytes = artifact.observed[0].bytes.clone();
        for len in 0..bytes.len() {
            artifact.observed[0].bytes = bytes[..len].to_vec();
            assert_eq!(
                audit(Arm::Full, &artifact.expected, &artifact.observed)
                    .unwrap()
                    .wrong_digests,
                1
            );
        }
    }

    #[test]
    fn unknown_broker_identity_and_corrupt_envelope_never_pass() {
        let mut artifact = synthetic_fixture(Arm::Full, None);
        artifact.observed[0].id = [0; 16];
        assert_eq!(
            audit(Arm::Full, &artifact.expected, &artifact.observed)
                .unwrap()
                .wrong_identities,
            1
        );
        artifact.observed[0].id = artifact.expected[0].attempt_ids[0];
        let last = artifact.observed[0].bytes.len() - 1;
        artifact.observed[0].bytes[last] ^= 1;
        assert_eq!(
            audit(Arm::Full, &artifact.expected, &artifact.observed)
                .unwrap()
                .wrong_digests,
            1
        );
    }
}
