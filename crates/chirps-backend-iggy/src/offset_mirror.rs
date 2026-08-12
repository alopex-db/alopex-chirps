//! Advisory-only official Iggy consumer-offset mirroring.
//!
//! Chirps' local checkpoint remains the sole replay authority. This module can
//! copy an already committed inclusive offset to one ordinary Iggy consumer
//! for observability and tooling, but its success is not a commit and its
//! failure cannot roll back or invalidate canonical checkpoint state.

use crate::protocol::ResourceLocation;
use crate::transport::{DataPlaneRequestFrame, InvocationError, OwnedTransport};
use async_trait::async_trait;
use iggy_binary_protocol::{ResponseFrame, STATUS_OK};
use std::sync::Arc;

/// Immutable ordinary-consumer and explicit resource binding for one mirror.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdvisoryOffsetBinding {
    consumer_id: u32,
    location: ResourceLocation,
}

impl AdvisoryOffsetBinding {
    /// Creates a numeric ordinary-consumer binding at one explicit partition.
    #[must_use]
    pub const fn new(consumer_id: u32, location: ResourceLocation) -> Self {
        Self {
            consumer_id,
            location,
        }
    }

    /// Returns the numeric ordinary-consumer identity (never a group).
    #[must_use]
    pub const fn consumer_id(self) -> u32 {
        self.consumer_id
    }

    /// Returns the fixed numeric stream/topic and explicit partition.
    #[must_use]
    pub const fn location(self) -> ResourceLocation {
        self.location
    }
}

/// One immutable advisory write derived from a committed Chirps checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdvisoryOffsetRequest {
    binding: AdvisoryOffsetBinding,
    committed_offset: u64,
}

impl AdvisoryOffsetRequest {
    /// Returns the ordinary-consumer and resource binding.
    #[must_use]
    pub const fn binding(self) -> AdvisoryOffsetBinding {
        self.binding
    }

    /// Returns the already committed inclusive offset being mirrored.
    #[must_use]
    pub const fn committed_offset(self) -> u64 {
        self.committed_offset
    }
}

/// Bounded outcome of the mirror port's sole official request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdvisoryOffsetPortError {
    /// Local validation or prior close proved the request was not invoked.
    NotInvoked,
    /// Invocation began but no valid success response was established.
    Indeterminate,
    /// A response arrived but was not one complete empty official success.
    InvalidResponse,
}

/// Narrow dependency-injection boundary for one advisory mirror attempt.
#[async_trait]
pub trait AdvisoryOffsetPort: Send + Sync {
    /// Stores one offset exactly once, without reconnect or automatic retry.
    async fn store_offset_once(
        &self,
        request: AdvisoryOffsetRequest,
    ) -> Result<(), AdvisoryOffsetPortError>;
}

/// Advisory evidence only; neither variant is canonical replay state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MirrorState {
    /// The broker returned one exact official success for the copied offset.
    Mirrored {
        /// Inclusive Chirps checkpoint value that was copied.
        offset: u64,
    },
    /// The copy failed; the canonical Chirps checkpoint remains unchanged.
    NotMirrored {
        /// Inclusive Chirps checkpoint value whose copy was attempted.
        offset: u64,
        /// Bounded failure from the sole port call.
        failure: AdvisoryOffsetPortError,
    },
}

/// Stateless one-call copier for an already committed checkpoint value.
#[derive(Debug)]
pub struct AdvisoryOffsetMirror<P> {
    port: Arc<P>,
    binding: AdvisoryOffsetBinding,
}

impl<P> AdvisoryOffsetMirror<P>
where
    P: AdvisoryOffsetPort,
{
    /// Fixes the port, ordinary consumer, and explicit resource location.
    #[must_use]
    pub const fn new(port: Arc<P>, binding: AdvisoryOffsetBinding) -> Self {
        Self { port, binding }
    }

    /// Copies the supplied committed offset with exactly one port call.
    pub async fn mirror_committed(&self, committed_offset: u64) -> MirrorState {
        let request = AdvisoryOffsetRequest {
            binding: self.binding,
            committed_offset,
        };
        match self.port.store_offset_once(request).await {
            Ok(()) => MirrorState::Mirrored {
                offset: committed_offset,
            },
            Err(failure) => MirrorState::NotMirrored {
                offset: committed_offset,
                failure,
            },
        }
    }
}

#[async_trait]
impl AdvisoryOffsetPort for OwnedTransport {
    async fn store_offset_once(
        &self,
        request: AdvisoryOffsetRequest,
    ) -> Result<(), AdvisoryOffsetPortError> {
        let frame = DataPlaneRequestFrame::from_standard_store_consumer_offset(
            request.binding.consumer_id,
            request.binding.location,
            request.committed_offset,
        )
        .map_err(|_| AdvisoryOffsetPortError::NotInvoked)?;
        match self.invoke(frame).await {
            Ok(bytes) if validate_empty_success(&bytes) => Ok(()),
            Ok(_) => Err(AdvisoryOffsetPortError::InvalidResponse),
            Err(InvocationError::NotInvoked(_)) => Err(AdvisoryOffsetPortError::NotInvoked),
            Err(InvocationError::Indeterminate(_)) => Err(AdvisoryOffsetPortError::Indeterminate),
        }
    }
}

fn validate_empty_success(bytes: &[u8]) -> bool {
    ResponseFrame::decode(bytes).is_ok_and(|(frame, consumed)| {
        consumed == bytes.len() && frame.status == STATUS_OK && frame.payload.is_empty()
    })
}

#[cfg(test)]
mod tests {
    use super::{
        AdvisoryOffsetBinding, AdvisoryOffsetMirror, AdvisoryOffsetPort, AdvisoryOffsetPortError,
        AdvisoryOffsetRequest, MirrorState, validate_empty_success,
    };
    use crate::protocol::ResourceLocation;
    use alopex_chirps_core::durable::ResourceId;
    use async_trait::async_trait;
    use bytes::{Bytes, BytesMut};
    use iggy_binary_protocol::ResponseFrame;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Debug)]
    struct ScriptedPort {
        calls: AtomicUsize,
        requests: Mutex<Vec<AdvisoryOffsetRequest>>,
        results: Mutex<VecDeque<Result<(), AdvisoryOffsetPortError>>>,
    }

    #[async_trait]
    impl AdvisoryOffsetPort for ScriptedPort {
        async fn store_offset_once(
            &self,
            request: AdvisoryOffsetRequest,
        ) -> Result<(), AdvisoryOffsetPortError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.requests.lock().unwrap().push(request);
            self.results
                .lock()
                .unwrap()
                .pop_front()
                .expect("scripted offset mirror result")
        }
    }

    fn location() -> ResourceLocation {
        let mut id = [0x51; 16];
        id[6] = 0x41;
        id[8] = 0x91;
        ResourceLocation::new(ResourceId::from_bytes(id), 9, 11, 22, 3).unwrap()
    }

    #[tokio::test]
    async fn v07_task_3_8_mirror_uses_one_fixed_regular_consumer_location_and_offset() {
        let port = Arc::new(ScriptedPort {
            calls: AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
            results: Mutex::new([Ok(())].into_iter().collect()),
        });
        let binding = AdvisoryOffsetBinding::new(77, location());
        let mirror = AdvisoryOffsetMirror::new(Arc::clone(&port), binding);

        assert_eq!(
            mirror.mirror_committed(123).await,
            MirrorState::Mirrored { offset: 123 }
        );
        assert_eq!(port.calls.load(Ordering::SeqCst), 1);
        let request = port.requests.lock().unwrap()[0];
        assert_eq!(request.binding(), binding);
        assert_eq!(request.binding().consumer_id(), 77);
        assert_eq!(request.binding().location(), location());
        assert_eq!(request.committed_offset(), 123);
    }

    #[tokio::test]
    async fn v07_task_3_8_mirror_failure_is_advisory_and_never_retried() {
        for failure in [
            AdvisoryOffsetPortError::NotInvoked,
            AdvisoryOffsetPortError::Indeterminate,
            AdvisoryOffsetPortError::InvalidResponse,
        ] {
            let port = Arc::new(ScriptedPort {
                calls: AtomicUsize::new(0),
                requests: Mutex::new(Vec::new()),
                results: Mutex::new([Err(failure)].into_iter().collect()),
            });
            let mirror = AdvisoryOffsetMirror::new(
                Arc::clone(&port),
                AdvisoryOffsetBinding::new(77, location()),
            );
            assert_eq!(
                mirror.mirror_committed(456).await,
                MirrorState::NotMirrored {
                    offset: 456,
                    failure,
                }
            );
            assert_eq!(port.calls.load(Ordering::SeqCst), 1);
        }
    }

    #[test]
    fn v07_task_3_8_offset_success_requires_exact_empty_complete_response() {
        let mut valid = BytesMut::new();
        ResponseFrame::encode_ok(&[], &mut valid).unwrap();
        assert!(validate_empty_success(&valid));

        let mut payload = BytesMut::new();
        ResponseFrame::encode_ok(b"unexpected", &mut payload).unwrap();
        assert!(!validate_empty_success(&payload));

        let mut trailing = valid.clone();
        trailing.extend_from_slice(b"trailing");
        assert!(!validate_empty_success(&trailing));
        assert!(!validate_empty_success(&Bytes::new()));
    }
}
