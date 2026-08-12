//! Optional Apache Iggy adapter for the Chirps Durable plane.
//!
//! This crate owns all Iggy-specific SDK, protocol, transport, and local-state
//! implementation. Provider-neutral contracts remain in `alopex-chirps-core`.
//! The official Iggy 0.10.0 crates are distinct from Chirps' versioned private
//! compatible-server extension.

#![forbid(unsafe_code)]

pub mod codec;
pub mod delivery;
pub mod lifecycle;
pub mod message_id;
pub mod observability;
pub mod offset_mirror;
pub mod poll;
pub mod producer;
pub mod protocol;
pub mod routing;
pub mod session;
#[allow(dead_code)] // staged local-state internals are composed by later Phase 4 tasks
pub(crate) mod state;
pub mod subscriber;
pub mod transport;
