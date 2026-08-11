//! Optional Apache Iggy adapter for the Chirps Durable plane.
//!
//! This crate owns all Iggy-specific SDK, protocol, transport, and local-state
//! implementation. Provider-neutral contracts remain in `alopex-chirps-core`.
//! The official Iggy 0.10.0 crates are distinct from Chirps' versioned private
//! compatible-server extension.

#![forbid(unsafe_code)]

pub mod codec;
pub mod message_id;
