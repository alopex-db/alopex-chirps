//! Bounded receive buffers with profile-aware backpressure.

mod backpressure;
mod message_buffer;
mod priority_queue;

pub use backpressure::{BackpressureController, BackpressureLevel};
pub use message_buffer::{BufferError, BufferedMessage, MessageBuffer};
pub use priority_queue::PriorityQueue;
