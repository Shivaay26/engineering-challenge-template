//! Order book engine: state, the single consumer, sharding, and WebSocket
//! workers.

pub mod consumer;
pub mod orderbook;
pub mod shard_manager;
pub mod socket;

pub use consumer::Consumer;
pub use orderbook::OrderBook;
pub use shard_manager::ShardManager;

use crate::codec::{ConnSlot, StampedEnvelope};

/// Everything a WebSocket worker sends to the single consumer.
#[derive(Debug)]
pub enum EngineMessage {
    /// A decoded order book event from one connection.
    Event(StampedEnvelope),
    /// A connection dropped; the consumer uses this for failover arbitration.
    Disconnected {
        shard_id: usize,
        slot: ConnSlot,
        generation: u64,
    },
}
