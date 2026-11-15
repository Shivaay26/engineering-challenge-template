//! Order book engine: state, the single consumer, and WebSocket workers.

pub mod consumer;
pub mod orderbook;
pub mod socket;

pub use consumer::Consumer;
pub use orderbook::OrderBook;
