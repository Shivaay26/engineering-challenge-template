//! Exchange-specific [`VenueCodec`](crate::codec::VenueCodec) implementations.
//!
//! Polymarket is the primary venue; Kalshi exists to prove the engine is
//! venue-agnostic (Extension 3).

pub mod kalshi;
pub mod polymarket;

pub use kalshi::KalshiCodec;
pub use polymarket::PolymarketCodec;
