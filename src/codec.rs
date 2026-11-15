//! Venue-agnostic domain types and the per-exchange [`VenueCodec`] boundary.
//!
//! Everything downstream of a codec (order books, redundancy, SBE output) is
//! identical for every exchange; the only thing an exchange adapter has to
//! provide is the wire encoding and decoding defined by [`VenueCodec`].
//!
//! Prices and quantities never touch floating point. Exchanges send decimal
//! strings, and [`parse_scaled_1e6`] converts them into exact `i64` values
//! scaled by $1e6$.

use std::time::Duration;

use anyhow::Context;
use chrono::{DateTime, Utc};

use crate::Side;

/// Fixed-point scale for prices and quantities: every value is an integer
/// multiple of `1e-6`.
const SCALE: i64 = 1_000_000;

/// Number of decimal digits representable at [`SCALE`].
const SCALE_DIGITS: u32 = 6;

/// Which of the two redundant connections an event arrived on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnSlot {
    /// Conn A.
    Primary,
    /// Conn B.
    Standby,
}

/// A venue-independent order book event produced by a [`VenueCodec`].
#[derive(Debug, Clone)]
pub enum NormalizedEvent {
    /// A full order book snapshot; replaces any previous state.
    Snapshot {
        symbol: String,
        exchange_ts: DateTime<Utc>,
        received_at: DateTime<Utc>,
        /// `(price_1e6, size_1e6)`, unsorted.
        bids: Vec<(i64, i64)>,
        /// `(price_1e6, size_1e6)`, unsorted.
        asks: Vec<(i64, i64)>,
    },
    /// A change to a single price level.
    Delta {
        symbol: String,
        exchange_ts: DateTime<Utc>,
        received_at: DateTime<Utc>,
        side: Side,
        price_level: i64,
        /// If true, `quantity` is the new total level size (Polymarket).
        /// If false, `quantity` is a signed relative delta to add to existing
        /// level (Kalshi).
        is_absolute: bool,
        quantity: i64,
    },
    /// A market was listed or resolved, so subscriptions must be reconciled.
    MarketLifecycle { symbol: String, is_active: bool },
}

/// A [`NormalizedEvent`] tagged with the connection that delivered it, so the
/// single consumer can attribute state to the right isolated book.
#[derive(Debug, Clone)]
pub struct StampedEnvelope {
    pub shard_id: usize,
    pub slot: ConnSlot,
    /// Increments on make-before-break connection rotation.
    pub generation: u64,
    pub event: NormalizedEvent,
}

/// The only exchange-specific layer in the collector.
///
/// Implementors translate their wire protocol into [`NormalizedEvent`]s; the
/// engine handles sharding, redundancy, failover, snapshots and SBE output for
/// every venue identically.
#[async_trait::async_trait]
pub trait VenueCodec: Send + Sync + 'static {
    /// Short venue identifier, e.g. `"kalshi"`.
    fn name(&self) -> &'static str;

    /// WebSocket endpoint to connect to.
    fn ws_url(&self) -> &str;

    /// How often to send [`Self::heartbeat_payload`], if the venue needs one.
    fn heartbeat_interval(&self) -> Option<Duration>;

    /// Frame to send on the heartbeat interval, if the venue needs one.
    fn heartbeat_payload(&self) -> Option<String>;

    /// Fetches all currently active, tradable, non-MVE symbols from REST.
    async fn fetch_active_symbols(&self, client: &reqwest::Client) -> anyhow::Result<Vec<String>>;

    /// Encodes the initial subscription frame sent immediately upon WebSocket
    /// connect.
    fn encode_initial_subscribe(&self, symbols: &[String]) -> anyhow::Result<String>;

    /// Encodes the incremental subscribe frame sent on an already-open socket.
    fn encode_subscribe(&self, symbols: &[String]) -> anyhow::Result<String>;

    /// Encodes the incremental unsubscribe frame sent on an already-open socket.
    fn encode_unsubscribe(&self, symbols: &[String]) -> anyhow::Result<String>;

    /// Decodes a raw WebSocket text frame into 0..N normalized events.
    ///
    /// Returns `Ok(vec![])` for heartbeats (`"PONG"`) or non-orderbook
    /// informational frames.
    fn decode_text_frame(
        &self,
        raw: &str,
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<Vec<NormalizedEvent>>;
}

/// Converts a decimal string such as `"0.29"`, `".48"` or `"-0.05"` into an
/// exact `i64` scaled by `1e6`, using integer arithmetic only.
///
/// Digits beyond the sixth decimal place are truncated, so `"12.3456789"`
/// becomes `12_345_678`. Never rounds, and never uses floating point: parsing
/// `"0.29"` through `f64` loses precision and breaks SBE replay equivalence.
pub fn parse_scaled_1e6(raw: &str) -> anyhow::Result<i64> {
    let raw = raw.trim();
    anyhow::ensure!(!raw.is_empty(), "cannot parse an empty decimal");

    let (negative, unsigned) = match raw.as_bytes()[0] {
        b'-' => (true, &raw[1..]),
        b'+' => (false, &raw[1..]),
        _ => (false, raw),
    };

    let (integer, fraction) = match unsigned.split_once('.') {
        Some((integer, fraction)) => (integer, fraction),
        None => (unsigned, ""),
    };
    anyhow::ensure!(
        !fraction.contains('.'),
        "invalid decimal {raw:?}: more than one decimal point"
    );
    anyhow::ensure!(
        !(integer.is_empty() && fraction.is_empty()),
        "invalid decimal {raw:?}: no digits"
    );

    let mut value = 0i64;
    for byte in integer.bytes() {
        let digit =
            digit_of(byte).with_context(|| format!("invalid decimal {raw:?}: not a number"))?;
        value = value
            .checked_mul(10)
            .and_then(|value| value.checked_add(digit))
            .with_context(|| format!("decimal {raw:?} overflows i64"))?;
    }
    value = value
        .checked_mul(SCALE)
        .with_context(|| format!("decimal {raw:?} overflows i64 at scale 1e6"))?;

    // Fractional digits are accumulated into six places, zero padded, and any
    // further digits are truncated.
    let mut units = 0i64;
    let mut digits = 0;
    for byte in fraction.bytes() {
        let digit =
            digit_of(byte).with_context(|| format!("invalid decimal {raw:?}: not a number"))?;
        if digits < SCALE_DIGITS {
            units = units * 10 + digit;
            digits += 1;
        }
    }
    while digits < SCALE_DIGITS {
        units *= 10;
        digits += 1;
    }

    value = value
        .checked_add(units)
        .with_context(|| format!("decimal {raw:?} overflows i64 at scale 1e6"))?;

    Ok(if negative { -value } else { value })
}

fn digit_of(byte: u8) -> Option<i64> {
    match byte {
        b'0'..=b'9' => Some(i64::from(byte - b'0')),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_scaled_1e6_exactness() {
        let cases = [
            ("0", 0),
            ("0.29", 290_000),
            (".48", 480_000),
            ("1.0", 1_000_000),
            ("12.345678", 12_345_678),
            // Truncates, does not round.
            ("12.3456789", 12_345_678),
            ("-0.05", -50_000),
            ("300.00", 300_000_000),
            ("0.000001", 1),
            ("0.0000009", 0),
            ("1.", 1_000_000),
            ("+2.5", 2_500_000),
            ("-0.00", 0),
        ];

        for (raw, expected) in cases {
            assert_eq!(parse_scaled_1e6(raw).unwrap(), expected, "parsing {raw:?}");
        }
    }

    #[test]
    fn test_parse_scaled_1e6_rejects_invalid_input() {
        for raw in [
            "", "   ", "-", ".", "1.2.3", "1e5", "abc", "1_000", "--1", "1,5",
        ] {
            assert!(
                parse_scaled_1e6(raw).is_err(),
                "expected {raw:?} to be rejected"
            );
        }
    }

    #[test]
    fn test_parse_scaled_1e6_has_no_floating_point_drift() {
        // 0.1 + 0.2 through f64 would drift; the integer path must not.
        assert_eq!(
            parse_scaled_1e6("0.1").unwrap() + parse_scaled_1e6("0.2").unwrap(),
            parse_scaled_1e6("0.3").unwrap()
        );
    }
}
