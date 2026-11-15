//! Kalshi [`VenueCodec`].
//!
//! Kalshi API keys cannot be issued in the author's region, so this codec is a
//! **compatibility proof for Extension 3** rather than the primary collector:
//! it compiles, and its encoding/decoding is unit tested against recorded
//! frames, but it is never exercised against the live feed. Polymarket is the
//! venue the service actually runs against.
//!
//! Consequence: the signed WebSocket handshake is intentionally not
//! implemented (it would require an RSA/Ed25519 crypto dependency for a codec
//! that cannot be run here). [`KalshiCodec::ws_request_headers`] fails fast with
//! a clear error instead of opening an unauthenticated socket.

use std::time::Duration;

use anyhow::{Context, bail};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    Side,
    codec::{NormalizedEvent, VenueCodec, parse_scaled_1e6},
};

const MARKETS_URL: &str = "https://api.elections.kalshi.com/trade-api/v2/markets";
const WS_URL: &str = "wss://external-api-ws.kalshi.com/trade-api/ws/v2";
const PAGE_SIZE: usize = 1_000;

/// `1e6` units per Kalshi cent: 1c == 0.01 dollars.
const CENT_1E6: i64 = 10_000;
/// `1e6` units per whole contract, used by legacy integer sizes.
const CONTRACT_1E6: i64 = 1_000_000;

/// Kalshi adapters, proving the engine is venue-agnostic.
#[derive(Debug, Default, Clone, Copy)]
pub struct KalshiCodec;

#[derive(Debug, Deserialize)]
struct MarketsPage {
    #[serde(default)]
    markets: Vec<Market>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Market {
    #[serde(default)]
    ticker: String,
    #[serde(default)]
    mve_collection_ticker: Option<String>,
}

fn market_container(frame: &Value) -> &Value {
    frame.get("msg").unwrap_or(frame)
}

/// Reads a `yes`/`no` ladder from either the current fixed-point string form
/// (`[[\"0.0800\", \"300.00\"]]`) or the legacy integer-cent form
/// (`[[8, 300]]`).
fn parse_levels(
    message: &Value,
    dollars_key: &str,
    cents_key: &str,
) -> anyhow::Result<Vec<(i64, i64)>> {
    if let Some(levels) = message.get(dollars_key).and_then(Value::as_array) {
        return levels
            .iter()
            .map(|level| {
                let level = level
                    .as_array()
                    .with_context(|| format!("{dollars_key} level is not a pair"))?;
                anyhow::ensure!(level.len() == 2, "{dollars_key} level must have 2 items");
                Ok((
                    parse_scaled_1e6(
                        level[0]
                            .as_str()
                            .with_context(|| format!("{dollars_key} price is not a string"))?,
                    )?,
                    parse_scaled_1e6(
                        level[1]
                            .as_str()
                            .with_context(|| format!("{dollars_key} size is not a string"))?,
                    )?,
                ))
            })
            .collect();
    }

    if let Some(levels) = message.get(cents_key).and_then(Value::as_array) {
        return levels
            .iter()
            .map(|level| {
                let level = level
                    .as_array()
                    .with_context(|| format!("{cents_key} level is not a pair"))?;
                anyhow::ensure!(level.len() == 2, "{cents_key} level must have 2 items");
                let price = level[0]
                    .as_i64()
                    .with_context(|| format!("{cents_key} price is not an integer"))?;
                let size = level[1]
                    .as_i64()
                    .with_context(|| format!("{cents_key} size is not an integer"))?;
                Ok((price * CENT_1E6, size * CONTRACT_1E6))
            })
            .collect();
    }

    // No ladder on this side simply means no resting orders, which Kalshi omits.
    Ok(Vec::new())
}

/// Kalshi sends the change time as `ts_ms` (when it was recorded) and, on the
/// envelope, `sending_ts_ms` (when it was queued for the wire). Prefer the
/// recorded time, fall back to the send time, then to local arrival.
fn exchange_timestamp(frame: &Value, message: &Value, fallback: DateTime<Utc>) -> DateTime<Utc> {
    let recorded = message.get("ts_ms").and_then(Value::as_i64);
    let queued = frame.get("sending_ts_ms").and_then(Value::as_i64);

    recorded
        .or(queued)
        .and_then(DateTime::from_timestamp_millis)
        .unwrap_or(fallback)
}

fn decode_level(side: &str, price: i64) -> anyhow::Result<(Side, i64)> {
    match side {
        // Kalshi's book is quoted in YES terms. A NO bid at p is a YES offer at
        // 1 - p, so the price inverts onto our ask ladder.
        "yes" => Ok((Side::Buy, price)),
        "no" => Ok((Side::Sell, 1_000_000 - price)),
        other => bail!("unknown Kalshi side {other:?}"),
    }
}

fn decode_value(frame: &Value, received_at: DateTime<Utc>) -> anyhow::Result<Vec<NormalizedEvent>> {
    let message = market_container(frame);
    let symbol = message
        .get("market_ticker")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    if symbol.is_empty() {
        return Ok(Vec::new());
    }

    match frame.get("type").and_then(Value::as_str) {
        Some("orderbook_snapshot") => {
            let yes = parse_levels(message, "yes_dollars_fp", "yes")?;
            let no = parse_levels(message, "no_dollars_fp", "no")?;
            let exchange_ts = exchange_timestamp(frame, message, received_at);

            let (mut bids, mut asks) = (Vec::new(), Vec::new());
            for (side, price, size) in yes
                .into_iter()
                .map(|(price, size)| (Side::Buy, price, size))
                .chain(
                    no.into_iter()
                        .map(|(price, size)| (Side::Sell, 1_000_000 - price, size)),
                )
            {
                match side {
                    Side::Buy => bids.push((price, size)),
                    Side::Sell => asks.push((price, size)),
                }
            }

            Ok(vec![NormalizedEvent::Snapshot {
                symbol,
                exchange_ts,
                received_at,
                bids,
                asks,
            }])
        }
        Some("orderbook_delta") => {
            let price = match message.get("price_dollars").and_then(Value::as_str) {
                Some(raw) => parse_scaled_1e6(raw)?,
                None => {
                    message
                        .get("price")
                        .and_then(Value::as_i64)
                        .context("orderbook delta without a price")?
                        * CENT_1E6
                }
            };
            let quantity = match message.get("delta_fp").and_then(Value::as_str) {
                Some(raw) => parse_scaled_1e6(raw)?,
                None => {
                    message
                        .get("delta")
                        .and_then(Value::as_i64)
                        .context("orderbook delta without a delta")?
                        * CONTRACT_1E6
                }
            };
            let (side, price_level) = decode_level(
                message
                    .get("side")
                    .and_then(Value::as_str)
                    .context("orderbook delta without a side")?,
                price,
            )?;

            Ok(vec![NormalizedEvent::Delta {
                symbol,
                exchange_ts: exchange_timestamp(frame, message, received_at),
                received_at,
                side,
                price_level,
                // Kalshi's delta applies relative to the current level size.
                is_absolute: false,
                quantity,
            }])
        }
        Some("market_lifecycle_v2") => {
            let is_active = matches!(
                message.get("event_type").and_then(Value::as_str),
                Some("created" | "activated")
            );
            Ok(vec![NormalizedEvent::MarketLifecycle { symbol, is_active }])
        }
        _ => Ok(Vec::new()),
    }
}

#[async_trait::async_trait]
impl VenueCodec for KalshiCodec {
    fn name(&self) -> &'static str {
        "kalshi"
    }

    fn ws_url(&self) -> &str {
        WS_URL
    }

    fn ws_request_headers(&self) -> anyhow::Result<Vec<(String, String)>> {
        bail!(
            "Kalshi authenticates the WebSocket handshake and this build does not implement \
             request signing; run the collector against Polymarket (see SPEC.md 3.4)"
        )
    }

    fn heartbeat_interval(&self) -> Option<Duration> {
        None
    }

    fn heartbeat_payload(&self) -> Option<String> {
        None
    }

    async fn fetch_active_symbols(
        &self,
        client: &reqwest::Client,
        max_symbols: Option<usize>,
    ) -> anyhow::Result<Vec<String>> {
        let mut symbols = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let mut request = client.get(MARKETS_URL).query(&[
                ("status", "open".to_owned()),
                ("limit", PAGE_SIZE.to_string()),
            ]);
            if let Some(cursor) = cursor.as_deref() {
                request = request.query(&[("cursor", cursor.to_owned())]);
            }

            let page: MarketsPage = request
                .send()
                .await
                .context("Kalshi markets request failed")?
                .error_for_status()
                .context("Kalshi markets returned an error")?
                .json()
                .await
                .context("decoding Kalshi markets page")?;

            for market in page.markets {
                // MVEs are explicitly out of scope for this challenge.
                if market.mve_collection_ticker.is_none() && !market.ticker.is_empty() {
                    symbols.push(market.ticker);
                }
            }

            if max_symbols.is_some_and(|max| symbols.len() >= max) {
                break;
            }

            match page.cursor {
                Some(next) if !next.is_empty() && Some(&next) != cursor.as_ref() => {
                    cursor = Some(next);
                }
                _ => break,
            }
        }

        if let Some(max) = max_symbols {
            symbols.truncate(max);
        }

        Ok(symbols)
    }

    fn encode_initial_subscribe(&self, symbols: &[String]) -> anyhow::Result<String> {
        Ok(json!({
            "id": 1,
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta"],
                "market_tickers": symbols,
            },
        })
        .to_string())
    }

    fn encode_subscribe(&self, symbols: &[String]) -> anyhow::Result<String> {
        Ok(json!({
            "id": 2,
            "cmd": "update_subscription",
            "params": {
                "sids": [1],
                "action": "add_markets",
                "market_tickers": symbols,
            },
        })
        .to_string())
    }

    fn encode_unsubscribe(&self, symbols: &[String]) -> anyhow::Result<String> {
        Ok(json!({
            "id": 3,
            "cmd": "update_subscription",
            "params": {
                "sids": [1],
                "action": "delete_markets",
                "market_tickers": symbols,
            },
        })
        .to_string())
    }

    fn decode_text_frame(
        &self,
        raw: &str,
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<Vec<NormalizedEvent>> {
        let raw = raw.trim();
        if raw.is_empty() {
            return Ok(Vec::new());
        }

        let frame: Value = serde_json::from_str(raw).context("decoding Kalshi frame")?;
        decode_value(&frame, received_at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn received_at() -> DateTime<Utc> {
        DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap()
    }

    #[test]
    fn test_kalshi_orderbook_snapshot_and_delta_decoding() -> anyhow::Result<()> {
        let codec = KalshiCodec;
        let at = received_at();

        let events = codec.decode_text_frame(
            r#"{
                "type": "orderbook_snapshot",
                "sending_ts_ms": 1669149841234,
                "sid": 2,
                "seq": 2,
                "msg": {
                    "market_ticker": "FED-23DEC-T3.00",
                    "clock": 4182057,
                    "yes_dollars_fp": [["0.0800", "300.00"]],
                    "no_dollars_fp": [["0.5400", "20.00"]]
                }
            }"#,
            at,
        )?;

        match &events[..] {
            [
                NormalizedEvent::Snapshot {
                    symbol,
                    bids,
                    asks,
                    exchange_ts,
                    ..
                },
            ] => {
                assert_eq!(symbol, "FED-23DEC-T3.00");
                assert_eq!(bids, &vec![(80_000, 300_000_000)]);
                // A NO bid at 0.54 is a YES offer at 1 - 0.54.
                assert_eq!(asks, &vec![(460_000, 20_000_000)]);
                assert_eq!(
                    *exchange_ts,
                    DateTime::from_timestamp_millis(1_669_149_841_234).unwrap()
                );
            }
            other => panic!("expected a snapshot, got {other:?}"),
        }

        // Legacy integer-cent delta, still seen on some responses.
        let events = codec.decode_text_frame(
            r#"{
                "type": "orderbook_delta",
                "seq": 3,
                "msg": {
                    "market_ticker": "FED-23DEC-T3.00",
                    "price": 96,
                    "delta": -54,
                    "side": "yes",
                    "ts_ms": 1669149841000
                }
            }"#,
            at,
        )?;

        match &events[..] {
            [
                NormalizedEvent::Delta {
                    side,
                    price_level,
                    quantity,
                    is_absolute,
                    ..
                },
            ] => {
                assert_eq!(*side, Side::Buy);
                assert_eq!(*price_level, 960_000);
                assert_eq!(*quantity, -54_000_000);
                assert!(!is_absolute);
            }
            other => panic!("expected a delta, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_kalshi_mve_filtering_and_subscribe_frames() -> anyhow::Result<()> {
        // MVEs must be dropped; regular markets kept.
        let page: MarketsPage = serde_json::from_str(
            r#"{
                "markets": [
                    {"ticker": "REGULAR-1"},
                    {"ticker": "MVE-1", "mve_collection_ticker": "KXMVE"},
                    {"ticker": ""}
                ],
                "cursor": ""
            }"#,
        )?;
        let kept: Vec<_> = page
            .markets
            .into_iter()
            .filter(|market| market.mve_collection_ticker.is_none() && !market.ticker.is_empty())
            .map(|market| market.ticker)
            .collect();
        assert_eq!(kept, vec!["REGULAR-1"]);

        let codec = KalshiCodec;
        let symbols = vec!["REGULAR-1".to_owned()];

        let initial: Value = serde_json::from_str(&codec.encode_initial_subscribe(&symbols)?)?;
        assert_eq!(initial["cmd"], "subscribe");
        assert_eq!(initial["params"]["channels"], json!(["orderbook_delta"]));

        let add: Value = serde_json::from_str(&codec.encode_subscribe(&symbols)?)?;
        assert_eq!(add["params"]["action"], "add_markets");

        let remove: Value = serde_json::from_str(&codec.encode_unsubscribe(&symbols)?)?;
        assert_eq!(remove["params"]["action"], "delete_markets");

        // No credentials in this build: fail loudly rather than connect anonymously.
        assert!(codec.ws_request_headers().is_err());

        Ok(())
    }
}
