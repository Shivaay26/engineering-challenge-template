//! Polymarket [`VenueCodec`]: market discovery and CLOB WebSocket order book
//! decoding.
//!
//! Discovery uses the **CLOB's own** market index
//! (`clob.polymarket.com/sampling-simplified-markets`), not the website/CMS API
//! on `gamma-api.polymarket.com`. The difference is not cosmetic:
//!
//! | Endpoint | Rows/page | Bytes/market | Markets | Time |
//! | --- | --- | --- | --- | --- |
//! | CLOB sampling | 1,000 | ~535 | 8,749 | ~5 s |
//! | Gamma keyset | 100 | ~6,500 | 105,000+ | >8 min |
//!
//! Gamma embeds a full description, rules, images and nested events per market
//! and hard-caps a page at 100, so a full walk is 1,000+ sequential requests.
//! The CLOB index returns only the identifiers, and only markets that are
//! actually accepting orders. Gamma remains as a fallback if the CLOB index is
//! unavailable.

use std::{collections::HashSet, time::Duration};

use anyhow::{Context, bail};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    Side,
    codec::{NormalizedEvent, VenueCodec, parse_scaled_1e6},
};

const CLOB_MARKETS_URL: &str = "https://clob.polymarket.com/sampling-simplified-markets";
const GAMMA_MARKETS_URL: &str = "https://gamma-api.polymarket.com/markets/keyset";
const CLOB_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
/// The CLOB index serves 1,000 markets per page.
const CLOB_PAGE_SIZE: usize = 1_000;
/// The Gamma keyset endpoint caps a page at 100 markets and rejects `offset`.
const GAMMA_PAGE_SIZE: usize = 100;
/// Attempts per page. Retrying the whole walk instead would throw away every
/// page already fetched whenever a single request is reset.
const PAGE_ATTEMPTS: u32 = 4;

/// Polymarket's order book feed over the CLOB WebSocket.
#[derive(Debug, Default, Clone, Copy)]
pub struct PolymarketCodec;

/// One page from the CLOB market index.
#[derive(Debug, Deserialize)]
struct ClobMarketsPage {
    #[serde(default)]
    data: Vec<ClobMarket>,
    /// `LTE=` on the last page.
    #[serde(default)]
    next_cursor: Option<String>,
}

/// One market from the CLOB index: identifiers only, no prose.
#[derive(Debug, Deserialize)]
struct ClobMarket {
    #[serde(default)]
    tokens: Vec<ClobToken>,
    #[serde(default)]
    active: bool,
    #[serde(default)]
    closed: bool,
    #[serde(default)]
    accepting_orders: bool,
    #[serde(default)]
    archived: bool,
}

#[derive(Debug, Deserialize)]
struct ClobToken {
    #[serde(default)]
    token_id: String,
}

/// Returns the tradable token ids of CLOB-indexed markets, in first-seen order.
fn clob_active_symbols(markets: impl IntoIterator<Item = ClobMarket>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut symbols = Vec::new();

    for market in markets {
        let tradable =
            market.active && !market.closed && market.accepting_orders && !market.archived;
        if !tradable {
            continue;
        }

        for token in market.tokens {
            if !token.token_id.is_empty() && seen.insert(token.token_id.clone()) {
                symbols.push(token.token_id);
            }
        }
    }

    symbols
}

/// Fetches one JSON page, retrying that request only.
async fn fetch_page<T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    label: &str,
    url: &str,
    query: &[(&str, String)],
) -> anyhow::Result<T> {
    let mut last_error = None;

    for attempt in 1..=PAGE_ATTEMPTS {
        match client.get(url).query(query).send().await {
            Ok(response) => match response.error_for_status() {
                Ok(response) => match response.json::<T>().await {
                    Ok(page) => return Ok(page),
                    Err(error) => {
                        last_error = Some(anyhow::Error::new(error).context("decoding page"))
                    }
                },
                Err(error) => last_error = Some(anyhow::Error::new(error)),
            },
            Err(error) => last_error = Some(anyhow::Error::new(error)),
        }

        if attempt < PAGE_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(300 * u64::from(attempt))).await;
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("{label} request failed")))
}

/// One page from Gamma's keyset `/markets/keyset` endpoint.
#[derive(Debug, Deserialize)]
struct MarketsPage {
    #[serde(default)]
    markets: Vec<GammaMarket>,
    /// Absent on the last page.
    #[serde(default)]
    next_cursor: Option<String>,
}

/// One market as returned by Gamma's `/markets` endpoint.
///
/// Only the fields needed to decide whether to subscribe are kept; unknown
/// fields are ignored by `serde`.
///
/// Outcome identifiers come in two shapes: Polymarket Protocol **V2** markets
/// carry `positionIds` as a JSON array, while legacy **CTF** markets carry
/// `clobTokenIds` (sometimes itself a JSON-encoded string). Reading only
/// `clobTokenIds` silently drops every V2 market.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarket {
    #[serde(default)]
    active: Option<bool>,
    #[serde(default)]
    closed: Option<bool>,
    #[serde(default)]
    enable_order_book: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_clob_token_ids")]
    clob_token_ids: Vec<String>,
    #[serde(default)]
    position_ids: Vec<String>,
}

/// Gamma sometimes returns `clobTokenIds` as an array and sometimes as a
/// JSON-encoded string containing the same array.
fn deserialize_clob_token_ids<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum ClobTokenIds {
        List(Vec<String>),
        Encoded(String),
    }

    match Option::<ClobTokenIds>::deserialize(deserializer)? {
        None => Ok(Vec::new()),
        Some(ClobTokenIds::List(ids)) => Ok(ids),
        Some(ClobTokenIds::Encoded(raw)) => {
            serde_json::from_str::<Vec<String>>(&raw).map_err(serde::de::Error::custom)
        }
    }
}

/// Returns the tradable symbols of `markets`, in first-seen order.
fn active_symbols(markets: impl IntoIterator<Item = GammaMarket>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut symbols = Vec::new();

    for market in markets {
        let tradable = market.active == Some(true)
            && market.closed != Some(true)
            && market.enable_order_book == Some(true);
        if !tradable {
            continue;
        }

        // V2 markets expose `positionIds`; legacy CTF markets expose
        // `clobTokenIds`. Prefer whichever is present.
        let token_ids = if market.position_ids.is_empty() {
            market.clob_token_ids
        } else {
            market.position_ids
        };

        for token in token_ids {
            if !token.is_empty() && seen.insert(token.clone()) {
                symbols.push(token);
            }
        }
    }

    symbols
}

fn parse_side(raw: &str) -> anyhow::Result<Side> {
    match raw.to_ascii_uppercase().as_str() {
        "BUY" => Ok(Side::Buy),
        "SELL" => Ok(Side::Sell),
        other => bail!("unknown Polymarket side {other:?}"),
    }
}

/// Reads Polymarket's optional stringified epoch-milliseconds `timestamp`,
/// falling back to local arrival time when absent or malformed.
fn exchange_timestamp(value: &Value, fallback: DateTime<Utc>) -> DateTime<Utc> {
    let millis = match value.get("timestamp") {
        Some(Value::String(raw)) => raw.parse::<i64>().ok(),
        Some(Value::Number(number)) => number.as_i64(),
        _ => None,
    };

    millis
        .and_then(DateTime::from_timestamp_millis)
        .unwrap_or(fallback)
}

fn required_str<'a>(value: &'a Value, key: &str) -> anyhow::Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .with_context(|| format!("missing string field {key:?}"))
}

/// Parses `[{"price": "0.48", "size": "30"}]` into `(price_1e6, size_1e6)`.
///
/// A missing side is treated as empty (Polymarket omits empty ladders in some
/// versions), but a `book` frame with neither side present is an error: emitting
/// an empty snapshot would silently wipe the local book.
fn parse_levels(value: &Value, key: &str) -> anyhow::Result<Vec<(i64, i64)>> {
    let Some(levels) = value.get(key) else {
        return Ok(Vec::new());
    };

    levels
        .as_array()
        .with_context(|| format!("field {key:?} is not an array"))?
        .iter()
        .map(|level| {
            Ok((
                parse_scaled_1e6(required_str(level, "price")?)?,
                parse_scaled_1e6(required_str(level, "size")?)?,
            ))
        })
        .collect()
}

fn lifecycle(value: &Value, is_active: bool) -> Vec<NormalizedEvent> {
    let symbols: Vec<String> = match value.get("assets_ids").and_then(Value::as_array) {
        Some(ids) => ids
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        None => value
            .get("asset_id")
            .and_then(Value::as_str)
            .map(|id| vec![id.to_owned()])
            .unwrap_or_default(),
    };

    symbols
        .into_iter()
        .filter(|symbol| !symbol.is_empty())
        .map(|symbol| NormalizedEvent::MarketLifecycle { symbol, is_active })
        .collect()
}

fn decode_value(value: &Value, received_at: DateTime<Utc>) -> anyhow::Result<Vec<NormalizedEvent>> {
    match value.get("event_type").and_then(Value::as_str) {
        Some("book") => {
            let has_bids = value.get("bids").is_some();
            let has_asks = value.get("asks").is_some();
            if !has_bids && !has_asks {
                bail!("book frame with neither bids nor asks");
            }

            Ok(vec![NormalizedEvent::Snapshot {
                symbol: required_str(value, "asset_id")?.to_owned(),
                exchange_ts: exchange_timestamp(value, received_at),
                received_at,
                bids: parse_levels(value, "bids")?,
                asks: parse_levels(value, "asks")?,
            }])
        }
        Some("price_change") => {
            let outer_symbol = value.get("asset_id").and_then(Value::as_str);
            let exchange_ts = exchange_timestamp(value, received_at);
            let changes = value
                .get("price_changes")
                .and_then(Value::as_array)
                .context("price_change frame without price_changes")?;

            let mut events = Vec::with_capacity(changes.len());
            for change in changes {
                let symbol = change
                    .get("asset_id")
                    .and_then(Value::as_str)
                    .or(outer_symbol)
                    .context("price change without an asset_id")?
                    .to_owned();

                events.push(NormalizedEvent::Delta {
                    symbol,
                    exchange_ts,
                    received_at,
                    side: parse_side(required_str(change, "side")?)?,
                    price_level: parse_scaled_1e6(required_str(change, "price")?)?,
                    // Polymarket sends the new absolute size of the level.
                    is_absolute: true,
                    quantity: parse_scaled_1e6(required_str(change, "size")?)?,
                });
            }

            Ok(events)
        }
        Some("new_market") => Ok(lifecycle(value, true)),
        Some("market_resolved") => Ok(lifecycle(value, false)),
        // last_trade_price, tick_size_change, best_bid_ask, ... are not order
        // book data; ignore them rather than failing the socket.
        _ => Ok(Vec::new()),
    }
}

impl PolymarketCodec {
    /// Fast path: the CLOB's own active-market index, 1,000 rows per page.
    async fn fetch_clob_symbols(
        &self,
        client: &reqwest::Client,
        max_symbols: Option<usize>,
    ) -> anyhow::Result<Vec<String>> {
        let mut seen = HashSet::new();
        let mut symbols = Vec::new();
        let mut cursor: Option<String> = None;
        let mut pages = 0usize;

        loop {
            let mut query = vec![("limit", CLOB_PAGE_SIZE.to_string())];
            if let Some(cursor) = cursor.as_deref() {
                query.push(("next_cursor", cursor.to_owned()));
            }

            let page: ClobMarketsPage =
                fetch_page(client, "CLOB markets", CLOB_MARKETS_URL, &query).await?;

            if page.data.is_empty() {
                break;
            }

            pages += 1;
            for symbol in clob_active_symbols(page.data) {
                if seen.insert(symbol.clone()) {
                    symbols.push(symbol);
                }
            }

            if pages.is_multiple_of(5) {
                eprintln!(
                    "discovery: {pages} CLOB page(s), {} symbol(s) so far",
                    symbols.len()
                );
            }

            if max_symbols.is_some_and(|max| symbols.len() >= max) {
                break;
            }

            match page.next_cursor {
                Some(next)
                    if !next.is_empty() && next != "LTE=" && Some(&next) != cursor.as_ref() =>
                {
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

    /// Fallback: the Gamma keyset walk. Complete but an order of magnitude
    /// slower, because each page carries full market prose and is capped at 100.
    async fn fetch_gamma_symbols(
        &self,
        client: &reqwest::Client,
        max_symbols: Option<usize>,
    ) -> anyhow::Result<Vec<String>> {
        let mut seen = HashSet::new();
        let mut symbols = Vec::new();
        let mut cursor: Option<String> = None;
        let mut pages = 0usize;

        loop {
            let mut query = vec![
                ("active", "true".to_owned()),
                ("closed", "false".to_owned()),
                ("limit", GAMMA_PAGE_SIZE.to_string()),
            ];
            if let Some(cursor) = cursor.as_deref() {
                query.push(("after_cursor", cursor.to_owned()));
            }

            let page: MarketsPage =
                fetch_page(client, "Gamma markets", GAMMA_MARKETS_URL, &query).await?;

            if page.markets.is_empty() {
                break;
            }

            pages += 1;
            if pages.is_multiple_of(50) {
                eprintln!(
                    "discovery: {pages} Gamma page(s), {} symbol(s) so far",
                    symbols.len()
                );
            }

            for symbol in active_symbols(page.markets) {
                if seen.insert(symbol.clone()) {
                    symbols.push(symbol);
                }
            }

            if max_symbols.is_some_and(|max| symbols.len() >= max) {
                break;
            }

            match page.next_cursor {
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
}

#[async_trait::async_trait]
impl VenueCodec for PolymarketCodec {
    fn name(&self) -> &'static str {
        "polymarket"
    }

    fn ws_url(&self) -> &str {
        CLOB_WS_URL
    }

    fn heartbeat_interval(&self) -> Option<Duration> {
        Some(HEARTBEAT_INTERVAL)
    }

    fn heartbeat_payload(&self) -> Option<String> {
        Some("PING".to_owned())
    }

    async fn fetch_active_symbols(
        &self,
        client: &reqwest::Client,
        max_symbols: Option<usize>,
    ) -> anyhow::Result<Vec<String>> {
        match self.fetch_clob_symbols(client, max_symbols).await {
            Ok(symbols) if !symbols.is_empty() => Ok(symbols),
            Ok(_) => {
                eprintln!("discovery: CLOB index returned no symbols, falling back to Gamma");
                self.fetch_gamma_symbols(client, max_symbols).await
            }
            Err(error) => {
                eprintln!("discovery: CLOB index failed ({error:#}), falling back to Gamma");
                self.fetch_gamma_symbols(client, max_symbols).await
            }
        }
    }

    fn encode_initial_subscribe(&self, symbols: &[String]) -> anyhow::Result<String> {
        Ok(json!({
            "type": "market",
            "assets_ids": symbols,
            "custom_feature_enabled": true,
        })
        .to_string())
    }

    fn encode_subscribe(&self, symbols: &[String]) -> anyhow::Result<String> {
        Ok(json!({ "assets_ids": symbols, "operation": "subscribe" }).to_string())
    }

    fn encode_unsubscribe(&self, symbols: &[String]) -> anyhow::Result<String> {
        Ok(json!({ "assets_ids": symbols, "operation": "unsubscribe" }).to_string())
    }

    fn decode_text_frame(
        &self,
        raw: &str,
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<Vec<NormalizedEvent>> {
        let raw = raw.trim();
        if raw.is_empty() || raw == "PONG" {
            return Ok(Vec::new());
        }

        // Dispatch on the first byte instead of using `#[serde(untagged)]`.
        // An untagged enum tries each variant in order, so a single-object frame
        // was parsed twice: once failing as `Vec<Value>`, then again as `Value`.
        // Experiment D measured that at ~115us for a 100-level book frame.
        if raw.starts_with('[') {
            let values: Vec<Value> =
                serde_json::from_str(raw).context("decoding Polymarket batch frame")?;
            let mut events = Vec::new();
            for value in &values {
                events.extend(decode_value(value, received_at)?);
            }
            Ok(events)
        } else {
            let value: Value = serde_json::from_str(raw).context("decoding Polymarket frame")?;
            decode_value(&value, received_at)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn received_at() -> DateTime<Utc> {
        DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap()
    }

    #[test]
    fn test_polymarket_gamma_clob_token_ids_formats() -> anyhow::Result<()> {
        let markets: Vec<GammaMarket> = serde_json::from_str(
            r#"[
                {
                    "active": true,
                    "closed": false,
                    "enableOrderBook": true,
                    "clobTokenIds": ["111", "222"]
                },
                {
                    "active": true,
                    "closed": false,
                    "enableOrderBook": true,
                    "clobTokenIds": "[\"333\", \"444\"]"
                },
                {
                    "active": true,
                    "closed": false,
                    "enableOrderBook": true,
                    "clobTokenIds": ["111", "", "555"]
                },
                {
                    "active": true,
                    "closed": false,
                    "enableOrderBook": false,
                    "clobTokenIds": ["666"]
                },
                {
                    "active": true,
                    "closed": true,
                    "enableOrderBook": true,
                    "clobTokenIds": ["777"]
                },
                {
                    "active": false,
                    "closed": false,
                    "enableOrderBook": true,
                    "clobTokenIds": ["888"]
                }
            ]"#,
        )?;

        assert_eq!(
            active_symbols(markets),
            vec!["111", "222", "333", "444", "555"]
        );

        Ok(())
    }

    #[test]
    fn test_polymarket_clob_market_discovery_decoding() -> anyhow::Result<()> {
        let page: ClobMarketsPage = serde_json::from_str(
            r#"{
                "data": [
                    {
                        "active": true,
                        "closed": false,
                        "accepting_orders": true,
                        "archived": false,
                        "tokens": [
                            {"token_id": "111", "outcome": "Yes"},
                            {"token_id": "222", "outcome": "No"}
                        ]
                    },
                    {
                        "active": true,
                        "closed": false,
                        "accepting_orders": false,
                        "archived": false,
                        "tokens": [{"token_id": "333"}]
                    },
                    {
                        "active": false,
                        "closed": false,
                        "accepting_orders": true,
                        "archived": false,
                        "tokens": [{"token_id": "444"}]
                    },
                    {
                        "active": true,
                        "closed": false,
                        "accepting_orders": true,
                        "archived": true,
                        "tokens": [{"token_id": "555"}]
                    }
                ],
                "next_cursor": "aWQ6MjE1NTM1Nw==",
                "limit": 1000
            }"#,
        )?;

        assert_eq!(page.next_cursor.as_deref(), Some("aWQ6MjE1NTM1Nw=="));
        // Only markets that are active, open, accepting orders and not archived.
        assert_eq!(clob_active_symbols(page.data), vec!["111", "222"]);

        // `LTE=` terminates the cursor chain.
        let last: ClobMarketsPage = serde_json::from_str(r#"{"data": [], "next_cursor": "LTE="}"#)?;
        assert!(last.data.is_empty());

        Ok(())
    }

    #[test]
    fn test_polymarket_gamma_position_ids_v2_format() -> anyhow::Result<()> {
        // Polymarket Protocol V2 markets expose `positionIds`; legacy CTF
        // markets expose `clobTokenIds`. Both must be collected, or every V2
        // market is silently dropped.
        let markets: Vec<GammaMarket> = serde_json::from_str(
            r#"[
                {
                    "version": "v2",
                    "active": true,
                    "closed": false,
                    "enableOrderBook": true,
                    "positionIds": [
                        "651150819117105875331414918119047680898421632356043490229292782433651916800",
                        "651150819117105875331414918119047680898421632356043490229292782433651916801"
                    ]
                },
                {
                    "active": true,
                    "closed": false,
                    "enableOrderBook": true,
                    "clobTokenIds": "[\"111\", \"222\"]"
                },
                {
                    "version": "v2",
                    "active": true,
                    "closed": false,
                    "enableOrderBook": true,
                    "positionIds": ["999"]
                }
            ]"#,
        )?;

        let symbols = active_symbols(markets);
        assert_eq!(symbols.len(), 5, "both wire shapes must contribute");
        assert_eq!(
            symbols[0],
            "651150819117105875331414918119047680898421632356043490229292782433651916800"
        );
        assert_eq!(symbols[2], "111");
        assert_eq!(symbols[4], "999");

        Ok(())
    }

    #[test]
    fn test_polymarket_gamma_keyset_page_decoding() -> anyhow::Result<()> {
        let page: MarketsPage = serde_json::from_str(
            r#"{
                "markets": [
                    {
                        "active": true,
                        "closed": false,
                        "enableOrderBook": true,
                        "clobTokenIds": "[\"111\", \"222\"]"
                    }
                ],
                "next_cursor": "abc"
            }"#,
        )?;
        assert_eq!(page.markets.len(), 1);
        assert_eq!(page.next_cursor.as_deref(), Some("abc"));

        // The final page omits next_cursor entirely.
        let last: MarketsPage = serde_json::from_str(r#"{"markets": []}"#)?;
        assert!(last.markets.is_empty());
        assert_eq!(last.next_cursor, None);

        Ok(())
    }

    #[test]
    fn test_polymarket_ws_book_and_price_change_decoding() -> anyhow::Result<()> {
        let codec = PolymarketCodec;
        let at = received_at();

        // Heartbeats and informational frames are not order book events.
        for ignored in ["PONG", "  ", r#"{"event_type":"last_trade_price"}"#] {
            assert!(
                codec.decode_text_frame(ignored, at)?.is_empty(),
                "expected {ignored:?} to be ignored"
            );
        }

        // Single object frame.
        let events = codec.decode_text_frame(
            r#"{
                "event_type": "book",
                "asset_id": "111",
                "timestamp": "1766789469958",
                "bids": [{"price": ".48", "size": "30"}],
                "asks": [{"price": ".52", "size": "25"}]
            }"#,
            at,
        )?;
        assert_eq!(events.len(), 1);
        match &events[0] {
            NormalizedEvent::Snapshot {
                symbol,
                exchange_ts,
                received_at: stamp,
                bids,
                asks,
            } => {
                assert_eq!(symbol, "111");
                assert_eq!(*stamp, at);
                assert_eq!(
                    *exchange_ts,
                    DateTime::from_timestamp_millis(1_766_789_469_958).unwrap()
                );
                assert_eq!(bids, &vec![(480_000, 30_000_000)]);
                assert_eq!(asks, &vec![(520_000, 25_000_000)]);
            }
            other => panic!("expected a snapshot, got {other:?}"),
        }

        // Batched frame: one empty-half book without a timestamp, plus two
        // absolute price changes (the second is a level deletion).
        let events = codec.decode_text_frame(
            r#"[
                {"event_type": "book", "asset_id": "222", "bids": [], "asks": []},
                {"event_type": "price_change", "timestamp": "1766789469958", "price_changes": [
                    {"asset_id": "222", "price": "0.49", "size": "0", "side": "SELL"},
                    {"asset_id": "333", "price": "0.51", "size": "12.5", "side": "BUY"}
                ]}
            ]"#,
            at,
        )?;
        assert_eq!(events.len(), 3);

        match &events[0] {
            NormalizedEvent::Snapshot {
                symbol,
                exchange_ts,
                bids,
                asks,
                ..
            } => {
                assert_eq!(symbol, "222");
                // No timestamp on the frame: fall back to local arrival time.
                assert_eq!(*exchange_ts, at);
                assert!(bids.is_empty() && asks.is_empty());
            }
            other => panic!("expected a snapshot, got {other:?}"),
        }

        for (event, symbol, side, price, quantity) in [
            (&events[1], "222", Side::Sell, 490_000, 0),
            (&events[2], "333", Side::Buy, 510_000, 12_500_000),
        ] {
            match event {
                NormalizedEvent::Delta {
                    symbol: decoded_symbol,
                    side: decoded_side,
                    price_level,
                    is_absolute,
                    quantity: decoded_quantity,
                    ..
                } => {
                    assert_eq!(decoded_symbol, symbol);
                    assert_eq!(*decoded_side, side);
                    assert_eq!(*price_level, price);
                    assert!(*is_absolute);
                    assert_eq!(*decoded_quantity, quantity);
                }
                other => panic!("expected a delta, got {other:?}"),
            }
        }

        Ok(())
    }

    #[test]
    fn test_polymarket_ws_market_lifecycle_decoding() -> anyhow::Result<()> {
        let codec = PolymarketCodec;
        let at = received_at();

        let events = codec.decode_text_frame(
            r#"{"event_type":"new_market","assets_ids":["111","222"],"question":"..."}"#,
            at,
        )?;
        assert_eq!(events.len(), 2);
        for event in &events {
            match event {
                NormalizedEvent::MarketLifecycle { is_active, .. } => assert!(is_active),
                other => panic!("expected a lifecycle event, got {other:?}"),
            }
        }

        let events = codec.decode_text_frame(
            r#"{"event_type":"market_resolved","assets_ids":["111"]}"#,
            at,
        )?;
        match &events[..] {
            [NormalizedEvent::MarketLifecycle { symbol, is_active }] => {
                assert_eq!(symbol, "111");
                assert!(!is_active);
            }
            other => panic!("expected one inactive lifecycle event, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_polymarket_subscribe_frames() -> anyhow::Result<()> {
        let codec = PolymarketCodec;
        let symbols = vec!["111".to_owned()];

        let initial: Value = serde_json::from_str(&codec.encode_initial_subscribe(&symbols)?)?;
        assert_eq!(initial["type"], "market");
        assert_eq!(initial["assets_ids"], json!(["111"]));
        assert_eq!(initial["custom_feature_enabled"], true);

        let subscribe: Value = serde_json::from_str(&codec.encode_subscribe(&symbols)?)?;
        assert_eq!(subscribe["operation"], "subscribe");

        let unsubscribe: Value = serde_json::from_str(&codec.encode_unsubscribe(&symbols)?)?;
        assert_eq!(unsubscribe["operation"], "unsubscribe");

        Ok(())
    }
}
