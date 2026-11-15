# `SPEC.md` — Voluntas Orderbook Data Collector (Rust)

## 0. Ground Rules & Non-Negotiable Invariants

1. **Language & Edition:** Rust `edition = "2024"`. Do not alter `src/util/build.rs` or any `ergo-sbe` schema definitions.
2. **All Operational Logs on `stderr`:** `stdout` is strictly reserved for binary SBE output. Output defaults to `stdout` when `--output` is omitted or set to `-`; any other value is treated as a file path. Every log line, status counter, warning, or error MUST use `eprintln!` or a logger configured to `std::io::stderr`. Never use `println!` anywhere in production code paths.
3. **Zero Floating-Point (`f64`/`f32`) Math on Prices or Quantities:** All prices and quantities MUST be parsed directly from decimal strings into `i64` scaled by **$1,000,000$ ($10^6$)** using integer arithmetic. Using `f64` causes IEEE-754 rounding drift (e.g., `"0.29"` becoming `289_999`) and breaks SBE replay equivalence.
4. **SBE Replay Invariant:** For every `symbol` (`asset_id`), applying every emitted `PriceLevelUpdateDomain` in order onto the preceding `SnapshotDomain` MUST produce the exact same orderbook state as the next emitted `SnapshotDomain`.
5. **Lock-Free Single-Consumer State:** WebSocket producer tasks MUST NOT share `Arc<Mutex<OrderBook>>`. All orderbook state (`BTreeMap<i64, i64>`), failover arbitration, activity tracking, 60-second snapshots, and `AlignedSbeWriter` writes live exclusively inside a single consumer task fed via `tokio::sync::mpsc`.
6. **Atomic Git Commits:** Execute one phase at a time. Run `cargo check`, `cargo test`, and any phase verification command before committing with the exact commit message specified in Section 9.

---

## 1. Repository & Module Layout

```text
.
├── Cargo.toml
├── SPEC.md
├── LATENCY_DISCOVERIES.md          # Generated & populated in Phase 8 & 9
├── README.md                       # Setup, CLI flags, architecture summary (Phase 9)
└── src/
    ├── lib.rs                      # Re-exports Side, util, codec, venues, engine, net, bench
    ├── main.rs                     # CLI parsing (clap), wiring MPSC pipeline, SIGINT graceful drain
    ├── util/
    │   ├── mod.rs
    │   ├── build.rs                # Existing SBE codegen (DO NOT MODIFY)
    │   ├── dtos/                   # Existing SBE DTOs (DO NOT MODIFY)
    │   └── aligned_sbe_writer.rs   # Generalized to AlignedSbeWriter<W: Write = File> (Phase 1)
    ├── codec.rs                    # VenueCodec trait, NormalizedEvent, parse_scaled_1e6()
    ├── venues/
    │   ├── mod.rs
    │   ├── polymarket.rs           # Full Gamma REST + CLOB WS implementation
    │   └── kalshi.rs               # Clean VenueCodec implementation/adapter stub proving generic core
    ├── net/
    │   ├── mod.rs
    │   └── tuned_ws.rs             # socket2 TCP tuning (TCP_NODELAY, TCP_QUICKACK, port bind) + N-way connection racing
    ├── engine/
    │   ├── mod.rs
    │   ├── orderbook.rs            # OrderBook (BTreeMap<i64, i64>), apply_snapshot, apply_delta, to_snapshot_domain
    │   ├── socket.rs               # Per-shard WS session: connect, subscribe, heartbeat, decode, reconnect (Phase 4)
    │   ├── shard_manager.rs        # Dynamic token-to-shard assignment, 2x redundant Conn A/B, make-before-break rotation
    │   └── consumer.rs             # Single-consumer MPSC loop, failover arbiter, activity watchdog, 60s timer, SBE writer
    └── bench.rs                    # --benchmark-latency empirical suite (Phase 8)

```

---

## 2. Exact Template SBE Contract (`src/util/`)

The existing SBE DTOs in `engineering_challenge::util::dtos::challenge_orderbook` have these exact fields (verified from `src/main.rs`):

```rust
pub struct SnapshotBidsEntryDomain {
    pub price_level: i64, // scaled by 1_000_000 (e.g., $0.48 -> 480_000)
    pub quantity: i64,    // scaled by 1_000_000 (e.g., 30.0  -> 30_000_000)
}

pub struct SnapshotAsksEntryDomain {
    pub price_level: i64,
    pub quantity: i64,
}

pub struct SnapshotDomain {
    pub received_at: DateTime<Utc>,
    pub exchange_timestamp: DateTime<Utc>,
    pub bids: Vec<SnapshotBidsEntryDomain>, // Sorted descending by price_level (best bid first)
    pub asks: Vec<SnapshotAsksEntryDomain>, // Sorted ascending by price_level (best ask first)
    pub symbol: String,                     // Polymarket asset_id (77-digit token ID)
}

pub struct PriceLevelUpdateDomain {
    pub backtest_timestamp: DateTime<Utc>, // Local arrival timestamp (received_at)
    pub exchange_timestamp: DateTime<Utc>,
    pub price_level: i64,                  // scaled by 1_000_000
    pub quantity: i64,                     // NEW ABSOLUTE quantity at price_level; 0 = delete level
    pub side: Side,                        // Side::Buy or Side::Sell
    pub symbol: String,
}

```

### `AlignedSbeWriter` Generalization (`Phase 1`)

In `AlignedSbeWriter`:

- Change struct definition to `pub struct AlignedSbeWriter<W: std::io::Write="std::fs::File">`.
- Keep `pub fn create(path: impl AsRef<Path>, chunk: usize) -> anyhow::Result<AlignedSbeWriter<std::fs::File>>` unchanged so existing callers and tests compile without modification.
- Add `pub fn from_writer(writer: W, chunk: usize) -> anyhow::Result<Self>` with the exact same `chunk > HEADER_LENGTH` check.
- Use `const CHUNK_SIZE_64_MEBIBYTES: usize = 64 << 20;` when writing to files, and a smaller chunk size (e.g. `1 << 20` = 1 MiB) when streaming to `stdout` so pipe consumers receive chunks promptly, while ALWAYS calling `writer.flush()?` on `SIGINT`.

---

## 3. Generic Exchange Layer (`src/codec.rs`) & Polymarket Specification (`src/venues/polymarket.rs`)

### 3.1 Normalized Domain Events (`src/codec.rs`)

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnSlot {
    Primary, // Conn A
    Standby, // Conn B
}

#[derive(Debug, Clone)]
pub enum NormalizedEvent {
    Snapshot {
        symbol: String,
        exchange_ts: DateTime<Utc>,
        received_at: DateTime<Utc>,
        bids: Vec<(i64, i64)>, // (price_1e6, size_1e6)
        asks: Vec<(i64, i64)>,
    },
    Delta {
        symbol: String,
        exchange_ts: DateTime<Utc>,
        received_at: DateTime<Utc>,
        side: Side,
        price_level: i64,
        /// If true, `quantity` is the new total level size (Polymarket).
        /// If false, `quantity` is a signed relative delta to add to existing level (Kalshi).
        is_absolute: bool,
        quantity: i64,
    },
    MarketLifecycle {
        symbol: String,
        is_active: bool,
    },
}

#[derive(Debug, Clone)]
pub struct StampedEnvelope {
    pub shard_id: usize,
    pub slot: ConnSlot,
    pub generation: u64, // Increments on make-before-break connection rotation
    pub event: NormalizedEvent,
}

```

### 3.2 `VenueCodec` Trait (`src/codec.rs`)

```rust
#[async_trait::async_trait]
pub trait VenueCodec: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    fn ws_url(&self) -> &str;

    /// Extra headers for the WebSocket upgrade request. Defaults to none.
    fn ws_request_headers(&self) -> anyhow::Result<Vec<(String, String)>> {
        Ok(Vec::new())
    }

    fn heartbeat_interval(&self) -> Option<std::time::Duration>;
    fn heartbeat_payload(&self) -> Option<String>;

    /// Fetches all currently active, tradable, non-MVE symbols from REST.
    /// `max_symbols` stops pagination early, so smoke tests need not walk the venue.
    async fn fetch_active_symbols(
        &self,
        client: &reqwest::Client,
        max_symbols: Option<usize>,
    ) -> anyhow::Result<Vec<String>>;

    /// Encodes the initial subscription frame sent immediately upon WebSocket connect.
    fn encode_initial_subscribe(&self, symbols: &[String]) -> anyhow::Result<String>;

    /// Encodes incremental subscribe/unsubscribe frames on an already-open WebSocket.
    fn encode_subscribe(&self, symbols: &[String]) -> anyhow::Result<String>;
    fn encode_unsubscribe(&self, symbols: &[String]) -> anyhow::Result<String>;

    /// Decodes a raw WebSocket text frame into 0..N normalized events.
    /// Returns `Ok(vec![])` for heartbeats ("PONG") or non-orderbook informational frames.
    fn decode_text_frame(
        &self,
        raw: &str,
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<Vec<NormalizedEvent>>;
}

```

### 3.3 Exact Polymarket Protocol Rules (`src/venues/polymarket.rs`)

1. **Market Discovery (`fetch_active_symbols`):**

- Paginate `GET https://gamma-api.polymarket.com/markets/keyset?active=true&closed=false&limit=100`, passing the previous response's `next_cursor` back as `after_cursor`. Stop when `markets` is empty or `next_cursor` is absent. **The legacy `/markets` endpoint cannot be used for a full scan: it 422s with `offset too large, use /markets/keyset for deeper pagination` past roughly 2,100 rows, and the keyset endpoint rejects `offset` entirely and caps `limit` at 100.** Gamma silently ignores unknown query parameters, so `cursor=`/`next_cursor=` return page 1 forever — the parameter name must be exactly `after_cursor`.
- Filter each market object:
- Must have `enableOrderBook == Some(true)` (JSON key `"enableOrderBook"`).
- Must have `closed != Some(true)` and `active == Some(true)`.

- Extract `"clobTokenIds"`:
- **CRITICAL QUIRK:** Gamma API sometimes returns `"clobTokenIds": ["123...", "456..."]` as a JSON array AND sometimes as a JSON-encoded string `"clobTokenIds": "[\"123...\", \"456...\"]"`. Implement a custom `serde` deserializer that accepts both `Vec<String>` and `String` (parsing the inner string via `serde_json::from_str::<Vec<String>>`).

- Emit all non-empty token IDs (both YES and NO outcome tokens) deduplicated via `HashSet`.

2. **WebSocket Connection & Framing:**

- URL: `wss://[ws-subscriptions-clob.polymarket.com/ws/market](https://ws-subscriptions-clob.polymarket.com/ws/market)`
- **Heartbeat:** Every `10 seconds`, send `Message::Text("PING".into())`. Ignore incoming `Message::Text("PONG")`.
- **Initial Subscribe:**

```json
{
  "type": "market",
  "assets_ids": ["<TOKEN_1>", "<TOKEN_2>"],
  "custom_feature_enabled": true
}
```

- **Dynamic Subscribe / Unsubscribe:**

```json
{ "assets_ids": ["<TOKEN_1>"], "operation": "subscribe" }
```

```json
{ "assets_ids": ["<TOKEN_1>"], "operation": "unsubscribe" }
```

3. **Incoming Frame Decoding (`decode_text_frame`):**

- Trim whitespace; if `raw == "PONG"` or `raw.is_empty()`, return `Ok(vec![])`.
- Polymarket sends either a **single JSON object** `{...}` or a **JSON array of objects** `[{...}, {...}]`. Deserialize into `#[serde(untagged)] enum RawWsFrame { Single(serde_json::Value), Batch(Vec<serde_json::Value>) }`.
- Inspect `"event_type"`:
- **`"book"` (Full Snapshot):**
- `asset_id`: string $\rightarrow$ `symbol`
- `timestamp`: optional stringified epoch milliseconds (e.g., `"1766789469958"`) $\rightarrow$ `DateTime::from_timestamp_millis(ms)` (fallback to `received_at`).
- `bids`: array of `{"price": ".48", "size": "30"}` $\rightarrow$ parse via `parse_scaled_1e6`.
- `asks`: array of `{"price": ".52", "size": "25"}` $\rightarrow$ parse via `parse_scaled_1e6`.
- Emit `NormalizedEvent::Snapshot`.

- **`"price_change"` (Incremental Level Update):**
- Outer `"timestamp"` (stringified ms) $\rightarrow$ `exchange_ts`.
- Iterate `"price_changes"` array. Each item has:
- `"asset_id"`: string
- `"price"`: decimal string $\rightarrow$ `parse_scaled_1e6`
- `"size"`: decimal string $\rightarrow$ `parse_scaled_1e6` (`0` = delete level)
- `"side"`: `"BUY"` $\rightarrow$ `Side::Buy`, `"SELL"` $\rightarrow$ `Side::Sell`

- Emit `NormalizedEvent::Delta { is_absolute: true, ... }` for each item.

- **`"new_market"` / `"market_resolved"` (from `custom_feature_enabled: true`):**
- Extract `"assets_ids"` array; emit `NormalizedEvent::MarketLifecycle { symbol, is_active: true/false }` so new/resolved markets are reacted to immediately in addition to the 30-second REST reconciliation poll.

- **All other `event_type`s** (`"last_trade_price"`, `"tick_size_change"`, `"best_bid_ask"`): ignore cleanly (`Ok(vec![])`) without failing.

### 3.4 Kalshi Adapter Proof (`src/venues/kalshi.rs`)

Kalshi is implemented as a **compatibility proof for Extension 3**, not as the primary collector. Kalshi API keys cannot be issued in the author's region, so this codec must compile and pass its unit tests but is never exercised against the live feed. Polymarket is the venue the service is actually run against (the challenge explicitly permits this fallback).

Implement `KalshiCodec` implementing `VenueCodec` to prove the generic core:

- `ws_url()` $\rightarrow$ `"wss://external-api-ws.kalshi.com/trade-api/ws/v2"` (`wss://api.elections.kalshi.com/trade-api/ws/v2` is also supported).
- **Handshake authentication:** Kalshi authenticates the WebSocket upgrade itself, including for public market-data channels, so there is no anonymous connection. Send `KALSHI-ACCESS-KEY`, `KALSHI-ACCESS-TIMESTAMP` (Unix milliseconds) and `KALSHI-ACCESS-SIGNATURE` = base64 signature over `"{timestamp}GET/trade-api/ws/v2"` (Ed25519, or RSA-PSS/SHA-256). Credentials come from `KALSHI_API_KEY_ID` / `KALSHI_PRIVATE_KEY_PATH`; when absent, fail with a clear error rather than attempting an unauthenticated connect.
- `fetch_active_symbols()` $\rightarrow$ cursor-paginates `GET /trade-api/v2/markets?status=open&limit=1000&cursor=...`, filtering out `mve_collection_ticker.is_some()` (non-MVE markets only).
- `encode_initial_subscribe()` $\rightarrow$ `{"id":<n>,"cmd":"subscribe","params":{"channels":["orderbook_delta"],"market_tickers":[...]}}`. `encode_subscribe()` / `encode_unsubscribe()` use `update_subscription` with `add_markets` / `delete_markets`, since a connection holds a single `orderbook_delta` subscription.
- `decode_text_frame()` $\rightarrow$ parses `"orderbook_snapshot"` (`yes_dollars_fp` / `no_dollars_fp`: arrays of `["0.0800","300.00"]` price/size pairs) and `"orderbook_delta"` (`price_dollars`, `delta_fp`, `side`), scaling every decimal string through `parse_scaled_1e6`. Kalshi's `yes` bids map to `Side::Buy` at `price`; `no` bids map to `Side::Sell` at `1_000_000 - price`; deltas use `is_absolute: false` because `delta_fp` is a relative change.
- `heartbeat_interval()` / `heartbeat_payload()` $\rightarrow$ `None` (no client heartbeat needed); `"market_lifecycle_v2"` frames map to `NormalizedEvent::MarketLifecycle`.

### 3.5 Implementation Decisions (Phases 2–3)

Small decisions taken while implementing the codec layer, recorded here so the reasoning is not lost:

1. **`VenueCodec::ws_request_headers()` is part of the trait, with a default of `Ok(vec![])`.** Kalshi authenticates the WebSocket *upgrade*, so a venue-neutral collector needs somewhere to put handshake headers. Defaulting to empty keeps Polymarket (and every future unauthenticated venue) from implementing anything, and keeps the trait object-safe.
2. **The Kalshi codec deliberately takes no crypto dependency.** Signing would pull in `rsa`/`ed25519-dalek`/`ring` for a codec that cannot be run without an API key in this region. `ws_request_headers()` fails fast with an explicit error rather than opening an unauthenticated socket. The codec is still fully exercised at the decode/encode level by unit tests, which is what Extension 3 demonstrates.
3. **A `book` frame with neither `bids` nor `asks` is rejected.** Polymarket omits an empty ladder on some versions, so a missing *one* side is read as empty — but a frame with no ladder at all is an error. Emitting an empty snapshot would silently wipe the local book, which is exactly the phantom-liquidity failure the challenge warns about.
4. **`price_change.size == "0"` is not special-cased.** It flows through as `quantity = 0, is_absolute = true`, and the order book removes the level. This keeps the codec free of book semantics.
5. **`new_market` / `market_resolved` read `assets_ids`, falling back to a single `asset_id`.** Polymarket has used both shapes; a missing symbol just yields no event rather than failing the socket.
6. **Gamma filtering is `active == true && closed != true && enableOrderBook == true`.** Token IDs are deduplicated both within and across pages, and empty IDs are dropped.
7. **Kalshi decoding accepts both payload generations.** Current responses use fixed-point strings (`yes_dollars_fp`, `no_dollars_fp`, `price_dollars`, `delta_fp`); legacy responses use integer cents (`yes`, `no`, `price`, `delta`). Both normalise to `1e6` via `parse_scaled_1e6` or `× 10_000` for cents. `ts_ms` is preferred over `sending_ts_ms`, then local arrival, because it is the exchange's recording time.
8. **Tests beyond the mandatory §7 list.** Added lifecycle decoding, subscription-frame encoding, and Kalshi snapshot/delta/MVE coverage, because the venue layer is where protocol drift will show up first.

---

## 4. Core Engine, Redundancy, Failover & Watchdog (`src/engine/`)

### 4.1 Per-Token Dual-Book State (`src/engine/consumer.rs`)

Because Polymarket sends **per-level** absolute updates (not whole-book snapshots on every delta), a dropped packet leaves stale phantom levels in memory. Therefore, each `symbol` in the single consumer maintains **two completely isolated orderbooks**:

```rust
pub struct TrackedSymbol {
    pub book_a: OrderBook,
    pub book_b: OrderBook,
    pub active_slot: ConnSlot,          // Default: ConnSlot::Primary
    pub last_exch_ts_a: Option<DateTime<Utc>>,
    pub last_exch_ts_b: Option<DateTime<Utc>>,
    pub last_recv_a: Option<Instant>,
    pub last_recv_b: Option<Instant>,
    pub has_emitted_initial_snapshot: bool,
    pub rolling_msg_timestamps: VecDeque<Instant>, // Last 5 minutes for activity tier
    pub last_resubscribe_attempt: Option<Instant>,
    pub shard_id: usize,
}

```

### 4.2 Exact Event Processing & Failover Rules (Extension 1)

When `StampedEnvelope { shard_id, slot, generation, event }` arrives at the single consumer:

1. **Ignore stale generations:** If `generation < current_generation_for(shard_id, slot)`, drop the envelope (it came from a retired connection during make-before-break cycling).
2. **Update the matching isolated book (`book_a` for `Primary`, `book_b` for `Standby`):**

- Record `rolling_msg_timestamps.push_back(Instant::now())` (pruning entries older than `300s`).
- Update `last_exch_ts_{a|b}` and `last_recv_{a|b}`.
- Apply `Snapshot` or `Delta` to `book_{a|b}` ONLY. Never cross-contaminate `book_a` with `Conn B` events.

3. **Check Failover Condition (when `slot != active_slot`):**

- Switch `active_slot` from `OldSlot` to `NewSlot` **if and only if** `book_{new}` has received at least one `Snapshot` (`book_{new}.is_hydrated() == true`) AND any of the following hold:

1. `book_{old}.is_hydrated() == false` (Primary hasn't received its snapshot yet, Standby already has).
2. `NewSlot`'s `exchange_timestamp` exceeds `OldSlot`'s `last_exch_ts` by **`> 150ms`** (Primary is lagging or stalled).
3. `OldSlot` is currently marked disconnected/reconnecting.

- **Mandatory Failover Action:** Upon switching `active_slot = NewSlot`, immediately serialize `book_{new}.to_snapshot_domain(symbol, received_at)` and write it via `writer.write_msg(&snapshot)?` BEFORE emitting any subsequent deltas from `NewSlot`.

4. **Emit Downstream SBE Message (only if `slot == active_slot`):**

- If `event` is `Snapshot`: emit `SnapshotDomain` and set `has_emitted_initial_snapshot = true`.
- If `event` is `Delta`:
- Only emit `PriceLevelUpdateDomain` if `book_{active}.is_hydrated() == true` (never emit deltas before a book has received its initial snapshot).
- Query the **resulting post-update quantity** at `price_level` from `book_{active}` (`0` if removed) so `PriceLevelUpdateDomain.quantity` is always the exact post-delta level size.

### 4.3 Periodic 60-Second Snapshots

- Run a `tokio::time::interval(Duration::from_secs(60))` inside the single consumer.
- On tick, iterate over all `TrackedSymbol`s where `book_{active}.is_hydrated() == true`, construct `SnapshotDomain` (`received_at = Utc::now()`), and call `writer.write_msg(&snap)?`.
- Because this runs inside the single consumer loop, it is impossible to serialize a half-updated book.

### 4.4 Subscription Monitoring & Make-Before-Break Cycling (Extension 2)

1. **Per-Symbol Activity Watchdog (runs every `10 seconds` in consumer):**

- Compute 5-minute message count `N_5m = rolling_msg_timestamps.len()`.
- **Tier Classification:**
- **`Active` (`N_5m >= 6`):** Expected to update at least once every ~50s. If `active_slot` has received zero messages for `> 30s` (and `last_resubscribe_attempt` was `> 60s` ago), emit `ShardCommand::ResubscribeToken { shard_id, slot: active_slot, symbol }` to force an `unsubscribe` $\rightarrow$ `100ms delay` $\rightarrow$ `subscribe` cycle on that single connection—while the partner connection keeps streaming uninterrupted!
- **`Quiet` (`N_5m < 6`):** Naturally low-activity market. Do **not** churn subscriptions unless one socket received an update and the partner socket missed it (handled automatically by the 150ms cross-socket failover rule).

2. **Make-Before-Break Full Connection Rotation:**

- Every WebSocket connection has a jittered lifetime of **`35 minutes ± 5 minutes`** (between `30m` and `40m`).
- **4-Step Zero-Loss Handover:**

1. Spawn replacement connection `Conn_New` (`generation + 1`) using the connection racer (`Phase 7`), subscribing it to all `assigned_symbols` in that shard.
2. Wait until `Conn_New` receives initial `"book"` snapshots for `>= 95%` of its assigned symbols (or a `5-second` warm-up timeout elapses).
3. Atomically update `current_generation` in the consumer so `Conn_New` becomes the active `Primary`/`Standby` slot, emitting failover `SnapshotDomain`s for any active symbols whose book state differs.
4. Send `SocketCommand::Shutdown` to `Conn_Old` (sends WebSocket `Close` frame and drops socket).

### 4.5 Implementation Decisions (Phase 4)

1. **`src/engine/socket.rs` is an addition to the original layout.** The connect/subscribe/heartbeat/decode/reconnect loop is separated from `main.rs` so it can be driven by a local WebSocket server in tests; `shard_manager.rs` (Phase 5) will own assignment and Conn A/B redundancy and simply drive these sessions. The URL is a parameter rather than read from the codec, which is what makes the offline end-to-end test possible.
2. **Chunk sizes differ by sink.** Files use 64 MiB chunks (fewer, larger writes); stdout uses 1 MiB so a downstream pipe consumer sees data promptly instead of waiting for a 64 MiB block.
3. **A delta for an unknown or unhydrated symbol is dropped, not buffered.** Applying it would build a book from partial state, and buffering would let memory grow unboundedly for symbols that never snapshot. The periodic snapshot of that symbol will hydrate it.
4. **A malformed frame is logged and the socket stays up.** Dropping the connection would cause a full resubscribe of the shard; redundancy and the next snapshot already cover the gap. Only transport errors trigger reconnect.
5. **Periodic snapshots use the last observed exchange timestamp** as `exchange_timestamp`, and wall-clock `received_at` as `received_at`. Using `Utc::now()` for both would claim the venue sent the snapshot now, which is untrue and pollutes latency analysis.
6. **The writer stays on the consumer task and uses blocking `Write`.** That is what guarantees no torn books. A 1 MiB stdout chunk bounds how long a slow pipe can stall the runtime; moving writes to `spawn_blocking` would reintroduce ordering problems for no measurable gain here.
7. **Reconnect backoff is 1s doubling to 30s, reset after a session survives 60s.** On reconnect the worker re-subscribes its *current* symbol set, so subscription changes survive transport failures.
8. **Subscription diffs are applied with `send().await`,** so a symbol is only removed from the reconciler's `known` set once the owning worker accepted the command. A full channel delays the next poll instead of silently losing a subscription.
9. **CLI flags land with the phase that implements them.** Phase 4 ships `--output`, `--tokens-per-socket`, `--poll-interval-secs`, `--snapshot-interval-secs` and `--max-tokens`. `--redundancy`, `--race-connections`, `--rotate-minutes` and `--benchmark-latency` arrive in Phases 5-8, so the binary never advertises a flag it ignores.
10. **`tests/end_to_end.rs` runs a local WebSocket server** and drives the real worker and consumer, because the development sandbox cannot reach Polymarket at all (egress is blocked). It asserts the written SBE reads back as the expected snapshot and delta, and that a malformed frame does not kill the connection.

### 4.6 Live-Validation Findings (Phase 4)

Verified against live Polymarket over a mobile hotspot (the campus network blocks the venue outright). Two findings changed the implementation:

1. **Gamma market discovery must use keyset pagination.** The legacy `/markets` endpoint returns `422 {"error": "offset too large, use /markets/keyset for deeper pagination"}` past roughly 2,100 rows, so the original "increment `offset` by 500 until empty" plan silently missed most markets. `PolymarketCodec` now walks `/markets/keyset` with `after_cursor` at 100 rows per page. The parameter name is load-bearing: Gamma ignores unknown parameters silently, so `cursor=`/`next_cursor=` re-serve page 1 forever.
2. **`fetch_active_symbols` takes `max_symbols`.** `--max-tokens` now bounds pagination itself instead of truncating after a full scan, so a smoke test makes one or two REST calls rather than dozens. Venues front their REST API with CDNs that hand out edge IPs which reset connections, so startup discovery retries 5 times with 1s..16s backoff, and the poller simply retries on the next tick.
3. **Observed live run** (40 symbols, two sockets, 50s, 15s snapshots): 120 snapshots, 370 deltas, 40 hydrated books, 5,050 resting levels, **0 replay-invariant violations** when the capture was replayed. `examples/inspect.rs` performs that replay against any capture file.
4. **`stdout` output is verified byte-clean.** With `--output` omitted and with `-o -`, the capture is exactly N × 1 MiB (`file` reports `data`), contains zero log text, and every status line appears on `stderr`.

### 4.7 Implementation Decisions (Phase 5)

1. **`EngineMessage` carries slot health, not just events.** Failover rule 3 ("the old slot is disconnected") cannot be inferred reliably from event timestamps, so workers send `EngineMessage::Disconnected { shard_id, slot, generation }` when a session dies. The consumer marks that slot down for every symbol on the shard and clears the flag on the next event.
2. **Generations are per connection attempt.** Each worker increments a counter before every `connect_async`, stamps every envelope with it, and the consumer keeps the highest generation seen per `(shard, slot)`. Events from a retired socket (a make-before-break or reconnect straggler) are dropped rather than written.
3. **The delta that triggers failover is folded into the reconciliation snapshot and not also emitted.** The snapshot already contains its effect, so emitting both would double-apply the change on replay. Order on switch is: apply to the standby book → decide → write snapshot → (no delta).
4. **Deltas from the non-active slot are applied but never written.** Both books stay warm so a switch is instant, but only the active slot produces SBE output.
5. **`ShardManager` sends every subscription change to all connections of a shard**, so `/` and `-` stay symmetric; a reconnect therefore re-subscribes the correct set. Placement is least-loaded, and `add`/`remove` still `send().await` so the reconciler only drops a symbol from `known` once a worker accepted the command.
6. **`--redundancy` is clamped to 1..=2.** One connection disables failover entirely (useful for isolation/debugging and for venues where a second socket is pointless); two is the production default.
7. **Density default is 500 symbols/socket** for the reason in Section 6. `test_failover_emits_reconciliation_snapshot` covers the switch itself, and `stale_generations_are_ignored` covers the retired-socket case.
8. **Lag-based failover requires evidence of missed updates, not just a timestamp gap.** Comparing exchange timestamps alone made the two connections trade places on quiet markets: a 50-second live run produced 157 switches with only 3 socket errors, so this was a rule bug rather than connectivity. Failover on lag now requires the standby to have delivered **at least 2 updates since the active slot last spoke**, *and* to be more than 150ms ahead. A genuinely stalled primary accumulates missed updates immediately; a quiet market cannot accumulate them at all, because both feeds go quiet together. `Consumer::with_failover_silence` was removed once the count-based rule made the silence window unnecessary.
9. **A resync is not an invariant violation.** When a failover snapshot or a post-reconnect venue `book` is emitted, the new snapshot is authoritative and may legitimately disagree with the state built from the previous feed — that is exactly why Extension 1 requires a snapshot on switch. `examples/inspect.rs` therefore reports `resync points` separately from structural corruption, and only fails on negative sizes or crossed books. The strict byte-equality invariant is demonstrated on a single-connection capture, where 120 snapshots and 370 deltas produced zero mismatches.

### 4.8 Implementation Decisions (Phase 6)

1. **Activity tiers use a rolling 5-minute window of arrival times**, pruned on every watchdog tick and capped at 512 samples per symbol so a hyperactive market cannot grow memory without bound. `>= 6` samples means `Active`; below that means `Quiet`.
2. **Only `Active` symbols are ever churned.** A quiet symbol is silent because the market is quiet, so unsubscribing it would add risk for no benefit. An `Active` symbol is revived only when the active slot has produced nothing for 30s *and* the last resubscribe attempt was more than 60s ago, which is what prevents a resubscribe loop.
3. **Revival is single-connection and local.** `SocketCommand::ResubscribeToken` performs `unsubscribe → 100ms → subscribe` on the one connection that stalled; the partner keeps streaming the symbol, so there is no observation gap.
4. **The watchdog acts through `MaintenanceSink`, not through subscription state.** The consumer decides *when* a symbol looks dead; `ShardManager` owns routing and implements the sink. This keeps the consumer free of channel topology and lets the watchdog be tested against a recording sink.
5. **Rotation is make-before-break and lives inside the worker task.** On the jittered deadline the worker connects a replacement, subscribes it, and forwards its events (tagged with a higher generation) while it warms up for up to 5s or until 95% of the shard's symbols have produced a snapshot. Only then is the old socket closed. A failed replacement is discarded and the healthy socket is kept — rotation must never cause an outage.
6. **Generations are shared per `(shard, slot)` across rotations**, not reset by the new task, so a replacement always outranks its predecessor and late frames from the old socket are dropped by the consumer's generation gate rather than written.
7. **Jitter is `base ± base/7` capped at 5 minutes, derived from the clock rather than a PRNG**, so ~150 sockets do not rotate in lockstep. The standby slot is offset by a further half lifetime, so the two connections of a shard never rotate together — while one warms up, the other is still streaming. `--rotate-minutes 0` disables rotation entirely, which is what the short verification runs use.
8. **Known cosmetic issue:** the venue sends `new_market` lifecycle frames on *both* connections, so each is logged twice. Subscriptions are driven by the REST reconciler, so this is only log noise; de-duplication belongs with the reconciler work.

### 4.9 Implementation Decisions (Phase 7)

1. **Sockets are built with `socket2` and handed to Tokio via `TcpSocket::from_std_stream`,** before the TLS handshake, so tuning applies to the handshake as well as the data path.
2. **`TCP_QUICKACK` needs `libc`,** which is the one dependency added beyond the Phase 1 list, and only on Unix. It is applied best-effort: a failure costs latency, never correctness. The option is not sticky on some kernels, which is recorded in `LATENCY_DISCOVERIES.md`.
3. **Racing dials before subscribing.** Candidates are ranked on `PING`/`PONG` while idle, so no order book traffic is mixed into the measurement, and only the winner ever receives a subscription.
4. **Racing is bounded and optional.** `--race-connections` defaults to 4 and caps at 16; `1` disables it and does a single tuned dial. Ten probes separated by 50 ms bound warm-up to roughly 0.6 s, which matters at ~143 sockets.
5. **Per-connection source-port binding is plumbed but off by default.** The kernel already assigns distinct ephemeral ports for Conn A and Conn B; the hook exists for hosts where RSS hashing collapses them.
6. **`LATENCY_DISCOVERIES.md` is maintained from Phase 7 onward,** not written at the end: each technique is recorded with its rationale, its cost, and an explicit `[IMPLEMENTED]`/`[MEASURED]`/`[PENDING]` marker so unmeasured claims cannot be mistaken for results.

---

## 5. Low-Latency Networking & Benchmark Suite (`src/net/` & `src/bench.rs`)

### 5.1 Socket Tuning (`src/net/tuned_ws.rs`)

Build TCP streams via `socket2::Socket` before passing to `tokio::net::TcpStream::from_std` and `tokio_tungstenite::client_async_tls_with_config`:

- `socket.set_nodelay(true)?` (`TCP_NODELAY` — disables Nagle's algorithm).
- `socket.set_recv_buffer_size(2 * 1024 * 1024)?` (2 MiB receive buffer to absorb news-event bursts without kernel packet drops).
- On Linux (`#[cfg(target_os = "linux")]`), apply `TCP_QUICKACK` via `libc::setsockopt` to suppress Linux's 40ms delayed-ACK timer on incoming frames.
- **Source-Port Separation:** Allow optional explicit ephemeral port binding (`bind_port_hint: Option<u16>`) so `Conn A` and `Conn B` use distinct local source ports, preventing NIC Receive-Side Scaling (RSS) / router 4-tuple hashing collisions.

### 5.2 Connection Racing ("Edge Lottery")

When opening a connection for a shard (`--race-connections N`, default `4` in production, configurable up to `16` in benchmarks):

1. Dial `N` parallel TLS + WebSocket connections concurrently to `ws_url()`.
2. Send 10 rapid `"PING"` frames spaced `50ms` apart on each candidate socket and record `"PONG"` RTTs using `std::time::Instant`.
3. Rank candidates by **`p50` RTT** (tie-broken by `p99` RTT).
4. Keep the **fastest** socket(s) (`Conn A` and `Conn B` from distinct top candidates) and cleanly close the remaining `N - 2` sockets.

### 5.3 `--benchmark-latency` Mode (`src/bench.rs`)

When invoked with `cargo run --release -- --benchmark-latency`, run 4 automated empirical experiments against live Polymarket endpoints (for ~30–60 seconds each) and output formatted Markdown tables to `stdout` (so you can paste/save them directly into `LATENCY_DISCOVERIES.md`):

1. **Experiment A — Connection Racing Variance:** Open `12` parallel WebSocket connections, run 30 `"PING"`/`"PONG"` probes each, and report min/median/max `p50` and `p99` RTT across the 12 sockets (quantifying Cloudflare PoP/worker spread).
2. **Experiment B — TCP Socket Tuning (`Default` vs. `TCP_NODELAY + TCP_QUICKACK + 2MB SO_RCVBUF`):** Measure one-way wire-to-process latency (`received_at - exchange_timestamp` in ms, reporting `p50`, `p95`, `p99`, `p99.9` over 2,000 live `price_change` deltas).
3. **Experiment C — Shard Density Sweep (`25` vs. `100` vs. `250` vs. `500` tokens per socket):** Measure Time-to-Last-Snapshot on initial subscribe and `p99` delta arrival latency under live load.
4. **Experiment D — Parser & Runtime Overhead:** Benchmark `serde_json` parsing throughput + `parse_scaled_1e6` latency per frame (`ns/frame`) on captured real Polymarket payloads.

---

## 6. CLI Interface (`src/main.rs`)

```text
voluntas-collector [OPTIONS]

Options:
  -o, --output <PATH>              Output file path for AlignedSbeWriter; `-` (or omitting the flag) streams SBE bytes to stdout, any other value is a file path
      --tokens-per-socket <N>      Max symbols per WebSocket shard [default: 500]
      --redundancy <N>             Connections per shard (1 or 2) [default: 2]
      --race-connections <N>       Parallel TLS handshakes to race per slot [default: 4]
      --poll-interval-secs <N>     REST market discovery interval [default: 30]
      --snapshot-interval-secs <N> Periodic full snapshot interval [default: 60]
      --rotate-minutes <N>         Base TTL for make-before-break socket cycling [default: 35]
      --max-tokens <N>             Optional cap on total tokens (useful for quick smoke tests)
      --benchmark-latency          Run empirical latency experiments instead of collector
  -h, --help                       Print help

```

Options are introduced by the phase that implements them, so the binary never advertises a flag it ignores. Phase 4 shipped `-o/--output`, `--tokens-per-socket`, `--poll-interval-secs`, `--snapshot-interval-secs` and `--max-tokens`; Phase 5 added `--redundancy`; Phase 6 adds `--rotate-minutes`; Phase 7 adds `--race-connections`; `--benchmark-latency` follows in Phase 8.

`--tokens-per-socket` defaults to **500**, not 150. Live discovery measured at least 39,500 active markets / 78,804 tokens (the walk was cut short, so this is a lower bound), which at 150/socket would mean ~526 connections. The collector derives the shard count from this value and logs the resulting socket count on startup.

### Graceful `SIGINT` / `SIGTERM` Shutdown Sequence

1. `tokio::select!` catches `tokio::signal::ctrl_c()` (and `SIGTERM` on Unix).
2. Coordinator cancels the REST poller and broadcasts `SocketCommand::Shutdown` to all active and standby WebSocket workers.
3. Each worker sends `Message::Close(None)` with a `1.5s` timeout and drops its `mpsc::Sender`.
4. Single consumer drains all remaining buffered events in `mpsc::Receiver` until the channel closes (`while let Some(env) = rx.recv().await`).
5. Consumer calls `writer.flush()?` and drops `writer` (closing the file handle cleanly).
6. Process exits with code `0`.

---

## 7. Mandatory Automated Test Suite (`cargo test`)

Every phase MUST include unit/integration tests that pass via `cargo test`:

1. **`test_parse_scaled_1e6_exactness`:** Verifies `"0"`, `"0.29"`, `".48"`, `"1.0"`, `"12.345678"`, `"12.3456789"` (truncates to 6 decimals), `"-0.05"` produce exact `i64` values with zero floating-point drift.
2. **`test_polymarket_gamma_clob_token_ids_formats`:** Verifies deserializing both JSON array `["111", "222"]` and stringified JSON `"[\"111\", \"222\"]"`, plus filtering out `enableOrderBook: false`.
3. **`test_polymarket_ws_book_and_price_change_decoding`:** Verifies single-object frames, array-batched frames, `"PONG"` heartbeats, `"book"` snapshots, and `"price_change"` deltas.
4. **`test_sbe_replay_invariant_roundtrip`:** Feeds a `Snapshot` followed by 20 `Delta`s (including level insertions, size modifications, and `size == 0` deletions) and a periodic 60s `Snapshot` into `OrderBook` + `AlignedSbeWriter`, reads them back with `AlignedSbeReader::<OrderbookSchema>`, replays the updates onto the first snapshot, and asserts byte-for-byte equality with the final snapshot.
5. **`test_failover_emits_reconciliation_snapshot`:** Simulates `Conn A` dropping a level deletion while `Conn B` processes it; verifies that when `Conn B` advances `exchange_timestamp` by `> 150ms` ahead of `Conn A`, the consumer switches `active_slot` to `Standby` and emits a `SnapshotDomain` matching `Conn B`'s exact state before emitting subsequent deltas.
6. **`test_activity_tier_classification`:** Verifies that an `Active` token (`>= 6` msgs in 5m) triggers a single-socket resubscribe command after `30s` of silence, while a `Quiet` token (`< 6` msgs in 5m) does not churn.

---

## 8. Step-by-Step Git Execution Plan

Execute these 9 phases strictly in order. After each phase, run `cargo test` (and the phase smoke check), verify zero compiler warnings, and commit:

| Phase | Git Commit Message                                                                            | Scope & Verification                                                                                                                                                                                                                             |
| ----- | --------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **1** | `chore: generalize AlignedSbeWriter and add async networking dependencies`                    | Generalize `AlignedSbeWriter<W: Write="File">` with `from_writer`; add `tokio`, `tokio-tungstenite`, `futures-util`, `reqwest`, `serde_json`, `socket2`, `clap`, `async-trait`, `rand`. Verify `cargo test` passes existing SBE round-trip test. |
| **2** | `feat(codec): add generic VenueCodec trait and exact 1e6 fixed-point parser`                  | Create `src/codec.rs` with `VenueCodec`, `NormalizedEvent`, `StampedEnvelope`, `parse_scaled_1e6`, and `test_parse_scaled_1e6_exactness`.                                                                                                        |
| **3** | `feat(polymarket): implement Gamma market discovery and CLOB WS codec`                        | Create `src/venues/polymarket.rs` + `src/venues/kalshi.rs` adapter; add unit tests for Gamma `clobTokenIds` parsing and `book`/`price_change` decoding.                                                                                          |
| **4** | `feat(engine): implement single-consumer OrderBook, 60s SBE snapshots, and SIGINT flush`      | Implement `src/engine/orderbook.rs`, `src/engine/consumer.rs`, `src/main.rs` (base collector with `--output` and `stdout` support, 10s `"PING"`, 30s REST poller, 60s snapshots, `SIGINT` flush); pass `test_sbe_replay_invariant_roundtrip`.    |
| **5** | `feat(resilience): add sharded 2x redundant websockets and failover reconciliation snapshots` | Implement `src/engine/shard_manager.rs` with dynamic bucket packing (~150 tokens/socket), `Conn A` + `Conn B` isolated books, 150ms cross-socket lag failover, and `test_failover_emits_reconciliation_snapshot`.                                |
| **6** | `feat(watchdog): add activity-tier zombie detection and make-before-break socket cycling`     | Add rolling 5-minute `Active`/`Quiet` watchdog (`test_activity_tier_classification`) and 35-minute jittered make-before-break connection rotation.                                                                                               |
| **7** | `perf(net): add socket2 TCP tuning and N-way parallel connection racing`                      | Create `src/net/tuned_ws.rs` with `TCP_NODELAY`, `TCP_QUICKACK`, `SO_RCVBUF`, source-port separation, and parallel TLS/WS connection racing.                                                                                                     |
| **8** | `perf(bench): add --benchmark-latency suite for empirical latency experiments`                | Create `src/bench.rs` and wire `--benchmark-latency` CLI flag to run Experiments A–D against Polymarket and print Markdown tables.                                                                                                               |
| **9** | `docs: add empirical LATENCY_DISCOVERIES.md and project README.md`                            | Run `--benchmark-latency` on WSL2 Ubuntu, populate `LATENCY_DISCOVERIES.md` with real measured `p50`/`p95`/`p99`/`p99.9` tables and WSL2 NAT vs. mirrored networking analysis, and write `README.md`.                                            |
