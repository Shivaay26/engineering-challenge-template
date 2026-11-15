# Voluntas order book collector

A Rust collector for Polymarket order book data. It discovers every live market,
maintains an order book per outcome token across sharded redundant WebSockets,
and writes snapshots plus price-level deltas as aligned SBE to `stdout` or a
file.

Kalshi is implemented as a second `VenueCodec` to show the engine is
venue-agnostic; see [Venues](#venues).

## Quick start

```bash
cargo build --release

# Stream SBE to stdout (default)
./target/release/engineering-challenge > capture.sbe

# Or write to a file
./target/release/engineering-challenge -o capture.sbe

# Smoke test: 40 symbols, two sockets
./target/release/engineering-challenge --max-tokens 40 --tokens-per-socket 20 -o smoke.sbe
```

Stop with `Ctrl-C` (or `SIGTERM`). The collector closes its sockets, drains
in-flight events, writes a final full snapshot chunk, and exits `0`.

`stdout` carries **binary SBE only**. Every status line, counter and error goes
to `stderr`, so `collector | your_reader` is safe.

## CLI

```text
-o, --output <PATH>              Output path; omit or `-` for stdout
    --tokens-per-socket <N>      Symbols per connection [default: 500]
    --redundancy <N>             1 = single connection, 2 = Conn A + Conn B [default: 2]
    --race-connections <N>       Parallel TLS+WS candidates raced per dial [default: 4]
    --rotate-minutes <N>         Base connection lifetime; 0 disables [default: 35]
    --poll-interval-secs <N>     REST discovery interval [default: 30]
    --snapshot-interval-secs <N> Periodic full snapshot interval [default: 60]
    --max-tokens <N>             Cap total symbols (smoke tests only)
    --startup-tokens <N>         Symbols before first output (0 = all) [default: 0]
    --benchmark-latency          Run latency experiments instead of collecting
```

**`--poll-interval-secs`:** with CLOB discovery a full refresh takes about five
seconds, so 60–300 s is comfortable and the 30 s default is fine for small runs.
Only the Gamma fallback needs ~900 s, because its walk takes ten minutes or more.

## Architecture

```text
CLOB REST ──> PolymarketCodec ──> ShardManager ──> socket workers (A/B per shard)
                                                          │
                                                          ▼
                                             mpsc<EngineMessage>
                                                          │
                                                          ▼
                                    Consumer (single owner of books + writer)
                                                          │
                                                          ▼
                                              AlignedSbeWriter -> stdout/file
```

- **One consumer owns all state.** Books and the SBE writer live in a single task
  fed by an mpsc channel. WebSocket tasks never touch book state, so a
  half-updated book cannot be serialised.
- **Each shard runs two isolated books per symbol** (`Primary` and `Standby`).
  Events are tagged with slot + connection generation; retired generations are
  dropped.
- **Failover emits a reconciliation snapshot.** Switching slots always writes a
  full snapshot of the newly active book before any of its deltas, because the
  two feeds can disagree after a dropped message.
- **Rotation is make-before-break.** The replacement connects and warms up while
  the old socket keeps streaming; only then is the old one closed. The two
  connections of a shard are staggered by half a lifetime.

## Correctness invariants

1. **No floating point on prices.** Exchange decimal strings are parsed straight
   into `i64` scaled by `1e6` (`parse_scaled_1e6`). `"0.29"` becomes `290_000`
   exactly; digits past the sixth decimal are truncated, never rounded.
2. **Replay equivalence.** For a symbol, applying every emitted
   `PriceLevelUpdateDomain` in order onto the preceding `SnapshotDomain` must
   reproduce the next snapshot. Deltas always carry the *post-update absolute*
   level size, so `0` means "level deleted".
3. **Never emit deltas for an unhydrated book.** A delta is only written after a
   snapshot established the book; otherwise a partial state would be published.
4. **A resync is not a bug.** At a failover, rotation, or a post-reconnect venue
   `book`, the new snapshot is authoritative and may legitimately differ from the
   replayed state. That is why a snapshot is emitted on switch.

`examples/inspect` verifies (1)–(4) against a capture:

```bash
cargo run --release --example inspect -- capture.sbe
```

It replays the file, reports snapshot/delta counts, counts **resync points**
(expected at failovers) and fails only on structural corruption: negative sizes
or a crossed book.

## Venues

| Venue | Status |
| --- | --- |
| Polymarket | Primary. Gamma keyset discovery + CLOB WebSocket decoding, fully implemented and tested. |
| Kalshi | Compatibility proof for the generic codec layer. Decoding, MVE filtering and subscription framing are unit-tested; the signed handshake is intentionally not implemented, so it cannot be run without credentials. |

Decoding is the only exchange-specific layer: `VenueCodec`. Everything else —
sharding, redundancy, failover, snapshots, SBE output — is shared.

## Testing

```bash
cargo test                 # 26 tests (24 unit + 2 integration)
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

Coverage includes: exact `1e6` parsing and its rejection cases, Gamma
`clobTokenIds` in both wire shapes plus V2 `positionIds`, the CLOB market-index
page shape, single/batched frame decoding, level
insertion/modification/deletion, the SBE replay invariant over 20 deltas,
generation filtering, failover with a reconciliation snapshot, activity-tier
classification, and an end-to-end test that runs a local WebSocket server through
the real worker and consumer.

## Latency

`--benchmark-latency` runs four experiments and prints Markdown tables (to
stdout, so they can be pasted into a report). Findings, including a negative
result for socket tuning on this network, are in
[LATENCY_DISCOVERIES.md](LATENCY_DISCOVERIES.md).

## Known limitations

- **Failover and watchdog thresholds were tuned on measured churn**, not on the numbers in the original outline: the failover lag is 2 s (not 150 ms) and `Active` means ≥30 messages per five minutes. A 10,000-symbol soak showed the original values produced ~31,800 failovers in three minutes.
- **`--tokens-per-socket` above ~750 silently loses coverage.** The venue accepts an oversized subscription and serves only a handful of assets with no error (1,000/socket → 2 of 2,000 hydrated). 500 is the default and has hydrated 100% in every run. The collector warns if fewer than half the requested symbols produce data.
- **Discovery uses the CLOB's own market index** (`clob.polymarket.com/sampling-simplified-markets`), with Gamma's keyset endpoint only as a fallback. Measured: **8,749 live markets / ~17.5k tokens in ~5 s** via CLOB versus **>8 minutes** via Gamma. `--poll-interval-secs` therefore does not need to exceed a ten-minute walk.
- **Verified coverage**: the complete live universe (17,488 symbols) across 35 shards / 70 sockets, 100% hydration, 1.27M deltas, 0 dropped frames, 0 negative levels, 0 crossed books, clean SIGINT.
- **Failover churn is bounded, not eliminated**: 8,326 switches in a 200 s complete-venue soak (~0.0024 per symbol per second), each of which writes a resync snapshot. A 30 s dwell and a 5 s silence gate keep it from oscillating, but raising the missed-update threshold further would reduce it at the cost of reacting later to a genuinely stalled feed.
- **Gamma's 209,604-token figure overstates the live universe.** That walk counts every open candidate and player-prop market, including thousands that never trade. The CLOB's active index reports what is actually accepting orders.
- **Gamma has two outcome-identifier shapes.** V2 markets expose `positionIds` (array); legacy markets expose `clobTokenIds` (array or JSON-encoded string). Both are handled; reading only the latter silently drops every V2 market.
- **`--max-tokens` is a debugging aid**, not a production setting.
- **Kalshi cannot be run** in this build (no request signing).
- Timestamps come from the system clock; a drifting host clock skews latency
  measurements.

## Design notes

The phase plan, protocol details, invariants and every implementation decision
are recorded in [SPEC.md](SPEC.md), including the sections on failover,
watchdog thresholds and the benchmark methodology.
