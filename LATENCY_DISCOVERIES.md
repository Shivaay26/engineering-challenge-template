# Latency Discoveries

Working notes for Extension 4 (latency improvements). This file grows as the
implementation lands: Phase 7 records the techniques and their rationale, Phase
8 adds measurements from `--benchmark-latency`, Phase 9 consolidates the
results.

**Status legend**

- **[IMPLEMENTED]** — in the code, exercised by tests or live runs.
- **[MEASURED]** — backed by numbers in this file.
- **[PENDING]** — designed but not yet measured.

---

## 1. TCP socket tuning — [IMPLEMENTED]

Implemented in `src/net/tuned_ws.rs`. Sockets are built with `socket2` and handed
to Tokio via `TcpSocket::from_std_stream`, so the options are applied before the
TLS handshake and therefore before any venue traffic.

| Option | Value | Why | Cost |
| --- | --- | --- | --- |
| `TCP_NODELAY` | on | Nagle's algorithm can delay a small write by up to one RTT (tens of ms on a WAN). Our writes are small control frames (`PING`, subscribe), which is exactly the bad case. | More packets, marginally more overhead. |
| `SO_RCVBUF` | 2 MiB | A burst (news event, market open) that overflows the receive buffer causes kernel drops, and recovery means a retransmit — hundreds of ms. 2 MiB of buffer is cheap. | Memory per socket: ~150 sockets × 2 MiB ≈ 300 MiB at full scale. |
| `TCP_QUICKACK` | on (Linux) | Suppresses the delayed-ACK timer (up to 40 ms) on inbound frames. | None measurable. |
| Source-port hint | optional | Two connections from the same host can hash to the same NIC receive queue. Explicit local ports force distinct 4-tuples. | Requires knowing a free port. |

Caveats worth recording:

- `TCP_QUICKACK` is documented as *not sticky* on some kernels: the ACK mode can
  revert after each ACK, so a production collector may need to re-arm it per
  readable event. Our implementation is best-effort and single-shot.
- On WSL2 the guest's socket options only affect the guest side of the NAT.
  The ISP and host path dominate end-to-end latency, so gains measured from
  inside WSL2 will understate what the same code does on a bare-metal host.
- `SO_RCVBUF` double-counts in the kernel (`net.core.rmem_max` clamping may
  reduce the effective value), so the requested 2 MiB is a ceiling, not a
  guarantee.

## 2. N-way connection racing ("edge lottery") — [IMPLEMENTED]

Implemented in `src/net/tuned_ws.rs`, enabled by `--race-connections N`
(default 4, range 1–16; `1` disables racing).

Method:

1. Resolve the venue host and dial `N` candidates in parallel, each with the
   tuned socket options and a full TLS + WebSocket handshake.
2. Send 10 `PING` frames spaced 50 ms apart on each candidate and measure each
   `PONG` round trip.
3. Rank by **p50** RTT, tie-break by **p99**.
4. Keep the winner; close the rest.

Rationale: Polymarket's front door is Cloudflare, which answers from many edge
workers. Round-trip times to different edges differ by tens of milliseconds, and
which edge you land on is effectively a lottery at dial time. Racing converts
that lottery into a measurement.

Costs and caveats:

- `N` parallel TLS handshakes per connection attempt. At startup only, and once
  more per rotation or reconnect, so the amortised cost is negligible.
- With 10 samples, p99 is simply the maximum and p50 is coarse; the probe count
  is a deliberate compromise between ranking quality and ~0.6 s of warm-up.
- `PING`/`PONG` measures the control path on the same socket, which is a good
  proxy for the data path but not identical: a server that deprioritises
  control frames under load could be under-ranked.

### First measurement — [MEASURED, but not a benchmark]

Two connections (redundancy 2), each racing 4 candidates, on a mobile hotspot:

| Connection | Winner p50 | Winner p99 |
| --- | --- | --- |
| shard 0 `Primary` | 343 ms | 464 ms |
| shard 0 `Standby` | 345 ms | 450 ms |

Read this as "the race runs and the spread is big enough to matter", not as a
latency result: it is one run, over cellular, with 10 samples (so p99 is simply
the maximum). Phase 8 Experiment A replaces it with 12 sockets × 30 probes on a
stable network.

## 3. Shard density — [MEASURED]

Density is a latency lever, not just a capacity knob: fewer connections means
fewer TLS sessions, fewer buffers, and less per-socket state for the exchange
to keep hot.

Measurements from live discovery and a live subscription run:

- A keyset walk on WiFi scanned **700 pages (~70,000 markets) in eight minutes and
  was still climbing**, having counted **139,604 unique tokens**. Every figure
  here is a lower bound: the walk was stopped, not finished. The cursor chain is
  sequential (the next page requires the previous page's cursor), so it cannot be
  parallelised, and a page costs roughly 0.7 s.
- Each market contributes two outcome tokens, hence tokens ≈ 2 × markets.
- A 1,000-symbol run with `--tokens-per-socket 500` subscribed **500 assets on a
  single connection without error**, and all 1,000 symbols hydrated across two
  sockets (4,004 snapshots, 15,588 deltas, 66,569 resting levels).

Consequences:

- At 500 tokens/socket, ~140k tokens implies **~280 shards**, or **~560 sockets**
  with redundancy 2. At the original 150/socket it would have been ~930 sockets.
  This is the real scale of the base challenge, and it is the strongest argument
  for both high density and a liquidity filter (see below).
- Full discovery takes **ten minutes or more**, which is why
  `--poll-interval-secs` must be well above the walk duration (900 s or more). A
  30 s or 300 s poll interval would mean the reconciler never pauses between
  walks.
- Gamma's keyset endpoint supports `liquidity_num_min` / `volume_num_min`. Those
  filters are the practical way to cut a 140k-token universe down to the markets
  that actually trade; the collector does not apply them today, because the
  challenge asks for all live symbols.

## 4. Empirical results — [MEASURED]

`--benchmark-latency` runs four experiments and prints Markdown tables to
stdout. Results below are from a **mobile-hotspot connection to WSL2**, which is
the honest label for everything in this section: absolute values are dominated
by the cellular path, so read the *shape* and the *deltas*, not the absolute
milliseconds.

### Experiment A — connection racing variance

12 sockets requested, 6–8 survived the network resets, 30 PING probes each.

| Metric | min | median | max | spread |
| --- | --- | --- | --- | --- |
| socket p50 | 332.1 ms | 338.0 ms | 344.0 ms | **11.9 ms** |
| socket p99 | 343.4 ms | 348.0 ms | 353.7 ms | **10.3 ms** |

Across three runs the p50 spread was 6.2 ms, 18.8 ms and 11.9 ms. So the Edge
Lottery is real but modest at the median: a few milliseconds, not tens. The p99
spread was as high as 480 ms on one run, which is the argument for tie-breaking
on p99 rather than trusting p50 alone.

### Experiment B — TCP socket tuning — **no benefit detected**

Both arms sample the same 50 symbols over the same wall-clock window. Sequential
arms were tried first and rejected: they produced 168 vs 2,162 samples because
the link shifted between arms, which would have manufactured a fake 3x win.

| Configuration | samples | p50 | p95 | p99 | p99.9 |
| --- | --- | --- | --- | --- | --- |
| default OS | 174 | 99.7 ms | 675.8 ms | 787.7 ms | 787.7 ms |
| `TCP_NODELAY` + 2 MiB `RCVBUF` + `QUICKACK` | 154 | 112.7 ms | 673.0 ms | 805.0 ms | 805.0 ms |

**Conclusion: no effect measurable in this environment.** The tuned arm is
marginally worse, which is noise at these sample counts. The plausible reasons:

- WSL2 NAT terminates the guest-side socket; the host and cellular stack decide
  the behaviour we actually observe.
- `QUICKACK` matters at sub-millisecond scale. Against a ~330 ms RTT the delayed
  ACK timer is invisible.
- `NODELAY` only helps many small writes; our write rate is a 10 s `PING`.

This is worth keeping in the code — it costs nothing and should help on a
low-RTT, bare-metal host — but the claim "tuning makes it faster" is **not
supported by these measurements** and is not made here.

### Experiment C — shard density sweep

| symbols/socket | time to last snapshot | delta p50 | delta p99 | samples |
| --- | --- | --- | --- | --- |
| 25 | 932.8 ms | 163.9 ms | 277.3 ms | 1,190 |
| 100 | 1,292.0 ms | 53.7 ms | 1,041.0 ms | 932 |
| 250 | 1,923.8 ms | 165.0 ms | 1,004.7 ms | 964 |
| 500 | 2,206.2 ms | 77.7 ms | 1,609.1 ms | 1,470 |

Time-to-last-snapshot grows close to linearly with density (933 ms at 25 →
2,206 ms at 500). Per-symbol delta latency does **not** show a clean trend: p50
is non-monotonic and p99 is noisy, so this run does not support a claim that
higher density costs per-message latency. What it does establish is warm-up
cost: at 500/socket a shard needs ~2.2 s before every symbol is hydrated, which
is the number the rotation warm-up budget (5 s) was chosen against.

### Experiment D — parser and runtime overhead

| Workload | ns/frame | frames/sec |
| --- | --- | --- |
| `book` frame, 50 levels/side | 64,000–67,600 | ~15,000 |
| `price_change` frame, 5 changes | 6,300–14,500 | ~69,000–158,000 |
| `parse_scaled_1e6("0.4812")` | 22–31 ns | ~32–45M |

At 71k symbols with per-symbol update rates well under 1/s, decoding is not the
bottleneck: even 15k book frames/sec is far above what the feed delivers.

## 5. Decoder: the double-parse tax — [MEASURED]

The Polymarket frame decoder originally used `#[serde(untagged)] enum RawWsFrame
{ Batch(Vec<Value>), Single(Value) }`. Serde tries untagged variants **in
order**, so every single-object frame was parsed twice: first as `Vec<Value>`
(failing), then again as `Value`.

Measured cost and result of dispatching on the first non-whitespace byte
instead:

| Frame | before | after | change |
| --- | --- | --- | --- |
| `book`, 50 levels/side | 115 µs | 64–68 µs | **~43% faster** |
| `price_change`, 5 changes | 11.7 µs | 6.3–14.5 µs | ~45% faster best case, noisy |

This is the one latency result with a clear causal attribution: same payloads,
same machine, same measurement harness, one duplicate parse removed. It also
applies to *every* frame the collector ever handles, whereas the socket tuning
above is unproven here.

## 6. Soak findings — [MEASURED]

A 10,000-symbol run (20 shards × 2 connections, rotation enabled, three minutes)
produced 37 successful rotations, zero dropped frames and a clean SIGINT, but
also **31,844 failovers and 7,115 watchdog resubscribes**.

That is the most useful result in this file, because it is a defect that small
runs hid: every failover writes a resync snapshot, so the capture was dominated
by them and the delta stream was degraded. Two thresholds were wrong:

| Knob | Was | Now | Why |
| --- | --- | --- | --- |
| Failover lag | 150 ms | **2 s** | At scale the two feeds are routinely >150 ms out of phase on low-rate symbols; that is propagation, not a stall. The pair oscillated, switching on every late arrival. |
| Missed updates | 2 | **10** | With 2, the 2 s gap alone still fired ~18,800 times: most markets update minutes apart, so one in-flight message already looks like a long gap. |
| Failover dwell | none | **30 s** | Stops the pair alternating on every subsequent message after a switch. |
| `Active` tier | ≥6 msgs / 5 min | **≥30** | One update per 50 s classified most of the venue as active, so ~7,100 symbols were "zombies". |
| Zombie silence | 30 s | **60 s** | Matched to the stricter tier, so a quiet market is not churned. |

The lesson generalises: thresholds that look reasonable on 40 symbols are
dominated by low-rate symbols at 10,000, because most of the venue updates on the
order of a minute, not a second.

Progression, all on the same 10,000-symbol / 40-socket soak:

| Pass | Failovers | Watchdog resubscribes |
| --- | --- | --- |
| 150 ms lag, ≥2 missed, ≥6 msgs/5 min | 31,844 | 7,115 |
| 2 s lag, ≥30 msgs/5 min, 60 s silence | 18,804 | 287 |
| + ≥10 missed updates and a 30 s dwell | **5,979** | **244** |

Zero dropped frames and a clean SIGINT in every pass, so the improvement is in
arbitration quality rather than in throughput.

## 7. Environment caveats for measurement

- The development host is WSL2 behind NAT; socket options apply guest-side only.
- The mobile-hotspot path used during development **resets roughly 80% of new TLS
  connections** to Gamma/CloudFront (observed repeatedly: `Connection reset by
  peer (os error 104)`). That is why startup discovery retries and why the
  numbers in this file are structural rather than latency figures.
- Any published latency numbers should be labelled with the network they were
  measured on; a campus network or a VPN will dominate every code-level effect.
