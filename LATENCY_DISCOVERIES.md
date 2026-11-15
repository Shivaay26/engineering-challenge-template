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

- Gamma's keyset walk reached **39,500 active markets / 78,804 tokens** before
  the walk was cut off by its own time limit, so those are **lower bounds**.
- Each market contributes two outcome tokens, hence tokens ≈ 2 × markets.
- A 1,000-symbol run with `--tokens-per-socket 500` subscribed **500 assets on a
  single connection without error**, and all 1,000 symbols hydrated across two
  sockets (4,004 snapshots, 15,588 deltas, 66,569 resting levels).

Consequence: at 500 tokens/socket, ~79k tokens implies roughly **143 sockets**,
versus ~526 at the original 150/socket assumption. That is why the default was
raised to 500.

## 4. Benchmark suite — [PENDING]

`--benchmark-latency` (Phase 8) will run four experiments against live
Polymarket and print Markdown tables:

| Experiment | Question | Metric |
| --- | --- | --- |
| A — Connection racing variance | How much RTT spread is there across 12 parallel sockets? | min / median / max of each socket's p50 and p99 |
| B — Socket tuning | Does tuned TCP beat defaults? | one-way `received_at − exchange_timestamp`, p50/p95/p99/p99.9 over ~2,000 deltas |
| C — Shard density sweep | 25 vs 100 vs 250 vs 500 tokens per socket | time-to-last-snapshot on subscribe, p99 delta latency |
| D — Parser overhead | How much of the wire-to-decision path is ours? | ns/frame for `serde_json` + `parse_scaled_1e6` on captured payloads |

## 5. Environment caveats for measurement

- The development host is WSL2 behind NAT; socket options apply guest-side only.
- The mobile-hotspot path used during development **resets roughly 80% of new TLS
  connections** to Gamma/CloudFront (observed repeatedly: `Connection reset by
  peer (os error 104)`). That is why startup discovery retries and why the
  numbers in this file are structural rather than latency figures.
- Any published latency numbers should be labelled with the network they were
  measured on; a campus network or a VPN will dominate every code-level effect.
