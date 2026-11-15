//! The single consumer: the only task that owns order book state and the SBE
//! writer.
//!
//! WebSocket workers never touch book state; they only forward decoded events
//! over an mpsc channel. Keeping every write in one place makes it impossible
//! to serialise a half-updated book, and it is what makes cross-connection
//! failover safe: switching the active slot and emitting the reconciliation
//! snapshot happen inside the same actor.

use std::{
    collections::{HashMap, VecDeque},
    io::Write,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};

use crate::{
    codec::{ConnSlot, NormalizedEvent, StampedEnvelope},
    engine::{EngineMessage, orderbook::OrderBook},
    util::AlignedSbeWriter,
};

/// A standby connection must be ahead by more than this to be preferred.
///
/// The original 150 ms came from the challenge text, but measurement at 10,000
/// symbols produced ~31,800 failovers in three minutes: at that scale the two
/// feeds are routinely more than 150 ms out of phase on low-rate symbols, so the
/// pair traded places continuously and every switch forced a resync snapshot.
/// 2 s is hysteresis: a genuinely stalled feed accumulates far more than that,
/// while ordinary inter-feed phase does not.
const FAILOVER_LAG_MS: i64 = 2_000;
/// ...and it must have delivered at least this many updates that the active
/// connection never showed. Time alone is ambiguous: a quiet market is silent on
/// both feeds, so a later arrival on the standby is not evidence of lag.
///
/// The value is 10, not 2: at 10,000 symbols a threshold of 2 fired ~18,800
/// times in three minutes, because most markets update minutes apart and a
/// single in-flight message already looks like a large timestamp gap.
const FAILOVER_MISSED_UPDATES: u32 = 10;
/// After a switch, ignore further lag-based switches for this long. Without a
/// dwell the pair can alternate on every subsequent message.
const FAILOVER_DWELL: Duration = Duration::from_secs(30);

/// Rolling window used to classify a symbol's activity tier.
const ACTIVITY_WINDOW: std::time::Duration = std::time::Duration::from_secs(300);
/// `>= 30` messages in five minutes means the symbol should be updating often.
/// The original 6 (one per 50 s) classified most of the venue as active and
/// produced ~7,100 resubscribes in three minutes across 10,000 symbols.
const ACTIVE_THRESHOLD: usize = 30;
/// An active symbol with no message for this long is treated as a zombie.
const ZOMBIE_SILENCE: std::time::Duration = std::time::Duration::from_secs(60);
/// Never churn the same symbol's subscription more often than this.
const RESUBSCRIBE_COOLDOWN: std::time::Duration = std::time::Duration::from_secs(60);
/// Bound per-symbol memory on hyperactive markets.
const MAX_ROLLING_SAMPLES: usize = 512;

/// Something that can act on a watchdog decision.
pub trait MaintenanceSink: Send + Sync {
    /// Unsubscribe then resubscribe `symbol` on one connection.
    fn resubscribe(&self, shard_id: usize, slot: ConnSlot, symbol: &str);
}

fn slot_index(slot: ConnSlot) -> usize {
    match slot {
        ConnSlot::Primary => 0,
        ConnSlot::Standby => 1,
    }
}

fn slot_from_index(index: usize) -> ConnSlot {
    if index == 0 {
        ConnSlot::Primary
    } else {
        ConnSlot::Standby
    }
}

/// Per-symbol state: two completely isolated books, one per connection.
#[derive(Debug)]
struct SymbolState {
    shard_id: usize,
    books: [OrderBook; 2],
    /// Index of the book currently used for output.
    active: usize,
    last_exchange_ts: [Option<DateTime<Utc>>; 2],
    last_recv: [Option<std::time::Instant>; 2],
    /// A connection dropped and has not produced an event since.
    down: [bool; 2],
    emitted_initial: bool,
    /// Events seen on the non-active slot since the active one last spoke.
    missed: u32,
    /// Arrival times inside the rolling activity window.
    rolling: VecDeque<Instant>,
    last_resubscribe_attempt: Option<Instant>,
    last_failover: Option<Instant>,
}

impl SymbolState {
    fn new(shard_id: usize) -> Self {
        Self {
            shard_id,
            books: [OrderBook::default(), OrderBook::default()],
            active: 0,
            last_exchange_ts: [None, None],
            last_recv: [None, None],
            down: [false, false],
            emitted_initial: false,
            missed: 0,
            rolling: VecDeque::new(),
            last_resubscribe_attempt: None,
            last_failover: None,
        }
    }

    /// Records one arrival for the activity watchdog.
    fn record(&mut self, at: Instant) {
        self.rolling.push_back(at);
        if self.rolling.len() > MAX_ROLLING_SAMPLES {
            self.rolling.pop_front();
        }
    }
}

/// Applies normalized events to books and writes SBE messages.
pub struct Consumer<W: Write> {
    writer: AlignedSbeWriter<W>,
    symbols: HashMap<String, SymbolState>,
    /// Highest generation accepted per `(shard, slot)`.
    generations: HashMap<(usize, usize), u64>,
    maintenance: Option<Arc<dyn MaintenanceSink>>,
}

impl<W: Write> Consumer<W> {
    pub fn new(writer: AlignedSbeWriter<W>) -> Self {
        Self {
            writer,
            symbols: HashMap::new(),
            generations: HashMap::new(),
            maintenance: None,
        }
    }

    /// Installs the sink used to act on watchdog decisions.
    pub fn set_maintenance(&mut self, sink: Arc<dyn MaintenanceSink>) {
        self.maintenance = Some(sink);
    }

    /// Handles one message from a worker.
    pub fn apply(&mut self, message: EngineMessage) -> anyhow::Result<()> {
        match message {
            EngineMessage::Event(envelope) => self.apply_event(envelope),
            EngineMessage::Disconnected {
                shard_id,
                slot,
                generation,
            } => {
                if !self.accept_generation(shard_id, slot, generation) {
                    return Ok(());
                }
                let index = slot_index(slot);
                for state in self.symbols.values_mut() {
                    if state.shard_id == shard_id {
                        state.down[index] = true;
                    }
                }
                Ok(())
            }
        }
    }

    /// Drops events from a connection generation that has been superseded.
    fn accept_generation(&mut self, shard_id: usize, slot: ConnSlot, generation: u64) -> bool {
        let seen = self
            .generations
            .entry((shard_id, slot_index(slot)))
            .or_insert(generation);

        if generation < *seen {
            false
        } else {
            *seen = generation;
            true
        }
    }

    fn apply_event(&mut self, envelope: StampedEnvelope) -> anyhow::Result<()> {
        if !self.accept_generation(envelope.shard_id, envelope.slot, envelope.generation) {
            return Ok(());
        }
        let index = slot_index(envelope.slot);

        match envelope.event {
            NormalizedEvent::Snapshot {
                symbol,
                exchange_ts,
                received_at,
                bids,
                asks,
            } => {
                let state = self
                    .symbols
                    .entry(symbol.clone())
                    .or_insert_with(|| SymbolState::new(envelope.shard_id));

                state.down[index] = false;
                state.last_recv[index] = Some(std::time::Instant::now());
                state.record(Instant::now());
                if index == state.active {
                    state.missed = 0;
                } else {
                    state.missed = state.missed.saturating_add(1);
                }
                state.books[index].apply_snapshot(&bids, &asks);
                state.last_exchange_ts[index] = Some(exchange_ts);

                if index == state.active {
                    let snapshot =
                        state.books[index].to_snapshot_domain(&symbol, received_at, exchange_ts);
                    state.emitted_initial = true;
                    self.writer.write_msg(&snapshot)?;
                } else {
                    Self::maybe_failover(&mut self.writer, &symbol, state, received_at)?;
                }
            }
            NormalizedEvent::Delta {
                symbol,
                exchange_ts,
                received_at,
                side,
                price_level,
                is_absolute,
                quantity,
            } => {
                let Some(state) = self.symbols.get_mut(&symbol) else {
                    return Ok(());
                };

                state.down[index] = false;
                state.last_recv[index] = Some(std::time::Instant::now());
                state.record(Instant::now());
                if index == state.active {
                    state.missed = 0;
                } else {
                    state.missed = state.missed.saturating_add(1);
                }

                // Never build a book from deltas alone.
                if !state.books[index].is_hydrated() {
                    return Ok(());
                }

                state.books[index].apply_delta(side, price_level, quantity, is_absolute);
                state.last_exchange_ts[index] = Some(exchange_ts);

                if index != state.active {
                    // The triggering delta is folded into the reconciliation
                    // snapshot, so it must not also be emitted.
                    Self::maybe_failover(&mut self.writer, &symbol, state, received_at)?;
                    return Ok(());
                }

                if !state.emitted_initial {
                    return Ok(());
                }

                let delta = state.books[index].to_delta_domain(
                    &symbol,
                    received_at,
                    exchange_ts,
                    side,
                    price_level,
                );
                self.writer.write_msg(&delta)?;
            }
            NormalizedEvent::MarketLifecycle { symbol, is_active } => {
                eprintln!(
                    "market {symbol} is now {}",
                    if is_active { "active" } else { "inactive" }
                );
            }
        }

        Ok(())
    }

    /// Switches the active slot if the standby is healthy and the active is not.
    ///
    /// On switch, a full snapshot of the newly active book is written *before*
    /// any of its deltas, because the two books can disagree after a dropped
    /// message.
    fn maybe_failover(
        writer: &mut AlignedSbeWriter<W>,
        symbol: &str,
        state: &mut SymbolState,
        now: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let active = state.active;
        let other = 1 - active;

        if !state.books[other].is_hydrated() {
            return Ok(());
        }

        let active_hydrated = state.books[active].is_hydrated();
        let lagging = state.missed >= FAILOVER_MISSED_UPDATES
            && match (
                state.last_exchange_ts[other],
                state.last_exchange_ts[active],
            ) {
                (Some(new), Some(old)) => (new - old).num_milliseconds() > FAILOVER_LAG_MS,
                (Some(_), None) => true,
                _ => false,
            };
        let dwell_ok = state
            .last_failover
            .map(|at| at.elapsed() >= FAILOVER_DWELL)
            .unwrap_or(true);
        let should_switch = !active_hydrated || state.down[active] || (lagging && dwell_ok);

        if !should_switch {
            return Ok(());
        }

        state.active = other;
        state.last_failover = Some(Instant::now());
        let exchange_ts = state.last_exchange_ts[other].unwrap_or(now);
        let snapshot = state.books[other].to_snapshot_domain(symbol, now, exchange_ts);
        state.emitted_initial = true;

        eprintln!(
            "[failover] {symbol}: {:?} -> {:?}",
            slot_from_index(active),
            slot_from_index(other)
        );
        writer.write_msg(&snapshot)?;

        Ok(())
    }

    /// Writes a full snapshot for every hydated active book.
    pub fn write_periodic_snapshots(&mut self, now: DateTime<Utc>) -> anyhow::Result<()> {
        let mut snapshots = Vec::with_capacity(self.symbols.len());
        for (symbol, state) in &self.symbols {
            let index = state.active;
            if !state.books[index].is_hydrated() {
                continue;
            }
            let exchange_ts = state.last_exchange_ts[index].unwrap_or(now);
            snapshots.push(state.books[index].to_snapshot_domain(symbol, now, exchange_ts));
        }

        for snapshot in &snapshots {
            self.writer.write_msg(snapshot)?;
        }

        Ok(())
    }

    /// Classifies each symbol's activity tier and asks for a single-connection
    /// unsubscribe/resubscribe when an *active* symbol has gone silent.
    ///
    /// Quiet symbols are deliberately left alone: low activity is legitimate and
    /// churn would only add risk. `now` is injected so tests can advance time.
    pub fn run_watchdog(&mut self, now: Instant) {
        let mut actions = Vec::new();

        for (symbol, state) in &mut self.symbols {
            while let Some(front) = state.rolling.front() {
                if now.saturating_duration_since(*front) > ACTIVITY_WINDOW {
                    state.rolling.pop_front();
                } else {
                    break;
                }
            }

            if state.rolling.len() < ACTIVE_THRESHOLD {
                continue;
            }

            let active = state.active;
            let silent = state.last_recv[active]
                .map(|at| now.saturating_duration_since(at) > ZOMBIE_SILENCE)
                .unwrap_or(true);
            let cooled_down = state
                .last_resubscribe_attempt
                .map(|at| now.saturating_duration_since(at) > RESUBSCRIBE_COOLDOWN)
                .unwrap_or(true);

            if silent && cooled_down {
                state.last_resubscribe_attempt = Some(now);
                actions.push((state.shard_id, slot_from_index(active), symbol.clone()));
            }
        }

        let Some(sink) = &self.maintenance else {
            return;
        };

        for (shard_id, slot, symbol) in actions {
            eprintln!("[watchdog] {symbol} stalled on shard {shard_id} {slot:?}, resubscribing");
            sink.resubscribe(shard_id, slot, &symbol);
        }
    }

    /// Flushes the final chunk. Call once, on shutdown.
    pub fn flush(&mut self) -> anyhow::Result<()> {
        self.writer.flush()
    }

    /// Number of symbols currently tracked, hydrated or not. Test/diagnostics.
    pub fn tracked_symbols(&self) -> usize {
        self.symbols.len()
    }

    /// Which slot is currently driving output for `symbol`.
    pub fn active_slot(&self, symbol: &str) -> Option<ConnSlot> {
        self.symbols
            .get(symbol)
            .map(|state| slot_from_index(state.active))
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;

    use super::*;
    use crate::{
        Side,
        util::{
            AlignedSbeReader, OrderbookSchema,
            dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
        },
    };

    /// The reader only accepts whole chunks, so tests must flush 64 MiB.
    const CHUNK: usize = 64 << 20;

    fn stamp(index: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap()
            + Duration::milliseconds(index)
    }

    fn envelope(slot: ConnSlot, generation: u64, event: NormalizedEvent) -> EngineMessage {
        EngineMessage::Event(StampedEnvelope {
            shard_id: 0,
            slot,
            generation,
            event,
        })
    }

    fn snapshot(
        slot: ConnSlot,
        at: DateTime<Utc>,
        bids: &[(i64, i64)],
        asks: &[(i64, i64)],
    ) -> EngineMessage {
        envelope(
            slot,
            1,
            NormalizedEvent::Snapshot {
                symbol: "TOKEN".to_owned(),
                exchange_ts: at,
                received_at: at,
                bids: bids.to_vec(),
                asks: asks.to_vec(),
            },
        )
    }

    fn delta(
        slot: ConnSlot,
        at: DateTime<Utc>,
        side: Side,
        price: i64,
        quantity: i64,
    ) -> EngineMessage {
        envelope(
            slot,
            1,
            NormalizedEvent::Delta {
                symbol: "TOKEN".to_owned(),
                exchange_ts: at,
                received_at: at,
                side,
                price_level: price,
                is_absolute: true,
                quantity,
            },
        )
    }

    fn read_all(path: &std::path::Path) -> anyhow::Result<Vec<SnapshotDomain>> {
        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path)?;
        let mut snapshots = Vec::new();
        while let Some(message) = reader.read_msg()? {
            if let AnyMessage::Snapshot(decoded) = message {
                snapshots.push(SnapshotDomain::try_from_decoder(decoded)?);
            }
        }
        Ok(snapshots)
    }

    #[test]
    fn deltas_before_a_snapshot_are_dropped() -> anyhow::Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let writer = AlignedSbeWriter::create(file.path(), CHUNK)?;
        let mut consumer = Consumer::new(writer);

        consumer.apply(delta(
            ConnSlot::Primary,
            stamp(0),
            Side::Buy,
            480_000,
            1_000_000,
        ))?;
        consumer.flush()?;

        assert_eq!(consumer.tracked_symbols(), 0);
        assert_eq!(std::fs::metadata(file.path())?.len(), 0);

        Ok(())
    }

    #[test]
    fn stale_generations_are_ignored() -> anyhow::Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let writer = AlignedSbeWriter::create(file.path(), CHUNK)?;
        let mut consumer = Consumer::new(writer);

        // Generation 2 hydrates the book.
        consumer.apply(envelope(
            ConnSlot::Primary,
            2,
            NormalizedEvent::Snapshot {
                symbol: "TOKEN".to_owned(),
                exchange_ts: stamp(0),
                received_at: stamp(0),
                bids: vec![(480_000, 1_000_000)],
                asks: vec![],
            },
        ))?;

        // Generation 1 is a retired socket; its delta must not be written.
        consumer.apply(envelope(
            ConnSlot::Primary,
            1,
            NormalizedEvent::Delta {
                symbol: "TOKEN".to_owned(),
                exchange_ts: stamp(1),
                received_at: stamp(1),
                side: Side::Buy,
                price_level: 480_000,
                is_absolute: true,
                quantity: 9_000_000,
            },
        ))?;
        consumer.flush()?;

        let snapshots = read_all(file.path())?;
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].bids[0].quantity, 1_000_000);

        Ok(())
    }

    #[test]
    fn test_failover_emits_reconciliation_snapshot() -> anyhow::Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let writer = AlignedSbeWriter::create(file.path(), CHUNK)?;
        let mut consumer = Consumer::new(writer);

        let t0 = stamp(0);

        // Both connections open with the same book.
        let initial_bids = [(480_000, 30_000_000), (470_000, 12_000_000)];
        let initial_asks = [(520_000, 25_000_000)];
        consumer.apply(snapshot(
            ConnSlot::Primary,
            t0,
            &initial_bids,
            &initial_asks,
        ))?;
        consumer.apply(snapshot(
            ConnSlot::Standby,
            t0,
            &initial_bids,
            &initial_asks,
        ))?;
        assert_eq!(consumer.active_slot("TOKEN"), Some(ConnSlot::Primary));

        // Conn A misses the deletion of 470_000 and nine size updates.
        // Conn B sees them, but nine missed messages are not yet enough.
        consumer.apply(delta(
            ConnSlot::Standby,
            t0 + Duration::milliseconds(100),
            Side::Buy,
            470_000,
            0,
        ))?;

        for step in 2..=8 {
            consumer.apply(delta(
                ConnSlot::Standby,
                t0 + Duration::milliseconds(300 * i64::from(step)),
                Side::Buy,
                480_000,
                10_000_000 + i64::from(step),
            ))?;
        }

        assert_eq!(
            consumer.active_slot("TOKEN"),
            Some(ConnSlot::Primary),
            "must not fail over below the missed-update threshold"
        );

        // The tenth missed update tips it over: failover plus reconciliation.
        consumer.apply(delta(
            ConnSlot::Standby,
            t0 + Duration::milliseconds(3_000),
            Side::Buy,
            480_000,
            20_000_000,
        ))?;
        assert_eq!(consumer.active_slot("TOKEN"), Some(ConnSlot::Standby));

        // A subsequent delta from the new active slot is emitted normally.
        consumer.apply(delta(
            ConnSlot::Standby,
            t0 + Duration::milliseconds(3_010),
            Side::Sell,
            520_000,
            22_000_000,
        ))?;
        consumer.flush()?;

        // Stream: A's snapshot, B's reconciliation snapshot, then the delta.
        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file.path())?;
        let mut snapshots = Vec::new();
        let mut deltas = Vec::new();
        while let Some(message) = reader.read_msg()? {
            match message {
                AnyMessage::Snapshot(decoded) => {
                    snapshots.push(SnapshotDomain::try_from_decoder(decoded)?)
                }
                AnyMessage::PriceLevelUpdate(decoded) => {
                    deltas.push(PriceLevelUpdateDomain::try_from_decoder(decoded)?)
                }
                _ => {}
            }
        }

        assert_eq!(snapshots.len(), 2, "A initial + B reconciliation");
        assert_eq!(deltas.len(), 1, "only the post-failover delta");

        let reconciliation = &snapshots[1];
        assert!(
            !reconciliation
                .bids
                .iter()
                .any(|level| level.price_level == 470_000),
            "reconciliation snapshot must reflect B's deletion"
        );
        assert_eq!(reconciliation.bids.len(), 1);
        assert_eq!(reconciliation.bids[0].quantity, 20_000_000);

        assert_eq!(deltas[0].side, Side::Sell);
        assert_eq!(deltas[0].price_level, 520_000);
        assert_eq!(deltas[0].quantity, 22_000_000);

        Ok(())
    }

    #[test]
    fn test_activity_tier_classification() -> anyhow::Result<()> {
        #[derive(Default)]
        struct RecordingSink(std::sync::Mutex<Vec<(usize, ConnSlot, String)>>);

        impl MaintenanceSink for RecordingSink {
            fn resubscribe(&self, shard_id: usize, slot: ConnSlot, symbol: &str) {
                self.0
                    .lock()
                    .unwrap()
                    .push((shard_id, slot, symbol.to_owned()));
            }
        }

        let file = tempfile::NamedTempFile::new()?;
        let writer = AlignedSbeWriter::create(file.path(), CHUNK)?;
        let mut consumer = Consumer::new(writer);
        let sink = Arc::new(RecordingSink::default());
        consumer.set_maintenance(sink.clone());

        // Active tier: one snapshot plus 29 deltas = 30 arrivals in the window.
        consumer.apply(snapshot(
            ConnSlot::Primary,
            stamp(0),
            &[(480_000, 1_000_000)],
            &[],
        ))?;
        for index in 1..=29 {
            consumer.apply(delta(
                ConnSlot::Primary,
                stamp(index),
                Side::Buy,
                480_000,
                1_000_000 + index,
            ))?;
        }

        let now = Instant::now();
        consumer.run_watchdog(now);
        assert!(
            sink.0.lock().unwrap().is_empty(),
            "freshly updated symbol must not be churned"
        );

        consumer.run_watchdog(now + std::time::Duration::from_secs(31));
        assert!(
            sink.0.lock().unwrap().is_empty(),
            "31s of silence is below the 60s zombie threshold"
        );

        consumer.run_watchdog(now + std::time::Duration::from_secs(61));
        let actions = sink.0.lock().unwrap().clone();
        assert_eq!(actions.len(), 1, "one resubscribe after 60s of silence");
        assert_eq!(actions[0], (0, ConnSlot::Primary, "TOKEN".to_owned()));

        consumer.run_watchdog(now + std::time::Duration::from_secs(62));
        assert_eq!(
            sink.0.lock().unwrap().len(),
            1,
            "cooldown must suppress a second attempt"
        );

        consumer.run_watchdog(now + std::time::Duration::from_secs(130));
        assert_eq!(
            sink.0.lock().unwrap().len(),
            2,
            "cooldown expired, so it may try again"
        );

        // Quiet tier: 29 arrivals in five minutes is below the threshold.
        let file = tempfile::NamedTempFile::new()?;
        let writer = AlignedSbeWriter::create(file.path(), CHUNK)?;
        let mut quiet = Consumer::new(writer);
        let quiet_sink = Arc::new(RecordingSink::default());
        quiet.set_maintenance(quiet_sink.clone());

        quiet.apply(snapshot(
            ConnSlot::Primary,
            stamp(0),
            &[(480_000, 1_000_000)],
            &[],
        ))?;
        for index in 1..=28 {
            quiet.apply(delta(
                ConnSlot::Primary,
                stamp(index),
                Side::Buy,
                480_000,
                1_000_000 + index,
            ))?;
        }
        quiet.run_watchdog(Instant::now() + std::time::Duration::from_secs(120));
        assert!(
            quiet_sink.0.lock().unwrap().is_empty(),
            "quiet symbols must not be unsubscribed"
        );

        Ok(())
    }

    #[test]
    fn test_sbe_replay_invariant_roundtrip() -> anyhow::Result<()> {
        let symbol = "TOKEN";
        let mut book = OrderBook::default();
        book.apply_snapshot(
            &[
                (480_000, 30_000_000),
                (470_000, 12_000_000),
                (460_000, 5_000_000),
            ],
            &[(520_000, 25_000_000), (530_000, 9_000_000)],
        );

        let start = stamp(0);
        let first = book.to_snapshot_domain(symbol, start, start);

        let operations: [(Side, i64, i64, bool); 20] = [
            (Side::Buy, 485_000, 4_000_000, true),
            (Side::Buy, 480_000, 31_000_000, true),
            (Side::Sell, 520_000, 0, true),
            (Side::Sell, 515_000, 6_000_000, true),
            (Side::Buy, 470_000, 0, true),
            (Side::Buy, 490_000, 2_500_000, true),
            (Side::Sell, 530_000, 11_000_000, true),
            (Side::Buy, 460_000, 4_000_000, false),
            (Side::Sell, 540_000, 3_000_000, true),
            (Side::Buy, 495_000, 1_000_000, true),
            (Side::Sell, 515_000, 0, true),
            (Side::Buy, 485_000, 4_500_000, true),
            (Side::Sell, 525_000, 8_000_000, true),
            (Side::Buy, 475_000, 7_500_000, true),
            (Side::Buy, 490_000, 0, true),
            (Side::Sell, 530_000, 0, true),
            (Side::Buy, 465_000, 3_000_000, true),
            (Side::Sell, 535_000, 2_000_000, true),
            (Side::Buy, 460_000, 1_000_000, false),
            (Side::Sell, 545_000, 1_500_000, true),
        ];

        let file = tempfile::NamedTempFile::new()?;
        let last_stamp = stamp(operations.len() as i64 + 1);
        let final_snapshot = {
            let mut writer = AlignedSbeWriter::create(file.path(), CHUNK)?;
            writer.write_msg(&first)?;

            for (index, (side, price, quantity, is_absolute)) in operations.iter().enumerate() {
                let at = stamp(index as i64 + 1);
                book.apply_delta(*side, *price, *quantity, *is_absolute);
                let delta = book.to_delta_domain(symbol, at, at, *side, *price);
                writer.write_msg(&delta)?;
            }

            let final_snapshot = book.to_snapshot_domain(symbol, last_stamp, last_stamp);
            writer.write_msg(&final_snapshot)?;
            writer.flush()?;
            final_snapshot
        };

        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file.path())?;

        let Some(AnyMessage::Snapshot(decoded)) = reader.read_msg()? else {
            anyhow::bail!("expected the initial snapshot");
        };
        let initial = SnapshotDomain::try_from_decoder(decoded)?;
        assert_eq!(initial, first);

        let mut replayed = OrderBook::default();
        replayed.apply_snapshot(
            &initial
                .bids
                .iter()
                .map(|level| (level.price_level, level.quantity))
                .collect::<Vec<_>>(),
            &initial
                .asks
                .iter()
                .map(|level| (level.price_level, level.quantity))
                .collect::<Vec<_>>(),
        );

        let mut replayed_deltas = 0;
        let mut recorded_final = None;
        while let Some(message) = reader.read_msg()? {
            match message {
                AnyMessage::PriceLevelUpdate(decoded) => {
                    let delta = PriceLevelUpdateDomain::try_from_decoder(decoded)?;
                    replayed.apply_delta(delta.side, delta.price_level, delta.quantity, true);
                    replayed_deltas += 1;
                }
                AnyMessage::Snapshot(decoded) => {
                    recorded_final = Some(SnapshotDomain::try_from_decoder(decoded)?);
                    break;
                }
                AnyMessage::Unknown { .. } => {
                    anyhow::bail!("unexpected SBE template in the recorded stream")
                }
            }
        }

        assert_eq!(replayed_deltas, operations.len());
        assert_eq!(recorded_final, Some(final_snapshot.clone()));
        assert_eq!(
            replayed.to_snapshot_domain(symbol, last_stamp, last_stamp),
            final_snapshot
        );

        Ok(())
    }
}
