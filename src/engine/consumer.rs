//! The single consumer: the only task that owns order book state and the SBE
//! writer.
//!
//! WebSocket workers never touch book state; they only forward
//! [`StampedEnvelope`]s over an mpsc channel. Keeping every write in one place
//! makes it impossible to serialise a half-updated book, and avoids any
//! `Arc<Mutex<OrderBook>>` between producers.

use std::{collections::HashMap, io::Write};

use chrono::{DateTime, Utc};

use crate::{
    codec::{NormalizedEvent, StampedEnvelope},
    engine::orderbook::OrderBook,
    util::AlignedSbeWriter,
};

/// Per-symbol state held by the consumer.
#[derive(Debug, Default)]
struct TrackedSymbol {
    book: OrderBook,
    /// Most recent exchange timestamp seen, used as the snapshot's
    /// `exchange_timestamp` so periodic snapshots do not lie about venue time.
    last_exchange_ts: Option<DateTime<Utc>>,
}

/// Applies normalized events to books and writes SBE messages.
pub struct Consumer<W: Write> {
    writer: AlignedSbeWriter<W>,
    books: HashMap<String, TrackedSymbol>,
}

impl<W: Write> Consumer<W> {
    pub fn new(writer: AlignedSbeWriter<W>) -> Self {
        Self {
            writer,
            books: HashMap::new(),
        }
    }

    /// Applies one event and writes the corresponding SBE message.
    pub fn apply(&mut self, envelope: StampedEnvelope) -> anyhow::Result<()> {
        match envelope.event {
            NormalizedEvent::Snapshot {
                symbol,
                exchange_ts,
                received_at,
                bids,
                asks,
            } => {
                let tracked = self.books.entry(symbol.clone()).or_default();
                tracked.book.apply_snapshot(&bids, &asks);
                tracked.last_exchange_ts = Some(exchange_ts);

                let snapshot = tracked
                    .book
                    .to_snapshot_domain(&symbol, received_at, exchange_ts);
                self.writer.write_msg(&snapshot)?;
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
                // Deltas for a symbol we never snapshotted are dropped: applying
                // them would build a book from a partial state.
                let Some(tracked) = self.books.get_mut(&symbol) else {
                    return Ok(());
                };
                if !tracked.book.is_hydrated() {
                    return Ok(());
                }

                tracked
                    .book
                    .apply_delta(side, price_level, quantity, is_absolute);
                tracked.last_exchange_ts = Some(exchange_ts);

                let delta = tracked.book.to_delta_domain(
                    &symbol,
                    received_at,
                    exchange_ts,
                    side,
                    price_level,
                );
                self.writer.write_msg(&delta)?;
            }
            NormalizedEvent::MarketLifecycle { symbol, is_active } => {
                // Subscription changes are driven by the REST reconciler; this is
                // just an early hint, so log it for operators.
                eprintln!(
                    "market {symbol} is now {}",
                    if is_active { "active" } else { "inactive" }
                );
            }
        }

        Ok(())
    }

    /// Writes a full snapshot for every hydrated book.
    pub fn write_periodic_snapshots(&mut self, now: DateTime<Utc>) -> anyhow::Result<()> {
        let mut snapshots = Vec::with_capacity(self.books.len());
        for (symbol, tracked) in &self.books {
            if !tracked.book.is_hydrated() {
                continue;
            }
            let exchange_ts = tracked.last_exchange_ts.unwrap_or(now);
            snapshots.push(tracked.book.to_snapshot_domain(symbol, now, exchange_ts));
        }

        for snapshot in &snapshots {
            self.writer.write_msg(snapshot)?;
        }

        Ok(())
    }

    /// Flushes the final chunk. Call once, on shutdown.
    pub fn flush(&mut self) -> anyhow::Result<()> {
        self.writer.flush()
    }

    /// Number of symbols currently tracked, hydrated or not. Test/diagnostics.
    pub fn tracked_symbols(&self) -> usize {
        self.books.len()
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;

    use super::*;
    use crate::{
        Side,
        codec::ConnSlot,
        util::{
            AlignedSbeReader, OrderbookSchema,
            dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
        },
    };

    /// The reader only accepts whole chunks, so tests must flush 64 MiB.
    const CHUNK: usize = 64 << 20;

    fn stamp(index: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap()
            + Duration::microseconds(index)
    }

    fn envelope(event: NormalizedEvent) -> StampedEnvelope {
        StampedEnvelope {
            shard_id: 0,
            slot: ConnSlot::Primary,
            generation: 0,
            event,
        }
    }

    #[test]
    fn deltas_before_a_snapshot_are_dropped() -> anyhow::Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let writer = AlignedSbeWriter::create(file.path(), CHUNK)?;
        let mut consumer = Consumer::new(writer);

        consumer.apply(envelope(NormalizedEvent::Delta {
            symbol: "TOKEN".to_owned(),
            exchange_ts: stamp(0),
            received_at: stamp(0),
            side: Side::Buy,
            price_level: 480_000,
            is_absolute: true,
            quantity: 1_000_000,
        }))?;
        consumer.flush()?;

        // Nothing was hydrated, so no book was created and no bytes were written.
        assert_eq!(consumer.tracked_symbols(), 0);
        assert_eq!(std::fs::metadata(file.path())?.len(), 0);

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

        // Insertions, size modifications, deletions, and a relative delta, so
        // both update styles are replayed.
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

        // Replay the recorded stream and assert it converges on the same book.
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
                    // Persisted quantities are always post-update absolute sizes.
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
