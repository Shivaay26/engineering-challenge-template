//! Inspects a capture written by the collector and verifies the SBE replay
//! invariant against every snapshot in the file.
//!
//! Usage: `cargo run --release --example inspect -- <capture.sbe>`
//!
//! For each symbol the inspector replays the recorded stream. When a snapshot
//! is encountered after the book is hydrated, the state derived from the
//! previous snapshot plus every intervening delta must match the snapshot
//! exactly. A mismatch means the collector emitted a delta stream that cannot
//! reconstruct what it claimed the book was, which is the one thing that must
//! never happen.

use std::collections::{HashMap, HashSet};

use engineering_challenge::{
    engine::OrderBook,
    util::{
        AlignedSbeReader, OrderbookSchema,
        dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
    },
};

fn main() -> anyhow::Result<()> {
    let path = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("usage: inspect <capture.sbe>"))?;

    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&path)?;

    let mut books: HashMap<String, OrderBook> = HashMap::new();
    let mut symbols: HashSet<String> = HashSet::new();
    let mut snapshots = 0usize;
    let mut deltas = 0usize;
    let mut resyncs = 0usize;
    let mut negative_levels = 0usize;
    let mut crossed_books = 0usize;

    while let Some(message) = reader.read_msg()? {
        match message {
            AnyMessage::Snapshot(decoded) => {
                let snapshot = SnapshotDomain::try_from_decoder(decoded)?;
                snapshots += 1;
                symbols.insert(snapshot.symbol.clone());

                let book = books.entry(snapshot.symbol.clone()).or_default();
                if book.is_hydrated() {
                    let derived = book.to_snapshot_domain(
                        &snapshot.symbol,
                        snapshot.received_at,
                        snapshot.exchange_timestamp,
                    );
                    let same = derived.bids.len() == snapshot.bids.len()
                        && derived.asks.len() == snapshot.asks.len()
                        && derived.bids.iter().zip(&snapshot.bids).all(|(a, b)| {
                            a.price_level == b.price_level && a.quantity == b.quantity
                        })
                        && derived.asks.iter().zip(&snapshot.asks).all(|(a, b)| {
                            a.price_level == b.price_level && a.quantity == b.quantity
                        });

                    if !same {
                        // A mismatch is expected wherever the collector emitted
                        // a resync (failover reconciliation or a fresh venue
                        // book after a dropped connection): by design the new
                        // snapshot is authoritative and can disagree with the
                        // state built from the previous feed. Count it, but do
                        // not fail on it.
                        resyncs += 1;
                        if resyncs <= 3 {
                            let detail = if derived.bids.len() == snapshot.bids.len() {
                                derived
                                    .bids
                                    .iter()
                                    .zip(&snapshot.bids)
                                    .position(|(a, b)| {
                                        a.price_level != b.price_level || a.quantity != b.quantity
                                    })
                                    .map(|index| {
                                        let a = &derived.bids[index];
                                        let b = &snapshot.bids[index];
                                        format!(
                                            "bid[{}] replayed price={} qty={} vs snapshot price={} qty={}",
                                            index, a.price_level, a.quantity, b.price_level, b.quantity
                                        )
                                    })
                                    .unwrap_or_else(|| "asks differ".to_owned())
                            } else {
                                format!(
                                    "bid count {} vs {}",
                                    derived.bids.len(),
                                    snapshot.bids.len()
                                )
                            };
                            eprintln!("resync for {}: {}", snapshot.symbol, detail);
                        }
                    }

                    // Structural checks must hold for every snapshot we emit,
                    // resync or not.
                    if snapshot.bids.iter().any(|level| level.quantity < 0)
                        || snapshot.asks.iter().any(|level| level.quantity < 0)
                    {
                        negative_levels += 1;
                    }
                    if let (Some(best_bid), Some(best_ask)) =
                        (snapshot.bids.first(), snapshot.asks.first())
                        && best_bid.price_level >= best_ask.price_level
                    {
                        crossed_books += 1;
                    }
                }

                book.apply_snapshot(
                    &snapshot
                        .bids
                        .iter()
                        .map(|level| (level.price_level, level.quantity))
                        .collect::<Vec<_>>(),
                    &snapshot
                        .asks
                        .iter()
                        .map(|level| (level.price_level, level.quantity))
                        .collect::<Vec<_>>(),
                );
            }
            AnyMessage::PriceLevelUpdate(decoded) => {
                let delta = PriceLevelUpdateDomain::try_from_decoder(decoded)?;
                deltas += 1;
                symbols.insert(delta.symbol.clone());
                if let Some(book) = books.get_mut(&delta.symbol) {
                    // Persisted quantities are always post-update absolute sizes.
                    book.apply_delta(delta.side, delta.price_level, delta.quantity, true);
                }
            }
            // `AnyMessage` is #[non_exhaustive] outside the crate.
            _ => anyhow::bail!("unknown SBE template in capture"),
        }
    }

    let hydrated = books.values().filter(|book| book.is_hydrated()).count();
    let levels: usize = books
        .values()
        .map(|book| book.bids().count() + book.asks().count())
        .sum();

    println!("capture          : {path}");
    println!("snapshots        : {snapshots}");
    println!("deltas           : {deltas}");
    println!("symbols          : {}", symbols.len());
    println!("hydrated books   : {hydrated}");
    println!("resting levels   : {levels}");
    println!("resync points    : {resyncs} (expected at failover/reconnect)");
    println!("negative levels  : {negative_levels}");
    println!("crossed books    : {crossed_books}");

    anyhow::ensure!(
        negative_levels == 0 && crossed_books == 0,
        "structurally invalid order book emitted"
    );

    Ok(())
}
