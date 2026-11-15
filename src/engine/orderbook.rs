//! In-memory order book for a single symbol.
//!
//! Prices and quantities are `i64` scaled by `1e6`; no floating point is used
//! anywhere. Both sides are `BTreeMap<price, size>`, so the best bid and best
//! ask are the map ends and snapshot construction needs no sorting.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};

use crate::{
    Side,
    util::dtos::challenge_orderbook::{
        PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
    },
};

/// One symbol's aggregated order book.
#[derive(Debug, Default, Clone)]
pub struct OrderBook {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    hydrated: bool,
}

impl OrderBook {
    /// Whether a snapshot has been applied. Deltas must not be emitted before
    /// this is true, and must never be applied before it either.
    pub fn is_hydrated(&self) -> bool {
        self.hydrated
    }

    /// Replaces the book with a full snapshot. Levels with a non-positive size
    /// are dropped, since exchanges occasionally include empty levels.
    pub fn apply_snapshot(&mut self, bids: &[(i64, i64)], asks: &[(i64, i64)]) {
        self.bids.clear();
        self.asks.clear();

        for &(price, quantity) in bids {
            if quantity > 0 {
                self.bids.insert(price, quantity);
            }
        }
        for &(price, quantity) in asks {
            if quantity > 0 {
                self.asks.insert(price, quantity);
            }
        }

        self.hydrated = true;
    }

    /// Applies a single level update.
    ///
    /// `is_absolute` distinguishes Polymarket (the value is the new level size)
    /// from Kalshi (the value is a relative change to add). A resulting
    /// non-positive size removes the level from the book.
    pub fn apply_delta(&mut self, side: Side, price: i64, quantity: i64, is_absolute: bool) {
        let levels = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };

        let new_quantity = if is_absolute {
            quantity
        } else {
            levels.get(&price).copied().unwrap_or(0) + quantity
        };

        if new_quantity > 0 {
            levels.insert(price, new_quantity);
        } else {
            levels.remove(&price);
        }
    }

    /// Current resting size at `price` on `side`, or `0` if the level is gone.
    ///
    /// This is the post-update quantity written to `PriceLevelUpdateDomain`, so
    /// replaying the delta stream always lands on the same book as a snapshot.
    pub fn quantity_at(&self, side: Side, price: i64) -> i64 {
        match side {
            Side::Buy => self.bids.get(&price).copied().unwrap_or(0),
            Side::Sell => self.asks.get(&price).copied().unwrap_or(0),
        }
    }

    /// Bids as `(price, size)`, best bid first.
    pub fn bids(&self) -> impl DoubleEndedIterator<Item = (i64, i64)> + '_ {
        self.bids.iter().rev().map(|(&price, &size)| (price, size))
    }

    /// Asks as `(price, size)`, best ask first.
    pub fn asks(&self) -> impl Iterator<Item = (i64, i64)> + '_ {
        self.asks.iter().map(|(&price, &size)| (price, size))
    }

    /// Builds the SBE snapshot DTO for this symbol.
    pub fn to_snapshot_domain(
        &self,
        symbol: &str,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
    ) -> SnapshotDomain {
        SnapshotDomain {
            received_at,
            exchange_timestamp,
            bids: self
                .bids()
                .map(|(price_level, quantity)| SnapshotBidsEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            asks: self
                .asks()
                .map(|(price_level, quantity)| SnapshotAsksEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            symbol: symbol.to_owned(),
        }
    }

    /// Builds the SBE delta DTO carrying the post-update level size.
    pub fn to_delta_domain(
        &self,
        symbol: &str,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
        side: Side,
        price_level: i64,
    ) -> PriceLevelUpdateDomain {
        PriceLevelUpdateDomain {
            backtest_timestamp: received_at,
            exchange_timestamp,
            price_level,
            quantity: self.quantity_at(side, price_level),
            side,
            symbol: symbol.to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absolute_deltas_replace_and_zero_deletes() {
        let mut book = OrderBook::default();
        assert!(!book.is_hydrated());
        assert_eq!(book.quantity_at(Side::Buy, 480_000), 0);

        book.apply_snapshot(&[(480_000, 30_000_000)], &[(520_000, 25_000_000)]);
        assert!(book.is_hydrated());

        // Absolute: sets the new size outright.
        book.apply_delta(Side::Buy, 480_000, 5_000_000, true);
        assert_eq!(book.quantity_at(Side::Buy, 480_000), 5_000_000);

        // A new level is inserted.
        book.apply_delta(Side::Sell, 530_000, 7_000_000, true);
        assert_eq!(book.quantity_at(Side::Sell, 530_000), 7_000_000);

        // Zero removes the level.
        book.apply_delta(Side::Buy, 480_000, 0, true);
        assert_eq!(book.quantity_at(Side::Buy, 480_000), 0);
        assert_eq!(book.bids().count(), 0);

        assert_eq!(
            book.asks().collect::<Vec<_>>(),
            vec![(520_000, 25_000_000), (530_000, 7_000_000)]
        );
    }

    #[test]
    fn relative_deltas_accumulate_and_clamp_to_zero() {
        let mut book = OrderBook::default();
        book.apply_snapshot(&[(480_000, 30_000_000)], &[]);

        book.apply_delta(Side::Buy, 480_000, -10_000_000, false);
        assert_eq!(book.quantity_at(Side::Buy, 480_000), 20_000_000);

        // Overshooting a delete removes the level rather than going negative.
        book.apply_delta(Side::Buy, 480_000, -25_000_000, false);
        assert_eq!(book.quantity_at(Side::Buy, 480_000), 0);
    }

    #[test]
    fn snapshot_sorts_best_first_and_round_trips() {
        let mut book = OrderBook::default();
        book.apply_snapshot(
            &[
                (460_000, 1_000_000),
                (480_000, 30_000_000),
                (470_000, 2_000_000),
            ],
            &[(540_000, 9_000_000), (520_000, 25_000_000)],
        );

        assert_eq!(
            book.bids().collect::<Vec<_>>(),
            vec![
                (480_000, 30_000_000),
                (470_000, 2_000_000),
                (460_000, 1_000_000)
            ]
        );
        assert_eq!(
            book.asks().collect::<Vec<_>>(),
            vec![(520_000, 25_000_000), (540_000, 9_000_000)]
        );

        // A snapshot must not grow the book across repeated applications.
        let before = book.bids().count();
        book.apply_snapshot(&[(480_000, 30_000_000)], &[]);
        assert_eq!(book.bids().count(), 1);
        assert!(book.bids().count() <= before);
    }
}
