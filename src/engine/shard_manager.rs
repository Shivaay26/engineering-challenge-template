//! Shard assignment and redundant connection management.
//!
//! Symbols are packed into shards of at most `tokens_per_socket`. Each shard
//! runs `redundancy` connections (Conn A = `Primary`, Conn B = `Standby`) that
//! receive identical subscriptions, so one connection stalling never stops the
//! data flow. Both feed the single consumer, which decides which one is
//! authoritative.

use std::{collections::HashMap, sync::Arc};

use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};

use crate::{
    codec::{ConnSlot, VenueCodec},
    engine::{
        EngineMessage,
        socket::{self, SocketCommand},
    },
};

const COMMAND_CHANNEL_CAPACITY: usize = 64;

struct SlotHandle {
    shard_id: usize,
    slot: ConnSlot,
    commands: mpsc::Sender<SocketCommand>,
}

/// Owns shard assignment and the command channels of every worker.
pub struct ShardManager {
    slots: Vec<SlotHandle>,
    /// Symbols assigned per shard, used for least-loaded placement.
    counts: Vec<usize>,
    assignment: HashMap<String, usize>,
    max_symbols: Option<usize>,
}

impl ShardManager {
    /// Packs `symbols` into shards and spawns `redundancy` workers per shard.
    pub fn spawn(
        codec: Arc<dyn VenueCodec>,
        symbols: &[String],
        tokens_per_socket: usize,
        redundancy: usize,
        events: mpsc::Sender<EngineMessage>,
        shutdown: watch::Receiver<bool>,
        workers: &mut JoinSet<()>,
    ) -> Self {
        let per_socket = tokens_per_socket.max(1);
        let redundancy = redundancy.clamp(1, 2);
        let url = codec.ws_url().to_owned();

        let mut slots = Vec::new();
        let mut counts = Vec::new();
        let mut assignment = HashMap::with_capacity(symbols.len());

        for (shard_id, chunk) in symbols.chunks(per_socket).enumerate() {
            counts.push(chunk.len());
            for symbol in chunk {
                assignment.insert(symbol.clone(), shard_id);
            }

            for slot in [ConnSlot::Primary, ConnSlot::Standby]
                .into_iter()
                .take(redundancy)
            {
                let (command_tx, command_rx) = mpsc::channel(COMMAND_CHANNEL_CAPACITY);
                slots.push(SlotHandle {
                    shard_id,
                    slot,
                    commands: command_tx,
                });

                let codec = Arc::clone(&codec);
                let url = url.clone();
                let events = events.clone();
                let shutdown = shutdown.clone();
                let symbols = chunk.to_vec();

                workers.spawn(async move {
                    socket::run(
                        shard_id, slot, codec, url, symbols, command_rx, events, shutdown,
                    )
                    .await;
                });
            }
        }

        Self {
            slots,
            counts,
            assignment,
            max_symbols: None,
        }
    }

    pub fn with_max_symbols(mut self, max_symbols: Option<usize>) -> Self {
        self.max_symbols = max_symbols;
        self
    }

    pub fn shard_count(&self) -> usize {
        self.counts.len()
    }

    pub fn connection_count(&self) -> usize {
        self.slots.len()
    }

    pub fn total_symbols(&self) -> usize {
        self.counts.iter().sum()
    }

    fn least_loaded(&self) -> usize {
        self.counts
            .iter()
            .enumerate()
            .min_by_key(|(_, count)| **count)
            .map(|(shard, _)| shard)
            .unwrap_or(0)
    }

    /// Subscribes to new symbols on every connection of their shard.
    pub async fn add(&mut self, symbols: &[String]) {
        let mut by_shard: HashMap<usize, Vec<String>> = HashMap::new();

        for symbol in symbols {
            if self.assignment.contains_key(symbol) {
                continue;
            }
            if let Some(max) = self.max_symbols
                && self.total_symbols() >= max
            {
                break;
            }

            let shard = self.least_loaded();
            self.assignment.insert(symbol.clone(), shard);
            self.counts[shard] += 1;
            by_shard.entry(shard).or_default().push(symbol.clone());
        }

        for (shard, batch) in by_shard {
            let count = batch.len();
            for handle in self.slots.iter().filter(|handle| handle.shard_id == shard) {
                if let Err(error) = handle
                    .commands
                    .send(SocketCommand::Subscribe(batch.clone()))
                    .await
                {
                    eprintln!(
                        "[shards] shard {shard} {:?} is gone, dropped {count} subscription(s): {error}",
                        handle.slot
                    );
                }
            }
        }
    }

    /// Unsubscribes symbols on every connection of their shard.
    pub async fn remove(&mut self, symbols: &[String]) {
        let mut by_shard: HashMap<usize, Vec<String>> = HashMap::new();

        for symbol in symbols {
            let Some(shard) = self.assignment.remove(symbol) else {
                continue;
            };
            self.counts[shard] = self.counts[shard].saturating_sub(1);
            by_shard.entry(shard).or_default().push(symbol.clone());
        }

        for (shard, batch) in by_shard {
            for handle in self.slots.iter().filter(|handle| handle.shard_id == shard) {
                if let Err(error) = handle
                    .commands
                    .send(SocketCommand::Unsubscribe(batch.clone()))
                    .await
                {
                    eprintln!("[shards] shard {shard} {:?} is gone: {error}", handle.slot);
                }
            }
        }
    }
}
