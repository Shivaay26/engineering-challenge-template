//! Shard assignment and redundant connection management.
//!
//! Symbols are packed into shards of at most `tokens_per_socket`. Each shard
//! runs `redundancy` connections (Conn A = `Primary`, Conn B = `Standby`) that
//! receive identical subscriptions, so one connection stalling never stops the
//! data flow. Both feed the single consumer, which decides which one is
//! authoritative.
//!
//! The manager is a clonable handle: the REST reconciler and the consumer's
//! watchdog both act through it without owning it.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};

use crate::{
    codec::{ConnSlot, VenueCodec},
    engine::{
        EngineMessage,
        consumer::MaintenanceSink,
        socket::{self, SocketCommand},
    },
    net::NetConfig,
};

const COMMAND_CHANNEL_CAPACITY: usize = 64;

struct SlotHandle {
    shard_id: usize,
    slot: ConnSlot,
    commands: mpsc::Sender<SocketCommand>,
}

struct Inner {
    slots: Vec<SlotHandle>,
    /// Symbols assigned per shard, used for least-loaded placement.
    counts: Vec<usize>,
    assignment: HashMap<String, usize>,
    max_symbols: Option<usize>,
}

impl Inner {
    fn total(&self) -> usize {
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

    fn senders_for(&self, shard: usize) -> Vec<mpsc::Sender<SocketCommand>> {
        self.slots
            .iter()
            .filter(|handle| handle.shard_id == shard)
            .map(|handle| handle.commands.clone())
            .collect()
    }
}

/// Owns shard assignment and the command channels of every worker.
#[derive(Clone)]
pub struct ShardManager {
    inner: Arc<Mutex<Inner>>,
}

impl ShardManager {
    /// Packs `symbols` into shards and spawns `redundancy` workers per shard.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        codec: Arc<dyn VenueCodec>,
        symbols: &[String],
        tokens_per_socket: usize,
        redundancy: usize,
        rotation: Duration,
        net: NetConfig,
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
                let net = net.clone();

                workers.spawn(async move {
                    socket::run(
                        shard_id, slot, codec, url, symbols, command_rx, events, shutdown,
                        rotation, net,
                    )
                    .await;
                });
            }
        }

        Self {
            inner: Arc::new(Mutex::new(Inner {
                slots,
                counts,
                assignment,
                max_symbols: None,
            })),
        }
    }

    pub fn with_max_symbols(self, max_symbols: Option<usize>) -> Self {
        if let Ok(mut inner) = self.inner.lock() {
            inner.max_symbols = max_symbols;
        }
        self
    }

    pub fn shard_count(&self) -> usize {
        self.inner
            .lock()
            .map(|inner| inner.counts.len())
            .unwrap_or(0)
    }

    pub fn connection_count(&self) -> usize {
        self.inner
            .lock()
            .map(|inner| inner.slots.len())
            .unwrap_or(0)
    }

    pub fn total_symbols(&self) -> usize {
        self.inner.lock().map(|inner| inner.total()).unwrap_or(0)
    }

    /// The configured symbol cap, so the reconciler can bound its own scan.
    pub fn max_symbols(&self) -> Option<usize> {
        self.inner.lock().ok().and_then(|inner| inner.max_symbols)
    }

    /// Subscribes to new symbols on every connection of their shard.
    pub async fn add(&self, symbols: &[String]) {
        let batches = {
            let Ok(mut inner) = self.inner.lock() else {
                return;
            };

            let mut by_shard: HashMap<usize, Vec<String>> = HashMap::new();
            for symbol in symbols {
                if inner.assignment.contains_key(symbol) {
                    continue;
                }
                if let Some(max) = inner.max_symbols
                    && inner.total() >= max
                {
                    break;
                }

                let shard = inner.least_loaded();
                inner.assignment.insert(symbol.clone(), shard);
                inner.counts[shard] += 1;
                by_shard.entry(shard).or_default().push(symbol.clone());
            }

            by_shard
                .into_iter()
                .map(|(shard, batch)| (batch, inner.senders_for(shard)))
                .collect::<Vec<_>>()
        };

        for (batch, senders) in batches {
            let count = batch.len();
            for sender in senders {
                if let Err(error) = sender.send(SocketCommand::Subscribe(batch.clone())).await {
                    eprintln!(
                        "[shards] dropped {count} subscription(s) for a dead connection: {error}"
                    );
                }
            }
        }
    }

    /// Unsubscribes symbols on every connection of their shard.
    pub async fn remove(&self, symbols: &[String]) {
        let batches = {
            let Ok(mut inner) = self.inner.lock() else {
                return;
            };

            let mut by_shard: HashMap<usize, Vec<String>> = HashMap::new();
            for symbol in symbols {
                let Some(shard) = inner.assignment.remove(symbol) else {
                    continue;
                };
                inner.counts[shard] = inner.counts[shard].saturating_sub(1);
                by_shard.entry(shard).or_default().push(symbol.clone());
            }

            by_shard
                .into_iter()
                .map(|(shard, batch)| (batch, inner.senders_for(shard)))
                .collect::<Vec<_>>()
        };

        for (batch, senders) in batches {
            for sender in senders {
                if let Err(error) = sender.send(SocketCommand::Unsubscribe(batch.clone())).await {
                    eprintln!("[shards] dropped an unsubscribe for a dead connection: {error}");
                }
            }
        }
    }
}

impl MaintenanceSink for ShardManager {
    fn resubscribe(&self, shard_id: usize, slot: ConnSlot, symbol: &str) {
        let sender = self.inner.lock().ok().and_then(|inner| {
            inner
                .slots
                .iter()
                .find(|handle| handle.shard_id == shard_id && handle.slot == slot)
                .map(|handle| handle.commands.clone())
        });

        let Some(sender) = sender else {
            return;
        };

        if let Err(error) = sender.try_send(SocketCommand::ResubscribeToken(symbol.to_owned())) {
            eprintln!("[watchdog] could not queue resubscribe for {symbol}: {error}");
        }
    }
}
