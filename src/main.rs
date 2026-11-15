//! Collector entry point: discover markets over REST, stream them over sharded
//! WebSockets, and write SBE snapshots and deltas to stdout or a file.
//!
//! `stdout` carries binary SBE bytes only. Every status line goes to `stderr`
//! via `eprintln!`, so the output can be piped straight into a replay tool.

use std::{
    collections::{HashMap, HashSet},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use chrono::Utc;
use clap::Parser;
use engineering_challenge::{
    codec::{StampedEnvelope, VenueCodec},
    engine::{
        Consumer,
        socket::{self, SocketCommand},
    },
    util::AlignedSbeWriter,
    venues::PolymarketCodec,
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
    time,
};

/// Large chunks when spooling to a file: fewer, bigger writes.
const FILE_CHUNK: usize = 64 << 20;
/// Small chunks to stdout so a pipe consumer sees data promptly.
const STDOUT_CHUNK: usize = 1 << 20;
const EVENT_CHANNEL_CAPACITY: usize = 8_192;
const COMMAND_CHANNEL_CAPACITY: usize = 64;
/// Venues front their REST API with CDNs that occasionally hand out an edge IP
/// which resets connections, so startup discovery is retried.
const DISCOVERY_ATTEMPTS: u32 = 5;

#[derive(Debug, Parser)]
#[command(
    name = "voluntas-collector",
    bin_name = "voluntas-collector",
    version,
    about = "Collect Polymarket order book data as aligned SBE"
)]
struct Cli {
    /// Output file path. Omit, or pass `-`, to stream SBE to stdout.
    #[arg(short, long, value_name = "PATH")]
    output: Option<PathBuf>,

    /// Maximum symbols subscribed per WebSocket connection.
    #[arg(long, default_value_t = 150)]
    tokens_per_socket: usize,

    /// REST market discovery interval, in seconds.
    #[arg(long, default_value_t = 30)]
    poll_interval_secs: u64,

    /// Periodic full-snapshot interval, in seconds.
    #[arg(long, default_value_t = 60)]
    snapshot_interval_secs: u64,

    /// Optional cap on total symbols, for smoke tests.
    #[arg(long)]
    max_tokens: Option<usize>,
}

/// Tracks which shard owns each symbol and routes subscription changes.
struct Router {
    command_txs: Vec<mpsc::Sender<SocketCommand>>,
    counts: Vec<usize>,
    assignment: HashMap<String, usize>,
    max_symbols: Option<usize>,
}

impl Router {
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

    async fn add(&mut self, symbols: &[String]) {
        let mut by_shard: HashMap<usize, Vec<String>> = HashMap::new();

        for symbol in symbols {
            if self.assignment.contains_key(symbol) {
                continue;
            }
            if let Some(max) = self.max_symbols
                && self.total() >= max
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
            if let Err(error) = self.command_txs[shard]
                .send(SocketCommand::Subscribe(batch))
                .await
            {
                eprintln!(
                    "[router] shard {shard} is gone, dropped {count} subscription(s): {error}"
                );
            }
        }
    }

    async fn remove(&mut self, symbols: &[String]) {
        let mut by_shard: HashMap<usize, Vec<String>> = HashMap::new();

        for symbol in symbols {
            let Some(shard) = self.assignment.remove(symbol) else {
                continue;
            };
            self.counts[shard] = self.counts[shard].saturating_sub(1);
            by_shard.entry(shard).or_default().push(symbol.clone());
        }

        for (shard, batch) in by_shard {
            if let Err(error) = self.command_txs[shard]
                .send(SocketCommand::Unsubscribe(batch))
                .await
            {
                eprintln!("[router] shard {shard} is gone: {error}");
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let codec: Arc<dyn VenueCodec> = Arc::new(PolymarketCodec);
    let client = reqwest::Client::builder()
        .user_agent(concat!(
            env!("CARGO_PKG_NAME"),
            "/",
            env!("CARGO_PKG_VERSION")
        ))
        .build()
        .context("building the HTTP client")?;

    eprintln!("discovering {} markets over REST", codec.name());
    let symbols = discover_with_retry(&client, codec.as_ref(), cli.max_tokens, DISCOVERY_ATTEMPTS)
        .await
        .context("initial market discovery failed")?;
    anyhow::ensure!(!symbols.is_empty(), "no tradable symbols found");
    eprintln!("discovered {} symbols", symbols.len());

    let writer = build_writer(cli.output.as_deref())?;
    let mut consumer = Consumer::new(writer);

    let per_socket = cli.tokens_per_socket.max(1);
    let chunks: Vec<Vec<String>> = symbols.chunks(per_socket).map(<[String]>::to_vec).collect();
    eprintln!(
        "opening {} websocket(s), at most {per_socket} symbols each",
        chunks.len()
    );

    let (events_tx, mut events_rx) = mpsc::channel::<StampedEnvelope>(EVENT_CHANNEL_CAPACITY);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut workers = JoinSet::new();

    let mut command_txs = Vec::with_capacity(chunks.len());
    let mut counts = Vec::with_capacity(chunks.len());
    let mut assignment = HashMap::with_capacity(symbols.len());

    for (shard_id, chunk) in chunks.into_iter().enumerate() {
        for symbol in &chunk {
            assignment.insert(symbol.clone(), shard_id);
        }
        counts.push(chunk.len());

        let (command_tx, command_rx) = mpsc::channel(COMMAND_CHANNEL_CAPACITY);
        command_txs.push(command_tx);

        let codec = Arc::clone(&codec);
        let url = codec.ws_url().to_owned();
        let events = events_tx.clone();
        let shutdown = shutdown_rx.clone();
        workers.spawn(async move {
            socket::run(shard_id, codec, url, chunk, command_rx, events, shutdown).await;
        });
    }

    // Only the workers keep senders now, so the channel closes once they stop.
    drop(events_tx);

    let router = Router {
        command_txs,
        counts,
        assignment,
        max_symbols: cli.max_tokens,
    };
    let poller = tokio::spawn(reconcile_symbols(
        client,
        Arc::clone(&codec),
        symbols.into_iter().collect(),
        router,
        Duration::from_secs(cli.poll_interval_secs.max(1)),
        shutdown_rx.clone(),
    ));

    let snapshot_interval = Duration::from_secs(cli.snapshot_interval_secs.max(1));
    let mut snapshots = time::interval(snapshot_interval);
    snapshots.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    // The first tick fires immediately; skip it so the first snapshot lands one
    // interval from now rather than duplicating the initial snapshots.
    snapshots.tick().await;

    let signal = wait_for_shutdown_signal();
    tokio::pin!(signal);

    loop {
        tokio::select! {
            biased;

            _ = &mut signal => {
                eprintln!("shutdown signal received");
                break;
            }

            envelope = events_rx.recv() => {
                match envelope {
                    Some(envelope) => consumer.apply(envelope)?,
                    None => {
                        eprintln!("all websockets stopped");
                        break;
                    }
                }
            }

            _ = snapshots.tick() => consumer.write_periodic_snapshots(Utc::now())?,
        }
    }

    // Stop the poller and tell every worker to close its socket, then drain the
    // channel so no events in flight are lost before the final flush.
    let _ = shutdown_tx.send(true);
    poller.abort();
    let _ = poller.await;

    let mut channel_open = true;
    loop {
        tokio::select! {
            biased;

            envelope = events_rx.recv(), if channel_open => {
                match envelope {
                    Some(envelope) => consumer.apply(envelope)?,
                    None => channel_open = false,
                }
            }

            joined = workers.join_next(), if !workers.is_empty() => {
                let _ = joined;
            }

            else => break,
        }
    }

    consumer.flush()?;
    eprintln!("flushed output and closed sockets");

    Ok(())
}

/// Retries startup discovery with exponential backoff.
async fn discover_with_retry(
    client: &reqwest::Client,
    codec: &dyn VenueCodec,
    max_symbols: Option<usize>,
    attempts: u32,
) -> anyhow::Result<Vec<String>> {
    let mut last_error = None;

    for attempt in 1..=attempts {
        match codec.fetch_active_symbols(client, max_symbols).await {
            Ok(symbols) => {
                if symbols.is_empty() && attempt < attempts {
                    eprintln!(
                        "discovery attempt {attempt}/{attempts} returned no symbols, retrying"
                    );
                } else {
                    return Ok(symbols);
                }
            }
            Err(error) => {
                eprintln!("discovery attempt {attempt}/{attempts} failed: {error:#}");
                last_error = Some(error);
            }
        }

        if attempt < attempts {
            let backoff = Duration::from_secs(1 << (attempt - 1).min(4));
            time::sleep(backoff).await;
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("discovery returned no symbols")))
}

/// Streams SBE to stdout when no path (or `-`) is given, else to a file.
fn build_writer(output: Option<&Path>) -> anyhow::Result<AlignedSbeWriter<Box<dyn Write + Send>>> {
    let (sink, chunk): (Box<dyn Write + Send>, usize) = match output {
        None => (Box::new(io::stdout()), STDOUT_CHUNK),
        Some(path) if path == Path::new("-") => (Box::new(io::stdout()), STDOUT_CHUNK),
        Some(path) => (
            Box::new(
                std::fs::File::create(path)
                    .with_context(|| format!("creating {}", path.display()))?,
            ),
            FILE_CHUNK,
        ),
    };

    AlignedSbeWriter::from_writer(sink, chunk)
}

/// Re-fetches the live symbol list every `interval` and applies the diff.
async fn reconcile_symbols(
    client: reqwest::Client,
    codec: Arc<dyn VenueCodec>,
    mut known: HashSet<String>,
    mut router: Router,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut ticker = time::interval(interval);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    ticker.tick().await;

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }

            _ = ticker.tick() => {
                match codec.fetch_active_symbols(&client, router.max_symbols).await {
                    Ok(latest) => {
                        let latest: HashSet<String> = latest.into_iter().collect();

                        let mut added: Vec<String> = latest.difference(&known).cloned().collect();
                        added.sort_unstable();
                        let mut removed: Vec<String> = known.difference(&latest).cloned().collect();
                        removed.sort_unstable();

                        if added.is_empty() && removed.is_empty() {
                            continue;
                        }

                        eprintln!(
                            "symbols changed: +{} -{} (now {})",
                            added.len(),
                            removed.len(),
                            latest.len()
                        );
                        router.add(&added).await;
                        router.remove(&removed).await;
                        known = latest;
                    }
                    Err(error) => eprintln!("symbol reconciliation failed: {error:#}"),
                }
            }
        }
    }
}

/// Resolves on Ctrl-C, or SIGTERM on Unix.
async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).ok();

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = async {
                match terminate.as_mut() {
                    Some(signal) => {
                        signal.recv().await;
                    }
                    None => std::future::pending::<()>().await,
                }
            } => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
