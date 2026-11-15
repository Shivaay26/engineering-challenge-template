//! Collector entry point: discover markets over REST, stream them over sharded
//! redundant WebSockets, and write SBE snapshots and deltas to stdout or a file.
//!
//! `stdout` carries binary SBE bytes only. Every status line goes to `stderr`
//! via `eprintln!`, so the output can be piped straight into a replay tool.

use std::{
    collections::HashSet,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use chrono::Utc;
use clap::Parser;
use engineering_challenge::{
    codec::VenueCodec,
    engine::{Consumer, EngineMessage, ShardManager},
    net::NetConfig,
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
const EVENT_CHANNEL_CAPACITY: usize = 32_768;
/// Venues front their REST API with CDNs that occasionally hand out an edge IP
/// which resets connections, so startup discovery is retried.
const DISCOVERY_ATTEMPTS: u32 = 5;
/// How often the consumer classifies symbol activity and checks for zombies.
const WATCHDOG_INTERVAL: Duration = Duration::from_secs(10);

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

    /// Symbols subscribed per WebSocket connection. Scale this, not the socket
    /// count: the collector derives shards from it.
    #[arg(long, default_value_t = 500)]
    tokens_per_socket: usize,

    /// Redundant connections per shard (1 = no failover, 2 = Conn A + Conn B).
    #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u8).range(1..=2))]
    redundancy: u8,

    /// REST market discovery interval, in seconds.
    #[arg(long, default_value_t = 30)]
    poll_interval_secs: u64,

    /// Periodic full-snapshot interval, in seconds.
    #[arg(long, default_value_t = 60)]
    snapshot_interval_secs: u64,

    /// Base connection lifetime before make-before-break rotation, in minutes.
    /// 0 disables rotation. Jittered by up to +/- base/7 so sockets do not
    /// rotate in lockstep.
    #[arg(long, default_value_t = 35)]
    rotate_minutes: u64,

    /// Parallel TLS + WebSocket candidates to race per connection. 1 disables
    /// racing; the fastest by p50 RTT is kept.
    #[arg(long, default_value_t = 4, value_parser = clap::value_parser!(u8).range(1..=16))]
    race_connections: u8,

    /// Optional cap on total symbols, for smoke tests only.
    #[arg(long)]
    max_tokens: Option<usize>,
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

    match cli.max_tokens {
        Some(max) => eprintln!(
            "discovered {} symbols (capped by --max-tokens {max})",
            symbols.len()
        ),
        None => eprintln!("discovered {} symbols", symbols.len()),
    }

    let writer = build_writer(cli.output.as_deref())?;
    let mut consumer = Consumer::new(writer);

    let (events_tx, mut events_rx) = mpsc::channel::<EngineMessage>(EVENT_CHANNEL_CAPACITY);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut workers = JoinSet::new();

    let rotation = Duration::from_secs(cli.rotate_minutes.saturating_mul(60));
    let net = NetConfig {
        race_candidates: usize::from(cli.race_connections).max(1),
        ..NetConfig::default()
    };
    let manager = ShardManager::spawn(
        Arc::clone(&codec),
        &symbols,
        cli.tokens_per_socket,
        usize::from(cli.redundancy),
        rotation,
        net.clone(),
        events_tx.clone(),
        shutdown_rx.clone(),
        &mut workers,
    )
    .with_max_symbols(cli.max_tokens);

    // The watchdog acts through the same handle the reconciler uses.
    consumer.set_maintenance(Arc::new(manager.clone()));

    eprintln!(
        "{} symbols -> {} shard(s) x {} connection(s), at most {} symbols each ({} sockets, rotation {}, {} racing candidate(s))",
        manager.total_symbols(),
        manager.shard_count(),
        cli.redundancy,
        cli.tokens_per_socket,
        manager.connection_count(),
        if cli.rotate_minutes == 0 {
            "disabled".to_owned()
        } else {
            format!("~{}", cli.rotate_minutes)
        },
        cli.race_connections
    );

    // Only the workers keep senders now, so the channel closes once they stop.
    drop(events_tx);

    let poller = tokio::spawn(reconcile_symbols(
        client,
        Arc::clone(&codec),
        symbols.into_iter().collect(),
        manager,
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

    let mut watchdog = time::interval(WATCHDOG_INTERVAL);
    watchdog.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    watchdog.tick().await;

    loop {
        tokio::select! {
            biased;

            _ = &mut signal => {
                eprintln!("shutdown signal received");
                break;
            }

            message = events_rx.recv() => {
                match message {
                    Some(message) => consumer.apply(message)?,
                    None => {
                        eprintln!("all websockets stopped");
                        break;
                    }
                }
            }

            _ = snapshots.tick() => consumer.write_periodic_snapshots(Utc::now())?,

            _ = watchdog.tick() => consumer.run_watchdog(std::time::Instant::now()),
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

            message = events_rx.recv(), if channel_open => {
                match message {
                    Some(message) => consumer.apply(message)?,
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
    manager: ShardManager,
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
                match codec.fetch_active_symbols(&client, None).await {
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
                        manager.add(&added).await;
                        manager.remove(&removed).await;
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
