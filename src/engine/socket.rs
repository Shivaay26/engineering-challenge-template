//! One WebSocket session for a shard, plus its reconnect and rotation loops.
//!
//! Each shard runs `--redundancy` connections (Conn A = `Primary`,
//! Conn B = `Standby`). Every connection attempt increments a shared generation,
//! so the consumer can discard events from a retired socket.
//!
//! Connections are also rotated on a jittered deadline. Rotation is
//! *make-before-break*: the replacement connects, subscribes and warms up while
//! the old socket keeps streaming, and only then is the old one closed. Without
//! that ordering the collector would have a blind window on every rotation.

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use tokio::{
    net::TcpStream,
    sync::{mpsc, watch},
    time,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        http::{HeaderName, HeaderValue},
    },
};

use crate::{
    codec::{ConnSlot, NormalizedEvent, StampedEnvelope, VenueCodec},
    engine::EngineMessage,
};

type WsSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

const CLOSE_TIMEOUT: Duration = Duration::from_millis(1_500);
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// A session that lasted this long is considered healthy, so backoff resets.
const HEALTHY_SESSION: Duration = Duration::from_secs(60);
/// Gap between unsubscribe and subscribe when reviving one subscription.
const RESUBSCRIBE_GAP: Duration = Duration::from_millis(100);
/// How long a replacement connection may take to hydrate before we promote it.
const ROTATION_WARMUP: Duration = Duration::from_secs(5);
/// Fraction of a shard's symbols that must be hydrated before promoting.
const ROTATION_HYDRATION_PERCENT: usize = 95;

/// Dynamic subscription changes sent from the shard manager to a worker.
#[derive(Debug)]
pub enum SocketCommand {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
    /// Unsubscribe then resubscribe a single symbol on this connection only.
    ResubscribeToken(String),
    Shutdown,
}

/// Runs one connection until `shutdown` is set, reconnecting on failure.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    shard_id: usize,
    slot: ConnSlot,
    codec: Arc<dyn VenueCodec>,
    url: String,
    mut symbols: Vec<String>,
    mut commands: mpsc::Receiver<SocketCommand>,
    events: mpsc::Sender<EngineMessage>,
    mut shutdown: watch::Receiver<bool>,
    rotation: Duration,
) {
    let mut backoff = INITIAL_BACKOFF;
    // Shared across rotation, so a replacement always outranks its predecessor.
    let generation = Arc::new(AtomicU64::new(0));

    loop {
        if *shutdown.borrow() {
            return;
        }

        let started = std::time::Instant::now();
        let result = session(
            shard_id,
            slot,
            &generation,
            codec.as_ref(),
            &url,
            &mut symbols,
            &mut commands,
            &events,
            &mut shutdown,
            rotation,
        )
        .await;

        match result {
            Ok(()) => return,
            Err(error) => {
                eprintln!("[shard {shard_id} {slot:?}] websocket error: {error:#}");
                if *shutdown.borrow() {
                    return;
                }

                let _ = events
                    .send(EngineMessage::Disconnected {
                        shard_id,
                        slot,
                        generation: generation.load(Ordering::Relaxed),
                    })
                    .await;

                if started.elapsed() >= HEALTHY_SESSION {
                    backoff = INITIAL_BACKOFF;
                }
                eprintln!("[shard {shard_id} {slot:?}] reconnecting in {backoff:?}");

                tokio::select! {
                    _ = time::sleep(backoff) => {}
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            return;
                        }
                    }
                }

                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn session(
    shard_id: usize,
    slot: ConnSlot,
    generation: &AtomicU64,
    codec: &dyn VenueCodec,
    url: &str,
    symbols: &mut Vec<String>,
    commands: &mut mpsc::Receiver<SocketCommand>,
    events: &mpsc::Sender<EngineMessage>,
    shutdown: &mut watch::Receiver<bool>,
    rotation: Duration,
) -> anyhow::Result<()> {
    let mut socket = connect(codec, url).await?;
    let mut current_generation = generation.fetch_add(1, Ordering::Relaxed) + 1;
    eprintln!("[shard {shard_id} {slot:?}] connected (gen {current_generation})");

    let initial = codec.encode_initial_subscribe(symbols)?;
    socket.send(Message::Text(initial.into())).await?;
    eprintln!(
        "[shard {shard_id} {slot:?}] subscribed to {} symbol(s)",
        symbols.len()
    );

    let mut heartbeat = codec.heartbeat_interval().map(|period| {
        let mut interval = time::interval(period);
        interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
        interval
    });
    let has_heartbeat = heartbeat.is_some();
    let heartbeat_payload = codec.heartbeat_payload();

    let rotating = !rotation.is_zero();
    let mut rotate_at = time::Instant::now() + jittered(rotation) + rotation_offset(rotation, slot);

    loop {
        tokio::select! {
            biased;

            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    close(&mut socket).await;
                    return Ok(());
                }
            }

            command = commands.recv() => {
                match command {
                    Some(SocketCommand::Subscribe(new_symbols)) => {
                        socket
                            .send(Message::Text(codec.encode_subscribe(&new_symbols)?.into()))
                            .await?;
                        for symbol in new_symbols {
                            if !symbols.contains(&symbol) {
                                symbols.push(symbol);
                            }
                        }
                    }
                    Some(SocketCommand::Unsubscribe(old_symbols)) => {
                        socket
                            .send(Message::Text(codec.encode_unsubscribe(&old_symbols)?.into()))
                            .await?;
                        symbols.retain(|symbol| !old_symbols.contains(symbol));
                    }
                    Some(SocketCommand::ResubscribeToken(symbol)) => {
                        socket
                            .send(Message::Text(
                                codec.encode_unsubscribe(std::slice::from_ref(&symbol))?.into(),
                            ))
                            .await?;
                        time::sleep(RESUBSCRIBE_GAP).await;
                        socket
                            .send(Message::Text(
                                codec.encode_subscribe(std::slice::from_ref(&symbol))?.into(),
                            ))
                            .await?;
                    }
                    Some(SocketCommand::Shutdown) | None => {
                        close(&mut socket).await;
                        return Ok(());
                    }
                }
            }

            _ = next_tick(&mut heartbeat), if has_heartbeat => {
                if let Some(payload) = &heartbeat_payload {
                    socket.send(Message::Text(payload.clone().into())).await?;
                }
            }

            _ = time::sleep_until(rotate_at), if rotating => {
                match rotate(shard_id, slot, generation, codec, url, symbols, events, &mut socket).await {
                    Ok(new_generation) => {
                        current_generation = new_generation;
                        rotate_at = time::Instant::now()
                            + jittered(rotation)
                            + rotation_offset(rotation, slot);
                    }
                    Err(error) => {
                        // Keep the healthy socket and try again later rather
                        // than dropping into an unplanned reconnect.
                        eprintln!("[shard {shard_id} {slot:?}] rotation failed, keeping current socket: {error:#}");
                        rotate_at = time::Instant::now()
                            + jittered(rotation)
                            + rotation_offset(rotation, slot);
                    }
                }
            }

            message = socket.next() => {
                let Some(message) = message else {
                    anyhow::bail!("socket closed by peer");
                };

                match message? {
                    Message::Text(text) => {
                        // A malformed frame must not take down the connection:
                        // log it and keep streaming. Redundancy covers the gap.
                        match codec.decode_text_frame(text.as_str(), Utc::now()) {
                            Ok(decoded) => {
                                for event in decoded {
                                    let envelope = StampedEnvelope {
                                        shard_id,
                                        slot,
                                        generation: current_generation,
                                        event,
                                    };
                                    if events.send(EngineMessage::Event(envelope)).await.is_err() {
                                        return Ok(());
                                    }
                                }
                            }
                            Err(error) => {
                                eprintln!("[shard {shard_id} {slot:?}] dropping frame: {error:#}");
                            }
                        }
                    }
                    Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
                    Message::Close(_) => anyhow::bail!("peer closed the socket"),
                    Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
                }
            }
        }
    }
}

/// Test-only escape hatch: a fresh generation for the replacement link.
fn next_generation(generation: &AtomicU64) -> u64 {
    generation.fetch_add(1, Ordering::Relaxed) + 1
}

/// Make-before-break: warm up a replacement, then close the current socket.
///
/// Events from the replacement are forwarded during warm-up, so the consumer
/// sees its snapshots and performs the reconciliation itself.
#[allow(clippy::too_many_arguments)]
async fn rotate(
    shard_id: usize,
    slot: ConnSlot,
    generation: &AtomicU64,
    codec: &dyn VenueCodec,
    url: &str,
    symbols: &[String],
    events: &mpsc::Sender<EngineMessage>,
    socket: &mut WsSocket,
) -> anyhow::Result<u64> {
    let mut replacement = connect(codec, url).await?;
    let new_generation = next_generation(generation);
    let target = (symbols.len() * ROTATION_HYDRATION_PERCENT).div_ceil(100);

    eprintln!(
        "[shard {shard_id} {slot:?}] rotating to gen {new_generation}, warming up {} symbol(s)",
        symbols.len()
    );

    replacement
        .send(Message::Text(
            codec.encode_initial_subscribe(symbols)?.into(),
        ))
        .await?;

    let mut hydrated: HashSet<String> = HashSet::new();
    let deadline = time::Instant::now() + ROTATION_WARMUP;

    while hydrated.len() < target {
        let remaining = deadline.saturating_duration_since(time::Instant::now());
        if remaining.is_zero() {
            break;
        }

        match time::timeout(remaining, replacement.next()).await {
            Err(_) => break,
            Ok(None) => anyhow::bail!("replacement socket closed during warm-up"),
            Ok(Some(message)) => {
                let Message::Text(text) = message? else {
                    continue;
                };
                let Ok(decoded) = codec.decode_text_frame(text.as_str(), Utc::now()) else {
                    continue;
                };

                for event in decoded {
                    if let NormalizedEvent::Snapshot { symbol, .. } = &event {
                        hydrated.insert(symbol.clone());
                    }
                    let envelope = StampedEnvelope {
                        shard_id,
                        slot,
                        generation: new_generation,
                        event,
                    };
                    if events.send(EngineMessage::Event(envelope)).await.is_err() {
                        return Ok(new_generation);
                    }
                }
            }
        }
    }

    eprintln!(
        "[shard {shard_id} {slot:?}] promoting gen {new_generation} ({}/{} hydrated), closing gen {current}",
        hydrated.len(),
        symbols.len(),
        current = new_generation - 1
    );

    close(socket).await;
    *socket = replacement;

    Ok(new_generation)
}

async fn connect(codec: &dyn VenueCodec, url: &str) -> anyhow::Result<WsSocket> {
    let mut request = url.into_client_request()?;
    for (name, value) in codec.ws_request_headers()? {
        let name = HeaderName::try_from(name.as_str())?;
        let value = HeaderValue::try_from(value.as_str()).context("invalid header value")?;
        request.headers_mut().insert(name, value);
    }

    let (socket, _response) = tokio_tungstenite::connect_async(request).await?;
    Ok(socket)
}

/// `base` ± up to `base/7` (capped at 5 min), so many sockets do not rotate in
/// lockstep. Jitter is derived from the clock rather than a PRNG to keep this
/// module dependency-free.
fn jittered(base: Duration) -> Duration {
    let seconds = base.as_secs();
    if seconds == 0 {
        return Duration::ZERO;
    }

    let spread = (seconds / 7).min(300);
    if spread == 0 {
        return base;
    }

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::from(elapsed.subsec_nanos()))
        .unwrap_or(0);

    base - Duration::from_secs(spread) + Duration::from_secs(nanos % (spread * 2 + 1))
}

/// Half a lifetime, so the two connections of a shard never rotate together:
/// while one is warming up, the other is still streaming.
fn rotation_offset(base: Duration, slot: ConnSlot) -> Duration {
    match slot {
        ConnSlot::Primary => Duration::ZERO,
        ConnSlot::Standby => base / 2,
    }
}

async fn close<S>(socket: &mut S)
where
    S: SinkExt<Message> + Unpin,
{
    let _ = time::timeout(CLOSE_TIMEOUT, socket.close()).await;
}

async fn next_tick(interval: &mut Option<time::Interval>) {
    match interval {
        Some(interval) => {
            interval.tick().await;
        }
        None => std::future::pending().await,
    }
}
