//! One WebSocket session for a shard, plus its reconnect loop.
//!
//! Each shard runs `--redundancy` connections (Conn A = `Primary`,
//! Conn B = `Standby`). Every connection attempt increments its generation, so
//! the consumer can discard events from a retired socket.

use std::{sync::Arc, time::Duration};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use tokio::{
    sync::{mpsc, watch},
    time,
};
use tokio_tungstenite::tungstenite::{
    Message,
    client::IntoClientRequest,
    http::{HeaderName, HeaderValue},
};

use crate::{
    codec::{ConnSlot, StampedEnvelope, VenueCodec},
    engine::EngineMessage,
};

const CLOSE_TIMEOUT: Duration = Duration::from_millis(1_500);
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// A session that lasted this long is considered healthy, so backoff resets.
const HEALTHY_SESSION: Duration = Duration::from_secs(60);

/// Dynamic subscription changes sent from the shard manager to a worker.
#[derive(Debug)]
pub enum SocketCommand {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
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
) {
    let mut backoff = INITIAL_BACKOFF;
    // Increments per connection attempt; the consumer drops stale generations.
    let mut generation = 0u64;

    loop {
        if *shutdown.borrow() {
            return;
        }

        generation += 1;
        let started = std::time::Instant::now();
        let result = session(
            shard_id,
            slot,
            generation,
            codec.as_ref(),
            &url,
            &mut symbols,
            &mut commands,
            &events,
            &mut shutdown,
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
                        generation,
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
    generation: u64,
    codec: &dyn VenueCodec,
    url: &str,
    symbols: &mut Vec<String>,
    commands: &mut mpsc::Receiver<SocketCommand>,
    events: &mpsc::Sender<EngineMessage>,
    shutdown: &mut watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut request = url.into_client_request()?;
    for (name, value) in codec.ws_request_headers()? {
        let name = HeaderName::try_from(name.as_str())?;
        let value = HeaderValue::try_from(value.as_str())?;
        request.headers_mut().insert(name, value);
    }

    let (mut socket, _response) = tokio_tungstenite::connect_async(request).await?;
    eprintln!("[shard {shard_id} {slot:?}] connected (gen {generation})");

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
                                        generation,
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
