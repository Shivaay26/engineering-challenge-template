//! Tuned TCP dialing and N-way connection racing.
//!
//! Two separate latency levers:
//!
//! 1. **Per-socket tuning.** `TCP_NODELAY` disables Nagle's algorithm, a 2 MiB
//!    `SO_RCVBUF` absorbs bursts without kernel drops, and `TCP_QUICKACK`
//!    suppresses Linux's delayed-ACK timer on inbound frames.
//! 2. **Racing.** Cloudflare (Polymarket's front door) answers from many edge
//!    workers, and round-trip times to them differ by tens of milliseconds. We
//!    dial several candidates in parallel, probe each with `PING`/`PONG`, and
//!    keep the fastest by p50 (ties broken by p99).

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs},
    time::Duration,
};

use anyhow::{Context, bail};
use futures_util::{SinkExt, StreamExt, future::join_all};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::{
    net::{TcpSocket, TcpStream},
    time,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, client_async_tls_with_config,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        http::{HeaderName, HeaderValue, Request},
    },
};

use super::NetConfig;
use crate::codec::VenueCodec;

pub type WsSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// PINGs sent per candidate when racing.
const PROBES: usize = 10;
/// Spacing between probes, so the measurement is not dominated by queueing.
const PROBE_GAP: Duration = Duration::from_millis(50);
const PONG_TIMEOUT: Duration = Duration::from_secs(5);

/// What the race measured, for logging.
#[derive(Debug, Clone, Copy)]
pub struct RaceOutcome {
    pub candidates: usize,
    pub winner_p50: Duration,
    pub winner_p99: Duration,
}

/// Dial `url`, racing `cfg.race_candidates` connections when configured.
pub async fn connect(
    codec: &dyn VenueCodec,
    url: &str,
    cfg: &NetConfig,
) -> anyhow::Result<(WsSocket, Option<RaceOutcome>)> {
    let request = url.into_client_request()?;
    let request = {
        let mut request = request;
        for (name, value) in codec.ws_request_headers()? {
            let name = HeaderName::try_from(name.as_str())?;
            let value = HeaderValue::try_from(value.as_str())?;
            request.headers_mut().insert(name, value);
        }
        request
    };
    let uri = request.uri();
    let host = uri
        .host()
        .context("WebSocket URL without a host")?
        .to_owned();
    let default_port = if uri.scheme_str() == Some("wss") {
        443
    } else {
        80
    };
    let port = uri.port_u16().unwrap_or(default_port);
    let addresses: Vec<SocketAddr> = (host.as_str(), port)
        .to_socket_addrs()
        .with_context(|| format!("resolving {host}:{port}"))?
        .collect();
    anyhow::ensure!(!addresses.is_empty(), "no addresses for {host}:{port}");

    let candidates = cfg.race_candidates.max(1);
    if candidates == 1 {
        let socket = dial(&request, &addresses, cfg).await?;
        return Ok((socket, None));
    }

    let dials = (0..candidates).map(|_| async {
        let mut socket = dial(&request, &addresses, cfg).await?;
        let rtts = probe(&mut socket).await?;
        Ok::<_, anyhow::Error>((socket, rtts))
    });

    let mut scored = Vec::new();
    let mut last_error = None;
    for result in join_all(dials).await {
        match result {
            Ok((socket, rtts)) => {
                let mut sorted = rtts;
                sorted.sort_unstable();
                scored.push((
                    percentile(&sorted, 50, 100),
                    percentile(&sorted, 99, 100),
                    socket,
                ));
            }
            Err(error) => last_error = Some(error),
        }
    }

    anyhow::ensure!(
        !scored.is_empty(),
        "every candidate connection failed: {}",
        last_error
            .map(|error| format!("{error:#}"))
            .unwrap_or_else(|| "unknown".to_owned())
    );

    // Fastest p50 wins; p99 breaks ties.
    scored.sort_by_key(|(p50, p99, _)| (*p50, *p99));
    let outcome = RaceOutcome {
        candidates,
        winner_p50: scored[0].0,
        winner_p99: scored[0].1,
    };

    let (_, _, winner) = scored.remove(0);
    for (_, _, mut loser) in scored {
        // Best effort: we already have what we need from this socket.
        let _ = loser.close(None).await;
    }

    Ok((winner, Some(outcome)))
}

/// Bounds one dial + handshake. Without this, an unresponsive edge can leave a
/// worker stuck inside `connect_async` forever — which also blocks shutdown.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Opens one TCP connection with the tuned options and completes the WebSocket
/// handshake. Addresses are tried in order, which matters on hosts that publish
/// both A and AAAA records but can only route one family.
async fn dial(
    request: &Request<()>,
    addresses: &[SocketAddr],
    cfg: &NetConfig,
) -> anyhow::Result<WsSocket> {
    let mut last_error = None;

    for address in addresses {
        let attempt = async {
            let stream = dial_one(address, cfg).await?;
            let (socket, _response) =
                client_async_tls_with_config(request.clone(), stream, None, None).await?;
            Ok::<WsSocket, anyhow::Error>(socket)
        };

        match time::timeout(CONNECT_TIMEOUT, attempt).await {
            Ok(Ok(socket)) => return Ok(socket),
            Ok(Err(error)) => last_error = Some(error),
            Err(_) => {
                last_error = Some(anyhow::anyhow!("connecting to {address} timed out"));
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("no address could be dialled")))
}

async fn dial_one(address: &SocketAddr, cfg: &NetConfig) -> anyhow::Result<TcpStream> {
    let socket = Socket::new(
        Domain::for_address(*address),
        Type::STREAM,
        Some(Protocol::TCP),
    )?;
    socket.set_nonblocking(true)?;

    if cfg.tuned {
        socket.set_tcp_nodelay(true)?;
        socket.set_recv_buffer_size(cfg.recv_buffer_bytes)?;
    }

    if let Some(port) = cfg.bind_port_hint {
        let local = match address {
            SocketAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port),
            SocketAddr::V6(_) => SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port),
        };
        socket.bind(&local.into())?;
    }

    let stream = TcpSocket::from_std_stream(socket.into())
        .connect(*address)
        .await?;

    #[cfg(target_os = "linux")]
    if cfg.tuned && cfg.quickack {
        set_quickack(&stream);
    }

    Ok(stream)
}

/// Suppress Linux's delayed-ACK timer. Best effort: failure costs latency, not
/// correctness.
#[cfg(target_os = "linux")]
fn set_quickack(stream: &TcpStream) {
    use std::os::fd::AsRawFd;

    let enable: libc::c_int = 1;
    // SAFETY: `fd` is a live socket owned by `stream`, and the option buffer is
    // a correctly sized `c_int` that outlives the call.
    unsafe {
        libc::setsockopt(
            stream.as_raw_fd(),
            libc::IPPROTO_TCP,
            libc::TCP_QUICKACK,
            std::ptr::from_ref(&enable).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}

/// Sends `PROBES` PINGs and records each round-trip time.
async fn probe(socket: &mut WsSocket) -> anyhow::Result<Vec<Duration>> {
    probe_rtts(socket, PROBES).await
}

/// Sends `probes` PINGs and records each round-trip time.
pub async fn probe_rtts(socket: &mut WsSocket, probes: usize) -> anyhow::Result<Vec<Duration>> {
    let mut rtts = Vec::with_capacity(probes);

    for _ in 0..probes {
        let started = time::Instant::now();
        socket.send(Message::Ping(Vec::new().into())).await?;

        loop {
            match time::timeout(PONG_TIMEOUT, socket.next()).await {
                Ok(Some(Ok(Message::Pong(_)))) => {
                    rtts.push(started.elapsed());
                    break;
                }
                // Ignore anything the venue volunteers before the PONG.
                Ok(Some(Ok(_))) => continue,
                Ok(Some(Err(error))) => return Err(error.into()),
                Ok(None) => bail!("connection closed while probing"),
                Err(_) => bail!("PONG timed out"),
            }
        }

        time::sleep(PROBE_GAP).await;
    }

    Ok(rtts)
}

/// Nearest-rank percentile over a sorted slice, using integer arithmetic.
/// Ranks are ceiling-based, so p99 of a small sample is the maximum.
pub fn percentile(sorted: &[Duration], numerator: usize, denominator: usize) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let rank = (sorted.len() * numerator).div_ceil(denominator);
    let index = rank.saturating_sub(1).min(sorted.len() - 1);
    sorted[index]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn millis(values: &[u64]) -> Vec<Duration> {
        values.iter().map(|ms| Duration::from_millis(*ms)).collect()
    }

    #[test]
    fn percentiles_use_nearest_rank() {
        let sorted = millis(&[10, 20, 30, 40, 50]);
        assert_eq!(percentile(&sorted, 50, 100), Duration::from_millis(30));
        assert_eq!(percentile(&sorted, 99, 100), Duration::from_millis(50));
        assert_eq!(percentile(&sorted, 0, 100), Duration::from_millis(10));
        assert_eq!(percentile(&[], 50, 100), Duration::ZERO);
    }

    #[test]
    fn candidates_rank_by_p50_then_p99() {
        let mut scored = [
            (Duration::from_millis(30), Duration::from_millis(90), "slow"),
            (
                Duration::from_millis(12),
                Duration::from_millis(40),
                "winner",
            ),
            (
                Duration::from_millis(12),
                Duration::from_millis(25),
                "winner-tie",
            ),
        ];
        scored.sort_by_key(|(p50, p99, _)| (*p50, *p99));
        assert_eq!(scored[0].2, "winner-tie");
        assert_eq!(scored[1].2, "winner");
        assert_eq!(scored[2].2, "slow");
    }
}
