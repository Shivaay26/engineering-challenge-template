//! End-to-end test of the collector pipeline without touching the network.
//!
//! A local WebSocket server speaks recorded Polymarket frames; the real
//! [`socket::run`] worker connects to it, decodes, forwards
//! [`StampedEnvelope`]s to the single [`Consumer`], and the consumer writes SBE
//! that we then read back with [`AlignedSbeReader`].
//!
//! This is the only test that exercises the connect/subscribe/send path, which
//! matters because the sandbox used during development cannot reach Polymarket.

use std::{sync::Arc, time::Duration};

use engineering_challenge::{
    Side,
    codec::{ConnSlot, NormalizedEvent, VenueCodec},
    engine::{
        Consumer, EngineMessage,
        socket::{self},
    },
    util::{
        AlignedSbeReader, AlignedSbeWriter, OrderbookSchema,
        dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
    },
    venues::PolymarketCodec,
};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    net::TcpListener,
    sync::{mpsc, watch},
    time,
};
use tokio_tungstenite::tungstenite::Message;

/// The SBE reader only accepts whole chunks.
const CHUNK: usize = 64 << 20;
const TOKEN: &str = "TOKEN";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_to_consumer_pipeline_writes_replayable_sbe() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();

        // The worker sends its initial subscribe frame first; drain it before
        // pushing market data.
        let _ = socket.next().await;

        socket
            .send(Message::Text(
                r#"{
                    "event_type": "book",
                    "asset_id": "TOKEN",
                    "bids": [{"price": "0.48", "size": "30"}],
                    "asks": [{"price": "0.52", "size": "25"}]
                }"#
                .into(),
            ))
            .await
            .unwrap();

        socket
            .send(Message::Text(
                r#"[{
                    "event_type": "price_change",
                    "price_changes": [
                        {"asset_id": "TOKEN", "price": "0.48", "size": "0", "side": "BUY"}
                    ]
                }]"#
                .into(),
            ))
            .await
            .unwrap();

        // Hold the socket open until the client closes it.
        while let Some(message) = socket.next().await {
            if message.is_err() {
                break;
            }
        }
    });

    let file = tempfile::NamedTempFile::new()?;
    let mut consumer = Consumer::new(AlignedSbeWriter::create(file.path(), CHUNK)?);

    let (events_tx, mut events_rx) = mpsc::channel::<EngineMessage>(16);
    let (commands_tx, commands_rx) = mpsc::channel(8);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let codec: Arc<dyn VenueCodec> = Arc::new(PolymarketCodec);
    let worker = tokio::spawn(socket::run(
        0,
        ConnSlot::Primary,
        codec,
        format!("ws://{address}"),
        vec![TOKEN.to_owned()],
        commands_rx,
        events_tx,
        shutdown_rx,
        Duration::ZERO,
        engineering_challenge::net::NetConfig::default(),
    ));

    for expected in 0..2 {
        let message = time::timeout(Duration::from_secs(5), events_rx.recv())
            .await?
            .unwrap_or_else(|| panic!("worker stopped before event {expected}"));
        consumer.apply(message)?;
    }

    consumer.flush()?;
    let _ = shutdown_tx.send(true);
    time::timeout(Duration::from_secs(5), worker).await??;
    let _ = time::timeout(Duration::from_secs(5), server).await?;

    // Drop our spare command sender only after the worker is done.
    drop(commands_tx);

    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file.path())?;

    let Some(AnyMessage::Snapshot(decoded)) = reader.read_msg()? else {
        anyhow::bail!("expected the initial snapshot");
    };
    let snapshot = SnapshotDomain::try_from_decoder(decoded)?;
    assert_eq!(snapshot.symbol, TOKEN);
    assert_eq!(snapshot.bids.len(), 1);
    assert_eq!(snapshot.bids[0].price_level, 480_000);
    assert_eq!(snapshot.bids[0].quantity, 30_000_000);
    assert_eq!(snapshot.asks[0].price_level, 520_000);
    assert_eq!(snapshot.asks[0].quantity, 25_000_000);

    let Some(AnyMessage::PriceLevelUpdate(decoded)) = reader.read_msg()? else {
        anyhow::bail!("expected the level update");
    };
    let delta = PriceLevelUpdateDomain::try_from_decoder(decoded)?;
    assert_eq!(delta.symbol, TOKEN);
    assert_eq!(delta.side, Side::Buy);
    assert_eq!(delta.price_level, 480_000);
    // The frame deleted the level (absolute size 0).
    assert_eq!(delta.quantity, 0);
    assert!(delta.backtest_timestamp >= snapshot.received_at);

    assert!(reader.read_msg()?.is_none());

    Ok(())
}

#[tokio::test]
async fn worker_survives_a_malformed_frame() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let _ = socket.next().await;

        // Garbage first: the worker must log and continue, not disconnect.
        socket
            .send(Message::Text("{not json".into()))
            .await
            .unwrap();
        socket
            .send(Message::Text(
                r#"{"event_type":"book","asset_id":"TOKEN","bids":[],"asks":[]}"#.into(),
            ))
            .await
            .unwrap();

        while let Some(message) = socket.next().await {
            if message.is_err() {
                break;
            }
        }
    });

    let (events_tx, mut events_rx) = mpsc::channel::<EngineMessage>(16);
    let (commands_tx, commands_rx) = mpsc::channel(8);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let codec: Arc<dyn VenueCodec> = Arc::new(PolymarketCodec);
    let worker = tokio::spawn(socket::run(
        0,
        ConnSlot::Primary,
        codec,
        format!("ws://{address}"),
        vec![TOKEN.to_owned()],
        commands_rx,
        events_tx,
        shutdown_rx,
        Duration::ZERO,
        engineering_challenge::net::NetConfig::default(),
    ));

    let message = time::timeout(Duration::from_secs(5), events_rx.recv())
        .await?
        .expect("a snapshot after the malformed frame");
    match message {
        EngineMessage::Event(envelope) => {
            assert!(matches!(envelope.event, NormalizedEvent::Snapshot { .. }));
        }
        other => panic!("unexpected message: {other:?}"),
    }

    let _ = shutdown_tx.send(true);
    time::timeout(Duration::from_secs(5), worker).await??;
    let _ = time::timeout(Duration::from_secs(5), server).await?;

    drop(commands_tx);

    Ok(())
}
