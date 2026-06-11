use crate::models::{
    AppConfig, PricePayload, PriceRow, SnapshotPayload, SubscribeRequest, Subscription, WsMessage,
};
use futures_util::{SinkExt, StreamExt};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::{interval, sleep};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{debug, error, info, trace, warn};

/// Spawn one WebSocket connection per symbol. Each connection has its own
/// reconnect loop and sends rows into the shared batch channel.
pub async fn run_symbol(config: &AppConfig, symbol: &str, batch_tx: mpsc::Sender<PriceRow>) {
    let mut attempt = 0u32;
    let mut last_connected: Option<Instant> = None;

    loop {
        match connect_and_listen(config, symbol, &batch_tx).await {
            Ok(()) => {
                last_connected = Some(Instant::now());
                info!(symbol, "WebSocket closed gracefully. Reconnecting...");
            }
            Err(e) => {
                error!(symbol, error = ?e, "WebSocket error. Reconnecting...");
            }
        }

        if let Some(t) = last_connected {
            if t.elapsed().as_secs() >= config.reconnect.reset_after_secs {
                info!(
                    symbol,
                    "Stable connection lasted >{}s, resetting backoff",
                    config.reconnect.reset_after_secs
                );
                attempt = 0;
            }
        }

        attempt += 1;
        if attempt >= config.reconnect.max_attempts {
            error!(
                symbol,
                max_attempts = config.reconnect.max_attempts,
                "Max reconnect attempts reached. Exiting WebSocket loop."
            );
            break;
        }

        let delay = compute_backoff(config, attempt);
        info!(
            symbol,
            attempt,
            delay_secs = delay.as_secs(),
            "Waiting before reconnect"
        );
        sleep(delay).await;
    }
}

fn compute_backoff(config: &AppConfig, attempt: u32) -> Duration {
    let base = config.reconnect.initial_backoff_secs;
    let max = config.reconnect.max_backoff_secs;
    let exp = base.saturating_mul(2_u64.saturating_pow(attempt.saturating_sub(1)));
    let capped = exp.min(max);
    let jitter_ms = if config.reconnect.jitter_millis > 0 {
        fastrand::u64(..config.reconnect.jitter_millis)
    } else {
        0
    };
    Duration::from_secs(capped) + Duration::from_millis(jitter_ms)
}

async fn connect_and_listen(
    config: &AppConfig,
    symbol: &str,
    batch_tx: &mpsc::Sender<PriceRow>,
) -> anyhow::Result<()> {
    let (ws_stream, _) = connect_async(&config.ws_url).await?;
    info!(symbol, url = %config.ws_url, "WebSocket connected");

    let (mut write, mut read) = ws_stream.split();

    // ── Subscribe ONE symbol per connection ──
    let subscribe_msg = SubscribeRequest {
        action: "subscribe".to_string(),
        subscriptions: vec![Subscription {
            topic: "crypto_prices_chainlink".to_string(),
            message_type: "*".to_string(),
            filters: serde_json::json!({ "symbol": symbol }).to_string(),
        }],
    };

    let subscribe_json = serde_json::to_string(&subscribe_msg)?;
    write
        .send(Message::Text(subscribe_json.clone().into()))
        .await?;
    info!(symbol, "Sent subscribe message: {}", subscribe_json);

    let mut ping_interval = interval(Duration::from_secs(config.ping_interval_secs));

    loop {
        tokio::select! {
            _ = ping_interval.tick() => {
                write.send(Message::Text("PING".into())).await?;
            }

            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        let text = text.trim();
                        if text.is_empty() || text == "PONG" {
                            continue;
                        }
                        trace!(symbol, "WS raw: {}", text);
                        if let Err(e) = handle_message(text, batch_tx).await {
                            warn!(symbol, "Failed to handle message: {:?}", e);
                        }
                    }
                    Some(Ok(Message::Close(_))) => {
                        info!(symbol, "WebSocket closed by server");
                        break;
                    }
                    Some(Ok(Message::Ping(data))) => {
                        write.send(Message::Pong(data)).await?;
                    }
                    Some(Err(e)) => {
                        error!(symbol, "WebSocket read error: {:?}", e);
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}

async fn handle_message(text: &str, batch_tx: &mpsc::Sender<PriceRow>) -> anyhow::Result<()> {
    let ws_msg: WsMessage = serde_json::from_str(text)?;

    let payload_value = match ws_msg.payload {
        Some(v) => v,
        None => return Ok(()),
    };

    match (ws_msg.topic.as_str(), ws_msg.message_type.as_str()) {
        // Snapshot message (initial historical data)
        ("crypto_prices", "subscribe") => {
            let snapshot: SnapshotPayload = serde_json::from_value(payload_value)?;
            let count = snapshot.data.len();
            for point in snapshot.data {
                batch_tx
                    .send(PriceRow {
                        symbol: snapshot.symbol.clone(),
                        price: point.value,
                        timestamp: point.timestamp,
                    })
                    .await
                    .map_err(|_| anyhow::anyhow!("Batch channel closed"))?;
            }
            info!(symbol = snapshot.symbol, count, "Queued snapshot rows");
        }

        // Real-time update message
        ("crypto_prices_chainlink", "update") => {
            let payload: PricePayload = serde_json::from_value(payload_value)?;
            batch_tx
                .send(PriceRow {
                    symbol: payload.symbol.clone(),
                    price: payload.value,
                    timestamp: payload.timestamp,
                })
                .await
                .map_err(|_| anyhow::anyhow!("Batch channel closed"))?;
            debug!(
                symbol = payload.symbol,
                price = payload.value,
                timestamp = payload.timestamp,
                "Queued price update"
            );
        }

        _ => {
            debug!(
                "Skipping topic={} type={}",
                ws_msg.topic, ws_msg.message_type
            );
        }
    }

    Ok(())
}
