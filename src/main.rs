use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::env;
use std::time::Duration;
use tokio::time::{interval, sleep};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;
use tracing::{debug, error, info, trace, warn};

const WS_URL: &str = "wss://ws-live-data.polymarket.com";
const PING_INTERVAL_SECS: u64 = 5;
const RECONNECT_DELAY_SECS: u64 = 5;
const MAX_RECONNECT_ATTEMPTS: u32 = 10;
const DEFAULT_SYMBOLS: &[&str] = &[
    "btc/usd",
    "eth/usd",
    "sol/usd",
    "xrp/usd",
    "hype/usd",
    "doge/usd",
    "bnb/usd",
];

#[derive(Debug, Serialize)]
struct SubscribeRequest {
    action: String,
    subscriptions: Vec<Subscription>,
}

#[derive(Debug, Serialize)]
struct Subscription {
    topic: String,
    #[serde(rename = "type")]
    message_type: String,
    filters: String,
}

#[derive(Debug, Deserialize)]
struct WsMessage {
    topic: String,
    #[serde(rename = "type")]
    message_type: String,
    timestamp: Option<i64>,
    payload: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct PricePayload {
    symbol: String,
    timestamp: i64,
    value: f64,
}

#[derive(Debug, Deserialize)]
struct SnapshotPayload {
    symbol: String,
    data: Vec<DataPoint>,
}

#[derive(Debug, Deserialize)]
struct DataPoint {
    timestamp: i64,
    value: f64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    dotenvy::dotenv().ok();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPool::connect(&database_url).await.expect("Failed to connect to Postgres");

    info!("Connected to Postgres");

    let symbol_filter = env::var("SYMBOL_FILTER").ok().filter(|s| !s.is_empty());
    let symbols: Vec<&'static str> = match symbol_filter.as_deref() {
        Some(s) => vec![s],
        None => DEFAULT_SYMBOLS.to_vec(),
    };

    let mut handles = vec![];
    for symbol in symbols {
        let pool = pool.clone();
        let handle = tokio::spawn(async move {
            let mut attempt = 0u32;
            loop {
                match connect_and_listen_symbol(&pool, symbol).await {
                    Ok(()) => {
                        info!(symbol, "WebSocket closed gracefully. Reconnecting...");
                    }
                    Err(e) => {
                        error!(symbol, error = ?e, "WebSocket error. Reconnecting...");
                    }
                }

                attempt += 1;
                if attempt >= MAX_RECONNECT_ATTEMPTS {
                    error!(symbol, "Max reconnect attempts reached. Exiting.");
                    break;
                }
                sleep(Duration::from_secs(RECONNECT_DELAY_SECS)).await;
            }
        });
        handles.push(handle);
    }

    for h in handles {
        let _ = h.await;
    }

    Ok(())
}

async fn connect_and_listen_symbol(pool: &PgPool, symbol: &str) -> anyhow::Result<()> {
    let (ws_stream, _) = connect_async(WS_URL).await?;
    info!(symbol, "Connected to {}", WS_URL);

    let (mut write, mut read) = ws_stream.split();

    let subscribe_msg = SubscribeRequest {
        action: "subscribe".to_string(),
        subscriptions: vec![Subscription {
            topic: "crypto_prices_chainlink".to_string(),
            message_type: "*".to_string(),
            filters: serde_json::json!({"symbol": symbol}).to_string(),
        }],
    };

    let subscribe_json = serde_json::to_string(&subscribe_msg)?;
    info!(symbol, "Sent subscribe message: {}", subscribe_json);
    write.send(Message::Text(subscribe_json.into())).await?;

    let mut ping_interval = interval(Duration::from_secs(PING_INTERVAL_SECS));

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
                        if let Err(e) = handle_message(text, pool).await {
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

async fn handle_message(text: &str, pool: &PgPool) -> anyhow::Result<()> {
    let ws_msg: WsMessage = serde_json::from_str(text)?;

    let payload_value = match ws_msg.payload {
        Some(v) => v,
        None => return Ok(()),
    };

    match (ws_msg.topic.as_str(), ws_msg.message_type.as_str()) {
        // Snapshot message (initial historical data)
        ("crypto_prices", "subscribe") => {
            let snapshot: SnapshotPayload = serde_json::from_value(payload_value)?;
            let mut inserted = 0usize;
            for point in snapshot.data {
                insert_price(pool, &snapshot.symbol, point.value, point.timestamp).await?;
                inserted += 1;
            }
            info!(symbol = snapshot.symbol, count = inserted, "Inserted snapshot");
        }
        // Real-time update message
        ("crypto_prices_chainlink", "update") => {
            let payload: PricePayload = serde_json::from_value(payload_value)?;
            insert_price(pool, &payload.symbol, payload.value, payload.timestamp).await?;
            info!(
                symbol = payload.symbol,
                price = payload.value,
                timestamp = payload.timestamp,
                "Inserted price"
            );
        }
        _ => {
            debug!("Skipping topic={} type={}", ws_msg.topic, ws_msg.message_type);
        }
    }

    Ok(())
}

async fn insert_price(pool: &PgPool, symbol: &str, value: f64, timestamp: i64) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        INSERT INTO chainlink_prices (symbol, price, timestamp)
        VALUES ($1, $2, $3)
        "#,
    )
    .bind(symbol)
    .bind(value)
    .bind(timestamp)
    .execute(pool)
    .await?;
    Ok(())
}
