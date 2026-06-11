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

    let mut attempt = 0u32;
    loop {
        match connect_and_listen(&pool, symbol_filter.as_deref()).await {
            Ok(()) => {
                info!("WebSocket connection closed gracefully. Reconnecting...");
            }
            Err(e) => {
                error!("WebSocket error: {:?}. Reconnecting...", e);
            }
        }

        attempt += 1;
        if attempt >= MAX_RECONNECT_ATTEMPTS {
            error!("Max reconnect attempts reached. Exiting.");
            break;
        }
        sleep(Duration::from_secs(RECONNECT_DELAY_SECS)).await;
    }

    Ok(())
}

fn make_subscriptions(symbol_filter: Option<&str>) -> Vec<Subscription> {
    let filters = match symbol_filter {
        Some(s) => serde_json::json!({"symbol": s}).to_string(),
        None => "".to_string(),
    };
    vec![Subscription {
        topic: "crypto_prices_chainlink".to_string(),
        message_type: "*".to_string(),
        filters,
    }]
}

async fn connect_and_listen(pool: &PgPool, symbol_filter: Option<&str>) -> anyhow::Result<()> {
    let (ws_stream, _) = connect_async(WS_URL).await?;
    info!("Connected to {}", WS_URL);

    let (mut write, mut read) = ws_stream.split();

    let subscribe_msg = SubscribeRequest {
        action: "subscribe".to_string(),
        subscriptions: make_subscriptions(symbol_filter),
    };

    let subscribe_json = serde_json::to_string(&subscribe_msg)?;
    info!("Sent subscribe message: {}", subscribe_json);
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
                        trace!("WS raw: {}", text);
                        if let Err(e) = handle_message(text, pool).await {
                            warn!("Failed to handle message: {:?}", e);
                        }
                    }
                    Some(Ok(Message::Close(_))) => {
                        info!("WebSocket closed by server");
                        break;
                    }
                    Some(Ok(Message::Ping(data))) => {
                        write.send(Message::Pong(data)).await?;
                    }
                    Some(Err(e)) => {
                        error!("WebSocket read error: {:?}", e);
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

    if ws_msg.topic != "crypto_prices_chainlink" {
        debug!("Skipping topic: {}", ws_msg.topic);
        return Ok(());
    }

    if ws_msg.message_type != "update" {
        debug!("Skipping type: {}", ws_msg.message_type);
        return Ok(());
    }

    let payload_value = match ws_msg.payload {
        Some(v) => v,
        None => return Ok(()),
    };

    let payload: PricePayload = serde_json::from_value(payload_value)?;

    sqlx::query(
        r#"
        INSERT INTO chainlink_prices (symbol, price, timestamp)
        VALUES ($1, $2, $3)
        "#,
    )
    .bind(&payload.symbol)
    .bind(payload.value)
    .bind(payload.timestamp)
    .execute(pool)
    .await?;

    info!(
        symbol = payload.symbol,
        price = payload.value,
        timestamp = payload.timestamp,
        "Inserted price"
    );

    Ok(())
}
