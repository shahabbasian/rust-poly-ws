use serde::{Deserialize, Serialize};

// ── WebSocket Messages ──────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct SubscribeRequest {
    pub action: String,
    pub subscriptions: Vec<Subscription>,
}

#[derive(Debug, Serialize)]
pub struct Subscription {
    pub topic: String,
    #[serde(rename = "type")]
    pub message_type: String,
    pub filters: String,
}

#[derive(Debug, Deserialize)]
pub struct WsMessage {
    pub topic: String,
    #[serde(rename = "type")]
    pub message_type: String,
    pub timestamp: Option<i64>,
    pub payload: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct PricePayload {
    pub symbol: String,
    pub timestamp: i64,
    pub value: f64,
}

#[derive(Debug, Deserialize)]
pub struct SnapshotPayload {
    pub symbol: String,
    pub data: Vec<DataPoint>,
}

#[derive(Debug, Deserialize)]
pub struct DataPoint {
    pub timestamp: i64,
    pub value: f64,
}

// ── Batch Insert Item ────────────────────────────────────────

#[derive(Debug)]
pub struct PriceRow {
    pub symbol: String,
    pub price: f64,
    pub timestamp: i64,
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub ws_url: String,
    pub database_url: String,
    pub symbols: Vec<String>,
    pub ping_interval_secs: u64,
    pub reconnect: ReconnectConfig,
    pub batch: BatchConfig,
}

#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    pub max_attempts: u32,
    pub initial_backoff_secs: u64,
    pub max_backoff_secs: u64,
    pub jitter_millis: u64,
    pub reset_after_secs: u64,
}

#[derive(Debug, Clone)]
pub struct BatchConfig {
    pub max_size: usize,
    pub flush_interval_ms: u64,
}
