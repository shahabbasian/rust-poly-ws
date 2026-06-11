use crate::models::{AppConfig, BatchConfig, ReconnectConfig};
use std::env;

const DEFAULT_WS_URL: &str = "wss://ws-live-data.polymarket.com";
const DEFAULT_SYMBOLS: &[&str] = &[
    "btc/usd", "eth/usd", "sol/usd", "xrp/usd", "hype/usd", "doge/usd", "bnb/usd",
];

pub fn load() -> AppConfig {
    dotenvy::dotenv().ok();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let ws_url = env::var("WS_URL").unwrap_or_else(|_| DEFAULT_WS_URL.to_string());

    let symbols: Vec<String> = match env::var("SYMBOL_FILTER") {
        Ok(s) if !s.is_empty() => vec![s],
        _ => DEFAULT_SYMBOLS.iter().map(|&s| s.to_string()).collect(),
    };

    if symbols.is_empty() {
        panic!("No symbols configured. Set SYMBOL_FILTER or rely on defaults.");
    }

    AppConfig {
        ws_url,
        database_url,
        symbols,
        ping_interval_secs: parse_env("PING_INTERVAL_SECS", 5),
        reconnect: ReconnectConfig {
            max_attempts: parse_env("RECONNECT_MAX_ATTEMPTS", 10),
            initial_backoff_secs: parse_env("RECONNECT_INITIAL_BACKOFF", 1),
            max_backoff_secs: parse_env("RECONNECT_MAX_BACKOFF", 60),
            jitter_millis: parse_env("RECONNECT_JITTER_MS", 1000),
            reset_after_secs: parse_env("RECONNECT_RESET_AFTER", 60),
        },
        batch: BatchConfig {
            max_size: parse_env("BATCH_MAX_SIZE", 500),
            flush_interval_ms: parse_env("BATCH_FLUSH_MS", 100),
        },
    }
}

fn parse_env<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}
