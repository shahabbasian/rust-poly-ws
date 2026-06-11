use sqlx::PgPool;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info};

mod config;
mod db;
mod models;
mod ws;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── Tracing ──
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // ── Config ──
    let cfg = config::load();
    info!(?cfg, "Configuration loaded");

    // ── Database ──
    let pool = PgPool::connect(&cfg.database_url)
        .await
        .expect("Failed to connect to Postgres");
    info!("Connected to Postgres");

    // ── Batch channel ──
    let (batch_tx, batch_rx) = mpsc::channel::<models::PriceRow>(cfg.batch.max_size * 2);

    // ── Spawn batch writer ──
    let pool_clone = pool.clone();
    let batch_config = cfg.batch.clone();
    let batch_handle = tokio::spawn(async move {
        db::start_batch_writer(pool_clone, batch_config, batch_rx).await;
    });

    // ── Spawn WebSocket reader ──
    let ws_cfg = cfg.clone();
    let ws_batch_tx = batch_tx.clone();
    let ws_handle = tokio::spawn(async move {
        ws::run(&ws_cfg, ws_batch_tx).await;
    });

    // ── Graceful shutdown ──
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received SIGINT/SIGTERM, initiating graceful shutdown");
        }
        _ = ws_handle => {
            error!("WebSocket task exited unexpectedly");
        }
        _ = batch_handle => {
            error!("Batch writer exited unexpectedly");
        }
    }

    // ── Cleanup ──
    // Close the batch channel so writer can flush remaining rows
    drop(batch_tx);

    // Wait up to 5 seconds for batch writer to finish flushing
    let flush_timeout = tokio::time::timeout(Duration::from_secs(5), batch_handle);
    match flush_timeout.await {
        Ok(Ok(())) => info!("Batch writer finished gracefully"),
        Ok(Err(e)) => error!("Batch writer panicked: {:?}", e),
        Err(_) => error!("Batch writer timed out during flush — remaining rows may be lost"),
    }

    // Close DB pool
    pool.close().await;
    info!("Postgres pool closed. Shutdown complete.");

    Ok(())
}
