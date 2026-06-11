use crate::models::{BatchConfig, PriceRow};
use sqlx::{PgPool, Postgres, QueryBuilder};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info};

pub async fn start_batch_writer(
    pool: PgPool,
    config: BatchConfig,
    mut rx: mpsc::Receiver<PriceRow>,
) {
    let mut buffer: Vec<PriceRow> = Vec::with_capacity(config.max_size);
    let mut flush_interval = tokio::time::interval(Duration::from_millis(config.flush_interval_ms));
    flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    info!(
        max_size = config.max_size,
        flush_ms = config.flush_interval_ms,
        "Batch writer started"
    );

    loop {
        tokio::select! {
            // Timer tick: flush stale items
            _ = flush_interval.tick() => {
                if !buffer.is_empty() {
                    if let Err(e) = flush_batch(&pool, &mut buffer).await {
                        error!("Batch flush failed (timer): {:?}", e);
                    }
                }
            }

            // Incoming row
            Some(row) = rx.recv() => {
                buffer.push(row);
                if buffer.len() >= config.max_size {
                    if let Err(e) = flush_batch(&pool, &mut buffer).await {
                        error!("Batch flush failed (full): {:?}", e);
                    }
                }
            }

            // Channel closed (graceful shutdown)
            else => {
                info!("Batch writer channel closed, flushing remaining rows");
                if !buffer.is_empty() {
                    if let Err(e) = flush_batch(&pool, &mut buffer).await {
                        error!("Final batch flush failed: {:?}", e);
                    }
                }
                break;
            }
        }
    }

    info!("Batch writer stopped");
}

async fn flush_batch(pool: &PgPool, buffer: &mut Vec<PriceRow>) -> anyhow::Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }

    let count = buffer.len();

    // Build a multi-row INSERT with QueryBuilder (efficient & type-safe)
    let mut builder: QueryBuilder<Postgres> =
        QueryBuilder::new("INSERT INTO chainlink_prices (symbol, price, timestamp) ");

    builder.push_values(buffer.drain(..), |mut b, row| {
        b.push_bind(row.symbol)
            .push_bind(row.price)
            .push_bind(row.timestamp);
    });

    builder.build().execute(pool).await?;

    info!(count, "Flushed batch");
    Ok(())
}

// ── Old single insert (kept for rare edge-cases if needed) ──

#[allow(dead_code)]
pub async fn insert_single(
    pool: &PgPool,
    symbol: &str,
    value: f64,
    timestamp: i64,
) -> anyhow::Result<()> {
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
