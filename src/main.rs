mod api {
    pub mod models;
    pub mod repository;
    pub mod requests;
    pub mod tools;
}
mod constants;
mod logic;
use crate::constants::*;
mod core;
mod infrastructure;
mod tracing_layer;
mod websocket;

use crate::api::tools::get_env;
use crate::core::traits::Repository;
use crate::infrastructure::postgres_repository::PostgresRepository;

use crate::logic::{cancel_all_stop_orders, clean_account, create_init_orders};
use crate::tracing_layer::DbErrorLayer;
use crate::websocket::run_websocket_loop;
use anyhow::Result;

use dotenvy::dotenv;

use tracing::{error, info};
use tracing_subscriber::{filter::EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use sqlx::postgres::PgPoolOptions;

use tokio::time::{Duration, sleep};

fn init_tracing(pool: sqlx::PgPool) {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_line_number(true)
        .with_thread_ids(true);

    let filter_layer = EnvFilter::from_default_env();

    let db_layer = DbErrorLayer::new(pool);

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .with(db_layer)
        .init();
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    let init_order_execute = true;

    let database_url = get_env("DATABASE_URL")?;
    let init_balance_per_bot = get_env("INIT_BALANCE_PER_BOT")?;

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .min_connections(1)
        .acquire_timeout(Duration::from_secs(10))
        .idle_timeout(Duration::from_secs(600))
        .max_lifetime(Duration::from_secs(1800))
        .connect(&database_url)
        .await?;

    init_tracing(pool.clone());

    let repository = PostgresRepository::new(pool.clone());

    match repository.clear_bots(&init_balance_per_bot).await {
        Ok(_) => {
            info!("wipe_bots_info");
        }
        Err(e) => {
            error!("{:#}", e);
            anyhow::bail!(e);
        }
    }

    if let Err(e) = cancel_all_stop_orders().await {
        error!("Failed to cancel stop orders: {:#}", e);
    }

    if let Err(e) = clean_account(&pool).await {
        error!("Failed to clean account: {:#}", e);
    }

    if !init_order_execute {
        let pool_init_orders = pool.clone();
        tokio::spawn(async move {
            info!("Initializing start orders...");
            match create_init_orders(&pool_init_orders).await {
                Ok(_) => {
                    info!("Success create new init orders")
                }
                Err(e) => {
                    error!("{:#}", e);
                }
            };
            sleep(INIT_ORDER_DELAY).await;
        });
    }
    run_websocket_loop(pool).await
}
