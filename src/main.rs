mod api;
mod constants;
mod core;
mod infrastructure;
mod logic;

use crate::api::utils::get_env;
use crate::core::repository_traits::BotManagement;
use crate::infrastructure::postgres_repository::PostgresRepository;
use crate::infrastructure::tracing_layer::DbErrorLayer;
use crate::infrastructure::websocket::run_websocket_loop;
use crate::logic::{cancel_all_stop_orders, clean_account, create_init_orders};

use anyhow::Result;
use dotenvy::dotenv;
use sqlx::postgres::PgPoolOptions;
use tokio::time::Duration;
use tracing::{error, info};
use tracing_subscriber::{filter::EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

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

    let repo = PostgresRepository::new(pool.clone());

    let bot_repo = repo.bot;
    let order_repo = repo.order;
    let symbol_repo = repo.symbol;
    let position_repo = repo.position;
    let balance_repo = repo.balance;
    let event_repo = repo.event;
    let message_repo = repo.message;

    // Очищаем ботов
    match bot_repo.clear_all_bots(&init_balance_per_bot).await {
        Ok(_) => info!("All bots cleared"),
        Err(e) => {
            error!("{:#}", e);
            anyhow::bail!(e);
        }
    }

    // Отменяем стоп-ордера
    if let Err(e) = cancel_all_stop_orders().await {
        error!("Failed to cancel stop orders: {:#}", e);
    }

    // Очищаем аккаунт
    if let Err(e) = clean_account(&symbol_repo, &message_repo).await {
        error!("Failed to clean account: {:#}", e);
    }

    let bot_repo_clone = bot_repo.clone();
    let symbol_repo_clone = symbol_repo.clone();
    let message_repo_clone = message_repo.clone();

    // Запускаем инициализацию в фоновом режиме
    tokio::spawn(async move {
        info!("Starting background initialization of bots...");
        // Даем WebSocket время подключиться
        tokio::time::sleep(Duration::from_secs(30)).await;

        if let Err(e) =
            create_init_orders(&bot_repo_clone, &symbol_repo_clone, &message_repo_clone).await
        {
            error!("Background initialization failed: {:#}", e);
        } else {
            info!("Background initialization completed successfully!");
        }
    });

    // Запускаем WebSocket
    run_websocket_loop(
        bot_repo,
        order_repo,
        symbol_repo,
        balance_repo,
        position_repo,
        event_repo,
        message_repo,
    )
    .await
}
