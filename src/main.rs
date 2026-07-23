mod api {
    pub mod db;
    pub mod models;
    pub mod requests;
    pub mod tools;
}
mod config;
mod logic;
use crate::api::db::{insert_db_error, wipe_bots_info};
use crate::api::requests::{
    api_v1_bullet_private_post, api_v3_hf_margin_stop_order_cancel_by_id_delete,
    api_v3_hf_margin_stop_orders_get, build_query_string,
};
use std::sync::mpsc::{Sender, channel};
use std::thread;
use tracing::{
    Event,
    field::{Field, Visit},
    subscriber::Subscriber,
};
use tracing_subscriber::{
    filter::EnvFilter,
    layer::{Context, Layer, SubscriberExt},
    registry::LookupSpan,
    util::SubscriberInitExt,
};

use crate::api::tools::get_env;
use crate::logic::{auto_clean_account, create_init_orders, spawn_process_kcn_msg};
use bytes::Bytes;
use dotenvy::dotenv;
use futures_util::{SinkExt, StreamExt};
use micromap::Map;
use sqlx::PgPool;
use tracing::{error, info};

use sqlx::postgres::PgPoolOptions;
use tokio::sync::mpsc;
use tokio::time::{Duration, Interval, interval, sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};

struct MessageVisitor {
    message: String,
}

impl MessageVisitor {
    fn new() -> Self {
        Self {
            message: String::new(),
        }
    }
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{:?}", value).trim_matches('"').to_string();
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        }
    }
}

pub struct DbErrorLayer {
    sender: Sender<String>,
}

impl DbErrorLayer {
    pub fn new(pool: sqlx::PgPool) -> Self {
        let (sender, receiver) = channel::<String>();

        thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
            rt.block_on(async move {
                while let Ok(msg) = receiver.recv() {
                    if let Err(e) = insert_db_error(&pool, &msg).await {
                        eprintln!("Failed to save error to DB: {e}");
                    }
                }
                eprintln!("DbErrorLayer: receiver closed, worker thread exiting");
            });
        });

        Self { sender }
    }
}

impl<S> Layer<S> for DbErrorLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if *event.metadata().level() != tracing::Level::ERROR {
            return;
        }

        let mut visitor = MessageVisitor::new();
        event.record(&mut visitor);

        let msg = if visitor.message.is_empty() {
            event.metadata().name().to_string()
        } else {
            visitor.message
        };

        // Синхронная, неблокирующая (или мало-блокирующая) отправка
        // Не зависит от tokio runtime, не создаёт потоки
        if let Err(e) = self.sender.send(msg) {
            eprintln!("DbErrorLayer: failed to queue error: {e}");
        }
    }
}

fn init_tracing(pool: sqlx::PgPool) {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
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
async fn main() -> Result<(), String> {
    dotenv().ok();
    let init_order_execute: bool = true;

    let database_url: String = get_env("DATABASE_URL")?;

    let init_balance_per_bot: String = get_env("INIT_BALANCE_PER_BOT")?;

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .min_connections(1)
        .acquire_timeout(Duration::from_secs(10))
        .idle_timeout(Duration::from_secs(600))
        .max_lifetime(Duration::from_secs(1800))
        .connect(&database_url)
        .await
        .map_err(|e| {
            eprintln!("Failed to create pg pool:{}", e);
            "".to_string()
        })?;

    init_tracing(pool.clone());

    // clear orders ids for bots
    wipe_bots_info(&pool, &init_balance_per_bot)
        .await
        .map_err(|e| {
            error!("{}", e);
            e
        })?;
    info!("wipe_bots_info");

    loop {
        sleep(config::DELETE_STOP_ORDER_DELAY).await;
        let mut query_params: Map<&str, &str, 8> = Map::new();
        query_params.insert("pageSize", "10");

        let open_stop_orders = api_v3_hf_margin_stop_orders_get(build_query_string(query_params))
            .await
            .map_err(|e| {
                error!("{}", e);
                e
            })?;

        let Some(open_stop_orders_data) = open_stop_orders else {
            error!("Fail get list open stop orders:None");
            return Err("".to_string());
        };

        info!(
            "Stop orders: current_page:{} page_size:{} total_num:{} total_page:{}",
            open_stop_orders_data.current_page,
            open_stop_orders_data.page_size,
            open_stop_orders_data.total_num,
            open_stop_orders_data.total_page
        );

        if open_stop_orders_data.total_num == 0 {
            break;
        }

        for stop_order in open_stop_orders_data.items {
            info!("Stop order:{}", stop_order);
            let mut query_params: Map<&str, &str, 8> = Map::new();

            query_params.insert("orderId", &stop_order.id);

            let canceled_stop_order =
                api_v3_hf_margin_stop_order_cancel_by_id_delete(build_query_string(query_params))
                    .await
                    .map_err(|e| {
                        error!("{}", e);
                        e
                    })?;

            let Some(canceled_stop_order) = canceled_stop_order else {
                error!("Cancel stop order:{} None", &stop_order.id);
                continue;
            };

            for st_order in canceled_stop_order.cancelled_order_ids {
                info!("Success cancel stop order:{}", st_order)
            }
        }
    }

    // repay all liability assets and sell
    loop {
        if auto_clean_account(&pool).await.map_err(|e| {
            error!("{}", e);
            e
        })? {
            break;
        }
    }

    let (tx_in, rx_in) = mpsc::channel::<String>(10000);

    let pool_process: PgPool = pool.clone();
    let _spawn_process_kcn_msg_point =
        tokio::spawn(async move { spawn_process_kcn_msg(&pool_process, rx_in).await });

    if !init_order_execute {
        let pool_init_orders: PgPool = pool.clone();
        tokio::spawn(async move {
            sleep(config::INIT_ORDER_DELAY).await;
            info!("Initializing start orders...");
            match create_init_orders(&pool_init_orders).await {
                Ok(_) => {}
                Err(e) => {
                    error!("{}", e);
                }
            }
        });
    }

    loop {
        // Position/Orders/Balance/AdvancedOrders WS
        let event_ws_url = api_v1_bullet_private_post().await.map_err(|e| {
            error!("{}", e);
            e
        })?;

        let (mut event_ws_write, mut event_ws_read) = match connect_async(event_ws_url).await {
            Ok((stream, _)) => stream.split(),
            Err(e) => {
                sleep(config::RECONNECT_DELAY).await;
                continue;
            }
        };

        // subscribtion
        event_ws_write
            .send(Message::text(serde_json::json!({"id":"subscribe_orders","type":"subscribe","topic":"/spotMarket/tradeOrdersV2","response":true,"privateChannel":"true"}).to_string()))
            .await
            .map_err(|e|{
                error!("Failed to subscribe topic:/spotMarket/tradeOrdersV2:{}", e);
                "".to_string()
            })?;

        info!("Subscribe:/spotMarket/tradeOrdersV2");

        event_ws_write
            .send(Message::text(serde_json::json!({"id":"subscribe_stop_orders","type":"subscribe","topic":"/spotMarket/advancedOrders","response":true,"privateChannel":"true"}).to_string()))
            .await
            .map_err(|e|{
                error!("Failed to subscribe subject:/spotMarket/advancedOrders:{}", e);
                "".to_string()
            })?;
        info!("Subscribe:/spotMarket/advancedOrders");

        event_ws_write.send(Message::text(serde_json::json!({"id":"subscribe_balance","type":"subscribe","topic":"/account/balance","response":true,"privateChannel":"true"}).to_string())).await.map_err(|e|{
            error!("Failed to subscribe subject:/account/balance:{}", e);
            "".to_string()
         })?;
        info!("Subscribe:/account/balance");

        event_ws_write.send(Message::text(serde_json::json!({"id":"subscribe_position","type":"subscribe","topic":"/margin/position","response":true,"privateChannel":"true"}).to_string())).await.map_err(|e|{
            error!("Failed to subscribe subject:/margin/position:{}", e);
            "".to_string()
        })?;

        info!("Subscribe:/margin/position");

        info!("Subscribed and listening for messages...");

        let event_ping_interval: Interval = interval(config::PING_INTERVAL);
        tokio::pin!(event_ping_interval);

        loop {
            tokio::select! {
                // Events
                _ = event_ping_interval.tick() => {
                   match event_ws_write.send(Message::Ping(Bytes::new())).await {
                        Ok(_) => {},
                        Err(e) =>  {
                            error!("Fail send Ping to WebSocket:{}", e);
                            break
                        }
                    };
                }

                event = event_ws_read.next() => {
                    let Some(event) = event else {error!("WebSocket stream ended");
                            break};

                    let event = match event {
                        Ok(e) => e,
                        Err(e) => {
                            error!("WebSocket read error: {}", e);
                            break;
                        }
                    };


                    match event {
                        Message::Text(text) => match tx_in.send(text.to_string()).await {
                            Ok(_) => {}
                            Err(e) => {
                                error!("Failed to send to handler, reconnecting...{}", e);
                                break;
                            }
                        }
                        Message::Ping(data) => match event_ws_write.send(Message::Pong(data)).await {
                            Ok(_) => {},
                            Err(e) => {
                                error!("Fail send Pong to WebSocket:{}", e);
                                break
                            }
                        }
                        Message::Close(_close) => {
                            error!("Connection closed by server:");
                            break
                        }
                        _ => {
                            error!("Unexpected msg:{}", event);
                            break
                        }
                    }
                }
            }
        }

        error!(
            "Reconnecting in {} seconds...",
            config::RECONNECT_DELAY.as_secs()
        );
        sleep(config::RECONNECT_DELAY).await;
    }
}
