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
use crate::api::tools::get_env;
use crate::logic::{auto_clean_account, create_init_orders, spawn_process_kcn_msg};
use anyhow::Result;
use bytes::Bytes;
use dotenvy::dotenv;
use futures_util::{SinkExt, StreamExt};
use micromap::Map;

use std::sync::mpsc::{Sender, channel};
use std::thread;
use tracing::{
    Event,
    field::{Field, Visit},
    subscriber::Subscriber,
};
use tracing::{debug, error, info};
use tracing_subscriber::{
    filter::EnvFilter,
    layer::{Context as layer_Context, Layer, SubscriberExt},
    registry::LookupSpan,
    util::SubscriberInitExt,
};

use sqlx::postgres::PgPoolOptions;
use tokio::sync::mpsc;
use tokio::time::{Duration, interval, sleep};
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
    fn on_event(&self, event: &Event<'_>, _ctx: layer_Context<'_, S>) {
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

        if let Err(e) = self.sender.send(format!("{:?}", msg)) {
            eprintln!("DbErrorLayer: failed to queue error: {e}");
        }
    }
}

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

    // clear orders ids for bots
    match wipe_bots_info(&pool, &init_balance_per_bot).await {
        Ok(_) => {
            info!("wipe_bots_info");
        }
        Err(e) => {
            error!("{:#}", e);
            anyhow::bail!(e);
        }
    }

    loop {
        let mut query_params: Map<&str, &str, 8> = Map::new();
        query_params.insert("pageSize", "10");

        let query_params = match build_query_string(query_params) {
            Ok(query_params) => query_params,
            Err(e) => {
                error!("{:#}", e);
                continue;
            }
        };

        let open_stop_orders = match api_v3_hf_margin_stop_orders_get(&query_params).await {
            Ok(open_stop_orders) => open_stop_orders,
            Err(e) => {
                error!("{:#}", e);
                continue;
            }
        };

        let open_stop_orders = match open_stop_orders {
            Some(open_stop_orders) => {
                info!(
                    "Stop orders: current_page:{} page_size:{} total_num:{} total_page:{}",
                    open_stop_orders.current_page,
                    open_stop_orders.page_size,
                    open_stop_orders.total_num,
                    open_stop_orders.total_page
                );
                open_stop_orders
            }
            None => {
                error!("Fail get list open stop orders:None");
                continue;
            }
        };

        if open_stop_orders.total_num == 0 {
            info!("All stop orders closed");
            break;
        }

        for stop_order in open_stop_orders.items {
            info!("Stop order:{}", stop_order);

            let mut query_params: Map<&str, &str, 8> = Map::new();
            query_params.insert("orderId", &stop_order.id);

            let query_params = match build_query_string(query_params) {
                Ok(query_params) => query_params,
                Err(e) => {
                    error!("{:#}", e);
                    continue;
                }
            };

            let canceled_stop_order =
                match api_v3_hf_margin_stop_order_cancel_by_id_delete(&query_params).await {
                    Ok(canceled_stop_order) => canceled_stop_order,
                    Err(e) => {
                        error!("{:#}", e);
                        continue;
                    }
                };

            let canceled_stop_order = match canceled_stop_order {
                Some(canceled_stop_order) => canceled_stop_order,
                None => {
                    error!("Cancel stop order:{} None", &stop_order.id);
                    continue;
                }
            };

            for st_order in canceled_stop_order.cancelled_order_ids {
                info!("Success cancel stop order:{}", st_order)
            }
        }
        sleep(config::DELETE_STOP_ORDER_DELAY).await;
    }

    // repay all liability assets and sell
    loop {
        let is_completed = match auto_clean_account(&pool).await {
            Ok(is_completed) => is_completed,
            Err(e) => {
                error!("{:#}", e);
                continue;
            }
        };
        if is_completed {
            info!("auto_clean_account success");
            break;
        }
    }

    let (tx_in, rx_in) = mpsc::channel::<Bytes>(8192);

    let pool_process = pool.clone();
    let _spawn_process_kcn_msg_point =
        tokio::spawn(async move { spawn_process_kcn_msg(&pool_process, rx_in).await });

    if !init_order_execute {
        let pool_init_orders = pool.clone();
        tokio::spawn(async move {
            sleep(config::INIT_ORDER_DELAY).await;
            info!("Initializing start orders...");
            match create_init_orders(&pool_init_orders).await {
                Ok(_) => {
                    info!("Success create new init orders")
                }
                Err(e) => {
                    error!("{:#}", e);
                }
            }
        });
    }

    loop {
        sleep(config::RECONNECT_DELAY).await;
        // Position/Orders/Balance/AdvancedOrders WS
        let event_ws_url = match api_v1_bullet_private_post().await {
            Ok(event_ws_url) => event_ws_url,
            Err(e) => {
                error!("{:#}", e);
                continue;
            }
        };

        let (stream, _) = match connect_async(event_ws_url).await {
            Ok(stream) => stream,
            Err(e) => {
                error!("{:#}", e);
                continue;
            }
        };

        let (mut event_ws_write, mut event_ws_read) = stream.split();

        // subscribtions
        let topics = vec![
            ("subscribe_orders", "/spotMarket/tradeOrdersV2"),
            ("subscribe_stop_orders", "/spotMarket/advancedOrders"),
            ("subscribe_balance", "/account/balance"),
            ("subscribe_position", "/margin/position"),
        ];

        for (id, topic) in topics {
            if let Err(e) = event_ws_write
                .send(Message::text(
                    serde_json::json!({
                        "id": id,
                        "type": "subscribe",
                        "topic": topic,
                        "response": true,
                        "privateChannel": true
                    })
                    .to_string(),
                ))
                .await
            {
                error!("Failed to subscribe to topic {}: {}", topic, e);
                anyhow::bail!("Failed to subscribe to topic {}: {}", topic, e);
            }
            info!("Subscribed to: {}", topic);
        }

        info!("Subscribed and listening for messages...");

        let event_ping_interval = interval(config::PING_INTERVAL);
        tokio::pin!(event_ping_interval);

        loop {
            tokio::select! {
                // Events
                _ = event_ping_interval.tick() => {
                   match event_ws_write.send(Message::Ping(Bytes::new())).await {
                        Ok(_) => {
                            debug!("Ping sent");
                        },
                        Err(e) =>  {
                            error!("Fail send Ping to WebSocket:{}", e);
                            break
                        }
                    };
                }

                event = event_ws_read.next() => {

                    let event = match event {
                        Some(event) => event,
                        None => {
                            error!("WebSocket event is None, connection closed");
                            break
                        }
                    };

                    let event = match event {
                        Ok(e) => e,
                        Err(e) => {
                            error!("WebSocket read error: {}", e);
                            break;
                        }
                    };

                    match event {
                        Message::Text(text) => {
                            match tx_in.send(Bytes::from(text)).await {
                                Ok(_) => {}
                                Err(e) => {
                                    error!("Failed to send message to handler: {}", e);
                                    break;
                                }
                            }
                        }
                        Message::Binary(data) => {
                            debug!("Received binary message, size: {} bytes", data.len());
                            match tx_in.send(Bytes::from(data)).await {
                                Ok(_) => {}
                                Err(e) => {
                                    error!("Failed to send binary message to handler: {}", e);
                                    break;
                                }
                            }
                        }
                        Message::Ping(_) => {
                            debug!("Received Ping, auto-reply with Pong");
                        }
                        Message::Pong(_) => {
                            debug!("Received Pong");
                        }
                        Message::Close(frame) => {
                            match frame {
                                Some(frame) => {
                                    error!("Connection closed by server: code={}, reason={}", frame.code, frame.reason);
                                },
                                None => {
                                    error!("Connection closed by server");
                                }
                            };
                            break
                        }
                        Message::Frame(_) => {
                            debug!("Received raw frame");
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
