mod api {
    pub mod db;
    pub mod models;
    pub mod requests;
    pub mod tools;
}
mod logic;
use crate::api::db::{clear_orders_ids_for_bots, handle_db_error};
use crate::api::requests::{batch_cancel_stop_orders, build_query_string, get_private_ws_url};
use crate::api::tools::get_env;
use crate::logic::{auto_clean_account, create_init_orders, spawn_process_kcn_msg};
use dotenvy::dotenv;
use futures_util::{SinkExt, StreamExt};
use log;
use micromap::Map;

use sqlx::postgres::PgPoolOptions;
use tokio::sync::mpsc;
use tokio::time::{Duration, interval, sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const CLEAR_DELAY: Duration = Duration::from_secs(10);
const PING_INTERVAL: Duration = Duration::from_secs(5);
const INIT_ORDER_DELAY: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::init();
    dotenv().ok();
    let mut init_order_execute: bool = true;

    let database_url: String = get_env("DATABASE_URL")?;

    let exchange = "kucoin";

    let pool = match PgPoolOptions::new()
        .max_connections(40)
        .min_connections(5)
        .acquire_timeout(Duration::from_secs(10))
        .idle_timeout(Duration::from_secs(600))
        .max_lifetime(Duration::from_secs(1800))
        .connect(&database_url)
        .await
    {
        Ok(pool) => pool,
        Err(e) => {
            let msg: String = format!("Failed to create pool:{}", e);
            log::error!("{}", msg);
            return Err(msg);
        }
    };

    // clear orders ids for bots
    match clear_orders_ids_for_bots(&pool, exchange, "1").await {
        Ok(_) => log::info!("clear orders ids bots"),
        Err(e) => match handle_db_error(&pool, exchange, e).await {
            Ok(error_msg) => return Err(error_msg),
            Err(error_msg) => return Err(error_msg),
        },
    }

    // cancel all stop orders
    let mut query_params: Map<&str, &str, 8> = Map::new();

    query_params.insert("tradeType", "MARGIN_TRADE");

    match batch_cancel_stop_orders(build_query_string(query_params)).await {
        Ok(_) => log::info!("batch cancel stop orders"),
        Err(e) => match handle_db_error(&pool, exchange, e).await {
            Ok(error_msg) => return Err(error_msg),
            Err(error_msg) => return Err(error_msg),
        },
    };

    // repay all liability assets and sell
    loop {
        sleep(CLEAR_DELAY).await;
        match auto_clean_account(&pool, exchange).await {
            Ok(true) => break,
            Ok(false) => {}
            Err(e) => match handle_db_error(&pool, exchange, e).await {
                Ok(error_msg) => return Err(error_msg),
                Err(error_msg) => return Err(error_msg),
            },
        };
    }

    loop {
        // websocket to pg
        let (tx_in, rx_in) = mpsc::channel::<String>(10000);

        let exchange_process = exchange;
        let pool_process = pool.clone();

        // Work with income events
        let spawn_process_kcn_msg_point = tokio::spawn(async move { spawn_process_kcn_msg(&pool_process, exchange_process, rx_in).await });

        // Position/Orders/Balance/AdvancedOrders WS
        let (mut event_ws_write, mut event_ws_read) = match get_private_ws_url().await {
            Ok(event_ws_url) => match connect_async(event_ws_url).await {
                Ok((stream, _)) => stream.split(),
                Err(e) => {
                    let _ = handle_db_error(&pool, exchange, format!("WebSocket connection failed:{}", e)).await;

                    drop(tx_in);
                    drop(spawn_process_kcn_msg_point);
                    sleep(RECONNECT_DELAY).await;
                    continue;
                }
            },
            Err(e) => {
                let _ = handle_db_error(&pool, exchange, format!("Failed to get WebSocket URL:{}", e)).await;

                drop(tx_in);
                drop(spawn_process_kcn_msg_point);
                sleep(RECONNECT_DELAY).await;
                continue;
            }
        };

        // subscribtion
        match event_ws_write
            .send(Message::text(serde_json::json!({"id":"subscribe_orders","type":"subscribe","topic":"/spotMarket/tradeOrdersV2","response":true,"privateChannel":"true"}).to_string()))
            .await
        {
            Ok(_) => log::info!("Subscribe:/spotMarket/tradeOrdersV2"),
            Err(e) => {
                let _ = handle_db_error(&pool, exchange, format!("Failed to subscribe topic:/spotMarket/tradeOrdersV2:{}", e)).await;
                continue;
            }
        }

        match event_ws_write
            .send(Message::text(serde_json::json!({"id":"subscribe_stop_orders","type":"subscribe","topic":"/spotMarket/advancedOrders","response":true,"privateChannel":"true"}).to_string()))
            .await
        {
            Ok(_) => log::info!("Subscribe:/spotMarket/advancedOrders"),
            Err(e) => {
                let _ = handle_db_error(&pool, exchange, format!("Failed to subscribe subject:/spotMarket/advancedOrders:{}", e)).await;
                continue;
            }
        }

        match event_ws_write.send(Message::text(serde_json::json!({"id":"subscribe_balance","type":"subscribe","topic":"/account/balance","response":true,"privateChannel":"true"}).to_string())).await
        {
            Ok(_) => log::info!("Subscribe:/account/balance"),
            Err(e) => {
                let _ = handle_db_error(&pool, exchange, format!("Failed to subscribe subject:/account/balance:{}", e)).await;

                continue;
            }
        }

        match event_ws_write.send(Message::text(serde_json::json!({"id":"subscribe_position","type":"subscribe","topic":"/margin/position","response":true,"privateChannel":"true"}).to_string())).await
        {
            Ok(_) => log::info!("Subscribe:/margin/position"),
            Err(e) => {
                let _ = handle_db_error(&pool, exchange, format!("Failed to subscribe subject:/margin/position:{}", e)).await;
                continue;
            }
        }

        log::info!("Subscribed and listening for messages...");

        let event_ping_interval = interval(PING_INTERVAL);
        tokio::pin!(event_ping_interval);

        if !init_order_execute {
            init_order_execute = true;
            let pool_clone = pool.clone();
            let exchange_clone = exchange;

            tokio::spawn(async move {
                sleep(INIT_ORDER_DELAY).await;
                log::info!("Initializing start orders...");
                match create_init_orders(&pool_clone, exchange_clone).await {
                    Ok(_) => {}
                    Err(e) => {
                        let _ = handle_db_error(&pool_clone, exchange_clone, format!("Failed create_init_orders:{}", e)).await;
                    }
                };
            });
        }

        loop {
            tokio::select! {
                // Events
                event = event_ws_read.next() => {
                    match event {
                        Some(Ok(Message::Text(text))) => {
                            match tx_in.send(text.to_string()).await {
                                Ok(_) => {}
                                Err(e) => {
                                    let msg: String = format!("Failed to send to handler, reconnecting...{}", e);
                                    log::error!("{}", msg);
                                    break;
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = event_ws_write.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Close(_close))) => {
                            let _ = handle_db_error(&pool, exchange, format!("Connection closed by server:")).await;
                            break;
                        }
                        Some(Err(e)) => {
                            let _ = handle_db_error(&pool, exchange, format!("WebSocket read error:{}",e)).await;
                            break;
                        }
                        Some(Ok(_)) => {}
                        None => {
                            let msg: String = "WebSocket stream ended".to_string();
                            log::info!("{}", msg);
                            // sent error to pg
                            break;
                        }
                    }
                }
                _ = event_ping_interval.tick() => {
                    let _ = event_ws_write.send(Message::Ping(vec![].into())).await;
                }
            }
        }

        drop(tx_in);

        let _ = spawn_process_kcn_msg_point.await;

        drop(event_ws_write);
        drop(event_ws_read);

        log::error!("Reconnecting in {} seconds...", RECONNECT_DELAY.as_secs());
        sleep(RECONNECT_DELAY).await;
    }
}
