mod api {
    pub mod db;
    pub mod models;
    pub mod requests;
    pub mod tools;
}
mod logic;
use crate::api::db::{handle_db_error, wipe_bots_info};
use crate::api::models::ApiV3HfMarginStopOrdersResData;
use crate::api::requests::{api_v1_bullet_private_post, api_v3_hf_margin_stop_order_cancel_delete, api_v3_hf_margin_stop_orders_get, build_query_string};
use crate::api::tools::get_env;
use crate::logic::{auto_clean_account, create_init_orders, spawn_process_kcn_msg};
use bytes::Bytes;
use dotenvy::dotenv;
use futures_util::{SinkExt, StreamExt};
use micromap::Map;

use sqlx::postgres::PgPoolOptions;
use tokio::sync::mpsc;
use tokio::time::{Duration, Interval, interval, sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const PING_INTERVAL: Duration = Duration::from_secs(5);
const INIT_ORDER_DELAY: Duration = Duration::from_secs(5);
const EXCHANGE: &str = "kucoin";

#[tokio::main]
async fn main() -> Result<(), String> {
    env_logger::init();
    dotenv().ok();
    let mut init_order_execute: bool = true;

    let database_url: String = get_env("DATABASE_URL")?;

    let init_balance_per_bot: String = get_env("INIT_BALANCE_PER_BOT")?;

    let pool: sqlx::Pool<sqlx::Postgres> = match PgPoolOptions::new()
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
            let msg: String = format!("Failed to create pg pool:{}", e);
            log::error!("{}", msg);
            return Err(msg);
        }
    };

    // clear orders ids for bots
    match wipe_bots_info(&pool, EXCHANGE, &init_balance_per_bot).await {
        Ok(_) => log::info!("wipe_bots_info"),
        Err(e) => return Err(handle_db_error(&pool, EXCHANGE, e).await),
    }

    let open_stop_orders: Option<ApiV3HfMarginStopOrdersResData> = match api_v3_hf_margin_stop_orders_get(String::new()).await {
        Ok(orders) => orders,
        Err(e) => return Err(handle_db_error(&pool, EXCHANGE, e).await),
    };

    let open_stop_orders_data: ApiV3HfMarginStopOrdersResData = match open_stop_orders {
        Some(open_stop_orders) => open_stop_orders,
        None => {
            let msg: String = format!("Fail get list open stop orders:None");
            log::error!("{}", msg);
            return Err(msg);
        }
    };

    if open_stop_orders_data.items.len() != 0 {
        // cancel all stop orders
        let mut query_params: Map<&str, &str, 8> = Map::new();
        query_params.insert("tradeType", "MARGIN_TRADE");

        match api_v3_hf_margin_stop_order_cancel_delete(build_query_string(query_params)).await {
            Ok(_) => log::info!("batch cancel stop orders"),
            Err(e) => return Err(handle_db_error(&pool, EXCHANGE, e).await),
        };
    }

    // repay all liability assets and sell
    loop {
        match auto_clean_account(&pool, EXCHANGE).await {
            Ok(true) => break,
            Ok(false) => continue,
            Err(e) => return Err(e),
        };
    }
    let (tx_in, rx_in) = mpsc::channel::<String>(10000);

    let pool_process = pool.clone();

    let _spawn_process_kcn_msg_point = tokio::spawn(async move { spawn_process_kcn_msg(&pool_process, EXCHANGE, rx_in).await });

    loop {
        // Position/Orders/Balance/AdvancedOrders WS
        let event_ws_url: String = match api_v1_bullet_private_post().await {
            Ok(event_ws_url) => event_ws_url,
            Err(e) => {
                handle_db_error(&pool, EXCHANGE, e).await;
                sleep(RECONNECT_DELAY).await;
                continue;
            }
        };

        let (mut event_ws_write, mut event_ws_read) = match connect_async(event_ws_url).await {
            Ok((stream, _)) => stream.split(),
            Err(e) => {
                let msg: String = format!("WebSocket connection failed:{}", e);
                log::error!("{}", msg);
                handle_db_error(&pool, EXCHANGE, msg).await;
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
                let msg: String = format!("Failed to subscribe topic:/spotMarket/tradeOrdersV2:{}", e);
                log::error!("{}", msg);
                handle_db_error(&pool, EXCHANGE, msg).await;
                sleep(RECONNECT_DELAY).await;
                continue;
            }
        }

        match event_ws_write
            .send(Message::text(serde_json::json!({"id":"subscribe_stop_orders","type":"subscribe","topic":"/spotMarket/advancedOrders","response":true,"privateChannel":"true"}).to_string()))
            .await
        {
            Ok(_) => log::info!("Subscribe:/spotMarket/advancedOrders"),
            Err(e) => {
                let msg: String = format!("Failed to subscribe subject:/spotMarket/advancedOrders:{}", e);
                log::error!("{}", msg);
                handle_db_error(&pool, EXCHANGE, msg).await;
                sleep(RECONNECT_DELAY).await;
                continue;
            }
        }

        match event_ws_write.send(Message::text(serde_json::json!({"id":"subscribe_balance","type":"subscribe","topic":"/account/balance","response":true,"privateChannel":"true"}).to_string())).await
        {
            Ok(_) => log::info!("Subscribe:/account/balance"),
            Err(e) => {
                let msg: String = format!("Failed to subscribe subject:/account/balance:{}", e);
                log::error!("{}", msg);
                handle_db_error(&pool, EXCHANGE, msg).await;
                sleep(RECONNECT_DELAY).await;
                continue;
            }
        }

        match event_ws_write.send(Message::text(serde_json::json!({"id":"subscribe_position","type":"subscribe","topic":"/margin/position","response":true,"privateChannel":"true"}).to_string())).await
        {
            Ok(_) => log::info!("Subscribe:/margin/position"),
            Err(e) => {
                let msg: String = format!("Failed to subscribe subject:/margin/position:{}", e);
                log::error!("{}", msg);
                handle_db_error(&pool, EXCHANGE, msg).await;
                sleep(RECONNECT_DELAY).await;
                continue;
            }
        }

        log::info!("Subscribed and listening for messages...");

        let event_ping_interval: Interval = interval(PING_INTERVAL);
        tokio::pin!(event_ping_interval);

        if !init_order_execute {
            init_order_execute = true;
            let pool_clone: sqlx::Pool<sqlx::Postgres> = pool.clone();

            tokio::spawn(async move {
                sleep(INIT_ORDER_DELAY).await;
                log::info!("Initializing start orders...");
                match create_init_orders(&pool_clone, EXCHANGE).await {
                    Ok(_) => {}
                    Err(e) => {
                        handle_db_error(&pool_clone, EXCHANGE, e).await;
                    }
                }
            });
        }

        loop {
            tokio::select! {
                // Events
                _ = event_ping_interval.tick() => {
                   match event_ws_write.send(Message::Ping(Bytes::new())).await {
                        Ok(_) => {},
                        Err(e) =>  {
                            let msg: String = format!("Fail send Ping to WebSocket:{}", e);
                            log::error!("{}", msg);
                            handle_db_error(&pool, EXCHANGE, msg).await;
                            break
                        }
                    };
                }

                event = event_ws_read.next() => {
                    match event {
                        Some(Ok(Message::Text(text))) => {
                            match tx_in.send(text.to_string()).await {
                                Ok(_) => {}
                                Err(e) => {
                                    let msg: String = format!("Failed to send to handler, reconnecting...{}", e);
                                    log::error!("{}", msg);
                                    handle_db_error(&pool, EXCHANGE, msg).await;
                                    break;
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                           match event_ws_write.send(Message::Pong(data)).await {
                                Ok(_) => {},
                                Err(e) => {
                                    let msg: String = format!("Fail send Pong to WebSocket:{}", e);
                                    log::error!("{}", msg);
                                    handle_db_error(&pool, EXCHANGE, msg).await;
                                    break
                                }
                           };
                        }
                        Some(Ok(Message::Close(_close))) => {
                            let msg: String = format!("Connection closed by server:");
                            log::error!("{}", msg);
                            handle_db_error(&pool, EXCHANGE, msg).await;
                            break
                        }
                        Some(Err(e)) =>  {
                            let msg: String = format!("WebSocket read error:{}", e);
                            log::error!("{}", msg);
                            handle_db_error(&pool, EXCHANGE, msg).await;
                            break
                        }
                        Some(Ok(_)) => {}
                        None => {
                            let msg: String = format!("WebSocket stream ended");
                            log::error!("{}", msg);
                            handle_db_error(&pool, EXCHANGE, msg).await;
                            break
                        }
                    }
                }
            }
        }

        drop(event_ws_write);
        drop(event_ws_read);

        log::error!("Reconnecting in {} seconds...", RECONNECT_DELAY.as_secs());
        sleep(RECONNECT_DELAY).await;
    }
}
