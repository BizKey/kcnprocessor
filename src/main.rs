mod api {
    pub mod db;
    pub mod models;
    pub mod requests;
}
mod logic;
use crate::api::db::{clear_orders_ids_for_bots, insert_db_error};

use crate::api::requests::{batch_cancel_stop_orders, get_private_ws_url};
use crate::logic::{auto_clean_account, build_subscription, create_init_orders, process_kcn_msg};
use dotenv::dotenv;

use futures_util::{SinkExt, StreamExt};
use log;

use sqlx::postgres::PgPoolOptions;
use std::env;
use tokio::sync::mpsc;
use tokio::time::{Duration, interval, sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const CLEAR_DELAY: Duration = Duration::from_secs(10);
const PING_INTERVAL: Duration = Duration::from_secs(5);
const INIT_ORDER_DELAY: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();
    dotenv().ok();
    let mut init_order_execute: bool = true;

    let database_url: String = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let exchange: String = "kucoin".to_string();

    let pool = PgPoolOptions::new().max_connections(40).connect(&database_url).await.expect("Failed to create pool");

    // clear orders ids for bots
    match clear_orders_ids_for_bots(&pool, &exchange).await {
        // passed
        Ok(_) => {
            log::info!("clear orders ids bots")
        }
        Err(e) => {
            let msg: String = format!("Failed clear all orders_ids for bots: {}", e);
            log::error!("{}", msg);
            match insert_db_error(&pool, &exchange, &msg).await {
                Ok(_) => {}
                Err(e) => {
                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                    log::error!("{}", msg);
                }
            }
            return Err(e);
        }
    }

    // cancel all stop orders
    match batch_cancel_stop_orders().await {
        // passed
        Ok(_) => {
            log::info!("batch cancel stop orders")
        }
        Err(e) => {
            let msg: String = format!("Failed batch cancel stop orders: {}", e);
            log::error!("{}", msg);
            match insert_db_error(&pool, &exchange, &msg).await {
                Ok(_) => {}
                Err(e) => {
                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                    log::error!("{}", msg);
                }
            }
            return Err(e);
        }
    }
    // repay all liability assets and sell
    loop {
        // passed
        sleep(CLEAR_DELAY).await;
        match auto_clean_account(&pool, &exchange).await {
            Ok(true) => break,
            Ok(false) => {}
            Err(_) => {}
        };
    }

    loop {
        let exchange_for_handler: String = exchange.clone();
        let pool_for_handler = pool.clone();

        // websocket to pg
        let (tx_in, mut rx_in) = mpsc::channel::<String>(1000);

        // Work with income events
        let _ = tokio::spawn(async move {
            loop {
                match rx_in.recv().await {
                    Some(msg) => match process_kcn_msg(&pool_for_handler, &exchange_for_handler, &msg).await {
                        Ok(_) => {}
                        Err(_) => {}
                    },
                    None => {
                        break;
                    }
                }
            }
        });

        // Position/Orders/Balance WS
        let (mut event_ws_write, mut event_ws_read) = match get_private_ws_url().await {
            Ok(event_ws_url) => match connect_async(event_ws_url).await {
                Ok((stream, _)) => stream.split(),
                Err(e) => {
                    log::error!("WebSocket connection failed:{}", e);
                    // sent error to pg
                    sleep(RECONNECT_DELAY).await;
                    continue;
                }
            },
            Err(e) => {
                log::error!("Failed to get WebSocket URL: {}", e);
                // sent error to pg
                sleep(RECONNECT_DELAY).await;
                continue;
            }
        };

        // subscribtion
        for subject in build_subscription() {
            match event_ws_write.send(Message::text(subject.to_string())).await {
                Ok(_) => {
                    log::info!("Subscripte:{}", subject)
                }
                Err(e) => {
                    let msg: String = format!("Failed to subscribe subject:{} {}", subject, e);
                    log::error!("{}", msg);
                    match insert_db_error(&pool, &exchange, &msg).await {
                        Ok(_) => {}
                        Err(e) => {
                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                            log::error!("{}", msg);
                        }
                    }
                    break;
                }
            }
        }

        log::info!("Subscribed and listening for messages...");

        let event_ping_interval = interval(PING_INTERVAL);
        tokio::pin!(event_ping_interval);

        if !init_order_execute {
            init_order_execute = true;
            let pool_clone = pool.clone();
            let exchange_clone: String = exchange.clone();

            tokio::spawn(async move {
                sleep(INIT_ORDER_DELAY).await;
                log::info!("Initializing start orders...");
                match create_init_orders(&pool_clone, &exchange_clone).await {
                    Ok(_) => {}
                    Err(e) => {}
                };
            });
        }

        let mut should_reconnect: bool = false;

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
                                    should_reconnect = true;
                                    break;
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = event_ws_write.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Close(close))) => {
                            let msg: String = format!("Connection closed by server: {:?}", close);
                            log::error!("{}", msg);
                            // sent error to pg
                            should_reconnect = true;
                            break;
                        }
                        Some(Err(e)) => {
                            let msg: String = format!("WebSocket read error: {}", e);
                            log::error!("{}", msg);
                            // sent error to pg
                            should_reconnect = true;
                            break;
                        }
                        Some(Ok(_)) => {}
                        None => {
                            let msg: String = format!("WebSocket stream ended");
                            log::info!("{}", msg);
                            // sent error to pg
                            should_reconnect = true;
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

        if !should_reconnect {
            break;
        }
        log::error!("Reconnecting in {} seconds...", RECONNECT_DELAY.as_secs());
        sleep(RECONNECT_DELAY).await;
    }

    Ok(())
}
