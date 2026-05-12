mod api {
    pub mod db;
    pub mod models;
    pub mod requests;
}
mod logic;
use crate::api::db::{clear_orders_ids_for_bots, get_all_bots_for_trade, insert_db_balance, insert_db_error, insert_db_event};
use crate::api::models::{AdvancedOrders, BalanceData, KuCoinMessage, OrderData, PositionData};
use crate::api::requests::{batch_cancel_stop_orders, create_repay_order, get_private_ws_url};
use crate::logic::{auto_clean_account, build_subscription, handle_advanced_orders, handle_position_event, handle_trade_order_event, make_random_trade};
use dotenv::dotenv;

use futures_util::{SinkExt, StreamExt};
use log::{error, info};
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use std::env;
use tokio::sync::mpsc;
use tokio::time::{Duration, interval, sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const CLEAR_DELAY: Duration = Duration::from_secs(3);
const PING_INTERVAL: Duration = Duration::from_secs(5);
const INIT_ORDER_DELAY: Duration = Duration::from_millis(5000);
const BOT_INIT_DELAY: Duration = Duration::from_millis(5000);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();
    dotenv().ok();
    let mut init_order_execute: bool = false;

    let database_url: String = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let exchange: String = "kucoin".to_string();

    let pool = PgPoolOptions::new().max_connections(40).connect(&database_url).await.expect("Failed to create pool");

    // clear orders ids for bots
    match clear_orders_ids_for_bots(&pool, &exchange).await {
        Ok(_) => {}
        Err(e) => {
            let msg: String = format!("Failed clear all orders_ids for bots: {}", e);
            error!("{}", msg);
            match insert_db_error(&pool, &exchange, &msg).await {
                Ok(_) => {}
                Err(e) => {
                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                    error!("{}", msg);
                }
            }
            return Err(e);
        }
    }

    // cancel all stop orders
    match batch_cancel_stop_orders().await {
        Ok(_) => {}
        Err(e) => {
            let msg: String = format!("Failed batch cancel stop orders: {}", e);
            error!("{}", msg);
            match insert_db_error(&pool, &exchange, &msg).await {
                Ok(_) => {}
                Err(e) => {
                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                    error!("{}", msg);
                }
            }
            return Err(e);
        }
    }
    // repay all liability assets and sell
    loop {
        sleep(CLEAR_DELAY).await;
        match auto_clean_account(&pool, &exchange).await {
            Ok(true) => break,
            Ok(false) => {}
            Err(e) => {}
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
                    Some(msg) => {
                        match serde_json::from_str::<KuCoinMessage>(&msg) {
                            Ok(kc_msg) => match kc_msg {
                                KuCoinMessage::Welcome(data) => {
                                    match serde_json::to_value(data) {
                                        Ok(v) => match insert_db_event(&pool_for_handler, &exchange_for_handler, v).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                let msg: String = format!("Failed insert_db_event {}", e);
                                                error!("{}", msg);
                                                match insert_db_error(&pool_for_handler, &exchange_for_handler, &msg).await {
                                                    Ok(_) => {}
                                                    Err(e) => {
                                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                        error!("{}", msg);
                                                    }
                                                }
                                            }
                                        },
                                        Err(e) => {
                                            error!("Failed to serialize event: {}", e);
                                            return;
                                        }
                                    };
                                }
                                KuCoinMessage::Message(data) => {
                                    if data.topic == "/account/balance" {
                                        match BalanceData::deserialize(&data.data) {
                                            Ok(balance) => {
                                                // sent balance to pg
                                                match insert_db_balance(&pool_for_handler, &exchange_for_handler, balance).await {
                                                    Ok(_) => {}
                                                    Err(e) => {
                                                        let msg: String = format!("Failed to insert balance into DB: {}", e);
                                                        error!("{}", msg);

                                                        match insert_db_error(&pool_for_handler, &exchange_for_handler, &msg).await {
                                                            Ok(_) => {}
                                                            Err(e) => {
                                                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                                error!("{}", msg);
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                info!("{:?}", data.data);
                                                // sent balance parse error to pg
                                                let msg: String = format!("Failed to parse message {}", e);
                                                error!("{}", msg);
                                                match insert_db_error(&pool_for_handler, &exchange_for_handler, &msg).await {
                                                    Ok(_) => {}
                                                    Err(e) => {
                                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                        error!("{}", msg);
                                                    }
                                                }
                                            }
                                        }
                                    } else if data.topic == "/spotMarket/tradeOrdersV2" {
                                        info!("{}", &data.data);
                                        match OrderData::deserialize(&data.data) {
                                            Ok(order) => {
                                                // order magic
                                                handle_trade_order_event(order, &pool_for_handler, &exchange_for_handler).await
                                            }
                                            Err(e) => {
                                                info!("{:?}", data.data);

                                                // sent order error to pg
                                                let msg: String = format!("Failed to parse message {}", e);
                                                error!("{}", msg);
                                                match insert_db_error(&pool_for_handler, &exchange_for_handler, &msg).await {
                                                    Ok(_) => {}
                                                    Err(e) => {
                                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                        error!("{}", msg);
                                                    }
                                                }
                                            }
                                        }
                                    } else if data.topic == "/spotMarket/advancedOrders" {
                                        // stop orders and other
                                        info!("{}", &data.data);
                                        match AdvancedOrders::deserialize(&data.data) {
                                            Ok(order) => {
                                                // watch order event
                                                handle_advanced_orders(order, &pool_for_handler, &exchange_for_handler).await
                                            }
                                            Err(e) => {
                                                info!("{:?}", data.data);

                                                // sent order error to pg
                                                let msg: String = format!("Failed to parse message {}", e);
                                                error!("{}", msg);
                                                match insert_db_error(&pool_for_handler, &exchange_for_handler, &msg).await {
                                                    Ok(_) => {}
                                                    Err(e) => {
                                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                        error!("{}", msg);
                                                    }
                                                }
                                            }
                                        }
                                    } else if data.topic == "/margin/position" {
                                        // save to db position
                                        // repay debt
                                        match PositionData::deserialize(&data.data) {
                                            Ok(position) => handle_position_event(position, &pool_for_handler, &exchange_for_handler).await,
                                            Err(e) => {
                                                info!("{:?}", data.data);
                                                // sent order error to pg
                                                let msg: String = format!("Failed to parse message {}", e);
                                                error!("{}", msg);
                                                match insert_db_error(&pool_for_handler, &exchange_for_handler, &msg).await {
                                                    Ok(_) => {}
                                                    Err(e) => {
                                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                        error!("{}", msg);
                                                    }
                                                }
                                            }
                                        }
                                    } else {
                                        info!("Unknown topic: {}", data.topic);
                                        // sent error to pg
                                        match insert_db_error(&pool_for_handler, &exchange_for_handler, &data.topic).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                error!("{}", msg);
                                            }
                                        }
                                    }
                                }
                                KuCoinMessage::Ack(data) => {
                                    info!("{:?}", data);
                                    // sent ack to pg
                                    match serde_json::to_value(data) {
                                        Ok(v) => match insert_db_event(&pool_for_handler, &exchange_for_handler, v).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                let msg: String = format!("Failed insert_db_event: {}", e);
                                                error!("{}", msg);
                                                match insert_db_error(&pool_for_handler, &exchange_for_handler, &msg).await {
                                                    Ok(_) => {}
                                                    Err(e) => {
                                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                        error!("{}", msg);
                                                    }
                                                }
                                            }
                                        },
                                        Err(e) => {
                                            error!("Failed to serialize event: {}", e);
                                            return;
                                        }
                                    };
                                }
                                KuCoinMessage::Error(data) => {
                                    info!("{:?}", data);
                                    // sent error to pg
                                    match insert_db_error(&pool_for_handler, &exchange_for_handler, &data.data).await {
                                        Ok(_) => {}
                                        Err(e) => {
                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                            error!("{}", msg);
                                        }
                                    }
                                }
                                KuCoinMessage::Unknown => {
                                    info!("Unknown WS message type");
                                }
                            },
                            Err(e) => {
                                // sent error to pg
                                let msg: String = format!("Failed to parse message: {} | Raw: {}", e, msg);
                                error!("{}", msg);
                                match insert_db_error(&pool_for_handler, &exchange_for_handler, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        error!("{}", msg);
                                    }
                                }
                            }
                        }
                    }
                    None => {
                        break;
                    }
                }
            }

            info!("Message handler finished");
        });

        // Position/Orders/Balance WS
        let event_ws_url: String = match get_private_ws_url().await {
            Ok(url) => url,
            Err(e) => {
                error!("Failed to get WebSocket URL: {}", e);
                // sent error to pg
                sleep(RECONNECT_DELAY).await;
                continue;
            }
        };

        let event_ws_stream = match connect_async(event_ws_url).await {
            Ok((stream, _)) => stream,
            Err(e) => {
                error!("WebSocket connection failed: {}", e);
                // sent error to pg
                sleep(RECONNECT_DELAY).await;
                continue;
            }
        };

        let (mut event_ws_write, mut event_ws_read) = event_ws_stream.split();

        // subscribtion
        for sub in build_subscription() {
            match event_ws_write.send(Message::text(sub.to_string())).await {
                Ok(_) => {}
                Err(e) => {
                    let msg: String = format!("Failed to subscribe: {}", e);
                    error!("{}", msg);
                    match insert_db_error(&pool, &exchange, &msg).await {
                        Ok(_) => {}
                        Err(e) => {
                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                            error!("{}", msg);
                        }
                    }
                    break;
                }
            }
        }

        info!("Subscribed and listening for messages...");

        let event_ping_interval = interval(PING_INTERVAL);
        tokio::pin!(event_ping_interval);

        let mut should_reconnect: bool = false;

        if !init_order_execute {
            init_order_execute = true;
            let pool_clone = pool.clone();
            let exchange_clone = exchange.clone();

            tokio::spawn(async move {
                sleep(INIT_ORDER_DELAY).await;
                info!("Initializing start orders...");
                match get_all_bots_for_trade(&pool_clone, &exchange_clone).await {
                    Ok(trade_bots) => {
                        for trade_bot in trade_bots.iter() {
                            sleep(BOT_INIT_DELAY).await;
                            match trade_bot.balance.parse::<f64>() {
                                Ok(token_funds) => match make_random_trade(&pool_clone, &exchange_clone, token_funds, trade_bot.id).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Error in make_random_trade: {}", e);
                                        error!("{}", msg);
                                        match insert_db_error(&pool_clone, &exchange_clone, &msg).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                error!("{}", msg);
                                            }
                                        }
                                    }
                                },
                                Err(e) => {
                                    let msg: String = format!("Failed parse balance: {} {}", trade_bot.balance, e);
                                    error!("{}", msg);
                                    match insert_db_error(&pool_clone, &exchange_clone, &msg).await {
                                        Ok(_) => {}
                                        Err(e) => {
                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                            error!("{}", msg);
                                        }
                                    }
                                }
                            }
                        }
                        info!("All bots initialized!");
                    }
                    Err(e) => {
                        let msg: String = format!("Failed get_all_bots_for_trade: {}", e);
                        error!("{}", msg);
                        match insert_db_error(&pool_clone, &exchange_clone, &msg).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                error!("{}", msg);
                            }
                        }
                    }
                }
            });
        }

        loop {
            tokio::select! {
                // Events
                event_msg = event_ws_read.next() => {
                    match event_msg {
                        Some(Ok(Message::Text(text))) => {
                            match tx_in.send(text.to_string()).await {
                                Ok(_) => {}
                                Err(e) => {
                                    error!("Failed to send to handler, reconnecting...{}", e);
                                    should_reconnect = true;
                                    break;
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = event_ws_write.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Close(close))) => {
                            error!("Connection closed by server: {:?}", close);
                            // sent error to pg
                            should_reconnect = true;
                            break;
                        }
                        Some(Err(e)) => {
                            error!("WebSocket read error: {}", e);
                            // sent error to pg
                            should_reconnect = true;
                            break;
                        }
                        Some(Ok(_)) => {}
                        None => {
                            info!("WebSocket stream ended");
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
        error!("Reconnecting in {} seconds...", RECONNECT_DELAY.as_secs());
        sleep(RECONNECT_DELAY).await;
    }

    Ok(())
}
