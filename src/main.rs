use crate::api::db::{
    delete_db_orderactive, insert_db_balance, insert_db_error, insert_db_event,
    insert_db_orderactive, insert_db_orderevent,
};
use crate::api::models::{BalanceData, KuCoinMessage, OrderData};
use dotenv::dotenv;
use futures_util::{SinkExt, StreamExt};
use log::{error, info, trace};
use sqlx::postgres::PgPoolOptions;
use std::env;
use tokio::sync::mpsc;
use tokio::time::{Duration, interval, sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};
mod api {
    pub mod db;
    pub mod models;
    pub mod requests;
}

const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const PING_INTERVAL: Duration = Duration::from_secs(5);

fn build_subscription() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({"id":"subscribe_orders","type":"subscribe","topic":"/spotMarket/tradeOrdersV2","response":true,"privateChannel":"true"}),
        serde_json::json!({"id":"subscribe_balance","type":"subscribe","topic":"/account/balance","response":true,"privateChannel":"true"}),
        serde_json::json!({"id":"subscribe_position","type":"subscribe","topic":"/margin/position","response":true,"privateChannel":"true"}),
    ]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();
    dotenv().ok();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let exchange: String = "kucoin".to_string();

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await
        .expect("Failed to create pool");

    // websocket to pg
    let (tx_in, mut rx_in) = mpsc::channel::<String>(1000);
    // pg to websocket
    let (tx_out, mut rx_out) = mpsc::channel::<String>(1000);

    let exchange_for_handler = exchange.clone();
    let pool_for_handler = pool.clone();
    let handler = tokio::spawn(async move {
        while let Some(msg) = rx_in.recv().await {
            info!("Processing: {}", msg);
            match serde_json::from_str::<KuCoinMessage>(&msg) {
                Ok(kc_msg) => match kc_msg {
                    KuCoinMessage::Welcome(data) => {
                        info!("{:?}", data);
                        insert_db_event(&pool_for_handler, &exchange_for_handler, &data).await;
                    }
                    KuCoinMessage::Message(data) => {
                        if data.topic == "/account/balance" {
                            match serde_json::from_value::<BalanceData>(data.data) {
                                Ok(balance) => {
                                    info!("{:?}", balance);
                                    // sent balance to pg
                                    insert_db_balance(
                                        &pool_for_handler,
                                        &exchange_for_handler,
                                        balance,
                                    )
                                    .await;
                                }
                                Err(e) => {
                                    error!("Failed to parse message {}", e);
                                    // sent balance error to pg
                                    insert_db_error(
                                        &pool_for_handler,
                                        &exchange_for_handler,
                                        &e.to_string(),
                                    )
                                    .await;
                                }
                            }
                        } else if data.topic == "/spotMarket/tradeOrdersV2" {
                            match serde_json::from_value::<OrderData>(data.data) {
                                Ok(order) => {
                                    info!("{:?}", order);
                                    // sent order to pg
                                    insert_db_orderevent(
                                        &pool_for_handler,
                                        &exchange_for_handler,
                                        &order,
                                    )
                                    .await;
                                    if order.order_type == "open" && order.status == "open" {
                                        // order in order book
                                        // add order to active orders
                                        insert_db_orderactive(
                                            &pool_for_handler,
                                            &exchange_for_handler,
                                            &order,
                                        )
                                        .await;
                                    };
                                    if order.order_type == "filled" && order.status == "done" {
                                        info!(
                                            "Order fully filled, removing from active: {}",
                                            order.order_id
                                        );
                                        delete_db_orderactive(&pool_for_handler, &order.order_id)
                                            .await;

                                        // order all filled
                                        // get order from db and cancel them
                                        // create new order
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to parse message {}", e);
                                    // sent order error to pg
                                    insert_db_error(
                                        &pool_for_handler,
                                        &exchange_for_handler,
                                        &e.to_string(),
                                    )
                                    .await;
                                }
                            }
                        } else {
                            info!("Unknown topic: {}", data.topic);
                            // sent error to pg
                            insert_db_error(&pool_for_handler, &exchange_for_handler, &data.topic)
                                .await;
                        }
                    }
                    KuCoinMessage::Ack(data) => {
                        info!("{:?}", data);
                        // sent ack to pg
                        insert_db_event(&pool_for_handler, &exchange_for_handler, &data).await;
                    }
                    KuCoinMessage::Error(data) => {
                        info!("{:?}", data);
                        // sent error to pg
                        insert_db_error(&pool_for_handler, &exchange_for_handler, &data.data).await;
                    }
                },
                Err(e) => {
                    error!("Failed to parse message: {} | Raw: {}", e, msg);
                    // sent error to pg
                    insert_db_error(&pool_for_handler, &exchange_for_handler, &e.to_string()).await;
                }
            }
        }
        info!("Message handler finished");
    });

    loop {
        let ws_url = match api::requests::get_private_ws_url().await {
            Ok(url) => url,
            Err(e) => {
                error!("Failed to get WebSocket URL: {}", e);
                // sent error to pg
                sleep(RECONNECT_DELAY).await;
                continue;
            }
        };

        let ws_stream = match connect_async(ws_url).await {
            Ok((stream, _)) => stream,
            Err(e) => {
                error!("WebSocket connection failed: {}", e);
                // sent error to pg
                sleep(RECONNECT_DELAY).await;
                continue;
            }
        };

        let (mut write, mut read) = ws_stream.split();

        for sub in build_subscription() {
            if let Err(e) = write.send(Message::text(sub.to_string())).await {
                error!("Failed to subscribe: {}", e);
                insert_db_error(&pool, &exchange, &e.to_string()).await;
                break;
            }
        }

        info!("Subscribed and listening for messages...");

        let ping_interval = interval(PING_INTERVAL);
        tokio::pin!(ping_interval);

        let mut should_reconnect = false;

        loop {
            tokio::select! {
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if tx_in.send(text.to_string()).await.is_err() {
                                drop(tx_in);
                                let _ = handler.await;
                                return Ok(());
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = write.send(Message::Pong(data)).await;
                            trace!("Ping recv");
                        }
                        Some(Ok(Message::Pong(_))) => {
                            trace!("Pong recv");
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
                _ = ping_interval.tick() => {
                    trace!("Ping sent");
                    let _ = write.send(Message::Ping(vec![].into())).await;
                }
            }
        }
        if should_reconnect {
            error!("Reconnecting in {} seconds...", RECONNECT_DELAY.as_secs());
            // sent error to pg
            sleep(RECONNECT_DELAY).await;
        } else {
            break;
        }
    }

    drop(tx_in);

    let _ = handler.await;
    info!("Application shutdown complete");

    Ok(())
}
