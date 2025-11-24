use dotenv::dotenv;
use futures_util::{SinkExt, StreamExt};
use log::{error, info, trace};
use sqlx::postgres::PgPoolOptions;
use std::env;
use tokio::sync::mpsc;
use tokio::time::{Duration, interval, sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::api::models::{BalanceData, BalanceRelationContext, KuCoinMessage, OrderData};
mod api {
    pub mod models;
    pub mod requests;
}

const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const PING_INTERVAL: Duration = Duration::from_secs(5);

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

    let handler = tokio::spawn(async move {
        while let Some(msg) = rx_in.recv().await {
            info!("Processing: {}", msg);
            match serde_json::from_str::<KuCoinMessage>(&msg) {
                Ok(kc_msg) => match kc_msg {
                    KuCoinMessage::Welcome(data) => {
                        info!("{:?}", data);
                        match sqlx::query("INSERT INTO events (exchange, msg) VALUES ($1, $2)")
                            .bind(exchange.clone())
                            .bind(serde_json::to_value(&data).unwrap())
                            .execute(&pool)
                            .await
                        {
                            Ok(_) => info!("Success insert events"),
                            Err(e) => error!("Error insert events: {}", e),
                        };
                    }
                    KuCoinMessage::Message(data) => {
                        if data.topic == "/account/balance" {
                            match serde_json::from_value::<BalanceData>(data.data) {
                                Ok(balance) => {
                                    info!("{:?}", balance);
                                    let relation_context = match balance.relationContext {
                                        Some(ctx) => ctx,
                                        None => {
                                            error!("Missing relationContext for balance");
                                            BalanceRelationContext {
                                                symbol: None,
                                                order_id: None,
                                                trade_id: None,
                                            }
                                        }
                                    };
                                    // sent balance to pg
                                    match sqlx::query(
                                        "INSERT INTO balance (exchange, account_id, available, available_change, currency, hold, hold_change, relation_event, relation_event_id, time, total, symbol, order_id, trade_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
                                    )
                                    .bind(exchange.clone())
                                    .bind(balance.account_id)
                                    .bind(balance.available)
                                    .bind(balance.available_change)
                                    .bind(balance.currency)
                                    .bind(balance.hold)
                                    .bind(balance.hold_change)
                                    .bind(balance.relation_event)
                                    .bind(balance.relation_event_id)
                                    .bind(balance.time)
                                    .bind(balance.total)
                                    .bind(relation_context.symbol)
                                    .bind(relation_context.order_id)
                                    .bind(relation_context.trade_id)
                                    .execute(&pool)
                                    .await
                                    {
                                        Ok(_) => info!("Success insert balance"),
                                        Err(e) => {
                                            error!("Error insert balance: {}", e);
                                            match sqlx::query(
                                        "INSERT INTO errors (exchange, msg) VALUES ($1, $2)",
                                    )
                                    .bind(exchange.clone())
                                    .bind(e.to_string())
                                    .execute(&pool)
                                    .await
                                    {
                                        Ok(_) => info!("Success insert error"),
                                        Err(e) => {
                                            error!("Error insert error: {}", e)
                                        }
                                    };
                                        }
                                    };
                                }
                                Err(e) => {
                                    error!("Failed to parse message {}", e);
                                    // sent balance error to pg
                                    match sqlx::query(
                                        "INSERT INTO errors (exchange, msg) VALUES ($1, $2)",
                                    )
                                    .bind(exchange.clone())
                                    .bind(e.to_string())
                                    .execute(&pool)
                                    .await
                                    {
                                        Ok(_) => info!("Success insert parsing error"),
                                        Err(e) => {
                                            error!("Error parsing balance: {}", e)
                                        }
                                    };
                                }
                            }
                        } else if data.topic == "/spotMarket/tradeOrdersV2" {
                            match serde_json::from_value::<OrderData>(data.data) {
                                Ok(order) => {
                                    info!("{:?}", order)
                                }
                                Err(e) => {
                                    error!("Failed to parse message {}", e);
                                    // sent order error to pg
                                    match sqlx::query(
                                        "INSERT INTO errors (exchange, msg) VALUES ($1, $2)",
                                    )
                                    .bind(exchange.clone())
                                    .bind(e.to_string())
                                    .execute(&pool)
                                    .await
                                    {
                                        Ok(_) => info!("Success insert error"),
                                        Err(e) => {
                                            error!("Error insert: {}", e)
                                        }
                                    };
                                }
                            }
                        } else {
                            info!("Unknown topic: {}", data.topic);
                            // sent error to pg
                            match sqlx::query("INSERT INTO errors (exchange, msg) VALUES ($1, $2)")
                                .bind(exchange.clone())
                                .bind(data.topic)
                                .execute(&pool)
                                .await
                            {
                                Ok(_) => info!("Success insert error"),
                                Err(e) => error!("Error insert: {}", e),
                            };
                        }
                    }
                    KuCoinMessage::Ack(data) => {
                        info!("{:?}", data);
                        // sent ack to pg
                        match sqlx::query("INSERT INTO events (exchange, msg) VALUES ($1, $2)")
                            .bind(exchange.clone())
                            .bind(serde_json::to_value(&data).unwrap())
                            .execute(&pool)
                            .await
                        {
                            Ok(_) => info!("Success insert Ack"),
                            Err(e) => error!("Error insert Ack: {}", e),
                        };
                    }
                },
                Err(e) => {
                    error!("Failed to parse message: {} | Raw: {}", e, msg);
                    // sent error to pg
                    match sqlx::query("INSERT INTO errors (exchange, msg) VALUES ($1, $2)")
                        .bind(exchange.clone())
                        .bind(e.to_string())
                        .execute(&pool)
                        .await
                    {
                        Ok(_) => info!("Success insert error"),
                        Err(e) => error!("Error insert: {}", e),
                    };
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

        if let Err(e) = write.send(Message::text(r#"{"id":"subscribe_orders","type":"subscribe","topic":"/spotMarket/tradeOrdersV2","response":true,"privateChannel":"true"}"#)).await {
            error!("Failed to send subscribe message: {}", e);
            // sent error to pg
            sleep(RECONNECT_DELAY).await;
            continue;
        }
        if let Err(e) = write.send(Message::text(r#"{"id":"subscribe_balance","type":"subscribe","topic":"/account/balance","response":true,"privateChannel":"true"}"#)).await {
            error!("Failed to send subscribe message: {}", e);
            // sent error to pg
            sleep(RECONNECT_DELAY).await;
            continue;
        }
        if let Err(e) = write.send(Message::text(r#"{"id":"subscribe_position","type":"subscribe","topic":"/margin/position","response":true,"privateChannel":"true"}"#)).await {
            error!("Failed to send subscribe message: {}", e);
            // sent error to pg
            sleep(RECONNECT_DELAY).await;
            continue;
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
