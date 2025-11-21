use dotenv::dotenv;
use futures_util::{SinkExt, StreamExt};
use log::{error, info, trace};
use sqlx::postgres::PgPoolOptions;
use std::env;
use tokio::sync::mpsc;
use tokio::time::{Duration, interval, sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::api::models::{BalanceData, KuCoinMessage, OrderData};
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
                            .bind("kucoin")
                            .bind("test")
                            .execute(&pool)
                            .await
                        {
                            Ok(_) => {
                                info!("Success insert kucoin test")
                            }
                            Err(e) => error!("Error on bulk insert tickers to db: {}", e),
                        };
                    }
                    KuCoinMessage::Message(data) => {
                        if data.topic == "/account/balance" {
                            match serde_json::from_value::<BalanceData>(data.data) {
                                Ok(balance) => {
                                    info!("{:?}", balance)
                                    // sent balance to pg
                                }
                                Err(e) => {
                                    error!("Failed to parse message {}", e);
                                    // sent balance error to pg
                                    match sqlx::query(
                                        "INSERT INTO errors (exchange, msg) VALUES ($1, $2)",
                                    )
                                    .bind("kucoin")
                                    .bind("test")
                                    .execute(&pool)
                                    .await
                                    {
                                        Ok(_) => {
                                            info!("Success insert kucoin test")
                                        }
                                        Err(e) => {
                                            error!("Error on bulk insert tickers to db: {}", e)
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
                                    .bind("kucoin")
                                    .bind("test")
                                    .execute(&pool)
                                    .await
                                    {
                                        Ok(_) => {
                                            info!("Success insert kucoin test")
                                        }
                                        Err(e) => {
                                            error!("Error on bulk insert tickers to db: {}", e)
                                        }
                                    };
                                }
                            }
                        } else {
                            info!("Unknown topic: {}", data.topic);
                            // sent error to pg
                            match sqlx::query("INSERT INTO errors (exchange, msg) VALUES ($1, $2)")
                                .bind("kucoin")
                                .bind("test")
                                .execute(&pool)
                                .await
                            {
                                Ok(_) => {
                                    info!("Success insert kucoin test")
                                }
                                Err(e) => error!("Error on bulk insert tickers to db: {}", e),
                            };
                        }
                    }
                    KuCoinMessage::Ack(data) => {
                        info!("{:?}", data);
                        // sent ack to pg
                        match sqlx::query("INSERT INTO events (exchange, msg) VALUES ($1, $2)")
                            .bind("kucoin")
                            .bind("test")
                            .execute(&pool)
                            .await
                        {
                            Ok(_) => {
                                info!("Success insert kucoin test")
                            }
                            Err(e) => error!("Error on bulk insert tickers to db: {}", e),
                        };
                    }
                },
                Err(e) => {
                    error!("Failed to parse message: {} | Raw: {}", e, msg);
                    // sent error to pg
                    match sqlx::query("INSERT INTO errors (exchange, msg) VALUES ($1, $2)")
                        .bind("kucoin")
                        .bind("test")
                        .execute(&pool)
                        .await
                    {
                        Ok(_) => {
                            info!("Success insert kucoin test")
                        }
                        Err(e) => error!("Error on bulk insert tickers to db: {}", e),
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

        let subscribe_orders = r#"{"id":"subscribe_orders","type":"subscribe","topic":"/spotMarket/tradeOrdersV2","response":true,"privateChannel":"true"}"#;
        if let Err(e) = write.send(Message::text(subscribe_orders)).await {
            error!("Failed to send subscribe message: {}", e);
            // sent error to pg
            sleep(RECONNECT_DELAY).await;
            continue;
        }
        let subscribe_balance = r#"{"id":"subscribe_balance","type":"subscribe","topic":"/account/balance","response":true,"privateChannel":"true"}"#;
        if let Err(e) = write.send(Message::text(subscribe_balance)).await {
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
