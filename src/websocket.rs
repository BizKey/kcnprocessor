use anyhow::Result;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::time::{interval, sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info};

use crate::api::requests::api_v1_bullet_private_post;
use crate::constants::*;
use crate::logic::spawn_process_kcn_msg;
use sqlx::PgPool;

pub async fn run_websocket_loop(pool: PgPool) -> Result<()> {
    let (tx_in, rx_in) = mpsc::channel::<Bytes>(8192);

    let pool_process = pool.clone();
    let _spawn_process_kcn_msg_point =
        tokio::spawn(async move { spawn_process_kcn_msg(&pool_process, rx_in).await });

    loop {
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

        // subscriptions
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

        let event_ping_interval = interval(PING_INTERVAL);
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

        error!("Reconnecting in {} seconds...", RECONNECT_DELAY.as_secs());
        sleep(RECONNECT_DELAY).await;
    }
}
