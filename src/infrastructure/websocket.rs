// src/infrastructure/websocket.rs

use anyhow::Result;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::time::{interval, sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info};

use crate::api::requests::api_v1_bullet_private_post;
use crate::constants::{PING_INTERVAL, RECONNECT_DELAY};
use crate::core::repository_traits::{
    BalanceRepositoryFull, BotRepositoryFull, EventRepositoryFull, MessageRepositoryFull,
    OrderRepositoryFull, PositionRepositoryFull, SymbolRepositoryFull,
};
use crate::logic::handlers::spawn_process_kcn_msg;

pub async fn run_websocket_loop<B, O, S, Bal, P, E, M>(
    bot_repo: B,
    order_repo: O,
    symbol_repo: S,
    balance_repo: Bal,
    position_repo: P,
    event_repo: E,
    message_repo: M,
) -> Result<()>
where
    B: BotRepositoryFull + Clone + Send + Sync + 'static,
    O: OrderRepositoryFull + Clone + Send + Sync + 'static,
    S: SymbolRepositoryFull + Clone + Send + Sync + 'static,
    Bal: BalanceRepositoryFull + Clone + Send + Sync + 'static,
    P: PositionRepositoryFull + Clone + Send + Sync + 'static,
    E: EventRepositoryFull + Clone + Send + Sync + 'static,
    M: MessageRepositoryFull + Clone + Send + Sync + 'static,
{
    let (tx_in, rx_in) = mpsc::channel::<Bytes>(8192);

    // Запускаем обработчик сообщений с клонированными репозиториями
    tokio::spawn(async move {
        spawn_process_kcn_msg(
            rx_in,
            bot_repo.clone(),
            order_repo.clone(),
            symbol_repo.clone(),
            balance_repo.clone(),
            position_repo.clone(),
            event_repo.clone(),
            message_repo.clone(),
        )
        .await;
    });

    loop {
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
                _ = event_ping_interval.tick() => {
                   match event_ws_write.send(Message::Ping(Bytes::new())).await {
                        Ok(_) => debug!("Ping sent"),
                        Err(e) => {
                            error!("Fail send Ping to WebSocket:{}", e);
                            break;
                        }
                    };
                }

                event = event_ws_read.next() => {
                    let event = match event {
                        Some(event) => event,
                        None => {
                            error!("WebSocket event is None, connection closed");
                            break;
                        }
                    };

                    let event = match event {
                        Ok(event) => event,
                        Err(e) => {
                            error!("WebSocket read error: {}", e);
                            break;
                        }
                    };

                    match event {
                        Message::Text(text) => {
                            if let Err(e) = tx_in.send(Bytes::from(text)).await {
                                error!("Failed to send message to handler: {}", e);
                                break;
                            }
                        }
                        Message::Binary(data) => {
                            debug!("Received binary message, size: {} bytes", data.len());
                            if let Err(e) = tx_in.send(data).await {
                                error!("Failed to send binary message to handler: {}", e);
                                break;
                            }
                        }
                        Message::Ping(_) => debug!("Received Ping, auto-reply with Pong"),
                        Message::Pong(_) => debug!("Received Pong"),
                        Message::Close(frame) => {
                            match frame {
                                Some(frame) => error!("Connection closed by server: code={}, reason={}", frame.code, frame.reason),
                                None => error!("Connection closed by server"),
                            };
                            break;
                        }
                        Message::Frame(_) => debug!("Received raw frame"),
                    }
                }
            }
        }

        error!("Reconnecting in {} seconds...", RECONNECT_DELAY.as_secs());
        sleep(RECONNECT_DELAY).await;
    }
}
