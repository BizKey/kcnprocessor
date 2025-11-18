use dotenv::dotenv;
use futures_util::{SinkExt, StreamExt};
use log::{error, info};
use std::env;
use tokio::sync::mpsc;
use tokio::time::{Duration, interval, sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};
mod api {
    pub mod models;
    pub mod requests;
}

const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const PING_INTERVAL: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();
    dotenv().ok();

    let (tx, mut rx) = mpsc::channel::<String>(1000);

    let handler = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            println!("Processing: {}", msg);
        }
        info!("Message handler finished");
    });

    loop {
        let ws_url = match get_public_ws_url().await {
            Ok(url) => url,
            Err(e) => {
                error!("Failed to get WebSocket URL: {}", e);
                sleep(RECONNECT_DELAY).await;
                continue;
            }
        };

        let ws_stream = match connect_async(ws_url).await {
            Ok((stream, _)) => stream,
            Err(e) => {
                error!("WebSocket connection failed: {}", e);
                sleep(RECONNECT_DELAY).await;
                continue;
            }
        };

        let (mut write, mut read) = ws_stream.split();

        let subscribe = r#"{"id":"1","type":"subscribe","topic":"/market/ticker:BTC-USDT","privateChannel":false,"response":true}"#;
        if let Err(e) = write.send(Message::text(subscribe)).await {
            error!("Failed to send subscribe message: {}", e);
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
                            if tx.send(text.to_string()).await.is_err() {
                                // Обработчик завершился — выходим полностью
                                drop(tx);
                                let _ = handler.await;
                                return Ok(());
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = write.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Close(close))) => {
                            info!("Connection closed by server: {:?}", close);
                            should_reconnect = true;
                            break;
                        }
                        Some(Err(e)) => {
                            error!("WebSocket read error: {}", e);
                            should_reconnect = true;
                            break;
                        }
                        Some(Ok(_)) => {}
                        None => {
                            info!("WebSocket stream ended");
                            should_reconnect = true;
                            break;
                        }
                    }
                }
                _ = ping_interval.tick() => {
                    let _ = write.send(Message::Ping(vec![].into())).await;
                }
            }
        }
        if should_reconnect {
            error!("Reconnecting in {} seconds...", RECONNECT_DELAY.as_secs());
            sleep(RECONNECT_DELAY).await;
        } else {
            break;
        }
    }

    drop(tx);

    let _ = handler.await;
    info!("Application shutdown complete");

    Ok(())
}
async fn get_public_ws_url() -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let client = api::requests::KuCoinClient::new("https://api.kucoin.com".to_string())?;
    let bullet_public = client.api_v1_bullet_public().await?;
    bullet_public
        .data
        .instanceServers
        .first()
        .map(|s| format!("{}?token={}", s.endpoint, bullet_public.data.token))
        .ok_or_else(|| "No instance servers in bullet response".into())
}
