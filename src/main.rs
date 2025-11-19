use dotenv::dotenv;
use futures_util::{SinkExt, StreamExt};
use log::{error, info, trace};
use tokio::sync::mpsc;
use tokio::time::{Duration, interval, sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};
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

    let (tx, mut rx) = mpsc::channel::<String>(1000);

    let handler = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            info!("Processing: {}", msg);
        }
        info!("Message handler finished");
    });

    loop {
        let ws_url = match api::requests::get_private_ws_url().await {
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

        let subscribe = r#"{"id":1545910660739,"type":"subscribe","topic":"/spotMarket/tradeOrdersV2","response":true,"privateChannel":"true"}"#;
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
                                drop(tx);
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
                    trace!("Ping sent");
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
