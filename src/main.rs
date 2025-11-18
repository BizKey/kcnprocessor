use dotenv::dotenv;
use futures_util::{SinkExt, StreamExt};
use log::{error, info};
use std::env;
use tokio::sync::mpsc;
use tokio::time::{Duration, interval};
use tokio_tungstenite::{connect_async, tungstenite::Message};
mod api {
    pub mod models;
    pub mod requests;
}

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

    let mut ws_url: String = "".to_string();

    let ws_url = {
        match api::requests::KuCoinClient::new("https://api.kucoin.com".to_string()) {
            Ok(client) => match client.api_v1_bullet_public().await {
                Ok(bullet_public) => {
                    info!("Public {:?}", bullet_public);
                    bullet_public
                        .data
                        .instanceServers
                        .first()
                        .map(|s| format!("{}?token={}", s.endpoint, bullet_public.data.token))
                        .unwrap_or_default()
                }
                Err(e) => {
                    error!("Ошибка при выполнении запроса: {}", e);
                    return Err(e);
                }
            },
            Err(e) => {
                error!("Ошибка при выполнении запроса: {}", e);
                return Err(e.into());
            }
        }
    };
    if ws_url.is_empty() {
        return Err("Не удалось получить WebSocket URL".into());
    }
    // match api::requests::KuCoinClient::new("https://api.kucoin.com".to_string()) {
    //     Ok(client) => match client.api_v1_bullet_private().await {
    //         Ok(bullet_public) => {
    //             info!("Public {:?}", bullet_public);
    //             for instance in bullet_public.data.instanceServers.iter() {
    //                 ws_url = format!("{}?token={}", instance.endpoint, bullet_public.data.token);
    //                 break;
    //             }
    //         }
    //         Err(e) => {
    //             error!("Ошибка при выполнении запроса: {}", e)
    //         }
    //     },
    //     Err(e) => {
    //         error!("Ошибка при выполнении запроса: {}", e)
    //     }
    // };

    let (ws_stream, _) = connect_async(ws_url).await.map_err(|e| {
        error!("WebSocket connection failed: {}", e);
        e
    })?;
    let (mut write, mut read) = ws_stream.split();

    let subscribe = r#"{"id":"1","type":"subscribe","topic":"/market/ticker:BTC-USDT","privateChannel":false,"response":true}"#;
    write.send(Message::text(subscribe)).await?;

    let ping_interval = interval(Duration::from_secs(10));
    tokio::pin!(ping_interval);

    loop {
        tokio::select! {
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        if tx.send(text.to_string()).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        write.send(Message::Pong(data)).await?;
                    }
                    Some(Ok(Message::Close(close))) => {
                        info!("Connection closed: {:?}", close);
                        break;
                    }
                    Some(Err(e)) => {
                        error!("WebSocket read error: {}", e);
                        break;
                    }
                    _ => {}
                }
            }

            _ = ping_interval.tick() => {
                write.send(Message::Ping(vec![].into())).await?;
            }
        }
    }
    drop(tx);

    let _ = handler.await;
    info!("Application shutdown complete");

    Ok(())
}
