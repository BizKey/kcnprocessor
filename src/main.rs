use dotenv::dotenv;
use log::{error, info};

mod api {
    pub mod models;
    pub mod requests;
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    dotenv().ok();

    match api::requests::KuCoinClient::new("https://api.kucoin.com".to_string()) {
        Ok(client) => match client.api_v1_bullet_public().await {
            Ok(bullet_public) => {
                info!("Public {:?}", bullet_public);
            }
            Err(e) => {
                error!("Ошибка при выполнении запроса: {}", e)
            }
        },
        Err(e) => {
            error!("Ошибка при выполнении запроса: {}", e)
        }
    };

    match api::requests::KuCoinClient::new("https://api.kucoin.com".to_string()) {
        Ok(client) => match client.api_v1_bullet_private().await {
            Ok(bullet_public) => {
                info!("Private {:?}", bullet_public);
            }
            Err(e) => {
                error!("Ошибка при выполнении запроса: {}", e)
            }
        },
        Err(e) => {
            error!("Ошибка при выполнении запроса: {}", e)
        }
    };

    Ok(())
}
