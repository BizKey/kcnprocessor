use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;

#[async_trait]
pub trait KuCoinClient: Send + Sync {
    async fn get_websocket_url(&self) -> Result<String>;
}
