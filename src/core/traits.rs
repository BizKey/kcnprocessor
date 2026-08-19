use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait KuCoinClient: Send + Sync {
    async fn get_websocket_url(&self) -> Result<String>;
}
