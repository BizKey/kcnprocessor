use anyhow::Result;
use async_trait::async_trait;

use crate::api::requests::api_v1_bullet_private_post;
use crate::api::utils::get_env;
use crate::core::traits::KuCoinClient;

pub struct KuCoinRestClient {
    _api_key: String,
    _api_secret: String,
    _api_passphrase: String,
    _base_url: String,
}

impl KuCoinRestClient {
    pub fn new() -> Result<Self> {
        Ok(Self {
            _api_key: get_env("KUCOIN_KEY")?,
            _api_secret: get_env("KUCOIN_SECRET")?,
            _api_passphrase: get_env("KUCOIN_PASS")?,
            _base_url: get_env("KUCOIN_BASE_URL")?,
        })
    }
}

#[async_trait]
impl KuCoinClient for KuCoinRestClient {
    async fn get_websocket_url(&self) -> Result<String> {
        api_v1_bullet_private_post().await
    }
}
