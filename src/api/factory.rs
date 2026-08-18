use anyhow::{Context, Result};
use std::sync::OnceLock;

use super::auth::AuthCredentials;
use super::client::HttpClient;
use super::endpoints::KuCoinEndpoints;
use super::tools::get_env;

static KUCLIENT: OnceLock<Result<KuCoinEndpoints>> = OnceLock::new();

/// Фабрика для создания клиента KuCoin API
pub struct KuCoinClientFactory;

impl KuCoinClientFactory {
    pub fn get_client() -> Result<&'static KuCoinEndpoints> {
        KUCLIENT
            .get_or_init(|| Self::create_client())
            .as_ref()
            .map_err(|e| anyhow::anyhow!("Fail get or init KuCoinClient: {e}"))
    }

    fn create_client() -> Result<KuCoinEndpoints> {
        let base_url = get_env("KUCOIN_BASE_URL").context("ENV KUCOIN_BASE_URL")?;
        let api_key = get_env("KUCOIN_KEY").context("ENV KUCOIN_KEY")?;
        let api_secret = get_env("KUCOIN_SECRET").context("ENV KUCOIN_SECRET")?;
        let api_passphrase = get_env("KUCOIN_PASS").context("ENV KUCOIN_PASS")?;

        let http_client = HttpClient::new()?;
        let auth = AuthCredentials::new(api_key, api_secret, api_passphrase);

        Ok(KuCoinEndpoints::new(http_client, auth, base_url))
    }
}