use crate::api::auth::AuthCredentials;
use crate::api::client::endpoints::KuCoinEndpoints;
use crate::api::client::http::HttpClient;
use crate::api::models::{
    ApiV1MarketOrderbookLevel1ResData, ApiV3AccountsUniversalTransferResData,
    ApiV3HfMarginStopOrderCancelByClientOidResData, ApiV3HfMarginStopOrderCancelByIdResData,
    ApiV3HfMarginStopOrdersResData, ApiV3MarginRepayResData, MakeOrderResData,
    MakeStopOrderResData, MarginAccountData,
};
use crate::api::utils::tools::get_env;
use anyhow::Result;
use std::sync::OnceLock;
// Re-export для обратной совместимости

// Модули
mod bullet;
mod margin;
mod market;
mod order;

// Re-export функций
pub use bullet::bullet_private_post;
pub use margin::margin_accounts_get;
pub use margin::margin_repay_post;
pub use market::market_orderbook_level1_get;
pub use order::accounts_universal_transfer_post;
pub use order::hf_margin_order_post;
pub use order::hf_margin_stop_order_cancel_by_client_oid_delete;
pub use order::hf_margin_stop_order_cancel_by_id_delete;
pub use order::hf_margin_stop_order_post;
pub use order::hf_margin_stop_orders_get;

// Фабрика клиента
static KUCLIENT: OnceLock<Result<KuCoinEndpoints>> = OnceLock::new();

fn get_client() -> Result<&'static KuCoinEndpoints> {
    KUCLIENT
        .get_or_init(|| create_client())
        .as_ref()
        .map_err(|e| anyhow::anyhow!("Fail get or init KuCoinClient: {e}"))
}

fn create_client() -> Result<KuCoinEndpoints> {
    let base_url = get_env("KUCOIN_BASE_URL")?;
    let api_key = get_env("KUCOIN_KEY")?;
    let api_secret = get_env("KUCOIN_SECRET")?;
    let api_passphrase = get_env("KUCOIN_PASS")?;

    let http_client = HttpClient::new()?;
    let auth = AuthCredentials::new(api_key, api_secret, api_passphrase);

    Ok(KuCoinEndpoints::new(http_client, auth, base_url))
}

// ============================================================================
// Обёртки для API функций (для обратной совместимости с старым кодом)
// ============================================================================

pub async fn api_v1_bullet_private_post() -> Result<String> {
    let client = get_client()?;
    bullet_private_post(client).await
}

pub async fn api_v3_margin_accounts_get(query_params: &str) -> Result<MarginAccountData> {
    let client = get_client()?;
    margin_accounts_get(client, query_params).await
}

pub async fn api_v3_margin_repay_post(body: &str) -> Result<Option<ApiV3MarginRepayResData>> {
    let client = get_client()?;
    margin_repay_post(client, body).await
}

pub async fn api_v3_hf_margin_order_post(body: &str) -> Result<Option<MakeOrderResData>> {
    let client = get_client()?;
    hf_margin_order_post(client, body).await
}

pub async fn api_v3_hf_margin_stop_order_post(body: &str) -> Result<Option<MakeStopOrderResData>> {
    let client = get_client()?;
    hf_margin_stop_order_post(client, body).await
}

pub async fn api_v3_hf_margin_stop_orders_get(
    query_params: &str,
) -> Result<Option<ApiV3HfMarginStopOrdersResData>> {
    let client = get_client()?;
    hf_margin_stop_orders_get(client, query_params).await
}

pub async fn api_v3_hf_margin_stop_order_cancel_by_id_delete(
    query_string: &str,
) -> Result<Option<ApiV3HfMarginStopOrderCancelByIdResData>> {
    let client = get_client()?;
    hf_margin_stop_order_cancel_by_id_delete(client, query_string).await
}

pub async fn api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(
    query_string: &str,
) -> Result<Option<ApiV3HfMarginStopOrderCancelByClientOidResData>> {
    let client = get_client()?;
    hf_margin_stop_order_cancel_by_client_oid_delete(client, query_string).await
}

pub async fn api_v3_accounts_universal_transfer_post(
    body: &str,
) -> Result<Option<ApiV3AccountsUniversalTransferResData>> {
    let client = get_client()?;
    accounts_universal_transfer_post(client, body).await
}

pub async fn api_v1_market_orderbook_level1_get(
    query_params: &str,
) -> Result<Option<ApiV1MarketOrderbookLevel1ResData>> {
    let client = get_client()?;
    market_orderbook_level1_get(client, query_params).await
}
