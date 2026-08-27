use crate::api::client::endpoints::KuCoinEndpoints;
use crate::api::models::{
    ApiV3MarginRepayRes, ApiV3MarginRepayResData, MarginAccount, MarginAccountData,
};
use anyhow::Result;

pub async fn margin_accounts_get(
    client: &KuCoinEndpoints,
    query_params: &str,
) -> Result<MarginAccountData> {
    let response_string: String = client.margin_accounts_get(query_params).await?;
    let response = serde_json::from_str::<MarginAccount>(&response_string)?;

    if response.code.as_str() == "200000" {
        Ok(response.data)
    } else {
        anyhow::bail!(
            "KuCoin API error /api/v3/hf/margin/order: code={}, msg={:?}, data={:?}",
            response.code,
            response.msg,
            response.data
        )
    }
}

pub async fn margin_repay_post(
    client: &KuCoinEndpoints,
    body: &str,
) -> Result<Option<ApiV3MarginRepayResData>> {
    let response_string: String = client.margin_repay_post(body).await?;
    let response = serde_json::from_str::<ApiV3MarginRepayRes>(&response_string)?;

    if response.code.as_str() == "200000" {
        Ok(response.data)
    } else {
        anyhow::bail!(
            "KuCoin API error /api/v3/margin/repay: code={}, msg={:?}, data={:?}",
            response.code,
            response.msg,
            response.data,
        )
    }
}
