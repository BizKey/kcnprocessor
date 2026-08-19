use crate::api::client::KuCoinEndpoints;
use crate::api::models::{ApiV1MarketOrderbookLevel1Res, ApiV1MarketOrderbookLevel1ResData};
use anyhow::Result;

pub async fn market_orderbook_level1_get(
    client: &KuCoinEndpoints,
    query_params: &str,
) -> Result<Option<ApiV1MarketOrderbookLevel1ResData>> {
    let response_string: String = client.market_orderbook_level1_get(query_params).await?;
    let response = serde_json::from_str::<ApiV1MarketOrderbookLevel1Res>(&response_string)?;

    if response.code.as_str() == "200000" {
        Ok(response.data)
    } else {
        anyhow::bail!(
            "KuCoin API error: code={}, msg={:?}, data={:?}",
            response.code,
            response.msg,
            response.data
        )
    }
}
