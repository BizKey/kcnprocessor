use crate::api::client::endpoints::KuCoinEndpoints;
use crate::api::models::{
    ApiV3AccountsUniversalTransferRes, ApiV3AccountsUniversalTransferResData,
    ApiV3HfMarginStopOrderCancelByClientOidRes, ApiV3HfMarginStopOrderCancelByClientOidResData,
    ApiV3HfMarginStopOrderCancelByIdRes, ApiV3HfMarginStopOrderCancelByIdResData,
    ApiV3HfMarginStopOrdersRes, ApiV3HfMarginStopOrdersResData, MakeOrderRes, MakeOrderResData,
    MakeStopOrderRes, MakeStopOrderResData,
};
use anyhow::Result;

pub async fn hf_margin_order_post(
    client: &KuCoinEndpoints,
    body: &str,
) -> Result<Option<MakeOrderResData>> {
    let response_string: String = client.hf_margin_order_post(body).await?;
    let response = serde_json::from_str::<MakeOrderRes>(&response_string)?;

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

pub async fn hf_margin_stop_order_post(
    client: &KuCoinEndpoints,
    body: &str,
) -> Result<Option<MakeStopOrderResData>> {
    let response_string: String = client.hf_margin_stop_order_post(body).await?;
    let response = serde_json::from_str::<MakeStopOrderRes>(&response_string)?;

    if response.code.as_str() == "200000" {
        Ok(response.data)
    } else {
        anyhow::bail!(
            "KuCoin API error /api/v3/hf/margin/stop-order: code={}, msg={:?}, data={:?}",
            response.code,
            response.msg,
            response.data
        )
    }
}

pub async fn hf_margin_stop_orders_get(
    client: &KuCoinEndpoints,
    query_params: &str,
) -> Result<Option<ApiV3HfMarginStopOrdersResData>> {
    let response_string: String = client.hf_margin_stop_orders_get(query_params).await?;
    let response = serde_json::from_str::<ApiV3HfMarginStopOrdersRes>(&response_string)?;

    if response.code.as_str() == "200000" {
        Ok(response.data)
    } else {
        anyhow::bail!(
            "KuCoin API error /api/v3/hf/margin/stop-orders: code={}, msg={:?}, data={:?}",
            response.code,
            response.msg,
            response.data
        )
    }
}

pub async fn hf_margin_stop_order_cancel_by_id_delete(
    client: &KuCoinEndpoints,
    query_string: &str,
) -> Result<Option<ApiV3HfMarginStopOrderCancelByIdResData>> {
    let response_string: String = client
        .hf_margin_stop_order_cancel_by_id_delete(query_string)
        .await?;
    let response = serde_json::from_str::<ApiV3HfMarginStopOrderCancelByIdRes>(&response_string)?;

    if response.code.as_str() == "200000" {
        Ok(response.data)
    } else {
        anyhow::bail!(
            "KuCoin API error /api/v3/hf/margin/stop-order/cancel-by-id: code={}, msg={:?}, data={:?}",
            response.code,
            response.msg,
            response.data
        )
    }
}

pub async fn hf_margin_stop_order_cancel_by_client_oid_delete(
    client: &KuCoinEndpoints,
    query_string: &str,
) -> Result<Option<ApiV3HfMarginStopOrderCancelByClientOidResData>> {
    let response_string: String = client
        .hf_margin_stop_order_cancel_by_client_oid_delete(query_string)
        .await?;
    let response =
        serde_json::from_str::<ApiV3HfMarginStopOrderCancelByClientOidRes>(&response_string)?;

    if response.code.as_str() == "200000" {
        Ok(response.data)
    } else {
        anyhow::bail!(
            "KuCoin API error /api/v3/hf/margin/stop-order/cancel-by-clientOid: code={}, msg={:?}, data={:?}",
            response.code,
            response.msg,
            response.data
        )
    }
}

pub async fn accounts_universal_transfer_post(
    client: &KuCoinEndpoints,
    body: &str,
) -> Result<Option<ApiV3AccountsUniversalTransferResData>> {
    let response_string: String = client.accounts_universal_transfer_post(body).await?;
    let response = serde_json::from_str::<ApiV3AccountsUniversalTransferRes>(&response_string)?;

    if response.code.as_str() == "200000" {
        Ok(response.data)
    } else {
        anyhow::bail!(
            "KuCoin API error /api/v3/accounts/universal-transfer: code={}, msg={:?}, data={:?}",
            response.code,
            response.msg,
            response.data
        )
    }
}
