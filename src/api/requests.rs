use crate::api::models::{
    ApiV1MarketOrderbookLevel1Res, ApiV1MarketOrderbookLevel1ResData,
    ApiV3AccountsUniversalTransferRes, ApiV3AccountsUniversalTransferResData, ApiV3BulletPrivate,
    ApiV3HfMarginStopOrderCancelByClientOidRes, ApiV3HfMarginStopOrderCancelByClientOidResData,
    ApiV3HfMarginStopOrderCancelByIdRes, ApiV3HfMarginStopOrderCancelByIdResData,
    ApiV3HfMarginStopOrdersRes, ApiV3HfMarginStopOrdersResData, ApiV3MarginRepayRes,
    ApiV3MarginRepayResData, MakeOrderRes, MakeOrderResData, MakeStopOrderRes,
    MakeStopOrderResData, MarginAccount, MarginAccountData,
};
use crate::api::tools::get_env;
use anyhow::{Context, Result};
use base64::Engine;
use hmac::{Hmac, KeyInit, Mac};
use micromap::Map;
use reqwest::{Client, Method, Response};
use sha2::Sha256;
use smallvec::SmallVec;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use urlencoding::encode;
type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct KuCoinClient {
    client: Client,
    api_key: String,
    api_secret: String,
    api_passphrase: String,
    base_url: String,
}

impl KuCoinClient {
    fn new() -> Result<Self> {
        let base_url: String = get_env("KUCOIN_BASE_URL").context("ENV KUCOIN_BASE_URL")?;
        let api_key: String = get_env("KUCOIN_KEY").context("ENV KUCOIN_KEY")?;
        let api_secret: String = get_env("KUCOIN_SECRET").context("ENV KUCOIN_SECRET")?;
        let api_passphrase: String = get_env("KUCOIN_PASS").context("ENV KUCOIN_PASS")?;

        let client: Client = Client::builder()
            .timeout(Duration::from_secs(15))
            .connect_timeout(Duration::from_secs(5))
            .tcp_keepalive(Duration::from_secs(60))
            .build()
            .context("Get error on Client::builder")?;

        Ok(Self {
            client,
            api_key,
            api_secret,
            api_passphrase,
            base_url,
        })
    }

    fn get_system_timestamp_ms(&self) -> Result<u64> {
        Ok(SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("Get error get UNIX_EPOCH")?
            .as_millis() as u64)
    }

    fn generate_signature(&self, to_sign: &[u8]) -> Result<String> {
        let mut mac =
            HmacSha256::new_from_slice(self.api_secret.as_bytes()).context("Fail HmacSha256")?;
        mac.update(to_sign);
        Ok(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
    }

    async fn api_v1_bullet_private_post(&self) -> Result<String> {
        // https://www.kucoin.com/docs-new/websocket-api/base-info/get-private-token-spot-margin
        let response = self
            .make_request(
                Method::POST,
                "/api/v1/bullet-private",
                String::new(),
                String::new(),
                true,
                self.get_system_timestamp_ms()?,
            )
            .await?;

        let status = response.status().as_u16();

        let body = response
            .text()
            .await
            .context("Fail read text from response:/api/v1/bullet-private")?;

        if status == 200 {
            Ok(body)
        } else {
            anyhow::bail!("API returned error status {status}: {body}")
        }
    }

    async fn api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(
        &self,
        query_string_str: String,
    ) -> Result<String> {
        // https://www.kucoin.com/docs-new/rest/margin-trading/orders/cancel-stop-order-by-clientoid
        let response = self
            .make_request(
                Method::DELETE,
                "/api/v3/hf/margin/stop-order/cancel-by-clientOid",
                query_string_str,
                String::new(),
                true,
                self.get_system_timestamp_ms()?,
            )
            .await?;

        let status = response.status().as_u16();

        let body = response.text().await.context(
            "Fail read text from response:/api/v3/hf/margin/stop-order/cancel-by-clientOid",
        )?;

        if status == 200 {
            Ok(body)
        } else {
            anyhow::bail!("API returned error status {status}: {body}")
        }
    }
    async fn api_v3_hf_margin_stop_order_cancel_by_id_delete(
        &self,
        query_string_str: String,
    ) -> Result<String> {
        // https://www.kucoin.com/docs-new/rest/margin-trading/orders/cancel-stop-order-by-clientoid
        let response = self
            .make_request(
                Method::DELETE,
                "/api/v3/hf/margin/stop-order/cancel-by-id",
                query_string_str,
                String::new(),
                true,
                self.get_system_timestamp_ms()?,
            )
            .await?;

        let status = response.status().as_u16();

        let body = response
            .text()
            .await
            .context("Fail read text from response:/api/v3/hf/margin/stop-order/cancel-by-id")?;

        if status == 200 {
            Ok(body)
        } else {
            anyhow::bail!("API returned error status {status}: {body}")
        }
    }

    async fn api_v3_margin_accounts_get(&self, query_params_str: String) -> Result<String> {
        // https://www.kucoin.com/docs-new/rest/account-info/account-funding/get-account-cross-margin
        let response = self
            .make_request(
                Method::GET,
                "/api/v3/margin/accounts",
                query_params_str,
                String::new(),
                true,
                self.get_system_timestamp_ms()?,
            )
            .await?;

        let status = response.status().as_u16();

        let body = response
            .text()
            .await
            .context("Fail read text from response:/api/v3/margin/accounts")?;

        if status == 200 {
            Ok(body)
        } else {
            anyhow::bail!("API returned error status {status}: {body}")
        }
    }

    async fn api_v3_hf_margin_stop_orders_get(&self, query_params_str: String) -> Result<String> {
        let response = self
            .make_request(
                Method::GET,
                "/api/v3/hf/margin/stop-orders",
                query_params_str,
                String::new(),
                true,
                self.get_system_timestamp_ms()?,
            )
            .await?;

        let status = response.status().as_u16();

        let body = response
            .text()
            .await
            .context("Fail read text from response:/api/v3/hf/margin/stop-orders")?;

        if status == 200 {
            Ok(body)
        } else {
            anyhow::bail!("API returned error status {status}: {body}")
        }
    }

    async fn api_v3_accounts_universal_transfer_post(&self, body_str: String) -> Result<String> {
        let response = self
            .make_request(
                Method::POST,
                "/api/v3/accounts/universal-transfer",
                String::new(),
                body_str,
                true,
                self.get_system_timestamp_ms()?,
            )
            .await?;

        let status = response.status().as_u16();

        let body = response
            .text()
            .await
            .context("Fail read text from response:/api/v3/accounts/universal-transfer")?;

        if status == 200 {
            Ok(body)
        } else {
            anyhow::bail!("API returned error status {status}: {body}")
        }
    }

    async fn api_v3_hf_margin_stop_order_post(&self, body_str: String) -> Result<String> {
        let response = self
            .make_request(
                Method::POST,
                "/api/v3/hf/margin/stop-order",
                String::new(),
                body_str,
                true,
                self.get_system_timestamp_ms()?,
            )
            .await?;

        let status = response.status().as_u16();

        let body = response
            .text()
            .await
            .context("Fail read text from response:/api/v3/hf/margin/stop-order")?;

        if status == 200 {
            Ok(body)
        } else {
            anyhow::bail!("API returned error status {status}: {body}")
        }
    }

    async fn api_v3_hf_margin_order_post(&self, body_str: String) -> Result<String> {
        let response = self
            .make_request(
                Method::POST,
                "/api/v3/hf/margin/order",
                String::new(),
                body_str,
                true,
                self.get_system_timestamp_ms()?,
            )
            .await?;

        let status = response.status().as_u16();

        let body = response
            .text()
            .await
            .context("Fail read text from response:/api/v3/hf/margin/order")?;

        if status == 200 {
            Ok(body)
        } else {
            anyhow::bail!("API returned error status {status}: {body}")
        }
    }

    async fn api_v3_margin_repay_post(&self, body_str: String) -> Result<String> {
        let response = self
            .make_request(
                Method::POST,
                "/api/v3/margin/repay",
                String::new(),
                body_str,
                true,
                self.get_system_timestamp_ms()?,
            )
            .await?;

        let status = response.status().as_u16();

        let body = response
            .text()
            .await
            .context("Fail read text from response:/api/v3/margin/repay")?;

        if status == 200 {
            Ok(body)
        } else {
            anyhow::bail!("API returned error status {status}: {body}")
        }
    }

    async fn api_v1_market_orderbook_level1_get(&self, query_params_str: String) -> Result<String> {
        let response = self
            .make_request(
                Method::GET,
                "/api/v1/market/orderbook/level1",
                query_params_str,
                String::new(),
                false,
                0,
            )
            .await?;

        let status = response.status().as_u16();

        let body = response
            .text()
            .await
            .context("Fail read text from response:/api/v1/market/orderbook/level1")?;

        if status == 200 {
            Ok(body)
        } else {
            anyhow::bail!("API returned error status {status}: {body}")
        }
    }

    async fn make_request(
        &self,
        method: Method,
        endpoint: &str,
        query_string: String,
        body_str: String,
        authenticated: bool,
        timestamp: u64,
    ) -> Result<Response> {
        let url = if !query_string.is_empty() {
            format!("{}{}?{}", self.base_url, endpoint, query_string)
        } else {
            format!("{}{}", self.base_url, endpoint)
        };

        let mut request_builder = self.client.request(method.clone(), &url);

        if authenticated {
            let mut str_to_sign = format!(
                "{}{}{}",
                timestamp,
                method.as_ref().to_uppercase(),
                endpoint
            );

            if !&query_string.is_empty() {
                str_to_sign.push('?');
                str_to_sign.push_str(&query_string);
            }
            if !&body_str.is_empty() {
                str_to_sign.push_str(&body_str);
            }

            let kc_api_sign = self
                .generate_signature(str_to_sign.as_bytes())
                .context("Fail generate signature")?;

            let kc_api_passphrase = self.generate_signature(self.api_passphrase.as_bytes())?;

            request_builder = request_builder
                .header("KC-API-KEY", &self.api_key)
                .header("KC-API-SIGN", kc_api_sign)
                .header("KC-API-TIMESTAMP", timestamp.to_string())
                .header("KC-API-PASSPHRASE", kc_api_passphrase)
                .header("KC-API-KEY-VERSION", "2");

            if !body_str.is_empty() {
                request_builder = request_builder
                    .header("Content-Type", "application/json")
                    .body(body_str);
            }
        }

        match request_builder.send().await {
            Ok(response) => Ok(response),
            Err(e) => {
                if e.is_timeout() {
                    anyhow::bail!("Timeout {}: {}", url, e)
                } else if e.is_connect() {
                    anyhow::bail!("Error connection {}: {}", url, e)
                } else if e.is_request() {
                    anyhow::bail!("Error prepare request {}: {}", url, e)
                } else if e.is_body() {
                    anyhow::bail!("Error in body {}: {}", url, e)
                } else {
                    anyhow::bail!("Unexpected error {}: {}", url, e)
                }
            }
        }
    }
}

static KUCLIENT: OnceLock<Result<KuCoinClient>> = OnceLock::new();

pub fn serialize_body(body: Option<serde_json::Value>) -> Result<String> {
    let Some(clear_value) = body else {
        return Ok(String::new());
    };
    Ok(serde_json::to_string(&clear_value)
        .with_context(|| format!("Failed to deserialize body '{}'", clear_value))?)
}
pub fn build_query_string(query_params: Map<&str, &str, 8>) -> String {
    if query_params.is_empty() {
        return String::new();
    }

    let mut params: SmallVec<[(&str, &str); 8]> = query_params.into_iter().collect();
    params.sort_by(|a, b| a.0.cmp(b.0));

    let capacity = params
        .iter()
        .map(|(k, v)| k.len() + v.len() + 1)
        .sum::<usize>()
        + params.len()
        - 1;

    let mut result = String::with_capacity(capacity);
    for (i, (k, v)) in params.iter().enumerate() {
        if i > 0 {
            result.push('&');
        }
        result.push_str(&encode(k));
        result.push('=');
        result.push_str(&encode(v));
    }
    result
}
fn get_client() -> Result<&'static KuCoinClient> {
    let client = KUCLIENT.get_or_init(|| KuCoinClient::new());
    match client {
        Ok(c) => Ok(c),
        Err(e) => Err(anyhow::anyhow!("Fail get or init KuCoinClient: {e}")),
    }
}
pub async fn api_v1_bullet_private_post() -> Result<String> {
    let client = get_client()?;

    let response_string = client.api_v1_bullet_private_post().await?;

    let response =
        serde_json::from_str::<ApiV3BulletPrivate>(&response_string).with_context(|| {
            format!(
                "Failed to deserialize response '{}' as {}",
                response_string,
                stringify!(ApiV3BulletPrivate),
            )
        })?;

    let ws = if response.code.as_str() == "200000" {
        response.data
    } else {
        anyhow::bail!(
            "KuCoin API error: code={}, msg={:?}, data={:?}",
            response.code,
            response.msg,
            response.data
        )
    };

    let Some(server) = ws else { anyhow::bail!("") };

    let Some(instance) = server.instance_servers.first() else {
        anyhow::bail!("No instance servers in bullet response{:?}", server)
    };

    Ok(format!("{}?token={}", instance.endpoint, server.token))
}
pub async fn api_v3_margin_accounts_get(query_params_str: String) -> Result<MarginAccountData> {
    let client = get_client()?;

    let response_string = client.api_v3_margin_accounts_get(query_params_str).await?;

    let response = serde_json::from_str::<MarginAccount>(&response_string).with_context(|| {
        format!(
            "Failed to deserialize response '{}' as {}",
            response_string,
            stringify!(MarginAccount),
        )
    })?;

    match response.code.as_str() {
        "200000" => Ok(response.data),
        _ => {
            anyhow::bail!(
                "KuCoin API error: code={}, msg={:?}, data={:?}",
                response.code,
                response.msg,
                response.data
            )
        }
    }
}
pub async fn api_v3_hf_margin_stop_order_cancel_by_id_delete(
    query_string_str: String,
) -> Result<Option<ApiV3HfMarginStopOrderCancelByIdResData>> {
    let client = get_client()?;

    let response_string = client
        .api_v3_hf_margin_stop_order_cancel_by_id_delete(query_string_str)
        .await?;

    let response = serde_json::from_str::<ApiV3HfMarginStopOrderCancelByIdRes>(&response_string)
        .with_context(|| {
            format!(
                "Failed to deserialize response '{}' as {}",
                response_string,
                stringify!(ApiV3HfMarginStopOrderCancelByIdRes),
            )
        })?;

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
pub async fn api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(
    query_string_str: String,
) -> Result<Option<ApiV3HfMarginStopOrderCancelByClientOidResData>> {
    let client = get_client()?;

    let response_string = client
        .api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(query_string_str)
        .await?;

    let response =
        serde_json::from_str::<ApiV3HfMarginStopOrderCancelByClientOidRes>(&response_string)
            .with_context(|| {
                format!(
                    "Failed to deserialize response '{}' as {}",
                    response_string,
                    stringify!(ApiV3HfMarginStopOrderCancelByClientOidRes),
                )
            })?;
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
pub async fn api_v3_accounts_universal_transfer_post(
    body_str: String,
) -> Result<Option<ApiV3AccountsUniversalTransferResData>> {
    let client = get_client()?;

    let response_string = client
        .api_v3_accounts_universal_transfer_post(body_str)
        .await?;

    let response = serde_json::from_str::<ApiV3AccountsUniversalTransferRes>(&response_string)
        .with_context(|| {
            format!(
                "Failed to deserialize response '{}' as {}",
                response_string,
                stringify!(ApiV3AccountsUniversalTransferRes),
            )
        })?;

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
pub async fn api_v1_market_orderbook_level1_get(
    query_params_str: String,
) -> Result<Option<ApiV1MarketOrderbookLevel1ResData>> {
    let client = get_client()?;

    let response_string = client
        .api_v1_market_orderbook_level1_get(query_params_str)
        .await?;

    let response = serde_json::from_str::<ApiV1MarketOrderbookLevel1Res>(&response_string)
        .with_context(|| {
            format!(
                "Failed to deserialize response '{}' as {}",
                response_string,
                stringify!(ApiV1MarketOrderbookLevel1Res),
            )
        })?;

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

pub async fn api_v3_hf_margin_stop_orders_get(
    query_params_str: String,
) -> Result<Option<ApiV3HfMarginStopOrdersResData>> {
    let client = get_client()?;

    let response_string = client
        .api_v3_hf_margin_stop_orders_get(query_params_str)
        .await?;

    let response = serde_json::from_str::<ApiV3HfMarginStopOrdersRes>(&response_string)
        .with_context(|| {
            format!(
                "Failed to deserialize response '{}' as {}",
                response_string,
                stringify!(ApiV3HfMarginStopOrdersRes),
            )
        })?;

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
pub async fn api_v3_hf_margin_stop_order_post(
    body_str: String,
) -> Result<Option<MakeStopOrderResData>> {
    let client = get_client()?;

    let response_string = client.api_v3_hf_margin_stop_order_post(body_str).await?;

    let response =
        serde_json::from_str::<MakeStopOrderRes>(&response_string).with_context(|| {
            format!(
                "Failed to deserialize response '{}' as {}",
                response_string,
                stringify!(MakeStopOrderRes),
            )
        })?;

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
pub async fn api_v3_hf_margin_order_post(body_str: String) -> Result<Option<MakeOrderResData>> {
    let client = get_client()?;

    let response_string = client.api_v3_hf_margin_order_post(body_str).await?;

    let response = serde_json::from_str::<MakeOrderRes>(&response_string).with_context(|| {
        format!(
            "Failed to deserialize response '{}' as {}",
            response_string,
            stringify!(MakeOrderRes),
        )
    })?;

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
pub async fn api_v3_margin_repay_post(body_str: String) -> Result<Option<ApiV3MarginRepayResData>> {
    let client = get_client()?;

    let response_string = client.api_v3_margin_repay_post(body_str).await?;

    let response =
        serde_json::from_str::<ApiV3MarginRepayRes>(&response_string).with_context(|| {
            format!(
                "Failed to deserialize response '{}' as {}",
                response_string,
                stringify!(ApiV3MarginRepayRes),
            )
        })?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::env;

    #[test]
    fn test_signature_consistency() {
        let client = KuCoinClient {
            client: Client::new(),
            api_key: "test_key".to_string(),
            api_secret: "test_secret".to_string(),
            api_passphrase: "test_pass".to_string(),
            base_url: "https://api.kucoin.com".to_string(),
        };

        let body: serde_json::Value = json!({
            "clientOid": "test-123",
            "side": "buy",
            "symbol": "BTC-USDT",
            "type": "market"
        });

        let body_str = serde_json::to_string(&body).unwrap();
        let timestamp = 1234567890u64;
        let method = "POST";
        let endpoint = "/api/v3/hf/margin/order";
        let query_string = "";

        let mut to_sign = format!("{}{}{}", timestamp, method, endpoint);
        if !query_string.is_empty() {
            to_sign.push('?');
            to_sign.push_str(query_string);
        }
        if !body_str.is_empty() {
            to_sign.push_str(&body_str);
        }

        let signature = client.generate_signature(to_sign.as_bytes()).unwrap();

        assert!(!signature.is_empty(), "Signature should not be empty");
        println!("Generated signature: {}", signature);

        let decoded = base64::engine::general_purpose::STANDARD.decode(&signature);
        assert!(decoded.is_ok(), "Signature should be valid base64");
    }

    #[test]
    fn test_signature_changes_with_body() {
        let client = KuCoinClient {
            client: Client::new(),
            api_key: "test_key".to_string(),
            api_secret: "test_secret".to_string(),
            api_passphrase: "test_pass".to_string(),
            base_url: "https://api.kucoin.com".to_string(),
        };

        let body1 = json!({"amount": "100"});
        let body2 = json!({"amount": "200"});

        let body_str1 = serde_json::to_string(&body1).unwrap();
        let body_str2 = serde_json::to_string(&body2).unwrap();

        let timestamp = 1234567890u64;
        let method = "POST";
        let endpoint = "/api/test";
        let query_string = "";

        let mut to_sign1: String = format!("{}{}{}", timestamp, method, endpoint);
        if !query_string.is_empty() {
            to_sign1.push('?');
            to_sign1.push_str(query_string);
        }
        if !body_str1.is_empty() {
            to_sign1.push_str(&body_str1);
        }

        let mut to_sign2: String = format!("{}{}{}", timestamp, method, endpoint);
        if !query_string.is_empty() {
            to_sign2.push('?');
            to_sign2.push_str(query_string);
        }
        if !body_str2.is_empty() {
            to_sign2.push_str(&body_str2);
        }

        let signature1 = client.generate_signature(to_sign1.as_bytes()).unwrap();
        let signature2 = client.generate_signature(to_sign2.as_bytes()).unwrap();

        assert_ne!(signature1, signature2);
    }

    #[test]
    fn test_empty_body_signature() {
        let client = KuCoinClient {
            client: Client::new(),
            api_key: "test_key".to_string(),
            api_secret: "test_secret".to_string(),
            api_passphrase: "test_pass".to_string(),
            base_url: "https://api.kucoin.com".to_string(),
        };

        let timestamp = 1234567890u64;
        let method = "GET";
        let endpoint = "/api/test";
        let query_string = "";
        let body_str = "";

        let to_sign: String = format!("{}{}{}", timestamp, method, endpoint);
        let signature: String = client.generate_signature(to_sign.as_bytes()).unwrap();

        // Так как оба вызова идентичны, подписи должны совпадать
        let to_sign2: String = format!("{}{}{}", timestamp, method, endpoint);
        let signature2: String = client.generate_signature(to_sign2.as_bytes()).unwrap();

        assert_eq!(signature, signature2);
    }

    #[test]
    fn test_signature_with_query_params() {
        let client = KuCoinClient {
            client: Client::new(),
            api_key: "test_key".to_string(),
            api_secret: "test_secret".to_string(),
            api_passphrase: "test_pass".to_string(),
            base_url: "https://api.kucoin.com".to_string(),
        };

        let timestamp = 1234567890u64;
        let method = "GET";
        let endpoint = "/api/v1/market/orderbook/level1";
        let query_string = "symbol=BTC-USDT&limit=10";
        let body_str = "";

        let mut to_sign: String = format!("{}{}{}", timestamp, method, endpoint);
        if !query_string.is_empty() {
            to_sign.push('?');
            to_sign.push_str(query_string);
        }
        if !body_str.is_empty() {
            to_sign.push_str(body_str);
        }

        let signature = client.generate_signature(to_sign.as_bytes()).unwrap();

        assert!(!signature.is_empty());
        println!("Signature with query params: {}", signature);
    }

    #[test]
    fn test_signature_with_query_params_and_body() {
        let client = KuCoinClient {
            client: Client::new(),
            api_key: "test_key".to_string(),
            api_secret: "test_secret".to_string(),
            api_passphrase: "test_pass".to_string(),
            base_url: "https://api.kucoin.com".to_string(),
        };

        let timestamp = 1234567890u64;
        let method = "POST";
        let endpoint = "/api/v3/hf/margin/order";
        let query_string = "symbol=BTC-USDT";
        let body = json!({
            "clientOid": "test-123",
            "side": "buy",
            "type": "market"
        });
        let body_str = serde_json::to_string(&body).unwrap();

        let mut to_sign: String = format!("{}{}{}", timestamp, method, endpoint);
        if !query_string.is_empty() {
            to_sign.push('?');
            to_sign.push_str(query_string);
        }
        if !body_str.is_empty() {
            to_sign.push_str(&body_str);
        }

        let signature = client.generate_signature(to_sign.as_bytes()).unwrap();

        assert!(!signature.is_empty());
        println!("Signature with query params and body: {}", signature);
    }

    #[test]
    fn test_get_client_initialization() {
        // Этот тест требует установленных переменных окружения
        // Поэтому пропускаем, если их нет
        if env::var("KUCOIN_BASE_URL").is_err()
            || env::var("KUCOIN_KEY").is_err()
            || env::var("KUCOIN_SECRET").is_err()
            || env::var("KUCOIN_PASS").is_err()
        {
            println!("Skipping test_get_client_initialization - environment variables not set");
            return;
        }

        let result = get_client();
        assert!(
            result.is_ok(),
            "Client should initialize successfully: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_build_query_string() {
        let mut params = Map::new();
        params.insert("symbol", "BTC-USDT");
        params.insert("limit", "10");

        let query_string = build_query_string(params);

        // Должно быть отсортировано по ключам: limit=10&symbol=BTC-USDT
        assert!(query_string.contains("limit=10") || query_string.contains("symbol=BTC-USDT"));
        assert_eq!(query_string.split('&').count(), 2);
        println!("Built query string: {}", query_string);
    }

    #[test]
    fn test_build_empty_query_string() {
        let params = Map::new();
        let query_string = build_query_string(params);
        assert_eq!(query_string, "");
    }

    #[test]
    fn test_serialize_body() {
        let body = json!({
            "clientOid": "test-123",
            "side": "buy"
        });

        let result = serialize_body(Some(body)).unwrap();
        assert!(!result.is_empty());
        assert!(result.contains("clientOid"));
        assert!(result.contains("test-123"));
    }

    #[test]
    fn test_serialize_none_body() {
        let result = serialize_body(None).unwrap();
        assert_eq!(result, "");
    }
}
