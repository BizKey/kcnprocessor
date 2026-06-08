use crate::api::models::{
    ApiV1MarketOrderbookLevel1Res, ApiV3AccountsUniversalTransferRes, ApiV3BulletPrivate, ApiV3HfMarginStopOrderCancelByClientOidRes, ApiV3HfMarginStopOrderCancelRes, ApiV3MarginRepayRes,
    MakeOrderRes, MakeStopOrderRes, MarginAccount,
};
use crate::api::tools::get_env;
use base64::Engine;
use hmac::{Hmac, Mac};
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
    fn new() -> Result<Self, String> {
        let base_url: String = match get_env("KUCOIN_BASE_URL") {
            Ok(base_url) => base_url,
            Err(e) => return Err(e),
        };

        let api_key: String = match get_env("KUCOIN_KEY") {
            Ok(api_key) => api_key,
            Err(e) => return Err(e),
        };

        let api_secret: String = match get_env("KUCOIN_SECRET") {
            Ok(api_secret) => api_secret,
            Err(e) => return Err(e),
        };

        let api_passphrase: String = match get_env("KUCOIN_PASS") {
            Ok(api_passphrase) => api_passphrase,
            Err(e) => return Err(e),
        };

        match Client::builder().timeout(Duration::from_secs(15)).connect_timeout(Duration::from_secs(5)).tcp_keepalive(Duration::from_secs(60)).build() {
            Ok(client) => Ok(Self { client, api_key: api_key, api_secret: api_secret, api_passphrase: api_passphrase, base_url }),
            Err(e) => {
                let msg: String = format!("Get error on Client::builder:{}", e);
                log::error!("{}", msg);
                return Err(msg);
            }
        }
    }

    fn get_system_timestamp_ms(&self) -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
    }

    fn generate_signature(&self, timestamp: u64, method: &str, endpoint: &str, query_string: &str, body: &str) -> Result<String, String> {
        let mut str_to_sign = format!("{}{}{}", timestamp, method.to_uppercase(), endpoint);

        if !query_string.is_empty() {
            str_to_sign.push('?');
            str_to_sign.push_str(query_string);
        }
        if !body.is_empty() {
            str_to_sign.push_str(body);
        }

        let mut mac = match HmacSha256::new_from_slice(self.api_secret.as_bytes()) {
            Ok(mac) => mac,
            Err(e) => return Err(format!("Fail get api secret:{}", e)),
        };

        mac.update(str_to_sign.as_bytes());
        Ok(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
    }

    fn generate_passphrase_signature(&self) -> Result<String, String> {
        let mut mac = match HmacSha256::new_from_slice(self.api_secret.as_bytes()) {
            Ok(mac) => mac,
            Err(e) => return Err(format!("Fail get api secret:{}", e)),
        };
        mac.update(self.api_passphrase.as_bytes());
        Ok(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
    }

    async fn api_v1_bullet_private(&self) -> Result<String, String> {
        let response: Response = match self.make_request(Method::POST, "/api/v1/bullet-private", String::new(), String::new(), true, self.get_system_timestamp_ms()).await {
            Ok(response) => response,
            Err(e) => return Err(e),
        };

        let status: reqwest::StatusCode = response.status();

        let response_string: String = match response.text().await {
            Ok(response_string) => response_string,
            Err(e) => {
                let msg: String = format!("Fail read text from response:{}", e);
                log::error!("{}", msg);
                return Err(msg);
            }
        };

        match status.as_u16() {
            200 => Ok(response_string),
            status_code => {
                let msg: String = format!("API returned error status {}: {}", status_code, response_string);
                log::error!("{}", msg);
                return Err(msg);
            }
        }
    }

    async fn api_v3_hf_margin_stop_order_cancel_by_client_oid(&self, query_string_str: String) -> Result<String, String> {
        let response: Response =
            match self.make_request(Method::DELETE, "/api/v3/hf/margin/stop-order/cancel-by-clientOid", query_string_str, String::new(), true, self.get_system_timestamp_ms()).await {
                Ok(response) => response,
                Err(e) => return Err(e),
            };

        let status: reqwest::StatusCode = response.status();

        let response_string: String = match response.text().await {
            Ok(response_string) => response_string,
            Err(e) => {
                let msg: String = format!("Fail read text from response:{}", e);
                log::error!("{}", msg);
                return Err(msg);
            }
        };

        match status.as_u16() {
            200 => Ok(response_string),
            status_code => {
                let msg: String = format!("API returned error status {}: {}", status_code, response_string);
                log::error!("{}", msg);
                return Err(msg);
            }
        }
    }

    async fn get_margin_accounts(&self, query_params_str: String) -> Result<String, String> {
        let response: Response = match self.make_request(Method::GET, "/api/v3/margin/accounts", query_params_str, String::new(), true, self.get_system_timestamp_ms()).await {
            Ok(response) => response,
            Err(e) => return Err(e),
        };

        let status: reqwest::StatusCode = response.status();

        let response_string: String = match response.text().await {
            Ok(response_string) => response_string,
            Err(e) => {
                let msg: String = format!("Fail read text from response:{}", e);
                log::error!("{}", msg);
                return Err(msg);
            }
        };

        match status.as_u16() {
            200 => Ok(response_string),
            status_code => {
                let msg: String = format!("API returned error status {}: {}", status_code, response_string);
                log::error!("{}", msg);
                return Err(msg);
            }
        }
    }

    async fn batch_cancel_stop_orders(&self, query_params_str: String) -> Result<String, String> {
        let response: Response = match self.make_request(Method::DELETE, "/api/v3/hf/margin/stop-order/cancel", query_params_str, String::new(), true, self.get_system_timestamp_ms()).await {
            Ok(response) => response,
            Err(e) => return Err(e),
        };

        let status: reqwest::StatusCode = response.status();

        let response_string: String = match response.text().await {
            Ok(response_string) => response_string,
            Err(e) => {
                let msg: String = format!("Fail read text from response:{}", e);
                log::error!("{}", msg);
                return Err(msg);
            }
        };

        match status.as_u16() {
            200 => Ok(response_string),
            status_code => {
                let msg: String = format!("API returned error status {}: {}", status_code, response_string);
                log::error!("{}", msg);
                return Err(msg);
            }
        }
    }

    async fn account_transfer(&self, body_str: String) -> Result<String, String> {
        let response: Response = match self.make_request(Method::POST, "/api/v3/accounts/universal-transfer", String::new(), body_str, true, self.get_system_timestamp_ms()).await {
            Ok(response) => response,
            Err(e) => return Err(e),
        };

        let status: reqwest::StatusCode = response.status();

        let response_string: String = match response.text().await {
            Ok(response_string) => response_string,
            Err(e) => {
                let msg: String = format!("Fail read text from response:{}", e);
                log::error!("{}", msg);
                return Err(msg);
            }
        };

        match status.as_u16() {
            200 => Ok(response_string),
            status_code => {
                let msg: String = format!("API returned error status {}: {}", status_code, response_string);
                log::error!("{}", msg);
                return Err(msg);
            }
        }
    }

    async fn api_v3_hf_margin_stop_order(&self, body_str: String) -> Result<String, String> {
        let response: Response = match self.make_request(Method::POST, "/api/v3/hf/margin/stop-order", String::new(), body_str, true, self.get_system_timestamp_ms()).await {
            Ok(response) => response,
            Err(e) => return Err(e),
        };

        let status: reqwest::StatusCode = response.status();

        let response_string: String = match response.text().await {
            Ok(response_string) => response_string,
            Err(e) => {
                let msg: String = format!("Fail read text from response:{}", e);
                log::error!("{}", msg);
                return Err(msg);
            }
        };

        match status.as_u16() {
            200 => Ok(response_string),
            status_code => {
                let msg: String = format!("API returned error status {}: {}", status_code, response_string);
                log::error!("{}", msg);
                return Err(msg);
            }
        }
    }

    async fn add_api_v3_hf_margin_order(&self, body_str: String) -> Result<String, String> {
        let response: Response = match self.make_request(Method::POST, "/api/v3/hf/margin/order", String::new(), body_str, true, self.get_system_timestamp_ms()).await {
            Ok(response) => response,
            Err(e) => return Err(e),
        };

        let status: reqwest::StatusCode = response.status();

        let response_string: String = match response.text().await {
            Ok(response_string) => response_string,
            Err(e) => {
                let msg: String = format!("Fail read text from response:{}", e);
                log::error!("{}", msg);
                return Err(msg);
            }
        };

        match status.as_u16() {
            200 => Ok(response_string),
            status_code => {
                let msg: String = format!("API returned error status {}: {}", status_code, response_string);
                log::error!("{}", msg);
                return Err(msg);
            }
        }
    }

    async fn margin_repay(&self, body_str: String) -> Result<String, String> {
        let response: Response = match self.make_request(Method::POST, "/api/v3/margin/repay", String::new(), body_str, true, self.get_system_timestamp_ms()).await {
            Ok(response) => response,
            Err(e) => return Err(e),
        };

        let status: reqwest::StatusCode = response.status();

        let response_string: String = match response.text().await {
            Ok(response_string) => response_string,
            Err(e) => {
                let msg: String = format!("Fail read text from response:{}", e);
                log::error!("{}", msg);
                return Err(msg);
            }
        };

        match status.as_u16() {
            200 => Ok(response_string),
            status_code => {
                let msg: String = format!("API returned error status {}: {}", status_code, response_string);
                log::error!("{}", msg);
                return Err(msg);
            }
        }
    }

    async fn get_ticker_price(&self, query_params_str: String) -> Result<String, String> {
        let response: Response = match self.make_request(Method::GET, "/api/v1/market/orderbook/level1", query_params_str, String::new(), false, 0).await {
            Ok(response) => response,
            Err(e) => return Err(e),
        };

        let status: reqwest::StatusCode = response.status();

        let response_string: String = match response.text().await {
            Ok(response_string) => response_string,
            Err(e) => {
                let msg: String = format!("Fail read text from response:{}", e);
                log::error!("{}", msg);
                return Err(msg);
            }
        };

        match status.as_u16() {
            200 => Ok(response_string),
            status_code => {
                let msg: String = format!("API returned error status {}: {}", status_code, response_string);
                log::error!("{}", msg);
                return Err(msg);
            }
        }
    }

    async fn make_request(&self, method: Method, endpoint: &str, query_string: String, body_str: String, authenticated: bool, timestamp: u64) -> Result<Response, String> {
        let url: String = if !query_string.is_empty() { format!("{}{}?{}", self.base_url, endpoint, query_string) } else { format!("{}{}", self.base_url, endpoint) };

        let mut request_builder: reqwest::RequestBuilder = self.client.request(method.clone(), &url);

        if authenticated {
            let kc_api_sign: String = match self.generate_signature(timestamp, method.as_ref(), endpoint, &query_string, &body_str) {
                Ok(kc_api_sign) => kc_api_sign,
                Err(e) => return Err(e),
            };

            let kc_api_passphrase: String = match self.generate_passphrase_signature() {
                Ok(kc_api_passphrase) => kc_api_passphrase,
                Err(e) => return Err(e),
            };

            request_builder = request_builder
                .header("KC-API-KEY", &self.api_key)
                .header("KC-API-SIGN", kc_api_sign)
                .header("KC-API-TIMESTAMP", timestamp.to_string())
                .header("KC-API-PASSPHRASE", kc_api_passphrase)
                .header("KC-API-KEY-VERSION", "2");

            if !body_str.is_empty() {
                request_builder = request_builder.header("Content-Type", "application/json").body(body_str);
            }
        }
        match request_builder.send().await {
            Ok(response) => Ok(response),
            Err(e) => {
                if e.is_timeout() {
                    let msg: String = format!("Timeout {}: {}", url, e);
                    log::error!("{}", msg);
                    return Err(msg);
                } else if e.is_connect() {
                    let msg: String = format!("Error connection {}: {}", url, e);
                    log::error!("{}", msg);
                    return Err(msg);
                } else if e.is_request() {
                    let msg: String = format!("Error prepare request {}: {}", url, e);
                    log::error!("{}", msg);
                    return Err(msg);
                } else if e.is_body() {
                    let msg: String = format!("Error in body {}: {}", url, e);
                    log::error!("{}", msg);
                    return Err(msg);
                } else {
                    let msg: String = format!("Unexpected error {}: {}", url, e);
                    log::error!("{}", msg);
                    return Err(msg);
                }
            }
        }
    }
}

static KUCLIENT: OnceLock<Result<KuCoinClient, String>> = OnceLock::new();

pub fn serialize_body(body: Option<serde_json::Value>) -> Result<String, serde_json::Error> {
    let clear_value: serde_json::Value = match body {
        Some(clear_value) => clear_value,
        None => return Ok(String::new()),
    };
    match serde_json::to_string(&clear_value) {
        Ok(json_string) => Ok(json_string),
        Err(e) => Err(e),
    }
}
pub fn build_query_string(query_params: Map<&str, &str, 8>) -> String {
    if query_params.is_empty() {
        return String::new();
    }

    let mut params: SmallVec<[(&str, &str); 8]> = query_params.into_iter().collect();
    params.sort_by(|a, b| a.0.cmp(b.0));

    let capacity = params.iter().map(|(k, v)| k.len() + v.len() + 1).sum::<usize>() + params.len() - 1;

    let mut result: String = String::with_capacity(capacity);
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
fn get_client() -> Result<&'static KuCoinClient, String> {
    match KUCLIENT
        .get_or_init({
            || match KuCoinClient::new() {
                Ok(client) => Ok(client),
                Err(e) => return Err(e),
            }
        })
        .as_ref()
    {
        Ok(client) => Ok(client),
        Err(e) => {
            let msg: String = format!("Fail get or init KuCoinClient:{}", e);
            log::error!("{}", msg);
            return Err(msg);
        }
    }
}
pub async fn get_private_ws_url() -> Result<String, String> {
    let client: &KuCoinClient = match get_client() {
        Ok(client) => client,
        Err(e) => return Err(e),
    };

    let response_string: String = match client.api_v1_bullet_private().await {
        Ok(response_string) => response_string,
        Err(e) => return Err(e),
    };

    let ws: ApiV3BulletPrivate = match serde_json::from_str::<ApiV3BulletPrivate>(&response_string) {
        Ok(res) => res,
        Err(e) => {
            let error_msg: String = format!("Failed to deserialize response '{}' as {}: {}", response_string, stringify!(ApiV3BulletPrivate), e);
            log::error!("{}", error_msg);
            return Err(error_msg);
        }
    };
    ws.data.instance_servers.first().map(|s| format!("{}?token={}", s.endpoint, ws.data.token)).ok_or_else(|| "No instance servers in bullet response".into())
}
pub async fn get_all_margin_accounts(query_params_str: String) -> Result<MarginAccount, String> {
    let client: &KuCoinClient = match get_client() {
        Ok(client) => client,
        Err(e) => return Err(e),
    };

    let response_string: String = match client.get_margin_accounts(query_params_str).await {
        Ok(response_string) => response_string,
        Err(e) => return Err(e.into()),
    };

    match serde_json::from_str::<MarginAccount>(&response_string) {
        Ok(res) => Ok(res),
        Err(e) => {
            let error_msg: String = format!("Failed to deserialize response '{}' as {}: {}", response_string, stringify!(MarginAccount), e);
            log::error!("{}", error_msg);
            return Err(error_msg);
        }
    }
}
pub async fn api_v3_hf_margin_stop_order_cancel_by_client_oid(query_string_str: String) -> Result<ApiV3HfMarginStopOrderCancelByClientOidRes, String> {
    let client: &KuCoinClient = match get_client() {
        Ok(client) => client,
        Err(e) => return Err(e),
    };

    let response_string: String = match client.api_v3_hf_margin_stop_order_cancel_by_client_oid(query_string_str).await {
        Ok(response_string) => response_string,
        Err(e) => return Err(e.into()),
    };

    match serde_json::from_str::<ApiV3HfMarginStopOrderCancelByClientOidRes>(&response_string) {
        Ok(res) => Ok(res),
        Err(e) => {
            let error_msg: String = format!("Failed to deserialize response '{}' as {}: {}", response_string, stringify!(ApiV3HfMarginStopOrderCancelByClientOidRes), e);
            log::error!("{}", error_msg);
            return Err(error_msg);
        }
    }
}
pub async fn sent_account_transfer(body_str: String) -> Result<ApiV3AccountsUniversalTransferRes, String> {
    let client: &KuCoinClient = match get_client() {
        Ok(client) => client,
        Err(e) => return Err(e),
    };

    let response_string: String = match client.account_transfer(body_str).await {
        Ok(response_string) => response_string,
        Err(e) => return Err(e.into()),
    };

    match serde_json::from_str::<ApiV3AccountsUniversalTransferRes>(&response_string) {
        Ok(res) => Ok(res),
        Err(e) => {
            let error_msg: String = format!("Failed to deserialize response '{}' as {}: {}", response_string, stringify!(ApiV3AccountsUniversalTransferRes), e);
            log::error!("{}", error_msg);
            return Err(error_msg);
        }
    }
}
pub async fn get_ticker_price(query_params_str: String) -> Result<ApiV1MarketOrderbookLevel1Res, String> {
    let client: &KuCoinClient = match get_client() {
        Ok(client) => client,
        Err(e) => return Err(e),
    };

    let response_string: String = match client.get_ticker_price(query_params_str).await {
        Ok(response_string) => response_string,
        Err(e) => return Err(e.into()),
    };

    match serde_json::from_str::<ApiV1MarketOrderbookLevel1Res>(&response_string) {
        Ok(res) => Ok(res),
        Err(e) => {
            let error_msg: String = format!("Failed to deserialize response '{}' as {}: {}", response_string, stringify!(ApiV1MarketOrderbookLevel1Res), e);
            log::error!("{}", error_msg);
            return Err(error_msg);
        }
    }
}
pub async fn batch_cancel_stop_orders(query_params_str: String) -> Result<ApiV3HfMarginStopOrderCancelRes, String> {
    let client: &KuCoinClient = match get_client() {
        Ok(client) => client,
        Err(e) => return Err(e),
    };

    let response_string: String = match client.batch_cancel_stop_orders(query_params_str).await {
        Ok(response_string) => response_string,
        Err(e) => return Err(e.into()),
    };

    match serde_json::from_str::<ApiV3HfMarginStopOrderCancelRes>(&response_string) {
        Ok(res) => Ok(res),
        Err(e) => {
            let error_msg: String = format!("Failed to deserialize response '{}' as {}: {}", response_string, stringify!(ApiV3HfMarginStopOrderCancelRes), e);
            log::error!("{}", error_msg);
            return Err(error_msg);
        }
    }
}
pub async fn api_v3_hf_margin_stop_order(body_str: String) -> Result<MakeStopOrderRes, String> {
    let client: &KuCoinClient = match get_client() {
        Ok(client) => client,
        Err(e) => return Err(e),
    };

    let response_string: String = match client.api_v3_hf_margin_stop_order(body_str).await {
        Ok(response_string) => response_string,
        Err(e) => return Err(e.into()),
    };

    match serde_json::from_str::<MakeStopOrderRes>(&response_string) {
        Ok(res) => Ok(res),
        Err(e) => {
            let error_msg: String = format!("Failed to deserialize response '{}' as {}: {}", response_string, stringify!(MakeStopOrderRes), e);
            log::error!("{}", error_msg);
            return Err(error_msg);
        }
    }
}
pub async fn add_api_v3_hf_margin_order(body_str: String) -> Result<MakeOrderRes, String> {
    let client: &KuCoinClient = match get_client() {
        Ok(client) => client,
        Err(e) => return Err(e),
    };

    let response_string: String = match client.add_api_v3_hf_margin_order(body_str).await {
        Ok(response_string) => response_string,
        Err(e) => return Err(e.into()),
    };

    match serde_json::from_str::<MakeOrderRes>(&response_string) {
        Ok(res) => Ok(res),
        Err(e) => {
            let error_msg: String = format!("Failed to deserialize response '{}' as {}: {}", response_string, stringify!(MakeOrderRes), e);
            log::error!("{}", error_msg);
            return Err(error_msg);
        }
    }
}
pub async fn create_repay_order(body_str: String) -> Result<ApiV3MarginRepayRes, String> {
    let client: &KuCoinClient = match get_client() {
        Ok(client) => client,
        Err(e) => return Err(e),
    };

    let response_string: String = match client.margin_repay(body_str).await {
        Ok(response_string) => response_string,
        Err(e) => return Err(e),
    };

    match serde_json::from_str::<ApiV3MarginRepayRes>(&response_string) {
        Ok(res) => Ok(res),
        Err(e) => {
            let error_msg: String = format!("Failed to deserialize response '{}' as {}: {}", response_string, stringify!(ApiV3MarginRepayRes), e);
            log::error!("{}", error_msg);
            return Err(error_msg);
        }
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

        let body = json!({
            "clientOid": "test-123",
            "side": "buy",
            "symbol": "BTC-USDT",
            "type": "market"
        });

        let body_str = serde_json::to_string(&body).unwrap();

        let signature = client.generate_signature(1234567890, "POST", "/api/v3/hf/margin/order", "", &body_str);

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

        let signature1 = client.generate_signature(1234567890, "POST", "/api/test", "", &body_str1);
        let signature2 = client.generate_signature(1234567890, "POST", "/api/test", "", &body_str2);

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

        let signature_with_empty = client.generate_signature(1234567890, "GET", "/api/test", "", "");

        let signature_with_none = client.generate_signature(1234567890, "GET", "/api/test", "", "");

        assert_eq!(signature_with_empty, signature_with_none);
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

        let query = "symbol=BTC-USDT&limit=10";
        let body_str = "";

        let signature = client.generate_signature(1234567890, "GET", "/api/v1/market/orderbook/level1", query, body_str);

        assert!(!signature.is_empty());
        println!("Signature with query params: {}", signature);
    }
}
