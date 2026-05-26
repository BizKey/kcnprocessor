use crate::api::models::{
    ApiV1MarketOrderbookLevel1Res, ApiV3AccountsUniversalTransferRes, ApiV3BulletPrivate, ApiV3HfMarginStopOrderCancelByClientOidRes, ApiV3HfMarginStopOrderCancelRes, ApiV3MarginRepayRes,
    MakeOrderRes, MakeStopOrderRes, MarginAccount,
};
use base64::Engine;
use hmac::{Hmac, Mac};
use micromap::Map;
use reqwest::{Client, Error, Method, Response};
use sha2::Sha256;
use smallvec::SmallVec;
use std::env;
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
    fn new(base_url: String) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        match Client::builder().timeout(Duration::from_secs(15)).connect_timeout(Duration::from_secs(5)).tcp_keepalive(Duration::from_secs(60)).build() {
            Ok(client) => Ok(Self {
                client,
                api_key: env::var("KUCOIN_KEY")?.trim().to_string(),
                api_secret: env::var("KUCOIN_SECRET")?.trim().to_string(),
                api_passphrase: env::var("KUCOIN_PASS")?.trim().to_string(),
                base_url,
            }),
            Err(e) => Err(e.into()),
        }
    }

    async fn api_v1_bullet_private(&self) -> Result<String, Error> {
        let response: Response = match self.make_request(Method::POST, "/api/v1/bullet-private", String::new(), String::new(), true, self.get_system_timestamp_ms()).await {
            Ok(response) => response,
            Err(e) => return Err(e),
        };

        let response: Response = match response.error_for_status() {
            Ok(response) => response,
            Err(e) => return Err(e.into()),
        };

        match response.text().await {
            Ok(response_string) => Ok(response_string),
            Err(e) => return Err(e.into()),
        }
    }

    fn generate_signature(&self, timestamp: u64, method: &str, endpoint: &str, query_string: &str, body: &str) -> String {
        let mut str_to_sign = format!("{}{}{}", timestamp, method.to_uppercase(), endpoint);

        if !query_string.is_empty() {
            str_to_sign.push('?');
            str_to_sign.push_str(query_string);
        }
        if !body.is_empty() {
            str_to_sign.push_str(body);
        }

        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes()).expect("HMAC can take key of any size");
        mac.update(str_to_sign.as_bytes());
        base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
    }

    fn generate_passphrase_signature(&self) -> String {
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes()).expect("HMAC can take key of any size");
        mac.update(self.api_passphrase.as_bytes());
        base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
    }

    async fn api_v3_hf_margin_stop_order_cancel_by_client_oid(&self, query_string_str: String) -> Result<String, Error> {
        let response: Response =
            match self.make_request(Method::DELETE, "/api/v3/hf/margin/stop-order/cancel-by-clientOid", query_string_str, String::new(), true, self.get_system_timestamp_ms()).await {
                Ok(response) => response,
                Err(e) => return Err(e),
            };

        let response: Response = match response.error_for_status() {
            Ok(response) => response,
            Err(e) => return Err(e.into()),
        };

        match response.text().await {
            Ok(response_string) => Ok(response_string),
            Err(e) => return Err(e),
        }
    }

    async fn get_margin_accounts(&self) -> Result<String, Error> {
        let mut query_params: Map<&str, &str, 8> = Map::new();

        query_params.insert("quoteCurrency", "USDT");
        query_params.insert("queryType", "MARGIN");

        let response: Response = match self.make_request(Method::GET, "/api/v3/margin/accounts", build_query_string(query_params), String::new(), true, self.get_system_timestamp_ms()).await {
            Ok(response) => response,
            Err(e) => return Err(e),
        };

        let response: Response = match response.error_for_status() {
            Ok(response) => response,
            Err(e) => return Err(e.into()),
        };

        match response.text().await {
            Ok(response_string) => Ok(response_string),
            Err(e) => return Err(e.into()),
        }
    }

    async fn batch_cancel_stop_orders(&self) -> Result<String, Error> {
        let mut query_params: Map<&str, &str, 8> = Map::new();

        query_params.insert("tradeType", "MARGIN_TRADE");

        let response: Response =
            match self.make_request(Method::DELETE, "/api/v3/hf/margin/stop-order/cancel", build_query_string(query_params), String::new(), true, self.get_system_timestamp_ms()).await {
                Ok(response) => response,
                Err(e) => return Err(e),
            };

        let response: Response = match response.error_for_status() {
            Ok(response) => response,
            Err(e) => return Err(e.into()),
        };

        match response.text().await {
            Ok(response_string) => Ok(response_string),
            Err(e) => Err(e.into()),
        }
    }

    async fn account_transfer(&self, body_str: String) -> Result<String, Error> {
        let response: Response = match self.make_request(Method::POST, "/api/v3/accounts/universal-transfer", String::new(), body_str, true, self.get_system_timestamp_ms()).await {
            Ok(response) => response,
            Err(e) => return Err(e),
        };

        let response: Response = match response.error_for_status() {
            Ok(response) => response,
            Err(e) => return Err(e.into()),
        };

        match response.text().await {
            Ok(response_string) => Ok(response_string),
            Err(e) => return Err(e.into()),
        }
    }

    async fn api_v3_hf_margin_stop_order(&self, body_str: String) -> Result<String, Error> {
        let response: Response = match self.make_request(Method::POST, "/api/v3/hf/margin/stop-order", String::new(), body_str, true, self.get_system_timestamp_ms()).await {
            Ok(response) => response,
            Err(e) => return Err(e),
        };

        let response: Response = match response.error_for_status() {
            Ok(response) => response,
            Err(e) => return Err(e.into()),
        };

        match response.text().await {
            Ok(response_string) => Ok(response_string),
            Err(e) => return Err(e.into()),
        }
    }

    async fn add_api_v3_hf_margin_order(&self, body_str: String) -> Result<String, Error> {
        let response: Response = match self.make_request(Method::POST, "/api/v3/hf/margin/order", String::new(), body_str, true, self.get_system_timestamp_ms()).await {
            Ok(response) => response,
            Err(e) => return Err(e),
        };

        let response: Response = match response.error_for_status() {
            Ok(response) => response,
            Err(e) => return Err(e.into()),
        };

        match response.text().await {
            Ok(response_string) => return Ok(response_string),
            Err(e) => return Err(e.into()),
        };
    }

    fn get_system_timestamp_ms(&self) -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
    }

    async fn margin_repay(&self, body_str: String) -> Result<String, Error> {
        let response: Response = match self.make_request(Method::POST, "/api/v3/margin/repay", String::new(), body_str, true, self.get_system_timestamp_ms()).await {
            Ok(response) => response,
            Err(e) => return Err(e),
        };

        let response: Response = match response.error_for_status() {
            Ok(response) => response,
            Err(e) => return Err(e),
        };

        match response.text().await {
            Ok(response_string) => return Ok(response_string),
            Err(e) => return Err(e.into()),
        };
    }

    async fn get_ticker_price(&self, symbol: &str) -> Result<String, Error> {
        let mut query_params: Map<&str, &str, 8> = Map::new();

        query_params.insert("symbol", symbol);

        let response: Response = match self.make_request(Method::GET, "/api/v1/market/orderbook/level1", build_query_string(query_params), String::new(), false, 0).await {
            Ok(response) => response,
            Err(e) => return Err(e),
        };

        let response: Response = match response.error_for_status() {
            Ok(response) => response,
            Err(e) => return Err(e),
        };

        match response.text().await {
            Ok(response_string) => Ok(response_string),
            Err(e) => return Err(e),
        }
    }

    async fn make_request(&self, method: Method, endpoint: &str, query_string: String, body_str: String, authenticated: bool, timestamp: u64) -> Result<Response, Error> {
        let url: String = if !query_string.is_empty() { format!("{}{}?{}", self.base_url, endpoint, query_string) } else { format!("{}{}", self.base_url, endpoint) };

        let mut request_builder: reqwest::RequestBuilder = self.client.request(method.clone(), &url);

        if authenticated {
            request_builder = request_builder
                .header("KC-API-KEY", &self.api_key)
                .header("KC-API-SIGN", self.generate_signature(timestamp, method.as_ref(), endpoint, &query_string, &body_str))
                .header("KC-API-TIMESTAMP", timestamp.to_string())
                .header("KC-API-PASSPHRASE", self.generate_passphrase_signature())
                .header("KC-API-KEY-VERSION", "2");

            if !body_str.is_empty() {
                request_builder = request_builder.header("Content-Type", "application/json").body(body_str);
            }
        }
        match request_builder.send().await {
            Ok(response) => Ok(response),
            Err(e) => Err(e),
        }
    }
}

static KUCLIENT: OnceLock<Result<KuCoinClient, String>> = OnceLock::new();

pub fn serialize_body(body: &Option<serde_json::Value>) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    match body {
        Some(b) => serde_json::to_string(b).map_err(|e| format!("JSON serialization error: {}", e).into()),
        None => Ok(String::new()),
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

fn get_client() -> Result<&'static KuCoinClient, Box<dyn std::error::Error + Send + Sync>> {
    KUCLIENT
        .get_or_init(|| {
            let base_url = env::var("KUCOIN_BASE_URL").unwrap_or_else(|_| "https://api.kucoin.com".to_string());

            KuCoinClient::new(base_url).map_err(|e| format!("Failed to init KuCoinClient: {}", e))
        })
        .as_ref()
        .map_err(|e| e.clone().into())
}

pub async fn get_private_ws_url() -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let client: &KuCoinClient = match get_client() {
        Ok(client) => client,
        Err(e) => return Err(e.into()),
    };

    let response_string: String = match client.api_v1_bullet_private().await {
        Ok(response_string) => response_string,
        Err(e) => return Err(e.into()),
    };

    let ws: ApiV3BulletPrivate = match serde_json::from_str::<ApiV3BulletPrivate>(&response_string) {
        Ok(res) => res,
        Err(e) => return Err(e.into()),
    };
    ws.data.instance_servers.first().map(|s| format!("{}?token={}", s.endpoint, ws.data.token)).ok_or_else(|| "No instance servers in bullet response".into())
}
pub async fn get_all_margin_accounts() -> Result<MarginAccount, Box<dyn std::error::Error + Send + Sync>> {
    let client: &KuCoinClient = match get_client() {
        Ok(client) => client,
        Err(e) => return Err(e.into()),
    };

    let response_string: String = match client.get_margin_accounts().await {
        Ok(response_string) => response_string,
        Err(e) => return Err(e.into()),
    };

    match serde_json::from_str::<MarginAccount>(&response_string) {
        Ok(res) => Ok(res),
        Err(e) => Err(e.into()),
    }
}
pub async fn api_v3_hf_margin_stop_order_cancel_by_client_oid(query_string_str: String) -> Result<ApiV3HfMarginStopOrderCancelByClientOidRes, Box<dyn std::error::Error + Send + Sync>> {
    let client: &KuCoinClient = match get_client() {
        Ok(client) => client,
        Err(e) => return Err(e.into()),
    };

    let response_string: String = match client.api_v3_hf_margin_stop_order_cancel_by_client_oid(query_string_str).await {
        Ok(response_string) => response_string,
        Err(e) => return Err(e.into()),
    };

    match serde_json::from_str::<ApiV3HfMarginStopOrderCancelByClientOidRes>(&response_string) {
        Ok(res) => Ok(res),
        Err(e) => Err(e.into()),
    }
}
pub async fn sent_account_transfer(body_str: String) -> Result<ApiV3AccountsUniversalTransferRes, Box<dyn std::error::Error + Send + Sync>> {
    let client: &KuCoinClient = match get_client() {
        Ok(client) => client,
        Err(e) => return Err(e.into()),
    };

    let response_string: String = match client.account_transfer(body_str).await {
        Ok(response_string) => response_string,
        Err(e) => return Err(e.into()),
    };

    match serde_json::from_str::<ApiV3AccountsUniversalTransferRes>(&response_string) {
        Ok(res) => Ok(res),
        Err(e) => Err(e.into()),
    }
}
pub async fn get_ticker_price(symbol: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let client: &KuCoinClient = match get_client() {
        Ok(client) => client,
        Err(e) => return Err(e.into()),
    };

    let response_string: String = match client.get_ticker_price(symbol).await {
        Ok(response_string) => response_string,
        Err(e) => return Err(e.into()),
    };

    match serde_json::from_str::<ApiV1MarketOrderbookLevel1Res>(&response_string) {
        Ok(res) => Ok(res.data.price),
        Err(e) => Err(e.into()),
    }
}

pub async fn batch_cancel_stop_orders() -> Result<ApiV3HfMarginStopOrderCancelRes, Box<dyn std::error::Error + Send + Sync>> {
    let client: &KuCoinClient = match get_client() {
        Ok(client) => client,
        Err(e) => return Err(e.into()),
    };

    let response_string: String = match client.batch_cancel_stop_orders().await {
        Ok(response_string) => response_string,
        Err(e) => return Err(e.into()),
    };

    match serde_json::from_str::<ApiV3HfMarginStopOrderCancelRes>(&response_string) {
        Ok(res) => Ok(res),
        Err(e) => Err(e.into()),
    }
}
pub async fn api_v3_hf_margin_stop_order(body_str: String) -> Result<MakeStopOrderRes, Box<dyn std::error::Error + Send + Sync>> {
    let client: &KuCoinClient = match get_client() {
        Ok(client) => client,
        Err(e) => return Err(e.into()),
    };

    let response_string: String = match client.api_v3_hf_margin_stop_order(body_str).await {
        Ok(response_string) => response_string,
        Err(e) => return Err(e.into()),
    };

    match serde_json::from_str::<MakeStopOrderRes>(&response_string) {
        Ok(res) => Ok(res),
        Err(e) => Err(e.into()),
    }
}
pub async fn add_api_v3_hf_margin_order(body_str: String) -> Result<MakeOrderRes, Box<dyn std::error::Error + Send + Sync>> {
    let client: &KuCoinClient = match get_client() {
        Ok(client) => client,
        Err(e) => return Err(e.into()),
    };

    let response_string: String = match client.add_api_v3_hf_margin_order(body_str).await {
        Ok(response_string) => response_string,
        Err(e) => return Err(e.into()),
    };

    match serde_json::from_str::<MakeOrderRes>(&response_string) {
        Ok(res) => Ok(res),
        Err(e) => Err(e.into()),
    }
}

pub async fn create_repay_order(body_str: String) -> Result<ApiV3MarginRepayRes, Box<dyn std::error::Error + Send + Sync>> {
    let client: &KuCoinClient = match get_client() {
        Ok(client) => client,
        Err(e) => return Err(e.into()),
    };

    let response_string: String = match client.margin_repay(body_str).await {
        Ok(response_string) => response_string,
        Err(e) => return Err(e.into()),
    };

    match serde_json::from_str::<ApiV3MarginRepayRes>(&response_string) {
        Ok(res) => Ok(res),
        Err(e) => Err(e.into()),
    }
}

// В конце файла src/api/requests.rs
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
