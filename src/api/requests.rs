use crate::api::models::{ActualPrice, ApiV3BulletPrivate, MakeOrderRes, MakeStopOrderRes, MarginAccount, MarginAccountData};
use base64::Engine;
use hmac::{Hmac, Mac};
use log;
use micromap::Map;
use reqwest::{Client, Error, Response};
use serde_json::json;
use sha2::Sha256;
use smallvec::SmallVec;
use std::collections::HashMap;
use std::env;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use urlencoding::encode;
use uuid::Uuid;
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
    pub fn new(base_url: String) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
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

    pub async fn api_v1_bullet_private(&self) -> Result<ApiV3BulletPrivate, Box<dyn std::error::Error + Send + Sync>> {
        return match self.make_request(reqwest::Method::POST, "/api/v1/bullet-private", String::new(), String::new(), true, self.get_system_timestamp_ms()).await {
            Ok(response) => match response.status().as_str() {
                "200" => match response.text().await {
                    Ok(text) => match serde_json::from_str::<ApiV3BulletPrivate>(&text) {
                        Ok(r) => match r.code.as_str() {
                            "200000" => Ok(r),
                            _ => Err(format!("API error: code {}", r.code).into()),
                        },
                        Err(e) => Err(format!("Error JSON deserialize:'{}' with data: '{}'", e, text).into()),
                    },
                    Err(e) => {
                        return Err(format!("Error get text response from HTTP:'{}'", e).into());
                    }
                },
                status => match response.text().await {
                    Ok(text) => Err(format!("Wrong HTTP status: '{}' with body: '{}'", status, text).into()),
                    Err(_) => Err(format!("Wrong HTTP status: '{}'", status).into()),
                },
            },
            Err(e) => return Err(format!("Error HTTP:'{}'", e).into()),
        };
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

    pub async fn api_v3_hf_margin_stop_order_cancel_by_client_oid(&self, client_oid: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut query_params: Map<&str, &str, 8> = Map::new();

        query_params.insert("clientOid", client_oid);

        let query_string: String = self.build_query_string(query_params);
        match self.make_request(reqwest::Method::DELETE, "/api/v3/hf/margin/stop-order/cancel-by-clientOid", query_string, String::new(), true, self.get_system_timestamp_ms()).await {
            Ok(response) => match response.status().as_str() {
                "200" => match response.text().await {
                    Ok(_) => Ok(()),
                    Err(e) => Err(format!("Error get text response from HTTP:'{}'", e).into()),
                },
                status => match response.text().await {
                    Ok(text) => Err(format!("Wrong HTTP status: '{}' with body: '{}'", status, text).into()),
                    Err(_) => Err(format!("Wrong HTTP status: '{}'", status).into()),
                },
            },
            Err(e) => Err(format!("Error HTTP:'{}'", e).into()),
        }
    }

    pub async fn get_margin_accounts(&self) -> Result<MarginAccountData, Box<dyn std::error::Error + Send + Sync>> {
        let mut query_params: Map<&str, &str, 8> = Map::new();

        query_params.insert("quoteCurrency", "USDT");
        query_params.insert("queryType", "MARGIN");

        let query_string: String = self.build_query_string(query_params);
        match self.make_request(reqwest::Method::GET, "/api/v3/margin/accounts", query_string, String::new(), true, self.get_system_timestamp_ms()).await {
            Ok(response) => match response.status().as_str() {
                "200" => match response.text().await {
                    Ok(text) => match serde_json::from_str::<MarginAccount>(&text) {
                        Ok(res) => Ok(res.data),
                        Err(e) => Err(format!("Error JSON deserialize:'{}' with data: '{}'", e, text).into()),
                    },
                    Err(e) => Err(format!("Error get text response from HTTP:'{}'", e).into()),
                },
                status => match response.text().await {
                    Ok(text) => Err(format!("Wrong HTTP status: '{}' with body: '{}'", status, text).into()),
                    Err(_) => Err(format!("Wrong HTTP status: '{}'", status).into()),
                },
            },
            Err(e) => Err(format!("Error HTTP:'{}'", e).into()),
        }
    }

    pub async fn batch_cancel_stop_orders(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut query_params: Map<&str, &str, 8> = Map::new();

        query_params.insert("tradeType", "MARGIN_TRADE");

        let query_string: String = self.build_query_string(query_params);
        match self.make_request(reqwest::Method::DELETE, "/api/v3/hf/margin/stop-order/cancel", query_string, String::new(), true, self.get_system_timestamp_ms()).await {
            Ok(response) => match response.status().as_str() {
                "200" => Ok(()),
                status => match response.text().await {
                    Ok(text) => Err(format!("Wrong HTTP status: '{}' with body: '{}'", status, text).into()),
                    Err(_) => Err(format!("Wrong HTTP status: '{}'", status).into()),
                },
            },
            Err(e) => Err(format!("Error HTTP:'{}'", e).into()),
        }
    }

    pub async fn account_transfer(
        &self,
        currency: &str,
        client_oid: &str,
        amount: &str,
        type_: &str,
        from_account_type: &str,
        to_account_type: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let body: serde_json::Value = json!({
            "currency": currency,
            "clientOid": client_oid,
            "amount": amount,
            "type": type_,
            "fromAccountType": from_account_type,
            "toAccountType": to_account_type
        });
        let body_str = self.serialize_body(&Some(body))?;
        match self.make_request(reqwest::Method::POST, "/api/v3/accounts/universal-transfer", String::new(), body_str, true, self.get_system_timestamp_ms()).await {
            Ok(response) => match response.status().as_str() {
                "200" => match response.text().await {
                    Ok(text) => {
                        log::info!("flex transfer {}", text);
                        Ok(())
                    }
                    Err(e) => Err(format!("Error get text response from HTTP:'{}'", e).into()),
                },
                status => match response.text().await {
                    Ok(text) => Err(format!("Wrong HTTP status: '{}' with body: '{}'", status, text).into()),
                    Err(_) => Err(format!("Wrong HTTP status: '{}'", status).into()),
                },
            },
            Err(e) => Err(format!("Account transfer request failed: {}", e).into()),
        }
    }

    pub async fn api_v3_hf_margin_stop_order(&self, body: serde_json::Value) -> Result<MakeStopOrderRes, Box<dyn std::error::Error + Send + Sync>> {
        // add stop margin hf order
        let body_str = self.serialize_body(&Some(body))?;
        match self.make_request(reqwest::Method::POST, "/api/v3/hf/margin/stop-order", String::new(), body_str, true, self.get_system_timestamp_ms()).await {
            Ok(response) => match response.status().as_str() {
                "200" => match response.text().await {
                    Ok(text) => match serde_json::from_str::<MakeStopOrderRes>(&text) {
                        Ok(res) => {
                            if res.code == "200000" {
                                Ok(res)
                            } else {
                                Err(format!("API business error: code={}, msg={:?}", res.code, res.msg).into())
                            }
                        }
                        Err(e) => Err(format!("Error JSON deserialize:'{}' with data: '{}'", e, text).into()),
                    },
                    Err(e) => Err(format!("Error get text response from HTTP:'{}'", e).into()),
                },
                status => match response.text().await {
                    Ok(text) => Err(format!("Wrong HTTP status: '{}' with body: '{}'", status, text).into()),
                    Err(_) => Err(format!("Wrong HTTP status: '{}'", status).into()),
                },
            },
            Err(e) => Err(format!("Margin repay request failed: {}", e).into()),
        }
    }

    pub async fn add_api_v3_hf_margin_order(&self, body: serde_json::Value) -> Result<MakeOrderRes, Box<dyn std::error::Error + Send + Sync>> {
        // add margin hf order
        let body_str = self.serialize_body(&Some(body))?;
        match self.make_request(reqwest::Method::POST, "/api/v3/hf/margin/order", String::new(), body_str, true, self.get_system_timestamp_ms()).await {
            Ok(response) => match response.status().as_str() {
                "200" => match response.text().await {
                    Ok(text) => match serde_json::from_str::<MakeOrderRes>(&text) {
                        Ok(res) => {
                            if res.code == "200000" {
                                Ok(res)
                            } else {
                                Err(format!("API business error: code={}, msg={:?}", res.code, res.msg).into())
                            }
                        }
                        Err(e) => Err(format!("Error JSON deserialize:'{}' with data: '{}'", e, text).into()),
                    },
                    Err(e) => Err(format!("Error get text response from HTTP:'{}'", e).into()),
                },
                status => match response.text().await {
                    Ok(text) => Err(format!("Wrong HTTP status: '{}' with body: '{}'", status, text).into()),
                    Err(_) => Err(format!("Wrong HTTP status: '{}'", status).into()),
                },
            },
            Err(e) => Err(format!("Margin repay request failed: {}", e).into()),
        }
    }
    fn get_system_timestamp_ms(&self) -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
    }
    pub async fn margin_repay(&self, currency: &str, size: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let body = json!({
            "currency": currency,
            "size": size,
            "isIsolated": false,
            "isHf": true
        });
        let body_str = self.serialize_body(&Some(body))?;

        match self.make_request(reqwest::Method::POST, "/api/v3/margin/repay", String::new(), body_str, true, self.get_system_timestamp_ms()).await {
            Ok(response) => match response.status().as_str() {
                "200" => {
                    log::info!("Successfully repaid {} {} debt", size, currency);
                    Ok(())
                }
                status => match response.text().await {
                    Ok(text) => Err(format!("Wrong HTTP status: '{}' with body: '{}'", status, text).into()),
                    Err(_) => Err(format!("Wrong HTTP status: '{}'", status).into()),
                },
            },
            Err(e) => Err(format!("Margin repay request failed: {}", e).into()),
        }
    }
    pub async fn get_ticker_price(&self, symbol: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let mut query_params: Map<&str, &str, 8> = Map::new();

        query_params.insert("symbol", symbol);

        let query_string: String = self.build_query_string(query_params);

        match self.make_request(reqwest::Method::GET, "/api/v1/market/orderbook/level1", query_string, String::new(), false, 0).await {
            Ok(response) => match response.status().as_str() {
                "200" => match response.text().await {
                    Ok(text) => match serde_json::from_str::<ActualPrice>(&text) {
                        Ok(res) => Ok(res.data.price),
                        Err(e) => Err(format!("Error JSON deserialize:'{}' with data: '{}'", e, text).into()),
                    },
                    Err(e) => Err(format!("Error get text response from HTTP:'{}'", e).into()),
                },
                status => match response.text().await {
                    Ok(text) => Err(format!("Wrong HTTP status: '{}' with body: '{}'", status, text).into()),
                    Err(_) => Err(format!("Wrong HTTP status: '{}'", status).into()),
                },
            },
            Err(e) => Err(format!("Margin repay request failed: {}", e).into()),
        }
    }

    fn serialize_body(&self, body: &Option<serde_json::Value>) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        match body {
            Some(b) => serde_json::to_string(b).map_err(|e| format!("JSON serialization error: {}", e).into()),
            None => Ok(String::new()),
        }
    }

    fn build_query_string(&self, query_params: Map<&str, &str, 8>) -> String {
        if query_params.is_empty() {
            return String::new();
        }

        let mut params: SmallVec<[(&str, &str); 8]> = query_params.into_iter().collect();
        params.sort_by(|a, b| a.0.cmp(b.0));

        let mut result = String::new();
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

    async fn make_request(&self, method: reqwest::Method, endpoint: &str, query_string: String, body_str: String, authenticated: bool, timestamp: u64) -> Result<Response, Error> {
        let url: String = if !query_string.is_empty() { format!("{}{}?{}", self.base_url, endpoint, query_string) } else { format!("{}{}", self.base_url, endpoint) };

        let mut request_builder = self.client.request(method.clone(), &url);

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
    match get_client() {
        Ok(client) => match client.api_v1_bullet_private().await {
            Ok(bullet_private) => {
                bullet_private.data.instance_servers.first().map(|s| format!("{}?token={}", s.endpoint, bullet_private.data.token)).ok_or_else(|| "No instance servers in bullet response".into())
            }
            Err(e) => Err(e),
        },
        Err(e) => Err(e),
    }
}
pub async fn get_all_margin_accounts() -> Result<MarginAccountData, Box<dyn std::error::Error + Send + Sync>> {
    match get_client() {
        Ok(client) => client.get_margin_accounts().await,
        Err(e) => Err(e),
    }
}
pub async fn api_v3_hf_margin_stop_order_cancel_by_client_oid(order_id: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match get_client() {
        Ok(client) => client.api_v3_hf_margin_stop_order_cancel_by_client_oid(order_id).await,
        Err(e) => Err(e),
    }
}
pub async fn sent_account_transfer(currency: &str, amount: &str, type_: &str, from_account_type: &str, to_account_type: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match get_client() {
        Ok(client) => client.account_transfer(currency, &Uuid::new_v4().to_string(), amount, type_, from_account_type, to_account_type).await,
        Err(e) => Err(e),
    }
}
pub async fn get_ticker_price(symbol: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    match get_client() {
        Ok(client) => client.get_ticker_price(symbol).await,
        Err(e) => Err(e),
    }
}

pub async fn batch_cancel_stop_orders() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match get_client() {
        Ok(client) => client.batch_cancel_stop_orders().await,
        Err(e) => Err(e),
    }
}
pub async fn api_v3_hf_margin_stop_order(body: serde_json::Value) -> Result<MakeStopOrderRes, Box<dyn std::error::Error + Send + Sync>> {
    match get_client() {
        Ok(client) => client.api_v3_hf_margin_stop_order(body).await,
        Err(e) => Err(e),
    }
}
pub async fn add_api_v3_hf_margin_order(body: serde_json::Value) -> Result<MakeOrderRes, Box<dyn std::error::Error + Send + Sync>> {
    match get_client() {
        Ok(client) => client.add_api_v3_hf_margin_order(body).await,
        Err(e) => Err(e),
    }
}

pub async fn create_repay_order(currency: &str, size: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match get_client() {
        Ok(client) => client.margin_repay(currency, size).await,
        Err(e) => Err(e),
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
