use crate::api::models::{
    ActualPrice, ApiV3BulletPrivate, CancelOrderRes, MakeOrderRes, MarginAccount,
    MarginAccountData, OldActiveOrders, OldActiveOrdersData, SymbolOpenOrder,
};
use base64::Engine;
use hmac::{Hmac, Mac};
use log::{error, info};
use reqwest::{Client, Response};
use serde_json::json;
use uuid::Uuid;

use sha2::Sha256;
use std::collections::HashMap;
use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

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
        let api_passphrase = match env::var("KUCOIN_PASS") {
            Ok(val) => val,
            Err(e) => return Err(e.into()),
        };

        let api_key = match env::var("KUCOIN_KEY") {
            Ok(val) => val,
            Err(e) => return Err(e.into()),
        };

        let api_secret = match env::var("KUCOIN_SECRET") {
            Ok(val) => val,
            Err(e) => return Err(e.into()),
        };

        Ok(Self {
            client: Client::new(),
            api_key,
            api_secret,
            api_passphrase,
            base_url,
        })
    }

    pub async fn api_v1_bullet_private(
        &self,
    ) -> Result<ApiV3BulletPrivate, Box<dyn std::error::Error + Send + Sync>> {
        return match self
            .make_request(
                reqwest::Method::POST,
                "/api/v1/bullet-private",
                None,
                None,
                true,
            )
            .await
        {
            Ok(response) => match response.status().as_str() {
                "200" => match response.text().await {
                    Ok(text) => match serde_json::from_str::<ApiV3BulletPrivate>(&text) {
                        Ok(r) => match r.code.as_str() {
                            "200000" => Ok(r),
                            _ => Err(format!("API error: code {}", r.code).into()),
                        },
                        Err(e) => Err(format!(
                            "Error JSON deserialize:'{}' with data: '{}'",
                            e, text
                        )
                        .into()),
                    },
                    Err(e) => {
                        return Err(format!("Error get text response from HTTP:'{}'", e).into());
                    }
                },
                status => match response.text().await {
                    Ok(text) => {
                        Err(format!("Wrong HTTP status: '{}' with body: '{}'", status, text).into())
                    }
                    Err(_) => Err(format!("Wrong HTTP status: '{}'", status).into()),
                },
            },
            Err(e) => return Err(format!("Error HTTP:'{}'", e).into()),
        };
    }

    fn generate_signature(
        &self,
        timestamp: u64,
        method: &str,
        endpoint: &str,
        query_string: &str,
        body: &str,
    ) -> String {
        let method_upper = method.to_uppercase();

        let string_to_sign = if method_upper == "DELETE" {
            if !query_string.is_empty() {
                format!("{}{}{}?{}", timestamp, method_upper, endpoint, query_string)
            } else {
                format!("{}{}{}", timestamp, method_upper, endpoint)
            }
        } else if !query_string.is_empty() {
            format!(
                "{}{}{}?{}{}",
                timestamp, method_upper, endpoint, query_string, body
            )
        } else {
            format!("{}{}{}{}", timestamp, method_upper, endpoint, body)
        };

        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(string_to_sign.as_bytes());
        let result = mac.finalize();
        base64::engine::general_purpose::STANDARD.encode(result.into_bytes())
    }

    fn generate_passphrase_signature(&self) -> String {
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(self.api_passphrase.as_bytes());
        let result = mac.finalize();
        base64::engine::general_purpose::STANDARD.encode(result.into_bytes())
    }

    pub async fn get_margin_accounts(
        &self,
    ) -> Result<MarginAccountData, Box<dyn std::error::Error + Send + Sync>> {
        let mut query_params = std::collections::HashMap::new();
        query_params.insert("quoteCurrency", "USDT");
        query_params.insert("queryType", "MARGIN");
        match self
            .make_request(
                reqwest::Method::GET,
                "/api/v3/margin/accounts",
                Some(query_params),
                None,
                true,
            )
            .await
        {
            Ok(response) => match response.status().as_str() {
                "200" => match response.text().await {
                    Ok(text) => match serde_json::from_str::<MarginAccount>(&text) {
                        Ok(res) => Ok(res.data),
                        Err(e) => Err(format!(
                            "Error JSON deserialize:'{}' with data: '{}'",
                            e, text
                        )
                        .into()),
                    },
                    Err(e) => Err(format!("Error get text response from HTTP:'{}'", e).into()),
                },
                status => match response.text().await {
                    Ok(text) => {
                        Err(format!("Wrong HTTP status: '{}' with body: '{}'", status, text).into())
                    }
                    Err(_) => Err(format!("Wrong HTTP status: '{}'", status).into()),
                },
            },
            Err(e) => Err(format!("Error HTTP:'{}'", e).into()),
        }
    }
    pub async fn get_old_active_orders_list(
        &self,
    ) -> Result<OldActiveOrdersData, Box<dyn std::error::Error + Send + Sync>> {
        let mut query_params = std::collections::HashMap::new();
        query_params.insert("status", "active");
        query_params.insert("tradeType", "MARGIN_TRADE");
        match self
            .make_request(
                reqwest::Method::GET,
                "/api/v1/orders",
                Some(query_params),
                None,
                true,
            )
            .await
        {
            Ok(response) => match response.status().as_str() {
                "200" => match response.text().await {
                    Ok(text) => match serde_json::from_str::<OldActiveOrders>(&text) {
                        Ok(res) => Ok(res.data),
                        Err(e) => Err(format!(
                            "Error JSON deserialize:'{}' with data: '{}'",
                            e, text
                        )
                        .into()),
                    },
                    Err(e) => Err(format!("Error get text response from HTTP:'{}'", e).into()),
                },
                status => match response.text().await {
                    Ok(text) => {
                        Err(format!("Wrong HTTP status: '{}' with body: '{}'", status, text).into())
                    }
                    Err(_) => Err(format!("Wrong HTTP status: '{}'", status).into()),
                },
            },
            Err(e) => Err(format!("Error HTTP:'{}'", e).into()),
        }
    }
    pub async fn get_symbols_with_open_order(
        &self,
    ) -> Result<SymbolOpenOrder, Box<dyn std::error::Error + Send + Sync>> {
        let mut query_params = std::collections::HashMap::new();
        query_params.insert("tradeType", "MARGIN_TRADE");
        match self
            .make_request(
                reqwest::Method::GET,
                "/api/v3/hf/margin/order/active/symbols",
                Some(query_params),
                None,
                true,
            )
            .await
        {
            Ok(response) => match response.status().as_str() {
                "200" => match response.text().await {
                    Ok(text) => match serde_json::from_str::<SymbolOpenOrder>(&text) {
                        Ok(res) => Ok(res),
                        Err(e) => Err(format!(
                            "Error JSON deserialize:'{}' with data: '{}'",
                            e, text
                        )
                        .into()),
                    },
                    Err(e) => Err(format!("Error get text response from HTTP:'{}'", e).into()),
                },
                status => match response.text().await {
                    Ok(text) => {
                        Err(format!("Wrong HTTP status: '{}' with body: '{}'", status, text).into())
                    }
                    Err(_) => Err(format!("Wrong HTTP status: '{}'", status).into()),
                },
            },
            Err(e) => Err(format!("Error HTTP:'{}'", e).into()),
        }
    }
    pub async fn cancel_all_orders_by_symbol(&self, symbol: &str) {
        let mut query_params = std::collections::HashMap::new();
        query_params.insert("tradeType", "MARGIN_TRADE");
        query_params.insert("symbol", symbol);
        match self
            .make_request(
                reqwest::Method::DELETE,
                "/api/v3/hf/margin/orders",
                Some(query_params),
                None,
                true,
            )
            .await
        {
            Ok(response) => match response.text().await {
                Ok(text) => {
                    info!("{:.?}", text);
                }
                Err(e) => {
                    error!("Error get text response from HTTP: {}", e);
                }
            },
            Err(e) => {
                error!("Error HTTP: {}", e);
            }
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

        match self
            .make_request(
                reqwest::Method::POST,
                "/api/v3/accounts/universal-transfer",
                None,
                Some(body),
                true,
            )
            .await
        {
            Ok(response) => match response.status().as_str() {
                "200" => match response.text().await {
                    Ok(text) => {
                        info!("flex transfer {}", text);
                        Ok(())
                    }
                    Err(e) => Err(format!("Error get text response from HTTP:'{}'", e).into()),
                },
                status => match response.text().await {
                    Ok(text) => {
                        Err(format!("Wrong HTTP status: '{}' with body: '{}'", status, text).into())
                    }
                    Err(_) => Err(format!("Wrong HTTP status: '{}'", status).into()),
                },
            },
            Err(e) => Err(format!("Account transfer request failed: {}", e).into()),
        }
    }
    pub async fn old_cancel_order(
        &self,
        order_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self
            .make_request(
                reqwest::Method::DELETE,
                format!("/api/v1/orders/{}", order_id).as_str(),
                None,
                None,
                true,
            )
            .await
        {
            Ok(response) => match response.status().as_str() {
                "200" => Ok(()),
                status => match response.text().await {
                    Ok(text) => {
                        Err(format!("Wrong HTTP status: '{}' with body: '{}'", status, text).into())
                    }
                    Err(_) => Err(format!("Wrong HTTP status: '{}'", status).into()),
                },
            },
            Err(e) => Err(format!("Margin repay request failed: {}", e).into()),
        }
    }
    pub async fn cancel_order(
        &self,
        symbol: &str,
        order_id: &str,
    ) -> Result<CancelOrderRes, Box<dyn std::error::Error + Send + Sync>> {
        let mut query_params = std::collections::HashMap::new();

        query_params.insert("symbol", symbol);
        match self
            .make_request(
                reqwest::Method::DELETE,
                format!("/api/v3/hf/margin/orders/{}", order_id).as_str(),
                Some(query_params),
                None,
                true,
            )
            .await
        {
            Ok(response) => match response.status().as_str() {
                "200" => match response.text().await {
                    Ok(text) => match serde_json::from_str::<CancelOrderRes>(&text) {
                        Ok(res) => Ok(res),
                        Err(e) => Err(format!(
                            "Error JSON deserialize:'{}' with data: '{}'",
                            e, text
                        )
                        .into()),
                    },
                    Err(e) => Err(format!("Error get text response from HTTP:'{}'", e).into()),
                },
                status => match response.text().await {
                    Ok(text) => {
                        Err(format!("Wrong HTTP status: '{}' with body: '{}'", status, text).into())
                    }
                    Err(_) => Err(format!("Wrong HTTP status: '{}'", status).into()),
                },
            },
            Err(e) => Err(format!("Margin repay request failed: {}", e).into()),
        }
    }
    pub async fn add_v1_order(
        &self,
        body: serde_json::Value,
    ) -> Result<MakeOrderRes, Box<dyn std::error::Error + Send + Sync>> {
        match self
            .make_request(
                reqwest::Method::POST,
                "/api/v1/margin/order",
                None,
                Some(body.clone()),
                true,
            )
            .await
        {
            Ok(response) => match response.status().as_str() {
                "200" => match response.text().await {
                    Ok(text) => match serde_json::from_str::<MakeOrderRes>(&text) {
                        Ok(res) => Ok(res),
                        Err(e) => Err(format!(
                            "Error JSON deserialize:'{}' with data: '{}'",
                            e, text
                        )
                        .into()),
                    },
                    Err(e) => Err(format!("Error get text response from HTTP:'{}'", e).into()),
                },
                status => match response.text().await {
                    Ok(text) => {
                        Err(format!("Wrong HTTP status: '{}' with body: '{}'", status, text).into())
                    }
                    Err(_) => Err(format!("Wrong HTTP status: '{}'", status).into()),
                },
            },
            Err(e) => Err(format!("Margin repay request failed: {}", e).into()),
        }
    }
    pub async fn add_order(
        &self,
        body: serde_json::Value,
    ) -> Result<MakeOrderRes, Box<dyn std::error::Error + Send + Sync>> {
        match self
            .make_request(
                reqwest::Method::POST,
                "/api/v3/hf/margin/order",
                None,
                Some(body.clone()),
                true,
            )
            .await
        {
            Ok(response) => match response.status().as_str() {
                "200" => match response.text().await {
                    Ok(text) => match serde_json::from_str::<MakeOrderRes>(&text) {
                        Ok(res) => Ok(res),
                        Err(e) => Err(format!(
                            "Error JSON deserialize:'{}' with data: '{}'",
                            e, text
                        )
                        .into()),
                    },
                    Err(e) => Err(format!("Error get text response from HTTP:'{}'", e).into()),
                },
                status => match response.text().await {
                    Ok(text) => {
                        Err(format!("Wrong HTTP status: '{}' with body: '{}'", status, text).into())
                    }
                    Err(_) => Err(format!("Wrong HTTP status: '{}'", status).into()),
                },
            },
            Err(e) => Err(format!("Margin repay request failed: {}", e).into()),
        }
    }
    pub async fn margin_repay(
        &self,
        currency: &str,
        size: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let body: serde_json::Value = json!({
            "currency": currency,
            "size": size,
            "isIsolated": false,
            "isHf": false
        });

        match self
            .make_request(
                reqwest::Method::POST,
                "/api/v3/margin/repay",
                None,
                Some(body),
                true,
            )
            .await
        {
            Ok(response) => match response.status().as_str() {
                "200" => {
                    info!("Successfully repaid {} {} debt", size, currency);
                    Ok(())
                }
                status => match response.text().await {
                    Ok(text) => {
                        Err(format!("Wrong HTTP status: '{}' with body: '{}'", status, text).into())
                    }
                    Err(_) => Err(format!("Wrong HTTP status: '{}'", status).into()),
                },
            },
            Err(e) => Err(format!("Margin repay request failed: {}", e).into()),
        }
    }
    pub async fn get_ticker_price(
        &self,
        symbol: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let mut query_params = std::collections::HashMap::new();
        query_params.insert("symbol", symbol);

        match self
            .make_request(
                reqwest::Method::GET,
                "/api/v1/market/orderbook/level1",
                Some(query_params),
                None,
                false,
            )
            .await
        {
            Ok(response) => match response.status().as_str() {
                "200" => match response.text().await {
                    Ok(text) => match serde_json::from_str::<ActualPrice>(&text) {
                        Ok(res) => Ok(res.data.price),
                        Err(e) => Err(format!(
                            "Error JSON deserialize:'{}' with data: '{}'",
                            e, text
                        )
                        .into()),
                    },
                    Err(e) => Err(format!("Error get text response from HTTP:'{}'", e).into()),
                },
                status => match response.text().await {
                    Ok(text) => {
                        Err(format!("Wrong HTTP status: '{}' with body: '{}'", status, text).into())
                    }
                    Err(_) => Err(format!("Wrong HTTP status: '{}'", status).into()),
                },
            },
            Err(e) => Err(format!("Margin repay request failed: {}", e).into()),
        }
    }

    async fn make_request(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        query_params: Option<HashMap<&str, &str>>,
        body: Option<serde_json::Value>,
        authenticated: bool,
    ) -> Result<Response, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}{}", self.base_url, endpoint);

        let mut request_builder = self.client.request(method.clone(), &url);

        if let Some(params) = &query_params {
            request_builder = request_builder.query(&params);
        }

        if let Some(body_data) = &body {
            request_builder = request_builder.json(&body_data);
        }

        if authenticated {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            let query_string = query_params
                .as_ref()
                .map(|params| {
                    let mut pairs: Vec<String> =
                        params.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
                    pairs.sort();
                    pairs.join("&")
                })
                .unwrap_or_default();
            let body_str = body
                .as_ref()
                .map(|b| serde_json::to_string(b).unwrap())
                .unwrap_or_default();

            let signature = self.generate_signature(
                timestamp,
                method.as_ref(),
                endpoint,
                &query_string,
                &body_str,
            );

            let passphrase_signature = self.generate_passphrase_signature();

            request_builder = request_builder
                .header("KC-API-KEY", &self.api_key)
                .header("KC-API-SIGN", signature)
                .header("KC-API-TIMESTAMP", timestamp.to_string())
                .header("KC-API-PASSPHRASE", passphrase_signature)
                .header("KC-API-KEY-VERSION", "2");
        }

        let response = request_builder.send().await.map_err(|e| {
            if e.is_timeout() {
                format!("Timeout {}: {}", url, e)
            } else if e.is_connect() {
                format!("Error connection {}: {}", url, e)
            } else if e.is_request() {
                format!("Error prepare request {}: {}", url, e)
            } else if e.is_body() {
                format!("Error in body {}: {}", url, e)
            } else {
                format!("Unexpected error {}: {}", url, e)
            }
        })?;

        Ok(response)
    }
}

pub async fn get_private_ws_url() -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let client: KuCoinClient = KuCoinClient::new("https://api.kucoin.com".to_string())?;
    let bullet_private: ApiV3BulletPrivate = client.api_v1_bullet_private().await?;
    bullet_private
        .data
        .instance_servers
        .first()
        .map(|s| format!("{}?token={}", s.endpoint, bullet_private.data.token))
        .ok_or_else(|| "No instance servers in bullet response".into())
}
pub async fn get_all_margin_accounts()
-> Result<MarginAccountData, Box<dyn std::error::Error + Send + Sync>> {
    let client: KuCoinClient = KuCoinClient::new("https://api.kucoin.com".to_string())?;
    client.get_margin_accounts().await
}
pub async fn sent_account_transfer(
    currency: &str,
    amount: &str,
    type_: &str,
    from_account_type: &str,
    to_account_type: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client: KuCoinClient = KuCoinClient::new("https://api.kucoin.com".to_string())?;
    client
        .account_transfer(
            currency,
            &Uuid::new_v4().to_string(),
            amount,
            type_,
            from_account_type,
            to_account_type,
        )
        .await
}
pub async fn get_ticker_price(
    symbol: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let client: KuCoinClient = KuCoinClient::new("https://api.kucoin.com".to_string())?;
    client.get_ticker_price(symbol).await
}
pub async fn cancel_all_open_orders() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client: KuCoinClient = KuCoinClient::new("https://api.kucoin.com".to_string())?;
    let symbols = client.get_symbols_with_open_order().await?;
    for symbol in symbols.data.symbols.iter() {
        info!("Open orders for:{:.?}", symbol);
        let client2: KuCoinClient = KuCoinClient::new("https://api.kucoin.com".to_string())?;
        client2.cancel_all_orders_by_symbol(symbol).await;
    }
    Ok(())
}
pub async fn get_old_active_orders_list()
-> Result<OldActiveOrdersData, Box<dyn std::error::Error + Send + Sync>> {
    let client: KuCoinClient = KuCoinClient::new("https://api.kucoin.com".to_string())?;
    client.get_old_active_orders_list().await
}
pub async fn add_order(
    body: serde_json::Value,
) -> Result<MakeOrderRes, Box<dyn std::error::Error + Send + Sync>> {
    let client = KuCoinClient::new("https://api.kucoin.com".to_string())?;
    client.add_order(body).await
}
pub async fn add_v1_order(
    body: serde_json::Value,
) -> Result<MakeOrderRes, Box<dyn std::error::Error + Send + Sync>> {
    let client = KuCoinClient::new("https://api.kucoin.com".to_string())?;
    client.add_v1_order(body).await
}
pub async fn cancel_order(
    symbol: &str,
    order_id: &str,
) -> Result<CancelOrderRes, Box<dyn std::error::Error + Send + Sync>> {
    let client = KuCoinClient::new("https://api.kucoin.com".to_string())?;
    client.cancel_order(symbol, order_id).await
}
pub async fn old_cancel_order(
    order_id: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = KuCoinClient::new("https://api.kucoin.com".to_string())?;
    client.old_cancel_order(order_id).await
}
pub async fn create_repay_order(
    currency: &str,
    size: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Ok(size_val) = size.parse::<f64>()
        && size_val <= 0.0
    {
        info!(
            "Skip repay for {} with zero/negative size: {}",
            currency, size
        );
        return Ok(());
    }

    let client = KuCoinClient::new("https://api.kucoin.com".to_string())?;
    client.margin_repay(currency, size).await?;

    Ok(())
}
