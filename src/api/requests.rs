use crate::api::models::ApiV3BulletPrivate;
use base64::Engine;
use hmac::{Hmac, Mac};
use reqwest::{Client, Response};

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
        let string_to_sign = format!("{}{}{}{}", timestamp, method, endpoint, query_string);
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
    async fn make_request(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        query_params: Option<HashMap<&str, &str>>,
        body: Option<HashMap<&str, &str>>,
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
