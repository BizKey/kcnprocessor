use anyhow::Result;
use reqwest::Method;

use super::http::HttpClient;
use crate::api::auth::AuthCredentials;
use crate::api::utils::ResponseHandler;

pub struct KuCoinEndpoints {
    client: HttpClient,
    auth: AuthCredentials,
    base_url: String,
}

impl KuCoinEndpoints {
    pub fn new(client: HttpClient, auth: AuthCredentials, base_url: String) -> Self {
        Self {
            client,
            auth,
            base_url,
        }
    }

    async fn make_request(
        &self,
        method: Method,
        endpoint: &str,
        query_string: &str,
        body_str: &str,
        authenticated: bool,
    ) -> Result<String> {
        let url = if !query_string.is_empty() {
            format!("{}{}?{}", self.base_url, endpoint, query_string)
        } else {
            format!("{}{}", self.base_url, endpoint)
        };

        let mut headers = Vec::new();

        if authenticated {
            let auth_headers =
                self.auth
                    .build_auth_headers(&method, endpoint, query_string, body_str)?;
            headers.extend(auth_headers);
        }

        let body = if !body_str.is_empty() {
            Some(body_str.to_string())
        } else {
            None
        };

        let response = self
            .client
            .send_request(method, &url, headers, body)
            .await?;
        ResponseHandler::read_response(response).await
    }

    // === Bullet ===
    pub async fn bullet_private_post(&self) -> Result<String> {
        self.make_request(Method::POST, "/api/v1/bullet-private", "", "", true)
            .await
    }

    // === Margin ===
    pub async fn margin_accounts_get(&self, query_params: &str) -> Result<String> {
        self.make_request(
            Method::GET,
            "/api/v3/margin/accounts",
            query_params,
            "",
            true,
        )
        .await
    }

    pub async fn margin_repay_post(&self, body: &str) -> Result<String> {
        self.make_request(Method::POST, "/api/v3/margin/repay", "", body, true)
            .await
    }

    // === Order ===
    pub async fn hf_margin_order_post(&self, body: &str) -> Result<String> {
        self.make_request(Method::POST, "/api/v3/hf/margin/order", "", body, true)
            .await
    }

    pub async fn hf_margin_stop_order_post(&self, body: &str) -> Result<String> {
        self.make_request(Method::POST, "/api/v3/hf/margin/stop-order", "", body, true)
            .await
    }

    pub async fn hf_margin_stop_orders_get(&self, query_params: &str) -> Result<String> {
        self.make_request(
            Method::GET,
            "/api/v3/hf/margin/stop-orders",
            query_params,
            "",
            true,
        )
        .await
    }

    pub async fn hf_margin_stop_order_cancel_by_id_delete(
        &self,
        query_string: &str,
    ) -> Result<String> {
        self.make_request(
            Method::DELETE,
            "/api/v3/hf/margin/stop-order/cancel-by-id",
            query_string,
            "",
            true,
        )
        .await
    }

    pub async fn hf_margin_stop_order_cancel_by_client_oid_delete(
        &self,
        query_string: &str,
    ) -> Result<String> {
        self.make_request(
            Method::DELETE,
            "/api/v3/hf/margin/stop-order/cancel-by-clientOid",
            query_string,
            "",
            true,
        )
        .await
    }

    pub async fn accounts_universal_transfer_post(&self, body: &str) -> Result<String> {
        self.make_request(
            Method::POST,
            "/api/v3/accounts/universal-transfer",
            "",
            body,
            true,
        )
        .await
    }

    // === Market ===
    pub async fn market_orderbook_level1_get(&self, query_params: &str) -> Result<String> {
        self.make_request(
            Method::GET,
            "/api/v1/market/orderbook/level1",
            query_params,
            "",
            false,
        )
        .await
    }
}
