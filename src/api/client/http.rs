use anyhow::{Context, Result};
use reqwest::{Client, Method, Response};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct HttpClient {
    client: Client,
}

impl HttpClient {
    pub fn new() -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(15))
            .connect_timeout(Duration::from_secs(5))
            .tcp_keepalive(Duration::from_secs(60))
            .build()
            .context("Get error on Client::builder")?;

        Ok(Self { client })
    }

    pub async fn send_request(
        &self,
        method: Method,
        url: &str,
        headers: Vec<(String, String)>,
        body: Option<String>,
    ) -> Result<Response> {
        let mut request_builder = self.client.request(method, url);

        for (key, value) in headers {
            request_builder = request_builder.header(key, value);
        }

        if let Some(body) = body {
            request_builder = request_builder
                .header("Content-Type", "application/json")
                .body(body);
        }

        let response = request_builder.send().await.map_err(|e| {
            if e.is_timeout() {
                anyhow::anyhow!("Timeout {}: {}", url, e)
            } else if e.is_connect() {
                anyhow::anyhow!("Error connection {}: {}", url, e)
            } else if e.is_request() {
                anyhow::anyhow!("Error prepare request {}: {}", url, e)
            } else if e.is_body() {
                anyhow::anyhow!("Error in body {}: {}", url, e)
            } else {
                anyhow::anyhow!("Unexpected error {}: {}", url, e)
            }
        })?;

        Ok(response)
    }
}
