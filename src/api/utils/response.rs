use anyhow::{Context, Result};
use reqwest::Response;

pub struct ResponseHandler;

impl ResponseHandler {
    pub async fn read_response(response: Response) -> Result<String> {
        let status = response.status().as_u16();
        let body = response
            .text()
            .await
            .context("Failed to read response body")?;

        match status {
            200 => Ok(body),
            _ => anyhow::bail!("API returned error status {}: {}", status, body),
        }
    }
}
