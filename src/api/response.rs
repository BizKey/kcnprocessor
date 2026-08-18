use anyhow::{Context, Result};
use reqwest::Response;
use serde::de::DeserializeOwned;

/// Обработчик ответов API
pub struct ResponseHandler;

impl ResponseHandler {
    /// Читает текст ответа и проверяет статус
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

    /// Десериализует ответ в указанный тип
    pub async fn parse_response<T: DeserializeOwned>(
        response: Response,
        type_name: &str,
    ) -> Result<T> {
        let body = Self::read_response(response).await?;
        Ok(serde_json::from_str(&body)
            .with_context(|| format!("Failed to deserialize response as {}", type_name))?)
    }
}