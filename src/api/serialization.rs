use anyhow::{Context, Result};
use serde_json;

/// Сериализатор для тела запроса
pub struct BodySerializer;

impl BodySerializer {
    /// Сериализует тело запроса в JSON
    pub fn serialize(body: Option<serde_json::Value>) -> Result<String> {
        let body = match body {
            Some(body) => body,
            None => return Ok(String::new()),
        };

        Ok(serde_json::to_string(&body)
            .with_context(|| format!("Failed to serialize body '{}'", body))?)
    }
}