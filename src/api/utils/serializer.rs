use anyhow::{Context, Result};
use serde_json;

pub struct BodySerializer;

impl BodySerializer {
    pub fn serialize(body: Option<serde_json::Value>) -> Result<String> {
        let body = match body {
            Some(body) => body,
            None => return Ok(String::new()),
        };

        serde_json::to_string(&body).with_context(|| format!("Failed to serialize body '{}'", body))
    }
}
