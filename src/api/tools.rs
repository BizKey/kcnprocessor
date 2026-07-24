use anyhow::Result;
use std::env;

pub fn get_env(key: &str) -> Result<String> {
    Ok(env::var(key)?.trim().to_string())
}
