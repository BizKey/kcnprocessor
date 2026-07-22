use std::env;
use tracing::error;

pub fn get_env(key: &str) -> Result<String, String> {
    env::var(key).map(|v| v.trim().to_string()).map_err(|e| {
        let msg: String = format!("ENV variable not found: {key} — {e}");
        error!("{msg}");
        msg
    })
}
