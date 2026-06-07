use std::env;

pub fn get_env(key: &str) -> Result<String, String> {
    match env::var(key) {
        Ok(value) => Ok(value.trim().to_string()),
        Err(e) => Err(format!("Don't find ENV:{} {}", key, e)),
    }
}
