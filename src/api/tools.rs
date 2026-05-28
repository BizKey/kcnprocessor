use std::env;

pub fn get_env(key: String) -> Result<String, env::VarError> {
    match env::var(key) {
        Ok(value) => Ok(value.trim().to_string()),
        Err(e) => Err(e),
    }
}
