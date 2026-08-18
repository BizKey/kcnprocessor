use anyhow::{Context, Result};
use reqwest::Method;
use std::time::{SystemTime, UNIX_EPOCH};

use super::signature::SignatureGenerator;

pub struct AuthCredentials {
    pub api_key: String,
    pub api_secret: String,
    pub api_passphrase: String,
}

impl AuthCredentials {
    pub fn new(api_key: String, api_secret: String, api_passphrase: String) -> Self {
        Self {
            api_key,
            api_secret,
            api_passphrase,
        }
    }

    pub fn get_timestamp_ms() -> Result<u64> {
        Ok(SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("Get error get UNIX_EPOCH")?
            .as_millis() as u64)
    }

    pub fn build_signature_string(
        timestamp: u64,
        method: &Method,
        endpoint: &str,
        query_string: &str,
        body_str: &str,
    ) -> String {
        let mut str_to_sign = format!(
            "{}{}{}",
            timestamp,
            method.as_ref().to_uppercase(),
            endpoint
        );

        if !query_string.is_empty() {
            str_to_sign.push('?');
            str_to_sign.push_str(query_string);
        }
        if !body_str.is_empty() {
            str_to_sign.push_str(body_str);
        }

        str_to_sign
    }

    pub fn build_auth_headers(
        &self,
        method: &Method,
        endpoint: &str,
        query_string: &str,
        body_str: &str,
    ) -> Result<Vec<(String, String)>> {
        let timestamp = Self::get_timestamp_ms()?;
        let signature_gen =
            SignatureGenerator::new(self.api_secret.clone(), self.api_passphrase.clone());

        let signature_string =
            Self::build_signature_string(timestamp, method, endpoint, query_string, body_str);

        let signature = signature_gen.generate_signature(signature_string.as_bytes())?;
        let passphrase_signature = signature_gen.generate_passphrase_signature()?;

        Ok(vec![
            ("KC-API-KEY".to_string(), self.api_key.clone()),
            ("KC-API-SIGN".to_string(), signature),
            ("KC-API-TIMESTAMP".to_string(), timestamp.to_string()),
            ("KC-API-PASSPHRASE".to_string(), passphrase_signature),
            ("KC-API-KEY-VERSION".to_string(), "2".to_string()),
        ])
    }
}
