use anyhow::{Context, Result};
use base64::Engine;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Генератор подписей для KuCoin API
pub struct SignatureGenerator {
    api_secret: String,
    api_passphrase: String,
}

impl SignatureGenerator {
    pub fn new(api_secret: String, api_passphrase: String) -> Self {
        Self {
            api_secret,
            api_passphrase,
        }
    }

    /// Генерирует подпись для запроса
    pub fn generate_signature(&self, to_sign: &[u8]) -> Result<String> {
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes())
            .context("Fail HmacSha256")?;
        mac.update(to_sign);
        Ok(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
    }

    /// Генерирует подпись для passphrase
    pub fn generate_passphrase_signature(&self) -> Result<String> {
        self.generate_signature(self.api_passphrase.as_bytes())
    }
}