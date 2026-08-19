use crate::constants::EXCHANGE;
use anyhow::{Context, Result};
use sqlx::PgPool;

#[derive(Clone)]
pub struct EventRepository {
    pool: PgPool,
}

impl EventRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save_event(&self, msg: &serde_json::Value) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO events (exchange, msg) VALUES ($1, $2);
            "#,
        )
        .bind(EXCHANGE)
        .bind(msg)
        .execute(&self.pool)
        .await
        .with_context(|| format!("Fail insert into events msg:{} exchange:{}", msg, EXCHANGE))?;
        Ok(())
    }
}
