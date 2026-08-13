use crate::constants::EXCHANGE;
use anyhow::{Context, Result};
use sqlx::PgPool;

pub struct ErrorRepository {
    pool: PgPool,
}

impl ErrorRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save_error(&self, msg: &str) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO errors (exchange, msg) VALUES ($1, $2);
            "#,
        )
        .bind(EXCHANGE)
        .bind(msg)
        .execute(&self.pool)
        .await
        .with_context(|| format!("Fail insert into errors msg:{}", msg))?;
        Ok(())
    }
}
