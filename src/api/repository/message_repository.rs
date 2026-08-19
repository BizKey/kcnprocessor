use crate::constants::EXCHANGE;
use anyhow::{Context, Result};
use sqlx::PgPool;

#[derive(Clone)]
pub struct MessageRepository {
    pool: PgPool,
}

impl MessageRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save_order_message(
        &self,
        args_symbol: Option<&str>,
        args_side: Option<&str>,
        args_size: Option<&str>,
        args_funds: Option<&str>,
        args_price: Option<&str>,
        args_time_in_force: Option<&str>,
        args_type: Option<&str>,
        args_auto_borrow: Option<&bool>,
        args_auto_repay: Option<&bool>,
        args_client_oid: Option<&str>,
        args_order_id: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO msgsend (
                exchange, args_symbol, args_side, args_size, args_funds, 
                args_price, args_time_in_force, args_type, args_auto_borrow, 
                args_auto_repay, args_client_oid, args_order_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12);
            "#,
        )
        .bind(EXCHANGE)
        .bind(args_symbol)
        .bind(args_side)
        .bind(args_size)
        .bind(args_funds)
        .bind(args_price)
        .bind(args_time_in_force)
        .bind(args_type)
        .bind(args_auto_borrow)
        .bind(args_auto_repay)
        .bind(args_client_oid)
        .bind(args_order_id)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail insert into msgsend args_client_oid:{:?} exchange:{}",
                args_client_oid, EXCHANGE,
            )
        })?;
        Ok(())
    }
}
