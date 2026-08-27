use crate::api::models::StopOrderData;
use crate::constants::EXCHANGE;
use anyhow::{Context, Result};
use sqlx::PgPool;

#[derive(Clone)]
pub struct StopOrdersRepository {
    pool: PgPool,
}

impl StopOrdersRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save_stop_order(&self, stop_order: &StopOrderData) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO stoporders (
                exchange, client_oid, side, symbol, order_type, stop_type,
                stop_price, size, funds, time_in_force, auto_borrow, auto_repay,
                is_isolated
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);
            "#,
        )
        .bind(EXCHANGE)
        .bind(&stop_order.client_oid)
        .bind(stop_order.side.as_str())
        .bind(&stop_order.symbol)
        .bind(stop_order.order_type.as_str())
        .bind(stop_order.stop.as_str())
        .bind(&stop_order.stop_price)
        .bind(&stop_order.size)
        .bind(&stop_order.funds)
        .bind(&stop_order.time_in_force)
        .bind(stop_order.auto_borrow)
        .bind(stop_order.auto_repay)
        .bind(stop_order.is_isolated)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail insert into stoporders client_oid:{} exchange:{}",
                stop_order.client_oid, EXCHANGE,
            )
        })?;
        Ok(())
    }
}
