use crate::constants::EXCHANGE;
use anyhow::{Context, Result};
use sqlx::PgPool;

#[derive(Clone)]
pub struct SendOrdersRepository {
    pool: PgPool,
}

impl SendOrdersRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save_send_orders(
        &self,
        symbol: Option<&str>,
        side: Option<&str>,
        size: Option<&str>,
        funds: Option<&str>,
        price: Option<&str>,
        time_in_force: Option<&str>,
        order_type: Option<&str>,
        auto_borrow: Option<&bool>,
        auto_repay: Option<&bool>,
        client_oid: Option<&str>,
        order_id: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO sendorders (
                exchange, symbol, side, size, funds, price, time_in_force, 
                order_type, auto_borrow, auto_repay, client_oid, order_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12);
            "#,
        )
        .bind(EXCHANGE)
        .bind(symbol)
        .bind(side)
        .bind(size)
        .bind(funds)
        .bind(price)
        .bind(time_in_force)
        .bind(order_type)
        .bind(auto_borrow)
        .bind(auto_repay)
        .bind(client_oid)
        .bind(order_id)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail insert into sendorders client_oid:{:?} exchange:{}",
                client_oid, EXCHANGE,
            )
        })?;
        Ok(())
    }
}
