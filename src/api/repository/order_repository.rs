use crate::api::models::OrderData;
use crate::constants::EXCHANGE;
use anyhow::{Context, Result};
use sqlx::{PgPool, Row};

#[derive(Clone)]
pub struct OrderRepository {
    pool: PgPool,
}

impl OrderRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save_order_event(&self, order: &OrderData) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO orderevent (
                exchange, status, type_, symbol, side, order_type, fee_type, 
                liquidity, price, order_id, client_oid, trade_id, origin_size, 
                size, filled_size, match_size, match_price, canceled_size, 
                old_size, remain_size, remain_funds, order_time, ts
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 
                $14, $15, $16, $17, $18, $19, $20, $21, $22, $23
            );
            "#,
        )
        .bind(EXCHANGE)
        .bind(&order.status)
        .bind(order.type_.as_str())
        .bind(&order.symbol)
        .bind(order.side.as_str())
        .bind(order.order_type.as_str())
        .bind(&order.fee_type)
        .bind(&order.liquidity)
        .bind(&order.price)
        .bind(&order.order_id)
        .bind(&order.client_oid)
        .bind(&order.trade_id)
        .bind(&order.origin_size)
        .bind(&order.size)
        .bind(&order.filled_size)
        .bind(&order.match_size)
        .bind(&order.match_price)
        .bind(&order.canceled_size)
        .bind(&order.old_size)
        .bind(&order.remain_size)
        .bind(&order.remain_funds)
        .bind(order.order_time)
        .bind(order.ts)
        .execute(&self.pool)
        .await
        .with_context(|| format!("Fail insert into orderevent order:{:?}", order))?;
        Ok(())
    }

    pub async fn get_total_match_value_by_client_oid(
        &self,
        client_oid: &str,
    ) -> Result<Option<String>> {
        let row = sqlx::query(
            r#"
            SELECT SUM(match_size::numeric * match_price::numeric)::text AS total_match_value
            FROM orderevent
            WHERE client_oid = $1 AND exchange = $2 
              AND match_size IS NOT NULL AND match_price IS NOT NULL;
            "#,
        )
        .bind(client_oid)
        .bind(EXCHANGE)
        .fetch_one(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail get total match value by client_oid:{} exchange:{}",
                client_oid, EXCHANGE
            )
        })?;

        Ok(row.try_get::<Option<String>, _>("total_match_value")?)
    }
}
