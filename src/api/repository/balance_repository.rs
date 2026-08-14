use crate::api::models::{BalanceData, BalanceRelationContext};
use crate::constants::EXCHANGE;
use anyhow::{Context, Result};
use sqlx::PgPool;

pub struct BalanceRepository {
    pool: PgPool,
}

impl BalanceRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn save_balance_event(&self, balance: BalanceData) -> Result<()> {
        let relation_context: &BalanceRelationContext = match &balance.relation_context {
            Some(ctx) => ctx,
            None => &BalanceRelationContext {
                symbol: None,
                order_id: None,
                trade_id: None,
            },
        };

        sqlx::query(
            r#"
            INSERT INTO balance (
                exchange, account_id, available, available_change, currency, 
                hold_value, hold_change, relation_event, relation_event_id, 
                event_time, total, symbol, order_id, trade_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14);
            "#,
        )
        .bind(EXCHANGE)
        .bind(&balance.account_id)
        .bind(&balance.available)
        .bind(&balance.available_change)
        .bind(&balance.currency)
        .bind(&balance.hold)
        .bind(&balance.hold_change)
        .bind(&balance.relation_event)
        .bind(&balance.relation_event_id)
        .bind(&balance.time)
        .bind(&balance.total)
        .bind(&relation_context.symbol)
        .bind(&relation_context.order_id)
        .bind(&relation_context.trade_id)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail insert into balance balance:{:?} relation_context:{:?} exchange:{}",
                balance, relation_context, EXCHANGE
            )
        })?;
        Ok(())
    }
}
