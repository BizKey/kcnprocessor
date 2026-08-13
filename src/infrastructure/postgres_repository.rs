use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use sqlx::PgPool;

use crate::api::db::*;
use crate::api::models::*;
use crate::core::traits::*;

pub struct PostgresRepository {
    pool: PgPool,
}

impl PostgresRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Repository for PostgresRepository {
    fn get_pool(&self) -> &PgPool {
        &self.pool
    }

    async fn save_error(&self, msg: &str) -> Result<()> {
        insert_db_error(&self.pool, msg).await
    }

    async fn save_event(&self, event: &serde_json::Value) -> Result<()> {
        insert_db_event(&self.pool, event).await
    }

    async fn save_order(&self, order: &OrderData) -> Result<()> {
        insert_db_orderevent(&self.pool, order.clone()).await
    }

    async fn save_balance(&self, balance: &BalanceData) -> Result<()> {
        insert_db_balance(&self.pool, balance.clone()).await
    }

    async fn save_order_message(&self, msg: &OrderMessage) -> Result<()> {
        let size_str = msg.size.map(|s| s.to_string());
        let funds_str = msg.funds.map(|s| s.to_string());
        let price_str = msg.price.map(|s| s.to_string());

        insert_db_msgsend(
            &self.pool,
            Some(&msg.symbol),
            Some(&msg.side),
            size_str.as_deref(),
            funds_str.as_deref(),
            price_str.as_deref(),
            Some(&msg.time_in_force),
            Some(&msg.order_type),
            Some(&msg.auto_borrow),
            Some(&msg.auto_repay),
            msg.client_oid.as_deref(),
            msg.order_id.as_deref(),
        )
        .await
    }

    async fn get_bot_by_client_oid(&self, client_oid: &str) -> Result<Option<Bot>> {
        get_bot_by_client_oid(&self.pool, client_oid).await
    }

    async fn get_all_bots(&self) -> Result<Vec<Bot>> {
        get_all_bots_for_trade(&self.pool).await
    }

    async fn get_random_symbol(&self) -> Result<Option<String>> {
        get_random_symbol(&self.pool).await
    }

    async fn get_symbol_info(&self, symbol: &str) -> Result<Option<Symbol>> {
        fetch_symbol_info_by_symbol(&self.pool, symbol).await
    }

    async fn get_currency_info(&self, currency: &str) -> Result<Option<Currency>> {
        let currencies = fetch_currency_info_by_symbol(&self.pool, currency).await?;
        Ok(currencies.map(|c| Currency {
            precision: c.precision,
        }))
    }

    async fn get_total_match_value(&self, client_oid: &str) -> Result<Option<Decimal>> {
        let value = get_total_match_value_by_client_oid(&self.pool, client_oid).await?;
        match value {
            Some(s) => {
                let decimal = Decimal::from_str(&s).map_err(|e| anyhow::anyhow!(e))?;
                Ok(Some(decimal))
            }
            None => Ok(None),
        }
    }

    async fn update_bot(&self, bot_update: BotUpdate) -> Result<()> {
        let entry_client_oid = bot_update.entry_client_oid.clone();
        let symbol = bot_update.symbol.clone();

        if let (Some(entry_client_oid_val), Some(symbol_val)) = (entry_client_oid.clone(), symbol) {
            update_bot_entry_client_oid_by_id(
                &self.pool,
                Some(&symbol_val),
                Some(&entry_client_oid_val),
                bot_update.bot_id,
            )
            .await?;
        }

        if let (Some(entry_client_oid_val), Some(exit_tp_client_oid)) =
            (entry_client_oid.clone(), bot_update.exit_tp_client_oid)
        {
            update_exit_tp_client_oid_bot_by_entry_client_oid(
                &self.pool,
                &entry_client_oid_val,
                &exit_tp_client_oid,
            )
            .await?;
        }

        if let (Some(entry_client_oid_val), Some(exit_sl_client_oid)) =
            (entry_client_oid, bot_update.exit_sl_client_oid)
        {
            update_exit_sl_client_oid_bot_by_entry_client_oid(
                &self.pool,
                &entry_client_oid_val,
                &exit_sl_client_oid,
            )
            .await?;
        }

        Ok(())
    }

    async fn update_position_ratio(&self, ratio: PositionRatio) -> Result<()> {
        upsert_position_ratio(
            &self.pool,
            ratio.debt_ratio,
            ratio.total_asset,
            &ratio.margin_coefficient_total_asset.to_string(),
            &ratio.total_debt.to_string(),
        )
        .await
    }

    async fn update_position_asset(&self, asset: PositionAsset) -> Result<()> {
        upsert_position_asset(
            &self.pool,
            &asset.symbol,
            &asset.total.to_string(),
            &asset.available.to_string(),
            &asset.hold.to_string(),
        )
        .await
    }

    async fn update_position_debt(&self, debt: PositionDebt) -> Result<()> {
        upsert_position_debt(&self.pool, &debt.symbol, &debt.value.to_string()).await
    }

    async fn clear_bots(&self, balance: &str) -> Result<()> {
        wipe_bots_info(&self.pool, balance).await
    }
}
