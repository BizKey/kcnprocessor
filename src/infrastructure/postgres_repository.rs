use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use sqlx::PgPool;

use crate::api::models::*;
use crate::api::repository::{
    BalanceRepository, BotRepository, ErrorRepository, EventRepository, MessageRepository,
    OrderRepository, PositionRepository, SymbolRepository,
};
use crate::core::traits::*;

pub struct PostgresRepository {
    pool: PgPool,
    bot_repo: BotRepository,
    order_repo: OrderRepository,
    balance_repo: BalanceRepository,
    position_repo: PositionRepository,
    symbol_repo: SymbolRepository,
    error_repo: ErrorRepository,
    event_repo: EventRepository,
    message_repo: MessageRepository,
}

impl PostgresRepository {
    pub fn new(pool: PgPool) -> Self {
        let bot_repo = BotRepository::new(pool.clone());
        let order_repo = OrderRepository::new(pool.clone());
        let balance_repo = BalanceRepository::new(pool.clone());
        let position_repo = PositionRepository::new(pool.clone());
        let symbol_repo = SymbolRepository::new(pool.clone());
        let error_repo = ErrorRepository::new(pool.clone());
        let event_repo = EventRepository::new(pool.clone());
        let message_repo = MessageRepository::new(pool.clone());

        Self {
            pool,
            bot_repo,
            order_repo,
            balance_repo,
            position_repo,
            symbol_repo,
            error_repo,
            event_repo,
            message_repo,
        }
    }
}

#[async_trait]
impl Repository for PostgresRepository {
    fn get_pool(&self) -> &PgPool {
        &self.pool
    }

    async fn save_error(&self, msg: &str) -> Result<()> {
        self.error_repo.save_error(msg).await
    }

    async fn save_event(&self, event: &serde_json::Value) -> Result<()> {
        self.event_repo.save_event(event).await
    }

    async fn save_order(&self, order: &OrderData) -> Result<()> {
        self.order_repo.save_order_event(order.clone()).await
    }

    async fn save_balance(&self, balance: &BalanceData) -> Result<()> {
        self.balance_repo.save_balance_event(balance.clone()).await
    }

    async fn save_order_message(&self, msg: &OrderMessage) -> Result<()> {
        let size_str = msg.size.map(|s| s.to_string());
        let funds_str = msg.funds.map(|s| s.to_string());
        let price_str = msg.price.map(|s| s.to_string());

        self.message_repo
            .save_order_message(
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
        self.bot_repo.get_by_client_oid(client_oid).await
    }

    async fn get_all_bots(&self) -> Result<Vec<Bot>> {
        self.bot_repo.get_all().await
    }

    async fn get_random_symbol(&self) -> Result<Option<String>> {
        self.symbol_repo.get_random_symbol().await
    }

    async fn get_symbol_info(&self, symbol: &str) -> Result<Option<Symbol>> {
        self.symbol_repo.get_symbol_info(symbol).await
    }

    async fn get_currency_info(&self, currency: &str) -> Result<Option<Currency>> {
        let currencies = self.symbol_repo.get_currency_info(currency).await?;
        Ok(currencies.map(|c| Currency {
            precision: c.precision,
        }))
    }

    async fn get_total_match_value(&self, client_oid: &str) -> Result<Option<Decimal>> {
        let value = self
            .order_repo
            .get_total_match_value_by_client_oid(client_oid)
            .await?;
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
            self.bot_repo
                .update_entry_client_oid_by_id(
                    Some(&symbol_val),
                    Some(&entry_client_oid_val),
                    bot_update.bot_id,
                )
                .await?;
        }

        if let (Some(entry_client_oid_val), Some(exit_tp_client_oid)) =
            (entry_client_oid.clone(), bot_update.exit_tp_client_oid)
        {
            self.bot_repo
                .update_exit_tp_client_oid_by_entry_client_oid(
                    &entry_client_oid_val,
                    &exit_tp_client_oid,
                )
                .await?;
        }

        if let (Some(entry_client_oid_val), Some(exit_sl_client_oid)) =
            (entry_client_oid, bot_update.exit_sl_client_oid)
        {
            self.bot_repo
                .update_exit_sl_client_oid_by_entry_client_oid(
                    &entry_client_oid_val,
                    &exit_sl_client_oid,
                )
                .await?;
        }

        Ok(())
    }

    async fn update_position_ratio(&self, ratio: PositionRatio) -> Result<()> {
        self.position_repo
            .upsert_position_ratio(
                ratio.debt_ratio,
                ratio.total_asset,
                &ratio.margin_coefficient_total_asset.to_string(),
                &ratio.total_debt.to_string(),
            )
            .await
    }

    async fn update_position_asset(&self, asset: PositionAsset) -> Result<()> {
        self.position_repo
            .upsert_position_asset(
                &asset.symbol,
                &asset.total.to_string(),
                &asset.available.to_string(),
                &asset.hold.to_string(),
            )
            .await
    }

    async fn update_position_debt(&self, debt: PositionDebt) -> Result<()> {
        self.position_repo
            .upsert_position_debt(&debt.symbol, &debt.value.to_string())
            .await
    }

    async fn clear_bots(&self, balance: &str) -> Result<()> {
        self.bot_repo.clear_all_bots(balance).await
    }
}
