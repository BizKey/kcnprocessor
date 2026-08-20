use crate::api::models::{BalanceData, Bot, Currencies, OrderData, Symbol};
use crate::api::repository::{
    BalanceRepository, BotRepository, ErrorRepository, EventRepository, MessageRepository,
    OrderRepository, PositionRepository, SymbolRepository,
};
use crate::core::repository_traits::{
    BalanceCommand, BotEntryUpdate, BotManagement, BotQuery, BotSlUpdate, BotTpUpdate,
    ErrorCommand, EventCommand, MessageCommand, OrderCommand, OrderQuery, PositionCommand,
    SymbolQuery,
};
use anyhow::Result;
use async_trait::async_trait;
use sqlx::PgPool;

// ============ BOT QUERY ============

#[derive(Clone)]
pub struct PostgresBotRepository {
    bot_repo: BotRepository,
}

impl PostgresBotRepository {
    pub fn new(pool: PgPool) -> Self {
        Self {
            bot_repo: BotRepository::new(pool),
        }
    }
}

#[async_trait]
impl BotQuery for PostgresBotRepository {
    async fn get_by_client_oid(&self, client_oid: &str) -> Result<Option<Bot>> {
        self.bot_repo.get_by_client_oid(client_oid).await
    }

    async fn get_by_entry_client_oid(&self, entry_client_oid: &str) -> Result<Option<Bot>> {
        self.bot_repo
            .get_by_entry_client_oid(entry_client_oid)
            .await
    }

    async fn get_by_exit_tp_client_oid(&self, exit_tp_client_oid: &str) -> Result<Option<Bot>> {
        self.bot_repo
            .get_by_exit_tp_client_oid(exit_tp_client_oid)
            .await
    }

    async fn get_by_exit_sl_client_oid(&self, exit_sl_client_oid: &str) -> Result<Option<Bot>> {
        self.bot_repo
            .get_by_exit_sl_client_oid(exit_sl_client_oid)
            .await
    }

    async fn get_all(&self) -> Result<Vec<Bot>> {
        self.bot_repo.get_all().await
    }
}

#[async_trait]
impl BotEntryUpdate for PostgresBotRepository {
    async fn update_entry_client_oid_by_id(
        &self,
        entry_client_oid: Option<&str>,
        id: i32,
    ) -> Result<()> {
        self.bot_repo
            .update_entry_client_oid_by_id(entry_client_oid, id)
            .await
    }

    async fn clear_entry_client_oid(&self, entry_client_oid: &str) -> Result<()> {
        self.bot_repo.clear_entry_client_oid(entry_client_oid).await
    }

    async fn update_balance_by_entry_client_oid(
        &self,
        entry_client_oid: &str,
        balance: &str,
    ) -> Result<()> {
        self.bot_repo
            .update_balance_by_entry_client_oid(entry_client_oid, balance)
            .await
    }
}

#[async_trait]
impl BotTpUpdate for PostgresBotRepository {
    async fn update_exit_tp_client_oid_by_entry_client_oid(
        &self,
        entry_client_oid: &str,
        symbol: &str,
        exit_tp_client_oid: &str,
        tp_stop_price: &str,
    ) -> Result<()> {
        self.bot_repo
            .update_exit_tp_client_oid_by_entry_client_oid(
                entry_client_oid,
                symbol,
                exit_tp_client_oid,
                tp_stop_price,
            )
            .await
    }

    async fn update_exit_tp_order_id_by_client_oid(
        &self,
        exit_tp_order_id: &str,
        exit_tp_client_oid: &str,
    ) -> Result<()> {
        self.bot_repo
            .update_exit_tp_order_id_by_client_oid(exit_tp_order_id, exit_tp_client_oid)
            .await
    }

    async fn update_exit_tp_client_oid_by_order_id(
        &self,
        exit_tp_order_id: &str,
        exit_tp_client_oid: &str,
    ) -> Result<()> {
        self.bot_repo
            .update_exit_tp_client_oid_by_order_id(exit_tp_order_id, exit_tp_client_oid)
            .await
    }

    async fn clear_exit_tp_by_client_oid(&self, exit_tp_client_oid: &str) -> Result<()> {
        self.bot_repo
            .clear_exit_tp_by_client_oid(exit_tp_client_oid)
            .await
    }

    async fn update_balance_and_clear_symbol_by_exit_tp(
        &self,
        exit_tp_client_oid: &str,
        balance: &str,
    ) -> Result<()> {
        self.bot_repo
            .update_balance_and_clear_symbol_by_exit_tp(exit_tp_client_oid, balance)
            .await
    }
}

#[async_trait]
impl BotSlUpdate for PostgresBotRepository {
    async fn update_exit_sl_client_oid_by_entry_client_oid(
        &self,
        entry_client_oid: &str,
        symbol: &str,
        exit_sl_client_oid: &str,
        sl_stop_price: &str,
    ) -> Result<()> {
        self.bot_repo
            .update_exit_sl_client_oid_by_entry_client_oid(
                entry_client_oid,
                symbol,
                exit_sl_client_oid,
                sl_stop_price,
            )
            .await
    }

    async fn update_exit_sl_order_id_by_client_oid(
        &self,
        exit_sl_order_id: &str,
        exit_sl_client_oid: &str,
    ) -> Result<()> {
        self.bot_repo
            .update_exit_sl_order_id_by_client_oid(exit_sl_order_id, exit_sl_client_oid)
            .await
    }

    async fn update_exit_sl_client_oid_by_order_id(
        &self,
        exit_sl_order_id: &str,
        exit_sl_client_oid: &str,
    ) -> Result<()> {
        self.bot_repo
            .update_exit_sl_client_oid_by_order_id(exit_sl_order_id, exit_sl_client_oid)
            .await
    }

    async fn clear_exit_sl_by_client_oid(&self, exit_sl_client_oid: &str) -> Result<()> {
        self.bot_repo
            .clear_exit_sl_by_client_oid(exit_sl_client_oid)
            .await
    }

    async fn update_balance_and_clear_symbol_by_exit_sl(
        &self,
        exit_sl_client_oid: &str,
        balance: &str,
    ) -> Result<()> {
        self.bot_repo
            .update_balance_and_clear_symbol_by_exit_sl(exit_sl_client_oid, balance)
            .await
    }

    async fn clear_symbol_by_exit_sl_client_oid(&self, exit_sl_client_oid: &str) -> Result<()> {
        self.bot_repo
            .clear_symbol_by_exit_sl_client_oid(exit_sl_client_oid)
            .await
    }
}

#[async_trait]
impl BotManagement for PostgresBotRepository {
    async fn clear_all_bots(&self, balance: &str) -> Result<()> {
        self.bot_repo.clear_all_bots(balance).await
    }
}

// ============ ORDER ============

#[derive(Clone)]
pub struct PostgresOrderRepository {
    order_repo: OrderRepository,
}

impl PostgresOrderRepository {
    pub fn new(pool: PgPool) -> Self {
        Self {
            order_repo: OrderRepository::new(pool),
        }
    }
}

#[async_trait]
impl OrderQuery for PostgresOrderRepository {
    async fn get_total_match_value_by_client_oid(
        &self,
        client_oid: &str,
    ) -> Result<Option<String>> {
        self.order_repo
            .get_total_match_value_by_client_oid(client_oid)
            .await
    }
}

#[async_trait]
impl OrderCommand for PostgresOrderRepository {
    async fn save_order_event(&self, order: &OrderData) -> Result<()> {
        self.order_repo.save_order_event(order).await
    }
}

// ============ BALANCE ============

#[derive(Clone)]
pub struct PostgresBalanceRepository {
    balance_repo: BalanceRepository,
}

impl PostgresBalanceRepository {
    pub fn new(pool: PgPool) -> Self {
        Self {
            balance_repo: BalanceRepository::new(pool),
        }
    }
}

#[async_trait]
impl BalanceCommand for PostgresBalanceRepository {
    async fn save_balance_event(&self, balance: BalanceData) -> Result<()> {
        self.balance_repo.save_balance_event(balance).await
    }
}

// ============ POSITION ============

#[derive(Clone)]
pub struct PostgresPositionRepository {
    position_repo: PositionRepository,
}

impl PostgresPositionRepository {
    pub fn new(pool: PgPool) -> Self {
        Self {
            position_repo: PositionRepository::new(pool),
        }
    }
}

#[async_trait]
impl PositionCommand for PostgresPositionRepository {
    async fn upsert_position_ratio(
        &self,
        debt_ratio: f64,
        total_asset: f64,
        margin_coefficient_total_asset: &str,
        total_debt: &str,
    ) -> Result<()> {
        self.position_repo
            .upsert_position_ratio(
                debt_ratio,
                total_asset,
                margin_coefficient_total_asset,
                total_debt,
            )
            .await
    }

    async fn upsert_position_debt(&self, debt_symbol: &str, debt_value: &str) -> Result<()> {
        self.position_repo
            .upsert_position_debt(debt_symbol, debt_value)
            .await
    }

    async fn upsert_position_asset(
        &self,
        asset_symbol: &str,
        asset_total: &str,
        asset_available: &str,
        asset_hold: &str,
    ) -> Result<()> {
        self.position_repo
            .upsert_position_asset(asset_symbol, asset_total, asset_available, asset_hold)
            .await
    }
}

// ============ SYMBOL ============

#[derive(Clone)]
pub struct PostgresSymbolRepository {
    symbol_repo: SymbolRepository,
}

impl PostgresSymbolRepository {
    pub fn new(pool: PgPool) -> Self {
        Self {
            symbol_repo: SymbolRepository::new(pool),
        }
    }
}

#[async_trait]
impl SymbolQuery for PostgresSymbolRepository {
    async fn get_random_symbol(&self) -> Result<Option<String>> {
        self.symbol_repo.get_random_symbol().await
    }

    async fn get_symbol_info(&self, symbol: &str) -> Result<Option<Symbol>> {
        self.symbol_repo.get_symbol_info(symbol).await
    }

    async fn get_currency_info(&self, currency: &str) -> Result<Option<Currencies>> {
        self.symbol_repo.get_currency_info(currency).await
    }
}

// ============ ERROR ============

#[derive(Clone)]
pub struct PostgresErrorRepository {
    error_repo: ErrorRepository,
}

impl PostgresErrorRepository {
    pub fn new(pool: PgPool) -> Self {
        Self {
            error_repo: ErrorRepository::new(pool),
        }
    }
}

#[async_trait]
impl ErrorCommand for PostgresErrorRepository {
    async fn save_error(&self, msg: &str) -> Result<()> {
        self.error_repo.save_error(msg).await
    }
}

// ============ EVENT ============

#[derive(Clone)]
pub struct PostgresEventRepository {
    event_repo: EventRepository,
}

impl PostgresEventRepository {
    pub fn new(pool: PgPool) -> Self {
        Self {
            event_repo: EventRepository::new(pool),
        }
    }
}

#[async_trait]
impl EventCommand for PostgresEventRepository {
    async fn save_event(&self, event: &serde_json::Value) -> Result<()> {
        self.event_repo.save_event(event).await
    }
}

// ============ MESSAGE ============

#[derive(Clone)]
pub struct PostgresMessageRepository {
    message_repo: MessageRepository,
}

impl PostgresMessageRepository {
    pub fn new(pool: PgPool) -> Self {
        Self {
            message_repo: MessageRepository::new(pool),
        }
    }
}

#[async_trait]
impl MessageCommand for PostgresMessageRepository {
    async fn save_order_message(
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
        self.message_repo
            .save_order_message(
                args_symbol,
                args_side,
                args_size,
                args_funds,
                args_price,
                args_time_in_force,
                args_type,
                args_auto_borrow,
                args_auto_repay,
                args_client_oid,
                args_order_id,
            )
            .await
    }
}

// ============ КОМПОЗИТНЫЙ РЕПОЗИТОРИЙ ============

#[derive(Clone)]
pub struct PostgresRepository {
    pub bot: PostgresBotRepository,
    pub order: PostgresOrderRepository,
    pub balance: PostgresBalanceRepository,
    pub position: PostgresPositionRepository,
    pub symbol: PostgresSymbolRepository,
    pub error: PostgresErrorRepository,
    pub event: PostgresEventRepository,
    pub message: PostgresMessageRepository,
}

impl PostgresRepository {
    pub fn new(pool: PgPool) -> Self {
        Self {
            bot: PostgresBotRepository::new(pool.clone()),
            order: PostgresOrderRepository::new(pool.clone()),
            balance: PostgresBalanceRepository::new(pool.clone()),
            position: PostgresPositionRepository::new(pool.clone()),
            symbol: PostgresSymbolRepository::new(pool.clone()),
            error: PostgresErrorRepository::new(pool.clone()),
            event: PostgresEventRepository::new(pool.clone()),
            message: PostgresMessageRepository::new(pool.clone()),
        }
    }
}

// ============ РЕАЛИЗАЦИЯ ВСЕХ ТРЕЙТОВ ДЛЯ POSTGRESREPOSITORY ============

#[async_trait]
impl BotQuery for PostgresRepository {
    async fn get_by_client_oid(&self, client_oid: &str) -> Result<Option<Bot>> {
        self.bot.get_by_client_oid(client_oid).await
    }

    async fn get_by_entry_client_oid(&self, entry_client_oid: &str) -> Result<Option<Bot>> {
        self.bot.get_by_entry_client_oid(entry_client_oid).await
    }

    async fn get_by_exit_tp_client_oid(&self, exit_tp_client_oid: &str) -> Result<Option<Bot>> {
        self.bot.get_by_exit_tp_client_oid(exit_tp_client_oid).await
    }

    async fn get_by_exit_sl_client_oid(&self, exit_sl_client_oid: &str) -> Result<Option<Bot>> {
        self.bot.get_by_exit_sl_client_oid(exit_sl_client_oid).await
    }

    async fn get_all(&self) -> Result<Vec<Bot>> {
        self.bot.get_all().await
    }
}

#[async_trait]
impl BotEntryUpdate for PostgresRepository {
    async fn update_entry_client_oid_by_id(
        &self,
        entry_client_oid: Option<&str>,
        id: i32,
    ) -> Result<()> {
        self.bot
            .update_entry_client_oid_by_id(entry_client_oid, id)
            .await
    }

    async fn clear_entry_client_oid(&self, entry_client_oid: &str) -> Result<()> {
        self.bot.clear_entry_client_oid(entry_client_oid).await
    }

    async fn update_balance_by_entry_client_oid(
        &self,
        entry_client_oid: &str,
        balance: &str,
    ) -> Result<()> {
        self.bot
            .update_balance_by_entry_client_oid(entry_client_oid, balance)
            .await
    }
}

#[async_trait]
impl BotTpUpdate for PostgresRepository {
    async fn update_exit_tp_client_oid_by_entry_client_oid(
        &self,
        entry_client_oid: &str,
        symbol: &str,
        exit_tp_client_oid: &str,
        tp_stop_price: &str,
    ) -> Result<()> {
        self.bot
            .update_exit_tp_client_oid_by_entry_client_oid(
                entry_client_oid,
                symbol,
                exit_tp_client_oid,
                tp_stop_price,
            )
            .await
    }

    async fn update_exit_tp_order_id_by_client_oid(
        &self,
        exit_tp_order_id: &str,
        exit_tp_client_oid: &str,
    ) -> Result<()> {
        self.bot
            .update_exit_tp_order_id_by_client_oid(exit_tp_order_id, exit_tp_client_oid)
            .await
    }

    async fn update_exit_tp_client_oid_by_order_id(
        &self,
        exit_tp_order_id: &str,
        exit_tp_client_oid: &str,
    ) -> Result<()> {
        self.bot
            .update_exit_tp_client_oid_by_order_id(exit_tp_order_id, exit_tp_client_oid)
            .await
    }

    async fn clear_exit_tp_by_client_oid(&self, exit_tp_client_oid: &str) -> Result<()> {
        self.bot
            .clear_exit_tp_by_client_oid(exit_tp_client_oid)
            .await
    }

    async fn update_balance_and_clear_symbol_by_exit_tp(
        &self,
        exit_tp_client_oid: &str,
        balance: &str,
    ) -> Result<()> {
        self.bot
            .update_balance_and_clear_symbol_by_exit_tp(exit_tp_client_oid, balance)
            .await
    }
}

#[async_trait]
impl BotSlUpdate for PostgresRepository {
    async fn update_exit_sl_client_oid_by_entry_client_oid(
        &self,
        entry_client_oid: &str,
        symbol: &str,
        exit_sl_client_oid: &str,
        sl_stop_price: &str,
    ) -> Result<()> {
        self.bot
            .update_exit_sl_client_oid_by_entry_client_oid(
                entry_client_oid,
                symbol,
                exit_sl_client_oid,
                sl_stop_price,
            )
            .await
    }

    async fn update_exit_sl_order_id_by_client_oid(
        &self,
        exit_sl_order_id: &str,
        exit_sl_client_oid: &str,
    ) -> Result<()> {
        self.bot
            .update_exit_sl_order_id_by_client_oid(exit_sl_order_id, exit_sl_client_oid)
            .await
    }

    async fn update_exit_sl_client_oid_by_order_id(
        &self,
        exit_sl_order_id: &str,
        exit_sl_client_oid: &str,
    ) -> Result<()> {
        self.bot
            .update_exit_sl_client_oid_by_order_id(exit_sl_order_id, exit_sl_client_oid)
            .await
    }

    async fn clear_exit_sl_by_client_oid(&self, exit_sl_client_oid: &str) -> Result<()> {
        self.bot
            .clear_exit_sl_by_client_oid(exit_sl_client_oid)
            .await
    }

    async fn update_balance_and_clear_symbol_by_exit_sl(
        &self,
        exit_sl_client_oid: &str,
        balance: &str,
    ) -> Result<()> {
        self.bot
            .update_balance_and_clear_symbol_by_exit_sl(exit_sl_client_oid, balance)
            .await
    }

    async fn clear_symbol_by_exit_sl_client_oid(&self, exit_sl_client_oid: &str) -> Result<()> {
        self.bot
            .clear_symbol_by_exit_sl_client_oid(exit_sl_client_oid)
            .await
    }
}

#[async_trait]
impl BotManagement for PostgresRepository {
    async fn clear_all_bots(&self, balance: &str) -> Result<()> {
        self.bot.clear_all_bots(balance).await
    }
}

#[async_trait]
impl OrderQuery for PostgresRepository {
    async fn get_total_match_value_by_client_oid(
        &self,
        client_oid: &str,
    ) -> Result<Option<String>> {
        self.order
            .get_total_match_value_by_client_oid(client_oid)
            .await
    }
}

#[async_trait]
impl OrderCommand for PostgresRepository {
    async fn save_order_event(&self, order: &OrderData) -> Result<()> {
        self.order.save_order_event(order).await
    }
}

#[async_trait]
impl BalanceCommand for PostgresRepository {
    async fn save_balance_event(&self, balance: BalanceData) -> Result<()> {
        self.balance.save_balance_event(balance).await
    }
}

#[async_trait]
impl PositionCommand for PostgresRepository {
    async fn upsert_position_ratio(
        &self,
        debt_ratio: f64,
        total_asset: f64,
        margin_coefficient_total_asset: &str,
        total_debt: &str,
    ) -> Result<()> {
        self.position
            .upsert_position_ratio(
                debt_ratio,
                total_asset,
                margin_coefficient_total_asset,
                total_debt,
            )
            .await
    }

    async fn upsert_position_debt(&self, debt_symbol: &str, debt_value: &str) -> Result<()> {
        self.position
            .upsert_position_debt(debt_symbol, debt_value)
            .await
    }

    async fn upsert_position_asset(
        &self,
        asset_symbol: &str,
        asset_total: &str,
        asset_available: &str,
        asset_hold: &str,
    ) -> Result<()> {
        self.position
            .upsert_position_asset(asset_symbol, asset_total, asset_available, asset_hold)
            .await
    }
}

#[async_trait]
impl SymbolQuery for PostgresRepository {
    async fn get_random_symbol(&self) -> Result<Option<String>> {
        self.symbol.get_random_symbol().await
    }

    async fn get_symbol_info(&self, symbol: &str) -> Result<Option<Symbol>> {
        self.symbol.get_symbol_info(symbol).await
    }

    async fn get_currency_info(&self, currency: &str) -> Result<Option<Currencies>> {
        self.symbol.get_currency_info(currency).await
    }
}

#[async_trait]
impl ErrorCommand for PostgresRepository {
    async fn save_error(&self, msg: &str) -> Result<()> {
        self.error.save_error(msg).await
    }
}

#[async_trait]
impl EventCommand for PostgresRepository {
    async fn save_event(&self, event: &serde_json::Value) -> Result<()> {
        self.event.save_event(event).await
    }
}

#[async_trait]
impl MessageCommand for PostgresRepository {
    async fn save_order_message(
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
        self.message
            .save_order_message(
                args_symbol,
                args_side,
                args_size,
                args_funds,
                args_price,
                args_time_in_force,
                args_type,
                args_auto_borrow,
                args_auto_repay,
                args_client_oid,
                args_order_id,
            )
            .await
    }
}
