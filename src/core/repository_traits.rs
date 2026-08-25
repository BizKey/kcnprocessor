use crate::api::models::{BalanceData, Bot, Currencies, OrderData, Symbol};
use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait BotQuery: Send + Sync {
    async fn get_by_client_oid(&self, client_oid: &str) -> Result<Option<Bot>>;
    async fn get_by_entry_client_oid(&self, entry_client_oid: &str) -> Result<Option<Bot>>;
    async fn get_by_exit_tp_client_oid(&self, exit_tp_client_oid: &str) -> Result<Option<Bot>>;
    async fn get_by_exit_sl_client_oid(&self, exit_sl_client_oid: &str) -> Result<Option<Bot>>;
    async fn get_all(&self) -> Result<Vec<Bot>>;
}

#[async_trait]
pub trait BotEntryUpdate: Send + Sync {
    async fn update_entry_client_oid_by_id(
        &self,
        entry_client_oid: Option<&str>,
        id: i32,
    ) -> Result<()>;

    async fn clear_entry_client_oid(&self, entry_client_oid: &str) -> Result<()>;

    async fn update_balance_by_entry_client_oid(
        &self,
        entry_client_oid: &str,
        balance: &str,
    ) -> Result<()>;

    async fn update_entry_price_by_client_oid(
        &self,
        entry_client_oid: &str,
        entry_price: &str,
    ) -> Result<()>;
}

#[async_trait]
pub trait BotTpUpdate: Send + Sync {
    async fn update_exit_tp_client_oid_by_entry_client_oid(
        &self,
        entry_client_oid: &str,

        exit_tp_client_oid: &str,
        tp_stop_price: &str,
    ) -> Result<()>;

    async fn update_exit_tp_order_id_by_client_oid(
        &self,
        exit_tp_order_id: &str,
        exit_tp_client_oid: &str,
    ) -> Result<()>;

    async fn update_exit_tp_client_oid_by_order_id(
        &self,
        exit_tp_order_id: &str,
        exit_tp_client_oid: &str,
    ) -> Result<()>;

    async fn clear_exit_tp_by_client_oid(&self, exit_tp_client_oid: &str) -> Result<()>;

    async fn update_balance_and_clear_symbol_by_exit_tp(
        &self,
        exit_tp_client_oid: &str,
        balance: &str,
    ) -> Result<()>;
}

#[async_trait]
pub trait BotSlUpdate: Send + Sync {
    async fn update_exit_sl_client_oid_by_entry_client_oid(
        &self,
        entry_client_oid: &str,

        exit_sl_client_oid: &str,
        sl_stop_price: &str,
    ) -> Result<()>;

    async fn update_exit_sl_order_id_by_client_oid(
        &self,
        exit_sl_order_id: &str,
        exit_sl_client_oid: &str,
    ) -> Result<()>;

    async fn update_exit_sl_client_oid_by_order_id(
        &self,
        exit_sl_order_id: &str,
        exit_sl_client_oid: &str,
    ) -> Result<()>;

    async fn clear_exit_sl_by_client_oid(&self, exit_sl_client_oid: &str) -> Result<()>;

    async fn update_balance_and_clear_symbol_by_exit_sl(
        &self,
        exit_sl_client_oid: &str,
        balance: &str,
    ) -> Result<()>;

    async fn update_symbol_by_entry_client_oid(
        &self,
        symbol: &str,
        entry_client_oid: &str,
    ) -> Result<()>;
}

#[async_trait]
pub trait BotManagement: Send + Sync {
    async fn clear_all_bots(&self, balance: &str) -> Result<()>;
}

#[async_trait]
pub trait OrderQuery: Send + Sync {
    async fn get_total_match_value_by_client_oid(&self, client_oid: &str)
    -> Result<Option<String>>;
}

#[async_trait]
pub trait OrderCommand: Send + Sync {
    async fn save_order_event(&self, order: &OrderData) -> Result<()>;
}

#[async_trait]
pub trait BalanceCommand: Send + Sync {
    async fn save_balance_event(&self, balance: BalanceData) -> Result<()>;
}

#[async_trait]
pub trait PositionCommand: Send + Sync {
    async fn upsert_position_ratio(
        &self,
        debt_ratio: f64,
        total_asset: f64,
        margin_coefficient_total_asset: &str,
        total_debt: &str,
    ) -> Result<()>;

    async fn upsert_position_debt(&self, debt_symbol: &str, debt_value: &str) -> Result<()>;

    async fn upsert_position_asset(
        &self,
        asset_symbol: &str,
        asset_total: &str,
        asset_available: &str,
        asset_hold: &str,
    ) -> Result<()>;
}

#[async_trait]
pub trait SymbolQuery: Send + Sync {
    async fn get_random_symbol(&self) -> Result<Option<String>>;
    async fn get_symbol_info(&self, symbol: &str) -> Result<Option<Symbol>>;
    async fn get_currency_info(&self, currency: &str) -> Result<Option<Currencies>>;
}

#[async_trait]
pub trait ErrorCommand: Send + Sync {
    async fn save_error(&self, msg: &str) -> Result<()>;
}

#[async_trait]
pub trait EventCommand: Send + Sync {
    async fn save_event(&self, event: &serde_json::Value) -> Result<()>;
}

#[async_trait]
pub trait MessageCommand: Send + Sync {
    async fn save_send_orders(
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
    ) -> Result<()>;
}

#[async_trait]
pub trait BotRepositoryFull:
    BotQuery + BotEntryUpdate + BotTpUpdate + BotSlUpdate + BotManagement
{
}

impl<T> BotRepositoryFull for T where
    T: BotQuery + BotEntryUpdate + BotTpUpdate + BotSlUpdate + BotManagement
{
}

#[async_trait]
pub trait OrderRepositoryFull: OrderQuery + OrderCommand {}

impl<T> OrderRepositoryFull for T where T: OrderQuery + OrderCommand {}

#[async_trait]
pub trait SymbolRepositoryFull: SymbolQuery {}

impl<T> SymbolRepositoryFull for T where T: SymbolQuery {}

#[async_trait]
pub trait BalanceRepositoryFull: BalanceCommand {}

impl<T> BalanceRepositoryFull for T where T: BalanceCommand {}

#[async_trait]
pub trait PositionRepositoryFull: PositionCommand {}

impl<T> PositionRepositoryFull for T where T: PositionCommand {}

#[async_trait]
pub trait EventRepositoryFull: EventCommand {}

impl<T> EventRepositoryFull for T where T: EventCommand {}

#[async_trait]
pub trait SendOrdersRepositoryFull: MessageCommand {}

impl<T> SendOrdersRepositoryFull for T where T: MessageCommand {}

#[async_trait]
pub trait ErrorRepositoryFull: ErrorCommand {}

impl<T> ErrorRepositoryFull for T where T: ErrorCommand {}
