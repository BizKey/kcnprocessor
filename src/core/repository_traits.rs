use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;

use crate::api::models::{BalanceData, Bot, OrderData, Symbol, Currencies};

pub trait BotRepositoryTrait: Send + Sync {
    async fn get_by_client_oid(&self, client_oid: &str) -> Result<Option<Bot>>;
    async fn get_by_entry_client_oid(&self, entry_client_oid: &str) -> Result<Option<Bot>>;
    async fn get_by_exit_tp_client_oid(&self, exit_tp_client_oid: &str) -> Result<Option<Bot>>;
    async fn get_by_exit_sl_client_oid(&self, exit_sl_client_oid: &str) -> Result<Option<Bot>>;
    async fn get_all(&self) -> Result<Vec<Bot>>;
    async fn update_entry_client_oid_by_id(
        &self,
        symbol: Option<&str>,
        entry_client_oid: Option<&str>,
        id: i32,
    ) -> Result<()>;
    async fn update_exit_tp_client_oid_by_entry_client_oid(
        &self,
        entry_client_oid: &str,
        exit_tp_client_oid: &str,
    ) -> Result<()>;
    async fn update_exit_sl_client_oid_by_entry_client_oid(
        &self,
        entry_client_oid: &str,
        exit_sl_client_oid: &str,
    ) -> Result<()>;
    async fn update_exit_tp_order_id_by_client_oid(
        &self,
        exit_tp_order_id: &str,
        exit_tp_client_oid: &str,
    ) -> Result<()>;
    async fn update_exit_sl_order_id_by_client_oid(
        &self,
        exit_sl_order_id: &str,
        exit_sl_client_oid: &str,
    ) -> Result<()>;
    async fn update_exit_tp_client_oid_by_order_id(
        &self,
        exit_tp_order_id: &str,
        exit_tp_client_oid: &str,
    ) -> Result<()>;
    async fn update_exit_sl_client_oid_by_order_id(
        &self,
        exit_sl_order_id: &str,
        exit_sl_client_oid: &str,
    ) -> Result<()>;
    async fn clear_entry_client_oid(&self, entry_client_oid: &str) -> Result<()>;
    async fn clear_exit_tp_by_client_oid(&self, exit_tp_client_oid: &str) -> Result<()>;
    async fn clear_exit_sl_by_client_oid(&self, exit_sl_client_oid: &str) -> Result<()>;
    async fn update_balance_by_entry_client_oid(
        &self,
        entry_client_oid: &str,
        balance: &str,
    ) -> Result<()>;
    async fn update_balance_and_clear_symbol_by_exit_tp(
        &self,
        exit_tp_client_oid: &str,
        balance: &str,
    ) -> Result<()>;
    async fn update_balance_and_clear_symbol_by_exit_sl(
        &self,
        exit_sl_client_oid: &str,
        balance: &str,
    ) -> Result<()>;
    async fn clear_all_bots(&self, balance: &str) -> Result<()>;
    async fn clear_symbol_by_exit_sl_client_oid(&self, exit_sl_client_oid: &str) -> Result<()>;
}

pub trait OrderRepositoryTrait: Send + Sync {
    async fn save_order_event(&self, order: OrderData) -> Result<()>;
    async fn get_total_match_value_by_client_oid(&self, client_oid: &str) -> Result<Option<String>>;
}

pub trait BalanceRepositoryTrait: Send + Sync {
    async fn save_balance_event(&self, balance: BalanceData) -> Result<()>;
}

pub trait SymbolRepositoryTrait: Send + Sync {
    async fn get_random_symbol(&self) -> Result<Option<String>>;
    async fn get_symbol_info(&self, symbol: &str) -> Result<Option<Symbol>>;
    async fn get_currency_info(&self, currency: &str) -> Result<Option<Currencies>>;
}

pub trait PositionRepositoryTrait: Send + Sync {
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