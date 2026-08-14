use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;
use serde_json;

use crate::api::models::{BalanceData, Bot, OrderData, Symbol};

#[derive(Debug, Clone)]
pub struct Currency {
    pub precision: i16,
}

#[derive(Debug, Clone, Default)]
pub struct BotUpdate {
    pub bot_id: i32,
    pub entry_client_oid: Option<String>,
    pub entry_price: Option<Decimal>,
    pub exit_tp_client_oid: Option<String>,
    pub exit_tp_price: Option<Decimal>,
    pub exit_sl_client_oid: Option<String>,
    pub exit_sl_price: Option<Decimal>,
    pub balance: Option<Decimal>,
    pub symbol: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OrderMessage {
    pub client_oid: Option<String>,
    pub order_id: Option<String>,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub size: Option<Decimal>,
    pub funds: Option<Decimal>,
    pub price: Option<Decimal>,
    pub time_in_force: String,
    pub auto_borrow: bool,
    pub auto_repay: bool,
}

#[derive(Debug, Clone)]
pub struct PositionRatio {
    pub debt_ratio: f64,
    pub total_asset: f64,
    pub margin_coefficient_total_asset: Decimal,
    pub total_debt: Decimal,
}

#[derive(Debug, Clone)]
pub struct PositionAsset {
    pub symbol: String,
    pub total: Decimal,
    pub available: Decimal,
    pub hold: Decimal,
}

#[derive(Debug, Clone)]
pub struct PositionDebt {
    pub symbol: String,
    pub value: Decimal,
}

#[async_trait]
pub trait KuCoinClient: Send + Sync {
    async fn get_websocket_url(&self) -> Result<String>;
}

#[async_trait]
pub trait Repository: Send + Sync {
    fn get_pool(&self) -> &sqlx::PgPool;

    async fn save_error(&self, msg: &str) -> Result<()>;
    async fn save_event(&self, event: &serde_json::Value) -> Result<()>;
    async fn save_order(&self, order: &OrderData) -> Result<()>;
    async fn save_balance(&self, balance: &BalanceData) -> Result<()>;
    async fn save_order_message(&self, msg: &OrderMessage) -> Result<()>;

    async fn get_bot_by_client_oid(&self, client_oid: &str) -> Result<Option<Bot>>;
    async fn get_all_bots(&self) -> Result<Vec<Bot>>;
    async fn get_random_symbol(&self) -> Result<Option<String>>;
    async fn get_symbol_info(&self, symbol: &str) -> Result<Option<Symbol>>;
    async fn get_currency_info(&self, currency: &str) -> Result<Option<Currency>>;
    async fn get_total_match_value(&self, client_oid: &str) -> Result<Option<Decimal>>;

    async fn update_bot(&self, bot_update: BotUpdate) -> Result<()>;
    async fn update_position_ratio(&self, ratio: PositionRatio) -> Result<()>;
    async fn update_position_asset(&self, asset: PositionAsset) -> Result<()>;
    async fn update_position_debt(&self, debt: PositionDebt) -> Result<()>;
    async fn clear_bots(&self, balance: &str) -> Result<()>;
}
