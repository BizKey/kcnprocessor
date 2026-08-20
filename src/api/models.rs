use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BotOrderType {
    Entry,
    TakeProfit,
    StopLoss,
}

impl Bot {
    pub fn get_order_type(&self, client_oid: &str) -> Option<BotOrderType> {
        if Some(client_oid) == self.entry_client_oid.as_deref() {
            Some(BotOrderType::Entry)
        } else if Some(client_oid) == self.exit_tp_client_oid.as_deref() {
            Some(BotOrderType::TakeProfit)
        } else if Some(client_oid) == self.exit_sl_client_oid.as_deref() {
            Some(BotOrderType::StopLoss)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub enum OrderAmount {
    Size(String),
    Funds(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum OrderTopic {
    #[serde(rename = "/account/balance")]
    Balance,
    #[serde(rename = "/spotMarket/tradeOrdersV2")]
    TradeOrders,
    #[serde(rename = "/spotMarket/advancedOrders")]
    AdvancedOrders,
    #[serde(rename = "/margin/position")]
    Position,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderType {
    Market,
    Limit,
    Stop,
    #[serde(other)]
    Unknown,
}

impl OrderType {
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderType::Market => "market",
            OrderType::Limit => "limit",
            OrderType::Stop => "stop",
            OrderType::Unknown => "unknown",
        }
    }
}

impl From<&str> for OrderType {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "market" => OrderType::Market,
            "limit" => OrderType::Limit,
            "stop" => OrderType::Stop,
            _ => OrderType::Unknown,
        }
    }
}

impl fmt::Display for OrderType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderEventType {
    Match,
    Canceled,
    Open,
    Filled,
    Partial,
    #[serde(other)]
    Unknown,
}

impl OrderEventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderEventType::Match => "match",
            OrderEventType::Canceled => "canceled",
            OrderEventType::Open => "open",
            OrderEventType::Filled => "filled",
            OrderEventType::Partial => "partial",
            OrderEventType::Unknown => "unknown",
        }
    }
}

impl From<&str> for OrderEventType {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "match" => OrderEventType::Match,
            "canceled" => OrderEventType::Canceled,
            "open" => OrderEventType::Open,
            "filled" => OrderEventType::Filled,
            "partial" => OrderEventType::Partial,
            _ => OrderEventType::Unknown,
        }
    }
}

impl fmt::Display for OrderEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderSide {
    Buy,
    Sell,
    #[serde(other)]
    Unknown,
}

impl OrderSide {
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderSide::Buy => "buy",
            OrderSide::Sell => "sell",
            OrderSide::Unknown => "unknown",
        }
    }
}

impl From<&str> for OrderSide {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "buy" => OrderSide::Buy,
            "sell" => OrderSide::Sell,
            _ => OrderSide::Unknown,
        }
    }
}

impl fmt::Display for OrderSide {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum StopType {
    Loss,
    Entry,
    #[serde(other)]
    Unknown,
}

impl StopType {
    pub fn as_str(&self) -> &'static str {
        match self {
            StopType::Loss => "loss",
            StopType::Entry => "entry",
            StopType::Unknown => "unknown",
        }
    }
}

impl From<&str> for StopType {
    fn from(s: &str) -> Self {
        match s {
            "loss" => StopType::Loss,
            "entry" => StopType::Entry,
            _ => StopType::Unknown,
        }
    }
}

impl fmt::Display for StopType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Deserialize)]
pub struct ApiV3BulletPrivateDataInstanceServers {
    pub endpoint: String,
}

#[derive(Debug, Deserialize)]
pub struct ApiV3BulletPrivateData {
    pub token: String,
    #[serde(rename = "instanceServers")]
    pub instance_servers: Vec<ApiV3BulletPrivateDataInstanceServers>,
}

#[derive(Debug, Deserialize)]
pub struct ApiV3BulletPrivate {
    pub code: String,
    pub msg: Option<String>,
    pub data: Option<ApiV3BulletPrivateData>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WelcomeData {
    pub id: String,
}
#[derive(Debug, Deserialize, Serialize)]
pub struct AckData {
    pub id: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BalanceRelationContext {
    pub symbol: Option<String>,
    #[serde(rename = "orderId")]
    pub order_id: Option<String>,
    #[serde(rename = "tradeId")]
    pub trade_id: Option<String>,
}
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BalanceData {
    #[serde(rename = "accountId")]
    pub account_id: String,
    pub available: String,
    #[serde(rename = "availableChange")]
    pub available_change: String,
    pub currency: String,
    pub hold: String,
    #[serde(rename = "holdChange")]
    pub hold_change: String,
    #[serde(rename = "relationEvent")]
    pub relation_event: String,
    #[serde(rename = "relationEventId")]
    pub relation_event_id: String,
    pub time: String,
    pub total: String,
    #[serde(rename = "relationContext")]
    pub relation_context: Option<BalanceRelationContext>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AssetInfo {
    pub total: String,
    pub available: String,
    pub hold: String,
}
impl AssetInfo {
    #[inline]
    pub fn available_decimal(&self) -> Result<Decimal> {
        Decimal::from_str(&self.available)
            .map_err(|e| anyhow::anyhow!(e))
            .with_context(|| format!("Fail parse decimal:{}", self.available))
    }
}
#[derive(Debug, Deserialize, Serialize)]
pub struct PositionData {
    #[serde(rename = "debtRatio")]
    pub debt_ratio: f64,
    #[serde(rename = "totalAsset")]
    pub total_asset: f64,
    #[serde(rename = "marginCoefficientTotalAsset")]
    pub margin_coefficient_total_asset: String,
    #[serde(rename = "totalDebt")]
    pub total_debt: String,
    #[serde(rename = "assetList")]
    pub asset_list: HashMap<String, AssetInfo>,
    #[serde(rename = "debtList")]
    pub debt_list: HashMap<String, String>,
    pub timestamp: i64,
}

impl PositionData {
    pub fn debt_pairs(&self) -> Result<Vec<(String, Decimal)>> {
        self.debt_list
            .iter()
            .map(|(asset, debt_str)| {
                Ok((
                    asset.clone(),
                    Decimal::from_str(debt_str)
                        .map_err(|e| anyhow::anyhow!(e))
                        .with_context(|| format!("Invalid decimal in debt_list: {}", debt_str))?,
                ))
            })
            .collect()
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct OrderData {
    pub status: String,
    #[serde(rename = "type")]
    pub type_: OrderEventType,
    pub symbol: String,
    pub side: OrderSide,
    #[serde(rename = "orderType")]
    pub order_type: OrderType,
    #[serde(rename = "feeType")]
    pub fee_type: Option<String>,
    pub liquidity: Option<String>,
    pub price: Option<String>,
    #[serde(rename = "orderId")]
    pub order_id: String,
    #[serde(rename = "clientOid")]
    pub client_oid: Option<String>,
    #[serde(rename = "tradeId")]
    pub trade_id: Option<String>,
    #[serde(rename = "originSize")]
    pub origin_size: Option<String>,
    #[serde(rename = "originFunds")]
    pub origin_funds: Option<String>,
    pub size: Option<String>,
    #[serde(rename = "filledSize")]
    pub filled_size: Option<String>,
    #[serde(rename = "matchSize")]
    pub match_size: Option<String>,
    #[serde(rename = "matchPrice")]
    pub match_price: Option<String>,
    #[serde(rename = "canceledSize")]
    pub canceled_size: Option<String>,
    #[serde(rename = "oldSize")]
    pub old_size: Option<String>,
    #[serde(rename = "remainSize")]
    pub remain_size: Option<String>,
    #[serde(rename = "remainFunds")]
    pub remain_funds: Option<String>,
    #[serde(rename = "orderTime")]
    pub order_time: i64,
    pub ts: i64,
}
impl OrderData {
    #[inline]
    pub fn filled_size_decimal(&self) -> Result<Decimal> {
        let filled_size = match self.filled_size.as_ref() {
            Some(filled_size) => filled_size,
            None => {
                anyhow::bail!("filled_size is None:{:?}", self)
            }
        };

        Decimal::from_str(filled_size)
            .map_err(|e| anyhow::anyhow!(e))
            .with_context(|| format!("Fail parse decimal:{}", filled_size))
    }

    #[inline]
    pub fn is_terminal(&self) -> bool {
        matches!(self.type_, OrderEventType::Match | OrderEventType::Canceled)
    }

    #[inline]
    pub fn is_remain_zero(&self) -> bool {
        self.remain_size.as_deref() == Some("0") || self.remain_funds.as_deref() == Some("0")
    }

    #[inline]
    pub fn should_process(&self) -> bool {
        self.is_terminal() && self.is_remain_zero()
    }
}
impl fmt::Display for OrderData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "OrderData {{ status: {}, type: {}, symbol: {}, side: {}, order_type: {}, fee_type: {:?}, liquidity: {:?}, price: {:?}, order_id: {}, client_oid: {:?}, trade_id: {:?}, origin_size: {:?}, origin_funds: {:?}, size: {:?}, filled_size: {:?}, match_size: {:?}, match_price: {:?}, canceled_size: {:?}, old_size: {:?}, remain_size: {:?}, remain_funds: {:?}, order_time: {}, ts: {} }}",
            self.status,
            self.type_,
            self.symbol,
            self.side,
            self.order_type,
            self.fee_type,
            self.liquidity,
            self.price,
            self.order_id,
            self.client_oid,
            self.trade_id,
            self.origin_size,
            self.origin_funds,
            self.size,
            self.filled_size,
            self.match_size,
            self.match_price,
            self.canceled_size,
            self.old_size,
            self.remain_size,
            self.remain_funds,
            self.order_time,
            self.ts
        )
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MessageData {
    pub topic: OrderTopic,
    #[serde(rename = "userId")]
    pub user_id: String,
    #[serde(rename = "channelType")]
    pub channel_type: String,
    pub subject: String,
    pub data: serde_json::Value,
}
#[derive(Debug, Deserialize, Serialize)]
pub struct ErrorData {
    pub id: String,
    pub code: i64,
    pub data: String,
}
#[derive(Debug, Deserialize, Serialize)]
pub struct ApiV1MarketOrderbookLevel1ResData {
    pub time: f64,
    pub sequence: String,
    pub price: String,
    #[serde(rename = "bestBid")]
    pub best_bid: String,
    #[serde(rename = "bestAsk")]
    pub best_ask: String,
}

impl ApiV1MarketOrderbookLevel1ResData {
    #[inline]
    pub fn price_decimal(&self) -> Result<Decimal> {
        Decimal::from_str(&self.price)
            .map_err(|e| anyhow::anyhow!(e))
            .with_context(|| format!("Fail parse decimal:{}", self.price))
    }
    #[inline]
    pub fn best_bid_decimal(&self) -> Result<Decimal> {
        Decimal::from_str(&self.best_bid)
            .map_err(|e| anyhow::anyhow!(e))
            .with_context(|| format!("Fail parse decimal:{}", self.best_bid))
    }
    #[inline]
    pub fn best_ask_decimal(&self) -> Result<Decimal> {
        Decimal::from_str(&self.best_ask)
            .map_err(|e| anyhow::anyhow!(e))
            .with_context(|| format!("Fail parse decimal:{}", self.best_ask))
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ApiV1MarketOrderbookLevel1Res {
    pub code: String,
    pub msg: Option<String>,
    pub data: Option<ApiV1MarketOrderbookLevel1ResData>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum KuCoinMessage {
    #[serde(rename = "welcome")]
    Welcome(WelcomeData),

    #[serde(rename = "ack")]
    Ack(AckData),

    #[serde(rename = "message")]
    Message(MessageData),

    #[serde(rename = "error")]
    Error(ErrorData),

    #[serde(other)]
    Unknown,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow, Clone)]
pub struct Symbol {
    pub exchange: String,
    pub symbol: String,
    base_increment: String,
    min_funds: Option<String>,
    price_increment: String,
    quote_increment: String,
    base_min_size: String,
    quote_min_size: String,
}

impl Symbol {
    #[inline]
    pub fn base_increment_decimal(&self) -> Result<Decimal> {
        Decimal::from_str(&self.base_increment)
            .map_err(|e| anyhow::anyhow!(e))
            .with_context(|| format!("Fail parse decimal:{}", self.base_increment))
    }
    #[inline]
    pub fn quote_increment_decimal(&self) -> Result<Decimal> {
        Decimal::from_str(&self.quote_increment)
            .map_err(|e| anyhow::anyhow!(e))
            .with_context(|| format!("Fail parse decimal:{}", self.quote_increment))
    }
    #[inline]
    pub fn price_increment_decimal(&self) -> Result<Decimal> {
        Decimal::from_str(&self.price_increment)
            .map_err(|e| anyhow::anyhow!(e))
            .with_context(|| format!("Fail parse decimal:{}", self.price_increment))
    }
    #[inline]
    pub fn base_min_size_decimal(&self) -> Result<Decimal> {
        Decimal::from_str(&self.base_min_size)
            .map_err(|e| anyhow::anyhow!(e))
            .with_context(|| format!("Fail parse decimal:{}", self.base_min_size))
    }
    #[inline]
    pub fn quote_min_size_decimal(&self) -> Result<Decimal> {
        Decimal::from_str(&self.quote_min_size)
            .map_err(|e| anyhow::anyhow!(e))
            .with_context(|| format!("Fail parse decimal:{}", self.quote_min_size))
    }
    #[inline]
    pub fn min_funds_decimal(&self) -> Result<Decimal> {
        let min_funds = match self.min_funds.as_ref() {
            Some(min_funds) => min_funds,
            None => {
                anyhow::bail!("min_funds is None for symbol {:?}", self)
            }
        };

        Decimal::from_str(min_funds)
            .map_err(|e| anyhow::anyhow!(e))
            .with_context(|| format!("Fail parse decimal:{}", min_funds))
    }
}

#[derive(Debug, Deserialize, sqlx::FromRow)]
pub struct Currencies {
    pub precision: i16,
}

impl Currencies {
    #[inline]
    pub fn precision_decimal(&self) -> Result<Decimal> {
        if self.precision < 0 {
            anyhow::bail!("Precision cannot be negative: {}", self.precision)
        }

        Decimal::from_str(&format!("1e-{}", self.precision))
            .map_err(|e| anyhow::anyhow!(e))
            .with_context(|| format!("Failed to parse decimal from precision: {}", self.precision))
    }
}

#[derive(Debug, Deserialize)]
pub struct MakeOrderResData {
    #[serde(rename = "orderId")]
    pub order_id: String,
    #[serde(rename = "clientOid")]
    pub client_oid: String,
}

#[derive(Debug, Deserialize)]
pub struct MakeOrderRes {
    pub code: String,
    pub msg: Option<String>,
    pub data: Option<MakeOrderResData>,
}
#[derive(Debug, Deserialize)]
pub struct MakeStopOrderResData {
    #[serde(rename = "orderId")]
    pub order_id: String,
    #[serde(rename = "clientOid")]
    pub client_oid: String,
}
#[derive(Debug, Deserialize)]
pub struct MakeStopOrderRes {
    pub code: String,
    pub msg: Option<String>,
    pub data: Option<MakeStopOrderResData>,
}
#[derive(Debug, Deserialize)]
pub struct ApiV3MarginRepayResData {
    pub timestamp: u64,
    #[serde(rename = "orderNo")]
    pub order_no: String,
    #[serde(rename = "actualSize")]
    pub actual_size: String,
}
#[derive(Debug, Deserialize)]
pub struct ApiV3MarginRepayRes {
    pub code: String,
    pub msg: Option<String>,
    pub data: Option<ApiV3MarginRepayResData>,
}
#[derive(Debug, Deserialize)]
pub struct ApiV3AccountsUniversalTransferResData {
    #[serde(rename = "orderId")]
    pub order_id: String,
}

#[derive(Debug, Deserialize)]
pub struct ApiV3HfMarginStopOrdersResDataItem {
    pub id: String,
}

#[derive(Debug, Deserialize)]
pub struct ApiV3HfMarginStopOrdersResData {
    #[serde(rename = "totalNum")]
    pub total_num: i32,
    pub items: Vec<ApiV3HfMarginStopOrdersResDataItem>,
}
#[derive(Debug, Deserialize)]
pub struct ApiV3HfMarginStopOrdersRes {
    pub code: String,
    pub msg: Option<String>,
    pub data: Option<ApiV3HfMarginStopOrdersResData>,
}
#[derive(Debug, Deserialize)]
pub struct ApiV3AccountsUniversalTransferRes {
    pub code: String,
    pub msg: Option<String>,
    pub data: Option<ApiV3AccountsUniversalTransferResData>,
}
#[derive(Debug, Deserialize)]
pub struct ApiV3HfMarginStopOrderCancelByClientOidResData {
    #[serde(rename = "cancelledOrderIds")]
    pub cancelled_order_ids: Vec<String>,
}
#[derive(Debug, Deserialize)]
pub struct ApiV3HfMarginStopOrderCancelByIdResData {
    #[serde(rename = "cancelledOrderIds")]
    pub cancelled_order_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct ApiV3HfMarginStopOrderCancelByClientOidRes {
    pub code: String,
    pub msg: Option<String>,
    pub data: Option<ApiV3HfMarginStopOrderCancelByClientOidResData>,
}
#[derive(Debug, Deserialize)]
pub struct ApiV3HfMarginStopOrderCancelByIdRes {
    pub code: String,
    pub msg: Option<String>,
    pub data: Option<ApiV3HfMarginStopOrderCancelByIdResData>,
}

#[derive(Debug, Deserialize)]
pub struct MarginAccountDataAccount {
    pub currency: String,
    pub available: String,
    pub liability: String,
}

impl MarginAccountDataAccount {
    #[inline]
    pub fn available_decimal(&self) -> Result<Decimal> {
        Decimal::from_str(&self.available)
            .map_err(|e| anyhow::anyhow!(e))
            .with_context(|| format!("Fail parse decimal:{}", self.available))
    }
    #[inline]
    pub fn liability_decimal(&self) -> Result<Decimal> {
        Decimal::from_str(&self.liability)
            .map_err(|e| anyhow::anyhow!(e))
            .with_context(|| format!("Fail parse decimal:{}", self.liability))
    }
}

#[derive(Debug, Deserialize)]
pub struct MarginAccountData {
    pub accounts: Vec<MarginAccountDataAccount>,
}
#[derive(Debug, Deserialize)]
pub struct MarginAccount {
    pub code: String,
    pub msg: Option<String>,
    pub data: MarginAccountData,
}
#[derive(sqlx::FromRow, Debug)]
pub struct Bot {
    pub id: i32,
    balance: String,
    pub entry_client_oid: Option<String>,
    pub entry_price: Option<String>,
    pub exit_tp_order_id: Option<String>,
    pub exit_tp_price: Option<String>,
    pub exit_tp_client_oid: Option<String>,
    pub exit_sl_order_id: Option<String>,
    pub exit_sl_price: Option<String>,
    pub exit_sl_client_oid: Option<String>,
}

impl Bot {
    #[inline]
    pub fn balance_decimal(&self) -> Result<Decimal> {
        Decimal::from_str(&self.balance)
            .map_err(|e| anyhow::anyhow!(e))
            .with_context(|| format!("Fail parse decimal:{}", self.balance))
    }
}

#[derive(Debug, Deserialize)]
pub struct AdvancedOrders {
    #[serde(rename = "createdAt")]
    pub created_at: i64,
    pub funds: Option<String>,
    pub size: Option<String>,
    #[serde(rename = "orderId")]
    pub order_id: String,
    #[serde(rename = "orderType")]
    pub order_type: String,
    pub side: OrderSide,
    pub stop: StopType,
    #[serde(rename = "stopPrice")]
    pub stop_price: String,
    pub symbol: String,
    #[serde(rename = "tradeType")]
    pub trade_type: String,
    pub ts: i64,
    #[serde(rename = "type")]
    pub type_: String,
    pub error: Option<String>,
}
impl fmt::Display for AdvancedOrders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "OrderData {{ created_at: {}, funds: {:?}, size: {:?}, order_id: {}, order_type: {}, side: {:?}, stop: {:?}, stop_price: {:?}, symbol: {}, trade_type: {:?}, ts: {:?}, type_: {:?}, error: {:?}}}",
            self.created_at,
            self.funds,
            self.size,
            self.order_id,
            self.order_type,
            self.side,
            self.stop,
            self.stop_price,
            self.symbol,
            self.trade_type,
            self.ts,
            self.type_,
            self.error
        )
    }
}
