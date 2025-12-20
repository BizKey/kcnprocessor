use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
    pub data: ApiV3BulletPrivateData,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WelcomeData {
    pub id: String,
}
#[derive(Debug, Deserialize, Serialize)]
pub struct AckData {
    pub id: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BalanceRelationContext {
    pub symbol: Option<String>,
    #[serde(rename = "orderId")]
    pub order_id: Option<String>,
    #[serde(rename = "tradeId")]
    pub trade_id: Option<String>,
}
#[derive(Debug, Deserialize, Serialize)]
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
#[derive(sqlx::FromRow, Debug)]
pub struct ActiveOrder {
    pub order_id: String,
    pub symbol: String,
}
#[derive(Debug, Deserialize, Serialize)]
pub struct AssetInfo {
    pub total: String,
    pub available: String,
    pub hold: String,
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
    pub debt_list: HashMap<String, String>, // ключ: актив, значение: строка долга
    pub timestamp: i64,
}
#[derive(Debug, Deserialize, Serialize)]
pub struct OrderData {
    pub status: String, // new open match done
    #[serde(rename = "type")]
    pub type_: String, // open match update filled canceled received
    pub symbol: String, // BTC-USDT ETH-USDT KCS-USDT
    pub side: String,   // buy sell
    #[serde(rename = "orderType")]
    pub order_type: String, // limit market
    #[serde(rename = "feeType")]
    pub fee_type: Option<String>, // takerFee makerFee
    pub liquidity: Option<String>, // taker maker
    pub price: Option<String>,
    #[serde(rename = "orderId")]
    pub order_id: String,
    #[serde(rename = "clientOid")]
    pub client_oid: String,
    #[serde(rename = "tradeId")]
    pub trade_id: Option<String>,
    #[serde(rename = "originSize")]
    pub origin_size: Option<String>,
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
    pub remain_size: Option<String>, // only on limit order
    #[serde(rename = "remainFunds")]
    pub remain_funds: Option<String>, // only on market order
    #[serde(rename = "orderTime")]
    pub order_time: i64,
    pub ts: i64,
}
#[derive(Debug, Deserialize, Serialize)]
pub struct MessageData {
    pub topic: String,
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
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow, Clone)]
pub struct Symbol {
    pub exchange: String,
    pub symbol: String,
    pub base_increment: String,
    pub price_increment: String,
    pub base_min_size: String,
}

#[derive(Debug, Deserialize)]
pub struct SymbolOpenOrderData {
    pub symbols: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct SymbolOpenOrder {
    pub data: SymbolOpenOrderData,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TradeMsgData {
    #[serde(rename = "borrowSize")]
    pub borrow_size: Option<String>,
    #[serde(rename = "clientOid")]
    pub client_oid: Option<String>,
    #[serde(rename = "orderId")]
    pub order_id: Option<String>,
    #[serde(rename = "loanApplyId")]
    pub loan_apply_id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TradeMsgRateLimit {
    pub limit: Option<i64>,
    pub reset: Option<i64>,
    pub remaining: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TradeMsg {
    pub id: Option<String>,
    pub op: Option<String>,
    pub msg: Option<String>,
    pub code: Option<String>,
    pub data: Option<TradeMsgData>,
    #[serde(rename = "inTime")]
    pub in_time: i64,
    #[serde(rename = "outTime")]
    pub out_time: i64,
    #[serde(rename = "userRateLimit")]
    pub user_rate_limit: Option<TradeMsgRateLimit>,
}
#[derive(sqlx::FromRow, Debug)]
pub struct MsgSend {
    pub args_symbol: Option<String>,
    pub args_side: Option<String>,
    pub args_size: Option<String>,
    pub args_price: Option<String>,
}
