use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
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

#[derive(Debug, Deserialize, Serialize)]
pub struct AssetInfo {
    pub total: String,
    pub available: String,
    pub hold: String,
}
impl AssetInfo {
    pub fn available_decimal(&self) -> Result<Decimal, String> {
        match Decimal::from_str(&self.available) {
            Ok(available) => Ok(available),
            Err(e) => {
                let msg: String = format!("Fail parse decimal:{} {}", self.available, e);
                log::error!("{}", msg);
                Err(msg)
            }
        }
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
    pub debt_list: HashMap<String, String>, // ключ: актив, значение: строка долга
    pub timestamp: i64,
}

impl PositionData {
    pub fn debt_pairs(&self) -> Result<Vec<(String, Decimal)>, String> {
        let mut result = Vec::new();
        for (asset, debt_str) in &self.debt_list {
            let decimal = match Decimal::from_str(debt_str) {
                Ok(decimal) => decimal,
                Err(e) => {
                    let msg: String = format!("Fail parse decimal:{} {}", debt_str, e);
                    log::error!("{}", msg);
                    return Err(msg);
                }
            };
            result.push((asset.clone(), decimal));
        }
        Ok(result)
    }
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
    pub remain_size: Option<String>, // only on limit order
    #[serde(rename = "remainFunds")]
    pub remain_funds: Option<String>, // only on market order
    #[serde(rename = "orderTime")]
    pub order_time: i64,
    pub ts: i64,
}
impl OrderData {
    pub fn filled_size_decimal(&self) -> Result<Decimal, String> {
        let filled_size_str = match &self.filled_size {
            Some(filled_size_str) => filled_size_str,
            None => {
                let msg: String = format!("filled_size is None:{:?}", &self);
                log::error!("{}", msg);
                return Err(msg);
            }
        };

        match Decimal::from_str(filled_size_str) {
            Ok(filled_size) => Ok(filled_size),
            Err(e) => {
                let msg: String = format!("Fail parse decimal:{} {}", filled_size_str, e);
                log::error!("{}", msg);
                Err(msg)
            }
        }
    }
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
pub struct ApiV1MarketOrderbookLevel1ResData {
    pub time: f64,
    pub sequence: String,
    pub price: String,
    pub size: String,
    #[serde(rename = "bestBid")]
    pub best_bid: String,
    #[serde(rename = "bestBidSize")]
    pub best_bid_size: String,
    #[serde(rename = "bestAsk")]
    pub best_ask: String,
    #[serde(rename = "bestAskSize")]
    pub best_ask_size: String,
}

impl ApiV1MarketOrderbookLevel1ResData {
    pub fn price_decimal(&self) -> Result<Decimal, String> {
        match Decimal::from_str(&self.price) {
            Ok(price) => Ok(price),
            Err(e) => {
                let msg: String = format!("Fail parse decimal:{} {}", self.price, e);
                log::error!("{}", msg);
                Err(msg)
            }
        }
    }

    pub fn size_decimal(&self) -> Result<Decimal, String> {
        match Decimal::from_str(&self.size) {
            Ok(size) => Ok(size),
            Err(e) => {
                let msg: String = format!("Fail parse decimal:{} {}", self.size, e);
                log::error!("{}", msg);
                Err(msg)
            }
        }
    }

    pub fn best_bid_decimal(&self) -> Result<Decimal, String> {
        match Decimal::from_str(&self.best_bid) {
            Ok(best_bid) => Ok(best_bid),
            Err(e) => {
                let msg: String = format!("Fail parse decimal:{} {}", self.best_bid, e);
                log::error!("{}", msg);
                Err(msg)
            }
        }
    }

    pub fn best_bid_size_decimal(&self) -> Result<Decimal, String> {
        match Decimal::from_str(&self.best_bid_size) {
            Ok(best_bid_size) => Ok(best_bid_size),
            Err(e) => {
                let msg: String = format!("Fail parse decimal:{} {}", self.best_bid_size, e);
                log::error!("{}", msg);
                Err(msg)
            }
        }
    }

    pub fn best_ask_decimal(&self) -> Result<Decimal, String> {
        match Decimal::from_str(&self.best_ask) {
            Ok(best_ask) => Ok(best_ask),
            Err(e) => {
                let msg: String = format!("Fail parse decimal:{} {}", self.best_ask, e);
                log::error!("{}", msg);
                Err(msg)
            }
        }
    }

    pub fn best_ask_size_decimal(&self) -> Result<Decimal, String> {
        match Decimal::from_str(&self.best_ask_size) {
            Ok(best_ask_size) => Ok(best_ask_size),
            Err(e) => {
                let msg: String = format!("Fail parse decimal:{} {}", self.best_ask_size, e);
                log::error!("{}", msg);
                Err(msg)
            }
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ApiV1MarketOrderbookLevel1Res {
    pub code: String,
    pub msg: Option<String>,
    pub data: ApiV1MarketOrderbookLevel1ResData,
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
    pub base_increment: String,
    pub min_funds: Option<String>,
    pub price_increment: String,
    pub quote_increment: String,
    pub base_min_size: String,
    pub quote_min_size: String,
}

impl Symbol {
    pub fn base_increment_decimal(&self) -> Result<Decimal, String> {
        match Decimal::from_str(&self.base_increment) {
            Ok(base_increment) => Ok(base_increment),
            Err(e) => {
                let msg: String = format!("Fail parse decimal:{} {}", self.base_increment, e);
                log::error!("{}", msg);
                Err(msg)
            }
        }
    }

    pub fn quote_increment_decimal(&self) -> Result<Decimal, String> {
        match Decimal::from_str(&self.quote_increment) {
            Ok(quote_increment) => Ok(quote_increment),
            Err(e) => {
                let msg: String = format!("Fail parse decimal:{} {}", self.quote_increment, e);
                log::error!("{}", msg);
                Err(msg)
            }
        }
    }

    pub fn price_increment_decimal(&self) -> Result<Decimal, String> {
        match Decimal::from_str(&self.price_increment) {
            Ok(price_increment) => Ok(price_increment),
            Err(e) => {
                let msg: String = format!("Fail parse decimal:{} {}", self.price_increment, e);
                log::error!("{}", msg);
                Err(msg)
            }
        }
    }

    pub fn base_min_size_decimal(&self) -> Result<Decimal, String> {
        match Decimal::from_str(&self.base_min_size) {
            Ok(base_min_size) => Ok(base_min_size),
            Err(e) => {
                let msg: String = format!("Fail parse decimal:{} {}", self.base_min_size, e);
                log::error!("{}", msg);
                Err(msg)
            }
        }
    }

    pub fn quote_min_size_decimal(&self) -> Result<Decimal, String> {
        match Decimal::from_str(&self.quote_min_size) {
            Ok(quote_min_size) => Ok(quote_min_size),
            Err(e) => {
                let msg: String = format!("Fail parse decimal:{} {}", self.quote_min_size, e);
                log::error!("{}", msg);
                Err(msg)
            }
        }
    }

    pub fn min_funds_decimal(&self) -> Result<Decimal, String> {
        let min_funds = match &self.min_funds {
            Some(min_funds_str) => min_funds_str,
            None => {
                let msg: String = format!("min_funds is None for symbol {:?}", &self);
                log::error!("{}", msg);
                return Err(msg);
            }
        };
        match Decimal::from_str(min_funds) {
            Ok(min_funds) => Ok(min_funds),
            Err(e) => {
                let msg: String = format!("Fail parse decimal:{} {}", min_funds, e);
                log::error!("{}", msg);
                Err(msg)
            }
        }
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
    pub timestamp: i32,
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
pub struct ApiV3HfMarginStopOrderCancelResData {
    #[serde(rename = "cancelledOrderIds")]
    pub cancelled_order_ids: Vec<String>,
}
#[derive(Debug, Deserialize)]
pub struct ApiV3HfMarginStopOrderCancelRes {
    pub code: String,
    pub msg: Option<String>,
    pub data: Option<ApiV3HfMarginStopOrderCancelResData>,
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
pub struct ApiV3HfMarginStopOrderCancelByClientOidRes {
    pub code: String,
    pub msg: Option<String>,
    pub data: Option<ApiV3HfMarginStopOrderCancelByClientOidResData>,
}

#[derive(Debug, Deserialize)]
pub struct MarginAccountDataAccount {
    pub currency: String,
    pub available: String,
    pub liability: String,
}

impl MarginAccountDataAccount {
    pub fn available_decimal(&self) -> Result<Decimal, String> {
        match Decimal::from_str(&self.available) {
            Ok(available) => Ok(available),
            Err(e) => {
                let msg: String = format!("Fail parse decimal:{} {}", self.available, e);
                log::error!("{}", msg);
                Err(msg)
            }
        }
    }

    pub fn liability_decimal(&self) -> Result<Decimal, String> {
        match Decimal::from_str(&self.liability) {
            Ok(liability) => Ok(liability),
            Err(e) => {
                let msg: String = format!("Fail parse decimal:{} {}", self.liability, e);
                log::error!("{}", msg);
                Err(msg)
            }
        }
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
    pub balance: String,
    pub entry_client_oid: Option<String>,
    pub exit_tp_order_id: Option<String>,
    pub exit_tp_client_oid: Option<String>,
    pub exit_sl_order_id: Option<String>,
    pub exit_sl_client_oid: Option<String>,
}

impl Bot {
    pub fn balance_decimal(&self) -> Result<Decimal, String> {
        match Decimal::from_str(&self.balance) {
            Ok(balance) => Ok(balance),
            Err(e) => {
                let msg: String = format!("Fail parse decimal:{} {}", self.balance, e);
                log::error!("{}", msg);
                Err(msg)
            }
        }
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
    pub side: String,
    pub stop: String,
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
