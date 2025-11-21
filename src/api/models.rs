use serde::{Deserialize, Serialize};
use tokio_tungstenite::tungstenite::Message;

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
    pub symbol: String,
    #[serde(rename = "orderId")]
    pub order_id: String,
    #[serde(rename = "tradeId")]
    pub trade_id: String,
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
    pub relationContext: Option<BalanceRelationContext>,
}
#[derive(Debug, Deserialize, Serialize)]
pub struct OrderData {
    pub status: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub symbol: String,
    pub side: String,
    #[serde(rename = "orderType")]
    pub order_type: String,
    #[serde(rename = "feeType")]
    pub fee_type: String,
    pub liquidity: String,
    pub price: String,
    #[serde(rename = "orderId")]
    pub order_id: String,
    #[serde(rename = "clientOid")]
    pub client_oid: String,
    #[serde(rename = "tradeId")]
    pub trade_id: String,
    #[serde(rename = "originSize")]
    pub origin_size: String,
    pub size: String,
    #[serde(rename = "filledSize")]
    pub filled_size: String,
    #[serde(rename = "matchSize")]
    pub match_size: String,
    #[serde(rename = "matchPrice")]
    pub match_price: String,
    #[serde(rename = "canceledSize")]
    pub canceled_size: String,
    #[serde(rename = "oldSize")]
    pub old_size: String,
    #[serde(rename = "remainSize")]
    pub remain_size: String,
    #[serde(rename = "remainFunds")]
    pub remain_funds: String,
    #[serde(rename = "orderTime")]
    pub order_time: String,
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
#[serde(tag = "type")]
pub enum KuCoinMessage {
    #[serde(rename = "welcome")]
    Welcome(WelcomeData),

    #[serde(rename = "ack")]
    Ack(AckData),

    #[serde(rename = "message")]
    Message(MessageData),
}
