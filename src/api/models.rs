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
