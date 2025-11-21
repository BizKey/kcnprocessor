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
pub struct MessageData {
    pub id: String,
    pub topic: String,
    #[serde(rename = "userId")]
    pub user_id: String,
    #[serde(rename = "channelType")]
    pub channel_type: String,
    pub subject: String,
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
