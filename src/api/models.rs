use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Deserialize)]
pub struct ApiV3BulletPublicDataInstanceServers {
    pub endpoint: String,
}

#[derive(Debug, Deserialize)]
pub struct ApiV3BulletPublicData {
    pub token: String,
    pub instanceServers: Vec<ApiV3BulletPublicDataInstanceServers>,
}

#[derive(Debug, Deserialize)]
pub struct ApiV3BulletPublic {
    pub code: String,
    pub data: ApiV3BulletPublicData,
}
#[derive(Debug, Deserialize)]
pub struct ApiV3BulletPrivateDataInstanceServers {
    pub endpoint: String,
}

#[derive(Debug, Deserialize)]
pub struct ApiV3BulletPrivateData {
    pub token: String,
    pub instanceServers: Vec<ApiV3BulletPrivateDataInstanceServers>,
}

#[derive(Debug, Deserialize)]
pub struct ApiV3BulletPrivate {
    pub code: String,
    pub data: ApiV3BulletPrivateData,
}
