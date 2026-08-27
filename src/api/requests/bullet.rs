use crate::api::client::endpoints::KuCoinEndpoints;
use crate::api::models::ApiV3BulletPrivate;
use anyhow::Result;

pub async fn bullet_private_post(client: &KuCoinEndpoints) -> Result<String> {
    let response_string: String = client.bullet_private_post().await?;
    let response = serde_json::from_str::<ApiV3BulletPrivate>(&response_string)?;

    let ws = if response.code.as_str() == "200000" {
        response.data
    } else {
        anyhow::bail!(
            "KuCoin API error /api/v1/bullet-private: code={}, msg={:?}, data={:?}",
            response.code,
            response.msg,
            response.data
        )
    };

    let server = match ws {
        Some(ws) => ws,
        None => anyhow::bail!("No data in bullet response"),
    };

    let instance = match server.instance_servers.first() {
        Some(instance) => instance,
        None => anyhow::bail!("No instance servers in bullet response"),
    };

    Ok(format!("{}?token={}", instance.endpoint, server.token))
}
