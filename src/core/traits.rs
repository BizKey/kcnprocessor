use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;

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
