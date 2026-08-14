use crate::constants::EXCHANGE;
use anyhow::{Context, Result};
use sqlx::PgPool;

pub struct PositionRepository {
    pool: PgPool,
}

impl PositionRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn upsert_position_ratio(
        &self,
        debt_ratio: f64,
        total_asset: f64,
        margin_coefficient_total_asset: &str,
        total_debt: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO positionratio (
                exchange, debt_ratio, total_asset, 
                margin_coefficient_total_asset, total_debt, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (exchange) 
            DO UPDATE SET
                debt_ratio = EXCLUDED.debt_ratio,
                total_asset = EXCLUDED.total_asset,
                margin_coefficient_total_asset = EXCLUDED.margin_coefficient_total_asset,
                total_debt = EXCLUDED.total_debt,
                updated_at = NOW();
            "#,
        )
        .bind(EXCHANGE)
        .bind(debt_ratio)
        .bind(total_asset)
        .bind(margin_coefficient_total_asset)
        .bind(total_debt)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail upsert positionratio debt_ratio:{} total_asset:{}",
                debt_ratio, total_asset,
            )
        })?;
        Ok(())
    }

    pub async fn upsert_position_debt(&self, debt_symbol: &str, debt_value: &str) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO positiondebt (exchange, debt_symbol, debt_value, updated_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (exchange, debt_symbol) 
            DO UPDATE SET debt_value = EXCLUDED.debt_value, updated_at = NOW();
            "#,
        )
        .bind(EXCHANGE)
        .bind(debt_symbol)
        .bind(debt_value)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail upsert positiondebt debt_symbol:{} debt_value:{}",
                debt_symbol, debt_value,
            )
        })?;
        Ok(())
    }

    pub async fn upsert_position_asset(
        &self,
        asset_symbol: &str,
        asset_total: &str,
        asset_available: &str,
        asset_hold: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO positionasset (
                exchange, asset_symbol, asset_total, asset_available, asset_hold, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (exchange, asset_symbol) 
            DO UPDATE SET
                asset_total = EXCLUDED.asset_total,
                asset_available = EXCLUDED.asset_available,
                asset_hold = EXCLUDED.asset_hold,
                updated_at = NOW();
            "#,
        )
        .bind(EXCHANGE)
        .bind(asset_symbol)
        .bind(asset_total)
        .bind(asset_available)
        .bind(asset_hold)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail upsert positionasset asset_symbol:{} asset_total:{}",
                asset_symbol, asset_total,
            )
        })?;
        Ok(())
    }
}
