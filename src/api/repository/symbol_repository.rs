use crate::api::models::{Currencies, Symbol};
use crate::constants::EXCHANGE;
use anyhow::{Context, Result};
use sqlx::PgPool;

pub struct SymbolRepository {
    pool: PgPool,
}

impl SymbolRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn get_random_symbol(&self) -> Result<Option<String>> {
        Ok(sqlx::query_scalar::<_, String>(
            r#"
            SELECT s.symbol
            FROM symbol s
            LEFT JOIN (
                SELECT symbol, COUNT(*) as bot_count
                FROM bots
                GROUP BY symbol
            ) b ON s.symbol = b.symbol
            WHERE s.is_margin_enabled = true 
              AND s.enable_trading = true 
              AND s.fee_category = 1 
              AND s.quote_currency = 'USDT' 
              AND s.base_currency <> 'USDC' 
              AND s.base_currency <> 'KCS' 
              AND s.base_currency <> 'ASTER' 
              AND s.exchange = $1
              AND (b.bot_count IS NULL OR b.bot_count < 10)
            ORDER BY RANDOM()
            LIMIT 1;
            "#,
        )
        .bind(EXCHANGE)
        .fetch_optional(&self.pool)
        .await
        .with_context(|| format!("Fail get random symbol by exchange:{}", EXCHANGE))?)
    }

    pub async fn get_symbol_info(&self, symbol: &str) -> Result<Option<Symbol>> {
        Ok(sqlx::query_as::<_, Symbol>(
            r#"
            SELECT exchange, symbol, base_increment, min_funds, 
                   price_increment, quote_increment, base_min_size, quote_min_size
            FROM symbol
            WHERE exchange = $1 AND symbol = $2;
            "#,
        )
        .bind(EXCHANGE)
        .bind(symbol)
        .fetch_optional(&self.pool)
        .await
        .with_context(|| format!("Fail get symbol by symbol:{} exchange:{}", symbol, EXCHANGE))?)
    }

    pub async fn get_currency_info(&self, currency: &str) -> Result<Option<Currencies>> {
        Ok(sqlx::query_as::<_, Currencies>(
            r#"
            SELECT precision
            FROM currency
            WHERE exchange = $1 AND currency = $2;
            "#,
        )
        .bind(EXCHANGE)
        .bind(currency)
        .fetch_optional(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail get currency by currency:{} exchange:{}",
                currency, EXCHANGE,
            )
        })?)
    }
}
