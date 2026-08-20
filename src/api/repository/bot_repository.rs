use crate::api::models::Bot;
use crate::constants::EXCHANGE;
use anyhow::{Context, Result};
use sqlx::PgPool;

#[derive(Clone)]
pub struct BotRepository {
    pool: PgPool,
}

impl BotRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn get_by_client_oid(&self, client_oid: &str) -> Result<Option<Bot>> {
        sqlx::query_as::<_, Bot>(
            r#"
            SELECT id, entry_client_oid, entry_price, exit_tp_price, 
                   exit_tp_order_id, exit_tp_client_oid, exit_sl_price, 
                   exit_sl_order_id, exit_sl_client_oid, balance
            FROM bots
            WHERE exchange = $1 AND (
                entry_client_oid = $2 OR 
                exit_tp_client_oid = $2 OR 
                exit_sl_client_oid = $2
            )
            LIMIT 1;
            "#,
        )
        .bind(EXCHANGE)
        .bind(client_oid)
        .fetch_optional(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail get bot by client_oid:{} exchange:{}",
                client_oid, EXCHANGE
            )
        })
    }

    pub async fn get_by_entry_client_oid(&self, entry_client_oid: &str) -> Result<Option<Bot>> {
        sqlx::query_as::<_, Bot>(
            r#"
            SELECT id, entry_client_oid, entry_price, exit_tp_price, 
                   exit_tp_order_id, exit_tp_client_oid, exit_sl_price, 
                   exit_sl_order_id, exit_sl_client_oid, balance
            FROM bots
            WHERE exchange = $1 AND entry_client_oid = $2
            LIMIT 1;
            "#,
        )
        .bind(EXCHANGE)
        .bind(entry_client_oid)
        .fetch_optional(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail get bot by entry_client_oid:{} exchange:{}",
                entry_client_oid, EXCHANGE
            )
        })
    }

    pub async fn get_by_exit_tp_client_oid(&self, exit_tp_client_oid: &str) -> Result<Option<Bot>> {
        sqlx::query_as::<_, Bot>(
            r#"
            SELECT id, entry_client_oid, entry_price, exit_tp_price, 
                   exit_tp_order_id, exit_tp_client_oid, exit_sl_price, 
                   exit_sl_order_id, exit_sl_client_oid, balance
            FROM bots
            WHERE exchange = $1 AND exit_tp_client_oid = $2
            LIMIT 1;
            "#,
        )
        .bind(EXCHANGE)
        .bind(exit_tp_client_oid)
        .fetch_optional(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail get bot by exit_tp_client_oid:{} exchange:{}",
                exit_tp_client_oid, EXCHANGE
            )
        })
    }

    pub async fn get_by_exit_sl_client_oid(&self, exit_sl_client_oid: &str) -> Result<Option<Bot>> {
        sqlx::query_as::<_, Bot>(
            r#"
            SELECT id, entry_client_oid, entry_price, exit_tp_price, 
                   exit_tp_order_id, exit_tp_client_oid, exit_sl_price, 
                   exit_sl_order_id, exit_sl_client_oid, balance
            FROM bots
            WHERE exchange = $1 AND exit_sl_client_oid = $2
            LIMIT 1;
            "#,
        )
        .bind(EXCHANGE)
        .bind(exit_sl_client_oid)
        .fetch_optional(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail get bot by exit_sl_client_oid:{} exchange:{}",
                exit_sl_client_oid, EXCHANGE
            )
        })
    }

    pub async fn get_all(&self) -> Result<Vec<Bot>> {
        sqlx::query_as::<_, Bot>(
            r#"
            SELECT id, entry_client_oid, entry_price, exit_tp_price, 
                   exit_tp_order_id, exit_tp_client_oid, exit_sl_price, 
                   exit_sl_order_id, exit_sl_client_oid, balance
            FROM bots
            WHERE exchange = $1;
            "#,
        )
        .bind(EXCHANGE)
        .fetch_all(&self.pool)
        .await
        .with_context(|| format!("Fail get bots by exchange:{}", EXCHANGE))
    }

    pub async fn update_entry_client_oid_by_id(
        &self,
        entry_client_oid: Option<&str>,
        id: i32,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE bots
            SET entry_client_oid = $1
            WHERE exchange = $2 AND id = $3;
            "#,
        )
        .bind(entry_client_oid)
        .bind(EXCHANGE)
        .bind(id)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail update entry_client_oid:{:?} by id:{} exchange:{}",
                entry_client_oid, id, EXCHANGE,
            )
        })?;
        Ok(())
    }

    pub async fn update_exit_tp_client_oid_by_entry_client_oid(
        &self,
        entry_client_oid: &str,
        symbol: &str,
        exit_tp_client_oid: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE bots
            SET exit_tp_client_oid = $1, symbol = $2, updated_at = CURRENT_TIMESTAMP
            WHERE entry_client_oid = $3 AND exchange = $4;
            "#,
        )
        .bind(exit_tp_client_oid)
        .bind(symbol)
        .bind(entry_client_oid)
        .bind(EXCHANGE)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail update exit_tp_client_oid:{} by entry_client_oid:{} and exchange:{}",
                exit_tp_client_oid, entry_client_oid, EXCHANGE,
            )
        })?;
        Ok(())
    }

    pub async fn update_exit_sl_client_oid_by_entry_client_oid(
        &self,
        entry_client_oid: &str,
        symbol: &str,
        exit_sl_client_oid: &str,
        sl_stop_price: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE bots
            SET symbol = $3, exit_sl_client_oid = $4, exit_tp_price = $5 updated_at = CURRENT_TIMESTAMP
            WHERE entry_client_oid = $1 AND exchange = $2;
            "#,
        )
        .bind(entry_client_oid)
        .bind(EXCHANGE)
        .bind(symbol)
        .bind(exit_sl_client_oid)
        .bind(sl_stop_price)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail update exit_sl_client_oid:{} by entry_client_oid:{} exchange:{}",
                exit_sl_client_oid, entry_client_oid, EXCHANGE,
            )
        })?;
        Ok(())
    }

    pub async fn update_exit_tp_order_id_by_client_oid(
        &self,
        exit_tp_order_id: &str,
        exit_tp_client_oid: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE bots
            SET exit_tp_order_id = $1, updated_at = CURRENT_TIMESTAMP
            WHERE exit_tp_client_oid = $2 AND exchange = $3;
            "#,
        )
        .bind(exit_tp_order_id)
        .bind(exit_tp_client_oid)
        .bind(EXCHANGE)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail update exit_tp_order_id:{} by exit_tp_client_oid:{} and exchange:{}",
                exit_tp_order_id, exit_tp_client_oid, EXCHANGE,
            )
        })?;
        Ok(())
    }

    pub async fn update_exit_sl_order_id_by_client_oid(
        &self,
        exit_sl_order_id: &str,
        exit_sl_client_oid: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE bots
            SET exit_sl_order_id = $1, updated_at = CURRENT_TIMESTAMP
            WHERE exit_sl_client_oid = $2 AND exchange = $3;
            "#,
        )
        .bind(exit_sl_order_id)
        .bind(exit_sl_client_oid)
        .bind(EXCHANGE)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail update exit_sl_order_id:{} bot by exit_sl_client_oid:{} and exchange:{}",
                exit_sl_order_id, exit_sl_client_oid, EXCHANGE,
            )
        })?;
        Ok(())
    }

    pub async fn update_exit_tp_client_oid_by_order_id(
        &self,
        exit_tp_order_id: &str,
        exit_tp_client_oid: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE bots
            SET exit_tp_client_oid = $1, updated_at = CURRENT_TIMESTAMP
            WHERE exit_tp_order_id = $2 AND exchange = $3;
            "#,
        )
        .bind(exit_tp_client_oid)
        .bind(exit_tp_order_id)
        .bind(EXCHANGE)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail update exit_tp_client_oid:{} for bot by exit_tp_order_id:{} exchange:{}",
                exit_tp_client_oid, exit_tp_order_id, EXCHANGE,
            )
        })?;
        Ok(())
    }

    pub async fn update_exit_sl_client_oid_by_order_id(
        &self,
        exit_sl_order_id: &str,
        exit_sl_client_oid: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE bots
            SET exit_sl_client_oid = $1, updated_at = CURRENT_TIMESTAMP
            WHERE exit_sl_order_id = $2 AND exchange = $3;
            "#,
        )
        .bind(exit_sl_client_oid)
        .bind(exit_sl_order_id)
        .bind(EXCHANGE)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail update exit_sl_client_oid:{} for bot by exit_sl_order_id:{} exchange:{}",
                exit_sl_client_oid, exit_sl_order_id, EXCHANGE,
            )
        })?;
        Ok(())
    }

    pub async fn clear_entry_client_oid(&self, entry_client_oid: &str) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE bots
            SET entry_client_oid = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE entry_client_oid = $1 AND exchange = $2;
            "#,
        )
        .bind(entry_client_oid)
        .bind(EXCHANGE)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail clear entry_client_oid:{} for bot exchange:{}",
                entry_client_oid, EXCHANGE,
            )
        })?;
        Ok(())
    }

    pub async fn clear_exit_tp_by_client_oid(&self, exit_tp_client_oid: &str) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE bots
            SET exit_tp_client_oid = NULL, exit_tp_order_id = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE exit_tp_client_oid = $1 AND exchange = $2;
            "#,
        )
        .bind(exit_tp_client_oid)
        .bind(EXCHANGE)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail clear exit_tp_client_oid:{} for bot exchange:{}",
                exit_tp_client_oid, EXCHANGE,
            )
        })?;
        Ok(())
    }

    pub async fn clear_exit_sl_by_client_oid(&self, exit_sl_client_oid: &str) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE bots
            SET exit_sl_client_oid = NULL, exit_sl_order_id = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE exit_sl_client_oid = $1 AND exchange = $2;
            "#,
        )
        .bind(exit_sl_client_oid)
        .bind(EXCHANGE)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail clear exit_sl_client_oid:{} for bot exchange:{}",
                exit_sl_client_oid, EXCHANGE,
            )
        })?;
        Ok(())
    }

    pub async fn update_balance_by_entry_client_oid(
        &self,
        entry_client_oid: &str,
        balance: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE bots
            SET balance = $1, updated_at = CURRENT_TIMESTAMP
            WHERE entry_client_oid = $2 AND exchange = $3;
            "#,
        )
        .bind(balance)
        .bind(entry_client_oid)
        .bind(EXCHANGE)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail update balance bot to:{} by entry_client_oid:{} exchange:{}",
                balance, entry_client_oid, EXCHANGE,
            )
        })?;
        Ok(())
    }

    pub async fn update_balance_and_clear_symbol_by_exit_tp(
        &self,
        exit_tp_client_oid: &str,
        balance: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE bots
            SET balance = $1, symbol = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE exit_tp_client_oid = $2 AND exchange = $3;
            "#,
        )
        .bind(balance)
        .bind(exit_tp_client_oid)
        .bind(EXCHANGE)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail update balance to:{} by exit_tp_client_oid:{} exchange:{}",
                balance, exit_tp_client_oid, EXCHANGE,
            )
        })?;
        Ok(())
    }

    pub async fn update_balance_and_clear_symbol_by_exit_sl(
        &self,
        exit_sl_client_oid: &str,
        balance: &str,
    ) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE bots
            SET balance = $1, symbol = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE exit_sl_client_oid = $2 AND exchange = $3;
            "#,
        )
        .bind(balance)
        .bind(exit_sl_client_oid)
        .bind(EXCHANGE)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail update balance:{} and symbol:NULL bot by exit_sl_client_oid:{} exchange:{}",
                balance, exit_sl_client_oid, EXCHANGE
            )
        })?;
        Ok(())
    }

    pub async fn clear_all_bots(&self, balance: &str) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE bots
            SET entry_price = NULL,
                entry_client_oid = NULL,
                exit_tp_price = NULL,
                exit_tp_order_id = NULL,
                exit_tp_client_oid = NULL,
                exit_sl_price = NULL,
                exit_sl_order_id = NULL,
                exit_sl_client_oid = NULL,
                balance = $1,
                symbol = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE exchange = $2;
            "#,
        )
        .bind(balance)
        .bind(EXCHANGE)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail clear all bots, balance:{}, exchange:{}",
                balance, EXCHANGE,
            )
        })?;
        Ok(())
    }

    pub async fn clear_symbol_by_exit_sl_client_oid(&self, exit_sl_client_oid: &str) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE bots
            SET symbol = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE exit_sl_client_oid = $1 AND exchange = $2;
            "#,
        )
        .bind(exit_sl_client_oid)
        .bind(EXCHANGE)
        .execute(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Fail clear symbol by exit_sl_client_oid:{} exchange:{}",
                exit_sl_client_oid, EXCHANGE,
            )
        })?;
        Ok(())
    }
}
