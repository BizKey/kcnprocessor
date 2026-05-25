use crate::api::models::{BalanceData, BalanceRelationContext, Bot, OrderData, Symbol};

use log;
use sqlx::Error;
use sqlx::PgPool;
use sqlx::Row;

pub async fn insert_db_error(pool: &PgPool, exchange: &str, msg: &str) -> Result<(), Error> {
    match sqlx::query(
        r#"
        INSERT INTO errors (exchange, msg)
        VALUES ($1, $2);
        "#,
    )
    .bind(exchange)
    .bind(msg)
    .execute(pool)
    .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}
pub async fn insert_db_event(pool: &PgPool, exchange: &str, json_value: serde_json::Value) -> Result<(), Error> {
    match sqlx::query(
        r#"
        INSERT INTO events (exchange, msg)
        VALUES ($1, $2);
        "#,
    )
    .bind(exchange)
    .bind(json_value)
    .execute(pool)
    .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}
pub async fn insert_db_msgsend(
    pool: &PgPool,
    exchange: &str,
    args_symbol: Option<&str>,
    args_side: Option<&str>,
    args_size: Option<&str>,
    args_funds: Option<&str>,
    args_price: Option<&str>,
    args_time_in_force: Option<&str>,
    args_type: Option<&str>,
    args_auto_borrow: Option<&bool>,
    args_auto_repay: Option<&bool>,
    args_client_oid: Option<&str>,
    args_order_id: Option<&str>,
) -> Result<(), Error> {
    match sqlx::query(
        r#"
        INSERT INTO msgsend (exchange, args_symbol, args_side, args_size, args_funds, args_price, args_time_in_force, args_type, args_auto_borrow, args_auto_repay, args_client_oid, args_order_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12);
        "#,
    )
    .bind(exchange)
    .bind(args_symbol)
    .bind(args_side)
    .bind(args_size)
    .bind(args_funds)
    .bind(args_price)
    .bind(args_time_in_force)
    .bind(args_type)
    .bind(args_auto_borrow)
    .bind(args_auto_repay)
    .bind(args_client_oid)
    .bind(args_order_id)
    .execute(pool)
    .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}
pub async fn insert_db_balance(pool: &PgPool, exchange: &str, balance: BalanceData) -> Result<(), Error> {
    let relation_context: BalanceRelationContext = match balance.relation_context {
        Some(ctx) => ctx,
        None => {
            log::error!("Missing relationContext for balance");
            BalanceRelationContext { symbol: None, order_id: None, trade_id: None }
        }
    };
    match sqlx::query(
        r#"
        INSERT INTO balance (exchange, account_id, available, available_change, currency, hold_value, hold_change, relation_event, relation_event_id, event_time, total, symbol, order_id, trade_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14);
        "#,
    )
    .bind(exchange)
    .bind(balance.account_id)
    .bind(balance.available)
    .bind(balance.available_change)
    .bind(balance.currency)
    .bind(balance.hold)
    .bind(balance.hold_change)
    .bind(balance.relation_event)
    .bind(balance.relation_event_id)
    .bind(balance.time)
    .bind(balance.total)
    .bind(relation_context.symbol)
    .bind(relation_context.order_id)
    .bind(relation_context.trade_id)
    .execute(pool)
    .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}

pub async fn insert_db_orderevent(pool: &PgPool, exchange: &str, order: &OrderData) -> Result<(), Error> {
    match sqlx::query(
            r#"
            INSERT INTO orderevent (exchange, status, type_, symbol, side, order_type, fee_type, liquidity, price, order_id, client_oid, trade_id, origin_size, size, filled_size, match_size, match_price, canceled_size, old_size, remain_size, remain_funds, order_time, ts)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23);
            "#)
        .bind(exchange)
        .bind(&order.status)
        .bind(&order.type_)
        .bind(&order.symbol)
        .bind(&order.side)
        .bind(&order.order_type)
        .bind(&order.fee_type)
        .bind(&order.liquidity)
        .bind(&order.price)
        .bind(&order.order_id)
        .bind(&order.client_oid)
        .bind(&order.trade_id)
        .bind(&order.origin_size)
        .bind(&order.size)
        .bind(&order.filled_size)
        .bind(&order.match_size)
        .bind(&order.match_price)
        .bind(&order.canceled_size)
        .bind(&order.old_size)
        .bind(&order.remain_size)
        .bind(&order.remain_funds)
        .bind(order.order_time)
        .bind(order.ts)
        .execute(pool)
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}
pub async fn delete_exit_sl_id_bot_by_client_oid(pool: &sqlx::PgPool, exchange: &str, client_oid: &str) -> Result<(), Error> {
    match sqlx::query(
        r#"
        UPDATE bots
        SET exit_sl_client_oid = NULL,
            exit_sl_order_id = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE exit_sl_client_oid = $1 AND
            exchange = $2;
        "#,
    )
    .bind(client_oid)
    .bind(exchange)
    .execute(pool)
    .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}
pub async fn fetch_symbol_info_by_symbol(pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str, symbol: &str) -> Result<Option<Symbol>, Error> {
    match sqlx::query_as::<_, Symbol>(
        r#"
        SELECT exchange, symbol, base_increment, min_funds, price_increment, quote_increment, base_min_size, quote_min_size
        FROM symbol
        WHERE exchange = $1 AND
            symbol = $2;
        "#,
    )
    .bind(exchange)
    .bind(symbol)
    .fetch_optional(pool)
    .await
    {
        Ok(res) => Ok(res),
        Err(e) => Err(e),
    }
}
pub async fn delete_symbol_bot_by_exit_sl_client_oid(pool: &sqlx::PgPool, exchange: &str, exit_sl_client_oid: &str) -> Result<(), Error> {
    match sqlx::query(
        r#"
        UPDATE bots
        SET symbol = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE exit_sl_client_oid = $1 AND
            exchange = $2;
        "#,
    )
    .bind(exit_sl_client_oid)
    .bind(exchange)
    .execute(pool)
    .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}
pub async fn delete_exit_tp_id_bot_by_client_oid(pool: &sqlx::PgPool, exchange: &str, client_oid: &str) -> Result<(), Error> {
    match sqlx::query(
        r#"
        UPDATE bots
        SET exit_tp_client_oid = NULL,
            exit_tp_order_id = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE exit_tp_client_oid = $1 AND
            exchange = $2;
        "#,
    )
    .bind(client_oid)
    .bind(exchange)
    .execute(pool)
    .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}
pub async fn get_total_match_value_by_client_oid(pool: &sqlx::PgPool, exchange: &str, client_oid: &str) -> Result<Option<String>, Error> {
    match sqlx::query(
        r#"
        SELECT SUM(match_size::numeric * match_price::numeric)::text AS total_match_value
        FROM orderevent
        WHERE client_oid = $1 AND
            exchange = $2 AND
            match_size IS NOT NULL AND
            match_price IS NOT NULL;
        "#,
    )
    .bind(client_oid)
    .bind(exchange)
    .fetch_one(pool)
    .await
    {
        Ok(row) => match row.try_get::<Option<String>, _>("total_match_value") {
            Ok(Some(value_str)) => Ok(Some(value_str)),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        },
        Err(e) => Err(e),
    }
}
pub async fn set_null_entry_client_oid_by_entry_client_oid(pool: &sqlx::PgPool, exchange: &str, client_oid: &str) -> Result<(), Error> {
    match sqlx::query(
        r#"
        UPDATE bots
        SET entry_client_oid = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE entry_client_oid = $1 AND
            exchange = $2;
        "#,
    )
    .bind(client_oid)
    .bind(exchange)
    .execute(pool)
    .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}

pub async fn update_exit_sl_client_oid_bot_by_exit_sl_order_id(pool: &sqlx::PgPool, exchange: &str, exit_sl_order_id: &str, exit_sl_client_oid: &str) -> Result<(), Error> {
    match sqlx::query(
        r#"
        UPDATE bots
        SET exit_sl_client_oid = $1,
            updated_at = CURRENT_TIMESTAMP
        WHERE exit_sl_order_id = $2 AND
            exchange = $3;
        "#,
    )
    .bind(exit_sl_client_oid)
    .bind(exit_sl_order_id)
    .bind(exchange)
    .execute(pool)
    .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}
pub async fn update_exit_tp_client_oid_bot_by_exit_tp_order_id(pool: &sqlx::PgPool, exchange: &str, exit_tp_order_id: &str, exit_tp_client_oid: &str) -> Result<(), Error> {
    match sqlx::query(
        r#"
        UPDATE bots
        SET exit_tp_client_oid = $1,
            updated_at = CURRENT_TIMESTAMP
        WHERE exit_tp_order_id = $2 AND
            exchange = $3;
        "#,
    )
    .bind(exit_tp_client_oid)
    .bind(exit_tp_order_id)
    .bind(exchange)
    .execute(pool)
    .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}
pub async fn update_exit_tp_client_oid_bot_by_entry_client_oid(pool: &sqlx::PgPool, exchange: &str, entry_client_oid: &str, exit_tp_client_oid: &str) -> Result<(), Error> {
    match sqlx::query(
        r#"
        UPDATE bots
        SET exit_tp_client_oid = $1,
            updated_at = CURRENT_TIMESTAMP
        WHERE entry_client_oid = $2 AND
            exchange = $3;
        "#,
    )
    .bind(exit_tp_client_oid)
    .bind(entry_client_oid)
    .bind(exchange)
    .execute(pool)
    .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}
pub async fn update_exit_tp_order_id_bot_by_exit_tp_client_oid(pool: &sqlx::PgPool, exchange: &str, exit_tp_order_id: &str, exit_tp_client_oid: &str) -> Result<(), Error> {
    match sqlx::query(
        r#"
        UPDATE bots
        SET exit_tp_order_id = $1,
            updated_at = CURRENT_TIMESTAMP
        WHERE exit_tp_client_oid = $2 AND
            exchange = $3;
        "#,
    )
    .bind(exit_tp_order_id)
    .bind(exit_tp_client_oid)
    .bind(exchange)
    .execute(pool)
    .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}
pub async fn update_exit_sl_order_id_bot_by_exit_sl_client_oid(pool: &sqlx::PgPool, exchange: &str, exit_sl_order_id: &str, exit_sl_client_oid: &str) -> Result<(), Error> {
    match sqlx::query(
        r#"
        UPDATE bots
        SET exit_sl_order_id = $1,
            updated_at = CURRENT_TIMESTAMP
        WHERE exit_sl_client_oid = $2 AND
            exchange = $3;
        "#,
    )
    .bind(exit_sl_order_id)
    .bind(exit_sl_client_oid)
    .bind(exchange)
    .execute(pool)
    .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}
pub async fn update_exit_sl_client_oid_bot_by_entry_client_oid(pool: &sqlx::PgPool, exchange: &str, entry_client_oid: &str, exit_sl_client_oid: &str) -> Result<(), Error> {
    match sqlx::query(
        r#"
        UPDATE bots
        SET exit_sl_client_oid = $1,
            updated_at = CURRENT_TIMESTAMP
        WHERE entry_client_oid = $2 AND
            exchange = $3;
        "#,
    )
    .bind(exit_sl_client_oid)
    .bind(entry_client_oid)
    .bind(exchange)
    .execute(pool)
    .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}
pub async fn update_balance_bot_by_exit_tp_client_oid(pool: &sqlx::PgPool, exchange: &str, exit_tp_client_oid: &str, balance: &str) -> Result<(), Error> {
    match sqlx::query(
        r#"
        UPDATE bots
        SET balance = $1,
            symbol = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE exit_tp_client_oid = $2 AND
            exchange = $3;
        "#,
    )
    .bind(balance)
    .bind(exit_tp_client_oid)
    .bind(exchange)
    .execute(pool)
    .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}
pub async fn update_bot_balance_by_entry_client_oid(pool: &sqlx::PgPool, exchange: &str, entry_client_oid: &str, balance: &str) -> Result<(), Error> {
    match sqlx::query(
        r#"
        UPDATE bots
        SET balance = $1,
            updated_at = CURRENT_TIMESTAMP
        WHERE entry_client_oid = $2 AND
            exchange = $3;
        "#,
    )
    .bind(balance)
    .bind(entry_client_oid)
    .bind(exchange)
    .execute(pool)
    .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}
pub async fn update_balance_bot_by_exit_sl_client_oid(pool: &sqlx::PgPool, exchange: &str, exit_sl_client_oid: &str, balance: &str) -> Result<(), Error> {
    match sqlx::query(
        r#"
        UPDATE bots
        SET balance = $1,
            symbol = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE exit_sl_client_oid = $2 AND
            exchange = $3;
        "#,
    )
    .bind(balance)
    .bind(exit_sl_client_oid)
    .bind(exchange)
    .execute(pool)
    .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}
pub async fn clear_orders_ids_for_bots(pool: &sqlx::PgPool, exchange: &str, balance: &str) -> Result<(), Error> {
    match sqlx::query(
        r#"
        UPDATE bots
        SET entry_client_oid = NULL,
            exit_tp_order_id = NULL,
            exit_tp_client_oid = NULL,
            exit_sl_order_id = NULL,
            exit_sl_client_oid = NULL,
            balance = $1,
            symbol = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE exchange = $2;
        "#,
    )
    .bind(exchange)
    .bind(balance)
    .execute(pool)
    .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}
pub async fn update_bot_entry_client_oid_by_id(pool: &sqlx::PgPool, exchange: &str, symbol: Option<&str>, entry_client_oid: Option<&str>, trade_bot_id: i32) -> Result<(), Error> {
    match sqlx::query(
        r#"
        UPDATE bots
        SET entry_client_oid = $1,
            symbol = $2
        WHERE exchange = $3 AND
            id = $4;
        "#,
    )
    .bind(entry_client_oid)
    .bind(symbol)
    .bind(exchange)
    .bind(trade_bot_id)
    .execute(pool)
    .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}

pub async fn get_bot_by_exit_sl_client_oid(pool: &PgPool, exchange: &str, client_oid: &str) -> Result<Option<Bot>, Error> {
    match sqlx::query_as::<_, Bot>(
        r#"
        SELECT id, entry_client_oid, exit_tp_order_id, exit_tp_client_oid, exit_sl_order_id, exit_sl_client_oid, balance
        FROM bots
        WHERE exchange = $1 AND
            exit_sl_client_oid = $2
        LIMIT 1;
        "#,
    )
    .bind(exchange)
    .bind(client_oid)
    .fetch_optional(pool)
    .await
    {
        Ok(bot) => Ok(bot),
        Err(e) => Err(e),
    }
}
pub async fn get_bot_by_exit_tp_client_oid(pool: &PgPool, exchange: &str, client_oid: &str) -> Result<Option<Bot>, Error> {
    match sqlx::query_as::<_, Bot>(
        r#"
        SELECT id, entry_client_oid, exit_tp_order_id, exit_tp_client_oid, exit_sl_order_id, exit_sl_client_oid, balance
        FROM bots
        WHERE exchange = $1 AND
            exit_tp_client_oid = $2
        LIMIT 1;
        "#,
    )
    .bind(exchange)
    .bind(client_oid)
    .fetch_optional(pool)
    .await
    {
        Ok(bot) => Ok(bot),
        Err(e) => Err(e),
    }
}
pub async fn get_bot_by_entry_client_oid(pool: &PgPool, exchange: &str, client_oid: &str) -> Result<Option<Bot>, Error> {
    match sqlx::query_as::<_, Bot>(
        r#"
        SELECT id, entry_client_oid, exit_tp_order_id, exit_tp_client_oid, exit_sl_order_id, exit_sl_client_oid, balance
        FROM bots
        WHERE exchange = $1 AND
            entry_client_oid = $2
        LIMIT 1;
        "#,
    )
    .bind(exchange)
    .bind(client_oid)
    .fetch_optional(pool)
    .await
    {
        Ok(bot) => Ok(bot),
        Err(e) => Err(e),
    }
}

pub async fn get_all_bots_for_trade(pool: &PgPool, exchange: &str) -> Result<Vec<Bot>, Error> {
    match sqlx::query_as::<_, Bot>(
        r#"
        SELECT id, entry_client_oid, exit_tp_order_id, exit_tp_client_oid, exit_sl_order_id, exit_sl_client_oid, balance
        FROM bots
        WHERE exchange = $1;
        "#,
    )
    .bind(exchange)
    .fetch_all(pool)
    .await
    {
        Ok(bots) => Ok(bots),
        Err(e) => Err(e),
    }
}

pub async fn get_random_symbol(pool: &PgPool, exchange: &str) -> Result<Option<String>, Error> {
    match sqlx::query_scalar::<_, String>(
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
    .bind(exchange)
    .fetch_optional(pool)
    .await
    {
        Ok(Some(symbol)) => Ok(Some(symbol)),
        Ok(None) => Ok(None),
        Err(e) => Err(e),
    }
}

pub async fn upsert_position_ratio(pool: &PgPool, exchange: &str, debt_ratio: f64, total_asset: f64, margin_coefficient_total_asset: &str, total_debt: &str) -> Result<(), Error> {
    match sqlx::query(
        r#"
        INSERT INTO positionratio (exchange, debt_ratio, total_asset, margin_coefficient_total_asset, total_debt, updated_at)
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
    .bind(exchange)
    .bind(debt_ratio)
    .bind(total_asset)
    .bind(margin_coefficient_total_asset)
    .bind(total_debt)
    .execute(pool)
    .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}

pub async fn upsert_position_debt(pool: &PgPool, exchange: &str, debt_symbol: &str, debt_value: &str) -> Result<(), Error> {
    match sqlx::query(
        r#"
        INSERT INTO positiondebt
        (exchange, debt_symbol, debt_value, updated_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (exchange, debt_symbol) 
        DO UPDATE SET
            debt_value = EXCLUDED.debt_value,
            updated_at = NOW();
        "#,
    )
    .bind(exchange)
    .bind(debt_symbol)
    .bind(debt_value)
    .execute(pool)
    .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}

pub async fn upsert_position_asset(pool: &PgPool, exchange: &str, asset_symbol: &str, asset_total: &str, asset_available: &str, asset_hold: &str) -> Result<(), Error> {
    match sqlx::query(
        r#"
        INSERT INTO positionasset
        (exchange, asset_symbol, asset_total, asset_available, asset_hold, updated_at)
        VALUES ($1, $2, $3, $4, $5, NOW())
        ON CONFLICT (exchange, asset_symbol) 
        DO UPDATE SET
            asset_total = EXCLUDED.asset_total,
            asset_available = EXCLUDED.asset_available,
            asset_hold = EXCLUDED.asset_hold,
            updated_at = NOW();
        "#,
    )
    .bind(exchange)
    .bind(asset_symbol)
    .bind(asset_total)
    .bind(asset_available)
    .bind(asset_hold)
    .execute(pool)
    .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}
pub async fn handle_db_error(pool: &sqlx::PgPool, exchange: &str, msg: String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    log::error!("{}", msg);
    match insert_db_error(pool, exchange, &msg).await {
        Ok(_) => {}
        Err(db_err) => {
            log::error!("Failed to insert error to DB: {} | Original: {}", db_err, msg);
        }
    }
    Err(msg.into())
}
