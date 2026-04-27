use crate::api::models::{
    BalanceData, BalanceRelationContext, Bots, OrderData, Symbol, TradeAbleSymbol, TradeBot,
};
use fastrand;
use log::error;
use serde::Serialize;
use sqlx::PgPool;
use sqlx::Row;

pub async fn insert_db_error(pool: &PgPool, exchange: &str, msg: &str) {
    if let Err(e) = sqlx::query("INSERT INTO errors (exchange, msg) VALUES ($1, $2)")
        .bind(exchange)
        .bind(msg)
        .execute(pool)
        .await
    {
        error!("Failed to log error to DB: {}", e);
    }
}
pub async fn insert_db_event<T: Serialize>(pool: &PgPool, exchange: &str, msg: &T) {
    let json_value = match serde_json::to_value(msg) {
        Ok(v) => v,
        Err(e) => {
            error!("Failed to serialize event: {}", e);
            return;
        }
    };
    if let Err(e) = sqlx::query("INSERT INTO events (exchange, msg) VALUES ($1, $2)")
        .bind(exchange)
        .bind(json_value)
        .execute(pool)
        .await
    {
        let err_msg = format!("Failed to insert event into DB: {}", e);
        error!("{}", err_msg);
        insert_db_error(pool, exchange, &err_msg).await;
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
) {
    if let Err(e) = sqlx::query("INSERT INTO msgsend (exchange, args_symbol, args_side, args_size, args_funds, args_price, args_time_in_force, args_type, args_auto_borrow, args_auto_repay, args_client_oid, args_order_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12);")
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
        let err_msg = format!("Failed to insert msgsend into DB: {}", e);
        error!("{}", err_msg);
        insert_db_error(pool, exchange, &err_msg).await;
    }
}
pub async fn insert_db_balance(pool: &PgPool, exchange: &str, balance: BalanceData) {
    let relation_context = match balance.relation_context {
        Some(ctx) => ctx,
        None => {
            error!("Missing relationContext for balance");
            BalanceRelationContext {
                symbol: None,
                order_id: None,
                trade_id: None,
            }
        }
    };
    if let Err(e) = sqlx::query("INSERT INTO balance (exchange, account_id, available, available_change, currency, hold_value, hold_change, relation_event, relation_event_id, event_time, total, symbol, order_id, trade_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)")
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
        let err_msg = format!("Failed to insert balance into DB: {}", e);
        error!("{}", err_msg);
        insert_db_error(pool, exchange, &err_msg).await;
    }
}

pub async fn insert_db_orderevent(pool: &PgPool, exchange: &str, order: &OrderData) {
    if let Err(e) = sqlx::query("INSERT INTO orderevent (exchange, status, type_, symbol, side, order_type, fee_type, liquidity, price, order_id, client_oid, trade_id, origin_size, size, filled_size, match_size, match_price, canceled_size, old_size, remain_size, remain_funds, order_time, ts) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)")
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
        let err_msg = format!("Failed to insert order event into DB: {}", e);
        error!("{}", err_msg);
        insert_db_error(pool, exchange, &err_msg).await;
    }
}
pub async fn delete_exit_sl_id_bot_by_entry_id(
    pool: &sqlx::PgPool,
    exchange: &str,
    entry_id: &str,
) {
    if let Err(e) = sqlx::query("UPDATE bots SET exit_sl_id = NULL, updated_at = CURRENT_TIMESTAMP WHERE entry_id = $1 AND exchange = $2;")
        .bind(entry_id)
        .bind(exchange)
        .execute(pool)
        .await
    {
        let err_msg = format!("Failed delete all orders_ids for bots: {}", e);
        error!("{}", err_msg);
        insert_db_error(pool, exchange, &err_msg).await;
    }
}
pub async fn delete_exit_tp_id_bot_by_entry_id(
    pool: &sqlx::PgPool,
    exchange: &str,
    entry_id: &str,
) {
    if let Err(e) = sqlx::query("UPDATE bots SET exit_tp_id = NULL, updated_at = CURRENT_TIMESTAMP WHERE entry_id = $1 AND exchange = $2;")
        .bind(entry_id)
        .bind(exchange)
        .execute(pool)
        .await
    {
        let err_msg = format!("Failed delete all orders_ids for bots: {}", e);
        error!("{}", err_msg);
        insert_db_error(pool, exchange, &err_msg).await;
    }
}
pub async fn get_total_match_value_by_order_id(
    pool: &sqlx::PgPool,
    exchange: &str,
    order_id: &str,
) -> Option<f64> {
    match sqlx::query(
        "SELECT SUM(match_size::numeric * match_price::numeric)::text AS total_match_value FROM orderevent WHERE order_id = $1 AND exchange = $2 AND match_size IS NOT NULL AND match_price IS NOT NULL;"
    )
    .bind(order_id)
    .bind(exchange)
    .fetch_one(pool)
    .await
    {
        Ok(row) => {
            match row.try_get::<Option<String>, _>("total_match_value"){
                Ok(Some(value_str)) => {
                    match value_str.parse::<f64>(){
                        Ok(value) => Some(value),
                        Err(e) => {
                            let err_msg = format!("Failed to parse numeric value '{}' to f64: {}", value_str, e);
                            error!("{}", err_msg);
                            insert_db_error(pool, exchange, &err_msg).await;
                            None
                        }
                    }
                }
                Ok(None) => {
                    None
                }
                Err(e) => {
                    let err_msg = format!("Failed to get total_match_value: {}", e);
                    error!("{}", err_msg);
                    insert_db_error(pool, exchange, &err_msg).await;
                    None
                }
            }
        }
        Err(e) => {
            let err_msg = format!("Failed to get total match value for order_id:{}: {}", order_id, e);
            error!("{}", err_msg);
            insert_db_error(pool, "orderevent", &err_msg).await;
            None
        }
    }
}
pub async fn delete_entry_id_bot_by_entry_id(pool: &sqlx::PgPool, exchange: &str, entry_id: &str) {
    if let Err(e) = sqlx::query("UPDATE bots SET entry_id = NULL, updated_at = CURRENT_TIMESTAMP WHERE entry_id = $1 AND exchange = $2;")
        .bind(entry_id)
        .bind(exchange)
        .execute(pool)
        .await
    {
        let err_msg = format!("Failed delete entry_id:{} by entry_id:{} for bots: {}",entry_id,entry_id, e);
        error!("{}", err_msg);
        insert_db_error(pool, exchange, &err_msg).await;
    }
}

pub async fn update_exit_tp_id_bot_by_entry_id(
    pool: &sqlx::PgPool,
    exchange: &str,
    entry_id: &str,
    exit_tp_id: &str,
) {
    if let Err(e) = sqlx::query("UPDATE bots SET exit_tp_id = $1, updated_at = CURRENT_TIMESTAMP WHERE entry_id = $2 AND exchange = $3;")
        .bind(exit_tp_id)
        .bind(entry_id)
        .bind(exchange)
        .execute(pool)
        .await
    {
        let err_msg = format!("Failed update exit_tp_id:{} by entry_id:{} for bots: {}",exit_tp_id,entry_id, e);
        error!("{}", err_msg);
        insert_db_error(pool, exchange, &err_msg).await;
    }
}
pub async fn update_exit_sl_id_bot_by_entry_id(
    pool: &sqlx::PgPool,
    exchange: &str,
    entry_id: &str,
    exit_sl_id: &str,
) {
    if let Err(e) = sqlx::query("UPDATE bots SET exit_sl_id = $1, updated_at = CURRENT_TIMESTAMP WHERE entry_id = $2 AND exchange = $3;")
        .bind(exit_sl_id)
        .bind(entry_id)
        .bind(exchange)
        .execute(pool)
        .await
    {
        let err_msg = format!("Failed update exit_sl_id:{} by entry_id:{} for bots: {}",exit_sl_id,entry_id, e);
        error!("{}", err_msg);
        insert_db_error(pool, exchange, &err_msg).await;
    }
}
pub async fn update_balance_by_exit_tp_id(
    pool: &sqlx::PgPool,
    exchange: &str,
    exit_tp_id: &str,
    balance: &str,
) {
    if let Err(e) = sqlx::query("UPDATE bots SET balance = $1, symbol = NULL, updated_at = CURRENT_TIMESTAMP WHERE exit_tp_id = $2 AND exchange = $3;")
        .bind(balance)
        .bind(exit_tp_id)
        .bind(exchange)
        .execute(pool)
        .await
    {
        let err_msg = format!("Failed update balance:{} by exit_tp_id:{} for bots: {}",balance,exit_tp_id, e);
        error!("{}", err_msg);
        insert_db_error(pool, exchange, &err_msg).await;
    }
}
pub async fn update_balance_by_entry_id(
    pool: &sqlx::PgPool,
    exchange: &str,
    entry_id: &str,
    balance: &str,
) {
    if let Err(e) = sqlx::query("UPDATE bots SET balance = $1, updated_at = CURRENT_TIMESTAMP WHERE entry_id = $2 AND exchange = $3;")
        .bind(balance)
        .bind(entry_id)
        .bind(exchange)
        .execute(pool)
        .await
    {
        let err_msg = format!("Failed update balance:{} by entry_id:{} for bots: {}",balance,entry_id, e);
        error!("{}", err_msg);
        insert_db_error(pool, exchange, &err_msg).await;
    }
}
pub async fn update_balance_by_exit_sl_id(
    pool: &sqlx::PgPool,
    exchange: &str,
    exit_sl_id: &str,
    balance: &str,
) {
    if let Err(e) = sqlx::query("UPDATE bots SET balance = $1, symbol = NULL, updated_at = CURRENT_TIMESTAMP WHERE exit_sl_id = $2 AND exchange = $3;")
        .bind(balance)
        .bind(exit_sl_id)
        .bind(exchange)
        .execute(pool)
        .await
    {
        let err_msg = format!("Failed update balance:{} by exit_sl_id:{} for bots: {}",balance,exit_sl_id, e);
        error!("{}", err_msg);
        insert_db_error(pool, exchange, &err_msg).await;
    }
}
pub async fn clear_orders_ids_for_bots(pool: &sqlx::PgPool, exchange: &str) {
    if let Err(e) = sqlx::query("UPDATE bots SET entry_client_oid = NULL, exit_tp_order_id = NULL, exit_tp_client_oid = NULL, exit_sl_order_id = NULL, exit_sl_client_oid = NULL, balance = '20', symbol = NULL, updated_at = CURRENT_TIMESTAMP WHERE exchange = $1;")
        .bind(exchange)
        .execute(pool)
        .await
    {
        let err_msg = format!("Failed clear all orders_ids for bots: {}", e);
        error!("{}", err_msg);
        insert_db_error(pool, exchange, &err_msg).await;
    }
}
pub async fn update_bots_entry_id(
    pool: &sqlx::PgPool,
    exchange: &str,
    symbol: Option<&str>,
    entry_id: Option<&str>,
    trade_bot_id: i32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Err(e) =
        sqlx::query("UPDATE bots SET entry_id = $1, symbol = $2 WHERE exchange = $3 AND id = $4;")
            .bind(entry_id)
            .bind(symbol)
            .bind(exchange)
            .bind(trade_bot_id)
            .execute(pool)
            .await
    {
        let err_msg = format!("Failed update bots entry_id: {}", e);
        error!("{}", err_msg);
        insert_db_error(pool, exchange, &err_msg).await;
    }
    Ok(())
}

pub async fn get_bots_by_exit_sl_id(
    pool: &PgPool,
    exchange: &str,
    client_oid: &str,
) -> Option<Bots> {
    match sqlx::query_as::<_, Bots>(
        "SELECT id, entry_id, exit_tp_id, exit_sl_id, balance FROM bots WHERE exchange = $1 AND exit_sl_id = $2 LIMIT 1",
    )
    .bind(exchange)
    .bind(client_oid)
    .fetch_optional(pool)
    .await
    {
        Ok(bot) => bot,
        Err(e) => {
            let err_msg = format!(
                "Failed to fetch bot by exit_sl_id '{}': {}",
                client_oid, e
            );
            error!("{}", err_msg);
            insert_db_error(pool, exchange, &err_msg).await;
            None
        }
    }
}
pub async fn get_bots_by_exit_tp_id(
    pool: &PgPool,
    exchange: &str,
    client_oid: &str,
) -> Option<Bots> {
    match sqlx::query_as::<_, Bots>(
        "SELECT id, entry_id, exit_tp_id, exit_sl_id, balance FROM bots WHERE exchange = $1 AND exit_tp_id = $2 LIMIT 1",
    )
    .bind(exchange)
    .bind(client_oid)
    .fetch_optional(pool)
    .await
    {
        Ok(bot) => bot,
        Err(e) => {
            let err_msg = format!(
                "Failed to fetch bot by exit_tp_id '{}': {}",
                client_oid, e
            );
            error!("{}", err_msg);
            insert_db_error(pool, exchange, &err_msg).await;
            None
        }
    }
}
pub async fn get_bots_by_entry_id(pool: &PgPool, exchange: &str, client_oid: &str) -> Option<Bots> {
    match sqlx::query_as::<_, Bots>(
        "SELECT id, entry_id, exit_tp_id, exit_sl_id, balance FROM bots WHERE exchange = $1 AND entry_id = $2 LIMIT 1",
    )
    .bind(exchange)
    .bind(client_oid)
    .fetch_optional(pool)
    .await
    {
        Ok(bot) => bot,
        Err(e) => {
            let err_msg = format!(
                "Failed to fetch bot by entry_id '{}': {}",
                client_oid, e
            );
            error!("{}", err_msg);
            insert_db_error(pool, exchange, &err_msg).await;
            None
        }
    }
}

pub async fn get_all_bots_for_trade(pool: &PgPool, exchange: &str) -> Vec<TradeBot> {
    match sqlx::query_as::<_, TradeBot>("SELECT id, balance FROM bots WHERE exchange = $1")
        .bind(exchange)
        .fetch_all(pool)
        .await
    {
        Ok(bots) => bots,
        Err(e) => {
            let err_msg = format!("Failed to fetch bots for trade '{}': {}", exchange, e);
            error!("{}", err_msg);
            insert_db_error(pool, exchange, &err_msg).await;
            vec![]
        }
    }
}

pub fn get_random_side() -> String {
    if fastrand::bool() {
        "buy".to_string()
    } else {
        "sell".to_string()
    }
}

pub async fn get_random_symbol(pool: &PgPool, exchange: &str) -> Option<TradeAbleSymbol> {
    match sqlx::query_as::<_, TradeAbleSymbol>(
        "SELECT s.symbol
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
        LIMIT 1;",
    )
    .bind(exchange)
    .fetch_optional(pool)
    .await
    {
        Ok(Some(symbol)) => Some(symbol),
        Ok(None) => {
            error!("No available symbols for exchange: {}", exchange);
            None
        }
        Err(e) => {
            let err_msg = format!("Failed to fetch symbols for trade '{}': {}", exchange, e);
            error!("{}", err_msg);
            insert_db_error(pool, exchange, &err_msg).await;
            None
        }
    }
}
pub async fn fetch_symbol_info(pool: &PgPool, exchange: &str) -> Vec<Symbol> {
    match sqlx::query_as::<_, Symbol>("SELECT * FROM symbol WHERE exchange = $1")
        .bind(exchange)
        .fetch_all(pool)
        .await
    {
        Ok(symbols) => symbols,
        Err(e) => {
            let err_msg = format!("Failed to fetch all symbols from symbol: {}", e);
            error!("{}", err_msg);
            insert_db_error(pool, exchange, &err_msg).await;
            vec![]
        }
    }
}

pub async fn upsert_position_ratio(
    pool: &PgPool,
    exchange: &str,
    debt_ratio: f64,
    total_asset: f64,
    margin_coefficient_total_asset: &str,
    total_debt: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO positionratio 
        (exchange, debt_ratio, total_asset, margin_coefficient_total_asset, total_debt, updated_at)
        VALUES ($1, $2, $3, $4, $5, NOW())
        ON CONFLICT (exchange) 
        DO UPDATE SET
            debt_ratio = EXCLUDED.debt_ratio,
            total_asset = EXCLUDED.total_asset,
            margin_coefficient_total_asset = EXCLUDED.margin_coefficient_total_asset,
            total_debt = EXCLUDED.total_debt,
            updated_at = NOW()
        "#,
    )
    .bind(exchange)
    .bind(debt_ratio)
    .bind(total_asset)
    .bind(margin_coefficient_total_asset)
    .bind(total_debt)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn upsert_position_debt(
    pool: &PgPool,
    exchange: &str,
    debt_symbol: &str,
    debt_value: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO positiondebt
        (exchange, debt_symbol, debt_value, updated_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (exchange, debt_symbol) 
        DO UPDATE SET
            debt_value = EXCLUDED.debt_value,
            updated_at = NOW()
        "#,
    )
    .bind(exchange)
    .bind(debt_symbol)
    .bind(debt_value)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn upsert_position_asset(
    pool: &PgPool,
    exchange: &str,
    asset_symbol: &str,
    asset_total: &str,
    asset_available: &str,
    asset_hold: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO positionasset
        (exchange, asset_symbol, asset_total, asset_available, asset_hold, updated_at)
        VALUES ($1, $2, $3, $4, $5, NOW())
        ON CONFLICT (exchange, asset_symbol) 
        DO UPDATE SET
            asset_total = EXCLUDED.asset_total,
            asset_available = EXCLUDED.asset_available,
            asset_hold = EXCLUDED.asset_hold,
            updated_at = NOW()
        "#,
    )
    .bind(exchange)
    .bind(asset_symbol)
    .bind(asset_total)
    .bind(asset_available)
    .bind(asset_hold)
    .execute(pool)
    .await?;

    Ok(())
}
