use crate::api::models::{
    ActiveOrder, BalanceData, BalanceRelationContext, OrderData, Symbol, TradeMsg, TradeMsgData,
    TradeMsgRateLimit,
};
use log::error;
use serde::Serialize;
use sqlx::PgPool;

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
    id_msg: Option<&str>,
    op: Option<&str>,
    args_symbol: Option<&str>,
    args_side: Option<&str>,
    args_size: Option<&str>,
    args_price: Option<&str>,
    args_time_in_force: Option<&str>,
    args_type: Option<&str>,
    args_auto_borrow: Option<&bool>,
    args_auto_repay: Option<&bool>,
    args_client_oid: Option<&str>,
    args_order_id: Option<&str>,
) {
    if let Err(e) = sqlx::query("INSERT INTO msgsend (exchange, id_msg, op, args_symbol, args_side, args_size, args_price, args_time_in_force, args_type, args_auto_borrow, args_auto_repay, args_client_oid, args_order_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);")
            .bind(exchange)
            .bind(id_msg)
            .bind(op)
            .bind(args_symbol)
            .bind(args_side)
            .bind(args_size)
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
pub async fn insert_db_msgevent(pool: &PgPool, exchange: &str, order: &TradeMsg) {
    let order_data = match &order.data {
        Some(ctx) => ctx,
        None => {
            error!("Missing order.data for TradeMsg");
            &TradeMsgData {
                borrow_size: None,
                client_oid: None,
                order_id: None,
                loan_apply_id: None,
            }
        }
    };
    let order_user_rate_limit = match &order.user_rate_limit {
        Some(ctx) => ctx,
        None => {
            error!("Missing user_rate_limit for TradeMsg");
            &TradeMsgRateLimit {
                limit: None,
                reset: None,
                remaining: None,
            }
        }
    };
    if let Err(e) = sqlx::query("INSERT INTO msgevent (exchange, idmsg, op, msg, code, borrow_size, client_oid, order_id, loan_apply_id, limit_rate, reset_rate, remaining_rate, in_time, out_time) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)")
            .bind(exchange)
            .bind(&order.id)
            .bind(&order.op)
            .bind(&order.msg)
            .bind(&order.code)
            .bind(&order_data.borrow_size)
            .bind(&order_data.client_oid)
            .bind(&order_data.order_id)
            .bind(&order_data.loan_apply_id)
            .bind(&order_user_rate_limit.limit)
            .bind(&order_user_rate_limit.reset)
            .bind(&order_user_rate_limit.remaining)
            .bind(&order.in_time)
            .bind(&order.out_time)
            .execute(pool)
            .await
    {
        let err_msg = format!("Failed to insert msg event into DB: {}", e);
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
pub async fn insert_current_orderactive_to_db(pool: &PgPool, exchange: &str, order: &OrderData) {
    if let Err(e) = sqlx::query(
        "INSERT INTO orderactive (exchange, order_id, symbol, side) VALUES ($1, $2, $3, $4)",
    )
    .bind(exchange)
    .bind(&order.order_id)
    .bind(&order.symbol)
    .bind(&order.side)
    .execute(pool)
    .await
    {
        let err_msg = format!("Failed to insert order active into DB: {}", e);
        error!("{}", err_msg);
        insert_db_error(pool, exchange, &err_msg).await;
    }
}
pub async fn delete_all_orderactive_from_db(pool: &sqlx::PgPool, exchange: &str) {
    if let Err(e) = sqlx::query("DELETE FROM orderactive WHERE exchange = $1")
        .bind(exchange)
        .execute(pool)
        .await
    {
        let err_msg = format!("Failed delete all active orders from DB: {}", e);
        error!("{}", err_msg);
        insert_db_error(pool, exchange, &err_msg).await;
    }
}

pub async fn delete_current_orderactive_from_db(pool: &PgPool, exchange: &str, order_id: &str) {
    if let Err(e) = sqlx::query("DELETE FROM orderactive WHERE exchange = $1 AND order_id = $2")
        .bind(exchange)
        .bind(order_id)
        .execute(pool)
        .await
    {
        let err_msg = format!("Failed to delete order from orderactive: {}", e);
        error!("{}", err_msg);
        insert_db_error(pool, "kucoin", &err_msg).await;
    }
}

pub async fn delete_oldest_orderactive(
    pool: &PgPool,
    exchange: &str,
    symbol: &str,
    side: &str,
) -> Option<ActiveOrder> {
    match sqlx::query_as::<_, ActiveOrder>(
        "DELETE FROM orderactive
        WHERE id = (
            SELECT id
            FROM orderactive
            WHERE exchange = $1 AND symbol = $2 AND side = $3
            ORDER BY updated_at ASC
            LIMIT 1
        )
        RETURNING order_id, symbol, updated_at",
    )
    .bind(exchange)
    .bind(symbol)
    .bind(side)
    .fetch_optional(pool)
    .await
    {
        Ok(order) => order,
        Err(e) => {
            let err_msg = format!("Failed delete active orders by symbol '{}': {}", symbol, e);
            error!("{}", err_msg);
            insert_db_error(pool, exchange, &err_msg).await;
            None
        }
    }
}

pub async fn fetch_all_active_orders_by_symbol(
    pool: &PgPool,
    exchange: &str,
    symbol: &str,
    side: &str,
) -> Vec<ActiveOrder> {
    match sqlx::query_as::<_, ActiveOrder>(
        "SELECT order_id, symbol, updated_at FROM orderactive WHERE exchange = $1 AND symbol = $2 AND side = $3",
    )
    .bind(exchange)
    .bind(symbol)
    .bind(side)
    .fetch_all(pool)
    .await
    {
        Ok(orders) => orders,
        Err(e) => {
            let err_msg = format!(
                "Failed to fetch active orders by symbol '{}': {}",
                symbol, e
            );
            error!("{}", err_msg);
            insert_db_error(pool, exchange, &err_msg).await;
            vec![]
        }
    }
}
pub async fn fetch_symbol_info(pool: &PgPool, exchange: &str) -> Vec<Symbol> {
    match sqlx::query_as::<_, Symbol>("SELECT exchange, symbol, base_increment, price_increment, base_min_size FROM symbol WHERE exchange = $1")
        .bind(exchange)
        .fetch_all(pool)
        .await
    {
        Ok(symbols) => symbols,
        Err(e) => {
            let err_msg = format!("Failed to fetch all symbols from symbol: {}", e);
            error!("{}", err_msg);
            insert_db_error(pool, "kucoin", &err_msg).await;
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
