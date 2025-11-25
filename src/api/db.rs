use crate::api::models::{BalanceData, BalanceRelationContext, OrderData};
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
pub async fn insert_db_balance(pool: &PgPool, exchange: &str, balance: BalanceData) {
    let relation_context = match balance.relationContext {
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
    if let Err(e) = sqlx::query("INSERT INTO balance (exchange, account_id, available, available_change, currency, hold, hold_change, relation_event, relation_event_id, time, total, symbol, order_id, trade_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)")
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
    if let Err(e) = sqlx::query("INSERT INTO orderevents (exchange, status, type_, symbol, side, order_type, fee_type, liquidity, price, order_id, client_oid, trade_id, origin_size, size, filled_size, match_size, match_price, canceled_size, old_size, remain_size, remain_funds, order_time, ts) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)")
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
            .bind(&order.order_time)
            .bind(&order.ts)
            .execute(pool)
            .await
    {
        let err_msg = format!("Failed to insert order event into DB: {}", e);
        error!("{}", err_msg);
        insert_db_error(pool, exchange, &err_msg).await;
    }
}
pub async fn insert_db_orderactive(pool: &PgPool, exchange: &str, order: &OrderData) {
    if let Err(e) =
        sqlx::query("INSERT INTO orderactive (exchange, order_id, symbol) VALUES ($1, $2, $3)")
            .bind(exchange)
            .bind(&order.order_id)
            .bind(&order.symbol)
            .execute(pool)
            .await
    {
        let err_msg = format!("Failed to insert order active into DB: {}", e);
        error!("{}", err_msg);
        insert_db_error(pool, exchange, &err_msg).await;
    }
}
