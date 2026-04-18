use crate::api::db::{
    clear_orders_ids_for_bots, delete_entry_id_bot_by_entry_id, delete_exit_sl_id_bot_by_entry_id,
    delete_exit_tp_id_bot_by_entry_id, fetch_symbol_info, get_all_bots_for_trade,
    get_bots_by_entry_id, get_bots_by_exit_sl_id, get_bots_by_exit_tp_id, get_random_side,
    get_random_symbol, get_total_match_value_by_client_oid, insert_db_balance, insert_db_error,
    insert_db_event, insert_db_msgsend, insert_db_orderevent, update_balance_by_entry_id,
    update_balance_by_exit_sl_id, update_balance_by_exit_tp_id, update_bots_entry_id,
    update_exit_sl_id_bot_by_entry_id, update_exit_tp_id_bot_by_entry_id, upsert_position_asset,
    upsert_position_debt, upsert_position_ratio,
};
use crate::api::models::{
    BalanceData, KuCoinMessage, OrderData, PositionData, StopOrderData, Symbol,
};
use dotenv::dotenv;
use futures_util::{SinkExt, StreamExt};
use log::{error, info};
use serde::Deserialize;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::env;
use tokio::sync::mpsc;
use tokio::time::{Duration, interval, sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use uuid::Uuid;
mod api {
    pub mod db;
    pub mod models;
    pub mod requests;
}

const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const CLEAR_DELAY: Duration = Duration::from_secs(10);
const PING_INTERVAL: Duration = Duration::from_secs(5);

fn build_subscription() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({"id":"subscribe_orders","type":"subscribe","topic":"/spotMarket/tradeOrdersV2","response":true,"privateChannel":"true"}),
        // serde_json::json!({"id":"subscribe_stop_orders","type":"subscribe","topic":"/spotMarket/advancedOrders","response":true,"privateChannel":"true"}),
        serde_json::json!({"id":"subscribe_balance","type":"subscribe","topic":"/account/balance","response":true,"privateChannel":"true"}),
        serde_json::json!({"id":"subscribe_position","type":"subscribe","topic":"/margin/position","response":true,"privateChannel":"true"}),
    ]
}

async fn make_hf_funds_margin_order(
    pool: &sqlx::Pool<sqlx::Postgres>,
    exchange: &str,
    client_oid: &str,
    side: &str,
    symbol: &str,
    funds: String,
    type_: String,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    // only for buy orders
    let args_time_in_force = "GTC";
    let auto_borrow = true;
    let auto_repay = true;

    insert_db_msgsend(
        pool,
        exchange,
        Some(symbol),
        Some(side),
        None,
        Some(&funds),
        None,
        Some(args_time_in_force),
        Some(&type_),
        Some(&auto_borrow),
        Some(&auto_repay),
        Some(client_oid),
        None,
    )
    .await;
    let msg = serde_json::json!({
        "clientOid": client_oid,
        "symbol": symbol,
        "side": side,
        "type": type_,
        "autoBorrow": auto_borrow,
        "autoRepay": auto_repay,
        "timeInForce": args_time_in_force,
        "funds": funds
    });
    info!("{}", msg);

    match api::requests::add_api_v3_hf_margin_order(msg.clone()).await {
        Ok(data) => {
            if data.code != "200000" {
                let msg = format!(
                    "Make order was error: {} {} {:?}",
                    symbol, data.code, data.msg
                );
                error!("{}", msg);
                insert_db_error(pool, exchange, &msg).await;
                Err(msg.into())
            } else {
                Ok(client_oid.to_string())
            }
        }
        Err(e) => {
            let msg: String = format!("Failed to send order: {}", e);
            error!("{}", msg);
            insert_db_error(pool, exchange, &msg).await;
            Err(msg.into())
        }
    }
}
async fn fetch_symbol_info_for_symbol(
    pool: &sqlx::Pool<sqlx::Postgres>,
    exchange: &str,
    symbol: &str,
) -> Option<Symbol> {
    sqlx::query_as::<_, Symbol>("SELECT * FROM symbol WHERE exchange = $1 AND symbol = $2")
        .bind(exchange)
        .bind(symbol)
        .fetch_optional(pool)
        .await
        .ok()?
}

async fn make_hf_size_margin_order(
    pool: &sqlx::Pool<sqlx::Postgres>,
    exchange: &str,
    client_oid: &str,
    side: &str,
    symbol: &str,
    size: String,
    type_: String,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    // only for sell orders
    let args_time_in_force = "GTC";
    let auto_borrow = true;
    let auto_repay = true;

    insert_db_msgsend(
        pool,
        exchange,
        Some(symbol),
        Some(side),
        Some(&size),
        None,
        None,
        Some(args_time_in_force),
        Some(&type_),
        Some(&auto_borrow),
        Some(&auto_repay),
        Some(client_oid),
        None,
    )
    .await;
    let msg = serde_json::json!({
        "clientOid": client_oid,
        "symbol": symbol,
        "side": side,
        "type": type_,
        "autoBorrow": auto_borrow,
        "autoRepay": auto_repay,
        "timeInForce": args_time_in_force,
        "size": size
    });
    info!("{}", msg);

    match api::requests::add_api_v3_hf_margin_order(msg.clone()).await {
        Ok(data) => {
            if data.code != "200000" {
                let msg = format!(
                    "Make order was error: {} {} {:?}",
                    symbol, data.code, data.msg
                );
                error!("{}", msg);
                insert_db_error(pool, exchange, &msg).await;
                Err(msg.into())
            } else {
                Ok(client_oid.to_string())
            }
        }
        Err(e) => {
            let msg: String = format!("Failed to send order: {}", e);
            error!("{}", msg);
            insert_db_error(pool, exchange, &msg).await;
            Err(msg.into())
        }
    }
}

fn format_assert(size: f64, increment: f64) -> String {
    let decimals = if increment >= 1.0 {
        0
    } else {
        (-increment.log10().floor() as usize).min(10)
    };
    format!("{:.decimals$}", size)
}

async fn make_random_trade(
    pool: &sqlx::Pool<sqlx::Postgres>,
    exchange: &str,
    balance_funds: f64,
    trade_bot_id: i32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // random sections
    let random_symbol = get_random_symbol(pool, exchange).await;

    match random_symbol {
        Some(tradeable) => {
            // get property of symbol
            let symbol_info =
                match fetch_symbol_info_for_symbol(pool, exchange, &tradeable.symbol).await {
                    Some(info) => info,
                    None => {
                        let msg = format!("Symbol info not found for {}", tradeable.symbol);
                        error!("{}", msg);
                        insert_db_error(pool, exchange, &msg).await;
                        return Ok(());
                    }
                };

            let trade_side: String = get_random_side();
            // end random sections
            info!(
                "Choice symbol {} on side {}",
                tradeable.symbol.clone(),
                trade_side
            );
            // make order
            let client_oid = Uuid::new_v4().to_string();
            // save entry_id for bots
            match update_bots_entry_id(
                pool,
                exchange,
                Some(&tradeable.symbol),
                Some(&client_oid),
                trade_bot_id,
            )
            .await
            {
                Ok(_) => {
                    match trade_side.as_str() {
                        "sell" => {
                            let base_increment: f64 =
                                match symbol_info.base_increment.parse::<f64>() {
                                    Ok(base_increment) => base_increment,
                                    Err(e) => {
                                        let msg: String = format!(
                                            "Failed parse base_increment: {} {}",
                                            symbol_info.base_increment, e
                                        );
                                        error!("{}", msg);
                                        insert_db_error(pool, exchange, &msg).await;
                                        return Ok(());
                                    }
                                };
                            // get price token
                            let token_price_str =
                                match api::requests::get_ticker_price(&tradeable.symbol).await {
                                    Ok(token_price_str) => {
                                        info!("Successfully get price:{}", &tradeable.symbol);
                                        token_price_str
                                    }
                                    Err(e) => {
                                        let msg: String =
                                            format!("Failed get price: {} {}", tradeable.symbol, e);
                                        error!("{}", msg);
                                        insert_db_error(pool, exchange, &msg).await;
                                        return Ok(());
                                    }
                                };
                            // convert price from str to int
                            let token_price: f64 = match token_price_str.parse::<f64>() {
                                Ok(token_price) => token_price,
                                Err(e) => {
                                    let msg: String =
                                        format!("Failed parse price: {} {}", token_price_str, e);
                                    error!("{}", msg);
                                    insert_db_error(pool, exchange, &msg).await;
                                    return Ok(());
                                }
                            };

                            // calc size token
                            let token_size: f64 = balance_funds / token_price;

                            match make_hf_size_margin_order(
                                pool,
                                exchange,
                                &client_oid,
                                &trade_side,
                                &tradeable.symbol,
                                format_assert(token_size, base_increment),
                                "market".to_string(),
                            )
                            .await
                            {
                                Ok(_) => {
                                    info!("Update bot info:{} {}", client_oid, trade_bot_id);
                                }
                                Err(_) => {
                                    // delete if order fail
                                    if update_bots_entry_id(
                                        pool,
                                        exchange,
                                        None,
                                        None,
                                        trade_bot_id,
                                    )
                                    .await
                                    .is_ok()
                                    {
                                        info!("Update bot info:None {}", trade_bot_id);
                                    }
                                }
                            };
                        }
                        "buy" => {
                            // parse quote increment for symbol
                            let quote_increment: f64 =
                                match symbol_info.quote_increment.parse::<f64>() {
                                    Ok(quote_increment) => quote_increment,
                                    Err(e) => {
                                        let msg: String = format!(
                                            "Failed parse quote_increment: {} {}",
                                            symbol_info.quote_increment, e
                                        );
                                        error!("{}", msg);
                                        insert_db_error(pool, exchange, &msg).await;
                                        return Ok(());
                                    }
                                };
                            match make_hf_funds_margin_order(
                                pool,
                                exchange,
                                &client_oid,
                                &trade_side,
                                &tradeable.symbol,
                                format_assert(balance_funds, quote_increment),
                                "market".to_string(),
                            )
                            .await
                            {
                                Ok(_) => {
                                    info!("Update bot info:{} {}", client_oid, trade_bot_id);
                                }
                                Err(_) => {
                                    // delete if order fail
                                    if update_bots_entry_id(
                                        pool,
                                        exchange,
                                        None,
                                        None,
                                        trade_bot_id,
                                    )
                                    .await
                                    .is_ok()
                                    {
                                        info!("Update bot info:None {}", trade_bot_id);
                                    }
                                }
                            };
                        }
                        _ => {
                            return Ok(());
                        }
                    }
                }
                Err(e) => {
                    let msg: String = format!(
                        "Failed save bot info: client_oid:{} trade_bot.id:{}, {}",
                        client_oid, trade_bot_id, e
                    );
                    error!("{}", msg);
                    insert_db_error(pool, exchange, &msg).await;
                    return Ok(());
                }
            }
        }
        None => {
            info!("not exist tradeable symbols")
        }
    }

    Ok(())
}

async fn handle_trade_order_event(
    order: OrderData,
    pool: &sqlx::Pool<sqlx::Postgres>,
    exchange: &str,
    symbol_map: &HashMap<String, Symbol>,
) {
    // sent order to pg
    insert_db_orderevent(pool, exchange, &order).await;

    if let Some(client_oid) = &order.client_oid {
        // client_oid exist
        if (order.type_ == "match" || order.type_ == "canceled")
            && (order.remain_size == Some("0".to_string())
                || order.remain_funds == Some("0".to_string()))
        {
            let symbol_info = match symbol_map.get(&order.symbol) {
                Some(info) => info,
                None => {
                    error!("Symbol info not found for: {}", order.symbol);
                    insert_db_error(
                        pool,
                        exchange,
                        &format!("Missing symbol info for {}", order.symbol),
                    )
                    .await;
                    return;
                }
            };
            let price_increment: f64 = match symbol_info.price_increment.parse::<f64>() {
                Ok(price_increment) => price_increment,
                Err(e) => {
                    let msg: String = format!(
                        "Failed parse price_increment: {} {}",
                        symbol_info.price_increment, e
                    );
                    error!("{}", msg);
                    insert_db_error(pool, exchange, &msg).await;
                    return;
                }
            };
            let quote_increment: f64 = match symbol_info.quote_increment.parse::<f64>() {
                Ok(quote_increment) => quote_increment,
                Err(e) => {
                    let msg: String = format!(
                        "Failed parse quote_increment: {} {}",
                        symbol_info.quote_increment, e
                    );
                    error!("{}", msg);
                    insert_db_error(pool, exchange, &msg).await;
                    return;
                }
            };
            // if clientOid in bots entry_id (2 phase)
            if let Some(bot) = get_bots_by_exit_tp_id(pool, exchange, client_oid).await {
                match get_total_match_value_by_client_oid(pool, exchange, client_oid).await {
                    Some(return_balance) => {
                        if order.side == "buy" {
                            match bot.balance {
                                Some(balance_str) => match balance_str.parse::<f64>() {
                                    Ok(old_balance) => {
                                        let new_balance: f64 =
                                            old_balance + old_balance - return_balance;
                                        update_balance_by_exit_tp_id(
                                            pool,
                                            exchange,
                                            client_oid,
                                            &format!("{:.4}", new_balance),
                                        )
                                        .await;
                                        // create new random order
                                        match make_random_trade(pool, exchange, new_balance, bot.id)
                                            .await
                                        {
                                            Ok(()) => {}
                                            Err(e) => {
                                                let msg: String =
                                                    format!("Error in make_random_trade: {}", e);
                                                error!("{}", msg);
                                                insert_db_error(pool, exchange, &msg).await;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        let msg =
                                            format!("Failed parse balance: {} {}", balance_str, e);
                                        error!("{}", msg);
                                        insert_db_error(pool, exchange, &msg).await;
                                    }
                                },
                                None => {
                                    let msg = format!("Balance is None for {}", exchange);
                                    error!("{}", msg);
                                    insert_db_error(pool, exchange, &msg).await;
                                }
                            }
                        } else if order.side == "sell" {
                            update_balance_by_exit_tp_id(
                                pool,
                                exchange,
                                client_oid,
                                &format!("{:.4}", return_balance),
                            )
                            .await;
                            // create new random order
                            match make_random_trade(pool, exchange, return_balance, bot.id).await {
                                Ok(()) => {}
                                Err(e) => {
                                    let msg: String = format!("Error in make_random_trade: {}", e);
                                    error!("{}", msg);
                                    insert_db_error(pool, exchange, &msg).await;
                                }
                            }
                        }
                    }
                    None => {
                        error!("No records found or error occurred");
                    }
                }
            }
            // if clientOid in bots entry_id (2 phase)
            if let Some(bot) = get_bots_by_exit_sl_id(pool, exchange, client_oid).await {
                match get_total_match_value_by_client_oid(pool, exchange, client_oid).await {
                    Some(return_balance) => {
                        if order.side == "buy" {
                            match bot.balance {
                                Some(balance_str) => match balance_str.parse::<f64>() {
                                    Ok(old_balance) => {
                                        let new_balance: f64 =
                                            old_balance + old_balance - return_balance;
                                        update_balance_by_exit_sl_id(
                                            pool,
                                            exchange,
                                            client_oid,
                                            &format!("{:.4}", new_balance),
                                        )
                                        .await;
                                        // create new random order
                                        match make_random_trade(pool, exchange, new_balance, bot.id)
                                            .await
                                        {
                                            Ok(()) => {}
                                            Err(e) => {
                                                let msg: String =
                                                    format!("Error in make_random_trade: {}", e);
                                                error!("{}", msg);
                                                insert_db_error(pool, exchange, &msg).await;
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        let msg =
                                            format!("Failed parse balance: {} {}", balance_str, e);
                                        error!("{}", msg);
                                        insert_db_error(pool, exchange, &msg).await;
                                    }
                                },
                                None => {
                                    let msg = format!("Balance is None for {}", exchange);
                                    error!("{}", msg);
                                    insert_db_error(pool, exchange, &msg).await;
                                }
                            }
                        } else if order.side == "sell" {
                            update_balance_by_exit_sl_id(
                                pool,
                                exchange,
                                client_oid,
                                &format!("{:.4}", return_balance),
                            )
                            .await;

                            // create new random order
                            match make_random_trade(pool, exchange, return_balance, bot.id).await {
                                Ok(()) => {}
                                Err(e) => {
                                    let msg: String = format!("Error in make_random_trade: {}", e);
                                    error!("{}", msg);
                                    insert_db_error(pool, exchange, &msg).await;
                                }
                            }
                        }
                    }
                    None => {
                        error!("No records found or error occurred");
                    }
                }
            }
            // if clientOid in bots entry_id (1 phase)
            if let Some(bot) = get_bots_by_entry_id(pool, exchange, client_oid).await {
                // delete exit_tp_id stop order
                if let Some(exit_tp_id) = bot.exit_tp_id {
                    // clear exit_tp_id in bots by entry_id
                    delete_exit_tp_id_bot_by_entry_id(pool, exchange, client_oid).await;
                    match api::requests::api_v3_hf_margin_stop_order_cancel_by_client_oid(
                        &exit_tp_id,
                    )
                    .await
                    {
                        Ok(_) => {
                            info!("Successfully cancel stop order :{}", &exit_tp_id);
                        }
                        Err(e) => {
                            let msg: String = format!("Failed cancel stop order: {}", e);
                            error!("{}", msg);
                            insert_db_error(pool, exchange, &msg).await;
                            return;
                        }
                    }
                }

                // delete exit_sl_id stop order
                if let Some(exit_sl_id) = bot.exit_sl_id {
                    // clear exit_sl_id in bots by id !!
                    delete_exit_sl_id_bot_by_entry_id(pool, exchange, client_oid).await;
                    match api::requests::api_v3_hf_margin_stop_order_cancel_by_client_oid(
                        &exit_sl_id,
                    )
                    .await
                    {
                        Ok(_) => {
                            info!("Successfully cancel stop order :{}", &exit_sl_id)
                        }
                        Err(e) => {
                            let msg: String = format!("Failed cancel stop order: {}", e);
                            error!("{}", msg);
                            insert_db_error(pool, exchange, &msg).await;
                            return;
                        }
                    }
                };

                // create new stop tp and sl orders
                if let Some(filled_size) = &order.filled_size {
                    let filled_size_f64: f64 = match filled_size.parse::<f64>() {
                        Ok(filled_size) => filled_size,
                        Err(e) => {
                            let msg: String =
                                format!("Failed parse order.filled_size: {} {}", filled_size, e);
                            error!("{}", msg);
                            insert_db_error(pool, exchange, &msg).await;
                            return;
                        }
                    };
                    match get_total_match_value_by_client_oid(pool, exchange, client_oid).await {
                        Some(new_balance) => {
                            update_balance_by_entry_id(
                                pool,
                                exchange,
                                client_oid,
                                &format!("{:.4}", new_balance),
                            )
                            .await;

                            if order.side == "buy" {
                                let match_price: f64 = new_balance / filled_size_f64;
                                let trigger_tp_price: f64 = match_price * 1.07; // price + 7%
                                let exit_tp_id: String = Uuid::new_v4().to_string();
                                // tp order
                                let msg_tp_order: serde_json::Value = serde_json::json!({
                                    "clientOid": exit_tp_id,
                                    "side": "sell",
                                    "symbol": order.symbol,
                                    "type": "market",
                                    "stop": "entry",
                                    "stopPrice": format_assert(trigger_tp_price, price_increment),
                                    "isIsolated": false,
                                    "autoBorrow": true,
                                    "autoRepay": true,
                                    "size": &order.filled_size,
                                    "timeInForce": "GTC",
                                });
                                info!("Stop profit order:{}", msg_tp_order);
                                // add exit_tp_id by entry_id
                                update_exit_tp_id_bot_by_entry_id(
                                    pool,
                                    exchange,
                                    client_oid,
                                    &exit_tp_id,
                                )
                                .await;
                                match api::requests::api_v3_hf_margin_stop_order(msg_tp_order).await
                                {
                                    Ok(_) => {
                                        info!("Successfully add stop profit order:{}", exit_tp_id);
                                    }
                                    Err(e) => {
                                        let msg: String = format!("Failed add stop order: {}", e);
                                        error!("{}", msg);
                                        insert_db_error(pool, exchange, &msg).await;
                                        // delete exit_tp_id by entry_id
                                        delete_exit_tp_id_bot_by_entry_id(
                                            pool, exchange, client_oid,
                                        )
                                        .await;
                                        return;
                                    }
                                }
                                // sl order
                                let trigger_sl_price: f64 = match_price * 0.95; // price - 5%
                                let exit_sl_id: String = Uuid::new_v4().to_string();
                                let msg_sl_order: serde_json::Value = serde_json::json!({
                                    "clientOid": exit_sl_id,
                                    "side": "sell",
                                    "symbol": order.symbol,
                                    "type": "market",
                                    "stop": "loss",
                                    "stopPrice": format_assert(trigger_sl_price, price_increment),
                                    "isIsolated": false,
                                    "autoBorrow": true,
                                    "autoRepay": true,
                                    "size": order.filled_size,
                                    "timeInForce": "GTC",
                                });
                                info!("Stop loss order:{}", msg_sl_order);
                                // add exit_sl_id by entry_id
                                update_exit_sl_id_bot_by_entry_id(
                                    pool,
                                    exchange,
                                    client_oid,
                                    &exit_sl_id,
                                )
                                .await;
                                match api::requests::api_v3_hf_margin_stop_order(msg_sl_order).await
                                {
                                    Ok(_) => {
                                        info!("Successfully add stop loss order:{}", exit_sl_id);
                                    }
                                    Err(e) => {
                                        let msg: String = format!("Failed add stop order: {}", e);
                                        error!("{}", msg);
                                        insert_db_error(pool, exchange, &msg).await;
                                        delete_exit_sl_id_bot_by_entry_id(
                                            pool, exchange, client_oid,
                                        )
                                        .await;
                                        return;
                                    }
                                }
                            } else if order.side == "sell" {
                                // tp order
                                let match_price: f64 = new_balance / filled_size_f64;
                                let trigger_tp_price: f64 = match_price * 0.93; // price - 7%
                                // !!! check in min_size
                                let funds_buy: f64 = trigger_tp_price * filled_size_f64;
                                let exit_tp_id: String = Uuid::new_v4().to_string();
                                let msg_tp_order: serde_json::Value = serde_json::json!({
                                    "clientOid": exit_tp_id,
                                    "side": "buy",
                                    "symbol": order.symbol,
                                    "type": "market",
                                    "stop": "loss",
                                    "stopPrice": format_assert(trigger_tp_price, price_increment), // price - 7%
                                    "isIsolated": false,
                                    "autoBorrow": true,
                                    "autoRepay": true,
                                    "timeInForce": "GTC",
                                    "funds": format_assert(funds_buy, quote_increment),
                                });
                                info!("Stop profit order:{}", msg_tp_order);
                                // add exit_tp_id by entry_id
                                update_exit_tp_id_bot_by_entry_id(
                                    pool,
                                    exchange,
                                    client_oid,
                                    &exit_tp_id,
                                )
                                .await;
                                match api::requests::api_v3_hf_margin_stop_order(msg_tp_order).await
                                {
                                    Ok(_) => {
                                        info!("Successfully add stop profit order:{}", exit_tp_id);
                                    }
                                    Err(e) => {
                                        let msg: String = format!("Failed add stop order: {}", e);
                                        error!("{}", msg);
                                        insert_db_error(pool, exchange, &msg).await;
                                        // delete exit_tp_id by entry_id
                                        delete_exit_tp_id_bot_by_entry_id(
                                            pool, exchange, client_oid,
                                        )
                                        .await;
                                        return;
                                    }
                                }
                                // sl order
                                let trigger_sl_price: f64 = match_price * 1.05; // price + 5%
                                // !!! check in min_size
                                let funds_buy: f64 = trigger_sl_price * filled_size_f64;
                                let exit_sl_id: String = Uuid::new_v4().to_string();
                                let msg_sl_order: serde_json::Value = serde_json::json!({
                                   "clientOid": exit_sl_id,
                                    "side": "buy",
                                    "symbol": order.symbol,
                                    "type": "market",
                                    "stop": "entry",
                                    "stopPrice": format_assert(trigger_sl_price, price_increment), // price + 5%
                                    "isIsolated": false,
                                    "autoBorrow": true,
                                    "autoRepay": true,
                                    "timeInForce": "GTC",
                                    "funds": format_assert(funds_buy, quote_increment),
                                });
                                info!("Stop loss order:{}", msg_sl_order);
                                // add exit_sl_id by entry_id
                                update_exit_sl_id_bot_by_entry_id(
                                    pool,
                                    exchange,
                                    client_oid,
                                    &exit_sl_id,
                                )
                                .await;
                                match api::requests::api_v3_hf_margin_stop_order(msg_sl_order).await
                                {
                                    Ok(_) => {
                                        info!("Successfully add stop loss order:{}", exit_sl_id);
                                    }
                                    Err(e) => {
                                        let msg: String = format!("Failed add stop order: {}", e);
                                        error!("{}", msg);
                                        insert_db_error(pool, exchange, &msg).await;
                                        // add exit_sl_id by entry_id
                                        delete_exit_sl_id_bot_by_entry_id(
                                            pool, exchange, client_oid,
                                        )
                                        .await
                                    }
                                }
                            }
                        }
                        None => {
                            error!("No records found or error occurred");
                        }
                    }
                    // delete entry_id from db
                    delete_entry_id_bot_by_entry_id(pool, exchange, client_oid).await;
                }
            }
        }
    }
}

async fn handle_position_event(
    position: PositionData,
    pool: &sqlx::Pool<sqlx::Postgres>,
    exchange: &str,
) {
    if let Err(e) = upsert_position_ratio(
        pool,
        exchange,
        position.debt_ratio,
        position.total_asset,
        &position.margin_coefficient_total_asset,
        &position.total_debt,
    )
    .await
    {
        let msg: String = format!("Failed to upsert margin account state: {}", e);
        error!("{}", msg);
        insert_db_error(pool, exchange, &msg).await;
    }
    for (symbol, amount) in &position.debt_list {
        if let Err(e) = upsert_position_debt(pool, exchange, symbol, amount).await {
            let msg: String = format!("Failed to insert debt margin account state: {}", e);
            error!("{}", msg);
            insert_db_error(pool, exchange, &msg).await;
        }
    }
    for (symbol, symbol_info) in &position.asset_list {
        if let Err(e) = upsert_position_asset(
            pool,
            exchange,
            symbol,
            &symbol_info.total,
            &symbol_info.available,
            &symbol_info.hold,
        )
        .await
        {
            let msg: String = format!("Failed to insert asset margin account state: {}", e);
            error!("{}", msg);
            insert_db_error(pool, exchange, &msg).await;
        }
    }
    // repay borrow
    for (asset, token_liability_str) in &position.debt_list {
        if let Ok(token_liability) = token_liability_str.parse::<f64>()
            && let Some(asset_info) = &position.asset_list.get(asset)
        {
            if let Ok(available) = asset_info.available.parse::<f64>() {
                if token_liability > 0.0 {
                    if available >= token_liability {
                        info!(
                            "Can repay {} {} liability with available {}",
                            token_liability, asset, available
                        );

                        if let Err(e) =
                            api::requests::create_repay_order(asset, token_liability_str).await
                        {
                            let msg: String = format!("Failed to repay liability: {}", e);
                            error!("{}", msg);
                            insert_db_error(pool, exchange, &msg).await;
                        };
                    } else if available > 0.0 {
                        info!(
                            "Can partially repay {} {} liability with available {}",
                            token_liability, asset, available
                        );

                        if let Err(e) =
                            api::requests::create_repay_order(asset, &asset_info.available).await
                        {
                            let msg: String = format!("Failed to partially repay debt: {}", e);
                            error!("{}", msg);
                            insert_db_error(pool, exchange, &msg).await;
                        }
                    }
                }
            } else {
                let msg: String = format!("Failed to parse available balance for {}", asset);
                error!("{}", msg);
                insert_db_error(pool, exchange, &msg).await;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();
    dotenv().ok();
    let mut init_order_execute = false;

    let database_url: String = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let exchange: String = "kucoin".to_string();

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await
        .expect("Failed to create pool");

    // clear orders ids for bots
    clear_orders_ids_for_bots(&pool, &exchange).await;

    // cancel all stop orders
    match api::requests::batch_cancel_stop_orders().await {
        Ok(_) => {}
        Err(e) => {
            let msg: String = format!("Failed batch cancel stop orders: {}", e);
            error!("{}", msg);
            insert_db_error(&pool, &exchange, &msg).await;
        }
    }
    // repay all liability assets and sell
    loop {
        let mut all_asset_clear: bool = true;
        match api::requests::get_all_margin_accounts().await {
            Ok(accounts) => {
                for account in accounts.accounts.iter() {
                    let token_liability: f64 = account.liability.parse().unwrap_or(0.0);
                    let token_available: f64 = account.available.parse().unwrap_or(0.0);
                    if token_liability > 0.0 {
                        if token_available >= token_liability {
                            info!(
                                "Can repay {} {} liability with available {}",
                                account.liability, &account.currency, account.available
                            );

                            if let Err(e) = api::requests::create_repay_order(
                                &account.currency,
                                &account.liability,
                            )
                            .await
                            {
                                let msg: String = format!("Failed to repay liability: {}", e);
                                error!("{}", msg);
                                insert_db_error(&pool, &exchange, &msg).await;
                            };
                        } else if token_available > 0.0 {
                            info!(
                                "Can partially repay {} {} liability with available {}",
                                account.liability, &account.currency, account.available
                            );

                            if let Err(e) = api::requests::create_repay_order(
                                &account.currency,
                                &account.available,
                            )
                            .await
                            {
                                let msg: String = format!("Failed to partially repay debt: {}", e);
                                error!("{}", msg);
                                insert_db_error(&pool, &exchange, &msg).await;
                            };
                        } else if account.currency != "USDT" && token_available == 0.0 {
                            // buy stock by market liability
                            let trade_symbol = &(account.currency.clone() + "-USDT");
                            let client_oid = Uuid::new_v4().to_string();
                            let symbol_info =
                                match fetch_symbol_info_for_symbol(&pool, &exchange, trade_symbol)
                                    .await
                                {
                                    Some(info) => info,
                                    None => {
                                        let msg =
                                            format!("Symbol info not found for {}", trade_symbol);
                                        error!("{}", msg);
                                        insert_db_error(&pool, &exchange, &msg).await;
                                        continue;
                                    }
                                };
                            // liability debt in tokens
                            // get price token
                            let token_price_str =
                                match api::requests::get_ticker_price(trade_symbol).await {
                                    Ok(token_price_str) => {
                                        info!("Successfully get price:{}", &trade_symbol);
                                        token_price_str
                                    }
                                    Err(e) => {
                                        let msg: String =
                                            format!("Failed get price: {} {}", trade_symbol, e);
                                        error!("{}", msg);
                                        insert_db_error(&pool, &exchange, &msg).await;
                                        continue;
                                    }
                                };
                            // convert price from str to int
                            let token_price: f64 = match token_price_str.parse::<f64>() {
                                Ok(token_price) => token_price,
                                Err(e) => {
                                    let msg: String =
                                        format!("Failed parse price: {} {}", token_price_str, e);
                                    error!("{}", msg);
                                    insert_db_error(&pool, &exchange, &msg).await;
                                    continue;
                                }
                            };

                            // calc price token on amount liability token
                            let token_funds: f64 = token_price * token_liability;

                            let quote_increment: f64 =
                                match symbol_info.quote_increment.parse::<f64>() {
                                    Ok(quote_increment) => quote_increment,
                                    Err(e) => {
                                        let msg: String = format!(
                                            "Failed parse quote_increment: {} {}",
                                            symbol_info.quote_increment, e
                                        );
                                        error!("{}", msg);
                                        insert_db_error(&pool, &exchange, &msg).await;
                                        continue;
                                    }
                                };
                            // parse quote_min_size to int
                            let quote_min_size: f64 =
                                match symbol_info.quote_min_size.parse::<f64>() {
                                    Ok(quote_min_size) => quote_min_size,
                                    Err(e) => {
                                        let msg: String = format!(
                                            "Failed parse quote_min_size: {} {}",
                                            symbol_info.quote_min_size, e
                                        );
                                        error!("{}", msg);
                                        insert_db_error(&pool, &exchange, &msg).await;
                                        continue;
                                    }
                                };
                            let base_min_size: f64 = match symbol_info.base_min_size.parse::<f64>()
                            {
                                Ok(base_min_size) => base_min_size,
                                Err(e) => {
                                    let msg: String = format!(
                                        "Failed parse base_min_size: {} {}",
                                        symbol_info.base_min_size, e
                                    );
                                    error!("{}", msg);
                                    insert_db_error(&pool, &exchange, &msg).await;
                                    continue;
                                }
                            };
                            // buy min funds or full
                            if token_funds <= quote_min_size || token_liability <= base_min_size {
                                let _ = make_hf_funds_margin_order(
                                    &pool,
                                    &exchange,
                                    &client_oid,
                                    "buy",
                                    trade_symbol,
                                    format_assert(quote_min_size, quote_increment),
                                    "market".to_string(),
                                )
                                .await;
                            } else {
                                let _ = make_hf_funds_margin_order(
                                    &pool,
                                    &exchange,
                                    &client_oid,
                                    "buy",
                                    trade_symbol,
                                    format_assert(token_funds, quote_increment),
                                    "market".to_string(),
                                )
                                .await;
                            }
                        }
                        all_asset_clear = false;
                    } else if account.currency != "USDT" && token_available > 0.0 {
                        // sell stocks by market available/ works
                        let client_oid = Uuid::new_v4().to_string();
                        let trade_symbol = &(account.currency.clone() + "-USDT");
                        let symbol_info = match fetch_symbol_info_for_symbol(
                            &pool,
                            &exchange,
                            trade_symbol,
                        )
                        .await
                        {
                            Some(info) => info,
                            None => {
                                let msg = format!("Symbol info not found for {}", trade_symbol);
                                error!("{}", msg);
                                insert_db_error(&pool, &exchange, &msg).await;
                                return Err(msg.into());
                            }
                        };

                        let base_increment: f64 = match symbol_info.base_increment.parse::<f64>() {
                            Ok(base_increment) => base_increment,
                            Err(e) => {
                                let msg: String = format!(
                                    "Failed parse base_increment: {} {}",
                                    symbol_info.base_increment, e
                                );
                                error!("{}", msg);
                                insert_db_error(&pool, &exchange, &msg).await;
                                continue;
                            }
                        };
                        // get price token
                        let token_price_str =
                            match api::requests::get_ticker_price(trade_symbol).await {
                                Ok(token_price_str) => {
                                    info!("Successfully get price:{}", &trade_symbol);
                                    token_price_str
                                }
                                Err(e) => {
                                    let msg: String =
                                        format!("Failed get price: {} {}", trade_symbol, e);
                                    error!("{}", msg);
                                    insert_db_error(&pool, &exchange, &msg).await;
                                    continue;
                                }
                            };
                        // convert price from str to int
                        let token_price: f64 = match token_price_str.parse::<f64>() {
                            Ok(token_price) => token_price,
                            Err(e) => {
                                let msg: String =
                                    format!("Failed parse price: {} {}", token_price_str, e);
                                error!("{}", msg);
                                insert_db_error(&pool, &exchange, &msg).await;
                                continue;
                            }
                        };

                        let base_min_size: f64 = match symbol_info.base_min_size.parse::<f64>() {
                            Ok(base_min_size) => base_min_size,
                            Err(e) => {
                                let msg: String = format!(
                                    "Failed parse base_min_size: {} {}",
                                    symbol_info.base_min_size, e
                                );
                                error!("{}", msg);
                                insert_db_error(&pool, &exchange, &msg).await;
                                continue;
                            }
                        };
                        let quote_min_size: f64 = match symbol_info.quote_min_size.parse::<f64>() {
                            Ok(quote_min_size) => quote_min_size,
                            Err(e) => {
                                let msg: String = format!(
                                    "Failed parse quote_min_size: {} {}",
                                    symbol_info.quote_min_size, e
                                );
                                error!("{}", msg);
                                insert_db_error(&pool, &exchange, &msg).await;
                                continue;
                            }
                        };
                        if token_available <= base_min_size
                            || token_price * token_available <= quote_min_size
                        {
                            match api::requests::sent_account_transfer(
                                &account.currency.clone(),
                                &account.available,
                                "INTERNAL",
                                "MARGIN",
                                "TRADE",
                            )
                            .await
                            {
                                Ok(_) => {}
                                Err(e) => {
                                    let msg: String = format!(
                                        "Failed send {} to TRADE from MARGIN on {} {}",
                                        &account.currency.clone(),
                                        &account.available,
                                        e
                                    );
                                    error!("{}", msg);
                                    insert_db_error(&pool, &exchange, &msg).await;
                                }
                            }
                        } else {
                            let _ = make_hf_size_margin_order(
                                &pool,
                                &exchange,
                                &client_oid,
                                "sell",
                                trade_symbol,
                                format_assert(token_available, base_increment),
                                "market".to_string(),
                            )
                            .await;
                        }
                        all_asset_clear = false;
                    }
                }
            }
            Err(e) => {
                let msg: String = format!("Failed to get margin accounts {}", e);
                error!("{}", msg);
                insert_db_error(&pool, &exchange, &msg).await;
                // exit with error
                all_asset_clear = false;
            }
        }
        if all_asset_clear {
            break;
        } else {
            sleep(CLEAR_DELAY).await;
        }
    }

    let symbol_info = fetch_symbol_info(&pool, &exchange).await;
    let symbol_map: HashMap<String, Symbol> = symbol_info
        .into_iter()
        .map(|s| (s.symbol.clone(), s))
        .collect();

    loop {
        let exchange_for_handler: String = exchange.clone();
        let pool_for_handler = pool.clone();
        let symbol_map_for_handler = symbol_map.clone();

        // websocket to pg
        let (tx_in, mut rx_in) = mpsc::channel::<String>(1000);

        // Work with income events
        let _ = tokio::spawn(async move {
            while let Some(msg) = rx_in.recv().await {
                match serde_json::from_str::<KuCoinMessage>(&msg) {
                    Ok(kc_msg) => match kc_msg {
                        KuCoinMessage::Welcome(data) => {
                            insert_db_event(&pool_for_handler, &exchange_for_handler, &data).await;
                        }
                        KuCoinMessage::Message(data) => {
                            if data.topic == "/account/balance" {
                                match BalanceData::deserialize(&data.data) {
                                    Ok(balance) => {
                                        // sent balance to pg
                                        insert_db_balance(
                                            &pool_for_handler,
                                            &exchange_for_handler,
                                            balance,
                                        )
                                        .await;
                                    }
                                    Err(e) => {
                                        info!("{:?}", data.data);
                                        // sent balance parse error to pg
                                        let msg: String = format!("Failed to parse message {}", e);
                                        error!("{}", msg);
                                        insert_db_error(
                                            &pool_for_handler,
                                            &exchange_for_handler,
                                            &msg,
                                        )
                                        .await;
                                    }
                                }
                            } else if data.topic == "/spotMarket/advancedOrders" {
                                // stop orders and other
                                match StopOrderData::deserialize(&data.data) {
                                    Ok(_) => {}
                                    Err(e) => {
                                        // info!("{:?}", data.data);

                                        // sent stop order error to pg
                                        let msg: String = format!("Failed to parse message {}", e);
                                        error!("{}", msg);
                                        insert_db_error(
                                            &pool_for_handler,
                                            &exchange_for_handler,
                                            &msg,
                                        )
                                        .await;
                                    }
                                }
                            } else if data.topic == "/spotMarket/tradeOrdersV2" {
                                info!("{}", &data.data);
                                match OrderData::deserialize(&data.data) {
                                    Ok(order) => {
                                        // order magic
                                        handle_trade_order_event(
                                            order,
                                            &pool_for_handler,
                                            &exchange_for_handler,
                                            &symbol_map_for_handler,
                                        )
                                        .await
                                    }
                                    Err(e) => {
                                        info!("{:?}", data.data);

                                        // sent order error to pg
                                        let msg: String = format!("Failed to parse message {}", e);
                                        error!("{}", msg);
                                        insert_db_error(
                                            &pool_for_handler,
                                            &exchange_for_handler,
                                            &msg,
                                        )
                                        .await;
                                    }
                                }
                            } else if data.topic == "/margin/position" {
                                // save to db position
                                // repay debt
                                match PositionData::deserialize(&data.data) {
                                    Ok(position) => {
                                        handle_position_event(
                                            position,
                                            &pool_for_handler,
                                            &exchange_for_handler,
                                        )
                                        .await
                                    }
                                    Err(e) => {
                                        info!("{:?}", data.data);
                                        // sent order error to pg
                                        let msg: String = format!("Failed to parse message {}", e);
                                        error!("{}", msg);
                                        insert_db_error(
                                            &pool_for_handler,
                                            &exchange_for_handler,
                                            &msg,
                                        )
                                        .await;
                                    }
                                }
                            } else {
                                info!("Unknown topic: {}", data.topic);
                                // sent error to pg
                                insert_db_error(
                                    &pool_for_handler,
                                    &exchange_for_handler,
                                    &data.topic,
                                )
                                .await;
                            }
                        }
                        KuCoinMessage::Ack(data) => {
                            info!("{:?}", data);
                            // sent ack to pg
                            insert_db_event(&pool_for_handler, &exchange_for_handler, &data).await;
                        }
                        KuCoinMessage::Error(data) => {
                            info!("{:?}", data);
                            // sent error to pg
                            insert_db_error(&pool_for_handler, &exchange_for_handler, &data.data)
                                .await;
                        }
                    },
                    Err(e) => {
                        // sent error to pg
                        let msg: String = format!("Failed to parse message: {} | Raw: {}", e, msg);
                        error!("{}", msg);
                        insert_db_error(&pool_for_handler, &exchange_for_handler, &msg).await;
                    }
                }
            }
            info!("Message handler finished");
        });

        // Position/Orders/Balance WS
        let event_ws_url: String = match api::requests::get_private_ws_url().await {
            Ok(url) => url,
            Err(e) => {
                error!("Failed to get WebSocket URL: {}", e);
                // sent error to pg
                sleep(RECONNECT_DELAY).await;
                continue;
            }
        };

        let event_ws_stream = match connect_async(event_ws_url).await {
            Ok((stream, _)) => stream,
            Err(e) => {
                error!("WebSocket connection failed: {}", e);
                // sent error to pg
                sleep(RECONNECT_DELAY).await;
                continue;
            }
        };

        let (mut event_ws_write, mut event_ws_read) = event_ws_stream.split();

        // subscribtion
        for sub in build_subscription() {
            if let Err(e) = event_ws_write.send(Message::text(sub.to_string())).await {
                let msg: String = format!("Failed to subscribe: {}", e);
                error!("{}", msg);
                insert_db_error(&pool, &exchange, &msg).await;

                break;
            }
        }

        info!("Subscribed and listening for messages...");

        let event_ping_interval = interval(PING_INTERVAL);
        tokio::pin!(event_ping_interval);

        let mut should_reconnect: bool = false;

        if !init_order_execute {
            init_order_execute = true;
            let pool_clone = pool.clone();
            let exchange_clone = exchange.clone();

            tokio::spawn(async move {
                sleep(Duration::from_millis(5000)).await;
                info!("Initializing start orders...");
                let trade_bots = get_all_bots_for_trade(&pool_clone, &exchange_clone).await;

                for trade_bot in trade_bots.iter() {
                    match trade_bot.balance.parse::<f64>() {
                        Ok(token_funds) => {
                            match make_random_trade(
                                &pool_clone,
                                &exchange_clone,
                                token_funds,
                                trade_bot.id,
                            )
                            .await
                            {
                                Ok(()) => {}
                                Err(e) => {
                                    let msg: String = format!("Error in make_random_trade: {}", e);
                                    error!("{}", msg);
                                    insert_db_error(&pool_clone, &exchange_clone, &msg).await;
                                }
                            }
                            sleep(Duration::from_millis(30000)).await;
                        }
                        Err(e) => {
                            let msg: String =
                                format!("Failed parse balance: {} {}", trade_bot.balance, e);
                            error!("{}", msg);
                            insert_db_error(&pool_clone, &exchange_clone, &msg).await;
                        }
                    }
                }
                info!("All bots initialized!");
            });
        }

        loop {
            tokio::select! {
                // Events
                event_msg = event_ws_read.next() => {
                    match event_msg {
                        Some(Ok(Message::Text(text))) => {
                            if tx_in.send(text.to_string()).await.is_err() {
                                error!("Failed to send to handler, reconnecting...");
                                should_reconnect = true;
                                break;
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = event_ws_write.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Close(close))) => {
                            error!("Connection closed by server: {:?}", close);
                            // sent error to pg
                            should_reconnect = true;
                            break;
                        }
                        Some(Err(e)) => {
                            error!("WebSocket read error: {}", e);
                            // sent error to pg
                            should_reconnect = true;
                            break;
                        }
                        Some(Ok(_)) => {}
                        None => {
                            info!("WebSocket stream ended");
                            // sent error to pg
                            should_reconnect = true;
                            break;
                        }
                    }
                }
                _ = event_ping_interval.tick() => {
                    let _ = event_ws_write.send(Message::Ping(vec![].into())).await;
                }
            }
        }

        drop(tx_in);

        if !should_reconnect {
            break;
        }
        error!("Reconnecting in {} seconds...", RECONNECT_DELAY.as_secs());
        sleep(RECONNECT_DELAY).await;
    }

    Ok(())
}
