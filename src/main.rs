use crate::api::db::{
    clear_orders_ids_for_bots, delete_current_orderactive_from_db, fetch_symbol_info,
    get_all_bots_for_trade, get_random_side, get_random_tradeable_symbol,
    insert_current_orderactive_to_db, insert_db_balance, insert_db_error, insert_db_event,
    insert_db_msgsend, insert_db_orderevent, update_bots_entry_id, upsert_position_asset,
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
        serde_json::json!({"id":"subscribe_stop_orders","type":"subscribe","topic":"/spotMarket/advancedOrders","response":true,"privateChannel":"true"}),
        serde_json::json!({"id":"subscribe_balance","type":"subscribe","topic":"/account/balance","response":true,"privateChannel":"true"}),
        serde_json::json!({"id":"subscribe_position","type":"subscribe","topic":"/margin/position","response":true,"privateChannel":"true"}),
    ]
}

async fn cancel_order(
    pool: &sqlx::Pool<sqlx::Postgres>,
    exchange: &str,
    symbol: &str,
    order_id: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    insert_db_msgsend(
        pool,
        exchange,
        Some(symbol),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(order_id),
    )
    .await;
    // cancel other orders by symbol
    match api::requests::old_cancel_order(order_id).await {
        Ok(_) => {
            info!("Successfully cancel order :{}", &order_id);
        }
        Err(e) => {
            let msg: String = format!("Failed cancel order: {}", e);
            error!("{}", msg);
            insert_db_error(pool, exchange, &msg).await;
        }
    }
    Ok(())
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
        Some(&client_oid),
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

    match api::requests::add_hf_margin_order(msg.clone()).await {
        Ok(data) => {
            if data.code != "200000" {
                let msg = format!(
                    "Make order was error: {} {} {:?}",
                    symbol, data.code, data.msg
                );
                error!("{}", msg);
                insert_db_error(pool, exchange, &msg).await;
                return Err(msg.into());
            } else {
                return Ok(client_oid.to_string());
            }
        }
        Err(e) => {
            let msg: String = format!("Failed to send order: {}", e);
            error!("{}", msg);
            insert_db_error(pool, exchange, &msg).await;
            return Err(msg.into());
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
        Some(&client_oid),
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

    match api::requests::add_hf_margin_order(msg.clone()).await {
        Ok(data) => {
            if data.code != "200000" {
                let msg = format!(
                    "Make order was error: {} {} {:?}",
                    symbol, data.code, data.msg
                );
                error!("{}", msg);
                insert_db_error(pool, exchange, &msg).await;
                return Err(msg.into());
            } else {
                return Ok(client_oid.to_string());
            }
        }
        Err(e) => {
            let msg: String = format!("Failed to send order: {}", e);
            error!("{}", msg);
            insert_db_error(pool, exchange, &msg).await;
            return Err(msg.into());
        }
    }
}

fn format_size(size: f64, increment: f64) -> String {
    let decimals = if increment >= 1.0 {
        0
    } else {
        (-increment.log10().floor() as usize).min(10)
    };
    format!("{:.decimals$}", size)
}

fn calculate_size(notional: f64, price: f64, base_increment: f64, min_size: f64) -> Option<String> {
    if price <= 0.0 || base_increment <= 0.0 {
        return None;
    }

    let raw_size = notional / price;

    let size = (raw_size / base_increment).floor() * base_increment;

    if size < min_size {
        // size is too low
        return None;
    }

    Some(format_size(size, base_increment))
}

fn format_price(price: f64, increment: f64) -> String {
    let decimals = if increment >= 1.0 {
        0
    } else {
        (-increment.log10().floor() as usize).min(10)
    };
    format!("{:.decimals$}", price)
}
fn calculate_transfer_amount(
    match_price: &Option<String>,
    origin_size: &Option<String>,
    increment: &str,
) -> Option<String> {
    let price = match match_price {
        Some(p) => p.parse::<f64>().ok()?,
        None => return None,
    };

    let size = match origin_size {
        Some(s) => s.parse::<f64>().ok()?,
        None => return None,
    };

    let inc = match increment.parse::<f64>() {
        Ok(v) if v > 0.0 => v,
        _ => return None,
    };

    if price <= 0.0 || size <= 0.0 {
        return None;
    }

    let raw_amount = price * size * 0.01;
    let rounded = (raw_amount / inc).round() * inc;

    Some(format_price(rounded, inc))
}
fn calculate_price(
    base_price: &Option<String>,
    increment: &str,
    operation: fn(f64, f64) -> f64,
) -> Option<String> {
    if let Some(match_price) = base_price {
        match (match_price.parse::<f64>(), increment.parse::<f64>()) {
            (Ok(price_num), Ok(inc_num)) if inc_num > 0.0 => {
                let calculated_price = operation(price_num, inc_num);

                let rounded_price = (calculated_price / inc_num).round() * inc_num;

                Some(format_price(rounded_price, inc_num))
            }
            _ => base_price.clone(),
        }
    } else {
        None
    }
}

async fn handle_trade_order_event(
    order: OrderData,
    pool: &sqlx::Pool<sqlx::Postgres>,
    exchange: &str,
    symbol_map: &HashMap<String, Symbol>,
) {
    // sent order to pg
    insert_db_orderevent(pool, exchange, &order).await;

    if order.type_ == "received" {
        // order in order book
        insert_current_orderactive_to_db(pool, exchange, &order).await;
    } else if order.type_ == "canceled" {
        // cancel order
        delete_current_orderactive_from_db(pool, exchange, &order.order_id).await;
    } else if order.type_ == "match" && order.remain_size == Some("0".to_string()) {
        // get last event on match size of position
        // next msg will filled, but it don't have match price
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
        info!("{:?}", position);
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
    for (asset, liability_str) in &position.debt_list {
        if let Ok(liability) = liability_str.parse::<f64>()
            && let Some(asset_info) = &position.asset_list.get(asset)
        {
            if let Ok(available) = asset_info.available.parse::<f64>() {
                if liability > 0.0 {
                    if available >= liability {
                        info!("Position:'{:.?}'", position);
                        info!(
                            "Can repay {} {} liability with available {}",
                            liability, asset, available
                        );

                        if let Err(e) =
                            api::requests::create_repay_order(asset, liability_str).await
                        {
                            let msg: String = format!("Failed to repay liability: {}", e);
                            error!("{}", msg);
                            insert_db_error(pool, exchange, &msg).await;
                        };
                    } else if available > 0.0 {
                        info!("Position:'{:.?}'", position);
                        info!(
                            "Can partially repay {} {} liability with available {}",
                            liability, asset, available
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
                    let liability: f64 = account.liability.parse().unwrap_or(0.0);
                    let available: f64 = account.available.parse().unwrap_or(0.0);
                    if liability > 0.0 {
                        if available >= liability {
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
                        } else if available > 0.0 {
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
                        } else if account.currency != "USDT" && available == 0.0 {
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
                                        return Err(msg.into());
                                    }
                                };

                            let quote_increment = symbol_info.quote_increment.parse::<f64>()?;
                            let funds_f64 = liability.to_string().parse::<f64>()?;
                            let rounded_funds =
                                (funds_f64 / quote_increment).floor() * quote_increment;
                            let rounded_funds_str = format_size(rounded_funds, quote_increment);

                            let min_funds = symbol_info.quote_min_size.parse::<f64>()?;
                            if rounded_funds < min_funds {
                                let msg = format!(
                                    "Size {} below min_funds {} for symbol {}",
                                    rounded_funds, min_funds, trade_symbol
                                );
                                error!("{}", msg);
                                insert_db_error(&pool, &exchange, &msg).await;
                                return Err(msg.into());
                            }

                            make_hf_funds_margin_order(
                                &pool,
                                &exchange,
                                &client_oid,
                                "buy",
                                trade_symbol,
                                rounded_funds_str,
                                "market".to_string(),
                            )
                            .await;
                        }
                        all_asset_clear = false;
                    } else if account.currency != "USDT" && available > 0.0 {
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

                        let base_increment = symbol_info.base_increment.parse::<f64>()?;
                        let size_f64 = available.to_string().parse::<f64>()?;
                        let rounded_size = (size_f64 / base_increment).floor() * base_increment;
                        let rounded_size_str = format_size(rounded_size, base_increment);

                        let min_size = symbol_info.base_min_size.parse::<f64>()?;
                        if rounded_size < min_size {
                            let msg = format!(
                                "Size {} below min_size {} for symbol {}",
                                rounded_size, min_size, trade_symbol
                            );
                            error!("{}", msg);
                            insert_db_error(&pool, &exchange, &msg).await;
                            return Err(msg.into());
                        }

                        make_hf_size_margin_order(
                            &pool,
                            &exchange,
                            &client_oid,
                            "sell",
                            trade_symbol,
                            rounded_size_str,
                            "market".to_string(),
                        )
                        .await;

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
                                info!("{}", &data.data);
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
                                info!("{}", &data.data);

                                match StopOrderData::deserialize(&data.data) {
                                    Ok(order) => {}
                                    Err(e) => {
                                        info!("{:?}", data.data);

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
                                info!("{}", &data.data);
                                // save to db position
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

        loop {
            if !init_order_execute {
                info!("Initializing start orders...");
                let trade_bots = get_all_bots_for_trade(&pool, &exchange).await;

                for trade_bot in trade_bots.iter() {
                    // random sections
                    let random_symbol = get_random_tradeable_symbol(&pool, &exchange).await;

                    let side = get_random_side();
                    // end random sections
                    info!("Choice symbol {} on side {}", random_symbol.clone(), side);
                    // make order
                    let client_oid = Uuid::new_v4().to_string();
                    // save entry_id for bots
                    match update_bots_entry_id(&pool, &exchange, Some(&client_oid), trade_bot.id)
                        .await
                    {
                        Ok(_) => {
                            info!("Update bot info:{} {}", client_oid, trade_bot.id);
                            match make_hf_funds_margin_order(
                                &pool,
                                &exchange,
                                &client_oid,
                                &side,
                                &random_symbol,
                                trade_bot.balance.clone(),
                                "market".to_string(),
                            )
                            .await
                            {
                                Ok(_) => {
                                    // update bots info by
                                }
                                Err(_) => {
                                    // delete if order fail
                                    match update_bots_entry_id(&pool, &exchange, None, trade_bot.id)
                                        .await
                                    {
                                        Ok(_) => {
                                            info!("Update bot info:None {}", trade_bot.id);
                                        }
                                        Err(_) => {}
                                    }
                                }
                            };
                        }
                        Err(e) => {
                            let msg: String = format!("Failed to send order: {}", e);
                            error!("{}", msg);
                            insert_db_error(&pool, &exchange, &msg).await;
                        }
                    }
                }
                init_order_execute = true;
            }
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
