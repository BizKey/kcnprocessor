use crate::api::db::{
    delete_all_orderactive_from_db, delete_current_orderactive_from_db, delete_oldest_orderactive,
    fetch_all_active_orders_by_symbol, fetch_symbol_info, get_all_symbol_for_trade,
    insert_current_orderactive_to_db, insert_db_balance, insert_db_error, insert_db_event,
    insert_db_msgsend, insert_db_orderevent, upsert_position_asset, upsert_position_debt,
    upsert_position_ratio,
};
use crate::api::models::{BalanceData, KuCoinMessage, OrderData, PositionData, Symbol};
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
const REPAY_DELAY: Duration = Duration::from_secs(5);
const PING_INTERVAL: Duration = Duration::from_secs(5);

fn build_subscription() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({"id":"subscribe_orders","type":"subscribe","topic":"/spotMarket/tradeOrdersV2","response":true,"privateChannel":"true"}),
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
        Some(&symbol),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(&order_id),
    )
    .await;
    // cancel other orders by symbol
    let msg = serde_json::json!({
        "symbol": symbol,
        "orderId": order_id
    });
    // make cancel order
    // if let Err(e) = tx_out.send(msg.to_string()).await {
    //     error!("Failed to send order: {}", e);
    //     insert_db_error(pool, exchange, &e.to_string()).await;
    //     return Err(e.into());
    // }
    Ok(())
}

async fn make_order(
    pool: &sqlx::Pool<sqlx::Postgres>,
    exchange: &str,
    side: &str,
    symbol: &str,
    price: String,
    size: String,
) {
    let args_time_in_force = "GTC";
    let type_ = "limit";
    let auto_borrow = true;
    let auto_repay = true;
    let client_oid = Uuid::new_v4().to_string();

    insert_db_msgsend(
        pool,
        exchange,
        Some(&symbol),
        Some(&side),
        Some(&size),
        Some(&price),
        Some(&args_time_in_force),
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
        "price": price,
        "autoBorrow": auto_borrow,
        "autoRepay": auto_repay,
        "timeInForce": args_time_in_force,
        "size": size
    });
    match api::requests::add_order(msg).await {
        Ok(_) => {}
        Err(e) => {
            error!("Failed to send order: {}", e);
            insert_db_error(pool, exchange, &e.to_string()).await;
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
async fn create_order_safely(
    pool: &sqlx::Pool<sqlx::Postgres>,
    exchange: &str,
    side: &str,
    symbol: &str,
    price_str: &str,
    size_option: Option<&str>,
    symbol_info: &Symbol,
) {
    let price_f64 = match price_str.parse::<f64>() {
        Ok(v) if v > 0.0 => v,
        _ => {
            let msg = format!("Invalid price '{}' for symbol {}", price_str, symbol);
            error!("{}", msg);
            insert_db_error(pool, exchange, &msg).await;
            return;
        }
    };

    let base_increment = match symbol_info.base_increment.parse::<f64>() {
        Ok(v) if v > 0.0 => v,
        _ => {
            let msg = format!(
                "Invalid base_increment '{}' for symbol {}",
                symbol_info.base_increment, symbol,
            );
            error!("{}", msg);
            insert_db_error(pool, exchange, &msg).await;
            return;
        }
    };

    let min_size = match symbol_info.base_min_size.parse::<f64>() {
        Ok(v) if v > 0.0 => v,
        _ => {
            let msg = format!(
                "Invalid min_size '{}' for symbol {}",
                symbol_info.base_min_size, symbol
            );
            error!("{}", msg);
            insert_db_error(pool, exchange, &msg).await;
            return;
        }
    };

    let size_str = match size_option {
        Some(size_str) => match size_str.parse::<f64>() {
            Ok(size_f64) => {
                if size_f64 >= min_size {
                    let rounded_size = (size_f64 / base_increment).floor() * base_increment;
                    format_size(rounded_size, base_increment)
                } else {
                    let msg = format!(
                        "Size {} below min_size {} for symbol {}",
                        size_str, min_size, symbol
                    );
                    error!("{}", msg);
                    insert_db_error(pool, exchange, &msg).await;
                    return;
                }
            }
            Err(_) => {
                let msg = format!("Invalid size '{}' for symbol {}", size_str, symbol);
                error!("{}", msg);
                insert_db_error(pool, exchange, &msg).await;
                return;
            }
        },
        None => match calculate_size(10.0, price_f64, base_increment, min_size) {
            Some(size) => size,
            None => {
                let msg = format!("Failed to calculate size for symbol {}", symbol);
                error!("{}", msg);
                insert_db_error(pool, exchange, &msg).await;
                return;
            }
        },
    };
    make_order(
        pool,
        exchange,
        side,
        symbol,
        price_str.to_string(),
        size_str,
    )
    .await;
}

fn format_price(price: f64, increment: f64) -> String {
    let decimals = if increment >= 1.0 {
        0
    } else {
        (-increment.log10().floor() as usize).min(10)
    };
    format!("{:.decimals$}", price)
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
    if order.type_ == "received" {
        // order in order book
        insert_current_orderactive_to_db(pool, exchange, &order).await;

        let mut active_buy_orders =
            fetch_all_active_orders_by_symbol(pool, exchange, &order.symbol, "buy").await;
        let mut active_sell_orders =
            fetch_all_active_orders_by_symbol(pool, exchange, &order.symbol, "sell").await;

        if order.side == "buy" {
            if active_sell_orders.is_empty() {
                // sell orders unexist
                if active_buy_orders.len() == 2 {
                    if let Some(price_str) = calculate_price(
                        &order.price,
                        &symbol_info.price_increment,
                        |a, _b| a * 1.01, // price + 1%
                    ) {
                        create_order_safely(
                            pool,
                            exchange,
                            &order.side,
                            &order.symbol,
                            &price_str,
                            order.origin_size.as_deref(),
                            &symbol_info,
                        )
                        .await;
                    } else {
                        error!("Failed to calculate price for order {}", order.order_id);
                        insert_db_error(pool, exchange, "Price calculation failed").await;
                    }
                }

                while active_buy_orders.len() >= 3 {
                    if let Some(oldest_order) =
                        delete_oldest_orderactive(pool, exchange, &order.symbol, "buy").await
                    {
                        let _ = cancel_order(
                            pool,
                            exchange,
                            &oldest_order.symbol,
                            &oldest_order.order_id,
                        )
                        .await;
                        active_buy_orders =
                            fetch_all_active_orders_by_symbol(pool, exchange, &order.symbol, "buy")
                                .await;
                    } else {
                        break;
                    }
                }
            }
        } else if order.side == "sell" {
            if active_buy_orders.is_empty() {
                // buy orders unexist
                if active_sell_orders.len() == 2 {
                    if let Some(price_str) = calculate_price(
                        &order.price,
                        &symbol_info.price_increment,
                        |a, _b| a * 100.0 / 101.0, // price - 1%
                    ) {
                        create_order_safely(
                            pool,
                            exchange,
                            &order.side,
                            &order.symbol,
                            &price_str,
                            order.origin_size.as_deref(),
                            &symbol_info,
                        )
                        .await;
                    } else {
                        error!("Failed to calculate price for order {}", order.order_id);
                        insert_db_error(pool, exchange, "Price calculation failed").await;
                    }
                }

                while active_sell_orders.len() >= 3 {
                    if let Some(oldest_order) =
                        delete_oldest_orderactive(pool, exchange, &order.symbol, "sell").await
                    {
                        let _ = cancel_order(
                            pool,
                            exchange,
                            &oldest_order.symbol,
                            &oldest_order.order_id,
                        )
                        .await;
                        active_sell_orders = fetch_all_active_orders_by_symbol(
                            pool,
                            exchange,
                            &order.symbol,
                            "sell",
                        )
                        .await;
                    } else {
                        break;
                    }
                }
            }
        }
    } else if order.type_ == "canceled" {
        // cancel order
        delete_current_orderactive_from_db(pool, exchange, &order.order_id).await;
    } else if order.type_ == "match" && order.remain_size == Some("0".to_string()) {
        // get last event on match size of position
        // next msg will filled, but it don't have match price

        // filled sell (cancel all buy orders)
        //     check if order on sell exist
        //         unexist - add buy order
        //                 - add buy order - 1%
        //         exist 	- add buy order - 1%
        // filled buy (cancel all sell orders)
        //     check if order on buy exist
        //         unexist - add sell order
        //                 - add sell order + 1%
        //         exist	- add sell order + 1%

        delete_current_orderactive_from_db(pool, exchange, &order.order_id).await;

        if order.side == "sell" {
            // create new buy order
            if let Some(price_str) = calculate_price(
                &order.match_price,
                &symbol_info.price_increment,
                |a, _b| a * 100.0 / 101.0, // match_price - 1%
            ) {
                create_order_safely(
                    pool,
                    exchange,
                    "buy",
                    &order.symbol,
                    &price_str,
                    order.origin_size.as_deref(),
                    &symbol_info,
                )
                .await;
            } else {
                error!("Failed to calculate price for order {}", order.order_id);
                insert_db_error(pool, exchange, "Price calculation failed").await;
            }
        } else if order.side == "buy" {
            // filled buy (cancel all sell orders)
            if let Some(price_str) = calculate_price(
                &order.match_price,
                &symbol_info.price_increment,
                |a, _b| a * 1.01, // match_price + 1%
            ) {
                create_order_safely(
                    pool,
                    exchange,
                    "sell",
                    &order.symbol,
                    &price_str,
                    order.origin_size.as_deref(),
                    &symbol_info,
                )
                .await;
            } else {
                error!("Failed to calculate price for order {}", order.order_id);
                insert_db_error(pool, exchange, "Price calculation failed").await;
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
        info!("{:?}", position);
        error!("Failed to upsert margin account state: {}", e);
        insert_db_error(pool, exchange, &e.to_string()).await;
    }
    for (symbol, amount) in &position.debt_list {
        if let Err(e) = upsert_position_debt(pool, exchange, symbol, amount).await {
            error!("Failed to insert debt margin account state: {}", e);
            insert_db_error(pool, exchange, &e.to_string()).await;
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
            error!("Failed to insert asset margin account state: {}", e);
            insert_db_error(pool, exchange, &e.to_string()).await;
        }
    }
    // repay borrow
    for (asset, liability_str) in &position.debt_list {
        if let Ok(liability) = liability_str.parse::<f64>() {
            if let Some(asset_info) = &position.asset_list.get(asset) {
                if let Ok(available) = asset_info.available.parse::<f64>() {
                    if let Ok(hold) = asset_info.hold.parse::<f64>() {
                        if liability > 0.0 {
                            if available >= liability {
                                info!(
                                    "Can repay {} {} liability with available {}",
                                    liability, asset, available
                                );

                                if let Err(e) =
                                    api::requests::create_repay_order(asset, &liability_str).await
                                {
                                    error!("Failed to repay liability: {}", e);
                                    insert_db_error(pool, exchange, &e.to_string()).await;
                                };
                            } else if available > 0.0 {
                                info!(
                                    "Can partially repay {} {} liability with available {}",
                                    liability, asset, available
                                );

                                if let Err(e) =
                                    api::requests::create_repay_order(asset, &asset_info.available)
                                        .await
                                {
                                    error!("Failed to partially repay debt: {}", e);
                                    insert_db_error(pool, exchange, &e.to_string()).await;
                                }
                            }
                        } else if available > 0.0 && asset != "USDC" {
                            // transfer available from margin
                            match api::requests::sent_account_transfer(
                                asset,
                                &asset_info.available,
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
                                        asset, &asset_info.available, e
                                    );
                                    error!("{}", msg);
                                    insert_db_error(&pool, &exchange, &msg).await;
                                }
                            }
                        }
                    } else {
                        error!("Failed to parse hold for {}", asset);
                        insert_db_error(
                            pool,
                            exchange,
                            &format!("Parse error: hold={}", asset_info.hold),
                        )
                        .await;
                    }
                } else {
                    error!("Failed to parse available balance for {}", asset);
                    insert_db_error(
                        pool,
                        exchange,
                        &format!("Parse error: available={}", asset_info.available),
                    )
                    .await;
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();
    dotenv().ok();
    let mut init_order_execute = true;

    let database_url: String = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let exchange: String = "kucoin".to_string();

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await
        .expect("Failed to create pool");

    delete_all_orderactive_from_db(&pool, &exchange).await;

    match api::requests::cancel_all_open_orders().await {
        Ok(()) => {
            info!("Successfully cancelled all open orders");
        }
        Err(e) => {
            let msg: String = format!("Failed to fetch margin accounts: {}", e);
            error!("{}", msg);
            insert_db_error(&pool, &exchange, &msg).await;
        }
    }
    loop {
        let mut all_asset_transfer: bool = true;
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
                                error!("Failed to repay liability: {}", e);
                                insert_db_error(&pool, &exchange, &e.to_string()).await;
                            };
                        }
                        all_asset_transfer = false;
                    } else if available > 0.0 && &account.currency != "USDC" {
                        match api::requests::sent_account_transfer(
                            &account.currency,
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
                                    &account.currency,
                                    &available.to_string(),
                                    e
                                );
                                error!("{}", msg);
                                insert_db_error(&pool, &exchange, &msg).await;
                            }
                        }
                        all_asset_transfer = false;
                    }
                }
            }
            Err(e) => {
                let msg: String = format!("Failed to get margin accounts {}", e);
                error!("{}", msg);
                insert_db_error(&pool, &exchange, &msg).await;
                // exit with error
                return Err(msg.into());
            }
        }
        if all_asset_transfer {
            break;
        } else {
            sleep(REPAY_DELAY).await;
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
                                        error!("Failed to parse message {}", e);
                                        // sent balance error to pg
                                        insert_db_error(
                                            &pool_for_handler,
                                            &exchange_for_handler,
                                            &e.to_string(),
                                        )
                                        .await;
                                    }
                                }
                            } else if data.topic == "/spotMarket/tradeOrdersV2" {
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
                                        error!("Failed to parse message {}", e);
                                        // sent order error to pg
                                        insert_db_error(
                                            &pool_for_handler,
                                            &exchange_for_handler,
                                            &e.to_string(),
                                        )
                                        .await;
                                    }
                                }
                            } else if data.topic == "/margin/position" {
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
                                        error!("Failed to parse message {}", e);
                                        // sent order error to pg
                                        insert_db_error(
                                            &pool_for_handler,
                                            &exchange_for_handler,
                                            &e.to_string(),
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
                        error!("Failed to parse message: {} | Raw: {}", e, msg);
                        // sent error to pg
                        insert_db_error(&pool_for_handler, &exchange_for_handler, &e.to_string())
                            .await;
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
                error!("Failed to subscribe: {}", e);
                insert_db_error(&pool, &exchange, &e.to_string()).await;
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
                let trade_orders = get_all_symbol_for_trade(&pool, &exchange).await;

                for trd_order in trade_orders.iter() {
                    if let Some(symbol_info) = symbol_map.get(&trd_order.symbol) {
                        match api::requests::get_ticker_price(&trd_order.symbol).await {
                            Ok(actual_price_str) => {
                                // sell order
                                if let Some(price_str) = calculate_price(
                                    &Some(actual_price_str.clone()),
                                    &symbol_info.price_increment,
                                    |a, _b| a * 1.01, // price + 1%
                                ) {
                                } else {
                                    error!(
                                        "Failed to calculate price for init order {}",
                                        &trd_order.symbol
                                    );
                                    insert_db_error(&pool, &exchange, "Price calculation failed")
                                        .await;
                                }
                                // buy order
                                if let Some(price_str) = calculate_price(
                                    &Some(actual_price_str.clone()),
                                    &symbol_info.price_increment,
                                    |a, _b| a * 100.0 / 101.0, // price - 1%
                                ) {
                                } else {
                                    error!(
                                        "Failed to calculate price for init order {}",
                                        &trd_order.symbol
                                    );
                                    insert_db_error(&pool, &exchange, "Price calculation failed")
                                        .await;
                                }
                            }
                            Err(e) => {
                                error!("Failed to get price for {}: {}", trd_order.symbol, e);
                                insert_db_error(&pool, &exchange, &e.to_string()).await;
                            }
                        };
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
