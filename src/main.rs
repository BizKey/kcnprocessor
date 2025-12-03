use crate::api::db::{
    delete_db_orderactive, fetch_all_active_orders_by_symbol, fetch_symbol_info, insert_db_balance,
    insert_db_error, insert_db_event, insert_db_orderactive, insert_db_orderevent,
    upsert_position_asset, upsert_position_debt, upsert_position_ratio,
};
use crate::api::models::{BalanceData, KuCoinMessage, OrderData, PositionData, Symbol};
use dotenv::dotenv;
use futures_util::{SinkExt, StreamExt};
use log::{error, info};
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::env;
use tokio::sync::mpsc;
use tokio::time::{Duration, interval, sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};
mod api {
    pub mod db;
    pub mod models;
    pub mod requests;
}

const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const PING_INTERVAL: Duration = Duration::from_secs(5);

fn build_subscription() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({"id":"subscribe_orders","type":"subscribe","topic":"/spotMarket/tradeOrdersV2","response":true,"privateChannel":"true"}),
        serde_json::json!({"id":"subscribe_balance","type":"subscribe","topic":"/account/balance","response":true,"privateChannel":"true"}),
        serde_json::json!({"id":"subscribe_position","type":"subscribe","topic":"/margin/position","response":true,"privateChannel":"true"}),
    ]
}

async fn cancel_order(
    tx_out: &mpsc::Sender<String>,
    pool: &sqlx::Pool<sqlx::Postgres>,
    exchange: &str,
    symbol: &str,
    order_id: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // cancel other orders by symbol
    let msg = serde_json::json!({
      "id": format!("cancel-{}", order_id),
    "op": "margin.cancel",
    "args": {
        "symbol": symbol,
        "orderId": order_id
    }
    });
    if let Err(e) = tx_out.send(msg.to_string()).await {
        error!("Failed to send order: {}", e);
        insert_db_error(pool, exchange, &e.to_string()).await;
        return Err(e.into());
    }
    Ok(())
}

async fn make_order(
    tx_out: &mpsc::Sender<String>,
    pool: &sqlx::Pool<sqlx::Postgres>,
    exchange: &str,
    side: &str,
    symbol: &str,
    price: String,
    size: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let msg = serde_json::json!({
        "id": format!("create-order-{}-{}", side, symbol),
        "op": "margin.order",
        "args": {
            "price": price,
            "size": size,
            "side": side,
            "symbol": symbol,
            "timeInForce": "GTC",
            "type": "limit",
            "autoBorrow": true,
            "autoRepay": true,
        }
    });
    if let Err(e) = tx_out.send(msg.to_string()).await {
        error!("Failed to send order: {}", e);
        insert_db_error(pool, exchange, &e.to_string()).await;
        return Err(e.into());
    }
    Ok(())
}

fn format_size(size: f64, increment: f64) -> String {
    let decimals = if increment >= 1.0 {
        0
    } else {
        (-increment.log10().floor() as usize).min(10)
    };
    format!("{:.decimals$}", size)
}

fn calculate_size(
    notional: f64,       // $10
    price: f64,          // цена актива
    base_increment: f64, // шаг количества (например, 0.0001 для BTC)
    min_size: f64,       // минимальный размер ордера (например, 0.0001)
) -> Option<String> {
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
    tx_out: &mpsc::Sender<String>,
    pool: &sqlx::Pool<sqlx::Postgres>,
    exchange: &str,
    side: &str,
    symbol: &str,
    price_str: &str,
    base_increment_str: &str,
    min_size_str: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let price_f64 = match price_str.parse::<f64>() {
        Ok(v) if v > 0.0 => v,
        _ => {
            let msg = format!("Invalid price '{}' for symbol {}", price_str, symbol);
            error!("{}", msg);
            insert_db_error(pool, exchange, &msg).await;
            return Ok(());
        }
    };

    let base_increment = match base_increment_str.parse::<f64>() {
        Ok(v) if v > 0.0 => v,
        _ => {
            let msg = format!(
                "Invalid base_increment '{}' for symbol {}",
                base_increment_str, symbol
            );
            error!("{}", msg);
            insert_db_error(pool, exchange, &msg).await;
            return Ok(());
        }
    };

    let min_size = match min_size_str.parse::<f64>() {
        Ok(v) if v > 0.0 => v,
        _ => {
            let msg = format!("Invalid min_size '{}' for symbol {}", min_size_str, symbol);
            error!("{}", msg);
            insert_db_error(pool, exchange, &msg).await;
            return Ok(());
        }
    };

    if let Some(size_str) = calculate_size(10.0, price_f64, base_increment, min_size) {
        make_order(
            tx_out,
            pool,
            exchange,
            side,
            symbol,
            price_str.to_string(),
            size_str,
        )
        .await?;
    } else {
        let msg = format!("Calculated size below min_size for symbol {}", symbol);
        error!("{}", msg);
        insert_db_error(pool, exchange, &msg).await;
    }

    Ok(())
}

fn calculate_price(
    base_price: &Option<String>,
    increment: &str,
    operation: fn(f64, f64) -> f64,
) -> Option<String> {
    if let Some(match_price) = base_price {
        if let (Ok(price_num), Ok(inc_num)) = (match_price.parse::<f64>(), increment.parse::<f64>())
        {
            Some(operation(price_num, inc_num).to_string())
        } else {
            base_price.clone()
        }
    } else {
        None
    }
}

async fn handle_trade_order_event(
    data: serde_json::Value,
    tx_out: &mpsc::Sender<String>,
    pool: &sqlx::Pool<sqlx::Postgres>,
    exchange: &str,
    symbol_map: &HashMap<String, Symbol>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match serde_json::from_value::<OrderData>(data) {
        Ok(order) => {
            info!("{:?}", order);
            // sent order to pg
            insert_db_orderevent(pool, exchange, &order).await;
            if order.type_ == "open" && order.status == "open" {
                // order in order book
                // add order to active orders
                insert_db_orderactive(pool, exchange, &order).await;
            } else if order.type_ == "match"
                && order.status == "match"
                && order.remain_size == Some("0".to_string())
            {
                // get last event on match size of position
                // next msg will filled, but it don't have match price

                // filled sell (cancel all buy orders)
                //     check if order on sell exist
                //         unexist - add buy order - 1 tick
                //                 - add buy order - 1%
                //         exist 	- add buy order - 1%
                // filled buy (cancel all sell orders)
                //     check if order on buy exist
                //         unexist - add sell order - 1 tick
                //                 - add sell order - 1%
                //         exist	- add sell order - 1%

                let mut active_orders =
                    fetch_all_active_orders_by_symbol(pool, exchange, &order.symbol).await;

                delete_db_orderactive(pool, exchange, &order.order_id).await;
                active_orders.retain(|o| o.order_id != order.order_id);

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
                        return Ok(());
                    }
                };

                if order.side == "sell" {
                    // filled sell (cancel all buy orders)
                    let mut sell_orders: Vec<api::models::ActiveOrder> =
                        Vec::with_capacity(active_orders.len());
                    for order in active_orders.drain(..) {
                        if order.side == "buy" {
                            let _ = cancel_order(
                                tx_out,
                                pool,
                                exchange,
                                &order.symbol,
                                &order.order_id,
                            )
                            .await;
                        } else {
                            sell_orders.push(order);
                        }
                    }
                    active_orders = sell_orders;
                    //     check if order on sell exist
                    if !active_orders.iter().any(|order| order.side == "sell") {
                        // create new buy order
                        if let Some(price_str) = calculate_price(
                            &order.match_price,
                            &symbol_info.price_increment,
                            |a, _b| a * 100.0 / 101.0,
                        ) {
                            create_order_safely(
                                tx_out,
                                pool,
                                exchange,
                                "buy",
                                &order.symbol,
                                &price_str,
                                &symbol_info.base_increment,
                                &symbol_info.base_min_size,
                            )
                            .await?;
                        } else {
                            error!("Failed to calculate price for order {}", order.order_id);
                            insert_db_error(pool, exchange, "Price calculation failed").await;
                        }

                        // create new buy order
                        if let Some(price_str) = calculate_price(
                            &order.match_price,
                            &symbol_info.price_increment,
                            |a, b| a - b,
                        ) {
                            create_order_safely(
                                tx_out,
                                pool,
                                exchange,
                                "buy",
                                &order.symbol,
                                &price_str,
                                &symbol_info.base_increment,
                                &symbol_info.base_min_size,
                            )
                            .await?;
                        } else {
                            error!("Failed to calculate price for order {}", order.order_id);
                            insert_db_error(pool, exchange, "Price calculation failed").await;
                        }
                    } else {
                        // create new buy order
                        if let Some(price_str) = calculate_price(
                            &order.match_price,
                            &symbol_info.price_increment,
                            |a, _b| a * 100.0 / 101.0,
                        ) {
                            create_order_safely(
                                tx_out,
                                pool,
                                exchange,
                                "buy",
                                &order.symbol,
                                &price_str,
                                &symbol_info.base_increment,
                                &symbol_info.base_min_size,
                            )
                            .await?;
                        } else {
                            error!("Failed to calculate price for order {}", order.order_id);
                            insert_db_error(pool, exchange, "Price calculation failed").await;
                        }
                    }
                } else if order.side == "buy" {
                    // filled buy (cancel all sell orders)
                    let mut buy_orders: Vec<api::models::ActiveOrder> =
                        Vec::with_capacity(active_orders.len());
                    for order in active_orders.drain(..) {
                        if order.side == "sell" {
                            let _ = cancel_order(
                                tx_out,
                                pool,
                                exchange,
                                &order.symbol,
                                &order.order_id,
                            )
                            .await;
                        } else {
                            buy_orders.push(order);
                        };
                    }
                    active_orders = buy_orders;
                    // count buy orders
                    if !active_orders.iter().any(|order| order.side == "buy") {
                        if let Some(price_str) = calculate_price(
                            &order.match_price,
                            &symbol_info.price_increment,
                            |a, b| a + b,
                        ) {
                            create_order_safely(
                                tx_out,
                                pool,
                                exchange,
                                "sell",
                                &order.symbol,
                                &price_str,
                                &symbol_info.base_increment,
                                &symbol_info.base_min_size,
                            )
                            .await?;
                        } else {
                            error!("Failed to calculate price for order {}", order.order_id);
                            insert_db_error(pool, exchange, "Price calculation failed").await;
                        }
                        if let Some(price_str) = calculate_price(
                            &order.match_price,
                            &symbol_info.price_increment,
                            |a, _b| a * 1.01,
                        ) {
                            create_order_safely(
                                tx_out,
                                pool,
                                exchange,
                                "sell",
                                &order.symbol,
                                &price_str,
                                &symbol_info.base_increment,
                                &symbol_info.base_min_size,
                            )
                            .await?;
                        } else {
                            error!("Failed to calculate price for order {}", order.order_id);
                            insert_db_error(pool, exchange, "Price calculation failed").await;
                        }
                    } else if let Some(price_str) = calculate_price(
                        &order.match_price,
                        &symbol_info.price_increment,
                        |a, _b| a * 1.01,
                    ) {
                        create_order_safely(
                            tx_out,
                            pool,
                            exchange,
                            "sell",
                            &order.symbol,
                            &price_str,
                            &symbol_info.base_increment,
                            &symbol_info.base_min_size,
                        )
                        .await?;
                    } else {
                        error!("Failed to calculate price for order {}", order.order_id);
                        insert_db_error(pool, exchange, "Price calculation failed").await;
                    }
                }
            }
        }
        Err(e) => {
            error!("Failed to parse message {}", e);
            // sent order error to pg
            insert_db_error(pool, exchange, &e.to_string()).await;
        }
    }
    Ok(())
}

async fn handle_position_event(
    data: serde_json::Value,
    pool: &sqlx::Pool<sqlx::Postgres>,
    exchange: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match serde_json::from_value::<PositionData>(data) {
        Ok(position) => {
            info!("{:?}", position);
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

            for (asset, debt_str) in &position.debt_list {
                if let Ok(debt) = debt_str.parse::<f64>() {
                    if debt <= 0.0 {
                        continue;
                    }

                    if let Some(asset_info) = &position.asset_list.get(asset) {
                        if let Ok(available) = asset_info.available.parse::<f64>() {
                            if available >= debt {
                                info!(
                                    "Can repay {} {} debt with available {}",
                                    debt, asset, available
                                );

                                if let Err(e) =
                                    api::requests::create_repay_order(asset, &debt.to_string())
                                        .await
                                {
                                    error!("Failed to repay debt: {}", e);
                                    insert_db_error(pool, exchange, &e.to_string()).await;
                                }
                            } else if available > 0.0 {
                                info!(
                                    "Can partially repay {} {} debt with available {}",
                                    debt, asset, available
                                );

                                if let Err(e) =
                                    api::requests::create_repay_order(asset, &available.to_string())
                                        .await
                                {
                                    error!("Failed to partially repay debt: {}", e);
                                    insert_db_error(pool, exchange, &e.to_string()).await;
                                }
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
        Err(e) => {
            error!("Failed to parse message {}", e);
            // sent order error to pg
            insert_db_error(pool, exchange, &e.to_string()).await;
        }
    }
    Ok(())
}

async fn outgoing_message_handler(
    mut rx_out: mpsc::Receiver<String>,
    mut ws_write: futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >,
) {
    while let Some(message) = rx_out.recv().await {
        info!("Sending outgoing message: {}", message);
        if let Err(e) = ws_write.send(Message::text(message)).await {
            error!("Failed to send outgoing message: {}", e);
            break;
        }
    }
    info!("Outgoing message handler finished");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();
    dotenv().ok();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let exchange: String = "kucoin".to_string();

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await
        .expect("Failed to create pool");

    let symbol_info = fetch_symbol_info(&pool, &exchange).await;
    let symbol_map: HashMap<String, Symbol> = symbol_info
        .into_iter()
        .map(|s| (s.symbol.clone(), s))
        .collect();

    loop {
        let exchange_for_handler = exchange.clone();
        let pool_for_handler = pool.clone();
        let symbol_map_for_handler = symbol_map.clone();

        // websocket to pg
        let (tx_in, mut rx_in) = mpsc::channel::<String>(1000);
        // pg to websocket
        let (tx_out, rx_out) = mpsc::channel::<String>(100);

        let tx_out_clone = tx_out.clone();

        // Work with oncome events
        let handler_event = tokio::spawn(async move {
            while let Some(msg) = rx_in.recv().await {
                match serde_json::from_str::<KuCoinMessage>(&msg) {
                    Ok(kc_msg) => match kc_msg {
                        KuCoinMessage::Welcome(data) => {
                            info!("{:?}", data);
                            insert_db_event(&pool_for_handler, &exchange_for_handler, &data).await;
                        }
                        KuCoinMessage::Message(data) => {
                            if data.topic == "/account/balance" {
                                match serde_json::from_value::<BalanceData>(data.data) {
                                    Ok(balance) => {
                                        info!("{:?}", balance);
                                        // sent balance to pg
                                        insert_db_balance(
                                            &pool_for_handler,
                                            &exchange_for_handler,
                                            balance,
                                        )
                                        .await;
                                    }
                                    Err(e) => {
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
                                if let Err(e) = handle_trade_order_event(
                                    data.data,
                                    &tx_out_clone,
                                    &pool_for_handler,
                                    &exchange_for_handler,
                                    &symbol_map_for_handler,
                                )
                                .await
                                {
                                    error!("Error handling trade order event: {}", e);
                                }
                            } else if data.topic == "/margin/position" {
                                // save to db position

                                if let Err(e) = handle_position_event(
                                    data.data,
                                    &pool_for_handler,
                                    &exchange_for_handler,
                                )
                                .await
                                {
                                    error!("Error handle position event: {}", e);
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
        //Add/Cancel orders WS
        let trade_ws_url = match api::requests::get_trading_ws_url() {
            Ok(url) => url,
            Err(e) => {
                error!("Failed to get trading WS URL: {}", e);
                sleep(RECONNECT_DELAY).await;
                continue;
            }
        };

        let trade_ws_stream = match connect_async(trade_ws_url).await {
            Ok((stream, _)) => stream,
            Err(e) => {
                error!("Failed to connect to trading WS: {}", e);
                sleep(RECONNECT_DELAY).await;
                continue;
            }
        };

        let (mut trade_ws_write, mut trade_ws_read) = trade_ws_stream.split();

        match trade_ws_read.next().await {
            Some(Ok(Message::Text(text))) => {
                info!("{:?}", text);
                let session_msg_text = text.to_string();
                if let Ok(signature) = api::requests::sign_kucoin(&session_msg_text) {
                    info!("Sending session signature");
                    let _ = trade_ws_write.send(Message::text(signature)).await;
                }
            }
            None => {
                info!("Trading WS stream ended while waiting for sessionId");
                sleep(RECONNECT_DELAY).await;
                continue;
            }
            _ => {}
        }

        // Position/Orders WS
        let event_ws_url = match api::requests::get_private_ws_url().await {
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

        let outgoing_handler = tokio::spawn(outgoing_message_handler(rx_out, trade_ws_write));
        info!("Subscribed and listening for messages...");

        let event_ping_interval = interval(PING_INTERVAL);
        tokio::pin!(event_ping_interval);

        let mut should_reconnect = false;

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
                        Some(Ok(Message::Pong(_))) => {
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
        drop(tx_out);
        handler_event.abort();
        outgoing_handler.abort();

        if !should_reconnect {
            break;
        }
        error!("Reconnecting in {} seconds...", RECONNECT_DELAY.as_secs());
        sleep(RECONNECT_DELAY).await;
    }

    Ok(())
}
