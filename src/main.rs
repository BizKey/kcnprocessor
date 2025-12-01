use crate::api::db::{
    delete_db_orderactive, fetch_all_active_orders_by_symbol, fetch_price_increment_by_symbol,
    insert_db_balance, insert_db_error, insert_db_event, insert_db_orderactive,
    insert_db_orderevent,
};
use crate::api::models::{BalanceData, KuCoinMessage, OrderData};
use dotenv::dotenv;
use futures_util::{SinkExt, StreamExt};
use log::{error, info, trace};
use sqlx::postgres::PgPoolOptions;
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
    size: i32,
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

    // websocket to pg
    let (tx_in, mut rx_in) = mpsc::channel::<String>(1000);
    // pg to websocket
    let (tx_out, mut rx_out) = mpsc::channel::<String>(100);

    let exchange_for_handler = exchange.clone();
    let pool_for_handler = pool.clone();
    let handler = tokio::spawn(async move {
        while let Some(msg) = rx_in.recv().await {
            info!("Processing: {}", msg);
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
                            match serde_json::from_value::<OrderData>(data.data) {
                                Ok(order) => {
                                    info!("{:?}", order);
                                    // sent order to pg
                                    insert_db_orderevent(
                                        &pool_for_handler,
                                        &exchange_for_handler,
                                        &order,
                                    )
                                    .await;
                                    if order.type_ == "open" && order.status == "open" {
                                        // order in order book
                                        // add order to active orders
                                        insert_db_orderactive(
                                            &pool_for_handler,
                                            &exchange_for_handler,
                                            &order,
                                        )
                                        .await;
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

                                        let mut active_orders = fetch_all_active_orders_by_symbol(
                                            &pool_for_handler,
                                            &exchange_for_handler,
                                            &order.symbol,
                                        )
                                        .await;

                                        delete_db_orderactive(
                                            &pool_for_handler,
                                            &exchange_for_handler,
                                            &order.order_id,
                                        )
                                        .await;
                                        active_orders.retain(|o| o.order_id != order.order_id);

                                        let price_increment_result =
                                            fetch_price_increment_by_symbol(
                                                &pool_for_handler,
                                                &exchange_for_handler,
                                                &order.symbol,
                                            )
                                            .await;

                                        let price_increment = match price_increment_result {
                                            Ok(inc) => inc,
                                            Err(e) => {
                                                error!("Failed to fetch price increment: {}", e);
                                                insert_db_error(
                                                    &pool_for_handler,
                                                    &exchange_for_handler,
                                                    &e,
                                                )
                                                .await;
                                                continue;
                                            }
                                        };

                                        if order.side == "sell" {
                                            // filled sell (cancel all buy orders)
                                            let mut sell_orders: Vec<api::models::ActiveOrder> =
                                                Vec::with_capacity(active_orders.len());
                                            for order in active_orders.drain(..) {
                                                if order.side == "buy" {
                                                    if let Err(e) = cancel_order(
                                                        &tx_out,
                                                        &pool_for_handler,
                                                        &exchange_for_handler,
                                                        &order.symbol,
                                                        &order.order_id,
                                                    )
                                                    .await
                                                    {
                                                        continue;
                                                    };
                                                } else {
                                                    sell_orders.push(order);
                                                }
                                            }
                                            active_orders = sell_orders;
                                            //     check if order on sell exist
                                            if active_orders
                                                .iter()
                                                .filter(|order| order.side == "sell")
                                                .count()
                                                == 0
                                            {
                                                // create new buy order
                                                if let Err(e) = make_order(
                                                    &tx_out,
                                                    &pool_for_handler,
                                                    &exchange_for_handler,
                                                    "buy",
                                                    &order.symbol,
                                                    calculate_price(
                                                        &order.match_price,
                                                        &price_increment,
                                                        |a, _b| a * 100.0 / 101.0,
                                                    )
                                                    .expect("REASON"),
                                                    1,
                                                )
                                                .await
                                                {
                                                    continue;
                                                }

                                                // create new buy order
                                                if let Err(e) = make_order(
                                                    &tx_out,
                                                    &pool_for_handler,
                                                    &exchange_for_handler,
                                                    "buy",
                                                    &order.symbol,
                                                    calculate_price(
                                                        &order.match_price,
                                                        &price_increment,
                                                        |a, b| a - b,
                                                    )
                                                    .expect("REASON"),
                                                    1,
                                                )
                                                .await
                                                {
                                                    continue;
                                                }
                                            } else {
                                                // create new buy order
                                                if let Err(e) = make_order(
                                                    &tx_out,
                                                    &pool_for_handler,
                                                    &exchange_for_handler,
                                                    "buy",
                                                    &order.symbol,
                                                    calculate_price(
                                                        &order.match_price,
                                                        &price_increment,
                                                        |a, _b| a * 100.0 / 101.0,
                                                    )
                                                    .expect("REASON"),
                                                    1,
                                                )
                                                .await
                                                {
                                                    continue;
                                                };
                                            }
                                        } else if order.side == "buy" {
                                            // filled buy (cancel all sell orders)
                                            let mut buy_orders: Vec<api::models::ActiveOrder> =
                                                Vec::with_capacity(active_orders.len());
                                            for order in active_orders.drain(..) {
                                                if order.side == "sell" {
                                                    if let Err(e) = cancel_order(
                                                        &tx_out,
                                                        &pool_for_handler,
                                                        &exchange_for_handler,
                                                        &order.symbol,
                                                        &order.order_id,
                                                    )
                                                    .await
                                                    {
                                                        continue;
                                                    };
                                                } else {
                                                    buy_orders.push(order);
                                                };
                                            }
                                            active_orders = buy_orders;
                                            // count buy orders
                                            if active_orders
                                                .iter()
                                                .filter(|order| order.side == "buy")
                                                .count()
                                                == 0
                                            {
                                                if let Err(e) = make_order(
                                                    &tx_out,
                                                    &pool_for_handler,
                                                    &exchange_for_handler,
                                                    "sell",
                                                    &order.symbol,
                                                    calculate_price(
                                                        &order.match_price,
                                                        &price_increment,
                                                        |a, b| a + b,
                                                    )
                                                    .expect("REASON"),
                                                    1,
                                                )
                                                .await
                                                {
                                                    continue;
                                                };

                                                if let Err(e) = make_order(
                                                    &tx_out,
                                                    &pool_for_handler,
                                                    &exchange_for_handler,
                                                    "sell",
                                                    &order.symbol,
                                                    calculate_price(
                                                        &order.match_price,
                                                        &price_increment,
                                                        |a, _b| a * 1.01,
                                                    )
                                                    .expect("REASON"),
                                                    1,
                                                )
                                                .await
                                                {
                                                    continue;
                                                };
                                            } else {
                                                if let Err(e) = make_order(
                                                    &tx_out,
                                                    &pool_for_handler,
                                                    &exchange_for_handler,
                                                    "sell",
                                                    &order.symbol,
                                                    calculate_price(
                                                        &order.match_price,
                                                        &price_increment,
                                                        |a, _b| a * 1.01,
                                                    )
                                                    .expect("REASON"),
                                                    1,
                                                )
                                                .await
                                                {
                                                    continue;
                                                };
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
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
                            insert_db_error(&pool_for_handler, &exchange_for_handler, &data.topic)
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
                        insert_db_error(&pool_for_handler, &exchange_for_handler, &data.data).await;
                    }
                },
                Err(e) => {
                    error!("Failed to parse message: {} | Raw: {}", e, msg);
                    // sent error to pg
                    insert_db_error(&pool_for_handler, &exchange_for_handler, &e.to_string()).await;
                }
            }
        }
        info!("Message handler finished");
    });

    loop {
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

                match api::requests::sign_kucoin(&session_msg_text) {
                    Ok(signature) => {
                        info!("Sending session signature");
                        let _ = trade_ws_write.send(Message::text(signature)).await;
                    }
                    Err(_) => {}
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

        for sub in build_subscription() {
            if let Err(e) = event_ws_write.send(Message::text(sub.to_string())).await {
                error!("Failed to subscribe: {}", e);
                insert_db_error(&pool, &exchange, &e.to_string()).await;
                break;
            }
        }

        info!("Subscribed and listening for messages...");

        let event_ping_interval = interval(PING_INTERVAL);
        let trade_ping_interval = interval(PING_INTERVAL);
        tokio::pin!(event_ping_interval);
        tokio::pin!(trade_ping_interval);

        let mut should_reconnect = false;

        loop {
            tokio::select! {
                // Events
                event_msg = event_ws_read.next() => {
                    info!("Event {:?}", event_msg);
                    match event_msg {
                        Some(Ok(Message::Text(text))) => {
                            if tx_in.send(text.to_string()).await.is_err() {
                                drop(tx_in);
                                let _ = handler.await;
                                return Ok(());
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = event_ws_write.send(Message::Pong(data)).await;
                            trace!("Ping recv");
                        }
                        Some(Ok(Message::Pong(_))) => {
                            trace!("Pong recv");
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
                    trace!("Ping sent");
                    let _ = event_ws_write.send(Message::Ping(vec![].into())).await;
                }
                trade_msg =  trade_ws_read.next() => {
                    info!("Trade {:?}", trade_msg);
                    match trade_msg {
                        Some(Ok(Message::Text(text))) => {
                            info!("{:?}", text);
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = trade_ws_write.send(Message::Pong(data)).await;
                            trace!("Ping recv");
                        }
                        Some(Ok(Message::Pong(_))) => {
                            trace!("Pong recv");
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
                _ = trade_ping_interval.tick() => {
                    trace!("Ping sent");
                    let _ = trade_ws_write.send(Message::Ping(vec![].into())).await;
                }

            }
        }
        if should_reconnect {
            error!("Reconnecting in {} seconds...", RECONNECT_DELAY.as_secs());
            // sent error to pg
            sleep(RECONNECT_DELAY).await;
        } else {
            break;
        }
    }

    drop(tx_in);

    let _ = handler.await;
    info!("Application shutdown complete");

    Ok(())
}
