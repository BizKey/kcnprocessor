use crate::api::db::{
    delete_db_orderactive, fetch_all_active_orders_by_symbol, insert_db_balance, insert_db_error,
    insert_db_event, insert_db_orderactive, insert_db_orderevent,
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

pub fn symbol_to_kucoin_api(symbol: &str) -> String {
    symbol.replace("-", "")
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
                                    };
                                    if order.type_ == "filled" && order.status == "done" {
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

                                        // delete filled order from active orders
                                        delete_db_orderactive(
                                            &pool_for_handler,
                                            &exchange_for_handler,
                                            &order.order_id,
                                        )
                                        .await;

                                        if order.side == "sell" {
                                            // filled sell (cancel all buy orders)
                                            // get active order's
                                            for active_order in fetch_all_active_orders_by_symbol(
                                                &pool_for_handler,
                                                &exchange_for_handler,
                                                &order.symbol,
                                                "buy",
                                            )
                                            .await
                                            {
                                                // cancel other orders by symbol
                                                let cancel_msg = serde_json::json!({
                                                    "id": format!("cancel-{}", active_order.order_id),
                                                    "op": "margin.cancel",
                                                    "args": {
                                                        "symbol": &active_order.symbol,
                                                        "orderId": active_order.order_id
                                                    }
                                                });
                                                if let Err(e) =
                                                    tx_out.send(cancel_msg.to_string()).await
                                                {
                                                    error!(
                                                        "Failed to send cancel message to tx_out: {}",
                                                        e
                                                    );
                                                    insert_db_error(
                                                        &pool_for_handler,
                                                        &exchange_for_handler,
                                                        &e.to_string(),
                                                    )
                                                    .await;
                                                }
                                            }
                                            //     check if order on sell exist
                                            let orders = fetch_all_active_orders_by_symbol(
                                                &pool_for_handler,
                                                &exchange_for_handler,
                                                &order.symbol,
                                                "sell",
                                            )
                                            .await;
                                            if orders.len() == 0 {
                                                // create new buy order
                                                let side = "buy";
                                                let buy_order_msg = serde_json::json!({
                                                    "id": format!("create-order-{}-{}", &side, &order.symbol),
                                                    "op": "margin.order",
                                                    "args": {
                                                        "price": order.match_price, // -1%
                                                        "size": 1,
                                                        "side":side,
                                                        "symbol": &order.symbol,
                                                        "timeInForce": "GTC",
                                                        "type":"limit",
                                                        "autoBorrow": true,
                                                        "autoRepay": true,
                                                    }
                                                });
                                                if let Err(e) =
                                                    tx_out.send(buy_order_msg.to_string()).await
                                                {
                                                    error!(
                                                        "Failed to send buy order message to tx_out: {}",
                                                        e
                                                    );
                                                    insert_db_error(
                                                        &pool_for_handler,
                                                        &exchange_for_handler,
                                                        &e.to_string(),
                                                    )
                                                    .await;
                                                }
                                                // create new buy order
                                                let side = "buy";
                                                let buy_order_msg = serde_json::json!({
                                                    "id": format!("create-order-{}-{}", &side, &order.symbol),
                                                    "op": "margin.order",
                                                    "args": {
                                                        "price": order.match_price, // -1 tick
                                                        "size": 1,
                                                        "side":side,
                                                        "symbol": &order.symbol,
                                                        "timeInForce": "GTC",
                                                        "type":"limit",
                                                        "autoBorrow": true,
                                                        "autoRepay": true,
                                                    }
                                                });
                                                if let Err(e) =
                                                    tx_out.send(buy_order_msg.to_string()).await
                                                {
                                                    error!(
                                                        "Failed to send buy order message to tx_out: {}",
                                                        e
                                                    );
                                                    insert_db_error(
                                                        &pool_for_handler,
                                                        &exchange_for_handler,
                                                        &e.to_string(),
                                                    )
                                                    .await;
                                                }
                                            } else {
                                                // create new buy order
                                                let side = "buy";
                                                let buy_order_msg = serde_json::json!({
                                                    "id": format!("create-order-{}-{}", &side, &order.symbol),
                                                    "op": "margin.order",
                                                    "args": {
                                                        "price": order.match_price, // -1%
                                                        "size": 1,
                                                        "side":side,
                                                        "symbol": &order.symbol,
                                                        "timeInForce": "GTC",
                                                        "type":"limit",
                                                        "autoBorrow": true,
                                                        "autoRepay": true,
                                                    }
                                                });
                                                if let Err(e) =
                                                    tx_out.send(buy_order_msg.to_string()).await
                                                {
                                                    error!(
                                                        "Failed to send buy order message to tx_out: {}",
                                                        e
                                                    );
                                                    insert_db_error(
                                                        &pool_for_handler,
                                                        &exchange_for_handler,
                                                        &e.to_string(),
                                                    )
                                                    .await;
                                                }
                                            }
                                        } else if order.side == "buy" {
                                            // filled buy (cancel all sell orders)
                                            // get active order's
                                            for active_order in fetch_all_active_orders_by_symbol(
                                                &pool_for_handler,
                                                &exchange_for_handler,
                                                &order.symbol,
                                                "sell",
                                            )
                                            .await
                                            {
                                                // cancel other orders by symbol
                                                let cancel_msg = serde_json::json!({
                                                    "id": format!("cancel-{}", active_order.order_id),
                                                    "op": "margin.cancel",
                                                    "args": {
                                                        "symbol": &active_order.symbol,
                                                        "orderId": active_order.order_id
                                                    }
                                                });
                                                if let Err(e) =
                                                    tx_out.send(cancel_msg.to_string()).await
                                                {
                                                    error!(
                                                        "Failed to send cancel message to tx_out: {}",
                                                        e
                                                    );
                                                    insert_db_error(
                                                        &pool_for_handler,
                                                        &exchange_for_handler,
                                                        &e.to_string(),
                                                    )
                                                    .await;
                                                }
                                            }
                                            //     check if order on buy exist
                                            let orders = fetch_all_active_orders_by_symbol(
                                                &pool_for_handler,
                                                &exchange_for_handler,
                                                &order.symbol,
                                                "buy",
                                            )
                                            .await;
                                            if orders.len() == 0 {
                                                let side = "sell";
                                                let sell_order_msg = serde_json::json!({
                                                    "id": format!("create-order-{}-{}",&side, &order.symbol),
                                                    "op": "margin.order",
                                                    "args": {
                                                        "price": order.match_price, // -1 tick
                                                        "size": 1,
                                                        "side":side,
                                                        "symbol": &order.symbol,
                                                        "timeInForce": "GTC",
                                                        "type":"limit",
                                                        "autoBorrow": true,
                                                        "autoRepay": true,
                                                    }
                                                });
                                                if let Err(e) =
                                                    tx_out.send(sell_order_msg.to_string()).await
                                                {
                                                    error!(
                                                        "Failed to send sell order message to tx_out: {}",
                                                        e
                                                    );
                                                    insert_db_error(
                                                        &pool_for_handler,
                                                        &exchange_for_handler,
                                                        &e.to_string(),
                                                    )
                                                    .await;
                                                }
                                                let side = "sell";
                                                let sell_order_msg = serde_json::json!({
                                                    "id": format!("create-order-{}-{}",&side, &order.symbol),
                                                    "op": "margin.order",
                                                    "args": {
                                                        "price": order.match_price, // -1%
                                                        "size": 1,
                                                        "side":side,
                                                        "symbol": &order.symbol,
                                                        "timeInForce": "GTC",
                                                        "type":"limit",
                                                        "autoBorrow": true,
                                                        "autoRepay": true,
                                                    }
                                                });
                                                if let Err(e) =
                                                    tx_out.send(sell_order_msg.to_string()).await
                                                {
                                                    error!(
                                                        "Failed to send sell order message to tx_out: {}",
                                                        e
                                                    );
                                                    insert_db_error(
                                                        &pool_for_handler,
                                                        &exchange_for_handler,
                                                        &e.to_string(),
                                                    )
                                                    .await;
                                                }
                                            } else {
                                                let side = "sell";
                                                let sell_order_msg = serde_json::json!({
                                                    "id": format!("create-order-{}-{}",&side, &order.symbol),
                                                    "op": "margin.order",
                                                    "args": {
                                                        "price": order.match_price, // -1%
                                                        "size": 1,
                                                        "side":side,
                                                        "symbol": &order.symbol,
                                                        "timeInForce": "GTC",
                                                        "type":"limit",
                                                        "autoBorrow": true,
                                                        "autoRepay": true,
                                                    }
                                                });
                                                if let Err(e) =
                                                    tx_out.send(sell_order_msg.to_string()).await
                                                {
                                                    error!(
                                                        "Failed to send sell order message to tx_out: {}",
                                                        e
                                                    );
                                                    insert_db_error(
                                                        &pool_for_handler,
                                                        &exchange_for_handler,
                                                        &e.to_string(),
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
