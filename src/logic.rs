use fastrand;

use crate::api::db::{
    delete_exit_sl_id_bot_by_client_oid, delete_exit_tp_id_bot_by_client_oid, delete_symbol_bot_by_exit_sl_client_oid, fetch_symbol_info_by_symbol, get_bot_by_entry_client_oid,
    get_bot_by_exit_sl_client_oid, get_bot_by_exit_tp_client_oid, get_random_symbol, get_total_match_value_by_client_oid, insert_db_error, insert_db_msgsend, insert_db_orderevent,
    set_null_entry_client_oid_by_entry_client_oid, update_balance_bot_by_exit_sl_client_oid, update_balance_bot_by_exit_tp_client_oid, update_bot_balance_by_entry_client_oid,
    update_bot_entry_client_oid_by_id, update_exit_sl_client_oid_bot_by_entry_client_oid, update_exit_sl_client_oid_bot_by_exit_sl_order_id, update_exit_sl_order_id_bot_by_exit_sl_client_oid,
    update_exit_tp_client_oid_bot_by_entry_client_oid, update_exit_tp_client_oid_bot_by_exit_tp_order_id, update_exit_tp_order_id_bot_by_exit_tp_client_oid, upsert_position_asset,
    upsert_position_debt, upsert_position_ratio,
};
use crate::api::models::{AdvancedOrders, MakeOrderRes, OrderData, PositionData, Symbol};
use crate::api::requests::{add_api_v3_hf_margin_order, api_v3_hf_margin_stop_order, api_v3_hf_margin_stop_order_cancel_by_client_oid, create_repay_order, get_ticker_price};

use log::{error, info};

use tokio::time::Duration;

use uuid::Uuid;

const TP_BUY_PERCENT: f64 = 1.07; // +7%
const SL_BUY_PERCENT: f64 = 0.95; // -5%
const TP_SELL_PERCENT: f64 = 0.93; // -7%
const SL_SELL_PERCENT: f64 = 1.05; // +5%
const RETRY_DELAY_BASE: u64 = 500;

pub fn get_random_side() -> String {
    if fastrand::bool() { "buy".to_string() } else { "sell".to_string() }
}
pub fn format_assert(size: f64, increment: f64) -> String {
    let precision = if increment >= 1.0 { 0 } else { (increment.recip().log10().ceil() as usize).min(10) };
    let rounded = (size / increment).round() * increment;
    format!("{:.prec$}", rounded, prec = precision)
}

pub async fn handle_trade_order_event(order: OrderData, pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str) {
    // sent order to pg
    match insert_db_orderevent(pool, exchange, &order).await {
        Ok(_) => {}
        Err(e) => {
            let msg: String = format!("Failed insert_db_orderevent: {}", e);
            error!("{}", msg);
            match insert_db_error(pool, exchange, &msg).await {
                Ok(_) => {}
                Err(e) => {
                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                    error!("{}", msg);
                }
            }
        }
    }

    match &order.client_oid {
        Some(client_oid) => {
            if (order.type_ == "match" || order.type_ == "canceled") && (order.remain_size == Some("0".to_string()) || order.remain_funds == Some("0".to_string())) {
                let symbol_info: Symbol = match fetch_symbol_info_by_symbol(&pool, &exchange, &order.symbol).await {
                    Ok(Some(info)) => info,
                    Ok(None) => {
                        let msg: String = format!("Symbol info not found for {}", order.symbol);
                        error!("{}", msg);
                        match insert_db_error(&pool, &exchange, &msg).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                error!("{}", msg);
                            }
                        }
                        return;
                    }
                    Err(e) => {
                        let msg: String = format!("Symbol info not found for {} {}", order.symbol, e);
                        error!("{}", msg);
                        match insert_db_error(&pool, &exchange, &msg).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                error!("{}", msg);
                            }
                        }
                        return;
                    }
                };

                let price_increment: f64 = match symbol_info.price_increment.parse::<f64>() {
                    Ok(price_increment) => price_increment,
                    Err(e) => {
                        let msg: String = format!("Failed parse price_increment: {} {}", symbol_info.price_increment, e);
                        error!("{}", msg);
                        match insert_db_error(pool, exchange, &msg).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                error!("{}", msg);
                            }
                        }
                        return;
                    }
                };
                let quote_increment: f64 = match symbol_info.quote_increment.parse::<f64>() {
                    Ok(quote_increment) => quote_increment,
                    Err(e) => {
                        let msg: String = format!("Failed parse quote_increment: {} {}", symbol_info.quote_increment, e);
                        error!("{}", msg);
                        match insert_db_error(pool, exchange, &msg).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                error!("{}", msg);
                            }
                        }
                        return;
                    }
                };
                // if clientOid in bots entry_id (2 phase)
                match get_bot_by_exit_tp_client_oid(pool, exchange, client_oid).await {
                    Ok(Some(bot)) => {
                        // client_oid == exit_tp_client_oid
                        // delete exit_tp_client_oid stop order
                        match delete_exit_tp_id_bot_by_client_oid(pool, exchange, client_oid).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed delete_exit_tp_id_bot_by_client_oid: {}", e);
                                error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        error!("{}", msg);
                                    }
                                }
                            }
                        }
                        match bot.exit_sl_client_oid {
                            Some(exit_sl_client_oid) => {
                                // clear exit_sl_client_oid in bots by id !!
                                match delete_exit_sl_id_bot_by_client_oid(pool, exchange, &exit_sl_client_oid).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed delete_exit_sl_id_bot_by_client_oid: {}", e);
                                        error!("{}", msg);
                                        match insert_db_error(pool, exchange, &msg).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                error!("{}", msg);
                                            }
                                        }
                                    }
                                }
                                match api_v3_hf_margin_stop_order_cancel_by_client_oid(&exit_sl_client_oid).await {
                                    Ok(_) => {
                                        info!("Successfully cancel stop order :{}", &exit_sl_client_oid)
                                    }
                                    Err(e) => {
                                        let msg: String = format!("Failed cancel stop order: {}", e);
                                        error!("{}", msg);
                                        match insert_db_error(pool, exchange, &msg).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                error!("{}", msg);
                                            }
                                        }
                                        return;
                                    }
                                }
                            }
                            None => {}
                        }
                        match get_total_match_value_by_client_oid(pool, exchange, client_oid).await {
                            Ok(Some(return_balance)) => {
                                if order.side == "buy" {
                                    match bot.balance.parse::<f64>() {
                                        Ok(old_balance) => {
                                            let new_balance: f64 = old_balance + old_balance - return_balance;
                                            match update_balance_bot_by_exit_tp_client_oid(pool, exchange, client_oid, &format!("{:.4}", new_balance)).await {
                                                Ok(_) => {}
                                                Err(e) => {
                                                    let msg: String = format!("Failed update_balance_bot_by_exit_tp_client_oid: {}", e);
                                                    error!("{}", msg);
                                                    match insert_db_error(pool, exchange, &msg).await {
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                            error!("{}", msg);
                                                        }
                                                    }
                                                }
                                            }
                                            // create new random order
                                            match make_random_trade(pool, exchange, new_balance, bot.id).await {
                                                Ok(()) => {}
                                                Err(e) => {
                                                    let msg: String = format!("Error in make_random_trade: {}", e);
                                                    error!("{}", msg);
                                                    match insert_db_error(pool, exchange, &msg).await {
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                            error!("{}", msg);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            let msg: String = format!("Failed parse balance: {} {}", bot.balance, e);
                                            error!("{}", msg);
                                            match insert_db_error(pool, exchange, &msg).await {
                                                Ok(_) => {}
                                                Err(e) => {
                                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                    error!("{}", msg);
                                                }
                                            }
                                        }
                                    }
                                } else if order.side == "sell" {
                                    match update_balance_bot_by_exit_tp_client_oid(pool, exchange, client_oid, &format!("{:.4}", return_balance)).await {
                                        Ok(_) => {}
                                        Err(e) => {
                                            let msg: String = format!("Failed update_balance_bot_by_exit_tp_client_oid: {}", e);
                                            error!("{}", msg);
                                            match insert_db_error(pool, exchange, &msg).await {
                                                Ok(_) => {}
                                                Err(e) => {
                                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                    error!("{}", msg);
                                                }
                                            }
                                        }
                                    }
                                    // create new random order
                                    match make_random_trade(pool, exchange, return_balance, bot.id).await {
                                        Ok(()) => {}
                                        Err(e) => {
                                            let msg: String = format!("Error in make_random_trade: {}", e);
                                            error!("{}", msg);
                                            match insert_db_error(pool, exchange, &msg).await {
                                                Ok(_) => {}
                                                Err(e) => {
                                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                    error!("{}", msg);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            Ok(None) => {
                                error!("No records found or error occurred");
                            }
                            Err(e) => {
                                let msg: String = format!("Failed get_total_match_value_by_client_oid: {}", e);
                                error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        error!("{}", msg);
                                    }
                                }
                            }
                        }
                        return;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        let msg: String = format!("Failed get_bot_by_exit_tp_client_oid: {}", e);
                        error!("{}", msg);
                        match insert_db_error(pool, exchange, &msg).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                error!("{}", msg);
                            }
                        }
                    }
                }

                // if clientOid in bots entry_id (2 phase)
                match get_bot_by_exit_sl_client_oid(pool, exchange, client_oid).await {
                    Ok(Some(bot)) => {
                        // client_oid == exit_sl_client_oid
                        // delete exit_sl_client_oid stop order
                        match delete_exit_sl_id_bot_by_client_oid(pool, exchange, client_oid).await {
                            Ok(_) => {
                                match bot.exit_tp_client_oid {
                                    Some(exit_tp_client_oid) => {
                                        // clear exit_tp_client_oid in bots by entry_id
                                        match delete_exit_tp_id_bot_by_client_oid(pool, exchange, &exit_tp_client_oid).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                let msg: String = format!("Failed delete_exit_tp_id_bot_by_client_oid: {}", e);
                                                error!("{}", msg);
                                                match insert_db_error(pool, exchange, &msg).await {
                                                    Ok(_) => {}
                                                    Err(e) => {
                                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                        error!("{}", msg);
                                                    }
                                                }
                                            }
                                        }
                                        match api_v3_hf_margin_stop_order_cancel_by_client_oid(&exit_tp_client_oid).await {
                                            Ok(_) => {
                                                info!("Successfully cancel stop order :{}", &exit_tp_client_oid);
                                            }
                                            Err(e) => {
                                                let msg: String = format!("Failed cancel stop order: {}", e);
                                                error!("{}", msg);
                                                match insert_db_error(pool, exchange, &msg).await {
                                                    Ok(_) => {}
                                                    Err(e) => {
                                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                        error!("{}", msg);
                                                    }
                                                }
                                                return;
                                            }
                                        }
                                    }
                                    None => {}
                                }

                                match get_total_match_value_by_client_oid(pool, exchange, client_oid).await {
                                    Ok(Some(return_balance)) => {
                                        if order.side == "buy" {
                                            match bot.balance.parse::<f64>() {
                                                Ok(old_balance) => {
                                                    let new_balance: f64 = old_balance + old_balance - return_balance;
                                                    match update_balance_bot_by_exit_sl_client_oid(pool, exchange, client_oid, &format!("{:.4}", new_balance)).await {
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            let msg: String = format!("Failed update_balance_bot_by_exit_sl_client_oid: {}", e);
                                                            error!("{}", msg);
                                                            match insert_db_error(pool, exchange, &msg).await {
                                                                Ok(_) => {}
                                                                Err(e) => {
                                                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                                    error!("{}", msg);
                                                                }
                                                            }
                                                        }
                                                    }
                                                    // create new random order
                                                    match make_random_trade(pool, exchange, new_balance, bot.id).await {
                                                        Ok(()) => {}
                                                        Err(e) => {
                                                            let msg: String = format!("Error in make_random_trade: {}", e);
                                                            error!("{}", msg);
                                                            match insert_db_error(pool, exchange, &msg).await {
                                                                Ok(_) => {}
                                                                Err(e) => {
                                                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                                    error!("{}", msg);
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                                Err(e) => {
                                                    let msg: String = format!("Failed parse balance: {} {}", bot.balance, e);
                                                    error!("{}", msg);
                                                    match insert_db_error(pool, exchange, &msg).await {
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                            error!("{}", msg);
                                                        }
                                                    }
                                                }
                                            }
                                        } else if order.side == "sell" {
                                            match update_balance_bot_by_exit_sl_client_oid(pool, exchange, client_oid, &format!("{:.4}", return_balance)).await {
                                                Ok(_) => {}
                                                Err(e) => {
                                                    let msg: String = format!("Failed update_balance_bot_by_exit_sl_client_oid: {}", e);
                                                    error!("{}", msg);
                                                    match insert_db_error(pool, exchange, &msg).await {
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                            error!("{}", msg);
                                                        }
                                                    }
                                                }
                                            }

                                            // create new random order
                                            match make_random_trade(pool, exchange, return_balance, bot.id).await {
                                                Ok(()) => {}
                                                Err(e) => {
                                                    let msg: String = format!("Error in make_random_trade: {}", e);
                                                    error!("{}", msg);
                                                    match insert_db_error(pool, exchange, &msg).await {
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                            error!("{}", msg);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Ok(None) => {
                                        error!("No records found or error occurred");
                                    }
                                    Err(e) => {
                                        let msg: String = format!("Failed get_total_match_value_by_client_oid: {}", e);
                                        error!("{}", msg);
                                        match insert_db_error(pool, exchange, &msg).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                error!("{}", msg);
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                let msg: String = format!("Failed delete_exit_sl_id_bot_by_client_oid: {}", e);
                                error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        error!("{}", msg);
                                    }
                                }
                            }
                        }

                        return;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        let msg: String = format!("Failed get_bot_by_exit_sl_client_oid: {}", e);
                        error!("{}", msg);
                        match insert_db_error(pool, exchange, &msg).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                error!("{}", msg);
                            }
                        }
                    }
                }

                // if clientOid in bots entry_id (1 phase)
                match get_bot_by_entry_client_oid(pool, exchange, client_oid).await {
                    Ok(Some(_)) => {
                        // create new stop tp and sl orders
                        match &order.filled_size {
                            Some(filled_size) => {
                                let filled_size_f64: f64 = match filled_size.parse::<f64>() {
                                    Ok(filled_size) => filled_size,
                                    Err(e) => {
                                        let msg: String = format!("Failed parse order.filled_size: {} {}", filled_size, e);
                                        error!("{}", msg);
                                        match insert_db_error(pool, exchange, &msg).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                error!("{}", msg);
                                            }
                                        }
                                        return;
                                    }
                                };
                                match get_total_match_value_by_client_oid(pool, exchange, client_oid).await {
                                    Ok(Some(new_balance)) => {
                                        match update_bot_balance_by_entry_client_oid(pool, exchange, client_oid, &format!("{:.4}", new_balance)).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                let msg: String = format!("Failed update_bot_balance_by_entry_client_oid: {}", e);
                                                error!("{}", msg);
                                                match insert_db_error(pool, exchange, &msg).await {
                                                    Ok(_) => {}
                                                    Err(e) => {
                                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                        error!("{}", msg);
                                                    }
                                                }
                                            }
                                        }

                                        if order.side == "buy" {
                                            let match_price: f64 = new_balance / filled_size_f64;
                                            let trigger_tp_price: f64 = match_price * TP_BUY_PERCENT; // price + 7%
                                            let trigger_sl_price: f64 = match_price * SL_BUY_PERCENT; // price - 5%

                                            let exit_tp_client_oid: String = Uuid::new_v4().to_string();
                                            let exit_sl_client_oid: String = Uuid::new_v4().to_string();

                                            // tp order
                                            let msg_tp_order: serde_json::Value = serde_json::json!({
                                                "clientOid": exit_tp_client_oid,
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
                                            // sl order
                                            let msg_sl_order: serde_json::Value = serde_json::json!({
                                                "clientOid": exit_sl_client_oid,
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

                                            info!("Stop profit order:{}", msg_tp_order);
                                            info!("Stop loss order:{}", msg_sl_order);

                                            // add exit_tp_client_oid by entry_id
                                            match update_exit_tp_client_oid_bot_by_entry_client_oid(pool, exchange, client_oid, &exit_tp_client_oid).await {
                                                Ok(_) => {}
                                                Err(e) => {
                                                    let msg: String = format!("Failed update_exit_tp_client_oid_bot_by_entry_client_oid: {}", e);
                                                    error!("{}", msg);
                                                    match insert_db_error(pool, exchange, &msg).await {
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                            error!("{}", msg);
                                                        }
                                                    }
                                                }
                                            }
                                            // add exit_sl_client_oid by entry_id
                                            match update_exit_sl_client_oid_bot_by_entry_client_oid(pool, exchange, client_oid, &exit_sl_client_oid).await {
                                                Ok(_) => {}
                                                Err(e) => {
                                                    let msg: String = format!("Failed update_exit_sl_client_oid_bot_by_entry_client_oid: {}", e);
                                                    error!("{}", msg);
                                                    match insert_db_error(pool, exchange, &msg).await {
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                            error!("{}", msg);
                                                        }
                                                    }
                                                }
                                            }

                                            let tp_fut = api_v3_hf_margin_stop_order(msg_tp_order);
                                            let sl_fut = api_v3_hf_margin_stop_order(msg_sl_order);

                                            let (tp_res, sl_res) = tokio::join!(tp_fut, sl_fut);

                                            match (&tp_res, &sl_res) {
                                                (Ok(tp_resp), Ok(sl_resp)) => {
                                                    match tp_resp.data {
                                                        Some(ref response_data) => {
                                                            match update_exit_tp_order_id_bot_by_exit_tp_client_oid(pool, exchange, &response_data.order_id, &response_data.client_oid).await {
                                                                Ok(_) => {}
                                                                Err(e) => {
                                                                    let msg: String = format!("Failed update_exit_tp_order_id_bot_by_exit_tp_client_oid: {}", e);
                                                                    error!("{}", msg);
                                                                    match insert_db_error(pool, exchange, &msg).await {
                                                                        Ok(_) => {}
                                                                        Err(e) => {
                                                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                                            error!("{}", msg);
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        None => {}
                                                    }

                                                    match sl_resp.data {
                                                        Some(ref response_data) => {
                                                            match update_exit_sl_order_id_bot_by_exit_sl_client_oid(pool, exchange, &response_data.order_id, &response_data.client_oid).await {
                                                                Ok(_) => {}
                                                                Err(e) => {
                                                                    let msg: String = format!("Failed update_exit_sl_order_id_bot_by_exit_sl_client_oid: {}", e);
                                                                    error!("{}", msg);
                                                                    match insert_db_error(pool, exchange, &msg).await {
                                                                        Ok(_) => {}
                                                                        Err(e) => {
                                                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                                            error!("{}", msg);
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        None => {}
                                                    }

                                                    info!("✅ Both stop orders created: TP={}, SL={}", exit_tp_client_oid, exit_sl_client_oid);
                                                }
                                                (Err(tp_err), Ok(sl_resp)) => {
                                                    match sl_resp.data {
                                                        Some(ref response_data) => {
                                                            match api_v3_hf_margin_stop_order_cancel_by_client_oid(&response_data.client_oid).await {
                                                                Ok(_) => {}
                                                                Err(e) => {
                                                                    let msg: String = format!("Failed api_v3_hf_margin_stop_order_cancel_by_client_oid: {}", e);
                                                                    error!("{}", msg);
                                                                    match insert_db_error(pool, exchange, &msg).await {
                                                                        Ok(_) => {}
                                                                        Err(e) => {
                                                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                                            error!("{}", msg);
                                                                        }
                                                                    }
                                                                }
                                                            };
                                                        }
                                                        None => {}
                                                    }

                                                    match delete_exit_sl_id_bot_by_client_oid(pool, exchange, &exit_sl_client_oid).await {
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            let msg: String = format!("Failed delete_exit_sl_id_bot_by_client_oid: {}", e);
                                                            error!("{}", msg);
                                                            match insert_db_error(pool, exchange, &msg).await {
                                                                Ok(_) => {}
                                                                Err(e) => {
                                                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                                    error!("{}", msg);
                                                                }
                                                            }
                                                        }
                                                    }

                                                    let msg: String = format!("Failed add TP order: {}. SL was cancelled for symmetry.", tp_err);
                                                    error!("{}", msg);
                                                    match insert_db_error(pool, exchange, &msg).await {
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                            error!("{}", msg);
                                                        }
                                                    }
                                                }
                                                (Ok(tp_resp), Err(sl_err)) => {
                                                    match tp_resp.data {
                                                        Some(ref response_data) => match api_v3_hf_margin_stop_order_cancel_by_client_oid(&response_data.client_oid).await {
                                                            Ok(_) => {}
                                                            Err(e) => {
                                                                let msg: String = format!("Failed api_v3_hf_margin_stop_order_cancel_by_client_oid: {}", e);
                                                                error!("{}", msg);
                                                                match insert_db_error(pool, exchange, &msg).await {
                                                                    Ok(_) => {}
                                                                    Err(e) => {
                                                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                                        error!("{}", msg);
                                                                    }
                                                                }
                                                            }
                                                        },
                                                        None => {}
                                                    }

                                                    match delete_exit_tp_id_bot_by_client_oid(pool, exchange, &exit_tp_client_oid).await {
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            let msg: String = format!("Failed delete_exit_tp_id_bot_by_client_oid: {}", e);
                                                            error!("{}", msg);
                                                            match insert_db_error(pool, exchange, &msg).await {
                                                                Ok(_) => {}
                                                                Err(e) => {
                                                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                                    error!("{}", msg);
                                                                }
                                                            }
                                                        }
                                                    }

                                                    let msg: String = format!("Failed add SL order: {}. TP was cancelled for symmetry.", sl_err);
                                                    error!("{}", msg);
                                                    match insert_db_error(pool, exchange, &msg).await {
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                            error!("{}", msg);
                                                        }
                                                    }
                                                }
                                                (Err(tp_err), Err(sl_err)) => {
                                                    let msg: String = format!("Failed add both stop orders: TP={}, SL={}", tp_err, sl_err);
                                                    error!("{}", msg);
                                                    match insert_db_error(pool, exchange, &msg).await {
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                            error!("{}", msg);
                                                        }
                                                    }
                                                    match delete_symbol_bot_by_exit_sl_client_oid(pool, exchange, &exit_sl_client_oid).await {
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            let msg: String = format!("Failed delete_symbol_bot_by_exit_sl_client_oid: {}", e);
                                                            error!("{}", msg);
                                                            match insert_db_error(&pool, &exchange, &msg).await {
                                                                Ok(_) => {}
                                                                Err(e) => {
                                                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                                    error!("{}", msg);
                                                                }
                                                            }
                                                        }
                                                    }
                                                    match delete_exit_sl_id_bot_by_client_oid(pool, exchange, &exit_sl_client_oid).await {
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            let msg: String = format!("Failed delete_exit_sl_id_bot_by_client_oid: {}", e);
                                                            error!("{}", msg);
                                                            match insert_db_error(pool, exchange, &msg).await {
                                                                Ok(_) => {}
                                                                Err(e) => {
                                                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                                    error!("{}", msg);
                                                                }
                                                            }
                                                        }
                                                    }
                                                    match delete_exit_tp_id_bot_by_client_oid(pool, exchange, &exit_tp_client_oid).await {
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            let msg: String = format!("Failed delete_exit_tp_id_bot_by_client_oid: {}", e);
                                                            error!("{}", msg);
                                                            match insert_db_error(pool, exchange, &msg).await {
                                                                Ok(_) => {}
                                                                Err(e) => {
                                                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                                    error!("{}", msg);
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        } else if order.side == "sell" {
                                            let match_price: f64 = new_balance / filled_size_f64;
                                            let trigger_tp_price: f64 = match_price * TP_SELL_PERCENT; // price - 7%
                                            let trigger_sl_price: f64 = match_price * SL_SELL_PERCENT; // price + 5%

                                            let funds_tp: f64 = trigger_tp_price * filled_size_f64;
                                            let funds_sl: f64 = trigger_sl_price * filled_size_f64;

                                            let exit_tp_client_oid: String = Uuid::new_v4().to_string();
                                            let exit_sl_client_oid: String = Uuid::new_v4().to_string();

                                            let msg_tp_order: serde_json::Value = serde_json::json!({
                                                "clientOid": exit_tp_client_oid,
                                                "side": "buy",
                                                "symbol": order.symbol,
                                                "type": "market",
                                                "stop": "loss",
                                                "stopPrice": format_assert(trigger_tp_price, price_increment), // price - 7%
                                                "isIsolated": false,
                                                "autoBorrow": true,
                                                "autoRepay": true,
                                                "timeInForce": "GTC",
                                                "funds": format_assert(funds_tp, quote_increment),
                                            });
                                            let msg_sl_order: serde_json::Value = serde_json::json!({
                                               "clientOid": exit_sl_client_oid,
                                                "side": "buy",
                                                "symbol": order.symbol,
                                                "type": "market",
                                                "stop": "entry",
                                                "stopPrice": format_assert(trigger_sl_price, price_increment), // price + 5%
                                                "isIsolated": false,
                                                "autoBorrow": true,
                                                "autoRepay": true,
                                                "timeInForce": "GTC",
                                                "funds": format_assert(funds_sl, quote_increment),
                                            });

                                            info!("Stop profit order:{}", msg_tp_order);
                                            info!("Stop loss order:{}", msg_sl_order);

                                            // add exit_tp_client_oid by entry_id
                                            match update_exit_tp_client_oid_bot_by_entry_client_oid(pool, exchange, client_oid, &exit_tp_client_oid).await {
                                                Ok(_) => {}
                                                Err(e) => {
                                                    let msg: String = format!("Failed update_exit_tp_client_oid_bot_by_entry_client_oid: {}", e);
                                                    error!("{}", msg);
                                                    match insert_db_error(pool, exchange, &msg).await {
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                            error!("{}", msg);
                                                        }
                                                    }
                                                }
                                            }
                                            // add exit_sl_client_oid by entry_id
                                            match update_exit_sl_client_oid_bot_by_entry_client_oid(pool, exchange, client_oid, &exit_sl_client_oid).await {
                                                Ok(_) => {}
                                                Err(e) => {
                                                    let msg: String = format!("Failed update_exit_sl_client_oid_bot_by_entry_client_oid: {}", e);
                                                    error!("{}", msg);
                                                    match insert_db_error(pool, exchange, &msg).await {
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                            error!("{}", msg);
                                                        }
                                                    }
                                                }
                                            }

                                            let tp_fut = api_v3_hf_margin_stop_order(msg_tp_order);
                                            let sl_fut = api_v3_hf_margin_stop_order(msg_sl_order);
                                            let (tp_res, sl_res) = tokio::join!(tp_fut, sl_fut);

                                            match (&tp_res, &sl_res) {
                                                (Ok(tp_resp), Ok(sl_resp)) => {
                                                    match tp_resp.data {
                                                        Some(ref response_data) => {
                                                            match update_exit_tp_order_id_bot_by_exit_tp_client_oid(pool, exchange, &response_data.order_id, &response_data.client_oid).await {
                                                                Ok(_) => {}
                                                                Err(e) => {
                                                                    let msg: String = format!("Failed update_exit_tp_order_id_bot_by_exit_tp_client_oid: {}", e);
                                                                    error!("{}", msg);
                                                                    match insert_db_error(pool, exchange, &msg).await {
                                                                        Ok(_) => {}
                                                                        Err(e) => {
                                                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                                            error!("{}", msg);
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        None => {}
                                                    }

                                                    match sl_resp.data {
                                                        Some(ref response_data) => {
                                                            match update_exit_sl_order_id_bot_by_exit_sl_client_oid(pool, exchange, &response_data.order_id, &response_data.client_oid).await {
                                                                Ok(_) => {}
                                                                Err(e) => {
                                                                    let msg: String = format!("Failed update_exit_sl_order_id_bot_by_exit_sl_client_oid: {}", e);
                                                                    error!("{}", msg);
                                                                    match insert_db_error(pool, exchange, &msg).await {
                                                                        Ok(_) => {}
                                                                        Err(e) => {
                                                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                                            error!("{}", msg);
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        None => {}
                                                    }

                                                    info!("✅ Both stop orders created: TP={}, SL={}", exit_tp_client_oid, exit_sl_client_oid);
                                                }
                                                (Err(tp_err), Ok(sl_resp)) => {
                                                    match sl_resp.data {
                                                        Some(ref response_data) => match api_v3_hf_margin_stop_order_cancel_by_client_oid(&response_data.client_oid).await {
                                                            Ok(_) => {}
                                                            Err(e) => {
                                                                let msg: String = format!("Failed api_v3_hf_margin_stop_order_cancel_by_client_oid: {}", e);
                                                                error!("{}", msg);
                                                                match insert_db_error(pool, exchange, &msg).await {
                                                                    Ok(_) => {}
                                                                    Err(e) => {
                                                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                                        error!("{}", msg);
                                                                    }
                                                                }
                                                            }
                                                        },
                                                        None => {}
                                                    }

                                                    match delete_exit_sl_id_bot_by_client_oid(pool, exchange, &exit_sl_client_oid).await {
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            let msg: String = format!("Failed delete_exit_sl_id_bot_by_client_oid: {}", e);
                                                            error!("{}", msg);
                                                            match insert_db_error(pool, exchange, &msg).await {
                                                                Ok(_) => {}
                                                                Err(e) => {
                                                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                                    error!("{}", msg);
                                                                }
                                                            }
                                                        }
                                                    }

                                                    let msg: String = format!("Failed add TP order: {}. SL was cancelled for symmetry.", tp_err);
                                                    error!("{}", msg);
                                                    match insert_db_error(pool, exchange, &msg).await {
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                            error!("{}", msg);
                                                        }
                                                    }
                                                }
                                                (Ok(tp_resp), Err(sl_err)) => match tp_resp.data {
                                                    Some(ref response_data) => match api_v3_hf_margin_stop_order_cancel_by_client_oid(&response_data.client_oid).await {
                                                        Ok(_) => {
                                                            match delete_exit_tp_id_bot_by_client_oid(pool, exchange, &exit_tp_client_oid).await {
                                                                Ok(_) => {}
                                                                Err(e) => {
                                                                    let msg: String = format!("Failed delete_exit_tp_id_bot_by_client_oid: {}", e);
                                                                    error!("{}", msg);
                                                                    match insert_db_error(pool, exchange, &msg).await {
                                                                        Ok(_) => {}
                                                                        Err(e) => {
                                                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                                            error!("{}", msg);
                                                                        }
                                                                    }
                                                                }
                                                            }

                                                            let msg: String = format!("Failed add SL order: {}. TP was cancelled for symmetry.", sl_err);
                                                            error!("{}", msg);
                                                            match insert_db_error(pool, exchange, &msg).await {
                                                                Ok(_) => {}
                                                                Err(e) => {
                                                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                                    error!("{}", msg);
                                                                }
                                                            }
                                                        }
                                                        Err(e) => {
                                                            let msg: String = format!("Failed api_v3_hf_margin_stop_order_cancel_by_client_oid: {}", e);
                                                            error!("{}", msg);
                                                            match insert_db_error(pool, exchange, &msg).await {
                                                                Ok(_) => {}
                                                                Err(e) => {
                                                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                                    error!("{}", msg);
                                                                }
                                                            }
                                                        }
                                                    },
                                                    None => {}
                                                },
                                                (Err(tp_err), Err(sl_err)) => {
                                                    let msg: String = format!("Failed add both stop orders: TP={}, SL={}", tp_err, sl_err);
                                                    error!("{}", msg);
                                                    match insert_db_error(pool, exchange, &msg).await {
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                            error!("{}", msg);
                                                        }
                                                    }
                                                    match delete_symbol_bot_by_exit_sl_client_oid(pool, exchange, &exit_sl_client_oid).await {
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            let msg: String = format!("Failed delete_symbol_bot_by_exit_sl_client_oid: {}", e);
                                                            error!("{}", msg);
                                                            match insert_db_error(pool, exchange, &msg).await {
                                                                Ok(_) => {}
                                                                Err(e) => {
                                                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                                    error!("{}", msg);
                                                                }
                                                            }
                                                        }
                                                    }
                                                    match delete_exit_sl_id_bot_by_client_oid(pool, exchange, &exit_sl_client_oid).await {
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            let msg: String = format!("Failed delete_exit_sl_id_bot_by_client_oid: {}", e);
                                                            error!("{}", msg);

                                                            match insert_db_error(pool, exchange, &msg).await {
                                                                Ok(_) => {}
                                                                Err(e) => {
                                                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                                    error!("{}", msg);
                                                                }
                                                            }
                                                        }
                                                    }
                                                    match delete_exit_tp_id_bot_by_client_oid(pool, exchange, &exit_tp_client_oid).await {
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            let msg: String = format!("Failed delete_exit_tp_id_bot_by_client_oid: {}", e);
                                                            error!("{}", msg);

                                                            match insert_db_error(pool, exchange, &msg).await {
                                                                Ok(_) => {}
                                                                Err(e) => {
                                                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                                    error!("{}", msg);
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Ok(None) => {
                                        error!("No records found or error occurred");
                                    }
                                    Err(e) => {
                                        let msg: String = format!("Failed get_total_match_value_by_client_oid: {}", e);
                                        error!("{}", msg);
                                        match insert_db_error(pool, exchange, &msg).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                error!("{}", msg);
                                            }
                                        }
                                    }
                                }
                                // delete entry_id from db
                                match set_null_entry_client_oid_by_entry_client_oid(pool, exchange, client_oid).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed set_null_entry_client_oid_by_entry_client_oid: {}", e);
                                        error!("{}", msg);
                                        match insert_db_error(pool, exchange, &msg).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                error!("{}", msg);
                                            }
                                        }
                                    }
                                }
                            }
                            None => {}
                        }
                        return;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        let msg: String = format!("Failed get_bot_by_entry_client_oid: {}", e);
                        error!("{}", msg);
                        match insert_db_error(pool, exchange, &msg).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                error!("{}", msg);
                            }
                        }
                    }
                }
            }
        }
        None => {}
    }
}

pub async fn handle_position_event(position: PositionData, pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str) {
    match upsert_position_ratio(pool, exchange, position.debt_ratio, position.total_asset, &position.margin_coefficient_total_asset, &position.total_debt).await {
        Ok(_) => {}
        Err(e) => {
            let msg: String = format!("Failed to upsert margin account state: {}", e);
            error!("{}", msg);
            match insert_db_error(pool, exchange, &msg).await {
                Ok(_) => {}
                Err(e) => {
                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                    error!("{}", msg);
                }
            }
        }
    }

    for (symbol, amount) in &position.debt_list {
        match upsert_position_debt(pool, exchange, symbol, amount).await {
            Ok(_) => {}
            Err(e) => {
                let msg: String = format!("Failed to insert debt margin account state: {}", e);
                error!("{}", msg);
                match insert_db_error(pool, exchange, &msg).await {
                    Ok(_) => {}
                    Err(e) => {
                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                        error!("{}", msg);
                    }
                }
            }
        }
    }
    for (symbol, symbol_info) in &position.asset_list {
        match upsert_position_asset(pool, exchange, symbol, &symbol_info.total, &symbol_info.available, &symbol_info.hold).await {
            Ok(_) => {}
            Err(e) => {
                let msg: String = format!("Failed to insert asset margin account state: {}", e);
                error!("{}", msg);
                match insert_db_error(pool, exchange, &msg).await {
                    Ok(_) => {}
                    Err(e) => {
                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                        error!("{}", msg);
                    }
                }
            }
        }
    }
    // repay borrow
    for (asset, token_liability_str) in &position.debt_list {
        match (token_liability_str.parse::<f64>(), position.asset_list.get(asset)) {
            (Ok(token_liability), Some(asset_info)) => match asset_info.available.parse::<f64>() {
                Ok(available) => {
                    if token_liability > 0.0 {
                        if available >= token_liability {
                            info!("Can repay {} {} liability with available {}", token_liability, asset, available);

                            match create_repay_order(asset, token_liability_str).await {
                                Ok(_) => {}
                                Err(e) => {
                                    let msg: String = format!("Failed to repay liability: {}", e);
                                    error!("{}", msg);
                                    match insert_db_error(pool, exchange, &msg).await {
                                        Ok(_) => {}
                                        Err(e) => {
                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                            error!("{}", msg);
                                        }
                                    }
                                }
                            }
                        } else if available > 0.0 {
                            info!("Can partially repay {} {} liability with available {}", token_liability, asset, available);

                            match create_repay_order(asset, &asset_info.available).await {
                                Ok(_) => {}
                                Err(e) => {
                                    let msg: String = format!("Failed to partially repay debt: {}", e);
                                    error!("{}", msg);
                                    match insert_db_error(pool, exchange, &msg).await {
                                        Ok(_) => {}
                                        Err(e) => {
                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                            error!("{}", msg);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    let msg: String = format!("Failed to parse available balance for {} {}", asset, e);
                    error!("{}", msg);
                    match insert_db_error(pool, exchange, &msg).await {
                        Ok(_) => {}
                        Err(e) => {
                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                            error!("{}", msg);
                        }
                    }
                }
            },
            (Err(e), _) => {}
            (_, None) => {}
        }
    }
}

pub async fn handle_advanced_orders(order: AdvancedOrders, pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str) {
    info!("{:?}", order);
    match order.error {
        Some(_) => {
            let msg: String = format!("Got error on stop order : {:?}", order);
            error!("{}", msg);
            match insert_db_error(pool, exchange, &msg).await {
                Ok(_) => {}
                Err(e) => {
                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                    error!("{}", msg);
                }
            }

            const MAX_RETRIES: u32 = 1000;
            let mut attempt = 0;
            loop {
                tokio::time::sleep(Duration::from_millis(RETRY_DELAY_BASE * attempt as u64)).await;
                attempt += 1;

                let order_id_clone: String = order.order_id.clone();
                let stop_clone: String = order.stop.clone();
                let side_clone: String = order.side.clone();
                let symbol_clone: String = order.symbol.clone();
                let funds_clone: Option<String> = order.funds.clone();
                let size_clone: Option<String> = order.size.clone();
                let new_exit_client_oid: String = Uuid::new_v4().to_string();

                let order_result = match stop_clone.as_str() {
                    "loss" => {
                        // need find sl
                        match update_exit_sl_client_oid_bot_by_exit_sl_order_id(pool, exchange, &order_id_clone, &new_exit_client_oid).await {
                            Ok(_) => match side_clone.as_str() {
                                "buy" => match funds_clone {
                                    Some(funds) => make_hf_funds_margin_order(pool, exchange, &new_exit_client_oid, &side_clone, &symbol_clone, funds, "market".to_string()).await,
                                    None => {
                                        let msg: String = format!("Fail parse funds order:{} new_exit_sl_client_oid:{} funds_clone:{:.?}", order_id_clone, new_exit_client_oid, funds_clone,);
                                        error!("{}", msg);
                                        match insert_db_error(pool, exchange, &msg).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                error!("{}", msg);
                                            }
                                        }
                                        return;
                                    }
                                },
                                "sell" => match size_clone {
                                    Some(size) => make_hf_size_margin_order(pool, exchange, &new_exit_client_oid, &side_clone, &symbol_clone, size, "market".to_string()).await,
                                    None => {
                                        let msg: String = format!("Fail parse size order:{} new_exit_sl_client_oid:{} size_clone:{:.?}", order_id_clone, new_exit_client_oid, size_clone,);
                                        error!("{}", msg);
                                        match insert_db_error(pool, exchange, &msg).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                error!("{}", msg);
                                            }
                                        }
                                        return;
                                    }
                                },
                                _ => {
                                    let msg: String = format!("Fail match side_clone:{}", side_clone);
                                    error!("{}", msg);
                                    match insert_db_error(pool, exchange, &msg).await {
                                        Ok(_) => {}
                                        Err(e) => {
                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                            error!("{}", msg);
                                        }
                                    }

                                    return;
                                }
                            },
                            Err(e) => {
                                let msg: String = format!("Failed save bot info: order_id_clone:{} new_exit_sl_client_oid:{}, {}", order_id_clone, new_exit_client_oid, e);
                                error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        error!("{}", msg);
                                    }
                                }
                                if attempt >= MAX_RETRIES {
                                    return;
                                }
                                continue;
                            }
                        }
                    }
                    "entry" => {
                        // need find tp
                        match update_exit_tp_client_oid_bot_by_exit_tp_order_id(pool, exchange, &order_id_clone, &new_exit_client_oid).await {
                            Ok(_) => match side_clone.as_str() {
                                "buy" => match funds_clone {
                                    Some(funds) => make_hf_funds_margin_order(pool, exchange, &new_exit_client_oid, &side_clone, &symbol_clone, funds, "market".to_string()).await,
                                    None => {
                                        let msg: String = format!("Fail parse funds_clone order:{} new_exit_tp_client_oid:{} funds_clone:{:.?}", order_id_clone, new_exit_client_oid, funds_clone,);
                                        error!("{}", msg);
                                        match insert_db_error(pool, exchange, &msg).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                error!("{}", msg);
                                            }
                                        }
                                        return;
                                    }
                                },
                                "sell" => match size_clone {
                                    Some(size) => make_hf_size_margin_order(pool, exchange, &new_exit_client_oid, &side_clone, &symbol_clone, size, "market".to_string()).await,
                                    None => {
                                        let msg: String = format!("Fail parse size_clone order:{} new_exit_tp_client_oid:{} size_clone:{:.?}", order_id_clone, new_exit_client_oid, size_clone,);
                                        error!("{}", msg);
                                        match insert_db_error(pool, exchange, &msg).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                error!("{}", msg);
                                            }
                                        }

                                        return;
                                    }
                                },
                                _ => {
                                    let msg: String = format!("Fail match side_clone:{}", side_clone);
                                    error!("{}", msg);
                                    match insert_db_error(pool, exchange, &msg).await {
                                        Ok(_) => {}
                                        Err(e) => {
                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                            error!("{}", msg);
                                        }
                                    }

                                    return;
                                }
                            },
                            Err(e) => {
                                let msg: String = format!("Failed save bot info: order_id_clone:{} new_exit_tp_client_oid:{}, {}", order_id_clone, new_exit_client_oid, e);
                                error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        error!("{}", msg);
                                    }
                                }
                                if attempt >= MAX_RETRIES {
                                    return;
                                }
                                continue;
                            }
                        }
                    }
                    _ => {
                        let msg: String = format!("Fail match stop_clone:{}", stop_clone);
                        error!("{}", msg);
                        match insert_db_error(pool, exchange, &msg).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                error!("{}", msg);
                            }
                        }

                        return;
                    }
                };

                match order_result {
                    Ok(_) => {
                        info!("✅ Order re-placed: {} {} (attempt {}/{})", order_id_clone, new_exit_client_oid, attempt, MAX_RETRIES);
                        break;
                    }
                    Err(e) => {
                        error!("❌ Order failed: {} {} (attempt {}/{}) {}", order_id_clone, new_exit_client_oid, attempt, MAX_RETRIES, e);
                        match insert_db_error(pool, exchange, &e.to_string()).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                error!("{}", msg);
                            }
                        }
                        if attempt >= MAX_RETRIES {
                            return;
                        }
                    }
                }
            }
        }
        None => {}
    }
}

pub async fn make_random_trade(pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str, balance_funds: f64, trade_bot_id: i32) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    const MAX_RETRIES: u32 = 10;
    let mut attempt = 0;
    // random sections

    loop {
        attempt += 1;
        match get_random_symbol(pool, exchange).await {
            Ok(Some(tradeable)) => {
                let symbol_info: Symbol = match fetch_symbol_info_by_symbol(pool, exchange, &tradeable.symbol).await {
                    Ok(Some(i)) => i,
                    Ok(None) => {
                        let msg: String = format!("Symbol info not found for {}", tradeable.symbol);
                        error!("{}", msg);
                        match insert_db_error(pool, exchange, &msg).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                error!("{}", msg);
                            }
                        }
                        if attempt >= MAX_RETRIES {
                            return Ok(());
                        }
                        continue;
                    }
                    Err(e) => {
                        let msg: String = format!("Symbol info not found for {}", &tradeable.symbol);
                        error!("{}", msg);
                        match insert_db_error(&pool, &exchange, &msg).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                error!("{}", msg);
                            }
                        }
                        return Err(e.into());
                    }
                };

                let entry_client_oid: String = Uuid::new_v4().to_string();

                match update_bot_entry_client_oid_by_id(pool, exchange, Some(&tradeable.symbol), Some(&entry_client_oid), trade_bot_id).await {
                    Ok(_) => {}
                    Err(e) => {
                        let msg: String = format!("Failed save bot info: entry_client_oid:{} trade_bot.id:{}, {}", entry_client_oid, trade_bot_id, e);
                        error!("{}", msg);
                        match insert_db_error(pool, exchange, &msg).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                error!("{}", msg);
                            }
                        }
                        if attempt >= MAX_RETRIES {
                            return Ok(());
                        }
                        continue;
                    }
                }

                let order_result = match get_random_side().as_str() {
                    "sell" => {
                        let base_increment: f64 = match symbol_info.base_increment.parse::<f64>() {
                            Ok(v) => v,
                            Err(e) => {
                                let msg: String = format!("Failed parse base_increment: {} {}", symbol_info.base_increment, e);
                                error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        error!("{}", msg);
                                    }
                                }
                                if attempt >= MAX_RETRIES {
                                    return Ok(());
                                }
                                continue;
                            }
                        };
                        let token_price_str: String = match get_ticker_price(&tradeable.symbol).await {
                            Ok(p) => p,
                            Err(e) => {
                                let msg: String = format!("Failed get price: {} {}", tradeable.symbol, e);
                                error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        error!("{}", msg);
                                    }
                                }
                                if attempt >= MAX_RETRIES {
                                    return Ok(());
                                }
                                continue;
                            }
                        };
                        let token_price: f64 = match token_price_str.parse::<f64>() {
                            Ok(v) => v,
                            Err(e) => {
                                let msg: String = format!("Failed parse price: {} {}", token_price_str, e);
                                error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        error!("{}", msg);
                                    }
                                }
                                if attempt >= MAX_RETRIES {
                                    return Ok(());
                                }
                                continue;
                            }
                        };
                        let token_size: f64 = balance_funds / token_price;
                        make_hf_size_margin_order(pool, exchange, &entry_client_oid, "sell", &tradeable.symbol, format_assert(token_size, base_increment), "market".to_string()).await
                    }
                    "buy" => {
                        let quote_increment: f64 = match symbol_info.quote_increment.parse::<f64>() {
                            Ok(v) => v,
                            Err(e) => {
                                let msg: String = format!("Failed parse quote_increment: {} {}", symbol_info.quote_increment, e);
                                error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        error!("{}", msg);
                                    }
                                }
                                if attempt >= MAX_RETRIES {
                                    return Ok(());
                                }
                                continue;
                            }
                        };
                        make_hf_funds_margin_order(pool, exchange, &entry_client_oid, "buy", &tradeable.symbol, format_assert(balance_funds, quote_increment), "market".to_string()).await
                    }
                    _ => {
                        if attempt >= MAX_RETRIES {
                            return Ok(());
                        }
                        continue;
                    }
                };

                match order_result {
                    Ok(_) => {
                        info!("✅ Order placed: {} {} (attempt {}/{})", entry_client_oid, trade_bot_id, attempt, MAX_RETRIES);
                        return Ok(());
                    }
                    Err(e) => {
                        match update_bot_entry_client_oid_by_id(pool, exchange, None, None, trade_bot_id).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed update_bot_entry_client_oid_by_id: {}", e);
                                error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        error!("{}", msg);
                                    }
                                }
                            }
                        }
                        let msg: String = format!("❌ Order failed (attempt {}/{}): {} - {}", attempt, MAX_RETRIES, tradeable.symbol, e);
                        error!("{}", msg);
                        match insert_db_error(pool, exchange, &e.to_string()).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                error!("{}", msg);
                            }
                        }
                        if attempt >= MAX_RETRIES {
                            return Ok(());
                        }
                        tokio::time::sleep(Duration::from_millis(RETRY_DELAY_BASE * attempt as u64)).await;
                        continue;
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                let msg: String = format!("Failed get_random_symbol: {}", e);
                error!("{}", msg);
                match insert_db_error(pool, exchange, &msg).await {
                    Ok(_) => {}
                    Err(e) => {
                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                        error!("{}", msg);
                    }
                }
            }
        }
    }
}

pub async fn make_hf_funds_margin_order(
    pool: &sqlx::Pool<sqlx::Postgres>,
    exchange: &str,
    client_oid: &str,
    side: &str,
    symbol: &str,
    funds: String,
    type_: String,
) -> Result<MakeOrderRes, Box<dyn std::error::Error + Send + Sync>> {
    // only for buy orders
    let args_time_in_force: &str = "GTC";
    let auto_borrow: bool = true;
    let auto_repay: bool = false;

    match insert_db_msgsend(pool, exchange, Some(symbol), Some(side), None, Some(&funds), None, Some(args_time_in_force), Some(&type_), Some(&auto_borrow), Some(&auto_repay), Some(client_oid), None)
        .await
    {
        Ok(_) => {}
        Err(e) => {
            let msg: String = format!("Failed insert_db_msgsend: {}", e);
            error!("{}", msg);
            match insert_db_error(pool, exchange, &msg).await {
                Ok(_) => {}
                Err(e) => {
                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                    error!("{}", msg);
                }
            }
        }
    }
    let msg: serde_json::Value = serde_json::json!({
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

    match add_api_v3_hf_margin_order(msg.clone()).await {
        Ok(data) => {
            if data.code != "200000" {
                let msg: String = format!("Make order was error: {} {} {:?}", symbol, data.code, data.msg);
                error!("{}", msg);
                match insert_db_error(pool, exchange, &msg).await {
                    Ok(_) => {}
                    Err(e) => {
                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                        error!("{}", msg);
                    }
                }
                Err(msg.into())
            } else {
                Ok(data)
            }
        }
        Err(e) => {
            let msg: String = format!("Failed to send order: {}", e);
            error!("{}", msg);
            match insert_db_error(pool, exchange, &msg).await {
                Ok(_) => {}
                Err(e) => {
                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                    error!("{}", msg);
                }
            }
            Err(msg.into())
        }
    }
}

pub async fn make_hf_size_margin_order(
    pool: &sqlx::Pool<sqlx::Postgres>,
    exchange: &str,
    client_oid: &str,
    side: &str,
    symbol: &str,
    size: String,
    type_: String,
) -> Result<MakeOrderRes, Box<dyn std::error::Error + Send + Sync>> {
    // only for sell orders
    let args_time_in_force: &str = "GTC";
    let auto_borrow: bool = true;
    let auto_repay: bool = false;

    match insert_db_msgsend(pool, exchange, Some(symbol), Some(side), Some(&size), None, None, Some(args_time_in_force), Some(&type_), Some(&auto_borrow), Some(&auto_repay), Some(client_oid), None)
        .await
    {
        Ok(_) => {}
        Err(e) => {
            let msg: String = format!("Failed insert_db_msgsend: {}", e);
            error!("{}", msg);
            match insert_db_error(pool, exchange, &msg).await {
                Ok(_) => {}
                Err(e) => {
                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                    error!("{}", msg);
                }
            }
        }
    }
    let msg: serde_json::Value = serde_json::json!({
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

    match add_api_v3_hf_margin_order(msg.clone()).await {
        Ok(data) => {
            if data.code != "200000" {
                let msg: String = format!("Make order was error: {} {} {:?}", symbol, data.code, data.msg);
                error!("{}", msg);
                match insert_db_error(pool, exchange, &msg).await {
                    Ok(_) => {}
                    Err(e) => {
                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                        error!("{}", msg);
                    }
                }
                Err(msg.into())
            } else {
                Ok(data)
            }
        }
        Err(e) => {
            let msg: String = format!("Failed to send order: {}", e);
            error!("{}", msg);
            match insert_db_error(pool, exchange, &msg).await {
                Ok(_) => {}
                Err(e) => {
                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                    error!("{}", msg);
                }
            }
            Err(msg.into())
        }
    }
}
