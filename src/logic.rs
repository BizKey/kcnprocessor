use fastrand;

use crate::api::db::{
    delete_exit_sl_id_bot_by_client_oid, delete_exit_tp_id_bot_by_client_oid, delete_symbol_bot_by_exit_sl_client_oid, fetch_symbol_info_by_symbol, get_all_bots_for_trade,
    get_bot_by_entry_client_oid, get_bot_by_exit_sl_client_oid, get_bot_by_exit_tp_client_oid, get_random_symbol, get_total_match_value_by_client_oid, insert_db_balance, insert_db_error,
    insert_db_event, insert_db_msgsend, insert_db_orderevent, set_null_entry_client_oid_by_entry_client_oid, update_balance_bot_by_exit_sl_client_oid, update_balance_bot_by_exit_tp_client_oid,
    update_bot_balance_by_entry_client_oid, update_bot_entry_client_oid_by_id, update_exit_sl_client_oid_bot_by_entry_client_oid, update_exit_sl_client_oid_bot_by_exit_sl_order_id,
    update_exit_sl_order_id_bot_by_exit_sl_client_oid, update_exit_tp_client_oid_bot_by_entry_client_oid, update_exit_tp_client_oid_bot_by_exit_tp_order_id,
    update_exit_tp_order_id_bot_by_exit_tp_client_oid, upsert_position_asset, upsert_position_debt, upsert_position_ratio,
};
use crate::api::models::{AdvancedOrders, BalanceData, KuCoinMessage, MakeOrderRes, OrderData, PositionData, Symbol};
use crate::api::requests::{
    add_api_v3_hf_margin_order, api_v3_hf_margin_stop_order, api_v3_hf_margin_stop_order_cancel_by_client_oid, create_repay_order, get_all_margin_accounts, get_ticker_price, sent_account_transfer,
};
use log;
use serde::Deserialize;
use tokio::time::{Duration, sleep};
use uuid::Uuid;

const TP_BUY_PERCENT: f64 = 1.07; // +7%
const SL_BUY_PERCENT: f64 = 0.95; // -5%
const TP_SELL_PERCENT: f64 = 0.93; // -7%
const SL_SELL_PERCENT: f64 = 1.05; // +5%
const RETRY_DELAY_BASE: u64 = 500;
const BOT_INIT_DELAY: Duration = Duration::from_secs(5);
const AUTO_CLEAN_DELAY: Duration = Duration::from_secs(1);

pub fn get_random_side() -> String {
    if fastrand::bool() { "buy".to_string() } else { "sell".to_string() }
}

pub fn build_subscription() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({"id":"subscribe_orders","type":"subscribe","topic":"/spotMarket/tradeOrdersV2","response":true,"privateChannel":"true"}),
        serde_json::json!({"id":"subscribe_stop_orders","type":"subscribe","topic":"/spotMarket/advancedOrders","response":true,"privateChannel":"true"}),
        serde_json::json!({"id":"subscribe_balance","type":"subscribe","topic":"/account/balance","response":true,"privateChannel":"true"}),
        serde_json::json!({"id":"subscribe_position","type":"subscribe","topic":"/margin/position","response":true,"privateChannel":"true"}),
    ]
}

pub fn format_assert(size: f64, increment: f64) -> String {
    let precision = if increment >= 1.0 { 0 } else { (increment.recip().log10().ceil() as usize).min(10) };
    let rounded = (size / increment).round() * increment;
    format!("{:.prec$}", rounded, prec = precision)
}

pub async fn create_init_orders(pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match get_all_bots_for_trade(pool, exchange).await {
        Ok(trade_bots) => {
            for trade_bot in trade_bots.iter() {
                sleep(BOT_INIT_DELAY).await;
                match trade_bot.balance.parse::<f64>() {
                    Ok(token_funds) => match make_random_trade(pool, exchange, token_funds, trade_bot.id).await {
                        Ok(_) => {}
                        Err(e) => {
                            let msg: String = format!("Error in make_random_trade: {}", e);
                            log::error!("{}", msg);
                            match insert_db_error(pool, exchange, &msg).await {
                                Ok(_) => {}
                                Err(e) => {
                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                    log::error!("{}", msg);
                                }
                            }
                            return Err(e.into());
                        }
                    },
                    Err(e) => {
                        let msg: String = format!("Failed parse balance: {} {}", trade_bot.balance, e);
                        log::error!("{}", msg);
                        match insert_db_error(pool, exchange, &msg).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                log::error!("{}", msg);
                            }
                        }
                        return Err(e.into());
                    }
                }
            }
            log::info!("All bots initialized!");
            return Ok(());
        }
        Err(e) => {
            let msg: String = format!("Failed get_all_bots_for_trade: {}", e);
            log::error!("{}", msg);
            match insert_db_error(pool, exchange, &msg).await {
                Ok(_) => {}
                Err(e) => {
                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                    log::error!("{}", msg);
                }
            }
            return Err(e.into());
        }
    }
}

pub async fn auto_clean_account(pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    match get_all_margin_accounts().await {
        Ok(accounts) => {
            sleep(AUTO_CLEAN_DELAY).await;
            let mut passed: bool = true;
            for account in accounts.accounts.iter() {
                let token_liability: f64 = match account.liability.parse::<f64>() {
                    Ok(token_liability) => token_liability,
                    Err(e) => {
                        let msg: String = format!("Failed parse price: {} {}", account.liability, e);
                        log::error!("{}", msg);
                        match insert_db_error(pool, exchange, &msg).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                log::error!("{}", msg);
                            }
                        }
                        passed = false;
                        continue;
                    }
                };
                let token_available: f64 = match account.available.parse::<f64>() {
                    Ok(token_liability) => token_liability,
                    Err(e) => {
                        let msg: String = format!("Failed parse price: {} {}", account.available, e);
                        log::error!("{}", msg);
                        match insert_db_error(pool, exchange, &msg).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                log::error!("{}", msg);
                            }
                        }
                        passed = false;
                        continue;
                    }
                };

                if token_liability > 0.0 {
                    passed = false;
                    if token_available >= token_liability {
                        log::info!("Can repay {} {} liability with available {}", account.liability, &account.currency, account.available);

                        match create_repay_order(&account.currency, &account.liability).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed to repay liability: {}", e);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                    }
                                }
                                continue;
                            }
                        }
                    } else if token_available > 0.0 {
                        log::info!("Can partially repay {} {} liability with available {}", account.liability, &account.currency, account.available);

                        match create_repay_order(&account.currency, &account.available).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed to partially repay debt: {}", e);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                    }
                                }
                                continue;
                            }
                        }
                    } else if account.currency != "USDT" && token_available == 0.0 {
                        // buy stock by market liability
                        let trade_symbol: &String = &(account.currency.clone() + "-USDT");
                        let client_oid: String = Uuid::new_v4().to_string();
                        let symbol_info: Symbol = match fetch_symbol_info_by_symbol(pool, exchange, trade_symbol).await {
                            Ok(Some(info)) => info,
                            Ok(None) => {
                                let msg: String = format!("Symbol info not found for {}", trade_symbol);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                    }
                                }
                                continue;
                            }
                            Err(e) => {
                                let msg: String = format!("Symbol info not found for {} {}", trade_symbol, e);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                    }
                                }
                                continue;
                            }
                        };
                        // liability debt in tokens
                        // get price token

                        let token_price: f64 = match get_ticker_price(trade_symbol).await {
                            Ok(token_price_str) => match token_price_str.parse::<f64>() {
                                Ok(token_price) => token_price,
                                Err(e) => {
                                    let msg: String = format!("Failed parse price: {} {}", token_price_str, e);
                                    log::error!("{}", msg);
                                    match insert_db_error(pool, exchange, &msg).await {
                                        Ok(_) => {}
                                        Err(e) => {
                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                            log::error!("{}", msg);
                                        }
                                    }
                                    continue;
                                }
                            },
                            Err(e) => {
                                let msg: String = format!("Failed get price: {} {}", trade_symbol, e);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                    }
                                }
                                continue;
                            }
                        };

                        log::info!("Successfully get token:{} price:{}", &trade_symbol, token_price);

                        // calc price token on amount liability token
                        let token_funds: f64 = token_price * token_liability;

                        let quote_increment: f64 = match symbol_info.quote_increment.parse::<f64>() {
                            Ok(quote_increment) => quote_increment,
                            Err(e) => {
                                let msg: String = format!("Failed parse quote_increment: {} {}", symbol_info.quote_increment, e);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                    }
                                }
                                continue;
                            }
                        };
                        // parse min_funds to int
                        let min_funds: f64 = match &symbol_info.min_funds {
                            Some(min_funds_str) => match min_funds_str.parse::<f64>() {
                                Ok(min_funds) => min_funds,
                                Err(e) => {
                                    let msg: String = format!("Failed parse min_funds: {:?} {}", symbol_info.min_funds, e);
                                    log::error!("{}", msg);
                                    match insert_db_error(pool, exchange, &msg).await {
                                        Ok(_) => {}
                                        Err(e) => {
                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                            log::error!("{}", msg);
                                        }
                                    }
                                    continue;
                                }
                            },
                            None => {
                                let msg: String = format!("min_funds is None for symbol {}", trade_symbol);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                    }
                                }
                                continue;
                            }
                        };
                        let base_min_size: f64 = match symbol_info.base_min_size.parse::<f64>() {
                            Ok(base_min_size) => base_min_size,
                            Err(e) => {
                                let msg: String = format!("Failed parse base_min_size: {} {}", symbol_info.base_min_size, e);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                    }
                                }
                                continue;
                            }
                        };

                        // calc price token on amount base_min_size token
                        let min_funds_by_size: f64 = token_price * base_min_size;

                        if token_funds <= min_funds.max(min_funds_by_size) {
                            match make_hf_funds_margin_order(
                                pool,
                                exchange,
                                &client_oid,
                                "buy",
                                trade_symbol,
                                format_assert(min_funds.max(min_funds_by_size), quote_increment),
                                "market".to_string(),
                                false,
                                false,
                            )
                            .await
                            {
                                Ok(_) => {}
                                Err(e) => {
                                    let msg: String = format!("Failed make_hf_funds_margin_order: {}", e);
                                    log::error!("{}", msg);

                                    match insert_db_error(pool, exchange, &msg).await {
                                        Ok(_) => {}
                                        Err(e) => {
                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                            log::error!("{}", msg);
                                        }
                                    }
                                    continue;
                                }
                            }
                        } else {
                            match make_hf_funds_margin_order(pool, exchange, &client_oid, "buy", trade_symbol, format_assert(token_funds, quote_increment), "market".to_string(), false, false).await {
                                Ok(_) => {}
                                Err(e) => {
                                    let msg: String = format!("Failed make_hf_funds_margin_order: {}", e);
                                    log::error!("{}", msg);
                                    match insert_db_error(pool, exchange, &msg).await {
                                        Ok(_) => {}
                                        Err(e) => {
                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                            log::error!("{}", msg);
                                        }
                                    }
                                    continue;
                                }
                            }
                        };
                    }
                } else if account.currency != "USDT" && token_available > 0.0 {
                    passed = false;
                    // sell stocks by market available/ works
                    let client_oid: String = Uuid::new_v4().to_string();
                    let trade_symbol: &String = &(account.currency.clone() + "-USDT");

                    let symbol_info: Symbol = match fetch_symbol_info_by_symbol(pool, exchange, trade_symbol).await {
                        Ok(Some(symbol_info)) => symbol_info,
                        Ok(None) => {
                            let msg: String = format!("Symbol info not found for {}", trade_symbol);
                            log::error!("{}", msg);
                            match insert_db_error(pool, exchange, &msg).await {
                                Ok(_) => {}
                                Err(e) => {
                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                    log::error!("{}", msg);
                                }
                            }
                            continue;
                        }
                        Err(e) => {
                            let msg: String = format!("Symbol info not found for {} {}", trade_symbol, e);
                            log::error!("{}", msg);
                            match insert_db_error(pool, exchange, &msg).await {
                                Ok(_) => {}
                                Err(e) => {
                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                    log::error!("{}", msg);
                                }
                            }
                            continue;
                        }
                    };

                    let base_increment: f64 = match symbol_info.base_increment.parse::<f64>() {
                        Ok(base_increment) => base_increment,
                        Err(e) => {
                            let msg: String = format!("Failed parse base_increment: {} {}", symbol_info.base_increment, e);
                            log::error!("{}", msg);
                            match insert_db_error(pool, exchange, &msg).await {
                                Ok(_) => {}
                                Err(e) => {
                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                    log::error!("{}", msg);
                                }
                            }
                            continue;
                        }
                    };
                    // get price token
                    let token_price: f64 = match get_ticker_price(trade_symbol).await {
                        Ok(token_price_str) => match token_price_str.parse::<f64>() {
                            Ok(token_price) => token_price,
                            Err(e) => {
                                let msg: String = format!("Failed parse price: {} {}", token_price_str, e);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                    }
                                }
                                continue;
                            }
                        },
                        Err(e) => {
                            let msg: String = format!("Failed get price: {} {}", trade_symbol, e);
                            log::error!("{}", msg);
                            match insert_db_error(pool, exchange, &msg).await {
                                Ok(_) => {}
                                Err(e) => {
                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                    log::error!("{}", msg);
                                }
                            }
                            continue;
                        }
                    };

                    log::info!("Successfully get token:{} price:{}", &trade_symbol, token_price);

                    let base_min_size: f64 = match symbol_info.base_min_size.parse::<f64>() {
                        Ok(base_min_size) => base_min_size,
                        Err(e) => {
                            let msg: String = format!("Failed parse base_min_size: {} {}", symbol_info.base_min_size, e);
                            log::error!("{}", msg);
                            match insert_db_error(pool, exchange, &msg).await {
                                Ok(_) => {}
                                Err(e) => {
                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                    log::error!("{}", msg);
                                }
                            }
                            continue;
                        }
                    };
                    let quote_min_size: f64 = match symbol_info.quote_min_size.parse::<f64>() {
                        Ok(quote_min_size) => quote_min_size,
                        Err(e) => {
                            let msg: String = format!("Failed parse quote_min_size: {} {}", symbol_info.quote_min_size, e);
                            log::error!("{}", msg);
                            match insert_db_error(pool, exchange, &msg).await {
                                Ok(_) => {}
                                Err(e) => {
                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                    log::error!("{}", msg);
                                }
                            }
                            continue;
                        }
                    };
                    if token_available <= base_min_size || (token_price * token_available) <= quote_min_size {
                        match sent_account_transfer(&account.currency.clone(), &account.available, "INTERNAL", "MARGIN", "TRADE").await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed send {} to TRADE from MARGIN on {} {}", &account.currency.clone(), &account.available, e);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                    }
                                }
                                continue;
                            }
                        }
                    } else {
                        match make_hf_size_margin_order(pool, exchange, &client_oid, "sell", trade_symbol, format_assert(token_available, base_increment), "market".to_string(), false, false).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed make_hf_size_margin_order: {}", e);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                    }
                                }
                                continue;
                            }
                        }
                    }
                }
            }
            if passed {
                return Ok(true);
            } else {
                return Ok(false);
            }
        }
        Err(e) => {
            let msg: String = format!("Failed to get margin accounts {}", e);
            log::error!("{}", msg);
            match insert_db_error(pool, exchange, &msg).await {
                Ok(_) => {}
                Err(e) => {
                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                    log::error!("{}", msg);
                }
            }
            return Ok(false);
        }
    }
}

pub async fn handle_trade_order_event(order: OrderData, pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match insert_db_orderevent(pool, exchange, &order).await {
        Ok(_) => {
            log::info!("{:.?}", order);
        }
        Err(e) => {
            let msg: String = format!("Failed insert_db_orderevent: {:.?} {}", order, e);
            log::error!("{}", msg);
            match insert_db_error(pool, exchange, &msg).await {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                    log::error!("{}", msg);
                    return Err(e);
                }
            }
        }
    }

    let client_oid = match &order.client_oid {
        Some(client_oid) => client_oid,
        None => {
            let msg: String = format!("client_oid in order is none: {:.?}", order);
            log::error!("{}", msg);
            match insert_db_error(pool, exchange, &msg).await {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                    log::error!("{}", msg);
                    return Err(e);
                }
            }
        }
    };

    if (order.type_ == "match" || order.type_ == "canceled") && (order.remain_size == Some("0".to_string()) || order.remain_funds == Some("0".to_string())) {
    } else {
        return Ok(());
    }

    let symbol_info: Symbol = match fetch_symbol_info_by_symbol(pool, exchange, &order.symbol).await {
        Ok(Some(symbol_info)) => symbol_info,
        Ok(None) => {
            let msg: String = format!("Symbol info not found for {}", order.symbol);
            log::error!("{}", msg);
            match insert_db_error(pool, exchange, &msg).await {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                    log::error!("{}", msg);
                    return Err(msg.into());
                }
            }
        }
        Err(e) => {
            let msg: String = format!("Symbol info not found for {} {}", order.symbol, e);
            log::error!("{}", msg);
            match insert_db_error(pool, exchange, &msg).await {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                    log::error!("{}", msg);
                    return Err(e.into());
                }
            }
        }
    };

    let price_increment: f64 = match symbol_info.price_increment.parse::<f64>() {
        Ok(price_increment) => price_increment,
        Err(e) => {
            let msg: String = format!("Failed parse price_increment: {} {}", symbol_info.price_increment, e);
            log::error!("{}", msg);
            match insert_db_error(pool, exchange, &msg).await {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                    log::error!("{}", msg);
                    return Err(e.into());
                }
            }
        }
    };
    let quote_increment: f64 = match symbol_info.quote_increment.parse::<f64>() {
        Ok(quote_increment) => quote_increment,
        Err(e) => {
            let msg: String = format!("Failed parse quote_increment: {} {}", symbol_info.quote_increment, e);
            log::error!("{}", msg);
            match insert_db_error(pool, exchange, &msg).await {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                    log::error!("{}", msg);
                    return Err(e.into());
                }
            }
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
                    log::error!("{}", msg);
                    match insert_db_error(pool, exchange, &msg).await {
                        Ok(_) => {
                            return Ok(());
                        }
                        Err(e) => {
                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                            log::error!("{}", msg);
                            return Err(e.into());
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
                            log::error!("{}", msg);
                            match insert_db_error(pool, exchange, &msg).await {
                                Ok(_) => {
                                    return Ok(());
                                }
                                Err(e) => {
                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                    log::error!("{}", msg);
                                    return Err(e.into());
                                }
                            }
                        }
                    }
                    match api_v3_hf_margin_stop_order_cancel_by_client_oid(&exit_sl_client_oid).await {
                        Ok(_) => {
                            log::info!("Successfully cancel stop order :{}", &exit_sl_client_oid)
                        }
                        Err(e) => {
                            let msg: String = format!("Failed cancel stop order: {}", e);
                            log::error!("{}", msg);
                            match insert_db_error(pool, exchange, &msg).await {
                                Ok(_) => {
                                    return Ok(());
                                }
                                Err(e) => {
                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                    log::error!("{}", msg);
                                    return Err(e.into());
                                }
                            }
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
                                        log::error!("{}", msg);
                                        match insert_db_error(pool, exchange, &msg).await {
                                            Ok(_) => {
                                                return Ok(());
                                            }
                                            Err(e) => {
                                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                log::error!("{}", msg);
                                                return Err(e.into());
                                            }
                                        }
                                    }
                                }
                                // create new random order
                                match make_random_trade(pool, exchange, new_balance, bot.id).await {
                                    Ok(()) => {}
                                    Err(e) => {
                                        let msg: String = format!("Error in make_random_trade: {}", e);
                                        log::error!("{}", msg);
                                        match insert_db_error(pool, exchange, &msg).await {
                                            Ok(_) => {
                                                return Ok(());
                                            }
                                            Err(e) => {
                                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                log::error!("{}", msg);
                                                return Err(e.into());
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                let msg: String = format!("Failed parse balance: {} {}", bot.balance, e);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {
                                        return Ok(());
                                    }
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                        return Err(e.into());
                                    }
                                }
                            }
                        }
                    } else if order.side == "sell" {
                        match update_balance_bot_by_exit_tp_client_oid(pool, exchange, client_oid, &format!("{:.4}", return_balance)).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed update_balance_bot_by_exit_tp_client_oid: {}", e);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {
                                        return Ok(());
                                    }
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                        return Err(e.into());
                                    }
                                }
                            }
                        }
                        // create new random order
                        match make_random_trade(pool, exchange, return_balance, bot.id).await {
                            Ok(()) => {}
                            Err(e) => {
                                let msg: String = format!("Error in make_random_trade: {}", e);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {
                                        return Ok(());
                                    }
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                        return Err(e.into());
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(None) => {
                    log::error!("No records found or error occurred");
                }
                Err(e) => {
                    let msg: String = format!("Failed get_total_match_value_by_client_oid: {}", e);
                    log::error!("{}", msg);
                    match insert_db_error(pool, exchange, &msg).await {
                        Ok(_) => {
                            return Ok(());
                        }
                        Err(e) => {
                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                            log::error!("{}", msg);
                            return Err(e.into());
                        }
                    }
                }
            }
            return Ok(());
        }
        Ok(None) => {}
        Err(e) => {
            let msg: String = format!("Failed get_bot_by_exit_tp_client_oid: {}", e);
            log::error!("{}", msg);
            match insert_db_error(pool, exchange, &msg).await {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                    log::error!("{}", msg);
                    return Err(e.into());
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
                                    log::error!("{}", msg);
                                    match insert_db_error(pool, exchange, &msg).await {
                                        Ok(_) => {
                                            return Ok(());
                                        }
                                        Err(e) => {
                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                            log::error!("{}", msg);
                                            return Err(e.into());
                                        }
                                    }
                                }
                            }
                            match api_v3_hf_margin_stop_order_cancel_by_client_oid(&exit_tp_client_oid).await {
                                Ok(_) => {
                                    log::info!("Successfully cancel stop order :{}", &exit_tp_client_oid);
                                }
                                Err(e) => {
                                    let msg: String = format!("Failed cancel stop order: {}", e);
                                    log::error!("{}", msg);
                                    match insert_db_error(pool, exchange, &msg).await {
                                        Ok(_) => {
                                            return Ok(());
                                        }
                                        Err(e) => {
                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                            log::error!("{}", msg);
                                            return Err(e.into());
                                        }
                                    }
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
                                                log::error!("{}", msg);
                                                match insert_db_error(pool, exchange, &msg).await {
                                                    Ok(_) => {
                                                        return Ok(());
                                                    }
                                                    Err(e) => {
                                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                        log::error!("{}", msg);
                                                        return Err(e.into());
                                                    }
                                                }
                                            }
                                        }
                                        // create new random order
                                        match make_random_trade(pool, exchange, new_balance, bot.id).await {
                                            Ok(()) => {}
                                            Err(e) => {
                                                let msg: String = format!("Error in make_random_trade: {}", e);
                                                log::error!("{}", msg);
                                                match insert_db_error(pool, exchange, &msg).await {
                                                    Ok(_) => {
                                                        return Ok(());
                                                    }
                                                    Err(e) => {
                                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                        log::error!("{}", msg);
                                                        return Err(e.into());
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        let msg: String = format!("Failed parse balance: {} {}", bot.balance, e);
                                        log::error!("{}", msg);
                                        match insert_db_error(pool, exchange, &msg).await {
                                            Ok(_) => {
                                                return Ok(());
                                            }
                                            Err(e) => {
                                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                log::error!("{}", msg);
                                                return Err(e.into());
                                            }
                                        }
                                    }
                                }
                            } else if order.side == "sell" {
                                match update_balance_bot_by_exit_sl_client_oid(pool, exchange, client_oid, &format!("{:.4}", return_balance)).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed update_balance_bot_by_exit_sl_client_oid: {}", e);
                                        log::error!("{}", msg);
                                        match insert_db_error(pool, exchange, &msg).await {
                                            Ok(_) => {
                                                return Ok(());
                                            }
                                            Err(e) => {
                                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                log::error!("{}", msg);
                                                return Err(e.into());
                                            }
                                        }
                                    }
                                }

                                // create new random order
                                match make_random_trade(pool, exchange, return_balance, bot.id).await {
                                    Ok(()) => {}
                                    Err(e) => {
                                        let msg: String = format!("Error in make_random_trade: {}", e);
                                        log::error!("{}", msg);
                                        match insert_db_error(pool, exchange, &msg).await {
                                            Ok(_) => {
                                                return Ok(());
                                            }
                                            Err(e) => {
                                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                log::error!("{}", msg);
                                                return Err(e.into());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Ok(None) => {
                            log::error!("No records found or error occurred");
                        }
                        Err(e) => {
                            let msg: String = format!("Failed get_total_match_value_by_client_oid: {}", e);
                            log::error!("{}", msg);
                            match insert_db_error(pool, exchange, &msg).await {
                                Ok(_) => {
                                    return Ok(());
                                }
                                Err(e) => {
                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                    log::error!("{}", msg);
                                    return Err(e.into());
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    let msg: String = format!("Failed delete_exit_sl_id_bot_by_client_oid: {}", e);
                    log::error!("{}", msg);
                    match insert_db_error(pool, exchange, &msg).await {
                        Ok(_) => {
                            return Ok(());
                        }
                        Err(e) => {
                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                            log::error!("{}", msg);
                            return Err(e.into());
                        }
                    }
                }
            }

            return Ok(());
        }
        Ok(None) => {}
        Err(e) => {
            let msg: String = format!("Failed get_bot_by_exit_sl_client_oid: {}", e);
            log::error!("{}", msg);
            match insert_db_error(pool, exchange, &msg).await {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                    log::error!("{}", msg);
                    return Err(e.into());
                }
            }
        }
    }

    // if clientOid in bots entry_id (1 phase)
    match get_bot_by_entry_client_oid(pool, exchange, client_oid).await {
        Ok(Some(bot)) => {
            // create new stop tp and sl orders

            let filled_size_f64 = match &order.filled_size {
                Some(filled_size) => match filled_size.parse::<f64>() {
                    Ok(filled_size_f64) => filled_size_f64,
                    Err(e) => {
                        let msg: String = format!("Failed parse order.filled_size: {} {}", filled_size, e);
                        log::error!("{}", msg);
                        match insert_db_error(pool, exchange, &msg).await {
                            Ok(_) => {
                                return Ok(());
                            }
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                log::error!("{}", msg);
                                return Ok(());
                            }
                        }
                    }
                },
                None => {
                    let msg: String = format!("Failed get filled_size from: {:?}", &order);
                    log::error!("{}", msg);
                    match insert_db_error(pool, exchange, &msg).await {
                        Ok(_) => {
                            return Ok(());
                        }
                        Err(e) => {
                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                            log::error!("{}", msg);
                            return Err(e.into());
                        }
                    }
                }
            };

            let new_balance = match get_total_match_value_by_client_oid(pool, exchange, client_oid).await {
                Ok(Some(new_balance)) => new_balance,
                Ok(None) => {
                    let msg: String = format!("No records found in events:{}", client_oid);
                    log::error!("{}", msg);
                    match insert_db_error(pool, exchange, &msg).await {
                        Ok(_) => {
                            return Ok(());
                        }
                        Err(e) => {
                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                            log::error!("{}", msg);
                            return Ok(());
                        }
                    }
                }
                Err(e) => {
                    let msg: String = format!("Failed get_total_match_value_by_client_oid: {}", e);
                    log::error!("{}", msg);
                    match insert_db_error(pool, exchange, &msg).await {
                        Ok(_) => {
                            return Ok(());
                        }
                        Err(e) => {
                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                            log::error!("{}", msg);
                            return Ok(());
                        }
                    }
                }
            };

            match update_bot_balance_by_entry_client_oid(pool, exchange, client_oid, &format!("{:.4}", new_balance)).await {
                Ok(_) => {}
                Err(e) => {
                    let msg: String = format!("Failed update_bot_balance_by_entry_client_oid: {}", e);
                    log::error!("{}", msg);
                    match insert_db_error(pool, exchange, &msg).await {
                        Ok(_) => {
                            return Ok(());
                        }
                        Err(e) => {
                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                            log::error!("{}", msg);
                            return Err(e.into());
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
                    "autoRepay": false,
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
                    "autoRepay": false,
                    "size": order.filled_size,
                    "timeInForce": "GTC",
                });

                log::info!("Stop profit order:{}", msg_tp_order);
                log::info!("Stop loss order:{}", msg_sl_order);

                // add exit_tp_client_oid by entry_id
                match update_exit_tp_client_oid_bot_by_entry_client_oid(pool, exchange, client_oid, &exit_tp_client_oid).await {
                    Ok(_) => {}
                    Err(e) => {
                        let msg: String = format!("Failed update_exit_tp_client_oid_bot_by_entry_client_oid: {}", e);
                        log::error!("{}", msg);
                        match insert_db_error(pool, exchange, &msg).await {
                            Ok(_) => {
                                return Ok(());
                            }
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                log::error!("{}", msg);
                                return Ok(());
                            }
                        }
                    }
                }
                // add exit_sl_client_oid by entry_id
                match update_exit_sl_client_oid_bot_by_entry_client_oid(pool, exchange, client_oid, &exit_sl_client_oid).await {
                    Ok(_) => {}
                    Err(e) => {
                        let msg: String = format!("Failed update_exit_sl_client_oid_bot_by_entry_client_oid: {}", e);
                        log::error!("{}", msg);
                        match insert_db_error(pool, exchange, &msg).await {
                            Ok(_) => {
                                return Ok(());
                            }
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                log::error!("{}", msg);
                                return Ok(());
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
                            Some(ref response_data) => match update_exit_tp_order_id_bot_by_exit_tp_client_oid(pool, exchange, &response_data.order_id, &response_data.client_oid).await {
                                Ok(_) => {}
                                Err(e) => {
                                    let msg: String = format!("Failed update_exit_tp_order_id_bot_by_exit_tp_client_oid: {}", e);
                                    log::error!("{}", msg);
                                    match insert_db_error(pool, exchange, &msg).await {
                                        Ok(_) => {
                                            return Ok(());
                                        }
                                        Err(e) => {
                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                            log::error!("{}", msg);
                                            return Ok(());
                                        }
                                    }
                                }
                            },
                            None => {}
                        }

                        match sl_resp.data {
                            Some(ref response_data) => match update_exit_sl_order_id_bot_by_exit_sl_client_oid(pool, exchange, &response_data.order_id, &response_data.client_oid).await {
                                Ok(_) => {}
                                Err(e) => {
                                    let msg: String = format!("Failed update_exit_sl_order_id_bot_by_exit_sl_client_oid: {}", e);
                                    log::error!("{}", msg);
                                    match insert_db_error(pool, exchange, &msg).await {
                                        Ok(_) => {
                                            return Ok(());
                                        }
                                        Err(e) => {
                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                            log::error!("{}", msg);
                                            return Ok(());
                                        }
                                    }
                                }
                            },
                            None => {}
                        }

                        log::info!("✅ Both stop orders created: TP={}, SL={}", exit_tp_client_oid, exit_sl_client_oid);
                    }
                    (Err(tp_err), Ok(sl_resp)) => {
                        match sl_resp.data {
                            Some(ref response_data) => {
                                match api_v3_hf_margin_stop_order_cancel_by_client_oid(&response_data.client_oid).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed api_v3_hf_margin_stop_order_cancel_by_client_oid: {}", e);
                                        log::error!("{}", msg);
                                        match insert_db_error(pool, exchange, &msg).await {
                                            Ok(_) => {
                                                return Ok(());
                                            }
                                            Err(e) => {
                                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                log::error!("{}", msg);
                                                return Ok(());
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
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {
                                        return Ok(());
                                    }
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                        return Ok(());
                                    }
                                }
                            }
                        }

                        let msg: String = format!("Failed add TP order: {}. SL was cancelled for symmetry.", tp_err);
                        log::error!("{}", msg);
                        match insert_db_error(pool, exchange, &msg).await {
                            Ok(_) => {
                                return Ok(());
                            }
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                log::error!("{}", msg);
                                return Ok(());
                            }
                        }
                    }
                    (Ok(tp_resp), Err(sl_err)) => {
                        match tp_resp.data {
                            Some(ref response_data) => match api_v3_hf_margin_stop_order_cancel_by_client_oid(&response_data.client_oid).await {
                                Ok(_) => {}
                                Err(e) => {
                                    let msg: String = format!("Failed api_v3_hf_margin_stop_order_cancel_by_client_oid: {}", e);
                                    log::error!("{}", msg);
                                    match insert_db_error(pool, exchange, &msg).await {
                                        Ok(_) => {
                                            return Ok(());
                                        }
                                        Err(e) => {
                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                            log::error!("{}", msg);
                                            return Ok(());
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
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {
                                        return Ok(());
                                    }
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                        return Ok(());
                                    }
                                }
                            }
                        }

                        let msg: String = format!("Failed add SL order: {}. TP was cancelled for symmetry.", sl_err);
                        log::error!("{}", msg);
                        match insert_db_error(pool, exchange, &msg).await {
                            Ok(_) => {
                                return Ok(());
                            }
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                log::error!("{}", msg);
                                return Ok(());
                            }
                        }
                    }
                    (Err(tp_err), Err(sl_err)) => {
                        let msg: String = format!("Failed add both stop orders: TP={}, SL={}", tp_err, sl_err);
                        log::error!("{}", msg);
                        match insert_db_error(pool, exchange, &msg).await {
                            Ok(_) => {
                                return Ok(());
                            }
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                log::error!("{}", msg);
                                return Ok(());
                            }
                        }
                        match delete_symbol_bot_by_exit_sl_client_oid(pool, exchange, &exit_sl_client_oid).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed delete_symbol_bot_by_exit_sl_client_oid: {}", e);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {
                                        return Ok(());
                                    }
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                        return Ok(());
                                    }
                                }
                            }
                        }
                        match delete_exit_sl_id_bot_by_client_oid(pool, exchange, &exit_sl_client_oid).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed delete_exit_sl_id_bot_by_client_oid: {}", e);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {
                                        return Ok(());
                                    }
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                        return Ok(());
                                    }
                                }
                            }
                        }
                        match delete_exit_tp_id_bot_by_client_oid(pool, exchange, &exit_tp_client_oid).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed delete_exit_tp_id_bot_by_client_oid: {}", e);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {
                                        return Ok(());
                                    }
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                        return Ok(());
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
                    "autoRepay": false,
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
                    "autoRepay": false,
                    "timeInForce": "GTC",
                    "funds": format_assert(funds_sl, quote_increment),
                });

                log::info!("Stop profit order:{}", msg_tp_order);
                log::info!("Stop loss order:{}", msg_sl_order);

                // add exit_tp_client_oid by entry_id
                match update_exit_tp_client_oid_bot_by_entry_client_oid(pool, exchange, client_oid, &exit_tp_client_oid).await {
                    Ok(_) => {}
                    Err(e) => {
                        let msg: String = format!("Failed update_exit_tp_client_oid_bot_by_entry_client_oid: {}", e);
                        log::error!("{}", msg);
                        match insert_db_error(pool, exchange, &msg).await {
                            Ok(_) => {
                                return Ok(());
                            }
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                log::error!("{}", msg);
                                return Ok(());
                            }
                        }
                    }
                }
                // add exit_sl_client_oid by entry_id
                match update_exit_sl_client_oid_bot_by_entry_client_oid(pool, exchange, client_oid, &exit_sl_client_oid).await {
                    Ok(_) => {}
                    Err(e) => {
                        let msg: String = format!("Failed update_exit_sl_client_oid_bot_by_entry_client_oid: {}", e);
                        log::error!("{}", msg);
                        match insert_db_error(pool, exchange, &msg).await {
                            Ok(_) => {
                                return Ok(());
                            }
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                log::error!("{}", msg);
                                return Ok(());
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
                            Some(ref response_data) => match update_exit_tp_order_id_bot_by_exit_tp_client_oid(pool, exchange, &response_data.order_id, &response_data.client_oid).await {
                                Ok(_) => {}
                                Err(e) => {
                                    let msg: String = format!("Failed update_exit_tp_order_id_bot_by_exit_tp_client_oid: {}", e);
                                    log::error!("{}", msg);
                                    match insert_db_error(pool, exchange, &msg).await {
                                        Ok(_) => {
                                            return Ok(());
                                        }
                                        Err(e) => {
                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                            log::error!("{}", msg);
                                            return Ok(());
                                        }
                                    }
                                }
                            },
                            None => {}
                        }

                        match sl_resp.data {
                            Some(ref response_data) => match update_exit_sl_order_id_bot_by_exit_sl_client_oid(pool, exchange, &response_data.order_id, &response_data.client_oid).await {
                                Ok(_) => {}
                                Err(e) => {
                                    let msg: String = format!("Failed update_exit_sl_order_id_bot_by_exit_sl_client_oid: {}", e);
                                    log::error!("{}", msg);
                                    match insert_db_error(pool, exchange, &msg).await {
                                        Ok(_) => {
                                            return Ok(());
                                        }
                                        Err(e) => {
                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                            log::error!("{}", msg);
                                            return Ok(());
                                        }
                                    }
                                }
                            },
                            None => {}
                        }

                        log::info!("✅ Both stop orders created: TP={}, SL={}", exit_tp_client_oid, exit_sl_client_oid);
                    }
                    (Err(tp_err), Ok(sl_resp)) => {
                        match sl_resp.data {
                            Some(ref response_data) => match api_v3_hf_margin_stop_order_cancel_by_client_oid(&response_data.client_oid).await {
                                Ok(_) => {}
                                Err(e) => {
                                    let msg: String = format!("Failed api_v3_hf_margin_stop_order_cancel_by_client_oid: {}", e);
                                    log::error!("{}", msg);
                                    match insert_db_error(pool, exchange, &msg).await {
                                        Ok(_) => {
                                            return Ok(());
                                        }
                                        Err(e) => {
                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                            log::error!("{}", msg);
                                            return Ok(());
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
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {
                                        return Ok(());
                                    }
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                        return Ok(());
                                    }
                                }
                            }
                        }

                        let msg: String = format!("Failed add TP order: {}. SL was cancelled for symmetry.", tp_err);
                        log::error!("{}", msg);
                        match insert_db_error(pool, exchange, &msg).await {
                            Ok(_) => {
                                return Ok(());
                            }
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                log::error!("{}", msg);
                                return Ok(());
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
                                        log::error!("{}", msg);
                                        match insert_db_error(pool, exchange, &msg).await {
                                            Ok(_) => {
                                                return Ok(());
                                            }
                                            Err(e) => {
                                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                log::error!("{}", msg);
                                                return Ok(());
                                            }
                                        }
                                    }
                                }

                                let msg: String = format!("Failed add SL order: {}. TP was cancelled for symmetry.", sl_err);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {
                                        return Ok(());
                                    }
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                        return Ok(());
                                    }
                                }
                            }
                            Err(e) => {
                                let msg: String = format!("Failed api_v3_hf_margin_stop_order_cancel_by_client_oid: {}", e);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {
                                        return Ok(());
                                    }
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                        return Ok(());
                                    }
                                }
                            }
                        },
                        None => {}
                    },
                    (Err(tp_err), Err(sl_err)) => {
                        let msg: String = format!("Failed add both stop orders: TP={}, SL={}", tp_err, sl_err);
                        log::error!("{}", msg);
                        match insert_db_error(pool, exchange, &msg).await {
                            Ok(_) => {
                                return Ok(());
                            }
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                log::error!("{}", msg);
                                return Ok(());
                            }
                        }
                        match delete_symbol_bot_by_exit_sl_client_oid(pool, exchange, &exit_sl_client_oid).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed delete_symbol_bot_by_exit_sl_client_oid: {}", e);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {
                                        return Ok(());
                                    }
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                        return Ok(());
                                    }
                                }
                            }
                        }
                        match delete_exit_sl_id_bot_by_client_oid(pool, exchange, &exit_sl_client_oid).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed delete_exit_sl_id_bot_by_client_oid: {}", e);
                                log::error!("{}", msg);

                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {
                                        return Ok(());
                                    }
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                        return Ok(());
                                    }
                                }
                            }
                        }
                        match delete_exit_tp_id_bot_by_client_oid(pool, exchange, &exit_tp_client_oid).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed delete_exit_tp_id_bot_by_client_oid: {}", e);
                                log::error!("{}", msg);

                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {
                                        return Ok(());
                                    }
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                        return Ok(());
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // delete entry_id from db
            match set_null_entry_client_oid_by_entry_client_oid(pool, exchange, client_oid).await {
                Ok(_) => {}
                Err(e) => {
                    let msg: String = format!("Failed set_null_entry_client_oid_by_entry_client_oid: {}", e);
                    log::error!("{}", msg);
                    match insert_db_error(pool, exchange, &msg).await {
                        Ok(_) => {
                            return Ok(());
                        }
                        Err(e) => {
                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                            log::error!("{}", msg);
                            return Ok(());
                        }
                    }
                }
            }
        }
        Ok(None) => {}
        Err(e) => {
            let msg: String = format!("Failed get_bot_by_entry_client_oid: {}", e);
            log::error!("{}", msg);
            match insert_db_error(pool, exchange, &msg).await {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                    log::error!("{}", msg);
                    return Err(e.into());
                }
            }
        }
    }

    return Ok(());
}

pub async fn handle_position_event(position: PositionData, pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // repay borrow
    for (asset, token_liability_str) in &position.debt_list {
        // passed
        match position.asset_list.get(asset) {
            Some(asset_info) => match (token_liability_str.parse::<f64>(), asset_info.available.parse::<f64>()) {
                (Ok(liability), Ok(available)) => {
                    if liability > 0.0 {
                        if available >= liability {
                            match create_repay_order(asset, token_liability_str).await {
                                Ok(_) => {
                                    log::info!("Repay {} {} liability with available {}", token_liability_str, asset, &asset_info.available);
                                }
                                Err(e) => {
                                    let msg: String = format!("Failed to repay liability:{} on asset:{} {}", token_liability_str, asset, e);
                                    log::error!("{}", msg);
                                    match insert_db_error(pool, exchange, &msg).await {
                                        Ok(_) => {}
                                        Err(e) => {
                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                            log::error!("{}", msg);
                                        }
                                    }
                                    continue;
                                }
                            }
                        } else if available > 0.0 {
                            match create_repay_order(asset, &asset_info.available).await {
                                Ok(_) => {
                                    log::info!("Partially repay {} {} liability with available {}", token_liability_str, asset, &asset_info.available);
                                }
                                Err(e) => {
                                    let msg: String = format!("Failed to partially repay liability:{} on asset:{} {}", token_liability_str, asset, e);
                                    log::error!("{}", msg);
                                    match insert_db_error(pool, exchange, &msg).await {
                                        Ok(_) => {}
                                        Err(e) => {
                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                            log::error!("{}", msg);
                                        }
                                    }
                                    continue;
                                }
                            }
                        }
                    }
                }
                (Err(e), _) => {
                    let msg: String = format!("Failed to parse token_liability_str:{}  {}", token_liability_str, e);
                    log::error!("{}", msg);
                    match insert_db_error(pool, exchange, &msg).await {
                        Ok(_) => {}
                        Err(e) => {
                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                            log::error!("{}", msg);
                        }
                    }
                    continue;
                }
                (_, Err(e)) => {
                    let msg: String = format!("Failed to parse asset_info.available:{}  {}", asset_info.available, e);
                    log::error!("{}", msg);
                    match insert_db_error(pool, exchange, &msg).await {
                        Ok(_) => {}
                        Err(e) => {
                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                            log::error!("{}", msg);
                        }
                    }
                    continue;
                }
            },
            None => {
                let msg: String = format!("Failed get asset:{} from:{:.?}", asset, position.asset_list);
                log::error!("{}", msg);
                match insert_db_error(pool, exchange, &msg).await {
                    Ok(_) => {}
                    Err(e) => {
                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                        log::error!("{}", msg);
                    }
                }
                continue;
            }
        }
    }
    match upsert_position_ratio(pool, exchange, position.debt_ratio, position.total_asset, &position.margin_coefficient_total_asset, &position.total_debt).await {
        Ok(_) => {}
        Err(e) => {
            let msg: String = format!("Failed to upsert margin account state: {}", e);
            log::error!("{}", msg);
            match insert_db_error(pool, exchange, &msg).await {
                Ok(_) => {}
                Err(e) => {
                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                    log::error!("{}", msg);
                }
            }
        }
    }

    for (symbol, amount) in &position.debt_list {
        match upsert_position_debt(pool, exchange, symbol, amount).await {
            Ok(_) => {}
            Err(e) => {
                let msg: String = format!("Failed to insert debt margin account state: {}", e);
                log::error!("{}", msg);
                match insert_db_error(pool, exchange, &msg).await {
                    Ok(_) => {}
                    Err(e) => {
                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                        log::error!("{}", msg);
                    }
                }
                continue;
            }
        }
    }
    for (symbol, symbol_info) in &position.asset_list {
        match upsert_position_asset(pool, exchange, symbol, &symbol_info.total, &symbol_info.available, &symbol_info.hold).await {
            Ok(_) => {}
            Err(e) => {
                let msg: String = format!("Failed to insert asset margin account state: {}", e);
                log::error!("{}", msg);
                match insert_db_error(pool, exchange, &msg).await {
                    Ok(_) => {}
                    Err(e) => {
                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                        log::error!("{}", msg);
                    }
                }
                continue;
            }
        }
    }

    return Ok(());
}

pub async fn handle_advanced_orders(order: AdvancedOrders, pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    log::info!("{:.?}", order);
    match order.error {
        Some(_) => {
            let msg: String = format!("Got error on stop order : {:?}", order);
            log::error!("{}", msg);
            match insert_db_error(pool, exchange, &msg).await {
                Ok(_) => {}
                Err(e) => {
                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                    log::error!("{}", msg);
                }
            }

            const MAX_RETRIES: u32 = 1000;
            let mut attempt = 0;
            loop {
                if attempt >= MAX_RETRIES {
                    break Ok(());
                }
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(RETRY_DELAY_BASE * attempt as u64)).await;

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
                                    Some(funds) => make_hf_funds_margin_order(pool, exchange, &new_exit_client_oid, &side_clone, &symbol_clone, funds, "market".to_string(), true, false).await,
                                    None => {
                                        let msg: String = format!("Fail parse funds order:{} new_exit_sl_client_oid:{} funds_clone:{:.?}", order_id_clone, new_exit_client_oid, funds_clone,);
                                        log::error!("{}", msg);
                                        match insert_db_error(pool, exchange, &msg).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                log::error!("{}", msg);
                                            }
                                        }
                                        continue;
                                    }
                                },
                                "sell" => match size_clone {
                                    Some(size) => make_hf_size_margin_order(pool, exchange, &new_exit_client_oid, &side_clone, &symbol_clone, size, "market".to_string(), true, false).await,
                                    None => {
                                        let msg: String = format!("Fail parse size order:{} new_exit_sl_client_oid:{} size_clone:{:.?}", order_id_clone, new_exit_client_oid, size_clone,);
                                        log::error!("{}", msg);
                                        match insert_db_error(pool, exchange, &msg).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                log::error!("{}", msg);
                                            }
                                        }
                                        continue;
                                    }
                                },
                                _ => {
                                    let msg: String = format!("Fail match side_clone:{}", side_clone);
                                    log::error!("{}", msg);
                                    match insert_db_error(pool, exchange, &msg).await {
                                        Ok(_) => {}
                                        Err(e) => {
                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                            log::error!("{}", msg);
                                        }
                                    }
                                    continue;
                                }
                            },
                            Err(e) => {
                                let msg: String = format!("Failed save bot info: order_id_clone:{} new_exit_sl_client_oid:{}, {}", order_id_clone, new_exit_client_oid, e);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                    }
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
                                    Some(funds) => make_hf_funds_margin_order(pool, exchange, &new_exit_client_oid, &side_clone, &symbol_clone, funds, "market".to_string(), true, false).await,
                                    None => {
                                        let msg: String = format!("Fail parse funds_clone order:{} new_exit_tp_client_oid:{} funds_clone:{:.?}", order_id_clone, new_exit_client_oid, funds_clone,);
                                        log::error!("{}", msg);
                                        match insert_db_error(pool, exchange, &msg).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                log::error!("{}", msg);
                                            }
                                        }
                                        continue;
                                    }
                                },
                                "sell" => match size_clone {
                                    Some(size) => make_hf_size_margin_order(pool, exchange, &new_exit_client_oid, &side_clone, &symbol_clone, size, "market".to_string(), true, false).await,
                                    None => {
                                        let msg: String = format!("Fail parse size_clone order:{} new_exit_tp_client_oid:{} size_clone:{:.?}", order_id_clone, new_exit_client_oid, size_clone,);
                                        log::error!("{}", msg);
                                        match insert_db_error(pool, exchange, &msg).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                                log::error!("{}", msg);
                                            }
                                        }
                                        continue;
                                    }
                                },
                                _ => {
                                    let msg: String = format!("Fail match side_clone:{}", side_clone);
                                    log::error!("{}", msg);
                                    match insert_db_error(pool, exchange, &msg).await {
                                        Ok(_) => {}
                                        Err(e) => {
                                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                            log::error!("{}", msg);
                                        }
                                    }
                                    continue;
                                }
                            },
                            Err(e) => {
                                let msg: String = format!("Failed save bot info: order_id_clone:{} new_exit_tp_client_oid:{}, {}", order_id_clone, new_exit_client_oid, e);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                    }
                                }
                                continue;
                            }
                        }
                    }
                    _ => {
                        let msg: String = format!("Fail match stop_clone:{}", stop_clone);
                        log::error!("{}", msg);
                        match insert_db_error(pool, exchange, &msg).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                log::error!("{}", msg);
                            }
                        }
                        continue;
                    }
                };

                match order_result {
                    Ok(_) => {
                        log::info!("✅ Order re-placed: {} {} (attempt {}/{})", order_id_clone, new_exit_client_oid, attempt, MAX_RETRIES);
                        break Ok(());
                    }
                    Err(e) => {
                        log::error!("❌ Order failed: {} {} (attempt {}/{}) {}", order_id_clone, new_exit_client_oid, attempt, MAX_RETRIES, e);
                        match insert_db_error(pool, exchange, &e.to_string()).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                log::error!("{}", msg);
                            }
                        }
                    }
                }
            }
        }
        None => return Ok(()),
    }
}

pub async fn process_kcn_msg(pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str, msg: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match serde_json::from_str::<KuCoinMessage>(msg) {
        Ok(event) => match event {
            KuCoinMessage::Welcome(data) => {
                // passed
                match serde_json::to_value(&data) {
                    Ok(data) => match insert_db_event(pool, exchange, data).await {
                        Ok(_) => {
                            return Ok(());
                        }
                        Err(e) => {
                            let msg: String = format!("Failed insert_db_event {}", e);
                            log::error!("{}", msg);
                            match insert_db_error(pool, exchange, &msg).await {
                                Ok(_) => {
                                    return Ok(());
                                }
                                Err(e) => {
                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                    log::error!("{}", msg);
                                }
                            }
                            return Err(e.into());
                        }
                    },
                    Err(e) => {
                        let msg: String = format!("Failed to serialize event {:?}: {}", data, e);
                        log::error!("{}", msg);
                        match insert_db_error(pool, exchange, &msg).await {
                            Ok(_) => {
                                return Ok(());
                            }
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                log::error!("{}", msg);
                            }
                        }
                        return Err(e.into());
                    }
                };
            }
            KuCoinMessage::Message(data) => {
                if data.topic == "/account/balance" {
                    // passed
                    match BalanceData::deserialize(&data.data) {
                        Ok(balance) => match insert_db_balance(pool, exchange, balance).await {
                            Ok(_) => {
                                return Ok(());
                            }
                            Err(e) => {
                                let msg: String = format!("Failed insert_db_balance data: {:.?} {}", data.data, e);
                                log::error!("{}", msg);

                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {
                                        return Ok(());
                                    }
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                        return Err(e);
                                    }
                                }
                            }
                        },
                        Err(e) => {
                            // sent balance parse error to pg
                            let msg: String = format!("Failed to parse message {:?} {}", data.data, e);
                            log::error!("{}", msg);
                            match insert_db_error(pool, exchange, &msg).await {
                                Ok(_) => {
                                    return Ok(());
                                }
                                Err(e) => {
                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                    log::error!("{}", msg);
                                    return Err(e);
                                }
                            }
                        }
                    }
                } else if data.topic == "/spotMarket/tradeOrdersV2" {
                    match OrderData::deserialize(&data.data) {
                        Ok(order) => match handle_trade_order_event(order, pool, exchange).await {
                            Ok(_) => {
                                return Ok(());
                            }
                            Err(e) => {
                                let msg: String = format!("Failed handle_trade_order_event data: {:.?} {}", data.data, e);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {
                                        return Ok(());
                                    }
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                        return Err(e);
                                    }
                                }
                            }
                        },
                        Err(e) => {
                            // sent order error to pg
                            let msg: String = format!("Failed to parse message {:?} {}", data.data, e);
                            log::error!("{}", msg);
                            match insert_db_error(pool, exchange, &msg).await {
                                Ok(_) => {
                                    return Ok(());
                                }
                                Err(e) => {
                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                    log::error!("{}", msg);
                                    return Err(e);
                                }
                            }
                        }
                    }
                } else if data.topic == "/spotMarket/advancedOrders" {
                    match AdvancedOrders::deserialize(&data.data) {
                        Ok(order) => match handle_advanced_orders(order, pool, exchange).await {
                            Ok(_) => {
                                return Ok(());
                            }
                            Err(e) => {
                                let msg: String = format!("Failed handle_advanced_orders data: {:.?} {}", data.data, e);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {
                                        return Ok(());
                                    }
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                        return Err(e);
                                    }
                                }
                            }
                        },
                        Err(e) => {
                            // sent order error to pg
                            let msg: String = format!("Failed to parse message {:?} {}", data.data, e);
                            log::error!("{}", msg);
                            match insert_db_error(pool, exchange, &msg).await {
                                Ok(_) => {
                                    return Ok(());
                                }
                                Err(e) => {
                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                    log::error!("{}", msg);
                                    return Err(e);
                                }
                            }
                        }
                    }
                } else if data.topic == "/margin/position" {
                    // passed
                    match PositionData::deserialize(&data.data) {
                        Ok(position) => match handle_position_event(position, pool, exchange).await {
                            Ok(_) => {
                                return Ok(());
                            }
                            Err(e) => {
                                let msg: String = format!("Failed handle_position_event data:{:.?} {}", data.data, e);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {
                                        return Ok(());
                                    }
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                        return Err(e);
                                    }
                                }
                            }
                        },
                        Err(e) => {
                            // sent order error to pg
                            let msg: String = format!("Failed to parse message {:?} {}", data.data, e);
                            log::error!("{}", msg);
                            match insert_db_error(pool, exchange, &msg).await {
                                Ok(_) => {
                                    return Ok(());
                                }
                                Err(e) => {
                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                    log::error!("{}", msg);
                                    return Err(e);
                                }
                            }
                        }
                    }
                } else {
                    // passed
                    let msg: String = format!("Unknown topic: {}", data.topic);
                    log::error!("{}", msg);
                    // sent error to pg
                    match insert_db_error(pool, exchange, &msg).await {
                        Ok(_) => {
                            return Ok(());
                        }
                        Err(e) => {
                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                            log::error!("{}", msg);
                            return Err(e);
                        }
                    }
                }
            }
            KuCoinMessage::Ack(data) => {
                // passed
                match serde_json::to_value(&data) {
                    Ok(data) => match insert_db_event(pool, exchange, data).await {
                        Ok(_) => {
                            return Ok(());
                        }
                        Err(e) => {
                            let msg: String = format!("Failed insert_db_event: {}", e);
                            log::error!("{}", msg);
                            match insert_db_error(pool, exchange, &msg).await {
                                Ok(_) => {
                                    return Ok(());
                                }
                                Err(e) => {
                                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                    log::error!("{}", msg);
                                }
                            }
                            return Err(e.into());
                        }
                    },
                    Err(e) => {
                        let msg: String = format!("Failed to serialize event:{:?} {}", data, e);
                        log::error!("{}", msg);
                        match insert_db_error(pool, exchange, &msg).await {
                            Ok(_) => {
                                return Ok(());
                            }
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                log::error!("{}", msg);
                            }
                        }
                        return Err(e.into());
                    }
                };
            }
            KuCoinMessage::Error(data) => {
                // passed
                let msg: String = format!("Got error in WS {:?}", data);
                log::error!("{}", msg);
                match insert_db_error(pool, exchange, &msg).await {
                    Ok(_) => {
                        return Ok(());
                    }
                    Err(e) => {
                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                        log::error!("{}", msg);
                    }
                }
                return Err(msg.into());
            }
            KuCoinMessage::Unknown => {
                // passed
                let msg: String = format!("Unknown WS message type");
                log::error!("{}", msg);
                match insert_db_error(pool, exchange, &msg).await {
                    Ok(_) => {
                        return Ok(());
                    }
                    Err(e) => {
                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                        log::error!("{}", msg);
                    }
                }
                return Err(msg.into());
            }
        },
        Err(e) => {
            // sent error to pg
            let msg: String = format!("Failed to parse message: {} | Raw: {}", e, msg);
            log::error!("{}", msg);
            match insert_db_error(pool, exchange, &msg).await {
                Ok(_) => {}
                Err(e) => {
                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                    log::error!("{}", msg);
                }
            }
            return Err(e.into());
        }
    }
}

pub async fn make_random_trade(pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str, balance_funds: f64, trade_bot_id: i32) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    const MAX_RETRIES: u32 = 10;
    let mut attempt = 0;

    loop {
        if attempt >= MAX_RETRIES {
            return Ok(());
        }
        attempt += 1;
        match get_random_symbol(pool, exchange).await {
            Ok(Some(tradeable)) => {
                let symbol_info: Symbol = match fetch_symbol_info_by_symbol(pool, exchange, &tradeable.symbol).await {
                    Ok(Some(symbol_info)) => symbol_info,
                    Ok(None) => {
                        let msg: String = format!("Symbol info not found for {}", tradeable.symbol);
                        log::error!("{}", msg);
                        match insert_db_error(pool, exchange, &msg).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                log::error!("{}", msg);
                            }
                        }
                        continue;
                    }
                    Err(e) => {
                        let msg: String = format!("Fail fetch_symbol_info_by_symbol: {} {}", &tradeable.symbol, e);
                        log::error!("{}", msg);
                        match insert_db_error(pool, exchange, &msg).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                log::error!("{}", msg);
                            }
                        }
                        continue;
                    }
                };

                let entry_client_oid: String = Uuid::new_v4().to_string();

                match update_bot_entry_client_oid_by_id(pool, exchange, Some(&tradeable.symbol), Some(&entry_client_oid), trade_bot_id).await {
                    Ok(_) => {}
                    Err(e) => {
                        let msg: String = format!("Failed save bot info: entry_client_oid:{} trade_bot.id:{}, {}", entry_client_oid, trade_bot_id, e);
                        log::error!("{}", msg);
                        match insert_db_error(pool, exchange, &msg).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                log::error!("{}", msg);
                            }
                        }
                        continue;
                    }
                }

                let order_result = match get_random_side().as_str() {
                    "sell" => {
                        let base_increment: f64 = match symbol_info.base_increment.parse::<f64>() {
                            Ok(base_increment) => base_increment,
                            Err(e) => {
                                let msg: String = format!("Failed parse base_increment: {} {}", symbol_info.base_increment, e);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                    }
                                }
                                continue;
                            }
                        };
                        let token_price_str: String = match get_ticker_price(&tradeable.symbol).await {
                            Ok(token_price_str) => token_price_str,
                            Err(e) => {
                                let msg: String = format!("Failed get price: {} {}", tradeable.symbol, e);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                    }
                                }
                                continue;
                            }
                        };
                        let token_price: f64 = match token_price_str.parse::<f64>() {
                            Ok(token_price) => token_price,
                            Err(e) => {
                                let msg: String = format!("Failed parse price: {} {}", token_price_str, e);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                    }
                                }
                                continue;
                            }
                        };
                        let token_size: f64 = balance_funds / token_price;
                        make_hf_size_margin_order(pool, exchange, &entry_client_oid, "sell", &tradeable.symbol, format_assert(token_size, base_increment), "market".to_string(), true, false).await
                    }
                    "buy" => {
                        let quote_increment: f64 = match symbol_info.quote_increment.parse::<f64>() {
                            Ok(quote_increment) => quote_increment,
                            Err(e) => {
                                let msg: String = format!("Failed parse quote_increment: {} {}", symbol_info.quote_increment, e);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                    }
                                }
                                continue;
                            }
                        };
                        make_hf_funds_margin_order(pool, exchange, &entry_client_oid, "buy", &tradeable.symbol, format_assert(balance_funds, quote_increment), "market".to_string(), true, false).await
                    }
                    _ => {
                        continue;
                    }
                };

                match order_result {
                    Ok(_) => {
                        log::info!("✅ Order placed: {} {} (attempt {}/{})", entry_client_oid, trade_bot_id, attempt, MAX_RETRIES);
                        return Ok(());
                    }
                    Err(e) => {
                        match update_bot_entry_client_oid_by_id(pool, exchange, None, None, trade_bot_id).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed update_bot_entry_client_oid_by_id: {}", e);
                                log::error!("{}", msg);
                                match insert_db_error(pool, exchange, &msg).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                        log::error!("{}", msg);
                                    }
                                }
                            }
                        }
                        let msg: String = format!("❌ Order failed (attempt {}/{}): {} - {}", attempt, MAX_RETRIES, tradeable.symbol, e);
                        log::error!("{}", msg);
                        match insert_db_error(pool, exchange, &e.to_string()).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                                log::error!("{}", msg);
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(RETRY_DELAY_BASE * attempt as u64)).await;
                        continue;
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                let msg: String = format!("Failed get_random_symbol: {}", e);
                log::error!("{}", msg);
                match insert_db_error(pool, exchange, &msg).await {
                    Ok(_) => {}
                    Err(e) => {
                        let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                        log::error!("{}", msg);
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
    auto_borrow: bool,
    auto_repay: bool,
) -> Result<MakeOrderRes, Box<dyn std::error::Error + Send + Sync>> {
    // only for buy orders
    let args_time_in_force: &str = "GTC";

    match insert_db_msgsend(pool, exchange, Some(symbol), Some(side), None, Some(&funds), None, Some(args_time_in_force), Some(&type_), Some(&auto_borrow), Some(&auto_repay), Some(client_oid), None)
        .await
    {
        Ok(_) => {
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
            log::info!("{}", msg);

            match add_api_v3_hf_margin_order(msg.clone()).await {
                Ok(data) => Ok(data),
                Err(e) => {
                    let msg: String = format!("Failed to send order: {}", e);
                    log::error!("{}", msg);
                    match insert_db_error(pool, exchange, &msg).await {
                        Ok(_) => {}
                        Err(e) => {
                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                            log::error!("{}", msg);
                        }
                    }
                    Err(msg.into())
                }
            }
        }
        Err(e) => {
            let msg: String = format!("Failed insert_db_msgsend: {}", e);
            log::error!("{}", msg);
            match insert_db_error(pool, exchange, &msg).await {
                Ok(_) => {}
                Err(e) => {
                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                    log::error!("{}", msg);
                }
            }
            return Err(e.into());
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
    auto_borrow: bool,
    auto_repay: bool,
) -> Result<MakeOrderRes, Box<dyn std::error::Error + Send + Sync>> {
    // only for sell orders
    let args_time_in_force: &str = "GTC";

    match insert_db_msgsend(pool, exchange, Some(symbol), Some(side), Some(&size), None, None, Some(args_time_in_force), Some(&type_), Some(&auto_borrow), Some(&auto_repay), Some(client_oid), None)
        .await
    {
        Ok(_) => {
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
            log::info!("{}", msg);

            match add_api_v3_hf_margin_order(msg.clone()).await {
                Ok(data) => Ok(data),
                Err(e) => {
                    let msg: String = format!("Failed to send order: {}", e);
                    log::error!("{}", msg);
                    match insert_db_error(pool, exchange, &msg).await {
                        Ok(_) => {}
                        Err(e) => {
                            let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                            log::error!("{}", msg);
                        }
                    }
                    Err(msg.into())
                }
            }
        }
        Err(e) => {
            let msg: String = format!("Failed insert_db_msgsend: {}", e);
            log::error!("{}", msg);
            match insert_db_error(pool, exchange, &msg).await {
                Ok(_) => {}
                Err(e) => {
                    let msg: String = format!("Failed insert error msg: {} {}", msg, e);
                    log::error!("{}", msg);
                }
            }
            return Err(e.into());
        }
    }
}
