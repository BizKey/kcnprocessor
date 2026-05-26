use crate::api::db::{
    delete_exit_sl_id_bot_by_client_oid, delete_exit_tp_id_bot_by_client_oid, delete_symbol_bot_by_exit_sl_client_oid, fetch_symbol_info_by_symbol, get_all_bots_for_trade,
    get_bot_by_entry_client_oid, get_bot_by_exit_sl_client_oid, get_bot_by_exit_tp_client_oid, get_random_symbol, get_total_match_value_by_client_oid, handle_db_error, insert_db_balance,
    insert_db_event, insert_db_msgsend, insert_db_orderevent, set_null_entry_client_oid_by_entry_client_oid, update_balance_bot_by_exit_sl_client_oid, update_balance_bot_by_exit_tp_client_oid,
    update_bot_balance_by_entry_client_oid, update_bot_entry_client_oid_by_id, update_exit_sl_client_oid_bot_by_entry_client_oid, update_exit_sl_client_oid_bot_by_exit_sl_order_id,
    update_exit_sl_order_id_bot_by_exit_sl_client_oid, update_exit_tp_client_oid_bot_by_entry_client_oid, update_exit_tp_client_oid_bot_by_exit_tp_order_id,
    update_exit_tp_order_id_bot_by_exit_tp_client_oid, upsert_position_asset, upsert_position_debt, upsert_position_ratio,
};
use crate::api::models::{AdvancedOrders, BalanceData, Bot, KuCoinMessage, MakeOrderRes, OrderData, PositionData, Symbol};
use crate::api::requests::{
    add_api_v3_hf_margin_order, api_v3_hf_margin_stop_order, api_v3_hf_margin_stop_order_cancel_by_client_oid, create_repay_order, get_all_margin_accounts, get_ticker_price, sent_account_transfer,
    serialize_body,
};
use serde::Deserialize;
use serde_json::json;
use tokio::time::{Duration, sleep};
use uuid::Uuid;

const TP_BUY_PERCENT: f64 = 1.07; // +7%
const SL_BUY_PERCENT: f64 = 0.95; // -5%
const TP_SELL_PERCENT: f64 = 0.93; // -7%
const SL_SELL_PERCENT: f64 = 1.05; // +5%
const RETRY_DELAY_BASE: u64 = 500;
const BOT_INIT_DELAY: Duration = Duration::from_secs(5);
const AUTO_CLEAN_DELAY: Duration = Duration::from_secs(1);

pub fn get_random_side() -> &'static str {
    if fastrand::bool() { "buy" } else { "sell" }
}

pub fn format_assert(size: f64, increment: f64) -> String {
    let precision = if increment >= 1.0 { 0 } else { (increment.recip().log10().ceil() as usize).min(10) };
    let rounded = (size / increment).round() * increment;
    format!("{:.prec$}", rounded, prec = precision)
}

pub async fn create_init_orders(pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let trade_bots: Vec<Bot> = match get_all_bots_for_trade(pool, exchange).await {
        Ok(trade_bots) => trade_bots,
        Err(e) => return handle_db_error(pool, exchange, format!("Failed get_all_bots_for_trade:{}", e)).await,
    };

    for trade_bot in trade_bots.iter() {
        sleep(BOT_INIT_DELAY).await;
        let token_funds: f64 = match trade_bot.balance.parse::<f64>() {
            Ok(token_funds) => token_funds,
            Err(e) => return handle_db_error(pool, exchange, format!("Failed parse balance: {} {}", trade_bot.balance, e)).await,
        };
        match make_random_trade(pool, exchange, token_funds, trade_bot.id).await {
            Ok(_) => {}
            Err(e) => {
                let _ = handle_db_error(pool, exchange, format!("Error in make_random_trade:{}", e)).await;
                continue;
            }
        }
    }
    log::info!("All bots initialized!");
    Ok(())
}

pub async fn auto_clean_account(pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    match get_all_margin_accounts().await {
        Ok(accounts) => {
            sleep(AUTO_CLEAN_DELAY).await;
            let mut passed: bool = true;
            for account in accounts.data.accounts.iter() {
                let token_liability: f64 = match account.liability.parse::<f64>() {
                    Ok(token_liability) => token_liability,
                    Err(e) => {
                        let _ = handle_db_error(pool, exchange, format!("Failed parse price: {} {}", account.liability, e)).await;
                        passed = false;
                        continue;
                    }
                };
                let token_available: f64 = match account.available.parse::<f64>() {
                    Ok(token_liability) => token_liability,
                    Err(e) => {
                        let _ = handle_db_error(pool, exchange, format!("Failed parse price: {} {}", account.available, e)).await;
                        passed = false;
                        continue;
                    }
                };

                if token_liability > 0.0 {
                    passed = false;
                    if token_available >= token_liability {
                        log::info!("Can repay {} {} liability with available {}", account.liability, &account.currency, account.available);
                        let body = json!({
                            "currency": &account.currency,
                            "size": &account.liability,
                            "isIsolated": false,
                            "isHf": true
                        });

                        let body_str: String = serialize_body(&Some(body))?;

                        match create_repay_order(body_str).await {
                            Ok(_) => {}
                            Err(e) => {
                                let _ = handle_db_error(pool, exchange, format!("Failed to repay liability:{}", e)).await;
                                continue;
                            }
                        }
                    } else if token_available > 0.0 {
                        log::info!("Can partially repay {} {} liability with available {}", account.liability, &account.currency, account.available);

                        let body = json!({
                            "currency": &account.currency,
                            "size": &account.available,
                            "isIsolated": false,
                            "isHf": true
                        });

                        let body_str: String = serialize_body(&Some(body))?;

                        match create_repay_order(body_str).await {
                            Ok(_) => {}
                            Err(e) => {
                                let _ = handle_db_error(pool, exchange, format!("Failed to partially repay debt:{}", e)).await;
                                continue;
                            }
                        }
                    } else if account.currency != "USDT" && token_available == 0.0 {
                        // buy stock by market liability
                        let trade_symbol: String = format!("{}-USDT", account.currency);
                        let client_oid: String = Uuid::new_v4().to_string();
                        let symbol_info: Symbol = match fetch_symbol_info_by_symbol(pool, exchange, &trade_symbol).await {
                            Ok(Some(info)) => info,
                            Ok(None) => {
                                let _ = handle_db_error(pool, exchange, format!("Symbol info not found for {}", trade_symbol)).await;
                                continue;
                            }
                            Err(e) => {
                                let _ = handle_db_error(pool, exchange, format!("Symbol info not found for {}", trade_symbol)).await;
                                continue;
                            }
                        };
                        // liability debt in tokens
                        // get price token

                        let token_price: f64 = match get_ticker_price(&trade_symbol).await {
                            Ok(token_price_str) => match token_price_str.parse::<f64>() {
                                Ok(token_price) => token_price,
                                Err(e) => {
                                    let _ = handle_db_error(pool, exchange, format!("Failed parse price: {} {}", token_price_str, e)).await;
                                    continue;
                                }
                            },
                            Err(e) => {
                                let _ = handle_db_error(pool, exchange, format!("Failed get price: {} {}", trade_symbol, e)).await;
                                continue;
                            }
                        };

                        log::info!("Successfully get token:{} price:{}", &trade_symbol, token_price);

                        // calc price token on amount liability token
                        let token_funds: f64 = token_price * token_liability;

                        let quote_increment: f64 = match symbol_info.quote_increment.parse::<f64>() {
                            Ok(quote_increment) => quote_increment,
                            Err(e) => {
                                let _ = handle_db_error(pool, exchange, format!("Failed parse quote_increment: {} {}", symbol_info.quote_increment, e)).await;
                                continue;
                            }
                        };
                        // parse min_funds to int
                        let min_funds: f64 = match &symbol_info.min_funds {
                            Some(min_funds_str) => match min_funds_str.parse::<f64>() {
                                Ok(min_funds) => min_funds,
                                Err(e) => {
                                    let _ = handle_db_error(pool, exchange, format!("Failed parse min_funds: {:?} {}", symbol_info.min_funds, e)).await;
                                    continue;
                                }
                            },
                            None => {
                                let _ = handle_db_error(pool, exchange, format!("min_funds is None for symbol {}", trade_symbol)).await;
                                continue;
                            }
                        };
                        let base_min_size: f64 = match symbol_info.base_min_size.parse::<f64>() {
                            Ok(base_min_size) => base_min_size,
                            Err(e) => {
                                let _ = handle_db_error(pool, exchange, format!("Failed parse base_min_size: {} {}", symbol_info.base_min_size, e)).await;
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
                                &trade_symbol,
                                format_assert(min_funds.max(min_funds_by_size), quote_increment),
                                "market",
                                false,
                                false,
                            )
                            .await
                            {
                                Ok(_) => {}
                                Err(e) => {
                                    let _ = handle_db_error(pool, exchange, format!("Failed make_hf_funds_margin_order:{}", e)).await;
                                    continue;
                                }
                            }
                        } else {
                            match make_hf_funds_margin_order(pool, exchange, &client_oid, "buy", &trade_symbol, format_assert(token_funds, quote_increment), "market", false, false).await {
                                Ok(_) => {}
                                Err(e) => {
                                    let _ = handle_db_error(pool, exchange, format!("Failed make_hf_funds_margin_order:{}", e)).await;
                                    continue;
                                }
                            }
                        };
                    }
                } else if account.currency != "USDT" && token_available > 0.0 {
                    passed = false;
                    // sell stocks by market available/ works
                    let client_oid: String = Uuid::new_v4().to_string();
                    let trade_symbol: String = format!("{}-USDT", account.currency);

                    let symbol_info: Symbol = match fetch_symbol_info_by_symbol(pool, exchange, &trade_symbol).await {
                        Ok(Some(symbol_info)) => symbol_info,
                        Ok(None) => {
                            let _ = handle_db_error(pool, exchange, format!("Symbol info not found for {}", trade_symbol)).await;

                            continue;
                        }
                        Err(e) => {
                            let _ = handle_db_error(pool, exchange, format!("Symbol info not found for {} {}", trade_symbol, e)).await;
                            continue;
                        }
                    };

                    let base_increment: f64 = match symbol_info.base_increment.parse::<f64>() {
                        Ok(base_increment) => base_increment,
                        Err(e) => {
                            let _ = handle_db_error(pool, exchange, format!("Failed parse base_increment: {} {}", symbol_info.base_increment, e)).await;
                            continue;
                        }
                    };
                    // get price token
                    let token_price: f64 = match get_ticker_price(&trade_symbol).await {
                        Ok(token_price_str) => match token_price_str.parse::<f64>() {
                            Ok(token_price) => token_price,
                            Err(e) => {
                                let _ = handle_db_error(pool, exchange, format!("Failed parse price: {} {}", token_price_str, e)).await;
                                continue;
                            }
                        },
                        Err(e) => {
                            let _ = handle_db_error(pool, exchange, format!("Failed get price: {} {}", trade_symbol, e)).await;
                            continue;
                        }
                    };

                    log::info!("Successfully get token:{} price:{}", &trade_symbol, token_price);

                    let base_min_size: f64 = match symbol_info.base_min_size.parse::<f64>() {
                        Ok(base_min_size) => base_min_size,
                        Err(e) => {
                            let _ = handle_db_error(pool, exchange, format!("Failed parse base_min_size: {} {}", symbol_info.base_min_size, e)).await;
                            continue;
                        }
                    };
                    let quote_min_size: f64 = match symbol_info.quote_min_size.parse::<f64>() {
                        Ok(quote_min_size) => quote_min_size,
                        Err(e) => {
                            let _ = handle_db_error(pool, exchange, format!("Failed parse quote_min_size: {} {}", symbol_info.quote_min_size, e)).await;
                            continue;
                        }
                    };
                    if token_available <= base_min_size || (token_price * token_available) <= quote_min_size {
                        let body: serde_json::Value = json!({
                            "currency": &account.currency,
                            "clientOid": Uuid::new_v4().to_string(),
                            "amount": &account.available,
                            "type": "INTERNAL",
                            "fromAccountType": "MARGIN",
                            "toAccountType": "TRADE"
                        });
                        let body_str: String = serialize_body(&Some(body))?;

                        match sent_account_transfer(body_str).await {
                            Ok(_) => {}
                            Err(e) => {
                                let _ = handle_db_error(pool, exchange, format!("Failed send {} to TRADE from MARGIN on {} {}", &account.currency.clone(), &account.available, e)).await;
                                continue;
                            }
                        }
                    } else {
                        match make_hf_size_margin_order(pool, exchange, &client_oid, "sell", &trade_symbol, format_assert(token_available, base_increment), "market", false, false).await {
                            Ok(_) => {}
                            Err(e) => {
                                let _ = handle_db_error(pool, exchange, format!("Failed make_hf_size_margin_order:{}", e)).await;
                                continue;
                            }
                        }
                    }
                }
            }
            if passed { Ok(true) } else { Ok(false) }
        }
        Err(e) => {
            let _ = handle_db_error(pool, exchange, format!("Failed to get margin accounts:{}", e)).await;

            Ok(false)
        }
    }
}

pub async fn handle_trade_order_event(order: OrderData, pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match insert_db_orderevent(pool, exchange, &order).await {
        Ok(_) => {
            log::info!("{:.?}", order);
        }
        Err(e) => return handle_db_error(pool, exchange, format!("Failed insert_db_orderevent: {:.?} {}", order, e)).await,
    }

    let client_oid = match &order.client_oid {
        Some(client_oid) => client_oid,
        None => {
            let _ = handle_db_error(pool, exchange, format!("client_oid in order is none: {:.?}", order)).await;
            return Err("".into());
        }
    };

    if (order.type_ == "match" || order.type_ == "canceled") && (order.remain_size == Some("0".to_string()) || order.remain_funds == Some("0".to_string())) {
    } else {
        return Ok(());
    }

    let symbol_info: Symbol = match fetch_symbol_info_by_symbol(pool, exchange, &order.symbol).await {
        Ok(Some(symbol_info)) => symbol_info,
        Ok(None) => {
            let _ = handle_db_error(pool, exchange, format!("Symbol info not found for {}", order.symbol)).await;
            return Err("".into());
        }
        Err(e) => {
            let _ = handle_db_error(pool, exchange, format!("Symbol info not found for {} {}", order.symbol, e)).await;
            return Err("".into());
        }
    };

    let price_increment: f64 = match symbol_info.price_increment.parse::<f64>() {
        Ok(price_increment) => price_increment,
        Err(e) => return handle_db_error(pool, exchange, format!("Failed parse price_increment: {} {}", symbol_info.price_increment, e)).await,
    };
    let quote_increment: f64 = match symbol_info.quote_increment.parse::<f64>() {
        Ok(quote_increment) => quote_increment,
        Err(e) => {
            let _ = handle_db_error(pool, exchange, format!("Failed parse quote_increment: {} {}", symbol_info.quote_increment, e)).await;
            return Err("".into());
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
                    let _ = handle_db_error(pool, exchange, format!("Failed delete_exit_tp_id_bot_by_client_oid:{}", e)).await;
                }
            }
            match bot.exit_sl_client_oid {
                Some(exit_sl_client_oid) => {
                    // clear exit_sl_client_oid in bots by id !!
                    match delete_exit_sl_id_bot_by_client_oid(pool, exchange, &exit_sl_client_oid).await {
                        Ok(_) => {}
                        Err(e) => {
                            let _ = handle_db_error(pool, exchange, format!("Failed delete_exit_sl_id_bot_by_client_oid:{}", e)).await;
                        }
                    }
                    match api_v3_hf_margin_stop_order_cancel_by_client_oid(&exit_sl_client_oid).await {
                        Ok(_) => {
                            log::info!("Successfully cancel stop order :{}", &exit_sl_client_oid)
                        }
                        Err(e) => {
                            let _ = handle_db_error(pool, exchange, format!("Failed cancel stop order:{}", e)).await;
                        }
                    }
                }
                None => {}
            }
            match get_total_match_value_by_client_oid(pool, exchange, client_oid).await {
                Ok(Some(return_balance_raw)) => {
                    match return_balance_raw.parse::<f64>() {
                        Ok(return_balance) => {
                            if order.side == "buy" {
                                match bot.balance.parse::<f64>() {
                                    Ok(old_balance) => {
                                        let new_balance: f64 = old_balance + old_balance - return_balance;
                                        match update_balance_bot_by_exit_tp_client_oid(pool, exchange, client_oid, &format!("{:.4}", new_balance)).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                let _ = handle_db_error(pool, exchange, format!("Failed update_balance_bot_by_exit_tp_client_oid:{}", e)).await;
                                            }
                                        }
                                        // create new random order
                                        match make_random_trade(pool, exchange, new_balance, bot.id).await {
                                            Ok(()) => {}
                                            Err(e) => {
                                                let _ = handle_db_error(pool, exchange, format!("Error in make_random_trade:{}", e)).await;
                                            }
                                        }
                                    }
                                    Err(e) => return handle_db_error(pool, exchange, format!("Failed parse balance: {} {}", bot.balance, e)).await,
                                }
                            } else if order.side == "sell" {
                                match update_balance_bot_by_exit_tp_client_oid(pool, exchange, client_oid, &format!("{:.4}", return_balance)).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        let _ = handle_db_error(pool, exchange, format!("Failed update_balance_bot_by_exit_tp_client_oid:{}", e)).await;
                                    }
                                }
                                // create new random order
                                match make_random_trade(pool, exchange, return_balance, bot.id).await {
                                    Ok(()) => {}
                                    Err(e) => {
                                        let _ = handle_db_error(pool, exchange, format!("Error in make_random_trade:{}", e)).await;
                                    }
                                }
                            }
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
                Ok(None) => {
                    log::error!("No records found or error occurred");
                }
                Err(e) => {
                    let _ = handle_db_error(pool, exchange, format!("Failed get_total_match_value_by_client_oid:{}", e)).await;
                }
            }
            return Ok(());
        }
        Ok(None) => {}
        Err(e) => {
            let _ = handle_db_error(pool, exchange, format!("Failed get_bot_by_exit_tp_client_oid:{}", e)).await;
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
                                Err(e) => return handle_db_error(pool, exchange, format!("Failed delete_exit_tp_id_bot_by_client_oid:{}", e)).await,
                            }
                            match api_v3_hf_margin_stop_order_cancel_by_client_oid(&exit_tp_client_oid).await {
                                Ok(_) => {
                                    log::info!("Successfully cancel stop order :{}", &exit_tp_client_oid);
                                }
                                Err(e) => return handle_db_error(pool, exchange, format!("Failed cancel stop order:{}", e)).await,
                            }
                        }
                        None => {}
                    }

                    match get_total_match_value_by_client_oid(pool, exchange, client_oid).await {
                        Ok(Some(return_balance_raw)) => {
                            match return_balance_raw.parse::<f64>() {
                                Ok(return_balance) => {
                                    if order.side == "buy" {
                                        match bot.balance.parse::<f64>() {
                                            Ok(old_balance) => {
                                                let new_balance: f64 = old_balance + old_balance - return_balance;
                                                match update_balance_bot_by_exit_sl_client_oid(pool, exchange, client_oid, &format!("{:.4}", new_balance)).await {
                                                    Ok(_) => {}
                                                    Err(e) => return handle_db_error(pool, exchange, format!("Failed update_balance_bot_by_exit_sl_client_oid:{}", e)).await,
                                                }
                                                // create new random order
                                                match make_random_trade(pool, exchange, new_balance, bot.id).await {
                                                    Ok(()) => {}
                                                    Err(e) => return handle_db_error(pool, exchange, format!("Error in make_random_trade:{}", e)).await,
                                                }
                                            }
                                            Err(e) => return handle_db_error(pool, exchange, format!("Failed parse balance: {} {}", bot.balance, e)).await,
                                        }
                                    } else if order.side == "sell" {
                                        match update_balance_bot_by_exit_sl_client_oid(pool, exchange, client_oid, &format!("{:.4}", return_balance)).await {
                                            Ok(_) => {}
                                            Err(e) => return handle_db_error(pool, exchange, format!("Failed update_balance_bot_by_exit_sl_client_oid:{}", e)).await,
                                        }

                                        // create new random order
                                        match make_random_trade(pool, exchange, return_balance, bot.id).await {
                                            Ok(()) => {}
                                            Err(e) => return handle_db_error(pool, exchange, format!("Error in make_random_trade:{}", e)).await,
                                        }
                                    }
                                }
                                Err(e) => return Err(e.into()),
                            }
                        }
                        Ok(None) => {
                            log::error!("No records found or error occurred");
                        }
                        Err(e) => {
                            return handle_db_error(pool, exchange, format!("Failed get_total_match_value_by_client_oid:{}", e)).await;
                        }
                    }
                }
                Err(e) => {
                    let _ = handle_db_error(pool, exchange, format!("Failed delete_exit_sl_id_bot_by_client_oid:{}", e)).await;
                }
            }

            return Ok(());
        }
        Ok(None) => {}
        Err(e) => {
            let _ = handle_db_error(pool, exchange, format!("Failed get_bot_by_exit_sl_client_oid:{}", e)).await;
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
                        return handle_db_error(pool, exchange, format!("Failed parse order.filled_size: {} {}", filled_size, e)).await;
                    }
                },
                None => {
                    return handle_db_error(pool, exchange, format!("Failed get filled_size from: {:?}", &order)).await;
                }
            };

            let new_balance_raw = match get_total_match_value_by_client_oid(pool, exchange, client_oid).await {
                Ok(Some(new_balance_raw)) => new_balance_raw,
                Ok(None) => return handle_db_error(pool, exchange, format!("No records found in events: {}", client_oid)).await,
                Err(e) => return handle_db_error(pool, exchange, format!("Failed get_total_match_value_by_client_oid: {} {}", client_oid, e)).await,
            };

            let new_balance = match new_balance_raw.parse::<f64>() {
                Ok(new_balance) => new_balance,
                Err(e) => return Err(e.into()),
            };

            match update_bot_balance_by_entry_client_oid(pool, exchange, client_oid, &format!("{:.4}", new_balance)).await {
                Ok(_) => {}
                Err(e) => {
                    return handle_db_error(pool, exchange, format!("Failed update_bot_balance_by_entry_client_oid:{}", e)).await;
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
                        return handle_db_error(pool, exchange, format!("Failed update_exit_tp_client_oid_bot_by_entry_client_oid:{}", e)).await;
                    }
                }
                // add exit_sl_client_oid by entry_id
                match update_exit_sl_client_oid_bot_by_entry_client_oid(pool, exchange, client_oid, &exit_sl_client_oid).await {
                    Ok(_) => {}
                    Err(e) => {
                        return handle_db_error(pool, exchange, format!("Failed update_exit_sl_client_oid_bot_by_entry_client_oid:{}", e)).await;
                    }
                }
                let msg_tp_order2: String = serialize_body(&Some(msg_tp_order))?;
                let tp_fut = api_v3_hf_margin_stop_order(msg_tp_order2);

                let msg_sl_order2: String = serialize_body(&Some(msg_sl_order))?;
                let sl_fut = api_v3_hf_margin_stop_order(msg_sl_order2);

                let (tp_res, sl_res) = tokio::join!(tp_fut, sl_fut);

                match (&tp_res, &sl_res) {
                    (Ok(tp_resp), Ok(sl_resp)) => {
                        match tp_resp.data {
                            Some(ref response_data) => match update_exit_tp_order_id_bot_by_exit_tp_client_oid(pool, exchange, &response_data.order_id, &response_data.client_oid).await {
                                Ok(_) => {}
                                Err(e) => {
                                    return handle_db_error(pool, exchange, format!("Failed update_exit_tp_order_id_bot_by_exit_tp_client_oid:{}", e)).await;
                                }
                            },
                            None => {}
                        }

                        match sl_resp.data {
                            Some(ref response_data) => match update_exit_sl_order_id_bot_by_exit_sl_client_oid(pool, exchange, &response_data.order_id, &response_data.client_oid).await {
                                Ok(_) => {}
                                Err(e) => {
                                    return handle_db_error(pool, exchange, format!("Failed update_exit_sl_order_id_bot_by_exit_sl_client_oid:{}", e)).await;
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
                                        return handle_db_error(pool, exchange, format!("Failed api_v3_hf_margin_stop_order_cancel_by_client_oid:{}", e)).await;
                                    }
                                };
                            }
                            None => {}
                        }

                        match delete_exit_sl_id_bot_by_client_oid(pool, exchange, &exit_sl_client_oid).await {
                            Ok(_) => {}
                            Err(e) => {
                                return handle_db_error(pool, exchange, format!("Failed delete_exit_sl_id_bot_by_client_oid:{}", e)).await;
                            }
                        }

                        let _ = handle_db_error(pool, exchange, format!("Failed add TP order: {}. SL was cancelled for symmetry.", tp_err)).await;
                    }
                    (Ok(tp_resp), Err(sl_err)) => {
                        match tp_resp.data {
                            Some(ref response_data) => match api_v3_hf_margin_stop_order_cancel_by_client_oid(&response_data.client_oid).await {
                                Ok(_) => {}
                                Err(e) => {
                                    return handle_db_error(pool, exchange, format!("Failed api_v3_hf_margin_stop_order_cancel_by_client_oid:{}", e)).await;
                                }
                            },
                            None => {}
                        }

                        match delete_exit_tp_id_bot_by_client_oid(pool, exchange, &exit_tp_client_oid).await {
                            Ok(_) => {}
                            Err(e) => {
                                return handle_db_error(pool, exchange, format!("Failed delete_exit_tp_id_bot_by_client_oid:{}", e)).await;
                            }
                        }

                        let _ = handle_db_error(pool, exchange, format!("Failed add SL order: {}. TP was cancelled for symmetry.", sl_err)).await;
                    }
                    (Err(tp_err), Err(sl_err)) => {
                        let _ = handle_db_error(pool, exchange, format!("Failed add both stop orders: TP={}, SL={}", tp_err, sl_err)).await;
                        match delete_symbol_bot_by_exit_sl_client_oid(pool, exchange, &exit_sl_client_oid).await {
                            Ok(_) => {}
                            Err(e) => {
                                return handle_db_error(pool, exchange, format!("Failed delete_symbol_bot_by_exit_sl_client_oid:{}", e)).await;
                            }
                        }
                        match delete_exit_sl_id_bot_by_client_oid(pool, exchange, &exit_sl_client_oid).await {
                            Ok(_) => {}
                            Err(e) => {
                                return handle_db_error(pool, exchange, format!("Failed delete_exit_sl_id_bot_by_client_oid:{}", e)).await;
                            }
                        }
                        match delete_exit_tp_id_bot_by_client_oid(pool, exchange, &exit_tp_client_oid).await {
                            Ok(_) => {}
                            Err(e) => {
                                return handle_db_error(pool, exchange, format!("Failed delete_exit_tp_id_bot_by_client_oid:{}", e)).await;
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
                        return handle_db_error(pool, exchange, format!("Failed update_exit_tp_client_oid_bot_by_entry_client_oid:{}", e)).await;
                    }
                }
                // add exit_sl_client_oid by entry_id
                match update_exit_sl_client_oid_bot_by_entry_client_oid(pool, exchange, client_oid, &exit_sl_client_oid).await {
                    Ok(_) => {}
                    Err(e) => {
                        return handle_db_error(pool, exchange, format!("Failed update_exit_sl_client_oid_bot_by_entry_client_oid:{}", e)).await;
                    }
                }

                let msg_tp_order2: String = serialize_body(&Some(msg_tp_order))?;
                let tp_fut = api_v3_hf_margin_stop_order(msg_tp_order2);

                let msg_sl_order2: String = serialize_body(&Some(msg_sl_order))?;
                let sl_fut = api_v3_hf_margin_stop_order(msg_sl_order2);
                let (tp_res, sl_res) = tokio::join!(tp_fut, sl_fut);

                match (&tp_res, &sl_res) {
                    (Ok(tp_resp), Ok(sl_resp)) => {
                        match tp_resp.data {
                            Some(ref response_data) => match update_exit_tp_order_id_bot_by_exit_tp_client_oid(pool, exchange, &response_data.order_id, &response_data.client_oid).await {
                                Ok(_) => {}
                                Err(e) => {
                                    return handle_db_error(pool, exchange, format!("Failed update_exit_tp_order_id_bot_by_exit_tp_client_oid:{}", e)).await;
                                }
                            },
                            None => {}
                        }

                        match sl_resp.data {
                            Some(ref response_data) => match update_exit_sl_order_id_bot_by_exit_sl_client_oid(pool, exchange, &response_data.order_id, &response_data.client_oid).await {
                                Ok(_) => {}
                                Err(e) => {
                                    return handle_db_error(pool, exchange, format!("Failed update_exit_sl_order_id_bot_by_exit_sl_client_oid:{}", e)).await;
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
                                    return handle_db_error(pool, exchange, format!("Failed api_v3_hf_margin_stop_order_cancel_by_client_oid:{}", e)).await;
                                }
                            },
                            None => {}
                        }

                        match delete_exit_sl_id_bot_by_client_oid(pool, exchange, &exit_sl_client_oid).await {
                            Ok(_) => {}
                            Err(e) => {
                                return handle_db_error(pool, exchange, format!("Failed delete_exit_sl_id_bot_by_client_oid:{}", e)).await;
                            }
                        }

                        let _ = handle_db_error(pool, exchange, format!("Failed add TP order: {}. SL was cancelled for symmetry.", tp_err)).await;
                    }
                    (Ok(tp_resp), Err(sl_err)) => match tp_resp.data {
                        Some(ref response_data) => match api_v3_hf_margin_stop_order_cancel_by_client_oid(&response_data.client_oid).await {
                            Ok(_) => {
                                match delete_exit_tp_id_bot_by_client_oid(pool, exchange, &exit_tp_client_oid).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        return handle_db_error(pool, exchange, format!("Failed delete_exit_tp_id_bot_by_client_oid:{}", e)).await;
                                    }
                                }
                                let _ = handle_db_error(pool, exchange, format!("Failed add SL order: {}. TP was cancelled for symmetry.", sl_err)).await;
                            }
                            Err(e) => {
                                return handle_db_error(pool, exchange, format!("Failed api_v3_hf_margin_stop_order_cancel_by_client_oid:{}", e)).await;
                            }
                        },
                        None => {}
                    },
                    (Err(tp_err), Err(sl_err)) => {
                        let _ = handle_db_error(pool, exchange, format!("Failed add both stop orders: TP={}, SL={}", tp_err, sl_err)).await;
                        match delete_symbol_bot_by_exit_sl_client_oid(pool, exchange, &exit_sl_client_oid).await {
                            Ok(_) => {}
                            Err(e) => {
                                return handle_db_error(pool, exchange, format!("Failed delete_symbol_bot_by_exit_sl_client_oid:{}", e)).await;
                            }
                        }
                        match delete_exit_sl_id_bot_by_client_oid(pool, exchange, &exit_sl_client_oid).await {
                            Ok(_) => {}
                            Err(e) => {
                                let _ = handle_db_error(pool, exchange, format!("Failed delete_exit_sl_id_bot_by_client_oid:{}", e)).await;
                            }
                        }
                        match delete_exit_tp_id_bot_by_client_oid(pool, exchange, &exit_tp_client_oid).await {
                            Ok(_) => {}
                            Err(e) => {
                                let _ = handle_db_error(pool, exchange, format!("Failed delete_exit_tp_id_bot_by_client_oid:{}", e)).await;
                            }
                        }
                    }
                }
            }

            // delete entry_id from db
            match set_null_entry_client_oid_by_entry_client_oid(pool, exchange, client_oid).await {
                Ok(_) => {}
                Err(e) => {
                    let _ = handle_db_error(pool, exchange, format!("Failed set_null_entry_client_oid_by_entry_client_oid: {}", e)).await;
                }
            }
        }
        Ok(None) => {}
        Err(e) => {
            let _ = handle_db_error(pool, exchange, format!("Failed get_bot_by_entry_client_oid: {}", e)).await;
        }
    }

    Ok(())
}

pub async fn handle_position_event(position: PositionData, pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // repay borrow
    for (asset, token_liability_str) in &position.debt_list {
        match position.asset_list.get(asset) {
            Some(asset_info) => match (token_liability_str.parse::<f64>(), asset_info.available.parse::<f64>()) {
                (Ok(liability), Ok(available)) => {
                    if liability > 0.0 {
                        if available >= liability {
                            let body = json!({
                                "currency": asset,
                                "size": token_liability_str,
                                "isIsolated": false,
                                "isHf": true
                            });

                            let body_str: String = serialize_body(&Some(body))?;

                            match create_repay_order(body_str).await {
                                Ok(_) => {
                                    log::info!("Repay {} {} liability with available {}", token_liability_str, asset, &asset_info.available);
                                }
                                Err(e) => {
                                    let _ = handle_db_error(pool, exchange, format!("Failed to repay liability:{} on asset:{} {}", token_liability_str, asset, e)).await;
                                    continue;
                                }
                            }
                        } else if available > 0.0 {
                            let body = json!({
                                "currency": asset,
                                "size": &asset_info.available,
                                "isIsolated": false,
                                "isHf": true
                            });

                            let body_str: String = serialize_body(&Some(body))?;

                            match create_repay_order(body_str).await {
                                Ok(_) => {
                                    log::info!("Partially repay {} {} liability with available {}", token_liability_str, asset, &asset_info.available);
                                }
                                Err(e) => {
                                    let _ = handle_db_error(pool, exchange, format!("Failed to partially repay liability:{} on asset:{} {}", token_liability_str, asset, e)).await;
                                    continue;
                                }
                            }
                        }
                    }
                }
                (Err(e), _) => {
                    let _ = handle_db_error(pool, exchange, format!("Failed to parse token_liability_str:{} {}", token_liability_str, e)).await;
                    continue;
                }
                (_, Err(e)) => {
                    let _ = handle_db_error(pool, exchange, format!("Failed to parse asset_info.available:{} {}", asset_info.available, e)).await;
                    continue;
                }
            },
            None => {
                let _ = handle_db_error(pool, exchange, format!("Failed get asset:{} from:{:.?}", asset, position.asset_list)).await;
                continue;
            }
        }
    }
    match upsert_position_ratio(pool, exchange, position.debt_ratio, position.total_asset, &position.margin_coefficient_total_asset, &position.total_debt).await {
        Ok(_) => {}
        Err(e) => {
            return handle_db_error(pool, exchange, format!("Failed to upsert margin account state:{}", e)).await;
        }
    }

    for (symbol, amount) in &position.debt_list {
        match upsert_position_debt(pool, exchange, symbol, amount).await {
            Ok(_) => {}
            Err(e) => {
                let _ = handle_db_error(pool, exchange, format!("Failed to insert debt margin account state:{}", e)).await;

                continue;
            }
        }
    }
    for (symbol, symbol_info) in &position.asset_list {
        match upsert_position_asset(pool, exchange, symbol, &symbol_info.total, &symbol_info.available, &symbol_info.hold).await {
            Ok(_) => {}
            Err(e) => {
                let _ = handle_db_error(pool, exchange, format!("Failed to insert asset margin account state:{}", e)).await;
                continue;
            }
        }
    }

    Ok(())
}

pub async fn handle_advanced_orders(order: AdvancedOrders, pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    log::info!("{:.?}", order);
    match order.error {
        Some(_) => {}
        None => return Ok(()),
    }

    let _ = handle_db_error(pool, exchange, format!("Got error on stop order : {:?}", order)).await;

    const MAX_RETRIES: u32 = 1000;
    let mut attempt = 0;

    loop {
        tokio::time::sleep(Duration::from_millis(RETRY_DELAY_BASE * attempt as u64)).await;
        if attempt >= MAX_RETRIES {
            break Ok(());
        }
        attempt += 1;

        let order_id_ref = &order.order_id;
        let stop_ref = &order.stop;
        let side_ref = &order.side;
        let symbol_ref = &order.symbol;
        let funds_clone: Option<String> = order.funds.clone();
        let size_clone: Option<String> = order.size.clone();
        let new_exit_client_oid: String = Uuid::new_v4().to_string();

        let order_result = match stop_ref.as_str() {
            "loss" => {
                // need find sl
                match update_exit_sl_client_oid_bot_by_exit_sl_order_id(pool, exchange, order_id_ref, &new_exit_client_oid).await {
                    Ok(_) => match side_ref.as_str() {
                        "buy" => match funds_clone {
                            Some(funds) => make_hf_funds_margin_order(pool, exchange, &new_exit_client_oid, side_ref, symbol_ref, funds, "market", true, false).await,
                            None => {
                                let _ =
                                    handle_db_error(pool, exchange, format!("Fail parse funds order:{} new_exit_sl_client_oid:{} funds_clone:{:.?}", order_id_ref, new_exit_client_oid, funds_clone,))
                                        .await;
                                continue;
                            }
                        },
                        "sell" => match size_clone {
                            Some(size) => make_hf_size_margin_order(pool, exchange, &new_exit_client_oid, side_ref, symbol_ref, size, "market", true, false).await,
                            None => {
                                let _ = handle_db_error(pool, exchange, format!("Fail parse size order:{} new_exit_sl_client_oid:{} size_clone:{:.?}", order_id_ref, new_exit_client_oid, size_clone,))
                                    .await;
                                continue;
                            }
                        },
                        _ => {
                            let _ = handle_db_error(pool, exchange, format!("Fail match side_clone:{}", side_ref)).await;
                            continue;
                        }
                    },
                    Err(e) => {
                        let _ = handle_db_error(pool, exchange, format!("Failed save bot info: order_id_clone:{} new_exit_sl_client_oid:{} {}", order_id_ref, new_exit_client_oid, e)).await;
                        continue;
                    }
                }
            }
            "entry" => {
                // need find tp
                match update_exit_tp_client_oid_bot_by_exit_tp_order_id(pool, exchange, order_id_ref, &new_exit_client_oid).await {
                    Ok(_) => match side_ref.as_str() {
                        "buy" => match funds_clone {
                            Some(funds) => make_hf_funds_margin_order(pool, exchange, &new_exit_client_oid, side_ref, symbol_ref, funds, "market", true, false).await,
                            None => {
                                let _ = handle_db_error(
                                    pool,
                                    exchange,
                                    format!("Fail parse funds_clone order:{} new_exit_tp_client_oid:{} funds_clone:{:.?}", order_id_ref, new_exit_client_oid, funds_clone),
                                )
                                .await;
                                continue;
                            }
                        },
                        "sell" => match size_clone {
                            Some(size) => make_hf_size_margin_order(pool, exchange, &new_exit_client_oid, side_ref, symbol_ref, size, "market", true, false).await,
                            None => {
                                let _ = handle_db_error(
                                    pool,
                                    exchange,
                                    format!("Fail parse size_clone order:{} new_exit_tp_client_oid:{} size_clone:{:.?}", order_id_ref, new_exit_client_oid, size_clone),
                                )
                                .await;
                                continue;
                            }
                        },
                        _ => {
                            let _ = handle_db_error(pool, exchange, format!("Fail match side_clone:{}", side_ref)).await;
                            continue;
                        }
                    },
                    Err(e) => {
                        let _ = handle_db_error(pool, exchange, format!("Failed save bot info: order_id_clone:{} new_exit_tp_client_oid:{} {}", order_id_ref, new_exit_client_oid, e)).await;
                        continue;
                    }
                }
            }
            _ => {
                let _ = handle_db_error(pool, exchange, format!("Fail match stop_clone:{}", stop_ref)).await;
                continue;
            }
        };

        match order_result {
            Ok(_) => {
                log::info!("✅ Order re-placed: {} {} (attempt {}/{})", order_id_ref, new_exit_client_oid, attempt, MAX_RETRIES);
                break Ok(());
            }
            Err(e) => return handle_db_error(pool, exchange, format!("❌ Order failed: {} {} (attempt {}/{}) {}", order_id_ref, new_exit_client_oid, attempt, MAX_RETRIES, e)).await,
        }
    }
}

pub async fn process_kcn_msg(pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str, msg: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match serde_json::from_str::<KuCoinMessage>(msg) {
        Ok(event) => match event {
            KuCoinMessage::Welcome(data) => {
                match serde_json::to_value(&data) {
                    Ok(data) => match insert_db_event(pool, exchange, data).await {
                        Ok(_) => {
                            return Ok(());
                        }
                        Err(e) => return handle_db_error(pool, exchange, format!("Failed insert_db_event:{}", e)).await,
                    },
                    Err(e) => return handle_db_error(pool, exchange, format!("Failed to serialize event {:?}:{}", data, e)).await,
                };
            }
            KuCoinMessage::Message(data) => {
                if data.topic == "/account/balance" {
                    match BalanceData::deserialize(&data.data) {
                        Ok(balance) => match insert_db_balance(pool, exchange, balance).await {
                            Ok(_) => {
                                return Ok(());
                            }
                            Err(e) => return handle_db_error(pool, exchange, format!("Failed insert_db_balance data: {:.?} {}", data.data, e)).await,
                        },
                        Err(e) => return handle_db_error(pool, exchange, format!("Failed to parse message {:?} {}", data.data, e)).await,
                    }
                } else if data.topic == "/spotMarket/tradeOrdersV2" {
                    match OrderData::deserialize(&data.data) {
                        Ok(order) => match handle_trade_order_event(order, pool, exchange).await {
                            Ok(_) => {
                                return Ok(());
                            }
                            Err(e) => return handle_db_error(pool, exchange, format!("Failed handle_trade_order_event data: {:.?} {}", data.data, e)).await,
                        },
                        Err(e) => return handle_db_error(pool, exchange, format!("Failed to parse message {:?} {}", data.data, e)).await,
                    }
                } else if data.topic == "/spotMarket/advancedOrders" {
                    match AdvancedOrders::deserialize(&data.data) {
                        Ok(order) => match handle_advanced_orders(order, pool, exchange).await {
                            Ok(_) => Ok(()),
                            Err(e) => return handle_db_error(pool, exchange, format!("Failed handle_advanced_orders data: {:.?} {}", data.data, e)).await,
                        },
                        Err(e) => return handle_db_error(pool, exchange, format!("Failed to parse message {:?} {}", data.data, e)).await,
                    }
                } else if data.topic == "/margin/position" {
                    match PositionData::deserialize(&data.data) {
                        Ok(position) => match handle_position_event(position, pool, exchange).await {
                            Ok(_) => {
                                return Ok(());
                            }
                            Err(e) => return handle_db_error(pool, exchange, format!("Failed handle_position_event data:{:.?} {}", data.data, e)).await,
                        },
                        Err(e) => return handle_db_error(pool, exchange, format!("Failed to parse message {:?} {}", data.data, e)).await,
                    }
                } else {
                    return handle_db_error(pool, exchange, format!("Unknown topic: {}", data.topic)).await;
                }
            }
            KuCoinMessage::Ack(data) => {
                match serde_json::to_value(&data) {
                    Ok(data) => match insert_db_event(pool, exchange, data).await {
                        Ok(_) => {
                            return Ok(());
                        }
                        Err(e) => return handle_db_error(pool, exchange, format!("Failed insert_db_event:{}", e)).await,
                    },
                    Err(e) => return handle_db_error(pool, exchange, format!("Failed to serialize event:{:?} {}", data, e)).await,
                };
            }
            KuCoinMessage::Error(data) => return handle_db_error(pool, exchange, format!("Got error in WS {:?}", data)).await,
            KuCoinMessage::Unknown => return handle_db_error(pool, exchange, format!("Unknown WS message type")).await,
        },
        Err(e) => return handle_db_error(pool, exchange, format!("Failed to parse message:{} {}", msg, e)).await,
    }
}

pub async fn make_random_trade(pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str, balance_funds: f64, trade_bot_id: i32) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    const MAX_RETRIES: u32 = 10;
    let mut attempt = 0;

    loop {
        if attempt >= MAX_RETRIES {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(RETRY_DELAY_BASE * attempt as u64)).await;
        attempt += 1;

        let tradeable_symbol: String = match get_random_symbol(pool, exchange).await {
            Ok(Some(tradeable_symbol)) => tradeable_symbol,
            Ok(None) => {
                let _ = handle_db_error(pool, exchange, format!("Failed get_random_symbol:")).await;
                continue;
            }
            Err(e) => {
                let _ = handle_db_error(pool, exchange, format!("Failed get_random_symbol:{}", e)).await;

                continue;
            }
        };

        let symbol_info: Symbol = match fetch_symbol_info_by_symbol(pool, exchange, &tradeable_symbol).await {
            Ok(Some(symbol_info)) => symbol_info,
            Ok(None) => {
                let _ = handle_db_error(pool, exchange, format!("Symbol info not found for {}", tradeable_symbol)).await;
                continue;
            }
            Err(e) => {
                let _ = handle_db_error(pool, exchange, format!("Fail fetch_symbol_info_by_symbol: {} {}", &tradeable_symbol, e)).await;
                continue;
            }
        };

        let entry_client_oid: String = Uuid::new_v4().to_string();

        match update_bot_entry_client_oid_by_id(pool, exchange, Some(&tradeable_symbol), Some(&entry_client_oid), trade_bot_id).await {
            Ok(_) => {}
            Err(e) => {
                let _ = handle_db_error(pool, exchange, format!("Failed save bot info: entry_client_oid:{} trade_bot.id:{}, {}", entry_client_oid, trade_bot_id, e)).await;
                continue;
            }
        }

        let order_result = match get_random_side() {
            "sell" => {
                let base_increment: f64 = match symbol_info.base_increment.parse::<f64>() {
                    Ok(base_increment) => base_increment,
                    Err(e) => {
                        let _ = handle_db_error(pool, exchange, format!("Failed parse base_increment: {} {}", symbol_info.base_increment, e)).await;
                        continue;
                    }
                };
                let token_price: f64 = match get_ticker_price(&tradeable_symbol).await {
                    Ok(token_price_str) => match token_price_str.parse::<f64>() {
                        Ok(token_price) => token_price,
                        Err(e) => {
                            let _ = handle_db_error(pool, exchange, format!("Failed parse price: {} {}", token_price_str, e)).await;
                            continue;
                        }
                    },
                    Err(e) => {
                        let _ = handle_db_error(pool, exchange, format!("Failed get price: {} {}", tradeable_symbol, e)).await;
                        continue;
                    }
                };
                let token_size: f64 = balance_funds / token_price;
                make_hf_size_margin_order(pool, exchange, &entry_client_oid, "sell", &tradeable_symbol, format_assert(token_size, base_increment), "market", true, false).await
            }
            "buy" => {
                let quote_increment: f64 = match symbol_info.quote_increment.parse::<f64>() {
                    Ok(quote_increment) => quote_increment,
                    Err(e) => {
                        let _ = handle_db_error(pool, exchange, format!("Failed parse quote_increment: {} {}", symbol_info.quote_increment, e)).await;
                        continue;
                    }
                };
                make_hf_funds_margin_order(pool, exchange, &entry_client_oid, "buy", &tradeable_symbol, format_assert(balance_funds, quote_increment), "market", true, false).await
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
                    Err(e) => return handle_db_error(pool, exchange, format!("Failed update_bot_entry_client_oid_by_id:{}", e)).await,
                }
                let _ = handle_db_error(pool, exchange, format!("❌ Order failed (attempt {}/{}): {} {}", attempt, MAX_RETRIES, tradeable_symbol, e)).await;
                continue;
            }
        }
    }
}

pub async fn spawn_process_kcn_msg(pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str, mut rx_in: tokio::sync::mpsc::Receiver<String>) {
    loop {
        match rx_in.recv().await {
            Some(msg) => match process_kcn_msg(pool, exchange, &msg).await {
                Ok(_) => {}
                Err(_) => {}
            },
            None => {
                log::info!("Channel closed, exiting message processor");
                break;
            }
        }
    }
    log::info!("Message processor stopped");
}

pub async fn make_hf_funds_margin_order(
    pool: &sqlx::Pool<sqlx::Postgres>,
    exchange: &str,
    client_oid: &str,
    side: &str,
    symbol: &str,
    funds: String,
    type_: &'static str,
    auto_borrow: bool,
    auto_repay: bool,
) -> Result<MakeOrderRes, Box<dyn std::error::Error + Send + Sync>> {
    // only for buy orders
    let args_time_in_force: &str = "GTC";

    match insert_db_msgsend(pool, exchange, Some(symbol), Some(side), None, Some(&funds), None, Some(args_time_in_force), Some(type_), Some(&auto_borrow), Some(&auto_repay), Some(client_oid), None)
        .await
    {
        Ok(_) => {}
        Err(e) => {
            let _ = handle_db_error(
                pool,
                exchange,
                format!(
                    "Failed insert_db_msgsend: exchange:{} client_oid:{} side:{} symbol:{} funds:{} type:{} auto_borrow:{} auto_repay:{} {}",
                    exchange, client_oid, side, symbol, funds, type_, auto_borrow, auto_repay, e
                ),
            )
            .await;
            return Err("".into());
        }
    };
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

    let body_str: String = serialize_body(&Some(msg))?;
    match add_api_v3_hf_margin_order(body_str).await {
        Ok(data) => Ok(data),
        Err(e) => {
            let _ = handle_db_error(pool, exchange, format!("Failed to send order:{}", e)).await;
            return Err("".into());
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
    type_: &'static str,
    auto_borrow: bool,
    auto_repay: bool,
) -> Result<MakeOrderRes, Box<dyn std::error::Error + Send + Sync>> {
    // only for sell orders
    let args_time_in_force: &str = "GTC";

    match insert_db_msgsend(pool, exchange, Some(symbol), Some(side), Some(&size), None, None, Some(args_time_in_force), Some(type_), Some(&auto_borrow), Some(&auto_repay), Some(client_oid), None)
        .await
    {
        Ok(_) => {}
        Err(e) => {
            let _ = handle_db_error(
                pool,
                exchange,
                format!(
                    "Failed insert_db_msgsend: exchange:{} client_oid:{} side:{} symbol:{} size:{} type:{} auto_borrow:{} auto_repay:{} {}",
                    exchange, client_oid, side, symbol, size, type_, auto_borrow, auto_repay, e
                ),
            )
            .await;
            return Err("".into());
        }
    };
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

    let body_str: String = serialize_body(&Some(msg))?;
    match add_api_v3_hf_margin_order(body_str).await {
        Ok(data) => Ok(data),
        Err(e) => {
            let _ = handle_db_error(pool, exchange, format!("Failed to send order:{}", e)).await;
            return Err(e);
        }
    }
}
