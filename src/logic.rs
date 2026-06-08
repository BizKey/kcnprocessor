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
    add_api_v3_hf_margin_order, api_v3_hf_margin_stop_order, api_v3_hf_margin_stop_order_cancel_by_client_oid, build_query_string, create_repay_order, get_all_margin_accounts, get_ticker_price,
    sent_account_transfer, serialize_body,
};
use micromap::Map;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::json;
use std::str::FromStr;
use tokio::time::{Duration, sleep};
use uuid::Uuid;

fn tp_buy_percent() -> Decimal {
    Decimal::from_str("1.07").unwrap() // +7%
}

fn sl_buy_percent() -> Decimal {
    Decimal::from_str("0.95").unwrap() // -5%
}

fn tp_sell_percent() -> Decimal {
    Decimal::from_str("0.93").unwrap() // -7%
}

fn sl_sell_percent() -> Decimal {
    Decimal::from_str("1.05").unwrap() // +5%
}

const RETRY_DELAY_BASE: u64 = 500;
const BOT_INIT_DELAY: Duration = Duration::from_secs(5);
const AUTO_CLEAN_DELAY: Duration = Duration::from_secs(1);

pub fn get_random_side() -> &'static str {
    if fastrand::bool() { "buy" } else { "sell" }
}

pub fn format_assert_decimal(size: Decimal, increment: Decimal) -> String {
    let precision = increment.scale() as usize;

    if precision == 0 {
        // Целый increment - округляем вниз до ближайшего кратного
        let increment_int: i64 = increment.to_string().parse().unwrap_or(1);
        let size_int: i64 = size.floor().to_string().parse().unwrap_or(0);
        let rounded_down = (size_int / increment_int) * increment_int;
        return rounded_down.to_string();
    }

    // Дробный increment (0.01, 0.001, и т.д.)
    let s = size.to_string();

    if let Some(dot_pos) = s.find('.') {
        let integer_part = &s[..dot_pos];
        let fractional_part = &s[dot_pos + 1..];

        let truncated = if fractional_part.len() >= precision { &fractional_part[..precision] } else { fractional_part };

        let trimmed = truncated.trim_end_matches('0');

        if trimmed.is_empty() { integer_part.to_string() } else { format!("{}.{}", integer_part, trimmed) }
    } else {
        if precision == 0 { s } else { format!("{}.{}", s, "0".repeat(precision)) }
    }
}

pub async fn create_init_orders(pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let trade_bots: Vec<Bot> = match get_all_bots_for_trade(pool, exchange).await {
        Ok(trade_bots) => trade_bots,
        Err(e) => return handle_db_error(pool, exchange, format!("Failed get_all_bots_for_trade:{}", e)).await,
    };

    for trade_bot in trade_bots.iter() {
        sleep(BOT_INIT_DELAY).await;
        let token_funds = match trade_bot.balance_decimal() {
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
    let mut query_params: Map<&str, &str, 8> = Map::new();

    query_params.insert("quoteCurrency", "USDT");
    query_params.insert("queryType", "MARGIN");

    match get_all_margin_accounts(build_query_string(query_params)).await {
        Ok(accounts) => {
            sleep(AUTO_CLEAN_DELAY).await;
            let mut passed: bool = true;
            for account in accounts.data.accounts.iter() {
                let token_liability: Decimal = match account.liability_decimal() {
                    Ok(token_liability) => token_liability,
                    Err(e) => {
                        let _ = handle_db_error(pool, exchange, format!("Failed parse price: {} {}", account.liability, e)).await;
                        passed = false;
                        continue;
                    }
                };
                let token_available: Decimal = match account.available_decimal() {
                    Ok(token_liability) => token_liability,
                    Err(e) => {
                        let _ = handle_db_error(pool, exchange, format!("Failed parse price: {} {}", account.available, e)).await;
                        passed = false;
                        continue;
                    }
                };

                if token_liability > Decimal::ZERO {
                    passed = false;
                    if token_available >= token_liability {
                        log::info!("Can repay {} {} liability with available {}", account.liability, &account.currency, account.available);
                        let body = json!({
                            "currency": &account.currency,
                            "size": &account.liability,
                            "isIsolated": false,
                            "isHf": true
                        });

                        let body_str: String = match serialize_body(Some(body)) {
                            Ok(body_str) => body_str,
                            Err(e) => return Err(e.into()),
                        };

                        match create_repay_order(body_str).await {
                            Ok(_) => {}
                            Err(e) => {
                                let _ = handle_db_error(pool, exchange, format!("Failed to repay liability:{}", e)).await;
                                continue;
                            }
                        }
                    } else if token_available > Decimal::ZERO {
                        log::info!("Can partially repay {} {} liability with available {}", account.liability, &account.currency, account.available);

                        let body = json!({
                            "currency": &account.currency,
                            "size": &account.available,
                            "isIsolated": false,
                            "isHf": true
                        });

                        let body_str: String = match serialize_body(Some(body)) {
                            Ok(body_str) => body_str,
                            Err(e) => return Err(e.into()),
                        };

                        match create_repay_order(body_str).await {
                            Ok(_) => {}
                            Err(e) => {
                                let _ = handle_db_error(pool, exchange, format!("Failed to partially repay debt:{}", e)).await;
                                continue;
                            }
                        }
                    } else if account.currency != "USDT" && token_available == Decimal::ZERO {
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

                        let mut query_params: Map<&str, &str, 8> = Map::new();

                        query_params.insert("symbol", &trade_symbol);

                        let token_price_obj = match get_ticker_price(build_query_string(query_params)).await {
                            Ok(token_price_obj) => token_price_obj,
                            Err(e) => {
                                let _ = handle_db_error(pool, exchange, format!("Failed get price: {} {}", trade_symbol, e)).await;
                                continue;
                            }
                        };

                        let token_price: Decimal = match token_price_obj.data.price_decimal() {
                            Ok(token_price) => token_price,
                            Err(e) => {
                                let _ = handle_db_error(pool, exchange, format!("Failed parse price: {:?} {}", token_price_obj, e)).await;
                                continue;
                            }
                        };

                        log::info!("Successfully get token:{} price:{}", &trade_symbol, token_price);

                        // calc price token on amount liability token
                        let token_funds: Decimal = token_price * token_liability;

                        let quote_increment: Decimal = match symbol_info.quote_increment_decimal() {
                            Ok(quote_increment) => quote_increment,
                            Err(e) => {
                                let _ = handle_db_error(pool, exchange, format!("Failed parse quote_increment: {} {}", symbol_info.quote_increment, e)).await;
                                continue;
                            }
                        };
                        // parse min_funds to int
                        let min_funds: Decimal = match symbol_info.min_funds_decimal() {
                            Ok(min_funds) => min_funds,
                            Err(e) => {
                                let _ = handle_db_error(pool, exchange, format!("Failed parse min_funds: {:?} {}", symbol_info.min_funds, e)).await;
                                continue;
                            }
                        };
                        let base_min_size: Decimal = match symbol_info.base_min_size_decimal() {
                            Ok(base_min_size) => base_min_size,
                            Err(e) => {
                                let _ = handle_db_error(pool, exchange, format!("Failed parse base_min_size: {} {}", symbol_info.base_min_size, e)).await;
                                continue;
                            }
                        };

                        // calc price token on amount base_min_size token
                        let min_funds_by_size: Decimal = token_price * base_min_size;

                        if token_funds <= min_funds.max(min_funds_by_size) {
                            match make_hf_funds_margin_order(
                                pool,
                                exchange,
                                &client_oid,
                                "buy",
                                &trade_symbol,
                                format_assert_decimal(min_funds.max(min_funds_by_size), quote_increment),
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
                            match make_hf_funds_margin_order(pool, exchange, &client_oid, "buy", &trade_symbol, format_assert_decimal(token_funds, quote_increment), "market", false, false).await {
                                Ok(_) => {}
                                Err(e) => {
                                    let _ = handle_db_error(pool, exchange, format!("Failed make_hf_funds_margin_order:{}", e)).await;
                                    continue;
                                }
                            }
                        };
                    }
                } else if account.currency != "USDT" && token_available > Decimal::ZERO {
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

                    let base_increment: Decimal = match symbol_info.base_increment_decimal() {
                        Ok(base_increment) => base_increment,
                        Err(e) => {
                            let _ = handle_db_error(pool, exchange, format!("Failed parse base_increment: {} {}", symbol_info.base_increment, e)).await;
                            continue;
                        }
                    };
                    // get price token
                    let mut query_params: Map<&str, &str, 8> = Map::new();

                    query_params.insert("symbol", &trade_symbol);

                    let token_price_obj = match get_ticker_price(build_query_string(query_params)).await {
                        Ok(token_price_obj) => token_price_obj,
                        Err(e) => {
                            let _ = handle_db_error(pool, exchange, format!("Failed get price: {} {}", trade_symbol, e)).await;
                            continue;
                        }
                    };

                    let token_price: Decimal = match token_price_obj.data.price_decimal() {
                        Ok(token_price) => token_price,
                        Err(e) => {
                            let _ = handle_db_error(pool, exchange, format!("Failed parse price: {:?} {}", token_price_obj, e)).await;
                            continue;
                        }
                    };

                    log::info!("Successfully get token:{} price:{}", &trade_symbol, token_price);

                    let base_min_size: Decimal = match symbol_info.base_min_size_decimal() {
                        Ok(base_min_size) => base_min_size,
                        Err(e) => {
                            let _ = handle_db_error(pool, exchange, format!("Failed parse base_min_size: {} {}", symbol_info.base_min_size, e)).await;
                            continue;
                        }
                    };

                    let quote_min_size: Decimal = match symbol_info.quote_min_size_decimal() {
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
                        let body_str: String = match serialize_body(Some(body)) {
                            Ok(body_str) => body_str,
                            Err(e) => return Err(e.into()),
                        };

                        match sent_account_transfer(body_str).await {
                            Ok(_) => {}
                            Err(e) => {
                                let _ = handle_db_error(pool, exchange, format!("Failed send {} to TRADE from MARGIN on {} {}", &account.currency.clone(), &account.available, e)).await;
                                continue;
                            }
                        }
                    } else {
                        match make_hf_size_margin_order(pool, exchange, &client_oid, "sell", &trade_symbol, format_assert_decimal(token_available, base_increment), "market", false, false).await {
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

    let price_increment = match symbol_info.price_increment_decimal() {
        Ok(price_increment) => price_increment,
        Err(e) => return handle_db_error(pool, exchange, format!("Failed parse price_increment: {} {}", symbol_info.price_increment, e)).await,
    };

    let quote_increment = match symbol_info.quote_increment_decimal() {
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
            match &bot.exit_sl_client_oid {
                Some(exit_sl_client_oid) => {
                    // clear exit_sl_client_oid in bots by id !!
                    match delete_exit_sl_id_bot_by_client_oid(pool, exchange, exit_sl_client_oid).await {
                        Ok(_) => {}
                        Err(e) => {
                            let _ = handle_db_error(pool, exchange, format!("Failed delete_exit_sl_id_bot_by_client_oid:{}", e)).await;
                        }
                    }
                    let mut query_params: Map<&str, &str, 8> = Map::new();

                    query_params.insert("clientOid", exit_sl_client_oid);

                    match api_v3_hf_margin_stop_order_cancel_by_client_oid(build_query_string(query_params)).await {
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
                Ok(Some(return_balance)) => {
                    {
                        if order.side == "buy" {
                            let old_balance = match bot.balance_decimal() {
                                Ok(old_balance) => old_balance,
                                Err(e) => return handle_db_error(pool, exchange, format!("Failed parse balance: {} {}", bot.balance, e)).await,
                            };
                            let new_balance = old_balance + old_balance - return_balance;
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
                    match &bot.exit_tp_client_oid {
                        Some(exit_tp_client_oid) => {
                            // clear exit_tp_client_oid in bots by entry_id
                            match delete_exit_tp_id_bot_by_client_oid(pool, exchange, exit_tp_client_oid).await {
                                Ok(_) => {}
                                Err(e) => return handle_db_error(pool, exchange, format!("Failed delete_exit_tp_id_bot_by_client_oid:{}", e)).await,
                            }
                            let mut query_params: Map<&str, &str, 8> = Map::new();

                            query_params.insert("clientOid", exit_tp_client_oid);

                            match api_v3_hf_margin_stop_order_cancel_by_client_oid(build_query_string(query_params)).await {
                                Ok(_) => {
                                    log::info!("Successfully cancel stop order :{}", &exit_tp_client_oid);
                                }
                                Err(e) => return handle_db_error(pool, exchange, format!("Failed cancel stop order:{}", e)).await,
                            }
                        }
                        None => {}
                    }

                    match get_total_match_value_by_client_oid(pool, exchange, client_oid).await {
                        Ok(Some(return_balance)) => {
                            if order.side == "buy" {
                                let old_balance = match bot.balance_decimal() {
                                    Ok(old_balance) => old_balance,
                                    Err(e) => return handle_db_error(pool, exchange, format!("Failed parse balance: {} {}", bot.balance, e)).await,
                                };
                                let new_balance = old_balance + old_balance - return_balance;
                                match update_balance_bot_by_exit_sl_client_oid(pool, exchange, client_oid, &format!("{:.4}", new_balance)).await {
                                    Ok(_) => {}
                                    Err(e) => return handle_db_error(pool, exchange, format!("Failed update_balance_bot_by_exit_sl_client_oid:{}", e)).await,
                                }
                                // create new random order
                                match make_random_trade(pool, exchange, new_balance, bot.id).await {
                                    Ok(()) => {}
                                    Err(e) => return handle_db_error(pool, exchange, format!("Error in make_random_trade:{}", e)).await,
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

            let filled_size: Decimal = match order.filled_size_decimal() {
                Ok(filled_size) => filled_size,
                Err(e) => {
                    let _ = handle_db_error(pool, exchange, format!("Failed parse filled_size: {:?} {}", &order, e)).await;
                    return Err(e.into());
                }
            };

            let new_balance: Decimal = match get_total_match_value_by_client_oid(pool, exchange, client_oid).await {
                Ok(Some(new_balance)) => new_balance,
                Ok(None) => return handle_db_error(pool, exchange, format!("No records found in events: {}", client_oid)).await,
                Err(e) => return handle_db_error(pool, exchange, format!("Failed get_total_match_value_by_client_oid: {} {}", client_oid, e)).await,
            };

            match update_bot_balance_by_entry_client_oid(pool, exchange, client_oid, &format!("{:.4}", new_balance)).await {
                Ok(_) => {}
                Err(e) => {
                    return handle_db_error(pool, exchange, format!("Failed update_bot_balance_by_entry_client_oid:{}", e)).await;
                }
            }

            if order.side == "buy" {
                let match_price: Decimal = new_balance / filled_size;
                let trigger_tp_price: Decimal = match_price * tp_buy_percent(); // price + 7%
                let trigger_sl_price: Decimal = match_price * sl_buy_percent(); // price - 5%

                let exit_tp_client_oid: String = Uuid::new_v4().to_string();
                let exit_sl_client_oid: String = Uuid::new_v4().to_string();

                // tp order
                let msg_tp_order: serde_json::Value = serde_json::json!({
                    "clientOid": exit_tp_client_oid,
                    "side": "sell",
                    "symbol": order.symbol,
                    "type": "market",
                    "stop": "entry",
                    "stopPrice": format_assert_decimal(trigger_tp_price, price_increment),
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
                    "stopPrice": format_assert_decimal(trigger_sl_price, price_increment),
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

                let msg_tp_order2: String = match serialize_body(Some(msg_tp_order)) {
                    Ok(body_str) => body_str,
                    Err(e) => return Err(e.into()),
                };
                let tp_fut = api_v3_hf_margin_stop_order(msg_tp_order2);

                let msg_sl_order2: String = match serialize_body(Some(msg_sl_order)) {
                    Ok(body_str) => body_str,
                    Err(e) => return Err(e.into()),
                };
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
                                let mut query_params: Map<&str, &str, 8> = Map::new();

                                query_params.insert("clientOid", &response_data.client_oid);

                                match api_v3_hf_margin_stop_order_cancel_by_client_oid(build_query_string(query_params)).await {
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
                            Some(ref response_data) => {
                                let mut query_params: Map<&str, &str, 8> = Map::new();

                                query_params.insert("clientOid", &response_data.client_oid);

                                match api_v3_hf_margin_stop_order_cancel_by_client_oid(build_query_string(query_params)).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        return handle_db_error(pool, exchange, format!("Failed api_v3_hf_margin_stop_order_cancel_by_client_oid:{}", e)).await;
                                    }
                                }
                            }
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
                let match_price: Decimal = new_balance / filled_size;
                let trigger_tp_price: Decimal = match_price * tp_sell_percent(); // price - 7%
                let trigger_sl_price: Decimal = match_price * sl_sell_percent(); // price + 5%

                let funds_tp: Decimal = trigger_tp_price * filled_size;
                let funds_sl: Decimal = trigger_sl_price * filled_size;

                let exit_tp_client_oid: String = Uuid::new_v4().to_string();
                let exit_sl_client_oid: String = Uuid::new_v4().to_string();

                let msg_tp_order: serde_json::Value = serde_json::json!({
                    "clientOid": exit_tp_client_oid,
                    "side": "buy",
                    "symbol": order.symbol,
                    "type": "market",
                    "stop": "loss",
                    "stopPrice": format_assert_decimal(trigger_tp_price, price_increment), // price - 7%
                    "isIsolated": false,
                    "autoBorrow": true,
                    "autoRepay": false,
                    "timeInForce": "GTC",
                    "funds": format_assert_decimal(funds_tp, quote_increment),
                });
                let msg_sl_order: serde_json::Value = serde_json::json!({
                   "clientOid": exit_sl_client_oid,
                    "side": "buy",
                    "symbol": order.symbol,
                    "type": "market",
                    "stop": "entry",
                    "stopPrice": format_assert_decimal(trigger_sl_price, price_increment), // price + 5%
                    "isIsolated": false,
                    "autoBorrow": true,
                    "autoRepay": false,
                    "timeInForce": "GTC",
                    "funds": format_assert_decimal(funds_sl, quote_increment),
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

                let msg_tp_order2: String = match serialize_body(Some(msg_tp_order)) {
                    Ok(body_str) => body_str,
                    Err(e) => return Err(e.into()),
                };
                let tp_fut = api_v3_hf_margin_stop_order(msg_tp_order2);

                let msg_sl_order2: String = match serialize_body(Some(msg_sl_order)) {
                    Ok(body_str) => body_str,
                    Err(e) => return Err(e.into()),
                };
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
                                let mut query_params: Map<&str, &str, 8> = Map::new();

                                query_params.insert("clientOid", &response_data.client_oid);
                                match api_v3_hf_margin_stop_order_cancel_by_client_oid(build_query_string(query_params)).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        return handle_db_error(pool, exchange, format!("Failed api_v3_hf_margin_stop_order_cancel_by_client_oid:{}", e)).await;
                                    }
                                }
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
                    (Ok(tp_resp), Err(sl_err)) => match tp_resp.data {
                        Some(ref response_data) => {
                            let mut query_params: Map<&str, &str, 8> = Map::new();

                            query_params.insert("clientOid", &response_data.client_oid);
                            match api_v3_hf_margin_stop_order_cancel_by_client_oid(build_query_string(query_params)).await {
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
                            }
                        }
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
    match position.debt_pairs() {
        Ok(debt_pair) => {
            for (asset, token_liability) in debt_pair {
                match position.asset_list.get(&asset) {
                    Some(asset_info) => match asset_info.available_decimal() {
                        Ok(available) => {
                            if token_liability > Decimal::ZERO {
                                if available >= token_liability {
                                    let body = json!({
                                        "currency": asset,
                                        "size": token_liability,
                                        "isIsolated": false,
                                        "isHf": true
                                    });

                                    let body_str: String = match serialize_body(Some(body)) {
                                        Ok(body_str) => body_str,
                                        Err(e) => return Err(e.into()),
                                    };

                                    match create_repay_order(body_str).await {
                                        Ok(_) => {
                                            log::info!("Repay {} {} liability with available {}", token_liability, asset, &asset_info.available);
                                        }
                                        Err(e) => {
                                            let _ = handle_db_error(pool, exchange, format!("Failed to repay liability:{} on asset:{} {}", token_liability, asset, e)).await;
                                            continue;
                                        }
                                    }
                                } else if available > Decimal::ZERO {
                                    let body = json!({
                                        "currency": asset,
                                        "size": &asset_info.available,
                                        "isIsolated": false,
                                        "isHf": true
                                    });

                                    let body_str: String = match serialize_body(Some(body)) {
                                        Ok(body_str) => body_str,
                                        Err(e) => return Err(e.into()),
                                    };

                                    match create_repay_order(body_str).await {
                                        Ok(_) => {
                                            log::info!("Partially repay {} {} liability with available {}", token_liability, asset, &asset_info.available);
                                        }
                                        Err(e) => {
                                            let _ = handle_db_error(pool, exchange, format!("Failed to partially repay liability:{} on asset:{} {}", token_liability, asset, e)).await;
                                            continue;
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
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
        }
        Err(e) => return Err(e.into()),
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
                    Ok(data) => match insert_db_event(pool, exchange, &data).await {
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
                            Ok(_) => Ok(()),
                            Err(e) => return handle_db_error(pool, exchange, format!("Failed insert_db_balance data: {:.?} {}", data.data, e)).await,
                        },
                        Err(e) => return handle_db_error(pool, exchange, format!("Failed to parse message {:?} {}", data.data, e)).await,
                    }
                } else if data.topic == "/spotMarket/tradeOrdersV2" {
                    match OrderData::deserialize(&data.data) {
                        Ok(order) => match handle_trade_order_event(order, pool, exchange).await {
                            Ok(_) => Ok(()),
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
                            Ok(_) => Ok(()),
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
                    Ok(data) => match insert_db_event(pool, exchange, &data).await {
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

pub async fn make_random_trade(pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str, balance_funds: Decimal, trade_bot_id: i32) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
                let base_increment: Decimal = match symbol_info.base_increment_decimal() {
                    Ok(base_increment) => base_increment,
                    Err(e) => {
                        let _ = handle_db_error(pool, exchange, format!("Failed parse base_increment: {} {}", symbol_info.base_increment, e)).await;
                        continue;
                    }
                };

                let mut query_params: Map<&str, &str, 8> = Map::new();

                query_params.insert("symbol", &tradeable_symbol);

                let token_price_obj = match get_ticker_price(build_query_string(query_params)).await {
                    Ok(token_price_obj) => token_price_obj,
                    Err(e) => {
                        let _ = handle_db_error(pool, exchange, format!("Failed get price: {} {}", &tradeable_symbol, e)).await;
                        continue;
                    }
                };

                let token_price: Decimal = match token_price_obj.data.price_decimal() {
                    Ok(token_price) => token_price,
                    Err(e) => {
                        let _ = handle_db_error(pool, exchange, format!("Failed parse price: {:?} {}", token_price_obj, e)).await;
                        continue;
                    }
                };
                let token_size = balance_funds / token_price;
                make_hf_size_margin_order(pool, exchange, &entry_client_oid, "sell", &tradeable_symbol, format_assert_decimal(token_size, base_increment), "market", true, false).await
            }
            "buy" => {
                let quote_increment = match symbol_info.quote_increment_decimal() {
                    Ok(quote_increment) => quote_increment,
                    Err(e) => {
                        let _ = handle_db_error(pool, exchange, format!("Failed parse quote_increment: {} {}", symbol_info.quote_increment, e)).await;
                        continue;
                    }
                };
                make_hf_funds_margin_order(pool, exchange, &entry_client_oid, "buy", &tradeable_symbol, format_assert_decimal(balance_funds, quote_increment), "market", true, false).await
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

    let body_str: String = match serialize_body(Some(msg)) {
        Ok(body_str) => body_str,
        Err(e) => return Err(e.into()),
    };
    match add_api_v3_hf_margin_order(body_str).await {
        Ok(data) => Ok(data),
        Err(e) => {
            let _ = handle_db_error(pool, exchange, format!("Failed to send order:{}", e)).await;
            Err("".into())
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

    let body_str: String = match serialize_body(Some(msg)) {
        Ok(body_str) => body_str,
        Err(e) => return Err(e.into()),
    };
    match add_api_v3_hf_margin_order(body_str).await {
        Ok(data) => Ok(data),
        Err(e) => {
            let _ = handle_db_error(pool, exchange, format!("Failed to send order:{}", e)).await;
            Err(e.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use rust_decimal::prelude::*;

    #[test]
    fn test_format_assert_decimal_real_data() {
        // Increment = 1000 (precision 0)
        let inc_1000 = Decimal::from_str("1000").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("1234.56").unwrap(), inc_1000), "1000");

        // Increment = 100 (precision 0)
        let inc_100 = Decimal::from_str("100").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("123.456").unwrap(), inc_100), "100");
        assert_eq!(format_assert_decimal(Decimal::from_str("199").unwrap(), inc_100), "100");
        assert_eq!(format_assert_decimal(Decimal::from_str("200").unwrap(), inc_100), "200");

        // Increment = 50 (precision 0)
        let inc_50 = Decimal::from_str("50").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("123.456").unwrap(), inc_50), "100");
        assert_eq!(format_assert_decimal(Decimal::from_str("149").unwrap(), inc_50), "100");
        assert_eq!(format_assert_decimal(Decimal::from_str("150").unwrap(), inc_50), "150");

        // Increment = 10 (precision 0)
        let inc_10 = Decimal::from_str("10").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("123.456").unwrap(), inc_10), "120");
        assert_eq!(format_assert_decimal(Decimal::from_str("125").unwrap(), inc_10), "120");

        // Increment = 1 (precision 0)
        let inc_1 = Decimal::from_str("1").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("123.456").unwrap(), inc_1), "123");
        assert_eq!(format_assert_decimal(Decimal::from_str("100").unwrap(), inc_1), "100");

        // Increment = 0.1 (precision 1)
        let inc_1 = Decimal::from_str("0.1").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("123.456").unwrap(), inc_1), "123.4");
        assert_eq!(format_assert_decimal(Decimal::from_str("99.999").unwrap(), inc_1), "99.9");

        // Increment = 0.01 (precision 2)
        let inc_2 = Decimal::from_str("0.01").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("123.456").unwrap(), inc_2), "123.45");
        assert_eq!(format_assert_decimal(Decimal::from_str("99.999").unwrap(), inc_2), "99.99");

        // Increment = 0.001 (precision 3)
        let inc_3 = Decimal::from_str("0.001").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("123.4567").unwrap(), inc_3), "123.456");

        // Increment = 0.0001 (precision 4)
        let inc_4 = Decimal::from_str("0.0001").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("123.45678").unwrap(), inc_4), "123.4567");

        // Increment = 0.0001 (precision 5)
        let inc_5 = Decimal::from_str("0.00001").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("123.45678").unwrap(), inc_5), "123.45678");

        // Increment = 0.000001 (precision 6)
        let inc_6 = Decimal::from_str("0.000001").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("123.456789").unwrap(), inc_6), "123.456789");

        // Increment = 0.0000001 (precision 7)
        let inc_7 = Decimal::from_str("0.0000001").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("123.4567891").unwrap(), inc_7), "123.4567891");

        // Increment = 0.00000001 (precision 8)
        let inc_8 = Decimal::from_str("0.00000001").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("0.123456789").unwrap(), inc_8), "0.12345678");

        // Increment = 0.000000001 (precision 9)
        let inc_9 = Decimal::from_str("0.000000001").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("0.00000000123").unwrap(), inc_9), "0.000000001");
    }
}
