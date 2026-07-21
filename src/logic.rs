use crate::api::db::{
    delete_exit_sl_id_bot_by_client_oid, delete_exit_tp_id_bot_by_client_oid, delete_symbol_bot_by_exit_sl_client_oid, fetch_currency_info_by_symbol, fetch_symbol_info_by_symbol,
    get_all_bots_for_trade, get_bot_by_client_oid, get_random_symbol, get_total_match_value_by_client_oid, handle_db_error, insert_db_balance, insert_db_event, insert_db_msgsend,
    insert_db_orderevent, set_null_entry_client_oid_by_entry_client_oid, update_balance_bot_by_exit_sl_client_oid, update_balance_bot_by_exit_tp_client_oid, update_bot_balance_by_entry_client_oid,
    update_bot_entry_client_oid_by_id, update_exit_sl_client_oid_bot_by_entry_client_oid, update_exit_sl_client_oid_bot_by_exit_sl_order_id, update_exit_sl_order_id_bot_by_exit_sl_client_oid,
    update_exit_tp_client_oid_bot_by_entry_client_oid, update_exit_tp_client_oid_bot_by_exit_tp_order_id, update_exit_tp_order_id_bot_by_exit_tp_client_oid, upsert_position_asset,
    upsert_position_debt, upsert_position_ratio,
};
use crate::api::models::{
    AdvancedOrders, ApiV1MarketOrderbookLevel1ResData, ApiV3MarginRepayResData, AssetInfo, BalanceData, Bot, Currencies, KuCoinMessage, MakeOrderResData, MarginAccountData, MessageData, OrderData,
    PositionData, Symbol,
};
use crate::api::requests::{
    api_v1_market_orderbook_level1_get, api_v3_accounts_universal_transfer_post, api_v3_hf_margin_order_post, api_v3_hf_margin_stop_order_cancel_by_client_oid_delete,
    api_v3_hf_margin_stop_order_post, api_v3_margin_accounts_get, api_v3_margin_repay_post, build_query_string, serialize_body,
};
use micromap::Map;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json;
use sqlx::PgPool;
use std::str::FromStr;
use tokio::time::{Duration, sleep};
use uuid::Uuid;

fn tp_buy_percent() -> Result<Decimal, String> {
    // +7%
    Ok(Decimal::from_str("1.07").map_err(|e| {
        log::error!("{}", e);
        format!("{}", e)
    })?)
}

fn sl_buy_percent() -> Result<Decimal, String> {
    // -5%
    Ok(Decimal::from_str("0.95").map_err(|e| {
        log::error!("{}", e);
        format!("{}", e)
    })?)
}

fn tp_sell_percent() -> Result<Decimal, String> {
    // -7%
    Ok(Decimal::from_str("0.93").map_err(|e| {
        log::error!("{}", e);
        format!("{}", e)
    })?)
}

fn sl_sell_percent() -> Result<Decimal, String> {
    // +5%
    Ok(Decimal::from_str("1.05").map_err(|e| {
        log::error!("{}", e);
        format!("{}", e)
    })?)
}

fn get_random_side() -> &'static str {
    if fastrand::bool() { "buy" } else { "sell" }
}

const RETRY_DELAY_BASE: u64 = 500;
const BOT_INIT_DELAY: Duration = Duration::from_secs(5);
const AUTO_CLEAN_DELAY: Duration = Duration::from_secs(5);

pub fn format_assert_decimal(size: Decimal, increment: Decimal) -> Result<String, String> {
    let precision = increment.scale() as usize;

    if precision == 0 {
        let increment_int: i64 = increment.to_string().parse().map_err(|e| {
            log::error!("Fail parse increment:{} error:{}", increment, e);
            format!("Fail parse increment:{} error:{}", increment, e)
        })?;

        let size_int: i64 = size.to_string().parse().map_err(|e| {
            log::error!("Fail parse size:{} error:{}", size, e);
            format!("Fail parse size:{} error:{}", size, e)
        })?;

        let rounded_down: i64 = (size_int / increment_int) * increment_int;
        return Ok(rounded_down.to_string());
    }

    // Дробный increment (0.01, 0.001, и т.д.)
    let s: String = size.to_string();

    if let Some(dot_pos) = s.find('.') {
        let integer_part = &s[..dot_pos];
        let fractional_part = &s[dot_pos + 1..];

        let truncated = if fractional_part.len() >= precision { &fractional_part[..precision] } else { fractional_part };

        let trimmed = truncated.trim_end_matches('0');

        if trimmed.is_empty() { Ok(integer_part.to_string()) } else { Ok(format!("{}.{}", integer_part, trimmed)) }
    } else {
        if precision == 0 { Ok(s) } else { Ok(format!("{}.{}", s, "0".repeat(precision))) }
    }
}

pub async fn create_init_orders(pool: &PgPool) -> Result<(), String> {
    let trade_bots: Vec<Bot> = match get_all_bots_for_trade(pool).await {
        Ok(trade_bots) => trade_bots,
        Err(e) => {
            handle_db_error(pool, e.clone()).await;
            return Err(e);
        }
    };

    for trade_bot in trade_bots.iter() {
        sleep(BOT_INIT_DELAY).await;
        let token_funds: Decimal = match trade_bot.balance_decimal() {
            Ok(token_funds) => token_funds,
            Err(e) => {
                handle_db_error(pool, e).await;
                continue;
            }
        };
        match make_random_trade(pool, token_funds, trade_bot.id).await {
            Ok(_) => {}
            Err(e) => {
                handle_db_error(pool, e).await;
            }
        }
    }
    log::info!("All bots initialized!");
    Ok(())
}

pub async fn get_all_accounts_data() -> Result<MarginAccountData, String> {
    let mut query_params: Map<&str, &str, 8> = Map::new();
    query_params.insert("quoteCurrency", "USDT");
    query_params.insert("queryType", "MARGIN");

    Ok(api_v3_margin_accounts_get(build_query_string(query_params)).await?)
}

pub async fn repay_account(pool: &PgPool, currency: &str, size: &str) -> Result<Option<ApiV3MarginRepayResData>, String> {
    log::info!("Repay {} liability:{}", size, currency);
    let body_str: String = match serialize_body(Some(serde_json::json!({
        "currency": currency,
        "size": size,
        "isIsolated": false,
        "isHf": true
    }))) {
        Ok(body_str) => body_str,
        Err(e) => {
            handle_db_error(pool, e.clone()).await;
            return Err(e);
        }
    };

    match api_v3_margin_repay_post(body_str).await {
        Ok(res) => Ok(res),
        Err(e) => {
            handle_db_error(pool, e.clone()).await;
            return Err(e);
        }
    }
}

pub async fn get_token_price(pool: &PgPool, trade_symbol: &str) -> Result<ApiV1MarketOrderbookLevel1ResData, String> {
    let mut query_params: Map<&str, &str, 8> = Map::new();
    query_params.insert("symbol", trade_symbol);

    let token_price_option: Option<ApiV1MarketOrderbookLevel1ResData> = match api_v1_market_orderbook_level1_get(build_query_string(query_params)).await {
        Ok(token_price_option) => token_price_option,
        Err(e) => return Err(e),
    };

    match token_price_option {
        Some(token_price_data) => Ok(token_price_data),
        None => {
            let msg: String = format!("Fail get token_price:{:?}", token_price_option);
            log::error!("{}", msg);
            handle_db_error(pool, msg.clone()).await;

            return Err(msg);
        }
    }
}

pub async fn transfer_amount(pool: &PgPool, currency: &str, amount: &str) -> Result<(), String> {
    let client_oid: String = Uuid::new_v4().to_string();

    let body_str: String = match serialize_body(Some(serde_json::json!({
        "currency": currency,
        "clientOid": client_oid,
        "amount": amount,
        "type": "INTERNAL",
        "fromAccountType": "MARGIN",
        "toAccountType": "TRADE"
    }))) {
        Ok(body_str) => body_str,
        Err(e) => {
            handle_db_error(pool, e.clone()).await;
            return Err(e);
        }
    };

    match api_v3_accounts_universal_transfer_post(body_str).await {
        Ok(_) => Ok(()),
        Err(e) => {
            handle_db_error(pool, e.clone()).await;
            return Err(e);
        }
    }
}

pub async fn auto_clean_account(pool: &PgPool) -> Result<bool, String> {
    sleep(AUTO_CLEAN_DELAY).await;

    let accounts: MarginAccountData = match get_all_accounts_data().await {
        Ok(accounts) => accounts,
        Err(e) => {
            handle_db_error(pool, e.clone()).await;
            return Err(e);
        }
    };

    let mut passed: bool = true;
    for account in accounts.accounts.iter() {
        let currency_info_option: Option<Currencies> = match fetch_currency_info_by_symbol(pool, &account.currency).await {
            Ok(currency_info_option) => currency_info_option,
            Err(e) => {
                handle_db_error(pool, e.clone()).await;
                return Err(e);
            }
        };

        let currency_info: Currencies = match currency_info_option {
            Some(currency_info) => currency_info,
            None => {
                let msg: String = format!("Currency info not found for {}", account.currency);
                log::error!("{}", msg);
                handle_db_error(pool, msg.clone()).await;

                return Err(msg);
            }
        };

        let precision_decimal: Decimal = match currency_info.precision_decimal() {
            Ok(precision_decimal) => precision_decimal,
            Err(e) => {
                handle_db_error(pool, e.clone()).await;
                return Err(e);
            }
        };

        let symbol_info_option: Option<Symbol> = match fetch_symbol_info_by_symbol(pool, &account.currency).await {
            Ok(symbol_info_option) => symbol_info_option,
            Err(e) => {
                handle_db_error(pool, e.clone()).await;
                return Err(e);
            }
        };

        let symbol_info: Symbol = match symbol_info_option {
            Some(info) => info,
            None => {
                let msg: String = format!("Symbol info not found for {}", &account.currency);
                log::error!("{}", msg);
                handle_db_error(pool, msg.clone()).await;

                return Err(msg);
            }
        };

        let quote_increment: Decimal = match symbol_info.quote_increment_decimal() {
            Ok(quote_increment) => quote_increment,
            Err(e) => {
                handle_db_error(pool, e.clone()).await;
                return Err(e);
            }
        };

        let min_funds: Decimal = match symbol_info.min_funds_decimal() {
            Ok(min_funds) => min_funds,
            Err(e) => {
                handle_db_error(pool, e.clone()).await;
                return Err(e);
            }
        };

        let base_min_size: Decimal = match symbol_info.base_min_size_decimal() {
            Ok(base_min_size) => base_min_size,
            Err(e) => {
                handle_db_error(pool, e.clone()).await;
                return Err(e);
            }
        };

        let base_increment: Decimal = match symbol_info.base_increment_decimal() {
            Ok(base_increment) => base_increment,
            Err(e) => {
                handle_db_error(pool, e.clone()).await;
                return Err(e);
            }
        };

        let quote_min_size: Decimal = match symbol_info.quote_min_size_decimal() {
            Ok(quote_min_size) => quote_min_size,
            Err(e) => {
                handle_db_error(pool, e.clone()).await;
                return Err(e);
            }
        };

        let token_liability: Decimal = match account.liability_decimal() {
            Ok(token_liability) => token_liability,
            Err(e) => {
                handle_db_error(pool, e.clone()).await;
                return Err(e);
            }
        };

        let token_available: Decimal = match account.available_decimal() {
            Ok(token_available) => token_available,
            Err(e) => {
                handle_db_error(pool, e.clone()).await;
                return Err(e);
            }
        };

        if token_liability > Decimal::ZERO {
            passed = false;
            if token_available >= token_liability {
                let size: String = match format_assert_decimal(token_liability, precision_decimal) {
                    Ok(size) => size,
                    Err(e) => {
                        handle_db_error(pool, e.clone()).await;
                        return Err(e);
                    }
                };
                match repay_account(pool, &account.currency, &size).await {
                    Ok(_) => continue,
                    Err(e) => {
                        handle_db_error(pool, e.clone()).await;
                        return Err(e);
                    }
                }
            } else if token_available > Decimal::ZERO {
                let size: String = match format_assert_decimal(token_available, precision_decimal) {
                    Ok(size) => size,
                    Err(e) => {
                        handle_db_error(pool, e.clone()).await;
                        return Err(e);
                    }
                };
                match repay_account(pool, &account.currency, &size).await {
                    Ok(_) => continue,
                    Err(e) => {
                        handle_db_error(pool, e.clone()).await;
                        return Err(e);
                    }
                }
            } else if account.currency != "USDT" && token_available == Decimal::ZERO {
                let trade_symbol: String = format!("{}-USDT", &account.currency);

                let token_price_data: ApiV1MarketOrderbookLevel1ResData = match get_token_price(pool, &trade_symbol).await {
                    Ok(token_price_data) => token_price_data,
                    Err(e) => {
                        handle_db_error(pool, e.clone()).await;
                        return Err(e);
                    }
                };

                let best_ask_token_price: Decimal = match token_price_data.best_ask_decimal() {
                    Ok(best_ask_token_price) => best_ask_token_price,
                    Err(e) => {
                        handle_db_error(pool, e.clone()).await;
                        return Err(e);
                    }
                };

                log::info!("Successfully get token:{} ask price:{}", trade_symbol, best_ask_token_price);

                let token_funds: Decimal = best_ask_token_price * token_liability;

                let min_funds_by_size: Decimal = best_ask_token_price * base_min_size;

                let final_funds: Decimal = token_funds.max(min_funds_by_size).max(min_funds);

                let funds: String = match format_assert_decimal(final_funds, quote_increment) {
                    Ok(funds) => funds,
                    Err(e) => {
                        handle_db_error(pool, e.clone()).await;
                        return Err(e);
                    }
                };

                let client_oid: String = Uuid::new_v4().to_string();

                match make_hf_funds_margin_order(pool, &client_oid, "buy", &trade_symbol, funds, "market", false, false).await {
                    Ok(_) => continue,
                    Err(e) => {
                        handle_db_error(pool, e.clone()).await;
                        return Err(e);
                    }
                }
            }
        } else if account.currency != "USDT" && token_available > Decimal::ZERO {
            passed = false;

            let trade_symbol: String = format!("{}-USDT", account.currency);

            let token_price_data: ApiV1MarketOrderbookLevel1ResData = match get_token_price(pool, &trade_symbol).await {
                Ok(token_price_data) => token_price_data,
                Err(e) => {
                    handle_db_error(pool, e.clone()).await;
                    return Err(e);
                }
            };

            let best_bid_token_price: Decimal = match token_price_data.best_bid_decimal() {
                Ok(best_bid_token_price) => best_bid_token_price,
                Err(e) => {
                    handle_db_error(pool, e.clone()).await;
                    return Err(e);
                }
            };

            log::info!("Successfully get token:{} price:{}", &trade_symbol, best_bid_token_price);

            let token_funds: Decimal = best_bid_token_price * token_available;

            if token_available <= base_min_size || token_funds <= quote_min_size {
                let amount: String = match format_assert_decimal(token_available, precision_decimal) {
                    Ok(size) => size,
                    Err(e) => {
                        handle_db_error(pool, e.clone()).await;
                        return Err(e);
                    }
                };
                match transfer_amount(pool, &account.currency, &amount).await {
                    Ok(_) => continue,
                    Err(e) => {
                        handle_db_error(pool, e.clone()).await;
                        return Err(e);
                    }
                };
            } else {
                let size: String = match format_assert_decimal(token_available, base_increment) {
                    Ok(size) => size,
                    Err(e) => {
                        handle_db_error(pool, e.clone()).await;
                        return Err(e);
                    }
                };
                let client_oid: String = Uuid::new_v4().to_string();

                match make_hf_size_margin_order(pool, &client_oid, "sell", &trade_symbol, size, "market", false, false).await {
                    Ok(_) => continue,
                    Err(e) => {
                        handle_db_error(pool, e.clone()).await;
                        return Err(e);
                    }
                }
            }
        }
    }
    Ok(passed)
}

pub async fn process_bot_by_exit_sl_client_oid(pool: &PgPool, bot: Bot, client_oid: &str, order: &OrderData) -> Result<(), String> {
    match delete_exit_sl_id_bot_by_client_oid(pool, client_oid).await {
        Err(e) => {
            handle_db_error(pool, e.clone()).await;
            return Err(e);
        }
        Ok(_) => {}
    };
    match &bot.exit_tp_client_oid {
        Some(exit_tp_client_oid) => {
            // clear exit_tp_client_oid in bots by entry_id
            match delete_exit_tp_id_bot_by_client_oid(pool, exit_tp_client_oid).await {
                Ok(_) => {}
                Err(e) => {
                    handle_db_error(pool, e.clone()).await;
                    return Err(e);
                }
            }
            let mut query_params: Map<&str, &str, 8> = Map::new();

            query_params.insert("clientOid", exit_tp_client_oid);

            match api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(build_query_string(query_params)).await {
                Ok(_) => {
                    log::info!("Successfully cancel stop order :{}", &exit_tp_client_oid);
                }
                Err(e) => {
                    handle_db_error(pool, e.clone()).await;
                    return Err(e);
                }
            }
        }
        None => {}
    }

    let return_balance_option: Option<String> = match get_total_match_value_by_client_oid(pool, client_oid).await {
        Ok(return_balance_option) => return_balance_option,
        Err(e) => {
            handle_db_error(pool, e.clone()).await;
            return Err(e);
        }
    };

    let return_balance_string: String = match return_balance_option {
        Some(return_balance_string) => return_balance_string,
        None => {
            log::error!("No records found or error occurred");
            return Ok(());
        }
    };

    let return_balance: Decimal = match Decimal::from_str(&return_balance_string) {
        Ok(return_balance) => return_balance,
        Err(e) => {
            let msg: String = format!("Fail parse return balance:{}", e);
            log::error!("{}", msg);
            return Err(msg);
        }
    };

    if order.side == "buy" {
        let old_balance = match bot.balance_decimal() {
            Ok(old_balance) => old_balance,
            Err(e) => {
                handle_db_error(pool, e.clone()).await;
                return Err(e);
            }
        };
        let new_balance = old_balance + old_balance - return_balance;
        match update_balance_bot_by_exit_sl_client_oid(pool, client_oid, &format!("{:.4}", new_balance)).await {
            Ok(_) => {}
            Err(e) => {
                handle_db_error(pool, e.clone()).await;
                return Err(e);
            }
        }
        // create new random order
        match make_random_trade(pool, new_balance, bot.id).await {
            Ok(()) => {}
            Err(e) => {
                handle_db_error(pool, e.clone()).await;
                return Err(e);
            }
        }
    } else if order.side == "sell" {
        match update_balance_bot_by_exit_sl_client_oid(pool, client_oid, &format!("{:.4}", return_balance)).await {
            Ok(_) => {}
            Err(e) => {
                handle_db_error(pool, e.clone()).await;
                return Err(e);
            }
        }

        // create new random order
        match make_random_trade(pool, return_balance, bot.id).await {
            Ok(()) => {}
            Err(e) => {
                handle_db_error(pool, e.clone()).await;
                return Err(e);
            }
        }
    };
    Ok(())
}

pub async fn process_bot_by_exit_tp_client_oid(pool: &PgPool, bot: Bot, client_oid: &str, order: &OrderData) -> Result<(), String> {
    match delete_exit_tp_id_bot_by_client_oid(pool, client_oid).await {
        Ok(_) => {}
        Err(e) => {
            handle_db_error(pool, e.clone()).await;
            return Err(e);
        }
    }
    match &bot.exit_sl_client_oid {
        Some(exit_sl_client_oid) => {
            // clear exit_sl_client_oid in bots by id !!
            match delete_exit_sl_id_bot_by_client_oid(pool, exit_sl_client_oid).await {
                Ok(_) => {}
                Err(e) => {
                    handle_db_error(pool, e).await;
                }
            }
            let mut query_params: Map<&str, &str, 8> = Map::new();

            query_params.insert("clientOid", exit_sl_client_oid);

            match api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(build_query_string(query_params)).await {
                Ok(_) => {
                    log::info!("Successfully cancel stop order :{}", &exit_sl_client_oid)
                }
                Err(e) => {
                    handle_db_error(pool, e).await;
                }
            }
        }
        None => {}
    }
    let return_balance_option: Option<String> = match get_total_match_value_by_client_oid(pool, client_oid).await {
        Ok(return_balance_option) => return_balance_option,
        Err(e) => {
            handle_db_error(pool, e).await;
            return Ok(());
        }
    };

    let return_balance_string: String = match return_balance_option {
        Some(return_balance_string) => return_balance_string,
        None => {
            log::error!("No records found or error occurred");
            return Ok(());
        }
    };

    let return_balance: Decimal = match Decimal::from_str(&return_balance_string) {
        Ok(return_balance) => return_balance,
        Err(e) => {
            let msg: String = format!("Fail parse return balance:{}", e);
            log::error!("{}", msg);
            return Err(msg);
        }
    };

    if order.side == "buy" {
        let old_balance: Decimal = match bot.balance_decimal() {
            Ok(old_balance) => old_balance,
            Err(e) => {
                handle_db_error(pool, e.clone()).await;
                return Err(e);
            }
        };
        let new_balance: Decimal = old_balance + old_balance - return_balance;
        match update_balance_bot_by_exit_tp_client_oid(pool, client_oid, &format!("{:.4}", new_balance)).await {
            Ok(_) => {}
            Err(e) => {
                handle_db_error(pool, e).await;
            }
        }
        // create new random order
        match make_random_trade(pool, new_balance, bot.id).await {
            Ok(()) => {}
            Err(e) => {
                handle_db_error(pool, e).await;
            }
        }
    } else if order.side == "sell" {
        match update_balance_bot_by_exit_tp_client_oid(pool, client_oid, &format!("{:.4}", return_balance)).await {
            Ok(_) => {}
            Err(e) => {
                handle_db_error(pool, e).await;
            }
        }
        // create new random order
        match make_random_trade(pool, return_balance, bot.id).await {
            Ok(()) => {}
            Err(e) => {
                handle_db_error(pool, e).await;
            }
        }
    };
    Ok(())
}

pub async fn process_bot_by_entry_client_oid(pool: &PgPool, client_oid: &str, order: &OrderData) -> Result<(), String> {
    let symbol_info_option: Option<Symbol> = match fetch_symbol_info_by_symbol(pool, &order.symbol).await {
        Ok(symbol_info_option) => symbol_info_option,
        Err(e) => {
            handle_db_error(pool, e.clone()).await;
            return Err(e);
        }
    };

    let symbol_info: Symbol = match symbol_info_option {
        Some(symbol_info) => symbol_info,
        None => {
            let msg: String = format!("Symbol info not found for {}", order.symbol);
            log::error!("{}", msg);
            handle_db_error(pool, msg.clone()).await;
            return Err(msg);
        }
    };

    let price_increment: Decimal = match symbol_info.price_increment_decimal() {
        Ok(price_increment) => price_increment,
        Err(e) => {
            handle_db_error(pool, e.clone()).await;
            return Err(e);
        }
    };

    let quote_increment: Decimal = match symbol_info.quote_increment_decimal() {
        Ok(quote_increment) => quote_increment,
        Err(e) => {
            handle_db_error(pool, e.clone()).await;
            return Err(e);
        }
    };

    let filled_size: Decimal = match order.filled_size_decimal() {
        Ok(filled_size) => filled_size,
        Err(e) => {
            handle_db_error(pool, e.clone()).await;
            return Err(e);
        }
    };

    let return_balance_option: Option<String> = match get_total_match_value_by_client_oid(pool, client_oid).await {
        Ok(return_balance_option) => return_balance_option,
        Err(e) => {
            handle_db_error(pool, e.clone()).await;
            return Err(e);
        }
    };

    let return_balance_string: String = match return_balance_option {
        Some(return_balance_string) => return_balance_string,
        None => {
            log::error!("No records found or error occurred");
            return Ok(());
        }
    };

    let new_balance: Decimal = Decimal::from_str(&return_balance_string).map_err(|e| {
        log::error!("Fail parse return balance:{}", e);
        format!("Fail parse return balance:{}", e)
    })?;

    match update_bot_balance_by_entry_client_oid(pool, client_oid, &format!("{:.4}", new_balance)).await {
        Err(e) => {
            handle_db_error(pool, e.clone()).await;
            return Err(e);
        }
        Ok(_) => {}
    }

    if order.side == "buy" {
        let tp_buy: Decimal = match tp_buy_percent() {
            Ok(tp_buy) => tp_buy,
            Err(e) => {
                log::error!("{}", e);
                return Err(e);
            }
        };

        let sl_buy: Decimal = match sl_buy_percent() {
            Ok(sl_buy) => sl_buy,
            Err(e) => {
                log::error!("{}", e);
                return Err(e);
            }
        };

        let match_price: Decimal = new_balance / filled_size;
        let trigger_tp_price: Decimal = match_price * tp_buy; // price + 7%
        let trigger_sl_price: Decimal = match_price * sl_buy; // price - 5%

        let exit_tp_client_oid: String = Uuid::new_v4().to_string();
        let exit_sl_client_oid: String = Uuid::new_v4().to_string();

        // tp order
        let stop_price_tp: String = match format_assert_decimal(trigger_tp_price, price_increment) {
            Ok(stop_price_tp) => stop_price_tp,
            Err(e) => {
                let msg: String = format!("Fail parse:{} {} error:{}", trigger_tp_price, price_increment, e);
                log::error!("{}", msg);
                handle_db_error(pool, msg.clone()).await;
                return Err(msg);
            }
        };
        let msg_tp_order: serde_json::Value = serde_json::json!({
            "clientOid": exit_tp_client_oid,
            "side": "sell",
            "symbol": order.symbol,
            "type": "market",
            "stop": "entry",
            "stopPrice": stop_price_tp,
            "isIsolated": false,
            "autoBorrow": true,
            "autoRepay": false,
            "size": &order.filled_size,
            "timeInForce": "GTC",
        });
        // sl order
        let stop_price_sl: String = match format_assert_decimal(trigger_sl_price, price_increment) {
            Ok(stop_price_sl) => stop_price_sl,
            Err(e) => {
                let msg: String = format!("Fail parse:{} {} error:{}", trigger_sl_price, price_increment, e);
                log::error!("{}", msg);
                handle_db_error(pool, msg.clone()).await;
                return Err(msg);
            }
        };
        let msg_sl_order: serde_json::Value = serde_json::json!({
            "clientOid": exit_sl_client_oid,
            "side": "sell",
            "symbol": order.symbol,
            "type": "market",
            "stop": "loss",
            "stopPrice": stop_price_sl,
            "isIsolated": false,
            "autoBorrow": true,
            "autoRepay": false,
            "size": order.filled_size,
            "timeInForce": "GTC",
        });

        log::info!("Stop profit order:{}", msg_tp_order);
        log::info!("Stop loss order:{}", msg_sl_order);

        // add exit_tp_client_oid by entry_id
        match update_exit_tp_client_oid_bot_by_entry_client_oid(pool, client_oid, &exit_tp_client_oid).await {
            Ok(_) => {}
            Err(e) => {
                handle_db_error(pool, e.clone()).await;
                return Err(e);
            }
        }
        // add exit_sl_client_oid by entry_id
        match update_exit_sl_client_oid_bot_by_entry_client_oid(pool, client_oid, &exit_sl_client_oid).await {
            Ok(_) => {}
            Err(e) => {
                handle_db_error(pool, e.clone()).await;
                return Err(e);
            }
        }

        let msg_tp_order2: String = match serialize_body(Some(msg_tp_order)) {
            Ok(body_str) => body_str,
            Err(e) => {
                handle_db_error(pool, e.clone()).await;
                return Err(e);
            }
        };
        let tp_fut = api_v3_hf_margin_stop_order_post(msg_tp_order2);

        let msg_sl_order2: String = match serialize_body(Some(msg_sl_order)) {
            Ok(body_str) => body_str,
            Err(e) => {
                handle_db_error(pool, e.clone()).await;
                return Err(e);
            }
        };
        let sl_fut = api_v3_hf_margin_stop_order_post(msg_sl_order2);

        let (tp_res, sl_res) = tokio::join!(tp_fut, sl_fut);

        match (&tp_res, &sl_res) {
            (Ok(tp_resp), Ok(sl_resp)) => {
                match tp_resp {
                    Some(response_data) => match update_exit_tp_order_id_bot_by_exit_tp_client_oid(pool, &response_data.order_id, &response_data.client_oid).await {
                        Ok(_) => {}
                        Err(e) => {
                            handle_db_error(pool, e.clone()).await;
                            return Err(e);
                        }
                    },
                    None => {}
                }

                match sl_resp {
                    Some(response_data) => match update_exit_sl_order_id_bot_by_exit_sl_client_oid(pool, &response_data.order_id, &response_data.client_oid).await {
                        Ok(_) => {}
                        Err(e) => {
                            handle_db_error(pool, e.clone()).await;
                            return Err(e);
                        }
                    },
                    None => {}
                }

                log::info!("✅ Both stop orders created: TP={}, SL={}", exit_tp_client_oid, exit_sl_client_oid);
            }
            (Err(tp_err), Ok(sl_resp)) => {
                match sl_resp {
                    Some(response_data) => {
                        let mut query_params: Map<&str, &str, 8> = Map::new();

                        query_params.insert("clientOid", &response_data.client_oid);

                        match api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(build_query_string(query_params)).await {
                            Ok(_) => {}
                            Err(e) => {
                                handle_db_error(pool, e.clone()).await;
                                return Err(e);
                            }
                        };
                    }
                    None => {}
                }

                match delete_exit_sl_id_bot_by_client_oid(pool, &exit_sl_client_oid).await {
                    Ok(_) => {}
                    Err(e) => {
                        handle_db_error(pool, e.clone()).await;
                        return Err(e);
                    }
                }

                let msg: String = format!("Failed add TP order: {}. SL was cancelled for symmetry.", tp_err);
                log::error!("{}", msg);

                handle_db_error(pool, msg).await;
                {}
            }
            (Ok(tp_resp), Err(sl_err)) => {
                match tp_resp {
                    Some(response_data) => {
                        let mut query_params: Map<&str, &str, 8> = Map::new();

                        query_params.insert("clientOid", &response_data.client_oid);

                        match api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(build_query_string(query_params)).await {
                            Ok(_) => {}
                            Err(e) => {
                                handle_db_error(pool, e.clone()).await;
                                return Err(e);
                            }
                        }
                    }
                    None => {}
                }

                match delete_exit_tp_id_bot_by_client_oid(pool, &exit_tp_client_oid).await {
                    Ok(_) => {}
                    Err(e) => {
                        handle_db_error(pool, e.clone()).await;
                        return Err(e);
                    }
                }

                let msg: String = format!("Failed add SL order: {}. TP was cancelled for symmetry.", sl_err);
                log::error!("{}", msg);

                handle_db_error(pool, msg).await;
                {}
            }
            (Err(tp_err), Err(sl_err)) => {
                let msg: String = format!("Failed add both stop orders: TP={}, SL={}", tp_err, sl_err);
                log::error!("{}", msg);

                handle_db_error(pool, msg).await;
                {}

                match delete_symbol_bot_by_exit_sl_client_oid(pool, &exit_sl_client_oid).await {
                    Ok(_) => {}
                    Err(e) => {
                        handle_db_error(pool, e.clone()).await;
                        return Err(e);
                    }
                }
                match delete_exit_sl_id_bot_by_client_oid(pool, &exit_sl_client_oid).await {
                    Ok(_) => {}
                    Err(e) => {
                        handle_db_error(pool, e.clone()).await;
                        return Err(e);
                    }
                }
                match delete_exit_tp_id_bot_by_client_oid(pool, &exit_tp_client_oid).await {
                    Ok(_) => {}
                    Err(e) => {
                        handle_db_error(pool, e.clone()).await;
                        return Err(e);
                    }
                }
            }
        }
    } else if order.side == "sell" {
        let tp_sell: Decimal = match tp_sell_percent() {
            Ok(tp_sell) => tp_sell,
            Err(e) => {
                log::error!("{}", e);
                return Err(e);
            }
        };

        let sl_sell: Decimal = match sl_sell_percent() {
            Ok(sl_sell) => sl_sell,
            Err(e) => {
                log::error!("{}", e);
                return Err(e);
            }
        };

        let match_price: Decimal = new_balance / filled_size;
        let trigger_tp_price: Decimal = match_price * tp_sell; // price - 7%
        let trigger_sl_price: Decimal = match_price * sl_sell; // price + 5%

        let funds_tp: Decimal = trigger_tp_price * filled_size;
        let funds_sl: Decimal = trigger_sl_price * filled_size;

        let exit_tp_client_oid: String = Uuid::new_v4().to_string();
        let exit_sl_client_oid: String = Uuid::new_v4().to_string();

        let stop_price_tp: String = match format_assert_decimal(trigger_tp_price, price_increment) {
            Ok(stop_price_tp) => stop_price_tp,
            Err(e) => {
                let msg: String = format!("Fail parse:{} {} error:{}", trigger_tp_price, price_increment, e);
                log::error!("{}", msg);
                handle_db_error(pool, msg.clone()).await;
                return Err(msg);
            }
        };
        let funds_tp_str: String = match format_assert_decimal(funds_tp, quote_increment) {
            Ok(funds_tp_str) => funds_tp_str,
            Err(e) => {
                let msg: String = format!("Fail parse:{} {} error:{}", funds_tp, quote_increment, e);
                log::error!("{}", msg);
                handle_db_error(pool, msg.clone()).await;
                return Err(msg);
            }
        };
        let msg_tp_order: serde_json::Value = serde_json::json!({
            "clientOid": exit_tp_client_oid,
            "side": "buy",
            "symbol": order.symbol,
            "type": "market",
            "stop": "loss",
            "stopPrice": stop_price_tp, // price - 7%
            "isIsolated": false,
            "autoBorrow": true,
            "autoRepay": false,
            "timeInForce": "GTC",
            "funds":funds_tp_str,
        });
        let stop_price_sl: String = match format_assert_decimal(trigger_sl_price, price_increment) {
            Ok(stop_price_sl) => stop_price_sl,
            Err(e) => {
                let msg: String = format!("Fail parse:{} {} error:{}", trigger_sl_price, price_increment, e);
                log::error!("{}", msg);
                handle_db_error(pool, msg.clone()).await;
                return Err(msg);
            }
        };
        let funds_sl_str: String = match format_assert_decimal(funds_sl, quote_increment) {
            Ok(funds_sl_str) => funds_sl_str,
            Err(e) => {
                let msg: String = format!("Fail parse:{} {} error:{}", funds_sl, quote_increment, e);
                log::error!("{}", msg);
                handle_db_error(pool, msg.clone()).await;
                return Err(msg);
            }
        };
        let msg_sl_order: serde_json::Value = serde_json::json!({
            "clientOid": exit_sl_client_oid,
            "side": "buy",
            "symbol": order.symbol,
            "type": "market",
            "stop": "entry",
            "stopPrice": stop_price_sl, // price + 5%
            "isIsolated": false,
            "autoBorrow": true,
            "autoRepay": false,
            "timeInForce": "GTC",
            "funds": funds_sl_str,
        });

        log::info!("Stop profit order:{}", msg_tp_order);
        log::info!("Stop loss order:{}", msg_sl_order);

        // add exit_tp_client_oid by entry_id
        match update_exit_tp_client_oid_bot_by_entry_client_oid(pool, client_oid, &exit_tp_client_oid).await {
            Ok(_) => {}
            Err(e) => {
                handle_db_error(pool, e.clone()).await;
                return Err(e);
            }
        }
        // add exit_sl_client_oid by entry_id
        match update_exit_sl_client_oid_bot_by_entry_client_oid(pool, client_oid, &exit_sl_client_oid).await {
            Ok(_) => {}
            Err(e) => {
                handle_db_error(pool, e.clone()).await;
                return Err(e);
            }
        }

        let msg_tp_order2: String = match serialize_body(Some(msg_tp_order)) {
            Ok(body_str) => body_str,
            Err(e) => {
                handle_db_error(pool, e.clone()).await;
                return Err(e);
            }
        };
        let tp_fut = api_v3_hf_margin_stop_order_post(msg_tp_order2);

        let msg_sl_order2: String = match serialize_body(Some(msg_sl_order)) {
            Ok(body_str) => body_str,
            Err(e) => {
                handle_db_error(pool, e.clone()).await;
                return Err(e);
            }
        };
        let sl_fut = api_v3_hf_margin_stop_order_post(msg_sl_order2);
        let (tp_res, sl_res) = tokio::join!(tp_fut, sl_fut);

        match (&tp_res, &sl_res) {
            (Ok(tp_resp), Ok(sl_resp)) => {
                match tp_resp {
                    Some(response_data) => match update_exit_tp_order_id_bot_by_exit_tp_client_oid(pool, &response_data.order_id, &response_data.client_oid).await {
                        Ok(_) => {}
                        Err(e) => {
                            handle_db_error(pool, e.clone()).await;
                            return Err(e);
                        }
                    },
                    None => {}
                }

                match sl_resp {
                    Some(response_data) => match update_exit_sl_order_id_bot_by_exit_sl_client_oid(pool, &response_data.order_id, &response_data.client_oid).await {
                        Ok(_) => {}
                        Err(e) => {
                            handle_db_error(pool, e.clone()).await;
                            return Err(e);
                        }
                    },
                    None => {}
                }

                log::info!("✅ Both stop orders created: TP={}, SL={}", exit_tp_client_oid, exit_sl_client_oid);
            }
            (Err(tp_err), Ok(sl_resp)) => {
                match sl_resp {
                    Some(response_data) => {
                        let mut query_params: Map<&str, &str, 8> = Map::new();

                        query_params.insert("clientOid", &response_data.client_oid);
                        match api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(build_query_string(query_params)).await {
                            Ok(_) => {}
                            Err(e) => {
                                handle_db_error(pool, e.clone()).await;
                                return Err(e);
                            }
                        }
                    }
                    None => {}
                }

                match delete_exit_sl_id_bot_by_client_oid(pool, &exit_sl_client_oid).await {
                    Ok(_) => {}
                    Err(e) => {
                        handle_db_error(pool, e.clone()).await;
                        return Err(e);
                    }
                }

                let msg: String = format!("Failed add TP order: {}. SL was cancelled for symmetry.", tp_err);
                log::error!("{}", msg);
                handle_db_error(pool, msg).await;
            }
            (Ok(tp_resp), Err(sl_err)) => match tp_resp {
                Some(response_data) => {
                    let mut query_params: Map<&str, &str, 8> = Map::new();

                    query_params.insert("clientOid", &response_data.client_oid);
                    match api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(build_query_string(query_params)).await {
                        Ok(_) => {
                            match delete_exit_tp_id_bot_by_client_oid(pool, &exit_tp_client_oid).await {
                                Ok(_) => {}
                                Err(e) => {
                                    handle_db_error(pool, e.clone()).await;
                                    return Err(e);
                                }
                            }
                            let msg: String = format!("Failed add SL order: {}. TP was cancelled for symmetry.", sl_err);
                            log::error!("{}", msg);

                            handle_db_error(pool, msg).await;
                            {}
                        }
                        Err(e) => {
                            handle_db_error(pool, e.clone()).await;
                            return Err(e);
                        }
                    }
                }
                None => {}
            },
            (Err(tp_err), Err(sl_err)) => {
                let msg: String = format!("Failed add both stop orders: TP={}, SL={}", tp_err, sl_err);
                log::error!("{}", msg);

                handle_db_error(pool, msg).await;
                {}

                match delete_symbol_bot_by_exit_sl_client_oid(pool, &exit_sl_client_oid).await {
                    Ok(_) => {}
                    Err(e) => {
                        handle_db_error(pool, e.clone()).await;
                        return Err(e);
                    }
                }
                match delete_exit_sl_id_bot_by_client_oid(pool, &exit_sl_client_oid).await {
                    Ok(_) => {}
                    Err(e) => {
                        handle_db_error(pool, e).await;
                    }
                }
                match delete_exit_tp_id_bot_by_client_oid(pool, &exit_tp_client_oid).await {
                    Ok(_) => {}
                    Err(e) => {
                        handle_db_error(pool, e).await;
                    }
                }
            }
        }
    }

    // delete entry_id from db
    match set_null_entry_client_oid_by_entry_client_oid(pool, client_oid).await {
        Ok(_) => Ok(()),
        Err(e) => {
            handle_db_error(pool, e).await;
            Ok(())
        }
    }
}

pub async fn trade_order_event(pool: &PgPool, order: &OrderData) -> Result<(), String> {
    let client_oid: &String = match &order.client_oid {
        Some(client_oid) => client_oid,
        None => {
            let msg: String = format!("client_oid in order is none: {}", order);
            log::error!("{}", msg);
            handle_db_error(pool, msg.clone()).await;
            return Err(msg);
        }
    };

    let bot_option: Option<Bot> = match get_bot_by_client_oid(pool, client_oid).await {
        Err(e) => {
            handle_db_error(pool, e.clone()).await;
            return Err(e);
        }
        Ok(bot_option) => bot_option,
    };

    let bot: Bot = match bot_option {
        Some(bot) => bot,
        None => {
            let msg: String = format!("Bot is None by:{}", client_oid);
            log::error!("{}", msg);
            return Err(msg);
        }
    };

    match client_oid.as_str() {
        s if Some(s.to_string()) == bot.entry_client_oid => match process_bot_by_entry_client_oid(pool, client_oid, order).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        },
        s if Some(s.to_string()) == bot.exit_tp_client_oid => match process_bot_by_exit_tp_client_oid(pool, bot, client_oid, order).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        },
        s if Some(s.to_string()) == bot.exit_sl_client_oid => match process_bot_by_exit_sl_client_oid(pool, bot, client_oid, order).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        },
        _ => {
            let msg: String = format!("don't find client_oid in:{}", order);
            log::error!("{}", msg);
            Err(msg)
        }
    }
}

pub async fn handle_trade_order_event(order: OrderData, pool: &PgPool) -> Result<(), String> {
    match insert_db_orderevent(pool, order.clone()).await {
        Ok(_) => log::info!("{}", order),
        Err(e) => {
            handle_db_error(pool, e.clone()).await;
            return Err(e);
        }
    }

    if (order.type_ == "match" || order.type_ == "canceled") && (order.remain_size == Some("0".to_string()) || order.remain_funds == Some("0".to_string())) {
        match trade_order_event(pool, &order).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    } else {
        Ok(())
    }
}

pub async fn handle_position_event(position: PositionData, pool: &PgPool) -> Result<(), String> {
    let debt_pair: Vec<(String, Decimal)> = match position.debt_pairs() {
        Err(e) => return Err(e),
        Ok(debt_pair) => debt_pair,
    };

    for (asset, token_liability) in debt_pair {
        let asset_info: &AssetInfo = match position.asset_list.get(&asset) {
            None => {
                let msg: String = format!("Failed get asset:{} from:{:.?}", asset, position.asset_list);
                log::error!("{}", msg);
                handle_db_error(pool, msg).await;
                continue;
            }
            Some(asset_info) => asset_info,
        };

        let token_available: Decimal = match asset_info.available_decimal() {
            Err(e) => {
                handle_db_error(pool, e).await;
                continue;
            }
            Ok(available) => available,
        };

        if token_liability > Decimal::ZERO && token_available > Decimal::ZERO {
            let currency_info_option: Option<Currencies> = match fetch_currency_info_by_symbol(pool, &asset).await {
                Ok(info) => info,
                Err(e) => {
                    handle_db_error(pool, e.clone()).await;
                    return Err(e);
                }
            };

            let currency_info: Currencies = match currency_info_option {
                Some(info) => info,
                None => {
                    let msg: String = format!("Currency info not found for {}", asset);
                    log::error!("{}", msg);
                    handle_db_error(pool, msg.clone()).await;
                    return Err(msg);
                }
            };

            let precision_decimal: Decimal = match currency_info.precision_decimal() {
                Ok(precision_decimal) => precision_decimal,
                Err(e) => {
                    handle_db_error(pool, e.clone()).await;
                    return Err(e);
                }
            };

            let size: String = match format_assert_decimal(token_liability.min(token_available), precision_decimal) {
                Ok(size) => size,
                Err(e) => {
                    handle_db_error(pool, e.clone()).await;
                    return Err(e);
                }
            };

            match repay_account(pool, &asset, &size).await {
                Ok(_) => continue,
                Err(e) => {
                    handle_db_error(pool, e).await;
                    continue;
                }
            }
        }
    }

    match upsert_position_ratio(pool, position.debt_ratio, position.total_asset, &position.margin_coefficient_total_asset, &position.total_debt).await {
        Ok(_) => {}
        Err(e) => {
            handle_db_error(pool, e.clone()).await;
            return Err(e);
        }
    };

    for (symbol, amount) in &position.debt_list {
        match upsert_position_debt(pool, symbol, amount).await {
            Ok(_) => {}
            Err(e) => {
                handle_db_error(pool, e).await;
                continue;
            }
        }
    }
    for (symbol, symbol_info) in &position.asset_list {
        match upsert_position_asset(pool, symbol, &symbol_info.total, &symbol_info.available, &symbol_info.hold).await {
            Ok(_) => {}
            Err(e) => {
                handle_db_error(pool, e).await;
                continue;
            }
        }
    }

    Ok(())
}

pub async fn handle_advanced_orders(order: AdvancedOrders, pool: &PgPool) -> Result<(), String> {
    log::info!("{}", order);
    match order.error {
        Some(_) => {}
        None => return Ok(()),
    }

    let msg: String = format!("Got error on stop order : {}", order);
    log::error!("{}", msg);

    handle_db_error(pool, msg).await;

    const MAX_RETRIES: u32 = 1000;
    let mut attempt: u32 = 0;

    loop {
        sleep(Duration::from_millis(RETRY_DELAY_BASE * attempt as u64)).await;
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
                match update_exit_sl_client_oid_bot_by_exit_sl_order_id(pool, order_id_ref, &new_exit_client_oid).await {
                    Ok(_) => match side_ref.as_str() {
                        "buy" => match funds_clone {
                            Some(funds) => make_hf_funds_margin_order(pool, &new_exit_client_oid, side_ref, symbol_ref, funds, "market", true, false).await,
                            None => {
                                let _ = handle_db_error(pool, format!("Fail parse funds order:{} new_exit_sl_client_oid:{} funds_clone:{:.?}", order_id_ref, new_exit_client_oid, funds_clone,)).await;
                                continue;
                            }
                        },
                        "sell" => match size_clone {
                            Some(size) => make_hf_size_margin_order(pool, &new_exit_client_oid, side_ref, symbol_ref, size, "market", true, false).await,
                            None => {
                                let msg: String = format!("Fail parse size order:{} new_exit_sl_client_oid:{} size_clone:{:.?}", order_id_ref, new_exit_client_oid, size_clone,);
                                log::error!("{}", msg);

                                handle_db_error(pool, msg).await;

                                continue;
                            }
                        },
                        _ => {
                            let msg: String = format!("Fail match side_clone:{}", side_ref);
                            log::error!("{}", msg);

                            handle_db_error(pool, msg).await;
                            continue;
                        }
                    },
                    Err(e) => {
                        handle_db_error(pool, e).await;
                        continue;
                    }
                }
            }
            "entry" => {
                // need find tp
                match update_exit_tp_client_oid_bot_by_exit_tp_order_id(pool, order_id_ref, &new_exit_client_oid).await {
                    Ok(_) => match side_ref.as_str() {
                        "buy" => match funds_clone {
                            Some(funds) => make_hf_funds_margin_order(pool, &new_exit_client_oid, side_ref, symbol_ref, funds, "market", true, false).await,
                            None => {
                                let msg: String = format!("Fail parse funds_clone order:{} new_exit_tp_client_oid:{} funds_clone:{:.?}", order_id_ref, new_exit_client_oid, funds_clone);
                                log::error!("{}", msg);

                                handle_db_error(pool, msg).await;
                                continue;
                            }
                        },
                        "sell" => match size_clone {
                            Some(size) => make_hf_size_margin_order(pool, &new_exit_client_oid, side_ref, symbol_ref, size, "market", true, false).await,
                            None => {
                                let msg: String = format!("Fail parse size_clone order:{} new_exit_tp_client_oid:{} size_clone:{:.?}", order_id_ref, new_exit_client_oid, size_clone);
                                log::error!("{}", msg);

                                handle_db_error(pool, msg).await;
                                continue;
                            }
                        },
                        _ => {
                            let msg: String = format!("Fail match side_clone:{}", side_ref);
                            log::error!("{}", msg);

                            handle_db_error(pool, msg).await;
                            continue;
                        }
                    },
                    Err(e) => {
                        handle_db_error(pool, e).await;
                        continue;
                    }
                }
            }
            _ => {
                let msg: String = format!("Fail match stop_clone:{}", stop_ref);
                log::error!("{}", msg);

                handle_db_error(pool, msg).await;
                continue;
            }
        };

        match order_result {
            Ok(_) => {
                log::info!("✅ Order re-placed: {} {} (attempt {}/{})", order_id_ref, new_exit_client_oid, attempt, MAX_RETRIES);
                break Ok(());
            }
            Err(e) => {
                let msg: String = format!("❌ Order failed: {} {} (attempt {}/{}) {}", order_id_ref, new_exit_client_oid, attempt, MAX_RETRIES, e);
                log::error!("{}", msg);

                handle_db_error(pool, msg.clone()).await;
                return Err(msg);
            }
        }
    }
}

pub async fn process_kcn_msg(pool: &PgPool, msg: &str) -> Result<(), String> {
    let event: KuCoinMessage = match serde_json::from_str::<KuCoinMessage>(msg) {
        Err(e) => {
            let msg: String = format!("Failed to parse message:{} {}", msg, e);
            log::error!("{}", msg);

            handle_db_error(pool, msg.clone()).await;
            return Err(msg);
        }
        Ok(event) => event,
    };

    let data: MessageData = match event {
        KuCoinMessage::Welcome(data) => match serde_json::to_value(&data) {
            Ok(data) => match insert_db_event(pool, &data).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    handle_db_error(pool, e.clone()).await;
                    return Err(e);
                }
            },
            Err(e) => {
                let msg: String = format!("Failed to serialize request '{:?}' as {}: {}", &data, stringify!(WelcomeData), e);
                log::error!("{}", msg);
                handle_db_error(pool, msg.clone()).await;
                return Err(msg);
            }
        },
        KuCoinMessage::Message(data) => data,
        KuCoinMessage::Ack(data) => match serde_json::to_value(&data) {
            Ok(data) => match insert_db_event(pool, &data).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    handle_db_error(pool, e.clone()).await;
                    return Err(e);
                }
            },
            Err(e) => {
                let msg: String = format!("Failed to serialize request '{:?}' as {}: {}", &data, stringify!(AckData), e);
                log::error!("{}", msg);
                handle_db_error(pool, msg.clone()).await;
                return Err(msg);
            }
        },
        KuCoinMessage::Error(data) => {
            let msg: String = format!("Got error in WS {:?}", data);
            log::error!("{}", msg);

            handle_db_error(pool, msg.clone()).await;
            return Err(msg);
        }

        KuCoinMessage::Unknown => {
            let msg: String = String::from("Unknown WS message type");
            log::error!("{}", msg);

            handle_db_error(pool, msg.clone()).await;
            return Err(msg);
        }
    };

    match data.topic.as_str() {
        "/account/balance" => match BalanceData::deserialize(&data.data) {
            Err(e) => {
                let msg: String = format!("Failed to serialize request '{:?}' as {}: {}", &data.data, stringify!(BalanceData), e);
                log::error!("{}", msg);
                handle_db_error(pool, msg.clone()).await;
                return Err(msg);
            }
            Ok(balance) => match insert_db_balance(pool, balance).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    handle_db_error(pool, e.clone()).await;
                    return Err(e);
                }
            },
        },
        "/spotMarket/tradeOrdersV2" => match OrderData::deserialize(&data.data) {
            Err(e) => {
                let msg: String = format!("Failed to serialize request '{:?}' as {}: {}", &data.data, stringify!(OrderData), e);
                log::error!("{}", msg);
                handle_db_error(pool, msg.clone()).await;
                return Err(msg);
            }
            Ok(order) => match handle_trade_order_event(order, pool).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    handle_db_error(pool, e.clone()).await;
                    return Err(e);
                }
            },
        },
        "/spotMarket/advancedOrders" => match AdvancedOrders::deserialize(&data.data) {
            Err(e) => {
                let msg: String = format!("Failed to serialize request '{:?}' as {}: {}", &data.data, stringify!(AdvancedOrders), e);
                log::error!("{}", msg);
                handle_db_error(pool, msg.clone()).await;
                return Err(msg);
            }
            Ok(order) => match handle_advanced_orders(order, pool).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    handle_db_error(pool, e.clone()).await;
                    return Err(e);
                }
            },
        },
        "/margin/position" => match PositionData::deserialize(&data.data) {
            Err(e) => {
                let msg: String = format!("Failed to serialize request '{:?}' as {}: {}", &data.data, stringify!(PositionData), e);
                log::error!("{}", msg);
                handle_db_error(&pool, msg.clone()).await;
                return Err(msg);
            }
            Ok(position) => match handle_position_event(position, pool).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    handle_db_error(pool, e.clone()).await;
                    return Err(e);
                }
            },
        },
        _ => {
            let msg: String = format!("Unknown topic: {}", data.topic);
            log::error!("{}", msg);

            handle_db_error(pool, msg.clone()).await;
            return Err(msg);
        }
    }
}

pub async fn make_random_trade(pool: &PgPool, balance_funds: Decimal, trade_bot_id: i32) -> Result<(), String> {
    const MAX_RETRIES: u32 = 10;
    let mut attempt: u32 = 0;

    loop {
        if attempt >= MAX_RETRIES {
            return Ok(());
        }
        sleep(Duration::from_millis(RETRY_DELAY_BASE * attempt as u64)).await;
        attempt += 1;

        let tradeable_symbol_option: Option<String> = match get_random_symbol(pool).await {
            Ok(tradeable_symbol_option) => tradeable_symbol_option,
            Err(e) => {
                handle_db_error(pool, e).await;
                continue;
            }
        };

        let tradeable_symbol: String = match tradeable_symbol_option {
            Some(tradeable_symbol) => tradeable_symbol,
            None => {
                let msg: String = String::from("Failed get_random_symbol:");
                log::error!("{}", msg);

                handle_db_error(pool, msg).await;
                continue;
            }
        };

        let symbol_info_option: Option<Symbol> = match fetch_symbol_info_by_symbol(pool, &tradeable_symbol).await {
            Ok(symbol_info_option) => symbol_info_option,
            Err(e) => {
                handle_db_error(pool, e).await;
                continue;
            }
        };

        let symbol_info: Symbol = match symbol_info_option {
            Some(symbol_info) => symbol_info,
            None => {
                let msg: String = format!("Symbol info not found for {}", tradeable_symbol);
                log::error!("{}", msg);

                handle_db_error(pool, msg).await;
                continue;
            }
        };

        let entry_client_oid: String = Uuid::new_v4().to_string();

        match update_bot_entry_client_oid_by_id(pool, Some(&tradeable_symbol), Some(&entry_client_oid), trade_bot_id).await {
            Ok(_) => {}
            Err(e) => {
                handle_db_error(pool, e).await;
                continue;
            }
        }

        let order_result = match get_random_side() {
            "sell" => {
                let base_increment: Decimal = match symbol_info.base_increment_decimal() {
                    Ok(base_increment) => base_increment,
                    Err(e) => {
                        handle_db_error(pool, e).await;
                        continue;
                    }
                };

                let mut query_params: Map<&str, &str, 8> = Map::new();

                query_params.insert("symbol", &tradeable_symbol);

                let token_price_obj = match api_v1_market_orderbook_level1_get(build_query_string(query_params)).await {
                    Ok(token_price_obj) => token_price_obj,
                    Err(e) => {
                        handle_db_error(pool, e).await;
                        continue;
                    }
                };

                let token_price_obj2 = match token_price_obj {
                    Some(token_price_obj2) => token_price_obj2,
                    None => return Err("".to_string()),
                };

                let token_price: Decimal = match token_price_obj2.price_decimal() {
                    Ok(token_price) => token_price,
                    Err(e) => {
                        handle_db_error(pool, e).await;
                        continue;
                    }
                };
                let token_size: Decimal = balance_funds / token_price;
                let size: String = match format_assert_decimal(token_size, base_increment) {
                    Ok(size) => size,
                    Err(e) => {
                        let msg: String = format!("Fail parse:{} {} error:{}", token_size, base_increment, e);
                        log::error!("{}", msg);
                        handle_db_error(pool, msg.clone()).await;
                        return Err(msg);
                    }
                };
                make_hf_size_margin_order(pool, &entry_client_oid, "sell", &tradeable_symbol, size, "market", true, false).await
            }
            "buy" => {
                let quote_increment = match symbol_info.quote_increment_decimal() {
                    Ok(quote_increment) => quote_increment,
                    Err(e) => {
                        handle_db_error(pool, e).await;
                        continue;
                    }
                };
                let funds: String = match format_assert_decimal(balance_funds, quote_increment) {
                    Ok(funds) => funds,
                    Err(e) => {
                        let msg: String = format!("Fail parse:{} {} error:{}", balance_funds, quote_increment, e);
                        log::error!("{}", msg);
                        handle_db_error(pool, msg.clone()).await;
                        return Err(msg);
                    }
                };
                make_hf_funds_margin_order(pool, &entry_client_oid, "buy", &tradeable_symbol, funds, "market", true, false).await
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
                match update_bot_entry_client_oid_by_id(pool, None, None, trade_bot_id).await {
                    Ok(_) => {}
                    Err(e) => {
                        handle_db_error(pool, e.clone()).await;
                        return Err(e);
                    }
                }

                let msg: String = format!("❌ Order failed (attempt {}/{}): {} {}", attempt, MAX_RETRIES, tradeable_symbol, e);
                log::error!("{}", msg);

                handle_db_error(pool, msg).await;
                continue;
            }
        }
    }
}

pub async fn spawn_process_kcn_msg(pool: &PgPool, mut rx_in: tokio::sync::mpsc::Receiver<String>) {
    loop {
        match rx_in.recv().await {
            Some(msg) => {
                match process_kcn_msg(pool, &msg).await {
                    Ok(_) => {}
                    Err(e) => {
                        handle_db_error(pool, e).await;
                        ()
                    }
                };
            }
            None => {
                let msg: String = String::from("Channel closed, exiting message processor");
                log::error!("{}", msg);
                handle_db_error(pool, msg).await;
                break;
            }
        }
    }
    log::info!("Message processor stopped");
}

pub async fn make_hf_funds_margin_order(
    pool: &PgPool,
    client_oid: &str,
    side: &str,
    symbol: &str,
    funds: String,
    type_: &'static str,
    auto_borrow: bool,
    auto_repay: bool,
) -> Result<MakeOrderResData, String> {
    // only for buy orders
    let args_time_in_force: &str = "GTC";

    match insert_db_msgsend(pool, Some(symbol), Some(side), None, Some(&funds), None, Some(args_time_in_force), Some(type_), Some(&auto_borrow), Some(&auto_repay), Some(client_oid), None).await {
        Ok(_) => {}
        Err(e) => {
            handle_db_error(pool, e.clone()).await;
            return Err(e);
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
        Err(e) => {
            handle_db_error(pool, e.clone()).await;
            return Err(e);
        }
    };
    match api_v3_hf_margin_order_post(body_str).await {
        Ok(data) => match data {
            Some(data) => Ok(data),
            None => Err("".to_string()),
        },
        Err(e) => {
            handle_db_error(pool, e.clone()).await;
            return Err(e);
        }
    }
}

pub async fn make_hf_size_margin_order(
    pool: &PgPool,
    client_oid: &str,
    side: &str,
    symbol: &str,
    size: String,
    type_: &'static str,
    auto_borrow: bool,
    auto_repay: bool,
) -> Result<MakeOrderResData, String> {
    // only for sell orders
    let args_time_in_force: &str = "GTC";

    match insert_db_msgsend(pool, Some(symbol), Some(side), Some(&size), None, None, Some(args_time_in_force), Some(type_), Some(&auto_borrow), Some(&auto_repay), Some(client_oid), None).await {
        Ok(_) => {}
        Err(e) => {
            handle_db_error(pool, e.clone()).await;
            return Err(e);
        }
    };

    let body_str: String = match serialize_body(Some(serde_json::json!({
        "clientOid": client_oid,
        "symbol": symbol,
        "side": side,
        "type": type_,
        "autoBorrow": auto_borrow,
        "autoRepay": auto_repay,
        "timeInForce": args_time_in_force,
        "size": size
    }))) {
        Ok(body_str) => body_str,
        Err(e) => {
            handle_db_error(pool, e.clone()).await;
            return Err(e);
        }
    };
    match api_v3_hf_margin_order_post(body_str).await {
        Ok(data) => match data {
            Some(data) => Ok(data),
            None => Err("".to_string()),
        },
        Err(e) => {
            handle_db_error(pool, e.clone()).await;
            return Err(e);
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
        let inc_1000: Decimal = Decimal::from_str("1000").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("1234.56").unwrap(), inc_1000).unwrap(), "1000".to_string());

        // Increment = 100 (precision 0)
        let inc_100: Decimal = Decimal::from_str("100").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("123.456").unwrap(), inc_100).unwrap(), "100".to_string());
        assert_eq!(format_assert_decimal(Decimal::from_str("199").unwrap(), inc_100).unwrap(), "100".to_string());
        assert_eq!(format_assert_decimal(Decimal::from_str("200").unwrap(), inc_100).unwrap(), "200".to_string());

        // Increment = 50 (precision 0)
        let inc_50: Decimal = Decimal::from_str("50").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("123.456").unwrap(), inc_50).unwrap(), "100".to_string());
        assert_eq!(format_assert_decimal(Decimal::from_str("149").unwrap(), inc_50).unwrap(), "100".to_string());
        assert_eq!(format_assert_decimal(Decimal::from_str("150").unwrap(), inc_50).unwrap(), "150".to_string());

        // Increment = 10 (precision 0)
        let inc_10: Decimal = Decimal::from_str("10").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("123.456").unwrap(), inc_10).unwrap(), "120".to_string());
        assert_eq!(format_assert_decimal(Decimal::from_str("125").unwrap(), inc_10).unwrap(), "120".to_string());

        // Increment = 1 (precision 0)
        let inc_1: Decimal = Decimal::from_str("1").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("123.456").unwrap(), inc_1).unwrap(), "123".to_string());
        assert_eq!(format_assert_decimal(Decimal::from_str("100").unwrap(), inc_1).unwrap(), "100".to_string());

        // Increment = 0.1 (precision 1)
        let inc_1: Decimal = Decimal::from_str("0.1").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("123.456").unwrap(), inc_1).unwrap(), "123.4".to_string());
        assert_eq!(format_assert_decimal(Decimal::from_str("99.999").unwrap(), inc_1).unwrap(), "99.9".to_string());

        // Increment = 0.01 (precision 2)
        let inc_2: Decimal = Decimal::from_str("0.01").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("123.456").unwrap(), inc_2).unwrap(), "123.45".to_string());
        assert_eq!(format_assert_decimal(Decimal::from_str("99.999").unwrap(), inc_2).unwrap(), "99.99".to_string());

        // Increment = 0.001 (precision 3)
        let inc_3: Decimal = Decimal::from_str("0.001").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("123.4567").unwrap(), inc_3).unwrap(), "123.456".to_string());

        // Increment = 0.0001 (precision 4)
        let inc_4: Decimal = Decimal::from_str("0.0001").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("123.45678").unwrap(), inc_4).unwrap(), "123.4567".to_string());

        // Increment = 0.0001 (precision 5)
        let inc_5: Decimal = Decimal::from_str("0.00001").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("123.45678").unwrap(), inc_5).unwrap(), "123.45678".to_string());

        // Increment = 0.000001 (precision 6)
        let inc_6: Decimal = Decimal::from_str("0.000001").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("123.456789").unwrap(), inc_6).unwrap(), "123.456789".to_string());

        // Increment = 0.0000001 (precision 7)
        let inc_7: Decimal = Decimal::from_str("0.0000001").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("123.4567891").unwrap(), inc_7).unwrap(), "123.4567891".to_string());

        // Increment = 0.00000001 (precision 8)
        let inc_8: Decimal = Decimal::from_str("0.00000001").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("0.123456789").unwrap(), inc_8).unwrap(), "0.12345678".to_string());

        // Increment = 0.000000001 (precision 9)
        let inc_9: Decimal = Decimal::from_str("0.000000001").unwrap();
        assert_eq!(format_assert_decimal(Decimal::from_str("0.00000000123").unwrap(), inc_9).unwrap(), "0.000000001".to_string());
    }
}
