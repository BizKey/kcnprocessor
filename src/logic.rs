use crate::api::db::{
    delete_exit_sl_id_bot_by_client_oid, delete_exit_tp_id_bot_by_client_oid, delete_symbol_bot_by_exit_sl_client_oid, fetch_symbol_info_by_symbol, get_all_bots_for_trade,
    get_bot_by_entry_client_oid, get_bot_by_exit_sl_client_oid, get_bot_by_exit_tp_client_oid, get_random_symbol, get_total_match_value_by_client_oid, handle_db_error, insert_db_balance,
    insert_db_event, insert_db_msgsend, insert_db_orderevent, set_null_entry_client_oid_by_entry_client_oid, update_balance_bot_by_exit_sl_client_oid, update_balance_bot_by_exit_tp_client_oid,
    update_bot_balance_by_entry_client_oid, update_bot_entry_client_oid_by_id, update_exit_sl_client_oid_bot_by_entry_client_oid, update_exit_sl_client_oid_bot_by_exit_sl_order_id,
    update_exit_sl_order_id_bot_by_exit_sl_client_oid, update_exit_tp_client_oid_bot_by_entry_client_oid, update_exit_tp_client_oid_bot_by_exit_tp_order_id,
    update_exit_tp_order_id_bot_by_exit_tp_client_oid, upsert_position_asset, upsert_position_debt, upsert_position_ratio,
};
use crate::api::models::{AdvancedOrders, BalanceData, Bot, KuCoinMessage, MakeOrderResData, MarginAccountData, OrderData, PositionData, Symbol};
use crate::api::requests::{
    api_v1_market_orderbook_level1_get, api_v3_accounts_universal_transfer_post, api_v3_hf_margin_order_post, api_v3_hf_margin_stop_order_cancel_by_client_oid_delete,
    api_v3_hf_margin_stop_order_post, api_v3_margin_accounts_get, api_v3_margin_repay_post, build_query_string, serialize_body,
};
use micromap::Map;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::json;
use std::num::ParseIntError;
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

pub fn format_assert_decimal(size: Decimal, increment: Decimal) -> Result<String, ParseIntError> {
    let precision = increment.scale() as usize;

    if precision == 0 {
        // Целый increment - округляем вниз до ближайшего кратного
        let increment_int: i64 = match increment.to_string().parse() {
            Ok(increment_int) => increment_int,
            Err(e) => return Err(e),
        };

        let size_int: i64 = match size.to_string().parse() {
            Ok(size_int) => size_int,
            Err(e) => return Err(e),
        };

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

pub async fn create_init_orders(pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str) -> Result<(), String> {
    let trade_bots: Vec<Bot> = match get_all_bots_for_trade(pool, exchange).await {
        Ok(trade_bots) => trade_bots,
        Err(e) => match handle_db_error(pool, exchange, e).await {
            Ok(error_msg) => return Err(error_msg),
            Err(error_msg) => return Err(error_msg),
        },
    };

    for trade_bot in trade_bots.iter() {
        sleep(BOT_INIT_DELAY).await;
        let token_funds = match trade_bot.balance_decimal() {
            Ok(token_funds) => token_funds,
            Err(e) => match handle_db_error(pool, exchange, e).await {
                Ok(error_msg) => return Err(error_msg),
                Err(error_msg) => return Err(error_msg),
            },
        };
        match make_random_trade(pool, exchange, token_funds, trade_bot.id).await {
            Ok(_) => {}
            Err(e) => match handle_db_error(pool, exchange, e).await {
                Ok(_) => continue,
                Err(_) => continue,
            },
        }
    }
    log::info!("All bots initialized!");
    Ok(())
}

pub async fn get_all_accounts_data() -> Result<MarginAccountData, String> {
    let mut query_params: Map<&str, &str, 8> = Map::new();
    query_params.insert("quoteCurrency", "USDT");
    query_params.insert("queryType", "MARGIN");

    match api_v3_margin_accounts_get(build_query_string(query_params)).await {
        Ok(accounts) => Ok(accounts),
        Err(e) => Err(e),
    }
}

pub async fn auto_clean_account(pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str) -> Result<bool, String> {
    sleep(AUTO_CLEAN_DELAY).await;

    let accounts: MarginAccountData = match get_all_accounts_data().await {
        Ok(accounts) => accounts,
        Err(e) => match handle_db_error(pool, exchange, e).await {
            Ok(_) => return Ok(false),
            Err(_) => return Ok(false),
        },
    };

    let mut passed: bool = true;
    for account in accounts.accounts.iter() {
        let token_liability: Decimal = match account.liability_decimal() {
            Ok(token_liability) => token_liability,
            Err(e) => match handle_db_error(pool, exchange, e).await {
                Ok(_) => {
                    passed = false;
                    continue;
                }
                Err(_) => {
                    passed = false;
                    continue;
                }
            },
        };
        let token_available: Decimal = match account.available_decimal() {
            Ok(token_liability) => token_liability,
            Err(e) => match handle_db_error(pool, exchange, e).await {
                Ok(_) => {
                    passed = false;
                    continue;
                }
                Err(_) => {
                    passed = false;
                    continue;
                }
            },
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
                    Err(e) => return Err(e),
                };

                match api_v3_margin_repay_post(body_str).await {
                    Ok(_) => {}
                    Err(e) => match handle_db_error(pool, exchange, e).await {
                        Ok(_) => continue,
                        Err(_) => continue,
                    },
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
                    Err(e) => return Err(e),
                };

                match api_v3_margin_repay_post(body_str).await {
                    Ok(_) => {}
                    Err(e) => match handle_db_error(pool, exchange, e).await {
                        Ok(_) => continue,
                        Err(_) => continue,
                    },
                }
            } else if account.currency != "USDT" && token_available == Decimal::ZERO {
                // buy stock by market liability
                let trade_symbol: String = format!("{}-USDT", account.currency);
                let client_oid: String = Uuid::new_v4().to_string();
                let symbol_info: Symbol = match fetch_symbol_info_by_symbol(pool, exchange, &trade_symbol).await {
                    Ok(Some(info)) => info,
                    Ok(None) => {
                        let msg: String = format!("Symbol info not found for {}", trade_symbol);
                        log::error!("{}", msg);

                        match handle_db_error(pool, exchange, msg).await {
                            Ok(_) => continue,
                            Err(_) => continue,
                        }
                    }
                    Err(e) => match handle_db_error(pool, exchange, e).await {
                        Ok(_) => continue,
                        Err(_) => continue,
                    },
                };
                // liability debt in tokens
                // get price token

                let mut query_params: Map<&str, &str, 8> = Map::new();

                query_params.insert("symbol", &trade_symbol);

                let token_price_obj = match api_v1_market_orderbook_level1_get(build_query_string(query_params)).await {
                    Ok(token_price_obj) => token_price_obj,
                    Err(e) => match handle_db_error(pool, exchange, e).await {
                        Ok(_) => continue,
                        Err(_) => continue,
                    },
                };

                let token_price_obj2 = match token_price_obj {
                    Some(token_price_obj2) => token_price_obj2,
                    None => return Err("".to_string()),
                };

                let token_price: Decimal = match token_price_obj2.price_decimal() {
                    Ok(token_price) => token_price,
                    Err(e) => match handle_db_error(pool, exchange, e).await {
                        Ok(_) => continue,
                        Err(_) => continue,
                    },
                };

                log::info!("Successfully get token:{} price:{}", &trade_symbol, token_price);

                // calc price token on amount liability token
                let token_funds: Decimal = token_price * token_liability;

                let quote_increment: Decimal = match symbol_info.quote_increment_decimal() {
                    Ok(quote_increment) => quote_increment,
                    Err(e) => match handle_db_error(pool, exchange, e).await {
                        Ok(_) => continue,
                        Err(_) => continue,
                    },
                };
                // parse min_funds to int
                let min_funds: Decimal = match symbol_info.min_funds_decimal() {
                    Ok(min_funds) => min_funds,
                    Err(e) => match handle_db_error(pool, exchange, e).await {
                        Ok(_) => continue,
                        Err(_) => continue,
                    },
                };
                let base_min_size: Decimal = match symbol_info.base_min_size_decimal() {
                    Ok(base_min_size) => base_min_size,
                    Err(e) => match handle_db_error(pool, exchange, e).await {
                        Ok(_) => continue,
                        Err(_) => continue,
                    },
                };

                // calc price token on amount base_min_size token
                let min_funds_by_size: Decimal = token_price * base_min_size;

                if token_funds <= min_funds.max(min_funds_by_size) {
                    let funds: String = match format_assert_decimal(min_funds.max(min_funds_by_size), quote_increment) {
                        Ok(funds) => funds,
                        Err(e) => {
                            let msg: String = format!("Fail parse:{} {} error:{}", min_funds.max(min_funds_by_size), quote_increment, e);
                            log::error!("{}", msg);
                            match handle_db_error(pool, exchange, msg).await {
                                Ok(error_msg) => return Err(error_msg),
                                Err(error_msg) => return Err(error_msg),
                            };
                        }
                    };
                    match make_hf_funds_margin_order(pool, exchange, &client_oid, "buy", &trade_symbol, funds, "market", false, false).await {
                        Ok(_) => {}
                        Err(e) => match handle_db_error(pool, exchange, e).await {
                            Ok(_) => continue,
                            Err(_) => continue,
                        },
                    }
                } else {
                    let funds: String = match format_assert_decimal(token_funds, quote_increment) {
                        Ok(funds) => funds,
                        Err(e) => {
                            let msg: String = format!("Fail parse:{} {} error:{}", token_funds, quote_increment, e);
                            log::error!("{}", msg);
                            match handle_db_error(pool, exchange, msg).await {
                                Ok(error_msg) => return Err(error_msg),
                                Err(error_msg) => return Err(error_msg),
                            };
                        }
                    };
                    match make_hf_funds_margin_order(pool, exchange, &client_oid, "buy", &trade_symbol, funds, "market", false, false).await {
                        Ok(_) => {}
                        Err(e) => match handle_db_error(pool, exchange, e).await {
                            Ok(_) => continue,
                            Err(_) => continue,
                        },
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
                    let msg: String = format!("Symbol info not found for {}", trade_symbol);
                    log::error!("{}", msg);

                    match handle_db_error(pool, exchange, msg).await {
                        Ok(_) => continue,
                        Err(_) => continue,
                    }
                }
                Err(e) => match handle_db_error(pool, exchange, e).await {
                    Ok(_) => continue,
                    Err(_) => continue,
                },
            };

            let base_increment: Decimal = match symbol_info.base_increment_decimal() {
                Ok(base_increment) => base_increment,
                Err(e) => match handle_db_error(pool, exchange, e).await {
                    Ok(_) => continue,
                    Err(_) => continue,
                },
            };
            // get price token
            let mut query_params: Map<&str, &str, 8> = Map::new();

            query_params.insert("symbol", &trade_symbol);

            let token_price_obj = match api_v1_market_orderbook_level1_get(build_query_string(query_params)).await {
                Ok(token_price_obj) => token_price_obj,
                Err(e) => match handle_db_error(pool, exchange, e).await {
                    Ok(_) => continue,
                    Err(_) => continue,
                },
            };

            let token_price_obj2 = match token_price_obj {
                Some(token_price_obj2) => token_price_obj2,
                None => return Err("".to_string()),
            };

            let token_price: Decimal = match token_price_obj2.price_decimal() {
                Ok(token_price) => token_price,
                Err(e) => match handle_db_error(pool, exchange, e).await {
                    Ok(_) => continue,
                    Err(_) => continue,
                },
            };

            log::info!("Successfully get token:{} price:{}", &trade_symbol, token_price);

            let base_min_size: Decimal = match symbol_info.base_min_size_decimal() {
                Ok(base_min_size) => base_min_size,
                Err(e) => match handle_db_error(pool, exchange, e).await {
                    Ok(_) => continue,
                    Err(_) => continue,
                },
            };

            let quote_min_size: Decimal = match symbol_info.quote_min_size_decimal() {
                Ok(quote_min_size) => quote_min_size,
                Err(e) => match handle_db_error(pool, exchange, e).await {
                    Ok(_) => continue,
                    Err(_) => continue,
                },
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
                    Err(e) => return Err(e),
                };

                match api_v3_accounts_universal_transfer_post(body_str).await {
                    Ok(_) => {}
                    Err(e) => match handle_db_error(pool, exchange, e).await {
                        Ok(_) => continue,
                        Err(_) => continue,
                    },
                }
            } else {
                let size: String = match format_assert_decimal(token_available, base_increment) {
                    Ok(size) => size,
                    Err(e) => {
                        let msg: String = format!("Fail parse:{} {} error:{}", token_available, base_increment, e);
                        log::error!("{}", msg);
                        match handle_db_error(pool, exchange, msg).await {
                            Ok(error_msg) => return Err(error_msg),
                            Err(error_msg) => return Err(error_msg),
                        };
                    }
                };
                match make_hf_size_margin_order(pool, exchange, &client_oid, "sell", &trade_symbol, size, "market", false, false).await {
                    Ok(_) => {}
                    Err(e) => match handle_db_error(pool, exchange, e).await {
                        Ok(_) => continue,
                        Err(_) => continue,
                    },
                }
            }
        }
    }
    if passed { Ok(true) } else { Ok(false) }
}

pub async fn get_bot_by_exit_tp_client_oid_p(
    pool: &sqlx::Pool<sqlx::Postgres>,
    exchange: &str,
    client_oid: &str,
    price_increment: Decimal,
    quote_increment: Decimal,
    order: &OrderData,
) -> Result<(), String> {
    // if clientOid in bots entry_id (2 phase)
    match get_bot_by_exit_tp_client_oid(pool, exchange, client_oid).await {
        Ok(Some(bot)) => {
            // client_oid == exit_tp_client_oid
            // delete exit_tp_client_oid stop order
            match delete_exit_tp_id_bot_by_client_oid(pool, exchange, client_oid).await {
                Ok(_) => {}
                Err(e) => match handle_db_error(pool, exchange, e).await {
                    Ok(_) => {}
                    Err(_) => {}
                },
            }
            match &bot.exit_sl_client_oid {
                Some(exit_sl_client_oid) => {
                    // clear exit_sl_client_oid in bots by id !!
                    match delete_exit_sl_id_bot_by_client_oid(pool, exchange, exit_sl_client_oid).await {
                        Ok(_) => {}
                        Err(e) => match handle_db_error(pool, exchange, e).await {
                            Ok(_) => {}
                            Err(_) => {}
                        },
                    }
                    let mut query_params: Map<&str, &str, 8> = Map::new();

                    query_params.insert("clientOid", exit_sl_client_oid);

                    match api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(build_query_string(query_params)).await {
                        Ok(_) => {
                            log::info!("Successfully cancel stop order :{}", &exit_sl_client_oid)
                        }
                        Err(e) => match handle_db_error(pool, exchange, e).await {
                            Ok(_) => {}
                            Err(_) => {}
                        },
                    }
                }
                None => {}
            }
            match get_total_match_value_by_client_oid(pool, exchange, client_oid).await {
                Ok(Some(return_balance)) => {
                    {
                        if order.side == "buy" {
                            let old_balance: Decimal = match bot.balance_decimal() {
                                Ok(old_balance) => old_balance,
                                Err(e) => match handle_db_error(pool, exchange, e).await {
                                    Ok(error_msg) => return Err(error_msg),
                                    Err(error_msg) => return Err(error_msg),
                                },
                            };
                            let new_balance: Decimal = old_balance + old_balance - return_balance;
                            match update_balance_bot_by_exit_tp_client_oid(pool, exchange, client_oid, &format!("{:.4}", new_balance)).await {
                                Ok(_) => {}
                                Err(e) => match handle_db_error(pool, exchange, e).await {
                                    Ok(_) => {}
                                    Err(_) => {}
                                },
                            }
                            // create new random order
                            match make_random_trade(pool, exchange, new_balance, bot.id).await {
                                Ok(()) => {}
                                Err(e) => match handle_db_error(pool, exchange, e).await {
                                    Ok(_) => {}
                                    Err(_) => {}
                                },
                            }
                        } else if order.side == "sell" {
                            match update_balance_bot_by_exit_tp_client_oid(pool, exchange, client_oid, &format!("{:.4}", return_balance)).await {
                                Ok(_) => {}
                                Err(e) => match handle_db_error(pool, exchange, e).await {
                                    Ok(_) => {}
                                    Err(_) => {}
                                },
                            }
                            // create new random order
                            match make_random_trade(pool, exchange, return_balance, bot.id).await {
                                Ok(()) => {}
                                Err(e) => match handle_db_error(pool, exchange, e).await {
                                    Ok(_) => {}
                                    Err(_) => {}
                                },
                            }
                        }
                    }
                }
                Ok(None) => {
                    log::error!("No records found or error occurred");
                }
                Err(e) => match handle_db_error(pool, exchange, e).await {
                    Ok(_) => {}
                    Err(_) => {}
                },
            }
            return Ok(());
        }
        Ok(None) => Ok(()),
        Err(e) => match handle_db_error(pool, exchange, e).await {
            Ok(_) => Ok(()),
            Err(_) => Ok(()),
        },
    }
}

pub async fn get_bot_by_exit_sl_client_oid_p(
    pool: &sqlx::Pool<sqlx::Postgres>,
    exchange: &str,
    client_oid: &str,
    price_increment: Decimal,
    quote_increment: Decimal,
    order: &OrderData,
) -> Result<(), String> {
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
                                Err(e) => match handle_db_error(pool, exchange, e).await {
                                    Ok(error_msg) => return Err(error_msg),
                                    Err(error_msg) => return Err(error_msg),
                                },
                            }
                            let mut query_params: Map<&str, &str, 8> = Map::new();

                            query_params.insert("clientOid", exit_tp_client_oid);

                            match api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(build_query_string(query_params)).await {
                                Ok(_) => {
                                    log::info!("Successfully cancel stop order :{}", &exit_tp_client_oid);
                                }
                                Err(e) => match handle_db_error(pool, exchange, e).await {
                                    Ok(error_msg) => return Err(error_msg),
                                    Err(error_msg) => return Err(error_msg),
                                },
                            }
                        }
                        None => {}
                    }

                    match get_total_match_value_by_client_oid(pool, exchange, client_oid).await {
                        Ok(Some(return_balance)) => {
                            if order.side == "buy" {
                                let old_balance = match bot.balance_decimal() {
                                    Ok(old_balance) => old_balance,
                                    Err(e) => match handle_db_error(pool, exchange, e).await {
                                        Ok(error_msg) => return Err(error_msg),
                                        Err(error_msg) => return Err(error_msg),
                                    },
                                };
                                let new_balance = old_balance + old_balance - return_balance;
                                match update_balance_bot_by_exit_sl_client_oid(pool, exchange, client_oid, &format!("{:.4}", new_balance)).await {
                                    Ok(_) => {}
                                    Err(e) => match handle_db_error(pool, exchange, e).await {
                                        Ok(error_msg) => return Err(error_msg),
                                        Err(error_msg) => return Err(error_msg),
                                    },
                                }
                                // create new random order
                                match make_random_trade(pool, exchange, new_balance, bot.id).await {
                                    Ok(()) => {}
                                    Err(e) => match handle_db_error(pool, exchange, e).await {
                                        Ok(error_msg) => return Err(error_msg),
                                        Err(error_msg) => return Err(error_msg),
                                    },
                                }
                            } else if order.side == "sell" {
                                match update_balance_bot_by_exit_sl_client_oid(pool, exchange, client_oid, &format!("{:.4}", return_balance)).await {
                                    Ok(_) => {}
                                    Err(e) => match handle_db_error(pool, exchange, e).await {
                                        Ok(error_msg) => return Err(error_msg),
                                        Err(error_msg) => return Err(error_msg),
                                    },
                                }

                                // create new random order
                                match make_random_trade(pool, exchange, return_balance, bot.id).await {
                                    Ok(()) => {}
                                    Err(e) => match handle_db_error(pool, exchange, e).await {
                                        Ok(error_msg) => return Err(error_msg),
                                        Err(error_msg) => return Err(error_msg),
                                    },
                                }
                            }
                        }
                        Ok(None) => {
                            log::error!("No records found or error occurred");
                        }
                        Err(e) => match handle_db_error(pool, exchange, e).await {
                            Ok(error_msg) => return Err(error_msg),
                            Err(error_msg) => return Err(error_msg),
                        },
                    }
                }
                Err(e) => match handle_db_error(pool, exchange, e).await {
                    Ok(_) => {}
                    Err(_) => {}
                },
            }

            return Ok(());
        }
        Ok(None) => Ok(()),
        Err(e) => match handle_db_error(pool, exchange, e).await {
            Ok(_) => Ok(()),
            Err(_) => Ok(()),
        },
    }
}

pub async fn get_bot_by_entry_client_oid_p(
    pool: &sqlx::Pool<sqlx::Postgres>,
    exchange: &str,
    client_oid: &str,
    price_increment: Decimal,
    quote_increment: Decimal,
    order: &OrderData,
) -> Result<(), String> {
    // if clientOid in bots entry_id (1 phase)
    match get_bot_by_entry_client_oid(pool, exchange, client_oid).await {
        Ok(Some(bot)) => {
            // create new stop tp and sl orders

            let filled_size: Decimal = match order.filled_size_decimal() {
                Ok(filled_size) => filled_size,
                Err(e) => match handle_db_error(pool, exchange, e).await {
                    Ok(error_msg) => return Err(error_msg),
                    Err(error_msg) => return Err(error_msg),
                },
            };

            let new_balance: Decimal = match get_total_match_value_by_client_oid(pool, exchange, client_oid).await {
                Ok(Some(new_balance)) => new_balance,
                Ok(None) => {
                    let msg: String = format!("No records found in events: {}", client_oid);
                    log::error!("{}", msg);
                    match handle_db_error(pool, exchange, msg).await {
                        Ok(error_msg) => return Err(error_msg),
                        Err(error_msg) => return Err(error_msg),
                    }
                }

                Err(e) => match handle_db_error(pool, exchange, e).await {
                    Ok(error_msg) => return Err(error_msg),
                    Err(error_msg) => return Err(error_msg),
                },
            };

            match update_bot_balance_by_entry_client_oid(pool, exchange, client_oid, &format!("{:.4}", new_balance)).await {
                Ok(_) => {}
                Err(e) => match handle_db_error(pool, exchange, e).await {
                    Ok(error_msg) => return Err(error_msg),
                    Err(error_msg) => return Err(error_msg),
                },
            }

            if order.side == "buy" {
                let match_price: Decimal = new_balance / filled_size;
                let trigger_tp_price: Decimal = match_price * tp_buy_percent(); // price + 7%
                let trigger_sl_price: Decimal = match_price * sl_buy_percent(); // price - 5%

                let exit_tp_client_oid: String = Uuid::new_v4().to_string();
                let exit_sl_client_oid: String = Uuid::new_v4().to_string();

                // tp order
                let stop_price_tp: String = match format_assert_decimal(trigger_tp_price, price_increment) {
                    Ok(stop_price_tp) => stop_price_tp,
                    Err(e) => {
                        let msg: String = format!("Fail parse:{} {} error:{}", trigger_tp_price, price_increment, e);
                        log::error!("{}", msg);
                        match handle_db_error(pool, exchange, msg).await {
                            Ok(error_msg) => return Err(error_msg),
                            Err(error_msg) => return Err(error_msg),
                        };
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
                        match handle_db_error(pool, exchange, msg).await {
                            Ok(error_msg) => return Err(error_msg),
                            Err(error_msg) => return Err(error_msg),
                        };
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
                match update_exit_tp_client_oid_bot_by_entry_client_oid(pool, exchange, client_oid, &exit_tp_client_oid).await {
                    Ok(_) => {}
                    Err(e) => match handle_db_error(pool, exchange, e).await {
                        Ok(error_msg) => return Err(error_msg),
                        Err(error_msg) => return Err(error_msg),
                    },
                }
                // add exit_sl_client_oid by entry_id
                match update_exit_sl_client_oid_bot_by_entry_client_oid(pool, exchange, client_oid, &exit_sl_client_oid).await {
                    Ok(_) => {}
                    Err(e) => match handle_db_error(pool, exchange, e).await {
                        Ok(error_msg) => return Err(error_msg),
                        Err(error_msg) => return Err(error_msg),
                    },
                }

                let msg_tp_order2: String = match serialize_body(Some(msg_tp_order)) {
                    Ok(body_str) => body_str,
                    Err(e) => return Err(e),
                };
                let tp_fut = api_v3_hf_margin_stop_order_post(msg_tp_order2);

                let msg_sl_order2: String = match serialize_body(Some(msg_sl_order)) {
                    Ok(body_str) => body_str,
                    Err(e) => return Err(e),
                };
                let sl_fut = api_v3_hf_margin_stop_order_post(msg_sl_order2);

                let (tp_res, sl_res) = tokio::join!(tp_fut, sl_fut);

                match (&tp_res, &sl_res) {
                    (Ok(tp_resp), Ok(sl_resp)) => {
                        match tp_resp {
                            Some(response_data) => match update_exit_tp_order_id_bot_by_exit_tp_client_oid(pool, exchange, &response_data.order_id, &response_data.client_oid).await {
                                Ok(_) => {}
                                Err(e) => match handle_db_error(pool, exchange, e).await {
                                    Ok(error_msg) => return Err(error_msg),
                                    Err(error_msg) => return Err(error_msg),
                                },
                            },
                            None => {}
                        }

                        match sl_resp {
                            Some(response_data) => match update_exit_sl_order_id_bot_by_exit_sl_client_oid(pool, exchange, &response_data.order_id, &response_data.client_oid).await {
                                Ok(_) => {}
                                Err(e) => match handle_db_error(pool, exchange, e).await {
                                    Ok(error_msg) => return Err(error_msg),
                                    Err(error_msg) => return Err(error_msg),
                                },
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
                                    Err(e) => match handle_db_error(pool, exchange, e).await {
                                        Ok(error_msg) => return Err(error_msg),
                                        Err(error_msg) => return Err(error_msg),
                                    },
                                };
                            }
                            None => {}
                        }

                        match delete_exit_sl_id_bot_by_client_oid(pool, exchange, &exit_sl_client_oid).await {
                            Ok(_) => {}
                            Err(e) => match handle_db_error(pool, exchange, e).await {
                                Ok(error_msg) => return Err(error_msg),
                                Err(error_msg) => return Err(error_msg),
                            },
                        }

                        let msg: String = format!("Failed add TP order: {}. SL was cancelled for symmetry.", tp_err);
                        log::error!("{}", msg);

                        match handle_db_error(pool, exchange, msg).await {
                            Ok(_) => {}
                            Err(_) => {}
                        }
                    }
                    (Ok(tp_resp), Err(sl_err)) => {
                        match tp_resp {
                            Some(response_data) => {
                                let mut query_params: Map<&str, &str, 8> = Map::new();

                                query_params.insert("clientOid", &response_data.client_oid);

                                match api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(build_query_string(query_params)).await {
                                    Ok(_) => {}
                                    Err(e) => match handle_db_error(pool, exchange, e).await {
                                        Ok(error_msg) => return Err(error_msg),
                                        Err(error_msg) => return Err(error_msg),
                                    },
                                }
                            }
                            None => {}
                        }

                        match delete_exit_tp_id_bot_by_client_oid(pool, exchange, &exit_tp_client_oid).await {
                            Ok(_) => {}
                            Err(e) => match handle_db_error(pool, exchange, e).await {
                                Ok(error_msg) => return Err(error_msg),
                                Err(error_msg) => return Err(error_msg),
                            },
                        }

                        let msg: String = format!("Failed add SL order: {}. TP was cancelled for symmetry.", sl_err);
                        log::error!("{}", msg);

                        match handle_db_error(pool, exchange, msg).await {
                            Ok(_) => {}
                            Err(_) => {}
                        }
                    }
                    (Err(tp_err), Err(sl_err)) => {
                        let msg: String = format!("Failed add both stop orders: TP={}, SL={}", tp_err, sl_err);
                        log::error!("{}", msg);

                        match handle_db_error(pool, exchange, msg).await {
                            Ok(_) => {}
                            Err(_) => {}
                        }
                        match delete_symbol_bot_by_exit_sl_client_oid(pool, exchange, &exit_sl_client_oid).await {
                            Ok(_) => {}
                            Err(e) => match handle_db_error(pool, exchange, e).await {
                                Ok(error_msg) => return Err(error_msg),
                                Err(error_msg) => return Err(error_msg),
                            },
                        }
                        match delete_exit_sl_id_bot_by_client_oid(pool, exchange, &exit_sl_client_oid).await {
                            Ok(_) => {}
                            Err(e) => match handle_db_error(pool, exchange, e).await {
                                Ok(error_msg) => return Err(error_msg),
                                Err(error_msg) => return Err(error_msg),
                            },
                        }
                        match delete_exit_tp_id_bot_by_client_oid(pool, exchange, &exit_tp_client_oid).await {
                            Ok(_) => {}
                            Err(e) => match handle_db_error(pool, exchange, e).await {
                                Ok(error_msg) => return Err(error_msg),
                                Err(error_msg) => return Err(error_msg),
                            },
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

                let stop_price_tp: String = match format_assert_decimal(trigger_tp_price, price_increment) {
                    Ok(stop_price_tp) => stop_price_tp,
                    Err(e) => {
                        let msg: String = format!("Fail parse:{} {} error:{}", trigger_tp_price, price_increment, e);
                        log::error!("{}", msg);
                        match handle_db_error(pool, exchange, msg).await {
                            Ok(error_msg) => return Err(error_msg),
                            Err(error_msg) => return Err(error_msg),
                        };
                    }
                };
                let funds_tp_str: String = match format_assert_decimal(funds_tp, quote_increment) {
                    Ok(funds_tp_str) => funds_tp_str,
                    Err(e) => {
                        let msg: String = format!("Fail parse:{} {} error:{}", funds_tp, quote_increment, e);
                        log::error!("{}", msg);
                        match handle_db_error(pool, exchange, msg).await {
                            Ok(error_msg) => return Err(error_msg),
                            Err(error_msg) => return Err(error_msg),
                        };
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
                        match handle_db_error(pool, exchange, msg).await {
                            Ok(error_msg) => return Err(error_msg),
                            Err(error_msg) => return Err(error_msg),
                        };
                    }
                };
                let funds_sl_str: String = match format_assert_decimal(funds_sl, quote_increment) {
                    Ok(funds_sl_str) => funds_sl_str,
                    Err(e) => {
                        let msg: String = format!("Fail parse:{} {} error:{}", funds_sl, quote_increment, e);
                        log::error!("{}", msg);
                        match handle_db_error(pool, exchange, msg).await {
                            Ok(error_msg) => return Err(error_msg),
                            Err(error_msg) => return Err(error_msg),
                        };
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
                match update_exit_tp_client_oid_bot_by_entry_client_oid(pool, exchange, client_oid, &exit_tp_client_oid).await {
                    Ok(_) => {}
                    Err(e) => match handle_db_error(pool, exchange, e).await {
                        Ok(error_msg) => return Err(error_msg),
                        Err(error_msg) => return Err(error_msg),
                    },
                }
                // add exit_sl_client_oid by entry_id
                match update_exit_sl_client_oid_bot_by_entry_client_oid(pool, exchange, client_oid, &exit_sl_client_oid).await {
                    Ok(_) => {}
                    Err(e) => match handle_db_error(pool, exchange, e).await {
                        Ok(error_msg) => return Err(error_msg),
                        Err(error_msg) => return Err(error_msg),
                    },
                }

                let msg_tp_order2: String = match serialize_body(Some(msg_tp_order)) {
                    Ok(body_str) => body_str,
                    Err(e) => return Err(e),
                };
                let tp_fut = api_v3_hf_margin_stop_order_post(msg_tp_order2);

                let msg_sl_order2: String = match serialize_body(Some(msg_sl_order)) {
                    Ok(body_str) => body_str,
                    Err(e) => return Err(e),
                };
                let sl_fut = api_v3_hf_margin_stop_order_post(msg_sl_order2);
                let (tp_res, sl_res) = tokio::join!(tp_fut, sl_fut);

                match (&tp_res, &sl_res) {
                    (Ok(tp_resp), Ok(sl_resp)) => {
                        match tp_resp {
                            Some(response_data) => match update_exit_tp_order_id_bot_by_exit_tp_client_oid(pool, exchange, &response_data.order_id, &response_data.client_oid).await {
                                Ok(_) => {}
                                Err(e) => match handle_db_error(pool, exchange, e).await {
                                    Ok(error_msg) => return Err(error_msg),
                                    Err(error_msg) => return Err(error_msg),
                                },
                            },
                            None => {}
                        }

                        match sl_resp {
                            Some(response_data) => match update_exit_sl_order_id_bot_by_exit_sl_client_oid(pool, exchange, &response_data.order_id, &response_data.client_oid).await {
                                Ok(_) => {}
                                Err(e) => match handle_db_error(pool, exchange, e).await {
                                    Ok(error_msg) => return Err(error_msg),
                                    Err(error_msg) => return Err(error_msg),
                                },
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
                                    Err(e) => match handle_db_error(pool, exchange, e).await {
                                        Ok(error_msg) => return Err(error_msg),
                                        Err(error_msg) => return Err(error_msg),
                                    },
                                }
                            }
                            None => {}
                        }

                        match delete_exit_sl_id_bot_by_client_oid(pool, exchange, &exit_sl_client_oid).await {
                            Ok(_) => {}
                            Err(e) => match handle_db_error(pool, exchange, e).await {
                                Ok(error_msg) => return Err(error_msg),
                                Err(error_msg) => return Err(error_msg),
                            },
                        }

                        let msg: String = format!("Failed add TP order: {}. SL was cancelled for symmetry.", tp_err);
                        log::error!("{}", msg);

                        match handle_db_error(pool, exchange, msg).await {
                            Ok(_) => {}
                            Err(_) => {}
                        }
                    }
                    (Ok(tp_resp), Err(sl_err)) => match tp_resp {
                        Some(response_data) => {
                            let mut query_params: Map<&str, &str, 8> = Map::new();

                            query_params.insert("clientOid", &response_data.client_oid);
                            match api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(build_query_string(query_params)).await {
                                Ok(_) => {
                                    match delete_exit_tp_id_bot_by_client_oid(pool, exchange, &exit_tp_client_oid).await {
                                        Ok(_) => {}
                                        Err(e) => match handle_db_error(pool, exchange, e).await {
                                            Ok(error_msg) => return Err(error_msg),
                                            Err(error_msg) => return Err(error_msg),
                                        },
                                    }
                                    let msg: String = format!("Failed add SL order: {}. TP was cancelled for symmetry.", sl_err);
                                    log::error!("{}", msg);

                                    match handle_db_error(pool, exchange, msg).await {
                                        Ok(_) => {}
                                        Err(_) => {}
                                    }
                                }
                                Err(e) => match handle_db_error(pool, exchange, e).await {
                                    Ok(error_msg) => return Err(error_msg),
                                    Err(error_msg) => return Err(error_msg),
                                },
                            }
                        }
                        None => {}
                    },
                    (Err(tp_err), Err(sl_err)) => {
                        let msg: String = format!("Failed add both stop orders: TP={}, SL={}", tp_err, sl_err);
                        log::error!("{}", msg);

                        match handle_db_error(pool, exchange, msg).await {
                            Ok(_) => {}
                            Err(_) => {}
                        }
                        match delete_symbol_bot_by_exit_sl_client_oid(pool, exchange, &exit_sl_client_oid).await {
                            Ok(_) => {}
                            Err(e) => match handle_db_error(pool, exchange, e).await {
                                Ok(error_msg) => return Err(error_msg),
                                Err(error_msg) => return Err(error_msg),
                            },
                        }
                        match delete_exit_sl_id_bot_by_client_oid(pool, exchange, &exit_sl_client_oid).await {
                            Ok(_) => {}
                            Err(e) => match handle_db_error(pool, exchange, e).await {
                                Ok(_) => {}
                                Err(_) => {}
                            },
                        }
                        match delete_exit_tp_id_bot_by_client_oid(pool, exchange, &exit_tp_client_oid).await {
                            Ok(_) => {}
                            Err(e) => match handle_db_error(pool, exchange, e).await {
                                Ok(_) => {}
                                Err(_) => {}
                            },
                        }
                    }
                }
            }

            // delete entry_id from db
            match set_null_entry_client_oid_by_entry_client_oid(pool, exchange, client_oid).await {
                Ok(_) => Ok(()),
                Err(e) => match handle_db_error(pool, exchange, e).await {
                    Ok(_) => Ok(()),
                    Err(_) => Ok(()),
                },
            }
        }
        Ok(None) => Ok(()),
        Err(e) => match handle_db_error(pool, exchange, e).await {
            Ok(_) => Ok(()),
            Err(_) => Ok(()),
        },
    }
}

pub async fn handle_trade_order_event(order: OrderData, pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str) -> Result<(), String> {
    match insert_db_orderevent(pool, exchange, &order).await {
        Ok(_) => log::info!("{:.?}", order),
        Err(e) => match handle_db_error(pool, exchange, e).await {
            Ok(error_msg) => return Err(error_msg),
            Err(error_msg) => return Err(error_msg),
        },
    }

    let client_oid = match order.client_oid.clone() {
        Some(client_oid) => client_oid,
        None => {
            let msg: String = format!("client_oid in order is none: {:.?}", order);
            log::error!("{}", msg);

            match handle_db_error(pool, exchange, msg).await {
                Ok(error_msg) => return Err(error_msg),
                Err(error_msg) => return Err(error_msg),
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

            match handle_db_error(pool, exchange, msg).await {
                Ok(error_msg) => return Err(error_msg),
                Err(error_msg) => return Err(error_msg),
            }
        }
        Err(e) => match handle_db_error(pool, exchange, e).await {
            Ok(error_msg) => return Err(error_msg),
            Err(error_msg) => return Err(error_msg),
        },
    };

    let price_increment: Decimal = match symbol_info.price_increment_decimal() {
        Ok(price_increment) => price_increment,
        Err(e) => match handle_db_error(pool, exchange, e).await {
            Ok(error_msg) => return Err(error_msg),
            Err(error_msg) => return Err(error_msg),
        },
    };

    let quote_increment: Decimal = match symbol_info.quote_increment_decimal() {
        Ok(quote_increment) => quote_increment,
        Err(e) => match handle_db_error(pool, exchange, e).await {
            Ok(error_msg) => return Err(error_msg),
            Err(error_msg) => return Err(error_msg),
        },
    };

    match get_bot_by_exit_tp_client_oid_p(pool, exchange, &client_oid, price_increment, quote_increment, &order).await {
        Ok(_) => {}
        Err(_) => {}
    };

    match get_bot_by_exit_sl_client_oid_p(pool, exchange, &client_oid, price_increment, quote_increment, &order).await {
        Ok(_) => {}
        Err(_) => {}
    };

    match get_bot_by_entry_client_oid_p(pool, exchange, &client_oid, price_increment, quote_increment, &order).await {
        Ok(_) => {}
        Err(_) => {}
    };

    Ok(())
}

pub async fn handle_position_event(position: PositionData, pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str) -> Result<(), String> {
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
                                        Err(e) => return Err(e),
                                    };

                                    match api_v3_margin_repay_post(body_str).await {
                                        Ok(_) => {
                                            log::info!("Repay {} {} liability with available {}", token_liability, asset, &asset_info.available);
                                        }
                                        Err(e) => match handle_db_error(pool, exchange, e).await {
                                            Ok(_) => continue,
                                            Err(_) => continue,
                                        },
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
                                        Err(e) => return Err(e),
                                    };

                                    match api_v3_margin_repay_post(body_str).await {
                                        Ok(_) => {
                                            log::info!("Partially repay {} {} liability with available {}", token_liability, asset, &asset_info.available);
                                        }
                                        Err(e) => match handle_db_error(pool, exchange, e).await {
                                            Ok(_) => continue,
                                            Err(_) => continue,
                                        },
                                    }
                                }
                            }
                        }
                        Err(e) => match handle_db_error(pool, exchange, e).await {
                            Ok(_) => continue,
                            Err(_) => continue,
                        },
                    },
                    None => {
                        let msg: String = format!("Failed get asset:{} from:{:.?}", asset, position.asset_list);
                        log::error!("{}", msg);

                        match handle_db_error(pool, exchange, msg).await {
                            Ok(_) => continue,
                            Err(_) => continue,
                        }
                    }
                }
            }
        }
        Err(e) => return Err(e),
    }

    match upsert_position_ratio(pool, exchange, position.debt_ratio, position.total_asset, &position.margin_coefficient_total_asset, &position.total_debt).await {
        Ok(_) => {}
        Err(e) => match handle_db_error(pool, exchange, e).await {
            Ok(error_msg) => return Err(error_msg),
            Err(error_msg) => return Err(error_msg),
        },
    }

    for (symbol, amount) in &position.debt_list {
        match upsert_position_debt(pool, exchange, symbol, amount).await {
            Ok(_) => {}
            Err(e) => match handle_db_error(pool, exchange, e).await {
                Ok(_) => continue,
                Err(_) => continue,
            },
        }
    }
    for (symbol, symbol_info) in &position.asset_list {
        match upsert_position_asset(pool, exchange, symbol, &symbol_info.total, &symbol_info.available, &symbol_info.hold).await {
            Ok(_) => {}
            Err(e) => match handle_db_error(pool, exchange, e).await {
                Ok(_) => continue,
                Err(_) => continue,
            },
        }
    }

    Ok(())
}

pub async fn handle_advanced_orders(order: AdvancedOrders, pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str) -> Result<(), String> {
    log::info!("{:.?}", order);
    match order.error {
        Some(_) => {}
        None => return Ok(()),
    }

    let msg: String = format!("Got error on stop order : {:?}", order);
    log::error!("{}", msg);

    match handle_db_error(pool, exchange, msg).await {
        Ok(_) => {}
        Err(_) => {}
    }

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
                                let msg: String = format!("Fail parse size order:{} new_exit_sl_client_oid:{} size_clone:{:.?}", order_id_ref, new_exit_client_oid, size_clone,);
                                log::error!("{}", msg);

                                match handle_db_error(pool, exchange, msg).await {
                                    Ok(_) => continue,
                                    Err(_) => continue,
                                }
                            }
                        },
                        _ => {
                            let msg: String = format!("Fail match side_clone:{}", side_ref);
                            log::error!("{}", msg);

                            match handle_db_error(pool, exchange, msg).await {
                                Ok(_) => continue,
                                Err(_) => continue,
                            }
                        }
                    },
                    Err(e) => match handle_db_error(pool, exchange, e).await {
                        Ok(_) => continue,
                        Err(_) => continue,
                    },
                }
            }
            "entry" => {
                // need find tp
                match update_exit_tp_client_oid_bot_by_exit_tp_order_id(pool, exchange, order_id_ref, &new_exit_client_oid).await {
                    Ok(_) => match side_ref.as_str() {
                        "buy" => match funds_clone {
                            Some(funds) => make_hf_funds_margin_order(pool, exchange, &new_exit_client_oid, side_ref, symbol_ref, funds, "market", true, false).await,
                            None => {
                                let msg: String = format!("Fail parse funds_clone order:{} new_exit_tp_client_oid:{} funds_clone:{:.?}", order_id_ref, new_exit_client_oid, funds_clone);
                                log::error!("{}", msg);

                                match handle_db_error(pool, exchange, msg).await {
                                    Ok(_) => continue,
                                    Err(_) => continue,
                                }
                            }
                        },
                        "sell" => match size_clone {
                            Some(size) => make_hf_size_margin_order(pool, exchange, &new_exit_client_oid, side_ref, symbol_ref, size, "market", true, false).await,
                            None => {
                                let msg: String = format!("Fail parse size_clone order:{} new_exit_tp_client_oid:{} size_clone:{:.?}", order_id_ref, new_exit_client_oid, size_clone);
                                log::error!("{}", msg);

                                match handle_db_error(pool, exchange, msg).await {
                                    Ok(_) => continue,
                                    Err(_) => continue,
                                }
                            }
                        },
                        _ => {
                            let msg: String = format!("Fail match side_clone:{}", side_ref);
                            log::error!("{}", msg);

                            match handle_db_error(pool, exchange, msg).await {
                                Ok(_) => continue,
                                Err(_) => continue,
                            }
                        }
                    },
                    Err(e) => match handle_db_error(pool, exchange, e).await {
                        Ok(_) => continue,
                        Err(_) => continue,
                    },
                }
            }
            _ => {
                let msg: String = format!("Fail match stop_clone:{}", stop_ref);
                log::error!("{}", msg);

                match handle_db_error(pool, exchange, msg).await {
                    Ok(_) => continue,
                    Err(_) => continue,
                }
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

                match handle_db_error(pool, exchange, msg).await {
                    Ok(error_msg) => return Err(error_msg),
                    Err(error_msg) => return Err(error_msg),
                }
            }
        }
    }
}

pub async fn process_kcn_msg(pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str, msg: &str) -> Result<(), String> {
    match serde_json::from_str::<KuCoinMessage>(msg) {
        Ok(event) => match event {
            KuCoinMessage::Welcome(data) => match serde_json::to_value(&data) {
                Ok(data) => match insert_db_event(pool, exchange, &data).await {
                    Ok(_) => Ok(()),
                    Err(e) => match handle_db_error(pool, exchange, e).await {
                        Ok(error_msg) => Err(error_msg),
                        Err(error_msg) => Err(error_msg),
                    },
                },
                Err(e) => {
                    let msg: String = format!("Failed to serialize request '{:?}' as {}: {}", &data, stringify!(WelcomeData), e);
                    log::error!("{}", msg);
                    match handle_db_error(pool, exchange, msg).await {
                        Ok(error_msg) => Err(error_msg),
                        Err(error_msg) => Err(error_msg),
                    }
                }
            },
            KuCoinMessage::Message(data) => {
                if data.topic == "/account/balance" {
                    match BalanceData::deserialize(&data.data) {
                        Ok(balance) => match insert_db_balance(pool, exchange, balance).await {
                            Ok(_) => Ok(()),
                            Err(e) => match handle_db_error(pool, exchange, e).await {
                                Ok(error_msg) => Err(error_msg),
                                Err(error_msg) => Err(error_msg),
                            },
                        },
                        Err(e) => {
                            let msg: String = format!("Failed to serialize request '{:?}' as {}: {}", &data.data, stringify!(BalanceData), e);
                            log::error!("{}", msg);
                            match handle_db_error(pool, exchange, msg).await {
                                Ok(error_msg) => Err(error_msg),
                                Err(error_msg) => Err(error_msg),
                            }
                        }
                    }
                } else if data.topic == "/spotMarket/tradeOrdersV2" {
                    match OrderData::deserialize(&data.data) {
                        Ok(order) => match handle_trade_order_event(order, pool, exchange).await {
                            Ok(_) => Ok(()),
                            Err(e) => match handle_db_error(pool, exchange, e).await {
                                Ok(error_msg) => Err(error_msg),
                                Err(error_msg) => Err(error_msg),
                            },
                        },
                        Err(e) => {
                            let msg: String = format!("Failed to serialize request '{:?}' as {}: {}", &data.data, stringify!(OrderData), e);
                            log::error!("{}", msg);
                            match handle_db_error(pool, exchange, msg).await {
                                Ok(error_msg) => Err(error_msg),
                                Err(error_msg) => Err(error_msg),
                            }
                        }
                    }
                } else if data.topic == "/spotMarket/advancedOrders" {
                    match AdvancedOrders::deserialize(&data.data) {
                        Ok(order) => match handle_advanced_orders(order, pool, exchange).await {
                            Ok(_) => Ok(()),
                            Err(e) => match handle_db_error(pool, exchange, e).await {
                                Ok(error_msg) => Err(error_msg),
                                Err(error_msg) => Err(error_msg),
                            },
                        },
                        Err(e) => {
                            let msg: String = format!("Failed to serialize request '{:?}' as {}: {}", &data.data, stringify!(AdvancedOrders), e);
                            log::error!("{}", msg);
                            match handle_db_error(pool, exchange, msg).await {
                                Ok(error_msg) => Err(error_msg),
                                Err(error_msg) => Err(error_msg),
                            }
                        }
                    }
                } else if data.topic == "/margin/position" {
                    match PositionData::deserialize(&data.data) {
                        Ok(position) => match handle_position_event(position, pool, exchange).await {
                            Ok(_) => Ok(()),
                            Err(e) => match handle_db_error(pool, exchange, e).await {
                                Ok(error_msg) => Err(error_msg),
                                Err(error_msg) => Err(error_msg),
                            },
                        },
                        Err(e) => {
                            let msg: String = format!("Failed to serialize request '{:?}' as {}: {}", &data.data, stringify!(PositionData), e);
                            log::error!("{}", msg);
                            match handle_db_error(pool, exchange, msg).await {
                                Ok(error_msg) => Err(error_msg),
                                Err(error_msg) => Err(error_msg),
                            }
                        }
                    }
                } else {
                    let msg: String = format!("Unknown topic: {}", data.topic);
                    log::error!("{}", msg);
                    match handle_db_error(pool, exchange, msg).await {
                        Ok(error_msg) => Err(error_msg),
                        Err(error_msg) => Err(error_msg),
                    }
                }
            }
            KuCoinMessage::Ack(data) => match serde_json::to_value(&data) {
                Ok(data) => match insert_db_event(pool, exchange, &data).await {
                    Ok(_) => Ok(()),
                    Err(e) => match handle_db_error(pool, exchange, e).await {
                        Ok(error_msg) => Err(error_msg),
                        Err(error_msg) => Err(error_msg),
                    },
                },
                Err(e) => {
                    let msg: String = format!("Failed to serialize request '{:?}' as {}: {}", &data, stringify!(AckData), e);
                    log::error!("{}", msg);
                    match handle_db_error(pool, exchange, msg).await {
                        Ok(error_msg) => Err(error_msg),
                        Err(error_msg) => Err(error_msg),
                    }
                }
            },
            KuCoinMessage::Error(data) => {
                let msg: String = format!("Got error in WS {:?}", data);
                log::error!("{}", msg);
                match handle_db_error(pool, exchange, msg).await {
                    Ok(error_msg) => Err(error_msg),
                    Err(error_msg) => Err(error_msg),
                }
            }

            KuCoinMessage::Unknown => {
                let msg: String = format!("Unknown WS message type");
                log::error!("{}", msg);
                match handle_db_error(pool, exchange, msg).await {
                    Ok(error_msg) => Err(error_msg),
                    Err(error_msg) => Err(error_msg),
                }
            }
        },
        Err(e) => {
            let msg: String = format!("Failed to parse message:{} {}", msg, e);
            log::error!("{}", msg);
            match handle_db_error(pool, exchange, msg).await {
                Ok(error_msg) => Err(error_msg),
                Err(error_msg) => Err(error_msg),
            }
        }
    }
}

pub async fn make_random_trade(pool: &sqlx::Pool<sqlx::Postgres>, exchange: &str, balance_funds: Decimal, trade_bot_id: i32) -> Result<(), String> {
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
                let msg: String = format!("Failed get_random_symbol:");
                log::error!("{}", msg);

                match handle_db_error(pool, exchange, msg).await {
                    Ok(_) => continue,
                    Err(_) => continue,
                }
            }
            Err(e) => match handle_db_error(pool, exchange, e).await {
                Ok(_) => continue,
                Err(_) => continue,
            },
        };

        let symbol_info: Symbol = match fetch_symbol_info_by_symbol(pool, exchange, &tradeable_symbol).await {
            Ok(Some(symbol_info)) => symbol_info,
            Ok(None) => {
                let msg: String = format!("Symbol info not found for {}", tradeable_symbol);
                log::error!("{}", msg);

                match handle_db_error(pool, exchange, msg).await {
                    Ok(_) => continue,
                    Err(_) => continue,
                }
            }
            Err(e) => match handle_db_error(pool, exchange, e).await {
                Ok(_) => continue,
                Err(_) => continue,
            },
        };

        let entry_client_oid: String = Uuid::new_v4().to_string();

        match update_bot_entry_client_oid_by_id(pool, exchange, Some(&tradeable_symbol), Some(&entry_client_oid), trade_bot_id).await {
            Ok(_) => {}
            Err(e) => match handle_db_error(pool, exchange, e).await {
                Ok(_) => continue,
                Err(_) => continue,
            },
        }

        let order_result = match get_random_side() {
            "sell" => {
                let base_increment: Decimal = match symbol_info.base_increment_decimal() {
                    Ok(base_increment) => base_increment,
                    Err(e) => match handle_db_error(pool, exchange, e).await {
                        Ok(_) => continue,
                        Err(_) => continue,
                    },
                };

                let mut query_params: Map<&str, &str, 8> = Map::new();

                query_params.insert("symbol", &tradeable_symbol);

                let token_price_obj = match api_v1_market_orderbook_level1_get(build_query_string(query_params)).await {
                    Ok(token_price_obj) => token_price_obj,
                    Err(e) => match handle_db_error(pool, exchange, e).await {
                        Ok(_) => continue,
                        Err(_) => continue,
                    },
                };

                let token_price_obj2 = match token_price_obj {
                    Some(token_price_obj2) => token_price_obj2,
                    None => return Err("".to_string()),
                };

                let token_price: Decimal = match token_price_obj2.price_decimal() {
                    Ok(token_price) => token_price,
                    Err(e) => match handle_db_error(pool, exchange, e).await {
                        Ok(_) => continue,
                        Err(_) => continue,
                    },
                };
                let token_size: Decimal = balance_funds / token_price;
                let size: String = match format_assert_decimal(token_size, base_increment) {
                    Ok(size) => size,
                    Err(e) => {
                        let msg: String = format!("Fail parse:{} {} error:{}", token_size, base_increment, e);
                        log::error!("{}", msg);
                        match handle_db_error(pool, exchange, msg).await {
                            Ok(error_msg) => return Err(error_msg),
                            Err(error_msg) => return Err(error_msg),
                        };
                    }
                };
                make_hf_size_margin_order(pool, exchange, &entry_client_oid, "sell", &tradeable_symbol, size, "market", true, false).await
            }
            "buy" => {
                let quote_increment = match symbol_info.quote_increment_decimal() {
                    Ok(quote_increment) => quote_increment,
                    Err(e) => match handle_db_error(pool, exchange, e).await {
                        Ok(_) => continue,
                        Err(_) => continue,
                    },
                };
                let funds: String = match format_assert_decimal(balance_funds, quote_increment) {
                    Ok(funds) => funds,
                    Err(e) => {
                        let msg: String = format!("Fail parse:{} {} error:{}", balance_funds, quote_increment, e);
                        log::error!("{}", msg);
                        match handle_db_error(pool, exchange, msg).await {
                            Ok(error_msg) => return Err(error_msg),
                            Err(error_msg) => return Err(error_msg),
                        };
                    }
                };
                make_hf_funds_margin_order(pool, exchange, &entry_client_oid, "buy", &tradeable_symbol, funds, "market", true, false).await
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
                    Err(e) => match handle_db_error(pool, exchange, e).await {
                        Ok(error_msg) => return Err(error_msg),
                        Err(error_msg) => return Err(error_msg),
                    },
                }

                let msg: String = format!("❌ Order failed (attempt {}/{}): {} {}", attempt, MAX_RETRIES, tradeable_symbol, e);
                log::error!("{}", msg);

                match handle_db_error(pool, exchange, msg).await {
                    Ok(_) => continue,
                    Err(_) => continue,
                }
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
) -> Result<MakeOrderResData, String> {
    // only for buy orders
    let args_time_in_force: &str = "GTC";

    match insert_db_msgsend(pool, exchange, Some(symbol), Some(side), None, Some(&funds), None, Some(args_time_in_force), Some(type_), Some(&auto_borrow), Some(&auto_repay), Some(client_oid), None)
        .await
    {
        Ok(_) => {}
        Err(e) => match handle_db_error(pool, exchange, e).await {
            Ok(error_msg) => return Err(error_msg),
            Err(error_msg) => return Err(error_msg),
        },
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
        Err(e) => return Err(e),
    };
    match api_v3_hf_margin_order_post(body_str).await {
        Ok(data) => match data {
            Some(data) => Ok(data),
            None => Err("".to_string()),
        },
        Err(e) => match handle_db_error(pool, exchange, e).await {
            Ok(error_msg) => Err(error_msg),
            Err(error_msg) => Err(error_msg),
        },
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
) -> Result<MakeOrderResData, String> {
    // only for sell orders
    let args_time_in_force: &str = "GTC";

    match insert_db_msgsend(pool, exchange, Some(symbol), Some(side), Some(&size), None, None, Some(args_time_in_force), Some(type_), Some(&auto_borrow), Some(&auto_repay), Some(client_oid), None)
        .await
    {
        Ok(_) => {}
        Err(e) => match handle_db_error(pool, exchange, e).await {
            Ok(error_msg) => return Err(error_msg),
            Err(error_msg) => return Err(error_msg),
        },
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
        Err(e) => return Err(e),
    };
    match api_v3_hf_margin_order_post(body_str).await {
        Ok(data) => match data {
            Some(data) => Ok(data),
            None => Err("".to_string()),
        },
        Err(e) => match handle_db_error(pool, exchange, e).await {
            Ok(error_msg) => Err(error_msg),
            Err(error_msg) => Err(error_msg),
        },
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
