use crate::api::db::{
    delete_exit_sl_id_bot_by_client_oid, delete_exit_tp_id_bot_by_client_oid,
    delete_symbol_bot_by_exit_sl_client_oid, fetch_currency_info_by_symbol,
    fetch_symbol_info_by_symbol, get_all_bots_for_trade, get_bot_by_client_oid, get_random_symbol,
    get_total_match_value_by_client_oid, insert_db_balance, insert_db_event, insert_db_msgsend,
    insert_db_orderevent, set_null_entry_client_oid_by_entry_client_oid,
    update_balance_bot_by_exit_sl_client_oid, update_balance_bot_by_exit_tp_client_oid,
    update_bot_balance_by_entry_client_oid, update_bot_entry_client_oid_by_id,
    update_exit_sl_client_oid_bot_by_entry_client_oid,
    update_exit_sl_client_oid_bot_by_exit_sl_order_id,
    update_exit_sl_order_id_bot_by_exit_sl_client_oid,
    update_exit_tp_client_oid_bot_by_entry_client_oid,
    update_exit_tp_client_oid_bot_by_exit_tp_order_id,
    update_exit_tp_order_id_bot_by_exit_tp_client_oid, upsert_position_asset, upsert_position_debt,
    upsert_position_ratio,
};
use crate::api::models::{
    AdvancedOrders, ApiV1MarketOrderbookLevel1ResData, ApiV3MarginRepayResData, BalanceData, Bot,
    Currencies, KuCoinMessage, MakeOrderResData, MarginAccountData, MessageData, OrderData,
    PositionData, Symbol,
};
use crate::api::requests::{
    api_v1_market_orderbook_level1_get, api_v3_accounts_universal_transfer_post,
    api_v3_hf_margin_order_post, api_v3_hf_margin_stop_order_cancel_by_client_oid_delete,
    api_v3_hf_margin_stop_order_post, api_v3_margin_accounts_get, api_v3_margin_repay_post,
    build_query_string, serialize_body,
};
use micromap::Map;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json;
use sqlx::PgPool;
use std::str::FromStr;
use tokio::time::{Duration, sleep};
use tracing::{error, info};
use uuid::Uuid;

fn tp_buy_percent() -> Result<Decimal, String> {
    // +7%
    Ok(Decimal::from_str("1.07").map_err(|e| {
        error!("{}", e);
        format!("{}", e)
    })?)
}

fn sl_buy_percent() -> Result<Decimal, String> {
    // -5%
    Ok(Decimal::from_str("0.95").map_err(|e| {
        error!("{}", e);
        format!("{}", e)
    })?)
}

fn tp_sell_percent() -> Result<Decimal, String> {
    // -7%
    Ok(Decimal::from_str("0.93").map_err(|e| {
        error!("{}", e);
        format!("{}", e)
    })?)
}

fn sl_sell_percent() -> Result<Decimal, String> {
    // +5%
    Ok(Decimal::from_str("1.05").map_err(|e| {
        error!("{}", e);
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
            error!("Fail parse increment:{} error:{}", increment, e);
            format!("Fail parse increment:{} error:{}", increment, e)
        })?;

        let size_int: i64 = size.to_string().parse().map_err(|e| {
            error!("Fail parse size:{} error:{}", size, e);
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

        let truncated = if fractional_part.len() >= precision {
            &fractional_part[..precision]
        } else {
            fractional_part
        };

        let trimmed = truncated.trim_end_matches('0');

        if trimmed.is_empty() {
            Ok(integer_part.to_string())
        } else {
            Ok(format!("{}.{}", integer_part, trimmed))
        }
    } else {
        if precision == 0 {
            Ok(s)
        } else {
            Ok(format!("{}.{}", s, "0".repeat(precision)))
        }
    }
}

pub async fn create_init_orders(pool: &PgPool) -> Result<(), String> {
    let trade_bots: Vec<Bot> = match get_all_bots_for_trade(pool).await {
        Ok(trade_bots) => trade_bots,
        Err(e) => {
            error!("{}", e);
            return Err(e);
        }
    };

    for trade_bot in trade_bots.iter() {
        sleep(BOT_INIT_DELAY).await;
        let token_funds: Decimal = match trade_bot.balance_decimal() {
            Ok(token_funds) => token_funds,
            Err(e) => {
                error!("{}", e);
                continue;
            }
        };
        match make_random_trade(pool, token_funds, trade_bot.id).await {
            Ok(_) => {}
            Err(e) => {
                error!("{}", e);
            }
        }
    }
    info!("All bots initialized!");
    Ok(())
}

pub async fn get_all_accounts_data() -> Result<MarginAccountData, String> {
    let mut query_params: Map<&str, &str, 8> = Map::new();
    query_params.insert("quoteCurrency", "USDT");
    query_params.insert("queryType", "MARGIN");

    Ok(api_v3_margin_accounts_get(build_query_string(query_params)).await?)
}

pub async fn repay_account(
    currency: &str,
    size: &str,
) -> Result<Option<ApiV3MarginRepayResData>, String> {
    info!("Repay {} liability:{}", size, currency);
    let body_str: String = serialize_body(Some(serde_json::json!({
        "currency": currency,
        "size": size,
        "isIsolated": false,
        "isHf": true
    })))
    .map_err(|e| {
        error!("{}", e);
        e
    })?;

    Ok(api_v3_margin_repay_post(body_str).await.map_err(|e| {
        error!("{}", e);
        e
    })?)
}

pub async fn get_token_price(
    trade_symbol: &str,
) -> Result<ApiV1MarketOrderbookLevel1ResData, String> {
    let mut query_params: Map<&str, &str, 8> = Map::new();
    query_params.insert("symbol", trade_symbol);

    let token_price: Option<ApiV1MarketOrderbookLevel1ResData> =
        api_v1_market_orderbook_level1_get(build_query_string(query_params)).await?;

    let Some(token_price) = token_price else {
        error!("Fail get token_price:{:?}", token_price);
        return Err("".to_string());
    };
    Ok(token_price)
}

pub async fn transfer_amount(currency: &str, amount: &str) -> Result<(), String> {
    let client_oid: String = Uuid::new_v4().to_string();

    let body_str: String = serialize_body(Some(serde_json::json!({
        "currency": currency,
        "clientOid": client_oid,
        "amount": amount,
        "type": "INTERNAL",
        "fromAccountType": "MARGIN",
        "toAccountType": "TRADE"
    })))
    .map_err(|e| {
        error!("{}", e);
        e
    })?;

    api_v3_accounts_universal_transfer_post(body_str)
        .await
        .map(|_| Ok(()))
        .map_err(|e| {
            error!("{}", e);
            e
        })?
}

pub async fn auto_clean_account(pool: &PgPool) -> Result<bool, String> {
    sleep(AUTO_CLEAN_DELAY).await;

    let accounts: MarginAccountData = get_all_accounts_data().await.map_err(|e| {
        error!("{}", e);
        e
    })?;

    let mut passed: bool = true;
    for account in accounts.accounts.iter() {
        let currency_info: Option<Currencies> =
            fetch_currency_info_by_symbol(pool, &account.currency)
                .await
                .map_err(|e| {
                    error!("{}", e);
                    e
                })?;

        let Some(currency_info) = currency_info else {
            error!("Currency info not found for {}", account.currency);
            return Err("".to_string());
        };

        let precision_decimal: Decimal = currency_info.precision_decimal().map_err(|e| {
            error!("{}", e);
            e
        })?;

        let symbol_info: Option<Symbol> = fetch_symbol_info_by_symbol(pool, &account.currency)
            .await
            .map_err(|e| {
                error!("{}", e);
                e
            })?;

        let Some(symbol_info) = symbol_info else {
            error!("Symbol info not found for {}", &account.currency);
            return Err("".to_string());
        };

        let quote_increment: Decimal = symbol_info.quote_increment_decimal().map_err(|e| {
            error!("{}", e);
            e
        })?;

        let min_funds: Decimal = symbol_info.min_funds_decimal().map_err(|e| {
            error!("{}", e);
            e
        })?;

        let base_min_size: Decimal = symbol_info.base_min_size_decimal().map_err(|e| {
            error!("{}", e);
            e
        })?;

        let base_increment: Decimal = symbol_info.base_increment_decimal().map_err(|e| {
            error!("{}", e);
            e
        })?;

        let quote_min_size: Decimal = symbol_info.quote_min_size_decimal().map_err(|e| {
            error!("{}", e);
            e
        })?;

        let token_liability: Decimal = account.liability_decimal().map_err(|e| {
            error!("{}", e);
            e
        })?;

        let token_available: Decimal = account.available_decimal().map_err(|e| {
            error!("{}", e);
            e
        })?;

        if token_liability > Decimal::ZERO {
            passed = false;
            if token_available >= token_liability {
                let size: String = format_assert_decimal(token_liability, precision_decimal)
                    .map_err(|e| {
                        error!("{}", e);
                        e
                    })?;

                repay_account(&account.currency, &size).await.map_err(|e| {
                    error!("{}", e);
                    e
                })?;
            } else if token_available > Decimal::ZERO {
                let size: String = format_assert_decimal(token_available, precision_decimal)
                    .map_err(|e| {
                        error!("{}", e);
                        e
                    })?;

                repay_account(&account.currency, &size).await.map_err(|e| {
                    error!("{}", e);
                    e
                })?;
            } else if account.currency != "USDT" && token_available == Decimal::ZERO {
                let trade_symbol: String = format!("{}-USDT", &account.currency);

                let token_price_data: ApiV1MarketOrderbookLevel1ResData =
                    get_token_price(&trade_symbol).await.map_err(|e| {
                        error!("{}", e);
                        e
                    })?;

                let best_ask_token_price: Decimal =
                    token_price_data.best_ask_decimal().map_err(|e| {
                        error!("{}", e);
                        e
                    })?;

                info!(
                    "Successfully get token:{} ask price:{}",
                    trade_symbol, best_ask_token_price
                );

                let token_funds: Decimal = best_ask_token_price * token_liability;

                let min_funds_by_size: Decimal = best_ask_token_price * base_min_size;

                let final_funds: Decimal = token_funds.max(min_funds_by_size).max(min_funds);

                let funds: String =
                    format_assert_decimal(final_funds, quote_increment).map_err(|e| {
                        error!("{}", e);
                        e
                    })?;

                let client_oid: String = Uuid::new_v4().to_string();

                make_hf_funds_margin_order(
                    pool,
                    &client_oid,
                    "buy",
                    &trade_symbol,
                    funds,
                    "market",
                    false,
                    false,
                )
                .await
                .map_err(|e| {
                    error!("{}", e);
                    e
                })?;
                continue;
            }
        } else if account.currency != "USDT" && token_available > Decimal::ZERO {
            passed = false;

            let trade_symbol: String = format!("{}-USDT", account.currency);

            let token_price_data: ApiV1MarketOrderbookLevel1ResData =
                match get_token_price(&trade_symbol).await {
                    Ok(token_price_data) => token_price_data,
                    Err(e) => {
                        error!("{}", e);
                        return Err(e);
                    }
                };

            let best_bid_token_price: Decimal = match token_price_data.best_bid_decimal() {
                Ok(best_bid_token_price) => best_bid_token_price,
                Err(e) => {
                    error!("{}", e);
                    return Err(e);
                }
            };

            info!(
                "Successfully get token:{} price:{}",
                &trade_symbol, best_bid_token_price
            );

            let token_funds: Decimal = best_bid_token_price * token_available;

            if token_available <= base_min_size || token_funds <= quote_min_size {
                let amount: String = format_assert_decimal(token_available, precision_decimal)
                    .map_err(|e| {
                        error!("{}", e);
                        e
                    })?;

                transfer_amount(&account.currency, &amount)
                    .await
                    .map_err(|e| {
                        error!("{}", e);
                        e
                    })?;
                continue;
            } else {
                let size: String =
                    format_assert_decimal(token_available, base_increment).map_err(|e| {
                        error!("{}", e);
                        e
                    })?;

                let client_oid: String = Uuid::new_v4().to_string();

                make_hf_size_margin_order(
                    pool,
                    &client_oid,
                    "sell",
                    &trade_symbol,
                    size,
                    "market",
                    false,
                    false,
                )
                .await
                .map_err(|e| {
                    error!("{}", e);
                    e
                })?;
            }
        }
    }
    Ok(passed)
}

pub async fn process_bot_by_exit_sl_client_oid(
    pool: &PgPool,
    bot: Bot,
    client_oid: &str,
    order: &OrderData,
) -> Result<(), String> {
    match delete_exit_sl_id_bot_by_client_oid(pool, client_oid).await {
        Err(e) => {
            error!("{}", e);
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
                    error!("{}", e);
                    return Err(e);
                }
            }
            let mut query_params: Map<&str, &str, 8> = Map::new();

            query_params.insert("clientOid", exit_tp_client_oid);

            match api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(build_query_string(
                query_params,
            ))
            .await
            {
                Ok(_) => {
                    info!("Successfully cancel stop order :{}", &exit_tp_client_oid);
                }
                Err(e) => {
                    error!("{}", e);
                    return Err(e);
                }
            }
        }
        None => {}
    }

    let return_balance: Option<String> =
        match get_total_match_value_by_client_oid(pool, client_oid).await {
            Ok(return_balance) => return_balance,
            Err(e) => {
                error!("{}", e);
                return Err(e);
            }
        };

    let Some(return_balance) = return_balance else {
        error!("No records found or error occurred");
        return Ok(());
    };

    let return_balance: Decimal = Decimal::from_str(&return_balance).map_err(|e| {
        error!("Fail parse return balance:{}", e);
        format!("Fail parse return balance:{}", e)
    })?;

    if order.side == "buy" {
        let old_balance = match bot.balance_decimal() {
            Ok(old_balance) => old_balance,
            Err(e) => {
                error!("{}", e);
                return Err(e);
            }
        };
        let new_balance = old_balance + old_balance - return_balance;
        match update_balance_bot_by_exit_sl_client_oid(
            pool,
            client_oid,
            &format!("{:.4}", new_balance),
        )
        .await
        {
            Ok(_) => {}
            Err(e) => {
                error!("{}", e);
                return Err(e);
            }
        }
        // create new random order
        make_random_trade(pool, new_balance, bot.id)
            .await
            .map_err(|e| {
                error!("{}", e);
                e
            })?;
    } else if order.side == "sell" {
        update_balance_bot_by_exit_sl_client_oid(
            pool,
            client_oid,
            &format!("{:.4}", return_balance),
        )
        .await
        .map_err(|e| {
            error!("{}", e);
            e
        })?;

        // create new random order
        make_random_trade(pool, return_balance, bot.id)
            .await
            .map_err(|e| {
                error!("{}", e);
                e
            })?;
    };
    Ok(())
}

pub async fn process_bot_by_exit_tp_client_oid(
    pool: &PgPool,
    bot: Bot,
    client_oid: &str,
    order: &OrderData,
) -> Result<(), String> {
    delete_exit_tp_id_bot_by_client_oid(pool, client_oid)
        .await
        .map_err(|e| {
            error!("{}", e);
            e
        })?;

    match &bot.exit_sl_client_oid {
        Some(exit_sl_client_oid) => {
            // clear exit_sl_client_oid in bots by id !!
            delete_exit_sl_id_bot_by_client_oid(pool, exit_sl_client_oid)
                .await
                .map_err(|e| {
                    error!("{}", e);
                    e
                })?;
            let mut query_params: Map<&str, &str, 8> = Map::new();

            query_params.insert("clientOid", exit_sl_client_oid);

            api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(build_query_string(
                query_params,
            ))
            .await
            .map_err(|e| {
                error!("{}", e);
                e
            })?;

            info!("Successfully cancel stop order :{}", &exit_sl_client_oid);
        }
        None => {}
    }
    let return_balance: Option<String> = get_total_match_value_by_client_oid(pool, client_oid)
        .await
        .map_err(|e| {
            error!("{}", e);
            e
        })?;

    let Some(return_balance) = return_balance else {
        error!("No records found or error occurred");
        return Ok(());
    };

    let return_balance: Decimal = Decimal::from_str(&return_balance).map_err(|e| {
        error!("Fail parse return balance:{}", e);
        format!("Fail parse return balance:{}", e)
    })?;

    if order.side == "buy" {
        let old_balance: Decimal = bot.balance_decimal().map_err(|e| {
            error!("{}", e);
            e
        })?;

        let new_balance: Decimal = old_balance + old_balance - return_balance;
        match update_balance_bot_by_exit_tp_client_oid(
            pool,
            client_oid,
            &format!("{:.4}", new_balance),
        )
        .await
        {
            Ok(_) => {}
            Err(e) => {
                error!("{}", e);
            }
        }
        // create new random order
        make_random_trade(pool, new_balance, bot.id)
            .await
            .map_err(|e| {
                error!("{}", e);
                e
            })?;
    } else if order.side == "sell" {
        update_balance_bot_by_exit_tp_client_oid(
            pool,
            client_oid,
            &format!("{:.4}", return_balance),
        )
        .await
        .map_err(|e| {
            error!("{}", e);
            e
        })?;

        // create new random order
        make_random_trade(pool, return_balance, bot.id)
            .await
            .map_err(|e| {
                error!("{}", e);
                e
            })?;
    };
    Ok(())
}

pub async fn process_bot_by_entry_client_oid(
    pool: &PgPool,
    client_oid: &str,
    order: &OrderData,
) -> Result<(), String> {
    let symbol_info: Option<Symbol> = fetch_symbol_info_by_symbol(pool, &order.symbol)
        .await
        .map_err(|e| {
            error!("{}", e);
            e
        })?;

    let Some(symbol_info) = symbol_info else {
        error!("Symbol info not found for {}", order.symbol);
        return Err("".to_string());
    };

    let price_increment: Decimal = symbol_info.price_increment_decimal().map_err(|e| {
        error!("{}", e);
        e
    })?;

    let quote_increment: Decimal = symbol_info.quote_increment_decimal().map_err(|e| {
        error!("{}", e);
        e
    })?;

    let filled_size: Decimal = order.filled_size_decimal().map_err(|e| {
        error!("{}", e);
        e
    })?;

    let return_balance: Option<String> = get_total_match_value_by_client_oid(pool, client_oid)
        .await
        .map_err(|e| {
            error!("{}", e);
            e
        })?;

    let Some(return_balance) = return_balance else {
        error!("No records found or error occurred");
        return Ok(());
    };

    let new_balance: Decimal = Decimal::from_str(&return_balance).map_err(|e| {
        error!("Fail parse return balance:{}", e);
        format!("Fail parse return balance:{}", e)
    })?;

    update_bot_balance_by_entry_client_oid(pool, client_oid, &format!("{:.4}", new_balance))
        .await
        .map_err(|e| {
            error!("{}", e);
            e
        })?;

    if order.side == "buy" {
        let tp_buy: Decimal = tp_buy_percent().map_err(|e| {
            error!("{}", e);
            e
        })?;

        let sl_buy: Decimal = sl_buy_percent().map_err(|e| {
            error!("{}", e);
            e
        })?;

        let match_price: Decimal = new_balance / filled_size;
        let trigger_tp_price: Decimal = match_price * tp_buy; // price + 7%
        let trigger_sl_price: Decimal = match_price * sl_buy; // price - 5%

        let exit_tp_client_oid: String = Uuid::new_v4().to_string();
        let exit_sl_client_oid: String = Uuid::new_v4().to_string();

        // tp order
        let stop_price_tp: String = format_assert_decimal(trigger_tp_price, price_increment)
            .map_err(|e| {
                error!(
                    "Fail parse:{} {} error:{}",
                    trigger_tp_price, price_increment, e
                );

                "".to_string()
            })?;

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
        let stop_price_sl: String = format_assert_decimal(trigger_sl_price, price_increment)
            .map_err(|e| {
                error!(
                    "Fail parse:{} {} error:{}",
                    trigger_sl_price, price_increment, e
                );

                "".to_string()
            })?;

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

        info!("Stop profit order:{}", msg_tp_order);
        info!("Stop loss order:{}", msg_sl_order);

        // add exit_tp_client_oid by entry_id
        update_exit_tp_client_oid_bot_by_entry_client_oid(pool, client_oid, &exit_tp_client_oid)
            .await
            .map_err(|e| {
                error!("{}", e);
                e
            })?;

        // add exit_sl_client_oid by entry_id
        update_exit_sl_client_oid_bot_by_entry_client_oid(pool, client_oid, &exit_sl_client_oid)
            .await
            .map_err(|e| {
                error!("{}", e);
                e
            })?;

        let msg_tp_order2: String = serialize_body(Some(msg_tp_order)).map_err(|e| {
            error!("{}", e);
            e
        })?;

        let tp_fut = api_v3_hf_margin_stop_order_post(msg_tp_order2);

        let msg_sl_order2: String = serialize_body(Some(msg_sl_order)).map_err(|e| {
            error!("{}", e);
            e
        })?;

        let sl_fut = api_v3_hf_margin_stop_order_post(msg_sl_order2);

        let (tp_res, sl_res) = tokio::join!(tp_fut, sl_fut);

        match (&tp_res, &sl_res) {
            (Ok(tp_resp), Ok(sl_resp)) => {
                match tp_resp {
                    Some(response_data) => match update_exit_tp_order_id_bot_by_exit_tp_client_oid(
                        pool,
                        &response_data.order_id,
                        &response_data.client_oid,
                    )
                    .await
                    {
                        Ok(_) => {}
                        Err(e) => {
                            error!("{}", e);
                            return Err(e);
                        }
                    },
                    None => {}
                }

                match sl_resp {
                    Some(response_data) => update_exit_sl_order_id_bot_by_exit_sl_client_oid(
                        pool,
                        &response_data.order_id,
                        &response_data.client_oid,
                    )
                    .await
                    .map_err(|e| {
                        error!("{}", e);
                        e
                    })?,
                    None => {}
                }

                info!(
                    "✅ Both stop orders created: TP={}, SL={}",
                    exit_tp_client_oid, exit_sl_client_oid
                );
            }
            (Err(tp_err), Ok(sl_resp)) => {
                match sl_resp {
                    Some(response_data) => {
                        let mut query_params: Map<&str, &str, 8> = Map::new();

                        query_params.insert("clientOid", &response_data.client_oid);

                        api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(
                            build_query_string(query_params),
                        )
                        .await
                        .map_err(|e| {
                            error!("{}", e);
                            e
                        })?;
                    }
                    None => {}
                }

                match delete_exit_sl_id_bot_by_client_oid(pool, &exit_sl_client_oid).await {
                    Ok(_) => {}
                    Err(e) => {
                        error!("{}", e);
                        return Err(e);
                    }
                }

                error!(
                    "Failed add TP order: {}. SL was cancelled for symmetry.",
                    tp_err
                );

                {}
            }
            (Ok(tp_resp), Err(sl_err)) => {
                match tp_resp {
                    Some(response_data) => {
                        let mut query_params: Map<&str, &str, 8> = Map::new();

                        query_params.insert("clientOid", &response_data.client_oid);

                        match api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(
                            build_query_string(query_params),
                        )
                        .await
                        {
                            Ok(_) => {}
                            Err(e) => {
                                error!("{}", e);
                                return Err(e);
                            }
                        }
                    }
                    None => {}
                }

                delete_exit_tp_id_bot_by_client_oid(pool, &exit_tp_client_oid)
                    .await
                    .map_err(|e| {
                        error!("{}", e);
                        e
                    })?;

                error!(
                    "Failed add SL order: {}. TP was cancelled for symmetry.",
                    sl_err
                );

                {}
            }
            (Err(tp_err), Err(sl_err)) => {
                error!("Failed add both stop orders: TP={}, SL={}", tp_err, sl_err);
                {}

                delete_symbol_bot_by_exit_sl_client_oid(pool, &exit_sl_client_oid)
                    .await
                    .map_err(|e| {
                        error!("{}", e);
                        e
                    })?;

                delete_exit_sl_id_bot_by_client_oid(pool, &exit_sl_client_oid)
                    .await
                    .map_err(|e| {
                        error!("{}", e);
                        e
                    })?;

                delete_exit_tp_id_bot_by_client_oid(pool, &exit_tp_client_oid)
                    .await
                    .map_err(|e| {
                        error!("{}", e);
                        e
                    })?;
            }
        }
    } else if order.side == "sell" {
        let tp_sell: Decimal = match tp_sell_percent() {
            Ok(tp_sell) => tp_sell,
            Err(e) => {
                error!("{}", e);
                return Err(e);
            }
        };

        let sl_sell: Decimal = match sl_sell_percent() {
            Ok(sl_sell) => sl_sell,
            Err(e) => {
                error!("{}", e);
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
                error!(
                    "Fail parse:{} {} error:{}",
                    trigger_tp_price, price_increment, e
                );

                return Err("".to_string());
            }
        };
        let funds_tp_str: String = match format_assert_decimal(funds_tp, quote_increment) {
            Ok(funds_tp_str) => funds_tp_str,
            Err(e) => {
                error!("Fail parse:{} {} error:{}", funds_tp, quote_increment, e);
                return Err("".to_string());
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
                error!(
                    "Fail parse:{} {} error:{}",
                    trigger_sl_price, price_increment, e
                );
                return Err("".to_string());
            }
        };
        let funds_sl_str: String =
            format_assert_decimal(funds_sl, quote_increment).map_err(|e| {
                error!("Fail parse:{} {} error:{}", funds_sl, quote_increment, e);
                "".to_string()
            })?;

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

        info!("Stop profit order:{}", msg_tp_order);
        info!("Stop loss order:{}", msg_sl_order);

        // add exit_tp_client_oid by entry_id
        match update_exit_tp_client_oid_bot_by_entry_client_oid(
            pool,
            client_oid,
            &exit_tp_client_oid,
        )
        .await
        {
            Ok(_) => {}
            Err(e) => {
                error!("{}", e);
                return Err(e);
            }
        }
        // add exit_sl_client_oid by entry_id
        match update_exit_sl_client_oid_bot_by_entry_client_oid(
            pool,
            client_oid,
            &exit_sl_client_oid,
        )
        .await
        {
            Ok(_) => {}
            Err(e) => {
                error!("{}", e);
                return Err(e);
            }
        }

        let msg_tp_order2: String = match serialize_body(Some(msg_tp_order)) {
            Ok(body_str) => body_str,
            Err(e) => {
                error!("{}", e);
                return Err(e);
            }
        };
        let tp_fut = api_v3_hf_margin_stop_order_post(msg_tp_order2);

        let msg_sl_order2: String = match serialize_body(Some(msg_sl_order)) {
            Ok(body_str) => body_str,
            Err(e) => {
                error!("{}", e);
                return Err(e);
            }
        };
        let sl_fut = api_v3_hf_margin_stop_order_post(msg_sl_order2);
        let (tp_res, sl_res) = tokio::join!(tp_fut, sl_fut);

        match (&tp_res, &sl_res) {
            (Ok(tp_resp), Ok(sl_resp)) => {
                match tp_resp {
                    Some(response_data) => match update_exit_tp_order_id_bot_by_exit_tp_client_oid(
                        pool,
                        &response_data.order_id,
                        &response_data.client_oid,
                    )
                    .await
                    {
                        Ok(_) => {}
                        Err(e) => {
                            error!("{}", e);
                            return Err(e);
                        }
                    },
                    None => {}
                }

                match sl_resp {
                    Some(response_data) => match update_exit_sl_order_id_bot_by_exit_sl_client_oid(
                        pool,
                        &response_data.order_id,
                        &response_data.client_oid,
                    )
                    .await
                    {
                        Ok(_) => {}
                        Err(e) => {
                            error!("{}", e);
                            return Err(e);
                        }
                    },
                    None => {}
                }

                info!(
                    "✅ Both stop orders created: TP={}, SL={}",
                    exit_tp_client_oid, exit_sl_client_oid
                );
            }
            (Err(tp_err), Ok(sl_resp)) => {
                match sl_resp {
                    Some(response_data) => {
                        let mut query_params: Map<&str, &str, 8> = Map::new();

                        query_params.insert("clientOid", &response_data.client_oid);
                        match api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(
                            build_query_string(query_params),
                        )
                        .await
                        {
                            Ok(_) => {}
                            Err(e) => {
                                error!("{}", e);
                                return Err(e);
                            }
                        }
                    }
                    None => {}
                }

                match delete_exit_sl_id_bot_by_client_oid(pool, &exit_sl_client_oid).await {
                    Ok(_) => {}
                    Err(e) => {
                        error!("{}", e);
                        return Err(e);
                    }
                }

                error!(
                    "Failed add TP order: {}. SL was cancelled for symmetry.",
                    tp_err
                );
            }
            (Ok(tp_resp), Err(sl_err)) => match tp_resp {
                Some(response_data) => {
                    let mut query_params: Map<&str, &str, 8> = Map::new();

                    query_params.insert("clientOid", &response_data.client_oid);
                    match api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(
                        build_query_string(query_params),
                    )
                    .await
                    {
                        Ok(_) => {
                            delete_exit_tp_id_bot_by_client_oid(pool, &exit_tp_client_oid)
                                .await
                                .map_err(|e| {
                                    error!(
                                        "Failed add SL order: {}. TP was cancelled for symmetry.",
                                        sl_err
                                    );
                                    error!("{}", e);
                                    e
                                })?;

                            {}
                        }
                        Err(e) => {
                            error!("{}", e);
                            return Err(e);
                        }
                    }
                }
                None => {}
            },
            (Err(tp_err), Err(sl_err)) => {
                error!("Failed add both stop orders: TP={}, SL={}", tp_err, sl_err);

                {}

                delete_symbol_bot_by_exit_sl_client_oid(pool, &exit_sl_client_oid)
                    .await
                    .map_err(|e| {
                        error!("{}", e);
                        e
                    })?;

                delete_exit_sl_id_bot_by_client_oid(pool, &exit_sl_client_oid)
                    .await
                    .map_err(|e| {
                        error!("{}", e);
                        e
                    })?;

                delete_exit_tp_id_bot_by_client_oid(pool, &exit_tp_client_oid)
                    .await
                    .map_err(|e| {
                        error!("{}", e);
                        e
                    })?;
            }
        }
    }

    // delete entry_id from db
    match set_null_entry_client_oid_by_entry_client_oid(pool, client_oid).await {
        Ok(_) => Ok(()),
        Err(e) => {
            error!("{}", e);
            Ok(())
        }
    }
}

pub async fn trade_order_event(pool: &PgPool, order: &OrderData) -> Result<(), String> {
    let client_oid: &String = match &order.client_oid {
        Some(client_oid) => client_oid,
        None => {
            error!("client_oid in order is none: {}", order);
            return Err("".to_string());
        }
    };

    let bot: Option<Bot> = match get_bot_by_client_oid(pool, client_oid).await {
        Err(e) => {
            error!("{}", e);
            return Err(e);
        }
        Ok(bot) => bot,
    };

    let Some(bot) = bot else {
        let msg: String = format!("Bot is None by:{}", client_oid);
        error!("{}", msg);
        return Err(msg);
    };

    match client_oid.as_str() {
        s if Some(s.to_string()) == bot.entry_client_oid => {
            process_bot_by_entry_client_oid(pool, client_oid, order)
                .await
                .map(|_| Ok(()))
                .map_err(|e| e)?
        }
        s if Some(s.to_string()) == bot.exit_tp_client_oid => {
            process_bot_by_exit_tp_client_oid(pool, bot, client_oid, order)
                .await
                .map(|_| Ok(()))
                .map_err(|e| e)?
        }
        s if Some(s.to_string()) == bot.exit_sl_client_oid => {
            process_bot_by_exit_sl_client_oid(pool, bot, client_oid, order)
                .await
                .map(|_| Ok(()))
                .map_err(|e| e)?
        }
        _ => {
            let msg: String = format!("don't find client_oid in:{}", order);
            error!("{}", msg);
            Err(msg)
        }
    }
}

pub async fn handle_trade_order_event(order: OrderData, pool: &PgPool) -> Result<(), String> {
    insert_db_orderevent(pool, order.clone())
        .await
        .map_err(|e| {
            error!("{}", e);
            e
        })?;

    info!("{}", order);

    if (order.type_ == "match" || order.type_ == "canceled")
        && (order.remain_size == Some("0".to_string())
            || order.remain_funds == Some("0".to_string()))
    {
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
        let Some(asset_info) = position.asset_list.get(&asset) else {
            error!("Failed get asset:{} from:{:.?}", asset, position.asset_list);
            continue;
        };

        let token_available: Decimal = asset_info.available_decimal().map_err(|e| {
            error!("{}", e);
            e
        })?;

        if token_liability > Decimal::ZERO && token_available > Decimal::ZERO {
            let currency_info = fetch_currency_info_by_symbol(pool, &asset)
                .await
                .map_err(|e| {
                    error!("{}", e);
                    e
                })?;

            let Some(currency_info) = currency_info else {
                error!("Currency info not found for {}", asset);
                return Err("".to_string());
            };

            let precision_decimal: Decimal = currency_info.precision_decimal().map_err(|e| {
                error!("{}", e);
                e
            })?;

            let size: String =
                format_assert_decimal(token_liability.min(token_available), precision_decimal)
                    .map_err(|e| {
                        error!("{}", e);
                        e
                    })?;

            repay_account(&asset, &size).await.map_err(|e| {
                error!("{}", e);
                e
            })?;
        }
    }

    match upsert_position_ratio(
        pool,
        position.debt_ratio,
        position.total_asset,
        &position.margin_coefficient_total_asset,
        &position.total_debt,
    )
    .await
    {
        Ok(_) => {}
        Err(e) => {
            error!("{}", e);
            return Err(e);
        }
    };

    for (symbol, amount) in &position.debt_list {
        upsert_position_debt(pool, symbol, amount)
            .await
            .map_err(|e| {
                error!("{}", e);
                e
            })?;
    }
    for (symbol, symbol_info) in &position.asset_list {
        upsert_position_asset(
            pool,
            symbol,
            &symbol_info.total,
            &symbol_info.available,
            &symbol_info.hold,
        )
        .await
        .map_err(|e| {
            error!("{}", e);
            e
        })?
    }

    Ok(())
}

pub async fn handle_advanced_orders(order: AdvancedOrders, pool: &PgPool) -> Result<(), String> {
    info!("{}", order);
    match order.error {
        Some(_) => {}
        None => return Ok(()),
    }

    error!("Got error on stop order : {}", order);

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
                match update_exit_sl_client_oid_bot_by_exit_sl_order_id(
                    pool,
                    order_id_ref,
                    &new_exit_client_oid,
                )
                .await
                {
                    Ok(_) => match side_ref.as_str() {
                        "buy" => match funds_clone {
                            Some(funds) => {
                                make_hf_funds_margin_order(
                                    pool,
                                    &new_exit_client_oid,
                                    side_ref,
                                    symbol_ref,
                                    funds,
                                    "market",
                                    true,
                                    false,
                                )
                                .await
                            }
                            None => {
                                error!(
                                    "Fail parse funds order:{} new_exit_sl_client_oid:{} funds_clone:{:.?}",
                                    order_id_ref, new_exit_client_oid, funds_clone,
                                );
                                continue;
                            }
                        },
                        "sell" => match size_clone {
                            Some(size) => {
                                make_hf_size_margin_order(
                                    pool,
                                    &new_exit_client_oid,
                                    side_ref,
                                    symbol_ref,
                                    size,
                                    "market",
                                    true,
                                    false,
                                )
                                .await
                            }
                            None => {
                                error!(
                                    "Fail parse size order:{} new_exit_sl_client_oid:{} size_clone:{:.?}",
                                    order_id_ref, new_exit_client_oid, size_clone,
                                );
                                continue;
                            }
                        },
                        _ => {
                            error!("Fail match side_clone:{}", side_ref);
                            continue;
                        }
                    },
                    Err(e) => {
                        error!("{}", e);
                        continue;
                    }
                }
            }
            "entry" => {
                // need find tp
                match update_exit_tp_client_oid_bot_by_exit_tp_order_id(
                    pool,
                    order_id_ref,
                    &new_exit_client_oid,
                )
                .await
                {
                    Ok(_) => match side_ref.as_str() {
                        "buy" => match funds_clone {
                            Some(funds) => {
                                make_hf_funds_margin_order(
                                    pool,
                                    &new_exit_client_oid,
                                    side_ref,
                                    symbol_ref,
                                    funds,
                                    "market",
                                    true,
                                    false,
                                )
                                .await
                            }
                            None => {
                                error!(
                                    "Fail parse funds_clone order:{} new_exit_tp_client_oid:{} funds_clone:{:.?}",
                                    order_id_ref, new_exit_client_oid, funds_clone
                                );

                                continue;
                            }
                        },
                        "sell" => match size_clone {
                            Some(size) => {
                                make_hf_size_margin_order(
                                    pool,
                                    &new_exit_client_oid,
                                    side_ref,
                                    symbol_ref,
                                    size,
                                    "market",
                                    true,
                                    false,
                                )
                                .await
                            }
                            None => {
                                error!(
                                    "Fail parse size_clone order:{} new_exit_tp_client_oid:{} size_clone:{:.?}",
                                    order_id_ref, new_exit_client_oid, size_clone
                                );
                                continue;
                            }
                        },
                        _ => {
                            error!("Fail match side_clone:{}", side_ref);
                            continue;
                        }
                    },
                    Err(e) => {
                        error!("{}", e);
                        continue;
                    }
                }
            }
            _ => {
                error!("Fail match stop_clone:{}", stop_ref);
                continue;
            }
        };

        match order_result {
            Ok(_) => {
                info!(
                    "✅ Order re-placed: {} {} (attempt {}/{})",
                    order_id_ref, new_exit_client_oid, attempt, MAX_RETRIES
                );
                break Ok(());
            }
            Err(e) => {
                error!(
                    "❌ Order failed: {} {} (attempt {}/{}) {}",
                    order_id_ref, new_exit_client_oid, attempt, MAX_RETRIES, e
                );
                return Err("".to_string());
            }
        }
    }
}

pub async fn process_kcn_msg(pool: &PgPool, msg: &str) -> Result<(), String> {
    let event: KuCoinMessage = serde_json::from_str::<KuCoinMessage>(msg).map_err(|e| {
        error!("Failed to parse message:{} {}", msg, e);
        "".to_string()
    })?;

    let data: MessageData = match event {
        KuCoinMessage::Welcome(data) => match serde_json::to_value(&data) {
            Ok(data) => {
                insert_db_event(pool, &data).await.map_err(|e| {
                    error!("{}", e);
                    e
                })?;
                return Ok(());
            }
            Err(e) => {
                error!(
                    "Failed to serialize request '{:?}' as {}: {}",
                    &data,
                    stringify!(WelcomeData),
                    e
                );
                return Err("".to_string());
            }
        },
        KuCoinMessage::Message(data) => data,
        KuCoinMessage::Ack(data) => match serde_json::to_value(&data) {
            Ok(data) => match insert_db_event(pool, &data).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    error!("{}", e);
                    return Err(e);
                }
            },
            Err(e) => {
                error!(
                    "Failed to serialize request '{:?}' as {}: {}",
                    &data,
                    stringify!(AckData),
                    e
                );
                return Err("".to_string());
            }
        },
        KuCoinMessage::Error(data) => {
            error!("Got error in WS {:?}", data);
            return Err("".to_string());
        }

        KuCoinMessage::Unknown => {
            error!("Unknown WS message type");
            return Err("".to_string());
        }
    };

    match data.topic.as_str() {
        "/account/balance" => match BalanceData::deserialize(&data.data) {
            Err(e) => {
                error!(
                    "Failed to serialize request '{:?}' as {}: {}",
                    &data.data,
                    stringify!(BalanceData),
                    e
                );
                return Err("".to_string());
            }
            Ok(balance) => match insert_db_balance(pool, balance).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    error!("{}", e);
                    return Err(e);
                }
            },
        },
        "/spotMarket/tradeOrdersV2" => match OrderData::deserialize(&data.data) {
            Err(e) => {
                error!(
                    "Failed to serialize request '{:?}' as {}: {}",
                    &data.data,
                    stringify!(OrderData),
                    e
                );
                return Err("".to_string());
            }
            Ok(order) => Ok(handle_trade_order_event(order, pool).await.map_err(|e| {
                error!("{}", e);
                e
            })?),
        },
        "/spotMarket/advancedOrders" => {
            let order = AdvancedOrders::deserialize(&data.data).map_err(|e| {
                error!(
                    "Failed to serialize request '{:?}' as {}: {}",
                    &data.data,
                    stringify!(AdvancedOrders),
                    e
                );
                "".to_string()
            })?;
            handle_advanced_orders(order, pool)
                .await
                .map(|_| Ok(()))
                .map_err(|e| {
                    error!("{}", e);
                    e
                })?
        }
        "/margin/position" => match PositionData::deserialize(&data.data) {
            Err(e) => {
                error!(
                    "Failed to serialize request '{:?}' as {}: {}",
                    &data.data,
                    stringify!(PositionData),
                    e
                );
                return Err("".to_string());
            }
            Ok(position) => handle_position_event(position, pool)
                .await
                .map(|_| Ok(()))
                .map_err(|e| {
                    error!("{}", e);
                    e
                })?,
        },
        _ => {
            error!("Unknown topic: {}", data.topic);
            return Err("".to_string());
        }
    }
}

pub async fn make_random_trade(
    pool: &PgPool,
    balance_funds: Decimal,
    trade_bot_id: i32,
) -> Result<(), String> {
    const MAX_RETRIES: u32 = 10;
    let mut attempt: u32 = 0;

    loop {
        if attempt >= MAX_RETRIES {
            return Ok(());
        }
        sleep(Duration::from_millis(RETRY_DELAY_BASE * attempt as u64)).await;
        attempt += 1;

        let tradeable_symbol: Option<String> = get_random_symbol(pool).await.map_err(|e| {
            error!("{}", e);
            e
        })?;

        let Some(tradeable_symbol) = tradeable_symbol else {
            error!("Failed get_random_symbol:");
            continue;
        };

        let symbol_info: Option<Symbol> = fetch_symbol_info_by_symbol(pool, &tradeable_symbol)
            .await
            .map_err(|e| {
                error!("{}", e);
                e
            })?;

        let Some(symbol_info) = symbol_info else {
            error!("Symbol info not found for {}", tradeable_symbol);
            continue;
        };

        let entry_client_oid: String = Uuid::new_v4().to_string();

        match update_bot_entry_client_oid_by_id(
            pool,
            Some(&tradeable_symbol),
            Some(&entry_client_oid),
            trade_bot_id,
        )
        .await
        {
            Ok(_) => {}
            Err(e) => {
                error!("{}", e);
                continue;
            }
        }

        let order_result = match get_random_side() {
            "sell" => {
                let base_increment = symbol_info.base_increment_decimal().map_err(|e| {
                    error!("{}", e);
                    e
                })?;

                let mut query_params: Map<&str, &str, 8> = Map::new();

                query_params.insert("symbol", &tradeable_symbol);

                let token_price_obj =
                    api_v1_market_orderbook_level1_get(build_query_string(query_params))
                        .await
                        .map_err(|e| {
                            error!("{}", e);
                            e
                        })?;

                let Some(token_price_obj) = token_price_obj else {
                    return Err("".to_string());
                };

                let token_price = token_price_obj.price_decimal().map_err(|e| {
                    error!("{}", e);
                    e
                })?;

                let token_size = balance_funds / token_price;
                let size = format_assert_decimal(token_size, base_increment).map_err(|e| {
                    error!("Fail parse:{} {} error:{}", token_size, base_increment, e);
                    e
                })?;

                make_hf_size_margin_order(
                    pool,
                    &entry_client_oid,
                    "sell",
                    &tradeable_symbol,
                    size,
                    "market",
                    true,
                    false,
                )
                .await
            }
            "buy" => {
                let quote_increment = symbol_info.quote_increment_decimal().map_err(|e| {
                    error!("{}", e);
                    e
                })?;

                let funds = format_assert_decimal(balance_funds, quote_increment).map_err(|e| {
                    error!(
                        "Fail parse:{} {} error:{}",
                        balance_funds, quote_increment, e
                    );
                    e
                })?;

                make_hf_funds_margin_order(
                    pool,
                    &entry_client_oid,
                    "buy",
                    &tradeable_symbol,
                    funds,
                    "market",
                    true,
                    false,
                )
                .await
            }
            _ => {
                continue;
            }
        };

        match order_result {
            Ok(_) => {
                info!(
                    "✅ Order placed: {} {} (attempt {}/{})",
                    entry_client_oid, trade_bot_id, attempt, MAX_RETRIES
                );
                return Ok(());
            }
            Err(e) => {
                update_bot_entry_client_oid_by_id(pool, None, None, trade_bot_id)
                    .await
                    .map_err(|e| {
                        error!("{}", e);
                        e
                    })?;

                error!(
                    "❌ Order failed (attempt {}/{}): {} {}",
                    attempt, MAX_RETRIES, tradeable_symbol, e
                );

                continue;
            }
        }
    }
}

pub async fn spawn_process_kcn_msg(pool: &PgPool, mut rx_in: tokio::sync::mpsc::Receiver<String>) {
    loop {
        let Some(msg) = rx_in.recv().await else {
            error!("Channel closed, exiting message processor");
            break;
        };

        match process_kcn_msg(pool, &msg).await {
            Ok(_) => {}
            Err(e) => {
                error!("{}", e);
                ()
            }
        }
    }
    info!("Message processor stopped");
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

    insert_db_msgsend(
        pool,
        Some(symbol),
        Some(side),
        None,
        Some(&funds),
        None,
        Some(args_time_in_force),
        Some(type_),
        Some(&auto_borrow),
        Some(&auto_repay),
        Some(client_oid),
        None,
    )
    .await
    .map_err(|e| {
        error!("{}", e);
        e
    })?;

    let msg = serde_json::json!({
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

    let body_str: String = serialize_body(Some(msg)).map_err(|e| {
        error!("{}", e);
        e
    })?;

    let data = api_v3_hf_margin_order_post(body_str).await.map_err(|e| {
        error!("{}", e);
        e
    })?;
    let Some(data) = data else {
        return Err("".to_string());
    };
    Ok(data)
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

    insert_db_msgsend(
        pool,
        Some(symbol),
        Some(side),
        Some(&size),
        None,
        None,
        Some(args_time_in_force),
        Some(type_),
        Some(&auto_borrow),
        Some(&auto_repay),
        Some(client_oid),
        None,
    )
    .await
    .map_err(|e| {
        error!("{}", e);
        e
    })?;

    let body_str = serialize_body(Some(serde_json::json!({
        "clientOid": client_oid,
        "symbol": symbol,
        "side": side,
        "type": type_,
        "autoBorrow": auto_borrow,
        "autoRepay": auto_repay,
        "timeInForce": args_time_in_force,
        "size": size
    })))
    .map_err(|e| {
        error!("{}", e);
        e
    })?;

    let data = api_v3_hf_margin_order_post(body_str).await.map_err(|e| {
        error!("{}", e);
        e
    })?;
    let Some(data) = data else {
        return Err("".to_string());
    };
    Ok(data)
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
        assert_eq!(
            format_assert_decimal(Decimal::from_str("1234.56").unwrap(), inc_1000).unwrap(),
            "1000".to_string()
        );

        // Increment = 100 (precision 0)
        let inc_100: Decimal = Decimal::from_str("100").unwrap();
        assert_eq!(
            format_assert_decimal(Decimal::from_str("123.456").unwrap(), inc_100).unwrap(),
            "100".to_string()
        );
        assert_eq!(
            format_assert_decimal(Decimal::from_str("199").unwrap(), inc_100).unwrap(),
            "100".to_string()
        );
        assert_eq!(
            format_assert_decimal(Decimal::from_str("200").unwrap(), inc_100).unwrap(),
            "200".to_string()
        );

        // Increment = 50 (precision 0)
        let inc_50: Decimal = Decimal::from_str("50").unwrap();
        assert_eq!(
            format_assert_decimal(Decimal::from_str("123.456").unwrap(), inc_50).unwrap(),
            "100".to_string()
        );
        assert_eq!(
            format_assert_decimal(Decimal::from_str("149").unwrap(), inc_50).unwrap(),
            "100".to_string()
        );
        assert_eq!(
            format_assert_decimal(Decimal::from_str("150").unwrap(), inc_50).unwrap(),
            "150".to_string()
        );

        // Increment = 10 (precision 0)
        let inc_10: Decimal = Decimal::from_str("10").unwrap();
        assert_eq!(
            format_assert_decimal(Decimal::from_str("123.456").unwrap(), inc_10).unwrap(),
            "120".to_string()
        );
        assert_eq!(
            format_assert_decimal(Decimal::from_str("125").unwrap(), inc_10).unwrap(),
            "120".to_string()
        );

        // Increment = 1 (precision 0)
        let inc_1: Decimal = Decimal::from_str("1").unwrap();
        assert_eq!(
            format_assert_decimal(Decimal::from_str("123.456").unwrap(), inc_1).unwrap(),
            "123".to_string()
        );
        assert_eq!(
            format_assert_decimal(Decimal::from_str("100").unwrap(), inc_1).unwrap(),
            "100".to_string()
        );

        // Increment = 0.1 (precision 1)
        let inc_1: Decimal = Decimal::from_str("0.1").unwrap();
        assert_eq!(
            format_assert_decimal(Decimal::from_str("123.456").unwrap(), inc_1).unwrap(),
            "123.4".to_string()
        );
        assert_eq!(
            format_assert_decimal(Decimal::from_str("99.999").unwrap(), inc_1).unwrap(),
            "99.9".to_string()
        );

        // Increment = 0.01 (precision 2)
        let inc_2: Decimal = Decimal::from_str("0.01").unwrap();
        assert_eq!(
            format_assert_decimal(Decimal::from_str("123.456").unwrap(), inc_2).unwrap(),
            "123.45".to_string()
        );
        assert_eq!(
            format_assert_decimal(Decimal::from_str("99.999").unwrap(), inc_2).unwrap(),
            "99.99".to_string()
        );

        // Increment = 0.001 (precision 3)
        let inc_3: Decimal = Decimal::from_str("0.001").unwrap();
        assert_eq!(
            format_assert_decimal(Decimal::from_str("123.4567").unwrap(), inc_3).unwrap(),
            "123.456".to_string()
        );

        // Increment = 0.0001 (precision 4)
        let inc_4: Decimal = Decimal::from_str("0.0001").unwrap();
        assert_eq!(
            format_assert_decimal(Decimal::from_str("123.45678").unwrap(), inc_4).unwrap(),
            "123.4567".to_string()
        );

        // Increment = 0.0001 (precision 5)
        let inc_5: Decimal = Decimal::from_str("0.00001").unwrap();
        assert_eq!(
            format_assert_decimal(Decimal::from_str("123.45678").unwrap(), inc_5).unwrap(),
            "123.45678".to_string()
        );

        // Increment = 0.000001 (precision 6)
        let inc_6: Decimal = Decimal::from_str("0.000001").unwrap();
        assert_eq!(
            format_assert_decimal(Decimal::from_str("123.456789").unwrap(), inc_6).unwrap(),
            "123.456789".to_string()
        );

        // Increment = 0.0000001 (precision 7)
        let inc_7: Decimal = Decimal::from_str("0.0000001").unwrap();
        assert_eq!(
            format_assert_decimal(Decimal::from_str("123.4567891").unwrap(), inc_7).unwrap(),
            "123.4567891".to_string()
        );

        // Increment = 0.00000001 (precision 8)
        let inc_8: Decimal = Decimal::from_str("0.00000001").unwrap();
        assert_eq!(
            format_assert_decimal(Decimal::from_str("0.123456789").unwrap(), inc_8).unwrap(),
            "0.12345678".to_string()
        );

        // Increment = 0.000000001 (precision 9)
        let inc_9: Decimal = Decimal::from_str("0.000000001").unwrap();
        assert_eq!(
            format_assert_decimal(Decimal::from_str("0.00000000123").unwrap(), inc_9).unwrap(),
            "0.000000001".to_string()
        );
    }
}
