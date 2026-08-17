use crate::api::models::{
    AdvancedOrders, ApiV1MarketOrderbookLevel1ResData, ApiV3MarginRepayResData, BalanceData, Bot,
    KuCoinMessage, MakeOrderResData, MarginAccountData, OrderData, PositionData,
};
use crate::api::repository::{
    BalanceRepository, BotRepository, ErrorRepository, EventRepository, MessageRepository,
    OrderRepository, PositionRepository, SymbolRepository,
};
use crate::api::requests::{
    api_v1_market_orderbook_level1_get, api_v3_accounts_universal_transfer_post,
    api_v3_hf_margin_order_post, api_v3_hf_margin_stop_order_cancel_by_client_oid_delete,
    api_v3_hf_margin_stop_order_post, api_v3_margin_accounts_get, api_v3_margin_repay_post,
    build_query_string, serialize_body,
};
use anyhow::{Context, Result};
use bytes::Bytes;
use micromap::Map;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use serde::Deserialize;
use serde_json;
use sqlx::PgPool;
use std::str::FromStr;
use tokio::time::{Duration, sleep};
use tracing::{error, info};
use uuid::Uuid;

fn tp_buy_percent() -> Result<Decimal> {
    Ok(Decimal::from_str("1.07").map_err(|e| anyhow::anyhow!(e))?)
}

fn sl_buy_percent() -> Result<Decimal> {
    Ok(Decimal::from_str("0.95").map_err(|e| anyhow::anyhow!(e))?)
}

fn tp_sell_percent() -> Result<Decimal> {
    Ok(Decimal::from_str("0.93").map_err(|e| anyhow::anyhow!(e))?)
}

fn sl_sell_percent() -> Result<Decimal> {
    Ok(Decimal::from_str("1.05").map_err(|e| anyhow::anyhow!(e))?)
}

fn get_random_side() -> &'static str {
    if fastrand::bool() { "buy" } else { "sell" }
}

const RETRY_DELAY_BASE: u64 = 500;
const BOT_INIT_DELAY: Duration = Duration::from_secs(5);
const AUTO_CLEAN_DELAY: Duration = Duration::from_secs(5);

pub fn format_assert_decimal(size: Decimal, increment: Decimal) -> Result<String> {
    let precision = increment.scale() as usize;

    if precision == 0 {
        let size_int = size
            .floor()
            .to_i64()
            .with_context(|| format!("Fail convert size:{}", size))?;
        let increment_int = increment
            .to_i64()
            .with_context(|| format!("Fail convert increment:{}", increment))?;

        let rounded_down = (size_int / increment_int) * increment_int;
        return Ok(rounded_down.to_string());
    }

    let factor = Decimal::from(10_u64.pow(precision as u32));
    let result = (size * factor).floor() / factor;

    Ok(result.normalize().to_string())
}

pub async fn create_init_orders(pool: &PgPool) -> Result<()> {
    let bot_repo = BotRepository::new(pool.clone());

    let trade_bots = match bot_repo.get_all().await {
        Ok(trade_bots) => trade_bots,
        Err(e) => {
            error!("{:#}", e);
            return Err(e);
        }
    };

    for trade_bot in trade_bots.iter() {
        sleep(BOT_INIT_DELAY).await;
        let token_funds = match trade_bot.balance_decimal() {
            Ok(token_funds) => token_funds,
            Err(e) => {
                error!("{:#}", e);
                continue;
            }
        };
        match make_random_trade(pool, token_funds, trade_bot.id).await {
            Ok(_) => {}
            Err(e) => {
                error!("{:#}", e);
            }
        }
    }
    info!("All bots initialized!");
    Ok(())
}

pub async fn get_all_accounts_data() -> Result<MarginAccountData> {
    let mut query_params: Map<&str, &str, 8> = Map::new();
    query_params.insert("quoteCurrency", "USDT");
    query_params.insert("queryType", "MARGIN");

    Ok(api_v3_margin_accounts_get(&build_query_string(query_params)?).await?)
}

pub async fn repay_account(currency: &str, size: &str) -> Result<Option<ApiV3MarginRepayResData>> {
    info!("Repay {} liability:{}", size, currency);
    let body_str = serialize_body(Some(serde_json::json!({
        "currency": currency,
        "size": size,
        "isIsolated": false,
        "isHf": true
    })))?;

    Ok(api_v3_margin_repay_post(&body_str).await?)
}

pub async fn get_token_price(trade_symbol: &str) -> Result<ApiV1MarketOrderbookLevel1ResData> {
    let mut query_params: Map<&str, &str, 8> = Map::new();
    query_params.insert("symbol", trade_symbol);

    let token_price =
        api_v1_market_orderbook_level1_get(&build_query_string(query_params)?).await?;

    match token_price {
        Some(token_price) => Ok(token_price),
        None => {
            anyhow::bail!("Fail get token_price:{:?}", token_price)
        }
    }
}

pub async fn transfer_in_account(
    currency: &str,
    amount: &str,
    type_: &str,
    from_account_type: &str,
    to_account_type: &str,
) -> Result<()> {
    let body_str = serialize_body(Some(serde_json::json!({
        "currency": currency,
        "clientOid":  Uuid::new_v4().to_string(),
        "amount": amount,
        "type": type_,
        "fromAccountType": from_account_type,
        "toAccountType": to_account_type,
    })))?;

    let result = match api_v3_accounts_universal_transfer_post(&body_str).await {
        Ok(result) => result,
        Err(e) => {
            anyhow::bail!(
                "Fail transfer {} from {} to {} with {} {:#}",
                currency,
                from_account_type,
                to_account_type,
                amount,
                e,
            )
        }
    };

    match result {
        Some(result) => {
            info!(
                "Success transfer {} from {} to {} with {} with id:{}",
                currency, from_account_type, to_account_type, amount, result.order_id,
            )
        }
        None => {
            anyhow::bail!(
                "None transfer {} from {} to {} with {}",
                currency,
                from_account_type,
                to_account_type,
                amount,
            )
        }
    };
    Ok(())
}

pub async fn auto_clean_account(pool: &PgPool) -> Result<bool> {
    let symbol_repo = SymbolRepository::new(pool.clone());

    let mut passed = true;
    for account in get_all_accounts_data().await?.accounts.iter() {
        let token_liability = account.liability_decimal()?;
        let token_available = account.available_decimal()?;

        if token_liability > Decimal::ZERO || token_available > Decimal::ZERO {
            let currency_info = match symbol_repo.get_currency_info(&account.currency).await? {
                Some(currency_info) => {
                    info!("Get currency info:{}", &account.currency);
                    currency_info
                }
                None => anyhow::bail!("Currency info not found for {}", account.currency),
            };

            let precision_decimal = currency_info.precision_decimal()?;

            if account.currency == "USDT" {
                if token_liability > Decimal::ZERO {
                    if token_available >= token_liability {
                        let size = &format_assert_decimal(token_liability, precision_decimal)?;
                        match repay_account(&account.currency, size).await {
                            Ok(_) => {}
                            Err(e) => {
                                error!("{:#}", e);
                                anyhow::bail!(e);
                            }
                        };
                    } else if token_available > Decimal::ZERO {
                        let size = &format_assert_decimal(token_available, precision_decimal)?;
                        match repay_account(&account.currency, size).await {
                            Ok(_) => {}
                            Err(e) => {
                                error!("{:#}", e);
                                anyhow::bail!(e);
                            }
                        };
                    };
                    passed = false;
                };
                continue;
            }

            let trade_symbol = format!("{}-USDT", &account.currency);
            let symbol_info = match symbol_repo.get_symbol_info(&trade_symbol).await? {
                Some(symbol_info) => {
                    info!("Get symbol info:{}", &account.currency);
                    symbol_info
                }
                None => {
                    anyhow::bail!("Symbol info not found for {}", &account.currency)
                }
            };
            if token_liability > Decimal::ZERO {
                if token_available > Decimal::ZERO {
                    if token_available >= token_liability {
                        let size = &format_assert_decimal(token_liability, precision_decimal)?;
                        match repay_account(&account.currency, size).await {
                            Ok(_) => {}
                            Err(e) => {
                                error!("{:#}", e);
                                anyhow::bail!(e);
                            }
                        };
                    } else if token_available > Decimal::ZERO {
                        let size = &format_assert_decimal(token_available, precision_decimal)?;
                        match repay_account(&account.currency, size).await {
                            Ok(_) => {}
                            Err(e) => {
                                error!("{:#}", e);
                                anyhow::bail!(e);
                            }
                        };
                    };
                } else {
                    let quote_increment = symbol_info.quote_increment_decimal()?;
                    let base_min_size = symbol_info.base_min_size_decimal()?;
                    let min_funds = symbol_info.min_funds_decimal()?;

                    let best_ask_token_price = match get_token_price(&trade_symbol).await {
                        Ok(best_ask_token_price) => best_ask_token_price,
                        Err(e) => {
                            error!("{:#}", e);
                            anyhow::bail!(e);
                        }
                    };

                    let best_ask_token_price = best_ask_token_price.best_ask_decimal()?;

                    info!(
                        "Get token ask price:{} {:?}",
                        &trade_symbol, best_ask_token_price
                    );

                    let token_funds = best_ask_token_price * token_liability;
                    let min_funds_by_size = best_ask_token_price * base_min_size;

                    let size = format_assert_decimal(
                        token_funds.max(min_funds_by_size).max(min_funds),
                        quote_increment,
                    )?;

                    match make_hf_funds_margin_order(
                        pool,
                        &Uuid::new_v4().to_string(),
                        "buy",
                        &trade_symbol,
                        &size,
                        "market",
                        false,
                        false,
                    )
                    .await
                    {
                        Ok(_) => {
                            info!("Buy by market {} on size {}", &trade_symbol, size);
                        }
                        Err(e) => {
                            error!("{:#}", e);
                            anyhow::bail!(e);
                        }
                    };
                }
                passed = false;
            } else if token_available > Decimal::ZERO {
                let base_min_size = symbol_info.base_min_size_decimal()?;
                let quote_min_size = symbol_info.quote_min_size_decimal()?;
                let base_increment = symbol_info.base_increment_decimal()?;

                let best_bid_token_price = match get_token_price(&trade_symbol).await {
                    Ok(best_bid_token_price) => best_bid_token_price,
                    Err(e) => {
                        error!("{:#}", e);
                        anyhow::bail!(e);
                    }
                };

                let best_bid_token_price = best_bid_token_price.best_bid_decimal()?;

                info!(
                    "Get token bid price:{} {:?}",
                    &trade_symbol, best_bid_token_price
                );

                let token_funds = best_bid_token_price * token_available;

                if token_available >= base_min_size && token_funds >= quote_min_size {
                    let size = format_assert_decimal(token_available, base_increment)?;
                    match make_hf_size_margin_order(
                        pool,
                        &Uuid::new_v4().to_string(),
                        "sell",
                        &trade_symbol,
                        &size,
                        "market",
                        false,
                        false,
                    )
                    .await
                    {
                        Ok(_) => {
                            info!("Sell by market {} on size {}", &trade_symbol, size);
                        }
                        Err(e) => {
                            error!("{:#}", e);
                            anyhow::bail!(e);
                        }
                    };
                } else {
                    let amount = format_assert_decimal(token_available, precision_decimal)?;
                    let type_ = "INTERNAL";
                    let from_account_type = "MARGIN";
                    let to_account_type = "TRADE";

                    match transfer_in_account(
                        &account.currency,
                        &amount,
                        type_,
                        from_account_type,
                        to_account_type,
                    )
                    .await
                    {
                        Ok(_) => {
                            info!(
                                "Success transfer {} {} {} {} {}",
                                &account.currency,
                                amount,
                                type_,
                                from_account_type,
                                to_account_type
                            )
                        }
                        Err(e) => {
                            error!("{:#}", e);
                            anyhow::bail!(e);
                        }
                    };
                }
                passed = false;
            }
        }
    }
    sleep(AUTO_CLEAN_DELAY).await;
    Ok(passed)
}

pub async fn process_bot_by_exit_sl_client_oid(
    pool: &PgPool,
    bot: Bot,
    client_oid: &str,
    order: &OrderData,
) -> Result<()> {
    let bot_repo = BotRepository::new(pool.clone());
    let order_repo = OrderRepository::new(pool.clone());
    bot_repo.clear_exit_sl_by_client_oid(client_oid).await?;
    match &bot.exit_tp_client_oid {
        Some(exit_tp_client_oid) => {
            bot_repo
                .clear_exit_tp_by_client_oid(exit_tp_client_oid)
                .await?;
            let mut query_params: Map<&str, &str, 8> = Map::new();

            query_params.insert("clientOid", exit_tp_client_oid);

            match api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(&build_query_string(
                query_params,
            )?)
            .await
            {
                Ok(_) => {
                    info!("Successfully cancel stop order :{}", &exit_tp_client_oid);
                }
                Err(e) => {
                    error!("{:#}", e);
                    return Err(e);
                }
            }
        }
        None => {}
    }

    let return_balance = order_repo
        .get_total_match_value_by_client_oid(client_oid)
        .await?;
    let return_balance = match return_balance {
        Some(return_balance) => return_balance,
        None => {
            error!("No records found or error occurred");
            return Ok(());
        }
    };

    let return_balance = Decimal::from_str(&return_balance).map_err(|e| anyhow::anyhow!(e))?;

    if order.side == "buy" {
        let old_balance = match bot.balance_decimal() {
            Ok(old_balance) => old_balance,
            Err(e) => {
                error!("{:#}", e);
                return Err(e);
            }
        };
        let new_balance = old_balance + old_balance - return_balance;
        let bot_repo = BotRepository::new(pool.clone());
        match bot_repo
            .update_balance_by_entry_client_oid(client_oid, &format!("{:.4}", new_balance))
            .await
        {
            Ok(_) => {}
            Err(e) => {
                error!("{:#}", e);
                return Err(e);
            }
        }

        make_random_trade(pool, new_balance, bot.id).await?;
    } else if order.side == "sell" {
        bot_repo
            .update_balance_and_clear_symbol_by_exit_sl(
                client_oid,
                &format!("{:.4}", return_balance),
            )
            .await?;

        make_random_trade(pool, return_balance, bot.id).await?;
    };
    Ok(())
}

pub async fn process_bot_by_exit_tp_client_oid(
    pool: &PgPool,
    bot: Bot,
    client_oid: &str,
    order: &OrderData,
) -> Result<()> {
    let bot_repo = BotRepository::new(pool.clone());
    let order_repo = OrderRepository::new(pool.clone());

    bot_repo.clear_exit_tp_by_client_oid(client_oid).await?;

    match &bot.exit_sl_client_oid {
        Some(exit_sl_client_oid) => {
            bot_repo
                .clear_exit_sl_by_client_oid(exit_sl_client_oid)
                .await?;
            let mut query_params: Map<&str, &str, 8> = Map::new();

            query_params.insert("clientOid", exit_sl_client_oid);

            api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(&build_query_string(
                query_params,
            )?)
            .await?;

            info!("Successfully cancel stop order :{}", &exit_sl_client_oid);
        }
        None => {}
    }
    let return_balance = order_repo
        .get_total_match_value_by_client_oid(client_oid)
        .await?;

    let return_balance = match return_balance {
        Some(return_balance) => return_balance,
        None => {
            error!("No records found or error occurred");
            return Ok(());
        }
    };

    let return_balance = Decimal::from_str(&return_balance).map_err(|e| anyhow::anyhow!(e))?;

    if order.side == "buy" {
        let old_balance = bot.balance_decimal()?;
        let new_balance = old_balance + old_balance - return_balance;

        bot_repo
            .update_balance_and_clear_symbol_by_exit_tp(client_oid, &format!("{:.4}", new_balance))
            .await?;

        make_random_trade(pool, new_balance, bot.id).await?;
    } else if order.side == "sell" {
        bot_repo
            .update_balance_and_clear_symbol_by_exit_tp(
                client_oid,
                &format!("{:.4}", return_balance),
            )
            .await?;

        make_random_trade(pool, return_balance, bot.id).await?;
    };
    Ok(())
}

pub async fn process_bot_by_entry_client_oid(
    pool: &PgPool,
    client_oid: &str,
    order: &OrderData,
) -> Result<()> {
    let bot_repo = BotRepository::new(pool.clone());
    let order_repo = OrderRepository::new(pool.clone());
    let symbol_repo = SymbolRepository::new(pool.clone());

    let symbol_info = match symbol_repo.get_symbol_info(&order.symbol).await? {
        Some(symbol_info) => symbol_info,
        None => {
            anyhow::bail!("Symbol info not found for {}", order.symbol)
        }
    };

    let price_increment = symbol_info.price_increment_decimal()?;
    let quote_increment = symbol_info.quote_increment_decimal()?;
    let filled_size = order.filled_size_decimal()?;

    let return_balance = match order_repo
        .get_total_match_value_by_client_oid(client_oid)
        .await?
    {
        Some(return_balance) => return_balance,
        None => {
            error!("No records found or error occurred");
            return Ok(());
        }
    };

    let new_balance = Decimal::from_str(&return_balance).map_err(|e| anyhow::anyhow!(e))?;

    bot_repo
        .update_balance_by_entry_client_oid(client_oid, &format!("{:.4}", new_balance))
        .await?;

    if order.side == "buy" {
        let tp_buy = tp_buy_percent()?;
        let sl_buy = sl_buy_percent()?;

        let match_price = new_balance / filled_size;
        let trigger_tp_price = match_price * tp_buy;
        let trigger_sl_price = match_price * sl_buy;

        let exit_tp_client_oid = Uuid::new_v4().to_string();
        let exit_sl_client_oid = Uuid::new_v4().to_string();

        let stop_price_tp = format_assert_decimal(trigger_tp_price, price_increment)?;

        let msg_tp_order = serde_json::json!({
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
        let stop_price_sl = format_assert_decimal(trigger_sl_price, price_increment)?;

        let msg_sl_order = serde_json::json!({
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

        bot_repo
            .update_exit_tp_client_oid_by_entry_client_oid(client_oid, &exit_tp_client_oid)
            .await?;

        bot_repo
            .update_exit_sl_client_oid_by_entry_client_oid(client_oid, &exit_sl_client_oid)
            .await?;

        let msg_tp_order2 = serialize_body(Some(msg_tp_order))?;

        let tp_fut = api_v3_hf_margin_stop_order_post(&msg_tp_order2);

        let msg_sl_order2 = serialize_body(Some(msg_sl_order))?;

        let sl_fut = api_v3_hf_margin_stop_order_post(&msg_sl_order2);

        let (tp_res, sl_res) = tokio::join!(tp_fut, sl_fut);

        match (&tp_res, &sl_res) {
            (Ok(tp_resp), Ok(sl_resp)) => {
                match tp_resp {
                    Some(response_data) => {
                        let bot_repo = BotRepository::new(pool.clone());
                        match bot_repo
                            .update_exit_tp_order_id_by_client_oid(
                                &response_data.order_id,
                                &response_data.client_oid,
                            )
                            .await
                        {
                            Ok(_) => {}
                            Err(e) => {
                                error!("{:#}", e);
                                return Err(e);
                            }
                        }
                    }
                    None => {}
                }

                match sl_resp {
                    Some(response_data) => {
                        let bot_repo = BotRepository::new(pool.clone());
                        bot_repo
                            .update_exit_sl_order_id_by_client_oid(
                                &response_data.order_id,
                                &response_data.client_oid,
                            )
                            .await?;
                    }
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
                            &build_query_string(query_params)?,
                        )
                        .await?;
                    }
                    None => {}
                }
                let bot_repo = BotRepository::new(pool.clone());

                match bot_repo
                    .clear_exit_sl_by_client_oid(&exit_sl_client_oid)
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        error!("{:#}", e);
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
                            &build_query_string(query_params)?,
                        )
                        .await
                        {
                            Ok(_) => {}
                            Err(e) => {
                                error!("{:#}", e);
                                return Err(e);
                            }
                        }
                    }
                    None => {}
                }

                let bot_repo = BotRepository::new(pool.clone());

                bot_repo
                    .clear_exit_tp_by_client_oid(&exit_tp_client_oid)
                    .await?;

                error!(
                    "Failed add SL order: {}. TP was cancelled for symmetry.",
                    sl_err
                );

                {}
            }
            (Err(tp_err), Err(sl_err)) => {
                error!("Failed add both stop orders: TP={}, SL={}", tp_err, sl_err);
                {}

                let bot_repo = BotRepository::new(pool.clone());

                bot_repo
                    .clear_exit_sl_by_client_oid(&exit_sl_client_oid)
                    .await?;
                bot_repo
                    .clear_exit_tp_by_client_oid(&exit_tp_client_oid)
                    .await?;
                bot_repo.clear_entry_client_oid(&exit_tp_client_oid).await?;
            }
        }
    } else if order.side == "sell" {
        let tp_sell = match tp_sell_percent() {
            Ok(tp_sell) => tp_sell,
            Err(e) => {
                error!("{:#}", e);
                return Err(e);
            }
        };

        let sl_sell = match sl_sell_percent() {
            Ok(sl_sell) => sl_sell,
            Err(e) => {
                error!("{:#}", e);
                return Err(e);
            }
        };

        let match_price = new_balance / filled_size;
        let trigger_tp_price = match_price * tp_sell;
        let trigger_sl_price = match_price * sl_sell;

        let funds_tp = trigger_tp_price * filled_size;
        let funds_sl = trigger_sl_price * filled_size;

        let exit_tp_client_oid = Uuid::new_v4().to_string();
        let exit_sl_client_oid = Uuid::new_v4().to_string();

        let stop_price_tp = match format_assert_decimal(trigger_tp_price, price_increment) {
            Ok(stop_price_tp) => stop_price_tp,
            Err(e) => {
                anyhow::bail!(
                    "Fail parse:{} {} error:{}",
                    trigger_tp_price,
                    price_increment,
                    e
                )
            }
        };
        let funds_tp_str = match format_assert_decimal(funds_tp, quote_increment) {
            Ok(funds_tp_str) => funds_tp_str,
            Err(e) => {
                anyhow::bail!("Fail parse:{} {} error:{}", funds_tp, quote_increment, e)
            }
        };
        let msg_tp_order = serde_json::json!({
            "clientOid": exit_tp_client_oid,
            "side": "buy",
            "symbol": order.symbol,
            "type": "market",
            "stop": "loss",
            "stopPrice": stop_price_tp,
            "isIsolated": false,
            "autoBorrow": true,
            "autoRepay": false,
            "timeInForce": "GTC",
            "funds":funds_tp_str,
        });
        let stop_price_sl = match format_assert_decimal(trigger_sl_price, price_increment) {
            Ok(stop_price_sl) => stop_price_sl,
            Err(e) => {
                anyhow::bail!(
                    "Fail parse:{} {} error:{}",
                    trigger_sl_price,
                    price_increment,
                    e
                )
            }
        };
        let funds_sl_str = format_assert_decimal(funds_sl, quote_increment)?;

        let msg_sl_order = serde_json::json!({
            "clientOid": exit_sl_client_oid,
            "side": "buy",
            "symbol": order.symbol,
            "type": "market",
            "stop": "entry",
            "stopPrice": stop_price_sl,
            "isIsolated": false,
            "autoBorrow": true,
            "autoRepay": false,
            "timeInForce": "GTC",
            "funds": funds_sl_str,
        });

        info!("Stop profit order:{}", msg_tp_order);
        info!("Stop loss order:{}", msg_sl_order);

        let bot_repo = BotRepository::new(pool.clone());
        match bot_repo
            .update_exit_tp_order_id_by_client_oid(client_oid, &exit_tp_client_oid)
            .await
        {
            Ok(_) => {}
            Err(e) => {
                error!("{:#}", e);
                return Err(e);
            }
        }
        let bot_repo = BotRepository::new(pool.clone());
        match bot_repo.clear_exit_sl_by_client_oid(client_oid).await {
            Ok(_) => {}
            Err(e) => {
                error!("{:#}", e);
                return Err(e);
            }
        }

        let msg_tp_order2 = match serialize_body(Some(msg_tp_order)) {
            Ok(body_str) => body_str,
            Err(e) => {
                error!("{:#}", e);
                return Err(e);
            }
        };
        let tp_fut = api_v3_hf_margin_stop_order_post(&msg_tp_order2);

        let msg_sl_order2 = match serialize_body(Some(msg_sl_order)) {
            Ok(body_str) => body_str,
            Err(e) => {
                error!("{:#}", e);
                return Err(e);
            }
        };
        let sl_fut = api_v3_hf_margin_stop_order_post(&msg_sl_order2);
        let (tp_res, sl_res) = tokio::join!(tp_fut, sl_fut);

        match (&tp_res, &sl_res) {
            (Ok(tp_resp), Ok(sl_resp)) => {
                match tp_resp {
                    Some(response_data) => {
                        let bot_repo = BotRepository::new(pool.clone());
                        match bot_repo
                            .update_exit_tp_order_id_by_client_oid(
                                &response_data.order_id,
                                &response_data.client_oid,
                            )
                            .await
                        {
                            Ok(_) => {}
                            Err(e) => {
                                error!("{:#}", e);
                                return Err(e);
                            }
                        }
                    }
                    None => {}
                }

                match sl_resp {
                    Some(response_data) => {
                        let bot_repo = BotRepository::new(pool.clone());

                        match bot_repo
                            .update_exit_sl_order_id_by_client_oid(
                                &response_data.order_id,
                                &response_data.client_oid,
                            )
                            .await
                        {
                            Ok(_) => {}
                            Err(e) => {
                                error!("{:#}", e);
                                return Err(e);
                            }
                        }
                    }
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
                            &build_query_string(query_params)?,
                        )
                        .await
                        {
                            Ok(_) => {}
                            Err(e) => {
                                error!("{:#}", e);
                                return Err(e);
                            }
                        }
                    }
                    None => {}
                }

                let bot_repo = BotRepository::new(pool.clone());

                match bot_repo
                    .clear_exit_sl_by_client_oid(&exit_sl_client_oid)
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        error!("{:#}", e);
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
                        &build_query_string(query_params)?,
                    )
                    .await
                    {
                        Ok(_) => {
                            let bot_repo = BotRepository::new(pool.clone());
                            bot_repo
                                .clear_exit_tp_by_client_oid(&exit_tp_client_oid)
                                .await?;

                            {}
                        }
                        Err(e) => {
                            error!("{:#}", e);
                            return Err(e);
                        }
                    }
                }
                None => {}
            },
            (Err(tp_err), Err(sl_err)) => {
                error!("Failed add both stop orders: TP={}, SL={}", tp_err, sl_err);

                {}

                let bot_repo = BotRepository::new(pool.clone());

                bot_repo
                    .clear_exit_sl_by_client_oid(&exit_sl_client_oid)
                    .await?;
                bot_repo
                    .clear_exit_tp_by_client_oid(&exit_tp_client_oid)
                    .await?;
                bot_repo.clear_entry_client_oid(&exit_tp_client_oid).await?;
            }
        }
    }

    bot_repo.clear_entry_client_oid(client_oid).await?;
    Ok(())
}

pub async fn trade_order_event(pool: &PgPool, order: &OrderData) -> Result<()> {
    let client_oid = match &order.client_oid {
        Some(client_oid) => client_oid,
        None => {
            anyhow::bail!("client_oid in order is none: {}", order)
        }
    };

    let bot_repo = BotRepository::new(pool.clone());
    let bot = bot_repo.get_by_client_oid(client_oid).await?;
    let bot = match bot {
        Some(bot) => bot,
        None => {
            anyhow::bail!("Bot is None by:{}", client_oid)
        }
    };

    match client_oid.as_str() {
        s if Some(s.to_string()) == bot.entry_client_oid => {
            process_bot_by_entry_client_oid(pool, client_oid, order)
                .await
                .map(|_| Ok(()))?
        }
        s if Some(s.to_string()) == bot.exit_tp_client_oid => {
            process_bot_by_exit_tp_client_oid(pool, bot, client_oid, order)
                .await
                .map(|_| Ok(()))?
        }
        s if Some(s.to_string()) == bot.exit_sl_client_oid => {
            process_bot_by_exit_sl_client_oid(pool, bot, client_oid, order)
                .await
                .map(|_| Ok(()))?
        }
        _ => {
            anyhow::bail!("don't find client_oid in:{}", order)
        }
    }
}

pub async fn handle_trade_order_event(order: OrderData, pool: &PgPool) -> Result<()> {
    let order_repo = OrderRepository::new(pool.clone());
    order_repo.save_order_event(order.clone()).await?;

    info!("{}", order);

    if (order.type_ == "match" || order.type_ == "canceled")
        && (order.remain_size == Some("0".to_string())
            || order.remain_funds == Some("0".to_string()))
    {
        trade_order_event(pool, &order).await?
    }
    Ok(())
}

pub async fn handle_position_event(position: PositionData, pool: &PgPool) -> Result<()> {
    let position_repo = PositionRepository::new(pool.clone());
    let symbol_repo = SymbolRepository::new(pool.clone());

    for (asset, token_liability) in position.debt_pairs()? {
        let asset_info = match position.asset_list.get(&asset) {
            Some(asset_info) => asset_info,
            None => {
                error!("Failed get asset:{} from:{:.?}", asset, position.asset_list);
                continue;
            }
        };

        let token_available = asset_info.available_decimal()?;

        if token_liability > Decimal::ZERO && token_available > Decimal::ZERO {
            let currency_info = match symbol_repo.get_currency_info(&asset).await? {
                Some(currency_info) => currency_info,
                None => {
                    anyhow::bail!("Currency info not found for {}", asset)
                }
            };

            let precision_decimal = currency_info.precision_decimal()?;

            let size =
                format_assert_decimal(token_liability.min(token_available), precision_decimal)?;

            match repay_account(&asset, &size).await {
                Ok(_) => {
                    info!("Repay {} size {}", &asset, size);
                }
                Err(e) => {
                    error!("{:#}", e);
                    anyhow::bail!(e);
                }
            };
        }
    }

    position_repo
        .upsert_position_ratio(
            position.debt_ratio,
            position.total_asset,
            &position.margin_coefficient_total_asset,
            &position.total_debt,
        )
        .await?;

    for (symbol, amount) in &position.debt_list {
        position_repo.upsert_position_debt(symbol, amount).await?;
    }
    for (symbol, symbol_info) in &position.asset_list {
        position_repo
            .upsert_position_asset(
                symbol,
                &symbol_info.total,
                &symbol_info.available,
                &symbol_info.hold,
            )
            .await?;
    }

    Ok(())
}

pub async fn handle_advanced_orders(order: AdvancedOrders, pool: &PgPool) -> Result<()> {
    if order.error.is_none() {
        return Ok(());
    }
    error!("Got error on stop order : {}", order);

    let bot_repo = BotRepository::new(pool.clone());

    const MAX_RETRIES: u32 = 1000;
    let mut attempt = 0;

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
        let funds_clone = order.funds.clone();
        let size_clone = order.size.clone();
        let new_exit_client_oid = Uuid::new_v4().to_string();

        let order_result = match stop_ref.as_str() {
            "loss" => {
                match bot_repo
                    .update_exit_sl_client_oid_by_order_id(order_id_ref, &new_exit_client_oid)
                    .await
                {
                    Ok(_) => match side_ref.as_str() {
                        "buy" => {
                            let funds = match funds_clone {
                                Some(funds) => funds,
                                None => {
                                    anyhow::bail!(
                                        "Fail parse funds order:{} new_exit_sl_client_oid:{} funds_clone:{:.?}",
                                        order_id_ref,
                                        new_exit_client_oid,
                                        funds_clone,
                                    );
                                }
                            };

                            make_hf_funds_margin_order(
                                pool,
                                &new_exit_client_oid,
                                side_ref,
                                symbol_ref,
                                &funds,
                                "market",
                                true,
                                false,
                            )
                            .await
                        }
                        "sell" => {
                            let size = match size_clone {
                                Some(size) => size,
                                None => {
                                    anyhow::bail!(
                                        "Fail parse size order:{} new_exit_sl_client_oid:{} size_clone:{:.?}",
                                        order_id_ref,
                                        new_exit_client_oid,
                                        size_clone,
                                    )
                                }
                            };

                            make_hf_size_margin_order(
                                pool,
                                &new_exit_client_oid,
                                side_ref,
                                symbol_ref,
                                &size,
                                "market",
                                true,
                                false,
                            )
                            .await
                        }
                        _ => {
                            error!("Fail match side_clone:{}", side_ref);
                            continue;
                        }
                    },
                    Err(e) => {
                        error!("{:#}", e);
                        continue;
                    }
                }
            }
            "entry" => {
                match bot_repo
                    .update_exit_tp_client_oid_by_order_id(order_id_ref, &new_exit_client_oid)
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
                                    &funds,
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
                                    &size,
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
                        error!("{:#}", e);
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
                anyhow::bail!(
                    "❌ Order failed: {} {} (attempt {}/{}) {}",
                    order_id_ref,
                    new_exit_client_oid,
                    attempt,
                    MAX_RETRIES,
                    e
                )
            }
        }
    }
}

pub async fn process_kcn_msg(pool: &PgPool, msg: &str) -> Result<()> {
    let data = match serde_json::from_str::<KuCoinMessage>(msg)? {
        KuCoinMessage::Message(data) => data,
        KuCoinMessage::Welcome(data) => match serde_json::to_value(&data) {
            Ok(data) => {
                let event_repo = EventRepository::new(pool.clone());
                event_repo.save_event(&data).await?;
                return Ok(());
            }
            Err(e) => {
                anyhow::bail!(
                    "Failed to serialize request '{:?}' as {}: {}",
                    &data,
                    stringify!(WelcomeData),
                    e
                )
            }
        },
        KuCoinMessage::Ack(data) => match serde_json::to_value(&data) {
            Ok(data) => {
                let event_repo = EventRepository::new(pool.clone());
                match event_repo.save_event(&data).await {
                    Ok(_) => return Ok(()),
                    Err(e) => {
                        error!("{:#}", e);
                        return Err(e);
                    }
                }
            }

            Err(e) => {
                anyhow::bail!(
                    "Failed to serialize request '{:?}' as {}: {}",
                    &data,
                    stringify!(AckData),
                    e
                )
            }
        },
        KuCoinMessage::Error(data) => {
            anyhow::bail!("Got error in WS {:?}", data)
        }
        KuCoinMessage::Unknown => {
            anyhow::bail!("Unknown WS message type");
        }
    };

    match data.topic.as_str() {
        "/account/balance" => {
            let data = match BalanceData::deserialize(&data.data) {
                Ok(data) => data,
                Err(e) => {
                    anyhow::bail!(e);
                }
            };
            let balance_repo = BalanceRepository::new(pool.clone());
            match balance_repo.save_balance_event(data).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    anyhow::bail!(e);
                }
            }
        }
        "/spotMarket/tradeOrdersV2" => {
            let data = match OrderData::deserialize(&data.data) {
                Ok(data) => data,
                Err(e) => {
                    anyhow::bail!(e);
                }
            };
            match handle_trade_order_event(data, pool).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    anyhow::bail!(e);
                }
            }
        }
        "/spotMarket/advancedOrders" => {
            let data = match AdvancedOrders::deserialize(&data.data) {
                Ok(data) => data,
                Err(e) => {
                    anyhow::bail!(e);
                }
            };
            match handle_advanced_orders(data, pool).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    anyhow::bail!(e);
                }
            }
        }
        "/margin/position" => {
            let data = match PositionData::deserialize(&data.data) {
                Ok(data) => data,
                Err(e) => {
                    anyhow::bail!(e);
                }
            };
            match handle_position_event(data, pool).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    anyhow::bail!(e);
                }
            }
        }
        _ => {
            anyhow::bail!("Unknown topic: {}", data.topic)
        }
    }
}

pub async fn make_random_trade(
    pool: &PgPool,
    balance_funds: Decimal,
    trade_bot_id: i32,
) -> Result<()> {
    let bot_repo = BotRepository::new(pool.clone());
    let symbol_repo = SymbolRepository::new(pool.clone());

    const MAX_RETRIES: u32 = 10;
    let mut attempt = 0;

    loop {
        if attempt >= MAX_RETRIES {
            return Ok(());
        }
        sleep(Duration::from_millis(RETRY_DELAY_BASE * attempt as u64)).await;
        attempt += 1;

        let tradeable_symbol = match symbol_repo.get_random_symbol().await? {
            Some(tradeable_symbol) => tradeable_symbol,
            None => {
                error!("Failed get_random_symbol:");
                continue;
            }
        };

        let symbol_info = match symbol_repo.get_symbol_info(&tradeable_symbol).await? {
            Some(symbol_info) => symbol_info,
            None => {
                error!("Symbol info not found for {}", tradeable_symbol);
                continue;
            }
        };

        let entry_client_oid = Uuid::new_v4().to_string();

        bot_repo
            .update_entry_client_oid_by_id(
                Some(&tradeable_symbol),
                Some(&entry_client_oid),
                trade_bot_id,
            )
            .await?;

        let order_result = match get_random_side() {
            "sell" => {
                let base_increment = symbol_info.base_increment_decimal()?;

                let mut query_params: Map<&str, &str, 8> = Map::new();
                query_params.insert("symbol", &tradeable_symbol);

                let token_price =
                    match api_v1_market_orderbook_level1_get(&build_query_string(query_params)?)
                        .await?
                    {
                        Some(token_price) => token_price,
                        None => {
                            anyhow::bail!("")
                        }
                    };

                let token_price = token_price.price_decimal()?;

                let token_size = balance_funds / token_price;
                let size = format_assert_decimal(token_size, base_increment)
                    .with_context(|| format!("Fail parse:{} {}", token_size, base_increment,))?;

                make_hf_size_margin_order(
                    pool,
                    &entry_client_oid,
                    "sell",
                    &tradeable_symbol,
                    &size,
                    "market",
                    true,
                    false,
                )
                .await
            }
            "buy" => {
                let quote_increment = symbol_info.quote_increment_decimal()?;

                let funds = format_assert_decimal(balance_funds, quote_increment)
                    .with_context(|| format!("Fail parse:{} {}", balance_funds, quote_increment))?;

                make_hf_funds_margin_order(
                    pool,
                    &entry_client_oid,
                    "buy",
                    &tradeable_symbol,
                    &funds,
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
                bot_repo
                    .update_entry_client_oid_by_id(None, None, trade_bot_id)
                    .await?;

                error!(
                    "❌ Order failed (attempt {}/{}): {} {}",
                    attempt, MAX_RETRIES, tradeable_symbol, e
                );

                continue;
            }
        }
    }
}

pub async fn spawn_process_kcn_msg(pool: &PgPool, mut rx_in: tokio::sync::mpsc::Receiver<Bytes>) {
    loop {
        let msg = match rx_in.recv().await {
            Some(msg) => msg,
            None => {
                error!("Message processor stopped - channel closed");
                break;
            }
        };

        let text = match String::from_utf8(msg.to_vec()) {
            Ok(text) => text,
            Err(e) => {
                error!("Failed to convert Bytes to UTF-8 string: {}", e);
                continue;
            }
        };

        match process_kcn_msg(pool, &text).await {
            Ok(_) => {}
            Err(e) => {
                error!("{:#}", e)
            }
        };
    }
}

pub async fn make_hf_funds_margin_order(
    pool: &PgPool,
    client_oid: &str,
    side: &str,
    symbol: &str,
    funds: &str,
    type_: &'static str,
    auto_borrow: bool,
    auto_repay: bool,
) -> Result<MakeOrderResData> {
    let args_time_in_force = "GTC";

    let message_repo = MessageRepository::new(pool.clone());

    message_repo
        .save_order_message(
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
        .await?;

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

    let body_str = serialize_body(Some(msg))?;

    let data = api_v3_hf_margin_order_post(&body_str).await?;
    let data = match data {
        Some(data) => data,
        None => {
            anyhow::bail!("")
        }
    };

    Ok(data)
}

pub async fn make_hf_size_margin_order(
    pool: &PgPool,
    client_oid: &str,
    side: &str,
    symbol: &str,
    size: &str,
    type_: &'static str,
    auto_borrow: bool,
    auto_repay: bool,
) -> Result<MakeOrderResData> {
    let args_time_in_force = "GTC";

    let message_repo = MessageRepository::new(pool.clone());

    message_repo
        .save_order_message(
            Some(symbol),
            Some(side),
            Some(size),
            None,
            None,
            Some(args_time_in_force),
            Some(type_),
            Some(&auto_borrow),
            Some(&auto_repay),
            Some(client_oid),
            None,
        )
        .await?;

    let body_str = serialize_body(Some(serde_json::json!({
        "clientOid": client_oid,
        "symbol": symbol,
        "side": side,
        "type": type_,
        "autoBorrow": auto_borrow,
        "autoRepay": auto_repay,
        "timeInForce": args_time_in_force,
        "size": size
    })))?;

    let data = api_v3_hf_margin_order_post(&body_str).await?;
    let data = match data {
        Some(data) => data,
        None => {
            anyhow::bail!("")
        }
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
        let inc_1000 = Decimal::from_str("1000").unwrap();
        assert_eq!(
            format_assert_decimal(Decimal::from_str("1234.56").unwrap(), inc_1000).unwrap(),
            "1000".to_string()
        );

        let inc_100 = Decimal::from_str("100").unwrap();
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

        let inc_50 = Decimal::from_str("50").unwrap();
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

        let inc_10 = Decimal::from_str("10").unwrap();
        assert_eq!(
            format_assert_decimal(Decimal::from_str("123.456").unwrap(), inc_10).unwrap(),
            "120".to_string()
        );
        assert_eq!(
            format_assert_decimal(Decimal::from_str("125").unwrap(), inc_10).unwrap(),
            "120".to_string()
        );

        let inc_1 = Decimal::from_str("1").unwrap();
        assert_eq!(
            format_assert_decimal(Decimal::from_str("123.456").unwrap(), inc_1).unwrap(),
            "123".to_string()
        );
        assert_eq!(
            format_assert_decimal(Decimal::from_str("100").unwrap(), inc_1).unwrap(),
            "100".to_string()
        );

        let inc_1 = Decimal::from_str("0.1").unwrap();
        assert_eq!(
            format_assert_decimal(Decimal::from_str("123.456").unwrap(), inc_1).unwrap(),
            "123.4".to_string()
        );
        assert_eq!(
            format_assert_decimal(Decimal::from_str("99.999").unwrap(), inc_1).unwrap(),
            "99.9".to_string()
        );

        let inc_2 = Decimal::from_str("0.01").unwrap();
        assert_eq!(
            format_assert_decimal(Decimal::from_str("123.456").unwrap(), inc_2).unwrap(),
            "123.45".to_string()
        );
        assert_eq!(
            format_assert_decimal(Decimal::from_str("99.999").unwrap(), inc_2).unwrap(),
            "99.99".to_string()
        );

        let inc_3 = Decimal::from_str("0.001").unwrap();
        assert_eq!(
            format_assert_decimal(Decimal::from_str("123.4567").unwrap(), inc_3).unwrap(),
            "123.456".to_string()
        );

        let inc_4 = Decimal::from_str("0.0001").unwrap();
        assert_eq!(
            format_assert_decimal(Decimal::from_str("123.45678").unwrap(), inc_4).unwrap(),
            "123.4567".to_string()
        );

        let inc_5 = Decimal::from_str("0.00001").unwrap();
        assert_eq!(
            format_assert_decimal(Decimal::from_str("123.45678").unwrap(), inc_5).unwrap(),
            "123.45678".to_string()
        );

        let inc_6 = Decimal::from_str("0.000001").unwrap();
        assert_eq!(
            format_assert_decimal(Decimal::from_str("123.456789").unwrap(), inc_6).unwrap(),
            "123.456789".to_string()
        );

        let inc_7 = Decimal::from_str("0.0000001").unwrap();
        assert_eq!(
            format_assert_decimal(Decimal::from_str("123.4567891").unwrap(), inc_7).unwrap(),
            "123.4567891".to_string()
        );
        assert_eq!(
            format_assert_decimal(Decimal::from_str("123.456789123121").unwrap(), inc_7).unwrap(),
            "123.4567891".to_string()
        );

        let inc_8 = Decimal::from_str("0.00000001").unwrap();
        assert_eq!(
            format_assert_decimal(Decimal::from_str("0.123456789").unwrap(), inc_8).unwrap(),
            "0.12345678".to_string()
        );

        let inc_9 = Decimal::from_str("0.000000001").unwrap();
        assert_eq!(
            format_assert_decimal(Decimal::from_str("0.00000000123").unwrap(), inc_9).unwrap(),
            "0.000000001".to_string()
        );
    }
}
