use crate::api::models::{Bot, OrderData, OrderSide, OrderType, StopType};
use crate::api::requests::{
    api_v3_hf_margin_stop_order_cancel_by_client_oid_delete, api_v3_hf_margin_stop_order_post,
};
use crate::api::utils::{BodySerializer, QueryBuilder};
use crate::core::repository_traits::{
    BotEntryUpdate, BotManagement, BotQuery, BotSlUpdate, BotTpUpdate, MessageCommand,
    OrderCommand, OrderQuery, SymbolQuery,
};
use crate::logic::order_handlers::make_random_trade;
use crate::logic::utils::{
    format_assert_decimal, sl_buy_percent, sl_sell_percent, tp_buy_percent, tp_sell_percent,
};
use anyhow::Result;
use micromap::Map;
use rust_decimal::Decimal;
use std::str::FromStr;
use tracing::{error, info};
use uuid::Uuid;

/// Обработка entry ордера бота
pub async fn process_bot_by_entry_client_oid(
    bot_repo: &(impl BotQuery + BotEntryUpdate + BotTpUpdate + BotSlUpdate),
    order_repo: &(impl OrderQuery + OrderCommand),
    symbol_repo: &impl SymbolQuery,
    client_oid: &str,
    order: &OrderData,
) -> Result<()> {
    let symbol_info = match symbol_repo.get_symbol_info(&order.symbol).await? {
        Some(symbol_info) => symbol_info,
        None => anyhow::bail!("Symbol info not found for {}", order.symbol),
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

    match order.side {
        OrderSide::Buy => {
            process_buy_entry(
                bot_repo,
                client_oid,
                order,
                new_balance,
                filled_size,
                price_increment,
            )
            .await?;
        }
        OrderSide::Sell => {
            process_sell_entry(
                bot_repo,
                client_oid,
                order,
                new_balance,
                filled_size,
                price_increment,
                quote_increment,
            )
            .await?;
        }
        OrderSide::Unknown => {}
    }

    bot_repo.clear_entry_client_oid(client_oid).await?;
    Ok(())
}

/// Обработка buy entry ордера
async fn process_buy_entry(
    bot_repo: &(impl BotQuery + BotEntryUpdate + BotTpUpdate + BotSlUpdate),
    client_oid: &str,
    order: &OrderData,
    new_balance: Decimal,
    filled_size: Decimal,
    price_increment: Decimal,
) -> Result<()> {
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
        "type": OrderType::Market,
        "stop": StopType::Entry,
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
        "type": OrderType::Market,
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

    let tp_body = BodySerializer::serialize(Some(msg_tp_order))?;
    let sl_body = BodySerializer::serialize(Some(msg_sl_order))?;
    let tp_fut = api_v3_hf_margin_stop_order_post(&tp_body);
    let sl_fut = api_v3_hf_margin_stop_order_post(&sl_body);

    let (tp_res, sl_res) = tokio::join!(tp_fut, sl_fut);

    handle_stop_order_results_buy(
        bot_repo,
        tp_res,
        sl_res,
        &exit_tp_client_oid,
        &exit_sl_client_oid,
        client_oid,
    )
    .await?;

    Ok(())
}

/// Обработка sell entry ордера
async fn process_sell_entry(
    bot_repo: &(impl BotQuery + BotEntryUpdate + BotTpUpdate + BotSlUpdate),
    client_oid: &str,
    order: &OrderData,
    new_balance: Decimal,
    filled_size: Decimal,
    price_increment: Decimal,
    quote_increment: Decimal,
) -> Result<()> {
    let tp_sell = tp_sell_percent()?;
    let sl_sell = sl_sell_percent()?;

    let match_price = new_balance / filled_size;
    let trigger_tp_price = match_price * tp_sell;
    let trigger_sl_price = match_price * sl_sell;
    let funds_tp = trigger_tp_price * filled_size;
    let funds_sl = trigger_sl_price * filled_size;

    let exit_tp_client_oid = Uuid::new_v4().to_string();
    let exit_sl_client_oid = Uuid::new_v4().to_string();

    let stop_price_tp = format_assert_decimal(trigger_tp_price, price_increment)?;
    let funds_tp_str = format_assert_decimal(funds_tp, quote_increment)?;
    let msg_tp_order = serde_json::json!({
        "clientOid": exit_tp_client_oid,
        "side": OrderSide::Buy,
        "symbol": order.symbol,
        "type": OrderType::Market,
        "stop": "loss",
        "stopPrice": stop_price_tp,
        "isIsolated": false,
        "autoBorrow": true,
        "autoRepay": false,
        "timeInForce": "GTC",
        "funds": funds_tp_str,
    });
    let stop_price_sl = format_assert_decimal(trigger_sl_price, price_increment)?;
    let funds_sl_str = format_assert_decimal(funds_sl, quote_increment)?;

    let msg_sl_order = serde_json::json!({
        "clientOid": exit_sl_client_oid,
        "side": OrderSide::Buy,
        "symbol": order.symbol,
        "type": OrderType::Market,
        "stop": StopType::Entry,
        "stopPrice": stop_price_sl,
        "isIsolated": false,
        "autoBorrow": true,
        "autoRepay": false,
        "timeInForce": "GTC",
        "funds": funds_sl_str,
    });

    info!("Stop profit order:{}", msg_tp_order);
    info!("Stop loss order:{}", msg_sl_order);

    bot_repo
        .update_exit_tp_order_id_by_client_oid(client_oid, &exit_tp_client_oid)
        .await?;
    bot_repo.clear_exit_sl_by_client_oid(client_oid).await?;

    let tp_body = BodySerializer::serialize(Some(msg_tp_order))?;
    let sl_body = BodySerializer::serialize(Some(msg_sl_order))?;
    let tp_fut = api_v3_hf_margin_stop_order_post(&tp_body);
    let sl_fut = api_v3_hf_margin_stop_order_post(&sl_body);

    let (tp_res, sl_res) = tokio::join!(tp_fut, sl_fut);

    handle_stop_order_results_sell(
        bot_repo,
        tp_res,
        sl_res,
        &exit_tp_client_oid,
        &exit_sl_client_oid,
        client_oid,
    )
    .await?;

    Ok(())
}

/// Обработка результатов создания стоп-ордеров для buy
async fn handle_stop_order_results_buy(
    bot_repo: &(impl BotQuery + BotEntryUpdate + BotTpUpdate + BotSlUpdate),
    tp_res: Result<Option<crate::api::models::MakeStopOrderResData>>,
    sl_res: Result<Option<crate::api::models::MakeStopOrderResData>>,
    exit_tp_client_oid: &str,
    exit_sl_client_oid: &str,
    client_oid: &str,
) -> Result<()> {
    match (&tp_res, &sl_res) {
        (Ok(tp_resp), Ok(sl_resp)) => {
            if let Some(response_data) = tp_resp {
                bot_repo
                    .update_exit_tp_order_id_by_client_oid(
                        &response_data.order_id,
                        &response_data.client_oid,
                    )
                    .await?;
            }
            if let Some(response_data) = sl_resp {
                bot_repo
                    .update_exit_sl_order_id_by_client_oid(
                        &response_data.order_id,
                        &response_data.client_oid,
                    )
                    .await?;
            }
            info!(
                "✅ Both stop orders created: TP={}, SL={}",
                exit_tp_client_oid, exit_sl_client_oid
            );
        }
        (Err(tp_err), Ok(sl_resp)) => {
            if let Some(response_data) = sl_resp {
                let mut query_params = Map::new();
                query_params.insert("clientOid", response_data.client_oid.as_str());
                api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(&QueryBuilder::build(
                    query_params,
                )?)
                .await?;
            }
            bot_repo
                .clear_exit_sl_by_client_oid(exit_sl_client_oid)
                .await?;
            error!(
                "Failed add TP order: {}. SL was cancelled for symmetry.",
                tp_err
            );
        }
        (Ok(tp_resp), Err(sl_err)) => {
            if let Some(response_data) = tp_resp {
                let mut query_params = Map::new();
                query_params.insert("clientOid", response_data.client_oid.as_str());
                api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(&QueryBuilder::build(
                    query_params,
                )?)
                .await?;
            }
            bot_repo
                .clear_exit_tp_by_client_oid(exit_tp_client_oid)
                .await?;
            error!(
                "Failed add SL order: {}. TP was cancelled for symmetry.",
                sl_err
            );
        }
        (Err(tp_err), Err(sl_err)) => {
            error!("Failed add both stop orders: TP={}, SL={}", tp_err, sl_err);
            bot_repo
                .clear_exit_sl_by_client_oid(exit_sl_client_oid)
                .await?;
            bot_repo
                .clear_exit_tp_by_client_oid(exit_tp_client_oid)
                .await?;
            bot_repo.clear_entry_client_oid(client_oid).await?;
        }
    }
    Ok(())
}

/// Обработка результатов создания стоп-ордеров для sell
async fn handle_stop_order_results_sell(
    bot_repo: &(impl BotQuery + BotEntryUpdate + BotTpUpdate + BotSlUpdate),
    tp_res: Result<Option<crate::api::models::MakeStopOrderResData>>,
    sl_res: Result<Option<crate::api::models::MakeStopOrderResData>>,
    exit_tp_client_oid: &str,
    exit_sl_client_oid: &str,
    client_oid: &str,
) -> Result<()> {
    match (&tp_res, &sl_res) {
        (Ok(tp_resp), Ok(sl_resp)) => {
            if let Some(response_data) = tp_resp {
                bot_repo
                    .update_exit_tp_order_id_by_client_oid(
                        &response_data.order_id,
                        &response_data.client_oid,
                    )
                    .await?;
            }
            if let Some(response_data) = sl_resp {
                bot_repo
                    .update_exit_sl_order_id_by_client_oid(
                        &response_data.order_id,
                        &response_data.client_oid,
                    )
                    .await?;
            }
            info!(
                "✅ Both stop orders created: TP={}, SL={}",
                exit_tp_client_oid, exit_sl_client_oid
            );
        }
        (Err(tp_err), Ok(sl_resp)) => {
            if let Some(response_data) = sl_resp {
                let mut query_params = Map::new();
                query_params.insert("clientOid", response_data.client_oid.as_str());
                api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(&QueryBuilder::build(
                    query_params,
                )?)
                .await?;
            }
            bot_repo
                .clear_exit_sl_by_client_oid(exit_sl_client_oid)
                .await?;
            error!(
                "Failed add TP order: {}. SL was cancelled for symmetry.",
                tp_err
            );
        }
        (Ok(tp_resp), Err(sl_err)) => {
            if let Some(response_data) = tp_resp {
                let mut query_params = Map::new();
                query_params.insert("clientOid", response_data.client_oid.as_str());
                api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(&QueryBuilder::build(
                    query_params,
                )?)
                .await?;
            }
            bot_repo
                .clear_exit_tp_by_client_oid(exit_tp_client_oid)
                .await?;
            error!(
                "Failed add SL order: {}. TP was cancelled for symmetry.",
                sl_err
            );
        }
        (Err(tp_err), Err(sl_err)) => {
            error!("Failed add both stop orders: TP={}, SL={}", tp_err, sl_err);
            bot_repo
                .clear_exit_sl_by_client_oid(exit_sl_client_oid)
                .await?;
            bot_repo
                .clear_exit_tp_by_client_oid(exit_tp_client_oid)
                .await?;
            bot_repo.clear_entry_client_oid(client_oid).await?;
        }
    }
    Ok(())
}

/// Обработка exit TP ордера
pub async fn process_bot_by_exit_tp_client_oid(
    bot_repo: &(impl BotQuery + BotEntryUpdate + BotTpUpdate + BotSlUpdate + BotManagement),
    order_repo: &(impl OrderQuery + OrderCommand),
    symbol_repo: &impl SymbolQuery,
    message_repo: &impl MessageCommand,
    bot: Bot,
    client_oid: &str,
    order: &OrderData,
) -> Result<()> {
    bot_repo.clear_exit_tp_by_client_oid(client_oid).await?;

    if let Some(exit_sl_client_oid) = &bot.exit_sl_client_oid {
        bot_repo
            .clear_exit_sl_by_client_oid(exit_sl_client_oid)
            .await?;

        let mut query_params = Map::new();
        query_params.insert("clientOid", exit_sl_client_oid.as_str());
        api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(&QueryBuilder::build(
            query_params,
        )?)
        .await?;
        info!("Successfully cancel stop order :{}", exit_sl_client_oid);
    }

    let return_balance = order_repo
        .get_total_match_value_by_client_oid(client_oid)
        .await?;
    let return_balance = match return_balance {
        Some(return_balance) => {
            Decimal::from_str(&return_balance).map_err(|e| anyhow::anyhow!(e))?
        }
        None => {
            error!("No records found or error occurred");
            return Ok(());
        }
    };

    match order.side {
        OrderSide::Buy => {
            let old_balance = bot.balance_decimal()?;
            let new_balance = old_balance + old_balance - return_balance;
            bot_repo
                .update_balance_and_clear_symbol_by_exit_tp(
                    client_oid,
                    &format!("{:.4}", new_balance),
                )
                .await?;
            make_random_trade(bot_repo, symbol_repo, message_repo, new_balance, bot.id).await?;
        }
        OrderSide::Sell => {
            bot_repo
                .update_balance_and_clear_symbol_by_exit_tp(
                    client_oid,
                    &format!("{:.4}", return_balance),
                )
                .await?;
            make_random_trade(bot_repo, symbol_repo, message_repo, return_balance, bot.id).await?;
        }
        OrderSide::Unknown => {}
    }
    Ok(())
}

/// Обработка exit SL ордера
pub async fn process_bot_by_exit_sl_client_oid(
    bot_repo: &(impl BotQuery + BotEntryUpdate + BotTpUpdate + BotSlUpdate + BotManagement),
    order_repo: &(impl OrderQuery + OrderCommand),
    symbol_repo: &impl SymbolQuery,
    message_repo: &impl MessageCommand,
    bot: Bot,
    client_oid: &str,
    order: &OrderData,
) -> Result<()> {
    bot_repo.clear_exit_sl_by_client_oid(client_oid).await?;

    if let Some(exit_tp_client_oid) = &bot.exit_tp_client_oid {
        bot_repo
            .clear_exit_tp_by_client_oid(exit_tp_client_oid)
            .await?;
        let mut query_params = Map::new();
        query_params.insert("clientOid", exit_tp_client_oid.as_str());

        api_v3_hf_margin_stop_order_cancel_by_client_oid_delete(&QueryBuilder::build(
            query_params,
        )?)
        .await?;
        info!("Successfully cancel stop order :{}", exit_tp_client_oid);
    }

    let return_balance = match order_repo
        .get_total_match_value_by_client_oid(client_oid)
        .await?
    {
        Some(return_balance) => {
            Decimal::from_str(&return_balance).map_err(|e| anyhow::anyhow!(e))?
        }
        None => {
            error!("No records found or error occurred");
            return Ok(());
        }
    };

    match order.side {
        OrderSide::Buy => {
            let old_balance = bot.balance_decimal()?;
            let new_balance = old_balance + old_balance - return_balance;
            bot_repo
                .update_balance_by_entry_client_oid(client_oid, &format!("{:.4}", new_balance))
                .await?;
            make_random_trade(bot_repo, symbol_repo, message_repo, new_balance, bot.id).await?;
        }
        OrderSide::Sell => {
            bot_repo
                .update_balance_and_clear_symbol_by_exit_sl(
                    client_oid,
                    &format!("{:.4}", return_balance),
                )
                .await?;
            make_random_trade(bot_repo, symbol_repo, message_repo, return_balance, bot.id).await?;
        }
        OrderSide::Unknown => {}
    }

    Ok(())
}

/// Обработка события торгового ордера
pub async fn trade_order_event(
    bot_repo: &(impl BotQuery + BotEntryUpdate + BotTpUpdate + BotSlUpdate + BotManagement),
    order_repo: &(impl OrderQuery + OrderCommand),
    symbol_repo: &impl SymbolQuery,
    message_repo: &impl MessageCommand,
    order: &OrderData,
) -> Result<()> {
    let client_oid = match &order.client_oid {
        Some(client_oid) => client_oid,
        None => anyhow::bail!("client_oid in order is none: {}", order),
    };

    let bot = match bot_repo.get_by_client_oid(client_oid).await? {
        Some(bot) => bot,
        None => anyhow::bail!("Bot is None by:{}", client_oid),
    };

    match client_oid.as_str() {
        s if Some(s.to_string()) == bot.entry_client_oid => {
            process_bot_by_entry_client_oid(bot_repo, order_repo, symbol_repo, client_oid, order)
                .await?;
        }
        s if Some(s.to_string()) == bot.exit_tp_client_oid => {
            process_bot_by_exit_tp_client_oid(
                bot_repo,
                order_repo,
                symbol_repo,
                message_repo,
                bot,
                client_oid,
                order,
            )
            .await?;
        }
        s if Some(s.to_string()) == bot.exit_sl_client_oid => {
            process_bot_by_exit_sl_client_oid(
                bot_repo,
                order_repo,
                symbol_repo,
                message_repo,
                bot,
                client_oid,
                order,
            )
            .await?;
        }
        _ => anyhow::bail!("don't find client_oid in:{}", order),
    }
    Ok(())
}
