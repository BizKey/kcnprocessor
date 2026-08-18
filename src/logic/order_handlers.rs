use anyhow::{Context, Result};
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use serde_json;
use std::str::FromStr;
use tokio::time::sleep;
use tracing::{error, info};
use uuid::Uuid;

use super::utils::{
    RETRY_DELAY_BASE, format_assert_decimal, get_random_side, sl_buy_percent, sl_sell_percent,
    tp_buy_percent, tp_sell_percent,
};
use crate::api::models::{AdvancedOrders, MakeOrderResData, OrderData};
use crate::api::requests::{
    api_v1_market_orderbook_level1_get, api_v3_hf_margin_order_post,
    api_v3_hf_margin_stop_order_cancel_by_client_oid_delete, api_v3_hf_margin_stop_order_post,
    build_query_string, serialize_body,
};
use crate::core::repository_traits::*;

/// Создание рыночного ордера с указанием суммы (funds)
pub async fn make_hf_funds_margin_order(
    message_repo: &impl MessageRepositoryTrait,
    client_oid: &str,
    side: &str,
    symbol: &str,
    funds: &str,
    type_: &'static str,
    auto_borrow: bool,
    auto_repay: bool,
) -> Result<MakeOrderResData> {
    let args_time_in_force = "GTC";

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
        None => anyhow::bail!("No data returned from API"),
    };

    Ok(data)
}

/// Создание рыночного ордера с указанием размера (size)
pub async fn make_hf_size_margin_order(
    message_repo: &impl MessageRepositoryTrait,
    client_oid: &str,
    side: &str,
    symbol: &str,
    size: &str,
    type_: &'static str,
    auto_borrow: bool,
    auto_repay: bool,
) -> Result<MakeOrderResData> {
    let args_time_in_force = "GTC";

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
        None => anyhow::bail!("No data returned from API"),
    };
    Ok(data)
}

/// Создание случайной сделки для бота
pub async fn make_random_trade(
    bot_repo: &impl BotRepositoryTrait,
    symbol_repo: &impl SymbolRepositoryTrait,
    message_repo: &impl MessageRepositoryTrait,
    balance_funds: Decimal,
    trade_bot_id: i32,
) -> Result<()> {
    const MAX_RETRIES: u32 = 10;
    let mut attempt = 0;

    loop {
        if attempt >= MAX_RETRIES {
            return Ok(());
        }
        sleep(tokio::time::Duration::from_millis(
            RETRY_DELAY_BASE * attempt as u64,
        ))
        .await;
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

                let mut query_params = micromap::Map::new();
                query_params.insert("symbol", tradeable_symbol.as_str());

                let token_price =
                    match api_v1_market_orderbook_level1_get(&build_query_string(query_params)?)
                        .await?
                    {
                        Some(token_price) => token_price,
                        None => anyhow::bail!("No price data"),
                    };

                let token_price = token_price.price_decimal()?;
                let token_size = balance_funds / token_price;
                let size = format_assert_decimal(token_size, base_increment)
                    .with_context(|| format!("Fail parse:{} {}", token_size, base_increment))?;

                make_hf_size_margin_order(
                    message_repo,
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
                    message_repo,
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
            _ => continue,
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

/// Создание начальных ордеров для всех ботов
pub async fn create_init_orders(
    bot_repo: &impl BotRepositoryTrait,
    symbol_repo: &impl SymbolRepositoryTrait,
    message_repo: &impl MessageRepositoryTrait,
) -> Result<()> {
    let trade_bots = bot_repo.get_all().await?;

    for trade_bot in trade_bots.iter() {
        sleep(crate::constants::INIT_ORDER_DELAY).await;
        let token_funds = trade_bot.balance_decimal()?;
        match make_random_trade(
            bot_repo,
            symbol_repo,
            message_repo,
            token_funds,
            trade_bot.id,
        )
        .await
        {
            Ok(_) => {}
            Err(e) => {
                error!("{:#}", e);
            }
        }
    }
    info!("All bots initialized!");
    Ok(())
}
