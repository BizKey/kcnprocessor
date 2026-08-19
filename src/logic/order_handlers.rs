use crate::api::models::MakeOrderResData;
use crate::api::requests::{api_v1_market_orderbook_level1_get, api_v3_hf_margin_order_post};
use crate::api::utils::{BodySerializer, QueryBuilder};
use crate::core::repository_traits::*;
use crate::logic::order_side::OrderSide;
use crate::logic::utils::{RETRY_DELAY_BASE, format_assert_decimal, get_next_side};
use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde_json;
use tokio::time::sleep;
use tracing::{error, info};
use uuid::Uuid;

/// Создание рыночного ордера с указанием суммы (funds)
pub async fn make_hf_funds_margin_order(
    message_repo: &impl MessageCommand,
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

    let body_str = BodySerializer::serialize(Some(msg))?;
    let data = match api_v3_hf_margin_order_post(&body_str).await? {
        Some(data) => data,
        None => anyhow::bail!("No data returned from API"),
    };

    Ok(data)
}

/// Создание рыночного ордера с указанием размера (size)
pub async fn make_hf_size_margin_order(
    message_repo: &impl MessageCommand,
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

    let body_str = BodySerializer::serialize(Some(serde_json::json!({
        "clientOid": client_oid,
        "symbol": symbol,
        "side": side,
        "type": type_,
        "autoBorrow": auto_borrow,
        "autoRepay": auto_repay,
        "timeInForce": args_time_in_force,
        "size": size
    })))?;

    let data = match api_v3_hf_margin_order_post(&body_str).await? {
        Some(data) => data,
        None => anyhow::bail!("No data returned from API"),
    };
    Ok(data)
}

/// Создание случайной сделки для бота
pub async fn make_random_trade(
    bot_repo: &(impl BotQuery + BotEntryUpdate + BotTpUpdate + BotSlUpdate + BotManagement),
    symbol_repo: &impl SymbolQuery,
    message_repo: &impl MessageCommand,
    balance_funds: Decimal,
    trade_bot_id: i32,
) -> Result<()> {
    let tradeable_symbol = match symbol_repo.get_random_symbol().await? {
        Some(tradeable_symbol) => tradeable_symbol,
        None => {
            error!("Failed get_random_symbol");
            anyhow::bail!("Failed get_random_symbol")
        }
    };

    let symbol_info = match symbol_repo.get_symbol_info(&tradeable_symbol).await? {
        Some(symbol_info) => symbol_info,
        None => {
            error!("Symbol info not found for {}", tradeable_symbol);
            anyhow::bail!("Symbol info not found for {}", tradeable_symbol)
        }
    };

    let entry_client_oid = Uuid::new_v4().to_string();

    bot_repo
        .update_entry_client_oid_by_id(Some(&entry_client_oid), trade_bot_id)
        .await?;

    let order_result = match get_next_side() {
        OrderSide::Sell => {
            let base_increment = symbol_info.base_increment_decimal()?;

            let mut query_params = micromap::Map::new();
            query_params.insert("symbol", tradeable_symbol.as_str());

            let token_price =
                match api_v1_market_orderbook_level1_get(&QueryBuilder::build(query_params)?)
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
        OrderSide::Buy => {
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
    };

    match order_result {
        Ok(res) => {
            info!(
                "✅ Order placed client_oid:{} order_id:{} entry_client_oid:{} trade_bot_id:{}",
                res.client_oid, res.order_id, entry_client_oid, trade_bot_id,
            );
            return Ok(());
        }
        Err(e) => {
            error!("❌ Order failed: {} {:.?}", tradeable_symbol, e);
            Err(e)
        }
    }
}

/// Создание начальных ордеров для всех ботов
pub async fn create_init_orders(
    bot_repo: &(impl BotQuery + BotEntryUpdate + BotTpUpdate + BotSlUpdate + BotManagement),
    symbol_repo: &impl SymbolQuery,
    message_repo: &impl MessageCommand,
) -> Result<()> {
    for trade_bot in bot_repo.get_all().await?.iter() {
        sleep(crate::constants::INIT_ORDER_DELAY).await;
        if let Err(e) = make_random_trade(
            bot_repo,
            symbol_repo,
            message_repo,
            trade_bot.balance_decimal()?,
            trade_bot.id,
        )
        .await
        {
            error!("{:#}", e);
        }
    }
    info!("All bots initialized!");
    Ok(())
}
