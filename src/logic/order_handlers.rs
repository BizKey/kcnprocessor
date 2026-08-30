use crate::api::models::{MakeOrderResData, OrderAmount, OrderSide, OrderType};
use crate::api::requests::{api_v1_market_orderbook_level1_get, api_v3_hf_margin_order_post};
use crate::api::utils::query_builder::QueryBuilder;
use crate::api::utils::serializer::BodySerializer;
use crate::core::repository_traits::{
    BotEntryUpdate, BotManagement, BotQuery, BotSlUpdate, BotTpUpdate, MessageCommand, SymbolQuery,
};
use crate::logic::utils::{format_assert_decimal, generate_entry_id, get_next_side};
use anyhow::{Context, Result};
use micromap::Map;
use rust_decimal::Decimal;
use serde_json;

use tokio::time::sleep;
use tracing::{error, info};

/// Создание рыночного ордера с указанием суммы (funds)
pub async fn make_hf_margin_order(
    sendorders_repo: &impl MessageCommand,
    client_oid: &str,
    side: OrderSide,
    symbol: &str,
    amount: OrderAmount,
    order_type: OrderType,
    auto_borrow: bool,
    auto_repay: bool,
) -> Result<MakeOrderResData> {
    let time_in_force = "GTC";
    let order_type_str = order_type.as_str();

    let (size, funds) = match &amount {
        OrderAmount::Size(s) => (Some(s.as_str()), None),
        OrderAmount::Funds(f) => (None, Some(f.as_str())),
    };

    sendorders_repo
        .save_send_orders(
            Some(symbol),
            Some(side.as_str()),
            size,
            funds,
            None,
            Some(time_in_force),
            Some(order_type_str),
            Some(&auto_borrow),
            Some(&auto_repay),
            Some(client_oid),
            None,
        )
        .await?;

    let mut msg = serde_json::json!({
        "clientOid": client_oid,
        "symbol": symbol,
        "side": side,
        "type": order_type_str,
        "autoBorrow": auto_borrow,
        "autoRepay": auto_repay,
        "timeInForce": time_in_force,
    });

    match amount {
        OrderAmount::Size(size) => {
            msg["size"] = serde_json::Value::String(size);
        }
        OrderAmount::Funds(funds) => {
            msg["funds"] = serde_json::Value::String(funds);
        }
    }

    info!("{}", msg);

    let body_str = BodySerializer::serialize(Some(msg))?;
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
    sendorders_repo: &impl MessageCommand,
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

    let entry_client_oid = generate_entry_id();

    // update entry_client_oid exchange
    bot_repo
        .update_entry_client_oid_by_id(Some(&entry_client_oid), trade_bot_id)
        .await?;

    let order_result = match get_next_side() {
        OrderSide::Sell => {
            let mut query_params = Map::new();
            query_params.insert("symbol", tradeable_symbol.as_str());

            let token_price =
                match api_v1_market_orderbook_level1_get(&QueryBuilder::build(query_params)?)
                    .await?
                {
                    Some(token_price) => token_price,
                    None => anyhow::bail!("No price data:{}", tradeable_symbol),
                };

            let base_increment = symbol_info.base_increment_decimal()?;
            let token_price = token_price.price_decimal()?;
            let token_size = balance_funds / token_price;
            let size = format_assert_decimal(token_size, base_increment)
                .with_context(|| format!("Fail parse:{} {}", token_size, base_increment))?;

            make_hf_margin_order(
                sendorders_repo,
                &entry_client_oid,
                OrderSide::Sell,
                &tradeable_symbol,
                OrderAmount::Size(size.clone()),
                OrderType::Market,
                true,
                false,
            )
            .await
        }
        OrderSide::Buy => {
            let quote_increment = symbol_info.quote_increment_decimal()?;
            let funds = format_assert_decimal(balance_funds, quote_increment)
                .with_context(|| format!("Fail parse:{} {}", balance_funds, quote_increment))?;

            make_hf_margin_order(
                sendorders_repo,
                &entry_client_oid,
                OrderSide::Buy,
                &tradeable_symbol,
                OrderAmount::Funds(funds.clone()),
                OrderType::Market,
                true,
                false,
            )
            .await
        }
        OrderSide::Unknown => {
            error!("get_next_side is Unknown");
            anyhow::bail!("get_next_side is Unknown")
        }
    };

    match order_result {
        Ok(res) => {
            info!(
                "Order placed client_oid:{} order_id:{} entry_client_oid:{} trade_bot_id:{}",
                res.client_oid, res.order_id, entry_client_oid, trade_bot_id,
            );
            Ok(())
        }
        Err(e) => {
            error!("Order failed: {} {:.?}", tradeable_symbol, e);
            Err(e)
        }
    }
}

/// Создание начальных ордеров для всех ботов
pub async fn create_init_orders(
    bot_repo: &(impl BotQuery + BotEntryUpdate + BotTpUpdate + BotSlUpdate + BotManagement),
    symbol_repo: &impl SymbolQuery,
    sendorders_repo: &impl MessageCommand,
) -> Result<()> {
    for trade_bot in bot_repo.get_all().await?.iter() {
        sleep(crate::constants::INIT_ORDER_DELAY).await;
        if let Err(e) = make_random_trade(
            bot_repo,
            symbol_repo,
            sendorders_repo,
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
