use crate::api::models::{
    ApiV1MarketOrderbookLevel1ResData, ApiV3MarginRepayResData, MarginAccountData, OrderAmount,
    OrderSide, OrderType,
};
use crate::api::requests::{
    api_v1_market_orderbook_level1_get, api_v3_accounts_universal_transfer_post,
    api_v3_margin_accounts_get, api_v3_margin_repay_post,
};
use crate::api::utils::query_builder::QueryBuilder;
use crate::api::utils::serializer::BodySerializer;
use crate::core::repository_traits::{MessageCommand, SymbolQuery};
use crate::logic::order_handlers::make_hf_margin_order;
use crate::logic::utils::{AUTO_CLEAN_DELAY, format_assert_decimal, generate_entry_id};
use anyhow::Result;
use micromap::Map;
use rust_decimal::Decimal;
use tokio::time::sleep;
use tracing::info;
use uuid::Uuid;

/// Получение данных всех аккаунтов
pub async fn get_all_accounts_data() -> Result<MarginAccountData> {
    let mut query_params = Map::new();
    query_params.insert("quoteCurrency", "USDT");
    query_params.insert("queryType", "MARGIN");

    api_v3_margin_accounts_get(&QueryBuilder::build(query_params)?).await
}

/// Репай (погашение) задолженности
pub async fn repay_account(currency: &str, size: &str) -> Result<Option<ApiV3MarginRepayResData>> {
    info!("Repay {} liability:{}", size, currency);
    let body_str = BodySerializer::serialize(Some(serde_json::json!({
        "currency": currency,
        "size": size,
        "isIsolated": false,
        "isHf": true
    })))?;

    api_v3_margin_repay_post(&body_str).await
}

/// Получение цены токена
pub async fn get_token_price(trade_symbol: &str) -> Result<ApiV1MarketOrderbookLevel1ResData> {
    let mut query_params = Map::new();
    query_params.insert("symbol", trade_symbol);

    match api_v1_market_orderbook_level1_get(&QueryBuilder::build(query_params)?).await? {
        Some(token_price) => Ok(token_price),
        None => anyhow::bail!("Fail get token_price"),
    }
}

/// Внутренний перевод средств
pub async fn transfer_in_account(
    currency: &str,
    amount: &str,
    type_: &str,
    from_account_type: &str,
    to_account_type: &str,
) -> Result<()> {
    let body_str = BodySerializer::serialize(Some(serde_json::json!({
        "currency": currency,
        "clientOid": generate_entry_id(),
        "amount": amount,
        "type": type_,
        "fromAccountType": from_account_type,
        "toAccountType": to_account_type,
    })))?;

    let result = api_v3_accounts_universal_transfer_post(&body_str)
        .await?
        .ok_or_else(|| anyhow::anyhow!("No result from transfer"))?;

    info!(
        "Success transfer {} from {} to {} with {} with id:{}",
        currency, from_account_type, to_account_type, amount, result.order_id,
    );
    Ok(())
}

/// Автоматическая очистка аккаунта
pub async fn auto_clean_account(
    symbol_repo: &impl SymbolQuery,
    message_repo: &impl MessageCommand,
) -> Result<bool> {
    let mut passed = true;

    for account in get_all_accounts_data().await?.accounts.iter() {
        let token_liability = account.liability_decimal()?;
        let token_available = account.available_decimal()?;

        if token_liability == Decimal::ZERO && token_available == Decimal::ZERO {
            continue;
        }

        let currency_info = match symbol_repo.get_currency_info(&account.currency).await? {
            Some(currency_info) => currency_info,
            None => anyhow::bail!("Currency info not found for {}", account.currency),
        };
        let precision_decimal = currency_info.precision_decimal()?;

        if account.currency == "USDT" {
            passed = handle_usdt_account(
                &account.currency,
                token_liability,
                token_available,
                precision_decimal,
            )
            .await?;
            continue;
        }

        let mut trade_symbol = String::with_capacity(account.currency.len() + 5);
        trade_symbol.push_str(&account.currency);
        trade_symbol.push_str("-USDT");

        let symbol_info = match symbol_repo.get_symbol_info(&trade_symbol).await? {
            Some(symbol_info) => symbol_info,
            None => anyhow::bail!("Symbol info not found for {}", account.currency),
        };

        passed = handle_non_usdt_account(
            message_repo,
            &account.currency,
            &trade_symbol,
            token_liability,
            token_available,
            &symbol_info,
            precision_decimal,
        )
        .await?;
    }

    sleep(AUTO_CLEAN_DELAY).await;
    Ok(passed)
}

/// Обработка USDT аккаунта
async fn handle_usdt_account(
    currency: &str,
    liability: Decimal,
    available: Decimal,
    precision: Decimal,
) -> Result<bool> {
    if liability > Decimal::ZERO {
        let size = if available >= liability {
            format_assert_decimal(liability, precision)?
        } else if available > Decimal::ZERO {
            format_assert_decimal(available, precision)?
        } else {
            return Ok(false);
        };
        repay_account(currency, &size).await?;
        return Ok(false);
    }
    Ok(true)
}

/// Обработка не-USDT аккаунта
async fn handle_non_usdt_account(
    message_repo: &impl MessageCommand,
    currency: &str,
    trade_symbol: &str,
    liability: Decimal,
    available: Decimal,
    symbol_info: &crate::api::models::Symbol,
    precision: Decimal,
) -> Result<bool> {
    let mut passed = true;

    if liability > Decimal::ZERO {
        passed = handle_liability(
            message_repo,
            currency,
            trade_symbol,
            liability,
            available,
            symbol_info,
            precision,
        )
        .await?;
    } else if available > Decimal::ZERO {
        passed = handle_available(
            message_repo,
            currency,
            trade_symbol,
            available,
            symbol_info,
            precision,
        )
        .await?;
    }

    Ok(passed)
}

/// Обработка задолженности
async fn handle_liability(
    message_repo: &impl MessageCommand,
    currency: &str,
    trade_symbol: &str,
    liability: Decimal,
    available: Decimal,
    symbol_info: &crate::api::models::Symbol,
    precision: Decimal,
) -> Result<bool> {
    if available > Decimal::ZERO {
        if available >= liability {
            let size = format_assert_decimal(liability, precision)?;
            repay_account(currency, &size).await?;
        } else {
            let size = format_assert_decimal(available, precision)?;
            repay_account(currency, &size).await?;
        }
    } else {
        let best_ask_token_price = get_token_price(trade_symbol).await?.best_ask_decimal()?;
        info!(
            "Get token ask price:{} {:?}",
            trade_symbol, best_ask_token_price
        );

        let token_funds = best_ask_token_price * liability;
        let base_min_size = symbol_info.base_min_size_decimal()?;
        let min_funds = symbol_info.min_funds_decimal()?;
        let min_funds_by_size = best_ask_token_price * base_min_size;
        let quote_increment = symbol_info.quote_increment_decimal()?;

        let size = format_assert_decimal(
            token_funds.max(min_funds_by_size).max(min_funds),
            quote_increment,
        )?;

        make_hf_margin_order(
            message_repo,
            &generate_entry_id(),
            OrderSide::Buy,
            trade_symbol,
            OrderAmount::Size(size.clone()),
            OrderType::Market,
            false,
            false,
        )
        .await?;
        info!("Buy by market {} on size {}", trade_symbol, size);
    }
    Ok(false)
}

/// Обработка доступных средств
async fn handle_available(
    message_repo: &impl MessageCommand,
    currency: &str,
    trade_symbol: &str,
    available: Decimal,
    symbol_info: &crate::api::models::Symbol,
    precision: Decimal,
) -> Result<bool> {
    let base_min_size = symbol_info.base_min_size_decimal()?;
    let quote_min_size = symbol_info.quote_min_size_decimal()?;
    let base_increment = symbol_info.base_increment_decimal()?;

    let best_bid_token_price = get_token_price(trade_symbol).await?.best_bid_decimal()?;
    info!(
        "Get token bid price:{} {:?}",
        trade_symbol, best_bid_token_price
    );

    let token_funds = best_bid_token_price * available;

    if available >= base_min_size && token_funds >= quote_min_size {
        let size = format_assert_decimal(available, base_increment)?;
        make_hf_margin_order(
            message_repo,
            &generate_entry_id(),
            OrderSide::Sell,
            trade_symbol,
            OrderAmount::Size(size.clone()),
            OrderType::Market,
            false,
            false,
        )
        .await?;
        info!("Sell by market {} on size {}", trade_symbol, size);
    } else {
        let amount = format_assert_decimal(available, precision)?;
        transfer_in_account(currency, &amount, "INTERNAL", "MARGIN", "TRADE").await?;
        info!("Success transfer {} {} to TRADE", currency, amount);
    }
    Ok(false)
}

/// Полная очистка аккаунта
pub async fn clean_account(
    symbol_repo: &impl SymbolQuery,
    message_repo: &impl MessageCommand,
) -> Result<()> {
    loop {
        if auto_clean_account(symbol_repo, message_repo).await? {
            info!("auto_clean_account success");
            break;
        }
    }
    Ok(())
}
