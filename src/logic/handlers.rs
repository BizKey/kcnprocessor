use crate::api::models::{
    AdvancedOrders, BalanceData, KuCoinMessage, OrderData, OrderTopic, PositionData,
};
use crate::core::repository_traits::{
    BalanceCommand, BotEntryUpdate, BotManagement, BotQuery, BotRepositoryFull, BotSlUpdate,
    BotTpUpdate, EventCommand, MessageCommand, OrderCommand, OrderQuery, OrderRepositoryFull,
    PositionCommand, SymbolQuery, SymbolRepositoryFull,
};
use crate::logic::account_handlers::repay_account;
use crate::logic::order_processor::trade_order_event;
use crate::logic::stop_order_handlers::handle_advanced_orders;
use crate::logic::utils::format_assert_decimal;
use anyhow::Result;
use bytes::Bytes;
use rust_decimal::Decimal;
use serde_json;
use tracing::{error, info};

/// Обработка событий позиции
pub async fn handle_position_event(
    position: PositionData,
    position_repo: &impl PositionCommand,
    symbol_repo: &impl SymbolQuery,
) -> Result<()> {
    // Репайм задолженности
    for (asset, liability) in position.debt_pairs()? {
        let asset_info = match position.asset_list.get(&asset) {
            Some(info) => info,
            None => {
                error!("Failed get asset:{} from:{:.?}", asset, position.asset_list);
                continue;
            }
        };

        let token_available = asset_info.available_decimal()?;

        if liability > Decimal::ZERO && token_available > Decimal::ZERO {
            let currency_info = match symbol_repo.get_currency_info(&asset).await? {
                Some(currency_info) => currency_info,
                None => anyhow::bail!("Currency info not found for {}", asset),
            };

            let precision_decimal = currency_info.precision_decimal()?;
            let size = format_assert_decimal(liability.min(token_available), precision_decimal)?;
            repay_account(&asset, &size).await?;
            info!("Repay {} size {}", &asset, size);
        }
    }

    // Сохраняем данные позиции
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

/// Основная обработка сообщений WebSocket
pub async fn process_kcn_msg(
    bot_repo: &(impl BotQuery + BotEntryUpdate + BotTpUpdate + BotSlUpdate + BotManagement),
    order_repo: &(impl OrderQuery + OrderCommand),
    symbol_repo: &impl SymbolQuery,
    balance_repo: &impl BalanceCommand,
    position_repo: &impl PositionCommand,
    event_repo: &impl EventCommand,
    message_repo: &impl MessageCommand,
    msg: &str,
) -> Result<()> {
    let data = match serde_json::from_str::<KuCoinMessage>(msg)? {
        KuCoinMessage::Message(data) => data,
        KuCoinMessage::Welcome(data) => {
            event_repo.save_event(&serde_json::to_value(&data)?).await?;
            return Ok(());
        }
        KuCoinMessage::Ack(data) => {
            event_repo.save_event(&serde_json::to_value(&data)?).await?;
            return Ok(());
        }
        KuCoinMessage::Error(data) => {
            anyhow::bail!("Got error in WS {:?}", data)
        }
        KuCoinMessage::Unknown => {
            anyhow::bail!("Unknown WS message type {:?}", msg);
        }
    };

    match data.topic {
        OrderTopic::Balance => {
            let balance: BalanceData = serde_json::from_value(data.data)?;
            balance_repo.save_balance_event(balance).await?;
        }
        OrderTopic::TradeOrders => {
            let order: OrderData = serde_json::from_value(data.data)?;
            handle_trade_order_event(bot_repo, order_repo, symbol_repo, message_repo, order)
                .await?;
        }
        OrderTopic::AdvancedOrders => {
            let order: AdvancedOrders = serde_json::from_value(data.data)?;
            handle_advanced_orders(order, bot_repo, message_repo).await?;
        }
        OrderTopic::Position => {
            let position: PositionData = serde_json::from_value(data.data)?;
            handle_position_event(position, position_repo, symbol_repo).await?;
        }
        OrderTopic::Unknown => anyhow::bail!("Unknown topic: {:.?}", data.topic),
    }
    Ok(())
}

/// Запуск обработчика сообщений в отдельном потоке
pub async fn spawn_process_kcn_msg(
    mut rx_in: tokio::sync::mpsc::Receiver<Bytes>,
    bot_repo: impl BotRepositoryFull + Clone + 'static,
    order_repo: impl OrderRepositoryFull + Clone + 'static,
    symbol_repo: impl SymbolRepositoryFull + Clone + 'static,
    balance_repo: impl BalanceCommand + Clone + 'static,
    position_repo: impl PositionCommand + Clone + 'static,
    event_repo: impl EventCommand + Clone + 'static,
    message_repo: impl MessageCommand + Clone + 'static,
) {
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

        if let Err(e) = process_kcn_msg(
            &bot_repo,
            &order_repo,
            &symbol_repo,
            &balance_repo,
            &position_repo,
            &event_repo,
            &message_repo,
            &text,
        )
        .await
        {
            error!("{:#}", e);
        }
    }
}

/// Обработка события торгового ордера
pub async fn handle_trade_order_event(
    bot_repo: &(impl BotQuery + BotEntryUpdate + BotTpUpdate + BotSlUpdate + BotManagement),
    order_repo: &(impl OrderQuery + OrderCommand),
    symbol_repo: &impl SymbolQuery,
    message_repo: &impl MessageCommand,
    order: OrderData,
) -> Result<()> {
    info!("{}", order);
    order_repo.save_order_event(&order).await?;

    if order.should_process() {
        trade_order_event(bot_repo, order_repo, symbol_repo, message_repo, &order).await?;
    }

    Ok(())
}
