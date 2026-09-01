use crate::api::models::{AdvancedOrders, OrderAmount, OrderSide, OrderType, StopType};
use crate::api::requests::{
    api_v3_hf_margin_stop_order_cancel_by_id_delete, api_v3_hf_margin_stop_orders_get,
};
use crate::api::utils::query_builder::QueryBuilder;
use crate::core::repository_traits::{
    BotEntryUpdate, BotManagement, BotQuery, BotSlUpdate, BotTpUpdate, MessageCommand,
};
use crate::logic::order_handlers::make_hf_margin_order;

use anyhow::Result;
use micromap::Map;

use tracing::{error, info};
/// Отмена всех стоп-ордеров
pub async fn cancel_all_stop_orders() -> Result<()> {
    loop {
        let mut query_params = Map::new();
        query_params.insert("pageSize", "10");

        let query_params = QueryBuilder::build(query_params)?;
        let open_stop_orders = match api_v3_hf_margin_stop_orders_get(&query_params).await? {
            Some(open_stop_orders) => open_stop_orders,
            None => {
                error!("Fail get list open stop orders:None");
                continue;
            }
        };

        info!("Stop orders:{:.?}", open_stop_orders);

        if open_stop_orders.total_num == 0 {
            info!("All stop orders closed");
            break;
        }

        for stop_order in open_stop_orders.items {
            info!("Stop order:{:.?}", stop_order);

            let mut query_params = Map::new();
            query_params.insert("orderId", stop_order.id.as_str());

            let query_params = QueryBuilder::build(query_params)?;

            let canceled_stop_order =
                match api_v3_hf_margin_stop_order_cancel_by_id_delete(&query_params).await? {
                    Some(canceled) => canceled,
                    None => {
                        error!("Cancel stop order:{} None", &stop_order.id);
                        continue;
                    }
                };

            for st_order in canceled_stop_order.cancelled_order_ids {
                info!("Success cancel stop order:{}", st_order)
            }
        }
    }

    Ok(())
}

/// Обработка событий стоп-ордеров
pub async fn handle_advanced_orders(
    order: AdvancedOrders,
    bot_repo: &(impl BotQuery + BotEntryUpdate + BotTpUpdate + BotSlUpdate + BotManagement),
    sendorders_repo: &impl MessageCommand,
) -> Result<()> {
    if order.error.is_none() {
        info!("{}", order);
        return Ok(());
    }
    error!("Got error on stop order:{}", order);

    let order_id_ref = order.order_id.as_ref();

    let bot = match order.stop {
        StopType::Loss => match bot_repo.get_bot_by_exit_sl_order_id(order_id_ref).await {
            Ok(bot) => bot,
            Err(e) => {
                error!("{:#}", e);
                anyhow::bail!("{:#}", e)
            }
        },
        StopType::Entry => match bot_repo.get_bot_by_exit_tp_order_id(order_id_ref).await {
            Ok(bot) => bot,
            Err(e) => {
                error!("{:#}", e);
                anyhow::bail!("{:#}", e)
            }
        },
        StopType::Unknown => {
            error!("Fail match stop_clone:{}", order.stop);
            anyhow::bail!("Fail match stop_clone:{}", order.stop)
        }
    };

    let bot = match bot {
        Some(bot) => bot,
        None => {
            error!("Fail parse bot:{}", order.stop);
            anyhow::bail!("Fail parse bot:{}", order.stop)
        }
    };

    let client_oid = match order.stop {
        StopType::Loss => bot.exit_sl_client_oid,
        StopType::Entry => bot.exit_tp_client_oid,
        StopType::Unknown => {
            error!("Fail match stop_clone:{}", order.stop);
            anyhow::bail!("Fail match stop_clone:{}", order.stop)
        }
    };

    let client_oid = match client_oid {
        Some(client_oid) => client_oid,
        None => {
            error!("Fail parse client_oid:{:.?}", client_oid);
            anyhow::bail!("Fail parse client_oid:{:.?}", client_oid)
        }
    };

    let amount = match order.side {
        OrderSide::Buy => {
            let funds = match order.funds {
                Some(funds) => funds,
                None => anyhow::bail!("Fail parse funds"),
            };
            OrderAmount::Funds(funds)
        }
        OrderSide::Sell => {
            let size = match order.size {
                Some(size) => size,
                None => anyhow::bail!("Fail parse size"),
            };
            OrderAmount::Size(size)
        }
        OrderSide::Unknown => {
            error!("Fail match side_clone:{}", order.side);
            anyhow::bail!("Fail match side_clone:{}", order.side)
        }
    };

    match make_hf_margin_order(
        sendorders_repo,
        &client_oid,
        order.side,
        &order.symbol,
        amount,
        OrderType::Market,
        true,
        false,
    )
    .await
    {
        Ok(_) => {
            info!("Order re-placed: {} {}", order_id_ref, client_oid);
        }
        Err(e) => {
            anyhow::bail!("Order failed: {} {} {}", order_id_ref, client_oid, e)
        }
    }
    Ok(())
}
