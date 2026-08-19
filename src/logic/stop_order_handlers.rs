use crate::api::utils::QueryBuilder;
use anyhow::Result;
use micromap::Map;
use tokio::time::sleep;
use tracing::{error, info};

use crate::api::models::{AdvancedOrders, StopType};
use crate::api::requests::{
    api_v3_hf_margin_stop_order_cancel_by_id_delete, api_v3_hf_margin_stop_orders_get,
};
use crate::constants::DELETE_STOP_ORDER_DELAY;
use crate::core::repository_traits::*;
use crate::logic::order_handlers::{make_hf_funds_margin_order, make_hf_size_margin_order};
use crate::logic::utils::RETRY_DELAY_BASE;
use uuid::Uuid;

/// Отмена всех стоп-ордеров
pub async fn cancel_all_stop_orders() -> Result<()> {
    loop {
        let mut query_params = Map::new();
        query_params.insert("pageSize", "10");

        let query_params = QueryBuilder::build(query_params)?;
        let open_stop_orders = match api_v3_hf_margin_stop_orders_get(&query_params).await? {
            Some(orders) => orders,
            None => {
                error!("Fail get list open stop orders:None");
                continue;
            }
        };

        if open_stop_orders.total_num == 0 {
            info!("All stop orders closed");
            break;
        }

        for stop_order in open_stop_orders.items {
            info!("Stop order:{}", stop_order);

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
        sleep(DELETE_STOP_ORDER_DELAY).await;
    }

    Ok(())
}

/// Обработка событий стоп-ордеров (повторная отправка при ошибке)
pub async fn handle_advanced_orders(
    order: AdvancedOrders,
    bot_repo: &(impl BotQuery + BotEntryUpdate + BotTpUpdate + BotSlUpdate + BotManagement),
    message_repo: &impl MessageCommand,
) -> Result<()> {
    if order.error.is_none() {
        info!("{}", order);
        return Ok(());
    }
    error!("Got error on stop order : {}", order);

    let order_id_ref = &order.order_id;
    let side_ref = &order.side;
    let symbol_ref = &order.symbol;
    let funds_clone = order.funds.clone();
    let size_clone = order.size.clone();
    let new_exit_client_oid = Uuid::new_v4().to_string();

    let order_result = match order.stop {
        StopType::Loss => {
            match bot_repo
                .update_exit_sl_client_oid_by_order_id(order_id_ref, &new_exit_client_oid)
                .await
            {
                Ok(_) => match side_ref.as_str() {
                    "buy" => {
                        let funds = match funds_clone {
                            Some(funds) => funds,
                            None => anyhow::bail!("Fail parse funds"),
                        };
                        make_hf_funds_margin_order(
                            message_repo,
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
                            None => anyhow::bail!("Fail parse size"),
                        };
                        make_hf_size_margin_order(
                            message_repo,
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
                        anyhow::bail!("Fail match side_clone:{}", side_ref)
                    }
                },
                Err(e) => {
                    error!("{:#}", e);
                    anyhow::bail!("{:#}", e)
                }
            }
        }
        StopType::Entry => {
            match bot_repo
                .update_exit_tp_client_oid_by_order_id(order_id_ref, &new_exit_client_oid)
                .await
            {
                Ok(_) => match side_ref.as_str() {
                    "buy" => {
                        let funds = match funds_clone {
                            Some(funds) => funds,
                            None => anyhow::bail!("Fail parse funds"),
                        };
                        make_hf_funds_margin_order(
                            message_repo,
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
                            None => anyhow::bail!("Fail parse size"),
                        };
                        make_hf_size_margin_order(
                            message_repo,
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
                        anyhow::bail!("Fail match side_clone:{}", side_ref)
                    }
                },
                Err(e) => {
                    error!("{:#}", e);
                    anyhow::bail!("{:#}", e)
                }
            }
        }
        StopType::Unknown => {
            error!("Fail match stop_clone:{}", order.stop);
            anyhow::bail!("Fail match stop_clone:{}", order.stop)
        }
    };

    match order_result {
        Ok(_) => {
            info!(
                "✅ Order re-placed: {} {}",
                order_id_ref, new_exit_client_oid,
            );
        }
        Err(e) => {
            anyhow::bail!(
                "❌ Order failed: {} {} {}",
                order_id_ref,
                new_exit_client_oid,
                e
            )
        }
    }
    Ok(())
}
