use anyhow::Result;
use micromap::Map;
use sqlx::PgPool;
use tokio::time::sleep;
use tracing::{error, info};

use crate::api::requests::{
    api_v3_hf_margin_stop_order_cancel_by_id_delete, api_v3_hf_margin_stop_orders_get,
    build_query_string,
};
use crate::constants::DELETE_STOP_ORDER_DELAY;
use crate::logic::logic::auto_clean_account;

pub async fn cancel_all_stop_orders() -> Result<()> {
    loop {
        let mut query_params: Map<&str, &str, 8> = Map::new();
        query_params.insert("pageSize", "10");

        let query_params = match build_query_string(query_params) {
            Ok(query_params) => query_params,
            Err(e) => {
                error!("{:#}", e);
                continue;
            }
        };

        let open_stop_orders = match api_v3_hf_margin_stop_orders_get(&query_params).await {
            Ok(open_stop_orders) => open_stop_orders,
            Err(e) => {
                error!("{:#}", e);
                continue;
            }
        };

        let open_stop_orders = match open_stop_orders {
            Some(open_stop_orders) => {
                info!(
                    "Stop orders: current_page:{} page_size:{} total_num:{} total_page:{}",
                    open_stop_orders.current_page,
                    open_stop_orders.page_size,
                    open_stop_orders.total_num,
                    open_stop_orders.total_page
                );
                open_stop_orders
            }
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

            let mut query_params: Map<&str, &str, 8> = Map::new();
            query_params.insert("orderId", &stop_order.id);

            let query_params = match build_query_string(query_params) {
                Ok(query_params) => query_params,
                Err(e) => {
                    error!("{:#}", e);
                    continue;
                }
            };

            let canceled_stop_order =
                match api_v3_hf_margin_stop_order_cancel_by_id_delete(&query_params).await {
                    Ok(canceled_stop_order) => canceled_stop_order,
                    Err(e) => {
                        error!("{:#}", e);
                        continue;
                    }
                };

            let canceled_stop_order = match canceled_stop_order {
                Some(canceled_stop_order) => canceled_stop_order,
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

pub async fn clean_account(pool: &PgPool) -> Result<()> {
    loop {
        let is_completed = match auto_clean_account(pool).await {
            Ok(is_completed) => is_completed,
            Err(e) => {
                error!("{:#}", e);
                continue;
            }
        };
        if is_completed {
            info!("auto_clean_account success");
            break;
        }
    }
    Ok(())
}
