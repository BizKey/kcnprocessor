use crate::logic::order_side_counter::ORDER_SIDE_COUNTER;
use anyhow::{Context, Result};
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use std::str::FromStr;

use crate::api::models::OrderSide;

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

pub fn get_next_side() -> OrderSide {
    ORDER_SIDE_COUNTER.next_side()
}

/// Процент для TP при покупке
pub fn tp_buy_percent() -> Result<Decimal> {
    Ok(Decimal::from_str("1.07").map_err(|e| anyhow::anyhow!(e))?)
}

/// Процент для SL при покупке
pub fn sl_buy_percent() -> Result<Decimal> {
    Ok(Decimal::from_str("0.95").map_err(|e| anyhow::anyhow!(e))?)
}

/// Процент для TP при продаже
pub fn tp_sell_percent() -> Result<Decimal> {
    Ok(Decimal::from_str("0.93").map_err(|e| anyhow::anyhow!(e))?)
}

/// Процент для SL при продаже
pub fn sl_sell_percent() -> Result<Decimal> {
    Ok(Decimal::from_str("1.05").map_err(|e| anyhow::anyhow!(e))?)
}

pub const RETRY_DELAY_BASE: u64 = 500;
pub const BOT_INIT_DELAY: tokio::time::Duration = tokio::time::Duration::from_secs(5);
pub const AUTO_CLEAN_DELAY: tokio::time::Duration = tokio::time::Duration::from_secs(5);
