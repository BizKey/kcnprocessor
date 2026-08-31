use crate::api::models::OrderSide;
use crate::logic::order_side_counter::ORDER_SIDE_COUNTER;
use anyhow::Result;
use rust_decimal::Decimal;
use std::str::FromStr;
use uuid::Uuid;

pub fn generate_entry_id() -> String {
    Uuid::new_v4().to_string()
}

pub fn format_assert_decimal(size: Decimal, increment: Decimal) -> Result<String> {
    Ok(size
        .trunc_with_scale(increment.scale())
        .normalize()
        .to_string())
}

pub fn get_next_side() -> OrderSide {
    ORDER_SIDE_COUNTER.next_side()
}

/// Процент для TP при покупке
pub fn tp_buy_percent() -> Result<Decimal> {
    Decimal::from_str("1.01").map_err(|e| anyhow::anyhow!(e))
}

/// Процент для SL при покупке
pub fn sl_buy_percent() -> Result<Decimal> {
    Decimal::from_str("0.99").map_err(|e| anyhow::anyhow!(e))
}

/// Процент для TP при продаже
pub fn tp_sell_percent() -> Result<Decimal> {
    Decimal::from_str("0.99").map_err(|e| anyhow::anyhow!(e))
}

/// Процент для SL при продаже
pub fn sl_sell_percent() -> Result<Decimal> {
    Decimal::from_str("1.01").map_err(|e| anyhow::anyhow!(e))
}

pub const AUTO_CLEAN_DELAY: tokio::time::Duration = tokio::time::Duration::from_secs(5);

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    #[test]
    fn test_format_assert_decimal_with_precision() {
        // Тестируем с разной точностью
        let test_cases = vec![
            // (size, increment, expected)
            (
                Decimal::from_str("123.456789123456").unwrap(),
                Decimal::from_str("0.000000000001").unwrap(),
                "123.456789123456",
            ),
            (
                Decimal::from_str("123.45678912345").unwrap(),
                Decimal::from_str("0.00000000001").unwrap(),
                "123.45678912345",
            ),
            (
                Decimal::from_str("123.4567891234").unwrap(),
                Decimal::from_str("0.0000000001").unwrap(),
                "123.4567891234",
            ),
            (
                Decimal::from_str("123.456789123").unwrap(),
                Decimal::from_str("0.000000001").unwrap(),
                "123.456789123",
            ),
            (
                Decimal::from_str("123.45678912").unwrap(),
                Decimal::from_str("0.00000001").unwrap(),
                "123.45678912",
            ),
            (
                Decimal::from_str("123.4567891").unwrap(),
                Decimal::from_str("0.0000001").unwrap(),
                "123.4567891",
            ),
            (
                Decimal::from_str("123.456789").unwrap(),
                Decimal::from_str("0.000001").unwrap(),
                "123.456789",
            ),
            (
                Decimal::from_str("123.456789").unwrap(),
                Decimal::from_str("0.00001").unwrap(),
                "123.45678",
            ),
            (
                Decimal::from_str("123.456789").unwrap(),
                Decimal::from_str("0.0001").unwrap(),
                "123.4567",
            ),
            (
                Decimal::from_str("123.456789").unwrap(),
                Decimal::from_str("0.001").unwrap(),
                "123.456",
            ),
            (
                Decimal::from_str("123.456789").unwrap(),
                Decimal::from_str("0.01").unwrap(),
                "123.45",
            ),
            (
                Decimal::from_str("123.456789").unwrap(),
                Decimal::from_str("0.1").unwrap(),
                "123.4",
            ),
        ];

        for (size, increment, expected) in test_cases {
            let result = format_assert_decimal(size, increment).unwrap();
            assert_eq!(
                result, expected,
                "Failed for size: {}, increment: {}",
                size, increment
            );
        }
    }

    #[test]
    fn test_format_assert_decimal_no_precision() {
        // Тестируем с целыми числами (precision = 0)
        let test_cases = vec![(
            Decimal::from_str("123.456").unwrap(),
            Decimal::from_str("1").unwrap(),
            "123",
        )];

        for (size, increment, expected) in test_cases {
            let result = format_assert_decimal(size, increment).unwrap();
            assert_eq!(
                result, expected,
                "Failed for size: {}, increment: {}",
                size, increment
            );
        }
    }
}
