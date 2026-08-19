use serde::{Deserialize, Serialize};

/// Сторона ордера
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum OrderSide {
    Buy,
    Sell,
}
