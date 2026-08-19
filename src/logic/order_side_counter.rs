use crate::logic::order_side::OrderSide;
use std::sync::atomic::{AtomicU8, Ordering};

/// Глобальный счетчик для чередования сторон ордеров
pub struct OrderSideCounter {
    counter: AtomicU8,
}

impl OrderSideCounter {
    /// Создает новый счетчик, начинающийся с "buy"
    pub const fn new() -> Self {
        Self {
            counter: AtomicU8::new(0),
        }
    }

    /// Возвращает следующую сторону (чередует buy/sell)
    pub fn next_side(&self) -> OrderSide {
        let current = self.counter.fetch_add(1, Ordering::Relaxed);
        if current % 2 == 0 {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        }
    }
}

/// Глобальный экземпляр счетчика
pub static ORDER_SIDE_COUNTER: OrderSideCounter = OrderSideCounter::new();
