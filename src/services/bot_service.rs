use anyhow::Result;
use sqlx::PgPool;
use rust_decimal::Decimal;
use crate::api::repository::{BotRepository, OrderRepository, SymbolRepository};
use crate::api::models::{Bot, OrderData};
use anyhow::Result;
use async_trait::async_trait;
use rust_decimal::Decimal;

use crate::api::models::{Bot, OrderData};
use crate::core::repository_traits::{
    BotRepositoryTrait, 
    OrderRepositoryTrait, 
    SymbolRepositoryTrait,
    PositionRepositoryTrait,
};


pub struct BotService<B, O, S, P> {
    bot_repo: B,
    order_repo: O,
    symbol_repo: S,
    position_repo: P,
}

impl<B, O, S, P> BotService<B, O, S, P>
where
    B: BotRepositoryTrait,
    O: OrderRepositoryTrait,
    S: SymbolRepositoryTrait,
    P: PositionRepositoryTrait,
{
    pub fn new(
        bot_repo: B,
        order_repo: O,
        symbol_repo: S,
        position_repo: P,
    ) -> Self {
        Self {
            bot_repo,
            order_repo,
            symbol_repo,
            position_repo,
        }
    }

    pub async fn process_entry_order(&self, client_oid: &str, order: &OrderData) -> Result<()> {
        // Реализация логики
        Ok(())
    }

    pub async fn process_exit_tp_order(&self, bot: Bot, client_oid: &str, order: &OrderData) -> Result<()> {
        // Реализация логики
        Ok(())
    }

    pub async fn process_exit_sl_order(&self, bot: Bot, client_oid: &str, order: &OrderData) -> Result<()> {
        // Реализация логики
        Ok(())
    }
}