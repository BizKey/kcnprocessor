use anyhow::Result;
use sqlx::PgPool;
use rust_decimal::Decimal;
use crate::api::repository::{BotRepository, OrderRepository, SymbolRepository};
use crate::api::models::{Bot, OrderData};

pub struct BotService {
    bot_repo: BotRepository,
    order_repo: OrderRepository,
    symbol_repo: SymbolRepository,
}

impl BotService {
    pub fn new(pool: PgPool) -> Self {
        Self {
            bot_repo: BotRepository::new(pool.clone()),
            order_repo: OrderRepository::new(pool.clone()),
            symbol_repo: SymbolRepository::new(pool.clone()),
        }
    }
    
    pub async fn process_entry_order(&self, client_oid: &str, order: &OrderData) -> Result<()> {
        
        Ok(())
    }
    
    pub async fn process_exit_tp_order(&self, bot: Bot, client_oid: &str, order: &OrderData) -> Result<()> {
        
        Ok(())
    }
    
    pub async fn process_exit_sl_order(&self, bot: Bot, client_oid: &str, order: &OrderData) -> Result<()> {
        
        Ok(())
    }
}