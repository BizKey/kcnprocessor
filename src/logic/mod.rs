pub mod account_handlers;
pub mod handlers;
pub mod order_handlers;
pub mod order_processor;
pub mod stop_order_handlers;
pub mod utils;

// Re-export для обратной совместимости
pub use account_handlers::{
    auto_clean_account, clean_account, get_all_accounts_data, get_token_price, repay_account,
    transfer_in_account,
};
pub use order_handlers::{
    create_init_orders, make_hf_funds_margin_order, make_hf_size_margin_order, make_random_trade,
};
pub use order_processor::{
    process_bot_by_entry_client_oid, process_bot_by_exit_sl_client_oid,
    process_bot_by_exit_tp_client_oid, trade_order_event,
};
pub use stop_order_handlers::{cancel_all_stop_orders, handle_advanced_orders};
