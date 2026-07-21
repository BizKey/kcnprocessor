use tokio::time::Duration;
pub const DELETE_STOP_ORDER_DELAY: Duration = Duration::from_secs(1);
pub const RECONNECT_DELAY: Duration = Duration::from_secs(5);
pub const PING_INTERVAL: Duration = Duration::from_secs(5);
pub const INIT_ORDER_DELAY: Duration = Duration::from_secs(5);
pub const EXCHANGE: &str = "kucoin";
