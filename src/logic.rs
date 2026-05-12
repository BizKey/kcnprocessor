use fastrand;

pub fn get_random_side() -> String {
    if fastrand::bool() { "buy".to_string() } else { "sell".to_string() }
}
