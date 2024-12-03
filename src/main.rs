use std::sync::OnceLock;

use redis_starter_rust::{
    adapters::{cancellation_token::CancellationToken, endec::EnDecoder},
    config::Config,
    start_up,
};

const NUM_OF_PERSISTENCE: usize = 10;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // bootstrap dependencies
    start_up::<CancellationToken>(config(), NUM_OF_PERSISTENCE, EnDecoder, ()).await
}

static CONFIG: OnceLock<Config> = OnceLock::new();

pub fn config() -> &'static Config {
    CONFIG.get_or_init(Config::default)
}
