use std::sync::OnceLock;

use redis_starter_rust::{adapters::persistence::decoder::Decoder, config::Config, start_up};

const NUM_OF_PERSISTENCE: usize = 10;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    start_up(config(), NUM_OF_PERSISTENCE, Decoder).await
}

static CONFIG: OnceLock<Config> = OnceLock::new();

pub fn config() -> &'static Config {
    CONFIG.get_or_init(Config::default)
}
