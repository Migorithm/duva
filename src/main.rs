use std::sync::OnceLock;

use redis_starter_rust::{
    adapters::{cancellation_token::CancellationToken, endec::EnDecoder},
    services::{
        config::config_actor::Config,
        query_manager::interface::{TRead, TWrite},
    },
    start_up, TSocketListener,
};
use tokio::net::TcpListener;

const NUM_OF_PERSISTENCE: usize = 10;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // bootstrap dependencies
    let config = config();
    let listener = AppStreamListener(TcpListener::bind(config.bind_addr()).await?);
    start_up::<CancellationToken>(config, NUM_OF_PERSISTENCE, EnDecoder, (), listener).await
}

static CONFIG: OnceLock<Config> = OnceLock::new();

pub fn config() -> &'static Config {
    CONFIG.get_or_init(Config::default)
}

struct AppStreamListener(TcpListener);

impl TSocketListener for AppStreamListener {
    async fn accept(&self) -> anyhow::Result<(impl TWrite + TRead, std::net::SocketAddr)> {
        self.0.accept().await.map_err(Into::into)
    }
}
