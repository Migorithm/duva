use redis_starter_rust::{
    adapters::{
        cancellation_token::CancellationToken, endec::EnDecoder,
        io::tokio_stream::AppStreamListener,
    },
    services::config::config_actor::Config,
    start_up,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // bootstrap dependencies
    let config = Config::default();
    start_up::<CancellationToken, AppStreamListener>(config, EnDecoder, ()).await
}
