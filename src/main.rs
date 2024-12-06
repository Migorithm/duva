use redis_starter_rust::{
    adapters::{
        cancellation_token::CancellationToken, endec::EnDecoder,
        io::tokio_stream::AppStreamListener,
    },
    services::config::config_actor::Config,
    start_up,
};

const NUM_OF_PERSISTENCE: usize = 10;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // bootstrap dependencies
    let config = Config::default();
    start_up::<CancellationToken, AppStreamListener>(config, NUM_OF_PERSISTENCE, EnDecoder, ())
        .await
}
