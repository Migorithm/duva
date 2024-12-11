use redis_starter_rust::{
    adapters::{
        cancellation_token::CancellationTokenFactory,
        io::tokio_stream::{TokioConnectStreamFactory, TokioStreamListenerFactory},
    },
    services::config::config_actor::Config,
    start_up,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // bootstrap dependencies
    let config = Config::default();
    start_up(
        TokioConnectStreamFactory,
        TokioStreamListenerFactory,
        CancellationTokenFactory,
        config,
        (),
    )
    .await
}
