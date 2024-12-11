use redis_starter_rust::{
    adapters::{
        cancellation_token::CancellationTokenFactory,
        io::tokio_stream::{TokioConnectStreamFactory, TokioStreamListenerFactory},
    },
    services::config::config_actor::Config,
    StartUpFacade,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // bootstrap dependencies
    let config = Config::default();

    let start_up_runner = StartUpFacade::new(
        TokioConnectStreamFactory,
        TokioStreamListenerFactory,
        CancellationTokenFactory,
        config,
        (),
    );
    start_up_runner.run().await
}
