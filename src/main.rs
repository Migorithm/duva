use redis_starter_rust::{
    adapters::{
        cancellation_token::CancellationTokenFactory,
        io::tokio_stream::{TokioConnectStreamFactory, TokioStreamListenerFactory},
    },
    services::config::{config_actor::Config, config_manager::ConfigManager},
    StartUpFacade,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // bootstrap dependencies
    let config_manager = ConfigManager::new(Config::default());
    let start_up_runner = StartUpFacade::new(
        TokioConnectStreamFactory,
        TokioStreamListenerFactory,
        CancellationTokenFactory,
        config_manager,
    );
    start_up_runner.run(()).await
}
