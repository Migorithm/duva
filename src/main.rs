use redis_starter_rust::{
    adapters::cancellation_token::CancellationTokenFactory,
    services::config::{actor::ConfigActor, manager::ConfigManager},
    StartUpFacade,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // bootstrap dependencies
    let config_manager = ConfigManager::new(ConfigActor::default());

    let mut start_up_runner = StartUpFacade::new(CancellationTokenFactory, config_manager);

    start_up_runner.run(()).await
}
