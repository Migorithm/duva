use duva::{
    services::config::{actor::ConfigActor, manager::ConfigManager},
    StartUpFacade,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // bootstrap dependencies
    let config_manager = ConfigManager::new(ConfigActor::default());

    let start_up_runner = StartUpFacade::new(config_manager);

    start_up_runner.run(()).await
}
