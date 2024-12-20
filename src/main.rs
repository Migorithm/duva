use redis_starter_rust::{
    adapters::cancellation_token::CancellationTokenFactory,
    services::{
        cluster::actor::ClusterActor,
        config::{config_actor::Config, config_manager::ConfigManager},
    },
    StartUpFacade,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // bootstrap dependencies
    let config_manager = ConfigManager::new(Config::default());
    let cluster_actor = ClusterActor::new();
    let start_up_runner =
        StartUpFacade::new(CancellationTokenFactory, config_manager, cluster_actor);
    start_up_runner.run(()).await
}
