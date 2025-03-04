use duva::{
    Environment, StartUpFacade,
    adapters::wal::{local_wal::LocalAof, memory_aof::InMemoryAof},
    domains::config_actors::{actor::ConfigActor, config_manager::ConfigManager},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // bootstrap dependencies
    let env = Environment::new();
    let config_manager = ConfigManager::new(
        ConfigActor::new(env.dir.clone(), env.dbfilename.clone()),
        env.host.clone(),
        env.port,
    );

    // ! should we support type erasure?
    if env.append_only {
        let local_aof = LocalAof::new(env.dbfilename.to_string() + ".aof").await?;
        let start_up_runner = StartUpFacade::new(config_manager, env, local_aof);
        start_up_runner.run().await
    } else {
        let in_memory_aof = InMemoryAof::default();
        let start_up_runner = StartUpFacade::new(config_manager, env, in_memory_aof);
        start_up_runner.run().await
    }
}
