use duva::{
    Environment, StartUpFacade,
    adapters::aof::local_aof::LocalAof,
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

    //TODO refactor the following
    let local_aof = LocalAof::new(env.dbfilename.to_string() + ".aof").await?;

    let start_up_runner = StartUpFacade::new(config_manager, env, local_aof);

    start_up_runner.run().await
}
