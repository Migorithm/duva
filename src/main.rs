use duva::{
    Environment, StartUpFacade,
    domains::{config_actors::actor::ConfigActor, config_actors::config_manager::ConfigManager},
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
    let start_up_runner = StartUpFacade::new(config_manager, env);

    start_up_runner.run().await
}
