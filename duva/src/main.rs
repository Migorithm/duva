use duva::{
    Environment, StartUpFacade,
    adapters::op_logs::{disk_based::FileOpLogs, memory_based::MemoryOpLogs},
    domains::config_actors::{actor::ConfigActor, config_manager::ConfigManager},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // bootstrap dependencies
    let mut env = Environment::init().await;
    let config_manager = ConfigManager::new(
        ConfigActor::new(env.dir.clone(), env.dbfilename.clone()),
        env.host.clone(),
        env.port,
    );

    // ! should we support type erasure?

    if env.append_only {
        let local_aof = FileOpLogs::new(env.dir.clone()).await?;
        let start_up_runner = StartUpFacade::new(config_manager, &mut env, local_aof);
        start_up_runner.run(env).await
    } else {
        let in_memory_aof = MemoryOpLogs::default();
        let start_up_runner = StartUpFacade::new(config_manager, &mut env, in_memory_aof);
        start_up_runner.run(env).await
    }
}
