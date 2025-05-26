use duva::{
    Environment, StartUpFacade,
    adapters::op_logs::{disk_based::FileOpLogs, memory_based::MemoryOpLogs},
    domains::config_actors::{actor::ConfigActor, config_manager::ConfigManager},
};
use tracing_subscriber::fmt::format::FmtSpan;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // bootstrap dependencies
    let mut env = Environment::init().await;
    let config_manager = ConfigManager::new(
        ConfigActor::new(env.dir.clone(), env.dbfilename.clone()),
        env.host.clone(),
        env.port,
    );

    //tracing-subscriber::FmtSubscriber, which prints formatted traces to standard output.
    let span_events = if env.log_level <= tracing::Level::DEBUG {
        FmtSpan::ENTER | FmtSpan::CLOSE // Detailed logging in debug mode
    } else {
        FmtSpan::CLOSE // Only timing info in production
    };
    tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(env.log_level)
        .with_span_events(span_events)
        .init(); // Initialize the subscriber

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
