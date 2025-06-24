use duva::{
    ENV, Environment, StartUpFacade,
    adapters::op_logs::{disk_based::FileOpLogs, memory_based::MemoryOpLogs},
};
use tracing_subscriber::fmt::format::FmtSpan;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // bootstrap dependencies

    //tracing-subscriber::FmtSubscriber, which prints formatted traces to standard output.
    let span_events = if ENV.log_level <= tracing::Level::DEBUG {
        FmtSpan::ENTER | FmtSpan::CLOSE // Detailed logging in debug mode
    } else {
        FmtSpan::CLOSE // Only timing info in production
    };
    tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(ENV.log_level)
        .with_span_events(span_events)
        .init(); // Initialize the subscriber

    let topology_writer = Environment::open_topology_file(ENV.tpp.clone()).await;

    // ! should we support type erasure?

    if ENV.append_only {
        let local_aof = FileOpLogs::new(ENV.dir.clone())?;
        let start_up_runner = StartUpFacade::new(local_aof, topology_writer);
        start_up_runner.run().await
    } else {
        let in_memory_aof = MemoryOpLogs::default();
        let start_up_runner = StartUpFacade::new(in_memory_aof, topology_writer);
        start_up_runner.run().await
    }
}
