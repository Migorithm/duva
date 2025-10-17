use duva::{
    ENV, Environment, StartUpFacade,
    adapters::op_logs::{disk_based::FileOpLogs, memory_based::MemoryOpLogs},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let topology_writer = Environment::open_topology_file(ENV.tpp.clone()).await;

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
