use duva::{ENV, Environment, StartUpFacade, adapters::loggers::op_logs::OperationLogs};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let topology_writer = Environment::open_topology_file(ENV.tpp.clone()).await;

    let op_logs = OperationLogs::new(ENV.append_only);

    let start_up_runner = StartUpFacade::new(op_logs, topology_writer);
    start_up_runner.run().await
}
