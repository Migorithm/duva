use duva::{
    ENV, Environment, StartUpFacade,
    adapters::op_logs::{disk_based::FileOpLogs, memory_based::MemoryOpLogs},
};
use futures::StreamExt;
use signal_hook::consts::{SIGINT, SIGQUIT, SIGTERM};
use signal_hook_tokio::Signals;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let signals = Signals::new(&[SIGTERM, SIGINT, SIGQUIT])?;
    let sig_handle = signals.handle();
    // Spawn task to handle signals
    let signals_task = tokio::spawn(handle_signals(signals));

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

async fn handle_signals(mut signals: Signals) -> anyhow::Result<()> {
    while let Some(signal) = signals.next().await {
        match signal {
            SIGTERM | SIGINT | SIGQUIT => {
                println!("\nReceived signal: {:?}", signal);
                println!("Initiating graceful shutdown...");
                std::process::abort();
            },
            _ => unreachable!(),
        }
    }
    Ok(())
}
