/// After three-way handshake, client will receive peers from the master server
mod common;
use common::{get_available_port, run_server_process, wait_for_message};

#[tokio::test]
async fn test_disseminate_peers() {
    // GIVEN - master server configuration
    // Find free ports for the master and replica
    let master_port = get_available_port();

    // Start the master server as a child process
    let mut master_process = run_server_process(master_port, None);

    let master_stdout = master_process.stdout.take();
    wait_for_message(
        master_stdout.expect("failed to take stdout"),
        format!("listening peer connection on localhost:{}...", master_port + 10000).as_str(),
        1,
    );

    // WHEN run replica
    let replica_port = get_available_port();
    let mut replica_process =
        run_server_process(replica_port, Some(format!("localhost:{}", master_port)));

    // Read stdout from the replica process
    let mut stdout = replica_process.stdout.take();
    wait_for_message(stdout.take().unwrap(), "[INFO] Received peer list: []", 1);
}
