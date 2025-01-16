/// After three-way handshake, client will receive peers from the master server
mod common;
use common::{get_available_port, run_server_process, spawn_server_process, wait_for_message};

#[tokio::test]
async fn test_disseminate_peers() {
    // GIVEN
    // Start the master server as a child process
    let master_process = spawn_server_process();

    // WHEN run replica
    let replica_port = get_available_port();
    let mut replica_process = run_server_process(replica_port, Some(master_process.bind_addr()));

    // Read stdout from the replica process
    let mut stdout = replica_process.stdout.take();
    wait_for_message(stdout.take().unwrap(), "[INFO] Received peer list: []", 1);
}
