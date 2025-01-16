/// After three-way handshake, client will receive peers from the master server
mod common;
use common::{spawn_server_as_slave, spawn_server_process, wait_for_message};

#[tokio::test]
async fn test_disseminate_peers() {
    // GIVEN
    // Start the master server as a child process
    let master_process = spawn_server_process();

    // WHEN run replica
    let mut replica_process = spawn_server_as_slave(&master_process);

    // Read stdout from the replica process
    let mut stdout = replica_process.stdout.take();
    wait_for_message(stdout.take().unwrap(), "[INFO] Received peer list: []", 1);
}
