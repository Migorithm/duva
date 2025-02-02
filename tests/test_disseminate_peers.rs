/// After three-way handshake, client will receive peers from the master server
mod common;
use common::{spawn_server_as_slave, spawn_server_process};

#[tokio::test]
async fn test_disseminate_peers() {
    // GIVEN
    // Start the master server as a child process
    let master_process = spawn_server_process();

    // WHEN run replica
    let mut replica_process = spawn_server_as_slave(&master_process);

    // THEN
    replica_process.wait_for_message("[INFO] Received peer list: []", 1);
}
