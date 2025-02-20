/// After three-way handshake, client will receive peers from the leader server
mod common;
use common::{spawn_server_as_follower, spawn_server_process};

#[tokio::test]
async fn test_disseminate_peers() {
    // GIVEN
    // Start the leader server as a child process
    let leader_p = spawn_server_process();

    // WHEN run replica
    let mut replica_process = spawn_server_as_follower(leader_p.bind_addr());

    // THEN
    replica_process.wait_for_message("[INFO] Received peer list: []", 1).unwrap();
}
