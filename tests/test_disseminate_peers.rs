/// After three-way handshake, client will receive peers from the leader server
mod common;
use common::{ServerEnv, spawn_server_process};

#[tokio::test]
async fn test_disseminate_peers() {
    // GIVEN
    // Start the leader server as a child process
    let env = ServerEnv::default();
    let leader_p = spawn_server_process(&env);

    // WHEN run replica
    let repl_env = ServerEnv::default().with_leader_bind_addr(leader_p.bind_addr().into());
    let mut replica_process = spawn_server_process(&repl_env);

    // THEN
    replica_process.wait_for_message("[INFO] Received peer list: []", 1).unwrap();
}
