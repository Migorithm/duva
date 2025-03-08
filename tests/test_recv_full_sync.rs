/// After three-way handshake, client will receive peers from the leader server
mod common;
use crate::common::array;
use common::{spawn_server_process, ServerEnv};
use duva::client_utils::ClientStreamHandler;

#[tokio::test]
async fn test_receive_full_sync() {
    // GIVEN
    let env = ServerEnv::default();
    // Start the leader server as a child process
    let leader_p = spawn_server_process(&env);
    let mut h = ClientStreamHandler::new(leader_p.bind_addr()).await;

    h.send_and_get(&array(vec!["SET", "foo", "bar"])).await;
    assert_eq!(h.send_and_get(&array(vec!["KEYS", "*"])).await, array(vec!["foo"]));

    // WHEN run replica
    let repl_env = ServerEnv::default().with_leader_bind_addr(leader_p.bind_addr().into());
    let mut replica_process = spawn_server_process(&repl_env);

    // THEN
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    replica_process.wait_for_message("[INFO] Received Leader State - length 1", 1).unwrap();
}
