/// After three-way handshake, client will receive peers from the leader server
mod common;
use crate::common::array;
use common::{ServerEnv, spawn_server_process};
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
    let replica_process = spawn_server_process(&repl_env);

    // THEN
    let mut client_to_repl = ClientStreamHandler::new(replica_process.bind_addr()).await;
    assert_eq!(client_to_repl.send_and_get(&array(vec!["GET", "foo"])).await, array(vec!["bar"]));
}
