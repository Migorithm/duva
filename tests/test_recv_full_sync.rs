/// After three-way handshake, client will receive peers from the leader server
mod common;
use crate::common::array;
use common::{ServerEnv, check_internodes_communication, spawn_server_process};
use duva::client_utils::ClientStreamHandler;

#[tokio::test]
async fn test_receive_full_sync() {
    // GIVEN
    let env = ServerEnv::default();
    // Start the leader server as a child process
    let mut leader_p = spawn_server_process(&env);
    let mut h = ClientStreamHandler::new(leader_p.bind_addr()).await;

    h.send_and_get(&array(vec!["SET", "foo", "bar"])).await;
    assert_eq!(h.send_and_get(&array(vec!["KEYS", "*"])).await, array(vec!["foo"]));

    // WHEN run replica
    let repl_env = ServerEnv::default().with_leader_bind_addr(leader_p.bind_addr().into());
    let mut replica_process = spawn_server_process(&repl_env);

    check_internodes_communication(&mut [&mut leader_p, &mut replica_process], 0, 1000).unwrap();

    // THEN
    std::thread::sleep(std::time::Duration::from_millis(1000));
    let mut client_to_repl = ClientStreamHandler::new(replica_process.bind_addr()).await;

    assert_eq!(
        client_to_repl.send_and_get(&array(vec!["GET", "foo"])).await,
        "$3\r\nbar\r\n".to_string()
    );
}
