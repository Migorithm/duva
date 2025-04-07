use crate::common::{ServerEnv, array, check_internodes_communication, spawn_server_process};
use duva::clients::ClientStreamHandler;

#[tokio::test]
async fn test_full_sync_on_newly_added_replica() {
    // GIVEN
    let env = ServerEnv::default();
    // Start the leader server as a child process
    let mut leader_p = spawn_server_process(&env);
    let mut h = ClientStreamHandler::new(leader_p.bind_addr()).await;

    h.send_and_get(&array(vec!["SET", "foo", "bar"])).await;

    // WHEN run replica
    let repl_env = ServerEnv::default().with_leader_bind_addr(leader_p.bind_addr().into());

    let mut replica_process = spawn_server_process(&repl_env);
    check_internodes_communication(&mut [&mut leader_p, &mut replica_process], 0, 1000).unwrap();

    // THEN
    let mut client_to_repl = ClientStreamHandler::new(replica_process.bind_addr()).await;
    assert_eq!(client_to_repl.send_and_get(&array(vec!["KEYS", "*"])).await, array(vec!["foo"]));
}
