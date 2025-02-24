mod common;
use common::{ServerEnv, array, spawn_server_process};
use duva::client_utils::ClientStreamHandler;

#[tokio::test]
async fn test_cluster_known_nodes_increase_when_new_replica_is_added() {
    // GIVEN
    let env = ServerEnv::default();
    let mut leader_p = spawn_server_process(&env);
    let mut client_handler = ClientStreamHandler::new(leader_p.bind_addr()).await;

    let cmd = &array(vec!["cluster", "info"]);

    let repl_env = ServerEnv::default().with_leader_bind_addr(leader_p.bind_addr().into());
    let mut repl_p = spawn_server_process(&repl_env);
    repl_p.wait_for_message(&leader_p.heartbeat_msg(0), 1).unwrap();
    leader_p.wait_for_message(&repl_p.heartbeat_msg(0), 1).unwrap();

    let cluster_info = client_handler.send_and_get(cmd).await;
    assert_eq!(cluster_info, array(vec!["cluster_known_nodes:1"]));

    // WHEN -- new replica is added
    let repl_env2 = ServerEnv::default().with_leader_bind_addr(leader_p.bind_addr().into());
    let mut new_repl_p = spawn_server_process(&repl_env2);
    new_repl_p.wait_for_message(&leader_p.heartbeat_msg(0), 1).unwrap();

    //THEN
    //TODO following is flaky when run with other tests!
    // left: "*1\r\n$21\r\ncluster_known_nodes:1\r\n"
    // right: b"*1\r\n$21\r\ncluster_known_nodes:2\r\n"
    let cluster_info = client_handler.send_and_get(cmd).await;
    assert_eq!(cluster_info, array(vec!["cluster_known_nodes:2"]));
}
