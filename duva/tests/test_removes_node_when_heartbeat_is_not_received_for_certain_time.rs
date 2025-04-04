mod common;
use common::{ServerEnv, array, spawn_server_process};
use duva::{clients::ClientStreamHandler, domains::query_parsers::query_io::QueryIO};

#[tokio::test]
async fn test_removes_node_when_heartbeat_is_not_received_for_certain_time() {
    // GIVEN
    let env = ServerEnv::default();
    let mut leader_p = spawn_server_process(&env);

    let cmd = &array(vec!["cluster", "info"]);
    let repl_env = ServerEnv::default().with_leader_bind_addr(leader_p.bind_addr().into());
    let mut repl_p = spawn_server_process(&repl_env);

    repl_p.wait_for_message(&leader_p.heartbeat_msg(0), 1).unwrap();
    leader_p.wait_for_message(&repl_p.heartbeat_msg(0), 1).unwrap();

    let mut h = ClientStreamHandler::new(leader_p.bind_addr()).await;
    let cluster_info = h.send_and_get(cmd).await;
    assert_eq!(cluster_info, QueryIO::BulkString("cluster_known_nodes:1".into()).serialize());

    // WHEN
    repl_p.kill().unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    let cluster_info = h.send_and_get(cmd).await;

    //THEN
    assert_eq!(cluster_info, QueryIO::BulkString("cluster_known_nodes:0".into()).serialize());
}
