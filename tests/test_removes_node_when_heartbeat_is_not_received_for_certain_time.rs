mod common;
use common::{array, spawn_server_as_follower, spawn_server_process};
use duva::client_utils::ClientStreamHandler;

#[tokio::test]
async fn test_removes_node_when_heartbeat_is_not_received_for_certain_time() {
    // GIVEN
    let mut leader_p = spawn_server_process(None);

    let cmd = &array(vec!["cluster", "info"]);

    let mut repl_p = spawn_server_as_follower(leader_p.bind_addr());
    repl_p.wait_for_message(&leader_p.heartbeat_msg(0), 1).unwrap();

    leader_p.wait_for_message(&repl_p.heartbeat_msg(0), 1).unwrap();

    let mut h = ClientStreamHandler::new(leader_p.bind_addr()).await;
    let cluster_info = h.send_and_get(cmd).await;
    assert_eq!(cluster_info, array(vec!["cluster_known_nodes:1"]));

    // WHEN
    repl_p.kill().unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    let cluster_info = h.send_and_get(cmd).await;

    //THEN
    assert_eq!(cluster_info, array(vec!["cluster_known_nodes:0"]));
}
