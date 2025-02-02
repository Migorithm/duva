mod common;
use common::{array, spawn_server_as_slave, spawn_server_process};
use duva::client_utils::ClientStreamHandler;

#[tokio::test]
async fn test_cluster_known_nodes_increase_when_new_replica_is_added() {
    // GIVEN
    let mut master_p = spawn_server_process();
    let mut client_handler = ClientStreamHandler::new(master_p.bind_addr()).await;

    let cmd = &array(vec!["cluster", "info"]);

    let mut repl_p = spawn_server_as_slave(&master_p);
    repl_p.wait_for_message(&master_p.heartbeat_msg(0), 1);
    master_p.wait_for_message(&repl_p.heartbeat_msg(0), 1);

    let cluster_info = client_handler.send_and_get(cmd).await;
    assert_eq!(cluster_info, array(vec!["cluster_known_nodes:1"]));

    // WHEN -- new replica is added
    let mut new_repl_p = spawn_server_as_slave(&master_p);
    new_repl_p.wait_for_message(&master_p.heartbeat_msg(0), 1);

    //THEN
    let cluster_info = client_handler.send_and_get(cmd).await;
    assert_eq!(cluster_info, array(vec!["cluster_known_nodes:2"]));
}

#[tokio::test]
async fn test_removes_node_when_heartbeat_is_not_received_for_certain_time() {
    // GIVEN
    let mut master_p = spawn_server_process();

    let cmd = &array(vec!["cluster", "info"]);

    let mut repl_p = spawn_server_as_slave(&master_p);
    repl_p.wait_for_message(&master_p.heartbeat_msg(0), 1);

    master_p.wait_for_message(&repl_p.heartbeat_msg(0), 1);

    let mut h = ClientStreamHandler::new(master_p.bind_addr()).await;
    let cluster_info = h.send_and_get(cmd).await;
    assert_eq!(cluster_info, array(vec!["cluster_known_nodes:1"]));

    // WHEN
    repl_p.kill().unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    let cluster_info = h.send_and_get(cmd).await;

    //THEN
    assert_eq!(cluster_info, array(vec!["cluster_known_nodes:0"]));
}
