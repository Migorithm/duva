mod common;
use common::{array, spawn_server_as_slave, spawn_server_process, wait_for_message};
use duva::client_utils::ClientStreamHandler;

#[tokio::test]
async fn test_make_peer_discovery_increase_known_node_count() {
    // GIVEN
    let mut master_process = spawn_server_process();

    let cmd = &array(vec!["cluster", "info"]);

    // WHEN
    let mut replica_process = spawn_server_as_slave(&master_process);
    let mut stdout_for_repl1 = replica_process.stdout.take().unwrap();
    wait_for_message(&mut stdout_for_repl1, "[INFO] Received peer state from master", 1);
    let mut master_stdout = master_process.stdout.take().unwrap();
    wait_for_message(&mut master_stdout, "[INFO] Received peer state from slave", 1);

    //THEN
    let mut h = ClientStreamHandler::new(master_process.bind_addr()).await;
    h.send(cmd).await;
    let cluster_info = h.get_response().await;
    assert_eq!(cluster_info, array(vec!["cluster_known_nodes:1"]));

    // WHEN2
    let mut replica_process2 = spawn_server_as_slave(&master_process);
    let mut stdout_for_repl2 = replica_process2.stdout.take().unwrap();
    wait_for_message(&mut stdout_for_repl2, "[INFO] Received peer state from master", 1);

    // THEN2
    let mut h = ClientStreamHandler::new(master_process.bind_addr()).await;
    h.send(cmd).await;
    let cluster_info = h.get_response().await;
    assert_eq!(cluster_info, array(vec!["cluster_known_nodes:2"]));
}

#[tokio::test]
async fn system_removes_node_when_heartbeat_is_not_received_for_certain_time() {
    // GIVEN
    let mut master_process = spawn_server_process();

    let cmd = &array(vec!["cluster", "info"]);

    let mut replica_process = spawn_server_as_slave(&master_process);
    let mut stdout_for_repl1 = replica_process.stdout.take().unwrap();
    wait_for_message(&mut stdout_for_repl1, "[INFO] Received peer state from master", 1);
    let mut master_stdout = master_process.stdout.take().unwrap();
    wait_for_message(&mut master_stdout, "[INFO] Received peer state from slave", 1);
    let mut h = ClientStreamHandler::new(master_process.bind_addr()).await;
    h.send(cmd).await;
    let cluster_info = h.get_response().await;
    assert_eq!(cluster_info, array(vec!["cluster_known_nodes:1"]));

    // WHEN
    replica_process.kill().unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    h.send(cmd).await;
    let cluster_info = h.get_response().await;

    //THEN
    assert_eq!(cluster_info, array(vec!["cluster_known_nodes:0"]));
}
