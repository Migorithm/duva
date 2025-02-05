/// Cluster forget {node id} is used in order to remove a node, from the set of known nodes for the node receiving the command.
/// In other words the specified node is removed from the nodes table of the node receiving the command.
mod common;
use common::{array, check_internodes_communication, spawn_server_as_slave, spawn_server_process};
use duva::client_utils::ClientStreamHandler;

#[tokio::test]
async fn test_cluster_forget_node() {
    // GIVEN
    const HOP_COUNT: usize = 0;
    let master_p = spawn_server_process();
    let repl_p = spawn_server_as_slave(&master_p);
    let mut processes = vec![master_p, repl_p];

    check_internodes_communication(&mut processes, HOP_COUNT, 2).unwrap();

    // WHEN
    let mut client_handler = ClientStreamHandler::new(processes[0].bind_addr()).await;
    let replica_id = processes[1].bind_addr();
    let cmd = &array(vec!["cluster", "forget", &replica_id]);
    let cluster_info = client_handler.send_and_get(cmd).await;

    // THEN
    assert_eq!(cluster_info, "+OK\r\n");

    // WHEN
    let cluster_info = client_handler.send_and_get(&array(vec!["cluster", "info"])).await;

    // THEN
    assert_eq!(cluster_info, array(vec!["cluster_known_nodes:0"]));
}

#[tokio::test]
async fn test_cluster_forget_node_return_error_when_wrong_id_given() {
    // GIVEN
    let master_p = spawn_server_process();

    // WHEN
    let mut client_handler = ClientStreamHandler::new(master_p.bind_addr()).await;
    let replica_id = "localhost:doesn't exist";
    let cmd = &array(vec!["cluster", "forget", &replica_id]);
    let cluster_info = client_handler.send_and_get(cmd).await;

    // THEN
    assert_eq!(cluster_info, "-No such peer\r\n");

    // WHEN
    let cluster_info = client_handler.send_and_get(&array(vec!["cluster", "info"])).await;

    // THEN
    assert_eq!(cluster_info, array(vec!["cluster_known_nodes:0"]));
}

#[tokio::test]
async fn test_cluster_forget_node_propagation() {
    // GIVEN
    const HOP_COUNT: usize = 0;
    let master_p = spawn_server_process();
    let repl_p = spawn_server_as_slave(&master_p);
    let repl_p2 = spawn_server_as_slave(&master_p);
    let mut processes = vec![master_p, repl_p, repl_p2];

    check_internodes_communication(&mut processes, HOP_COUNT, 2).unwrap();

    // WHEN
    const TIMEOUT: u64 = 2;
    let mut client_handler = ClientStreamHandler::new(processes[0].bind_addr()).await;
    let replica_id = processes[1].bind_addr();
    let cmd = &array(vec!["cluster", "forget", &replica_id]);
    client_handler.send_and_get(cmd).await;
    let never_arrivable_msg = processes[1].heartbeat_msg(1);

    // THEN - master_p and repl_p2 doesn't get message from repl_p2

    let res1 = processes[0].timed_wait_for_message(&never_arrivable_msg, HOP_COUNT, TIMEOUT);
    let res2 = processes[2].timed_wait_for_message(&never_arrivable_msg, HOP_COUNT, TIMEOUT);

    assert!(res1.is_err());
    assert!(res2.is_err());
}
