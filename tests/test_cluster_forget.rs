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
    let replica_id = dbg!(processes[1].bind_addr());
    let cmd = &array(vec!["cluster", "forget", &replica_id]);
    let cluster_info = client_handler.send_and_get(cmd).await;

    // THEN
    assert_eq!(cluster_info, array(vec!["OK"]));

    // WHEN
    let cluster_info = client_handler.send_and_get(&array(vec!["cluster", "info"])).await;

    // THEN
    assert_eq!(cluster_info, array(vec!["cluster_known_nodes:0"]));
}
