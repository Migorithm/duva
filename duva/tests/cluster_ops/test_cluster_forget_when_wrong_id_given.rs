/// Cluster forget {node id} is used in order to remove a node, from the set of known nodes for the node receiving the command.
/// In other words the specified node is removed from the nodes table of the node receiving the command.
use crate::common::{ServerEnv, array, spawn_server_process};
use duva::{clients::ClientStreamHandler, domains::query_parsers::query_io::QueryIO};

#[tokio::test]
async fn test_cluster_forget_node_return_error_when_wrong_id_given() {
    // GIVEN

    let env = ServerEnv::default();
    let leader_p = spawn_server_process(&env);

    // WHEN
    let mut client_handler = ClientStreamHandler::new(leader_p.bind_addr()).await;
    let replica_id = "localhost:doesn't exist";
    let cmd = &array(vec!["cluster", "forget", &replica_id]);
    let cluster_info = client_handler.send_and_get(cmd).await;

    // THEN
    assert_eq!(cluster_info, "-No such peer\r\n");

    // WHEN
    let cluster_info = client_handler.send_and_get(&array(vec!["cluster", "info"])).await;

    // THEN
    assert_eq!(cluster_info, QueryIO::BulkString("cluster_known_nodes:0".into()).serialize());
}
