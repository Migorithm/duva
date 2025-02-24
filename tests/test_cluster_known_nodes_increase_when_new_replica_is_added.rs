mod common;
use common::{FileName, array, spawn_server_as_follower, spawn_server_process};
use duva::client_utils::ClientStreamHandler;

#[tokio::test]
async fn test_cluster_known_nodes_increase_when_new_replica_is_added() {
    // GIVEN
    let file_name: FileName = FileName(None);
    let mut leader_p = spawn_server_process(&file_name);
    let mut client_handler = ClientStreamHandler::new(leader_p.bind_addr()).await;

    let cmd = &array(vec!["cluster", "info"]);

    let mut repl_p = spawn_server_as_follower(leader_p.bind_addr(), &file_name);
    repl_p.wait_for_message(&leader_p.heartbeat_msg(0), 1).unwrap();
    leader_p.wait_for_message(&repl_p.heartbeat_msg(0), 1).unwrap();

    let cluster_info = client_handler.send_and_get(cmd).await;
    assert_eq!(cluster_info, array(vec!["cluster_known_nodes:1"]));

    // WHEN -- new replica is added
    let mut new_repl_p = spawn_server_as_follower(leader_p.bind_addr(), &file_name);
    new_repl_p.wait_for_message(&leader_p.heartbeat_msg(0), 1).unwrap();

    //THEN
    //TODO following is flaky when run with other tests!
    // left: "*1\r\n$21\r\ncluster_known_nodes:1\r\n"
    // right: b"*1\r\n$21\r\ncluster_known_nodes:2\r\n"
    let cluster_info = client_handler.send_and_get(cmd).await;
    assert_eq!(cluster_info, array(vec!["cluster_known_nodes:2"]));
}
