mod common;
use common::{ServerEnv, array, spawn_server_process};
use duva::clients::ClientStreamHandler;

#[tokio::test]
async fn test_cluster_topology_change_when_new_node_added() {
    // GIVEN
    let env = ServerEnv::default().with_topology_path("test_leader.tp");
    let mut leader_p = spawn_server_process(&env);
    let mut client_handler = ClientStreamHandler::new(leader_p.bind_addr()).await;

    let cmd = &array(vec!["cluster", "info"]);

    let repl_env = ServerEnv::default()
        .with_leader_bind_addr(leader_p.bind_addr().into())
        .with_topology_path("test_repl.tp");
    let mut repl_p = spawn_server_process(&repl_env);
    repl_p.wait_for_message(&leader_p.heartbeat_msg(0), 1).unwrap();
    leader_p.wait_for_message(&repl_p.heartbeat_msg(0), 1).unwrap();

    let cluster_info = client_handler.send_and_get(cmd).await;
    assert_eq!(cluster_info, array(vec!["cluster_known_nodes:1"]));

    // WHEN -- new replica is added
    let repl_env2 = ServerEnv::default()
        .with_leader_bind_addr(leader_p.bind_addr().into())
        .with_topology_path("test_repl2.tp");
    let mut new_repl_p = spawn_server_process(&repl_env2);
    new_repl_p.wait_for_message(&leader_p.heartbeat_msg(0), 1).unwrap();

    //THEN
    //TODO following is flaky when run with other tests!
    // left: "*1\r\n$21\r\ncluster_known_nodes:1\r\n"
    // right: b"*1\r\n$21\r\ncluster_known_nodes:2\r\n"
    let cluster_info = client_handler.send_and_get(cmd).await;
    assert_eq!(cluster_info, array(vec!["cluster_known_nodes:2"]));

    let mut nodes = Vec::new();
    client_handler
        .send_and_get(&array(vec!["cluster", "nodes"]))
        .await
        .lines()
        .for_each(|line| nodes.push(line.to_string()));

    let mut leader_nodes = Vec::new();
    tokio::fs::read_to_string("test_leader.tp")
        .await
        .unwrap()
        .lines()
        .for_each(|line| leader_nodes.push(line.to_string()));

    for node in leader_nodes {
        assert!(nodes.contains(&node));
    }
}
