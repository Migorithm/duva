use crate::common::{Client, ServerEnv, spawn_server_process};

#[tokio::test]
async fn test_cluster_topology_change_when_new_node_added() {
    // GIVEN
    let env = ServerEnv::default().with_topology_path("test_leader.tp");
    let mut leader_p = spawn_server_process(&env);

    let cmd = "cluster info";

    let repl_env = ServerEnv::default()
        .with_leader_bind_addr(leader_p.bind_addr().into())
        .with_topology_path("test_repl.tp");
    let mut repl_p = spawn_server_process(&repl_env);
    repl_p.wait_for_message(&leader_p.heartbeat_msg(0), 1).unwrap();
    leader_p.wait_for_message(&repl_p.heartbeat_msg(0), 1).unwrap();

    let mut client_handler = Client::new(leader_p.port);
    let cluster_info = client_handler.send_and_get(cmd.as_bytes(), 1);
    assert_eq!(cluster_info, vec!["cluster_known_nodes:1".to_string()]);

    // // WHEN -- new replica is added
    let repl_env2 = ServerEnv::default()
        .with_leader_bind_addr(leader_p.bind_addr().into())
        .with_topology_path("test_repl2.tp");
    let mut new_repl_p = spawn_server_process(&repl_env2);
    new_repl_p.wait_for_message(&leader_p.heartbeat_msg(0), 1).unwrap();

    //THEN
    let cluster_info = client_handler.send_and_get(cmd.as_bytes(), 1);
    assert_eq!(cluster_info, vec!["cluster_known_nodes:2".to_string()]);

    let nodes = client_handler.send_and_get("cluster nodes".as_bytes(), 3);
    assert_eq!(nodes.len(), 3);

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
