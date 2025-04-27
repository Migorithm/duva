use crate::common::{Client, ServerEnv, spawn_server_process};

#[tokio::test]
async fn test_cluster_topology_change_when_new_node_added() -> anyhow::Result<()> {
    // GIVEN
    let env = ServerEnv::default();
    let mut leader_p = spawn_server_process(&env).await?;

    let cmd = "cluster info";

    let repl_env = ServerEnv::default().with_leader_bind_addr(leader_p.bind_addr().into());
    let mut repl_p = spawn_server_process(&repl_env).await?;
    repl_p.wait_for_message(&leader_p.heartbeat_msg(0), 1).await.unwrap();
    leader_p.wait_for_message(&repl_p.heartbeat_msg(0), 1).await.unwrap();

    let mut client_handler = Client::new(leader_p.port);
    let cluster_info = client_handler.send_and_get(cmd.as_bytes(), 1).await;
    assert_eq!(cluster_info, vec!["cluster_known_nodes:1".to_string()]);

    // // WHEN -- new replica is added
    let repl_env2 = ServerEnv::default().with_leader_bind_addr(leader_p.bind_addr().into());
    let mut new_repl_p = spawn_server_process(&repl_env2).await?;
    new_repl_p.wait_for_message(&leader_p.heartbeat_msg(0), 1).await.unwrap();

    //THEN
    let cluster_info = client_handler.send_and_get(cmd.as_bytes(), 1).await;
    assert_eq!(cluster_info, vec!["cluster_known_nodes:2".to_string()]);

    let nodes = client_handler.send_and_get("cluster nodes".as_bytes(), 3).await;
    assert_eq!(nodes.len(), 3);

    let mut leader_nodes = Vec::new();
    tokio::fs::read_to_string(&env.topology_path.0)
        .await
        .unwrap()
        .lines()
        .for_each(|line| leader_nodes.push(line.to_string()));

    for node in leader_nodes {
        assert!(nodes.contains(&node));
    }

    Ok(())
}
