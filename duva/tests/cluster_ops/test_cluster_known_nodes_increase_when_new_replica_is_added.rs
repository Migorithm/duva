use crate::common::{Client, ServerEnv, form_cluster, spawn_server_process};

fn run_cluster_topology_change_when_new_node_added(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let mut env = ServerEnv::default().with_append_only(with_append_only);
    let mut repl_env = ServerEnv::default().with_append_only(with_append_only);

    // Form cluster with leader and replica
    let [leader_p, _repl_p] = form_cluster([&mut env, &mut repl_env]);

    let mut client_handler = Client::new(leader_p.port);
    assert_eq!(
        client_handler.send_and_get_vec("cluster info", 1),
        vec!["cluster_known_nodes:1".to_string()]
    );

    // WHEN -- new replica is added
    let repl_env2 = ServerEnv::default()
        .with_bind_addr(leader_p.bind_addr())
        .with_append_only(with_append_only);
    let mut _new_repl_p = spawn_server_process(&repl_env2)?;

    //THEN
    assert_eq!(
        client_handler.send_and_get_vec("cluster info", 1),
        vec!["cluster_known_nodes:2".to_string()]
    );
    let nodes = client_handler.send_and_get_vec("cluster nodes", 3);
    assert_eq!(nodes.len(), 3);
    std::fs::read_to_string(&env.topology_path)
        .unwrap()
        .lines()
        .for_each(|line| assert!(nodes.contains(&line.to_string())));

    Ok(())
}

#[test]
fn test_cluster_topology_change_when_new_node_added() -> anyhow::Result<()> {
    run_cluster_topology_change_when_new_node_added(false)?;
    run_cluster_topology_change_when_new_node_added(true)?;

    Ok(())
}
