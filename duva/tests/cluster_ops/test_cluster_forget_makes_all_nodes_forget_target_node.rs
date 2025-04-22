use crate::common::{Client, ServerEnv, check_internodes_communication, spawn_server_process};

#[tokio::test]
async fn test_cluster_forget_makes_all_nodes_forget_target_node() {
    // GIVEN
    const HOP_COUNT: usize = 0;

    let env = ServerEnv::default()
        .with_ttl(500)
        .with_hf(2)
        .with_topology_path("test_cluster_forget_makes_all_nodes_forget_target_node-leader.tp");
    let mut leader_p = spawn_server_process(&env);

    let repl_env = ServerEnv::default()
        .with_leader_bind_addr(leader_p.bind_addr().clone())
        .with_hf(10)
        .with_topology_path("test_cluster_forget_makes_all_nodes_forget_target_node-follower.tp");
    let mut repl_p = spawn_server_process(&repl_env);

    let repl_env2 = ServerEnv::default()
        .with_leader_bind_addr(leader_p.bind_addr().clone())
        .with_hf(10)
        .with_topology_path("test_cluster_forget_makes_all_nodes_forget_target_node-follower2.tp");
    let mut repl_p2 = spawn_server_process(&repl_env2);

    check_internodes_communication(
        &mut [&mut leader_p, &mut repl_p, &mut repl_p2],
        HOP_COUNT,
        1000,
    )
    .unwrap();

    // WHEN
    const TIMEOUT_IN_MILLIS: u128 = 500;
    let never_arrivable_msg = repl_p.heartbeat_msg(1);
    let mut client_handler = Client::new(leader_p.port);
    let replica_id = repl_p.bind_addr();

    let response1 = client_handler.send_and_get(format!("cluster forget {}", &replica_id), 1);
    let response2 = client_handler.send_and_get("cluster info", 1);

    // THEN
    assert_eq!(response1.first().unwrap(), "OK");
    assert_eq!(response2.first().unwrap(), "cluster_known_nodes:1");

    // leader_p and repl_p2 doesn't get message from repl_p2
    let h1 = std::thread::spawn({
        let never_arrivable_msg = never_arrivable_msg.clone();
        move || {
            leader_p.timed_wait_for_message(
                vec![&never_arrivable_msg],
                HOP_COUNT,
                TIMEOUT_IN_MILLIS,
            )
        }
    });
    let h2 = std::thread::spawn(move || {
        repl_p2.timed_wait_for_message(vec![&never_arrivable_msg], HOP_COUNT, TIMEOUT_IN_MILLIS)
    });

    assert!(h1.join().unwrap().is_err());
    assert!(h2.join().unwrap().is_err());
}
