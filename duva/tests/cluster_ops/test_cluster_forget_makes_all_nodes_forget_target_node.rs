use duva::domains::cluster_actors::heartbeats::scheduler::LEADER_HEARTBEAT_INTERVAL_MAX;

use crate::common::{Client, ServerEnv, check_internodes_communication, spawn_server_process};

#[tokio::test]
async fn test_cluster_forget_makes_all_nodes_forget_target_node() {
    // GIVEN
    const HOP_COUNT: usize = 0;

    let env = ServerEnv::default().with_ttl(500).with_hf(2);
    let mut leader_p = spawn_server_process(&env).await;

    let repl_env =
        ServerEnv::default().with_leader_bind_addr(leader_p.bind_addr().clone()).with_hf(10);
    let mut repl_p = spawn_server_process(&repl_env).await;

    let repl_env2 =
        ServerEnv::default().with_leader_bind_addr(leader_p.bind_addr().clone()).with_hf(10);
    let mut repl_p2 = spawn_server_process(&repl_env2).await;

    check_internodes_communication(
        &mut [&mut leader_p, &mut repl_p, &mut repl_p2],
        HOP_COUNT,
        1000,
    )
    .await
    .unwrap();

    // WHEN
    let mut client_handler = Client::new(leader_p.port);
    let replica_id = repl_p.bind_addr();
    let response1 = client_handler.send_and_get(format!("cluster forget {}", &replica_id), 1).await;
    assert_eq!(response1, vec!["OK"]);

    // THEN
    let response2 = client_handler.send_and_get("cluster info", 1).await;
    assert_eq!(response2.first().unwrap(), "cluster_known_nodes:1");

    // leader_p and repl_p2 doesn't get message from repl_p2
    let f1 =
        leader_p.timed_wait_for_message(vec![&never_arrivable_msg], HOP_COUNT, TIMEOUT_IN_MILLIS);
    let f2 =
        repl_p2.timed_wait_for_message(vec![&never_arrivable_msg], HOP_COUNT, TIMEOUT_IN_MILLIS);

    assert!(f1.await.is_err());
    assert!(f2.await.is_err());
}
