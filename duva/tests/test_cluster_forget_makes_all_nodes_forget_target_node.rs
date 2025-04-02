mod common;
use common::{ServerEnv, array, check_internodes_communication, spawn_server_process};
use duva::{clients::ClientStreamHandler, domains::query_parsers::query_io::QueryIO};

#[tokio::test]
async fn test_cluster_forget_makes_all_nodes_forget_target_node() {
    // GIVEN
    const HOP_COUNT: usize = 0;

    let env = ServerEnv::default().with_ttl(500).with_hf(2);
    let mut leader_p = spawn_server_process(&env);

    let repl_env =
        ServerEnv::default().with_leader_bind_addr(leader_p.bind_addr().clone()).with_hf(10);
    let mut repl_p = spawn_server_process(&repl_env);

    let repl_env2 =
        ServerEnv::default().with_leader_bind_addr(leader_p.bind_addr().clone()).with_hf(10);
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
    let mut client_handler = ClientStreamHandler::new(leader_p.bind_addr()).await;
    let replica_id = repl_p.bind_addr();
    let cmd = &array(vec!["cluster", "forget", &replica_id]);
    let response1 = client_handler.send_and_get(cmd).await;
    let response2 = client_handler.send_and_get(&array(vec!["cluster", "info"])).await;

    // THEN
    assert_eq!(response1, "+OK\r\n");
    assert_eq!(response2, QueryIO::BulkString("cluster_known_nodes:1".into()).serialize());

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
