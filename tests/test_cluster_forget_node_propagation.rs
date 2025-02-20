mod common;
use common::{
    array, check_internodes_communication, spawn_server_as_follower, spawn_server_process,
};
use duva::client_utils::ClientStreamHandler;

#[tokio::test]
async fn test_cluster_forget_node_propagation() {
    // GIVEN
    const HOP_COUNT: usize = 0;
    let mut leader_p = spawn_server_process();
    let mut repl_p = spawn_server_as_follower(leader_p.bind_addr());
    let mut repl_p2 = spawn_server_as_follower(leader_p.bind_addr());

    check_internodes_communication(&mut [&mut leader_p, &mut repl_p, &mut repl_p2], HOP_COUNT, 2)
        .unwrap();

    // WHEN
    const TIMEOUT: u64 = 2;
    let never_arrivable_msg = repl_p.heartbeat_msg(1);
    let mut client_handler = ClientStreamHandler::new(leader_p.bind_addr()).await;
    let replica_id = repl_p.bind_addr();
    let cmd = &array(vec!["cluster", "forget", &replica_id]);
    let response1 = client_handler.send_and_get(cmd).await;
    let response2 = client_handler.send_and_get(&array(vec!["cluster", "info"])).await;

    // THEN
    assert_eq!(response1, "+OK\r\n");
    assert_eq!(response2, array(vec!["cluster_known_nodes:1"]));

    // leader_p and repl_p2 doesn't get message from repl_p2
    let h1 = std::thread::spawn({
        let never_arrivable_msg = never_arrivable_msg.clone();
        move || leader_p.timed_wait_for_message(vec![&never_arrivable_msg], HOP_COUNT, TIMEOUT)
    });
    let h2 = std::thread::spawn(move || {
        repl_p2.timed_wait_for_message(vec![&never_arrivable_msg], HOP_COUNT, TIMEOUT)
    });

    assert!(h1.join().unwrap().is_err());
    assert!(h2.join().unwrap().is_err());
}
