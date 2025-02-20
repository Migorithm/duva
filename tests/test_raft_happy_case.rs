use duva::client_utils::ClientStreamHandler;

mod common;

#[tokio::test]
async fn test_set_operation_reaches_to_all_replicas() {
    // GIVEN

    // loads the leader/follower processes
    let mut leader_p = common::spawn_server_process();
    let mut client_handler = ClientStreamHandler::new(leader_p.bind_addr()).await;

    let mut repl_p = common::spawn_server_as_follower(leader_p.bind_addr());

    repl_p.wait_for_message(&leader_p.heartbeat_msg(0), 1).unwrap();
    leader_p.wait_for_message(&repl_p.heartbeat_msg(0), 1).unwrap();

    // WHEN -- set operation is made
    client_handler.send(&common::array(vec!["SET", "foo", "bar"])).await;

    //THEN - run the following together
    let h = std::thread::spawn(move || {
        repl_p.timed_wait_for_message(
        "[INFO] Received log entries: [WriteOperation { op: Set { key: \"foo\", value: \"bar\" }",
        1,
        2,
        )
    });

    let h2 = std::thread::spawn(move || {
        leader_p.timed_wait_for_message("[INFO] Received acks for offset:", 1, 2)
    });

    h.join().unwrap().unwrap();
    h2.join().unwrap().unwrap();
    // TODO state change of the leader is not being checked
    // TODO state change of the follower is not being checked
}
