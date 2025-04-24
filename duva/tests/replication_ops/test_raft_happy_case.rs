use crate::common::{Client, ServerEnv, spawn_server_process};

#[tokio::test]
async fn test_set_operation_reaches_to_all_replicas() {
    // GIVEN

    let env = ServerEnv::default();

    // loads the leader/follower processes
    let mut leader_p = spawn_server_process(&env).await;
    let mut client_handler = Client::new(leader_p.port);

    let repl_env = ServerEnv::default()
        .with_leader_bind_addr(leader_p.bind_addr().into())
        .with_file_name("follower_dbfilename");

    let mut repl_p = spawn_server_process(&repl_env).await;

    repl_p.wait_for_message(&leader_p.heartbeat_msg(0), 1).await.unwrap();
    leader_p.wait_for_message(&repl_p.heartbeat_msg(0), 1).await.unwrap();

    // WHEN -- set operation is made
    client_handler.send_and_get("SET foo bar", 1).await;

    //THEN - run the following together
    let f1 = repl_p.timed_wait_for_message(
        vec!["[INFO] Received log entry with log index up to 1", "[INFO] Received commit offset 1"],
        1,
        2000,
    );

    let f2 = leader_p.timed_wait_for_message(
        vec!["[INFO] Received acks for log index num: 1", "[INFO] log 1 commited"],
        1,
        2000,
    );

    f1.await.unwrap();
    f2.await.unwrap();
}
