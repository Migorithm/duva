mod common;
use common::{ServerEnv, spawn_server_process};

use crate::common::array;
use duva::client_utils::ClientStreamHandler;

#[tokio::test]
async fn test_lazy_leader_discovery() {
    // Lazy Leader Discovery: When a server is already replicating from a master,
    // issuing `REPLICAOF` hostname port switches replication to a new master,
    // synchronizing with it while discarding the previous dataset.

    // GIVEN
    let env = ServerEnv::default();
    // loads the leader/follower processes
    let mut leader_p = spawn_server_process(&env);
    let mut leader_client_h = ClientStreamHandler::new(leader_p.bind_addr()).await;

    let future_leader_env = ServerEnv::default();
    let mut future_leader_p = spawn_server_process(&future_leader_env);
    let mut future_leader_h = ClientStreamHandler::new(future_leader_p.bind_addr()).await;

    let repl_env = ServerEnv::default().with_leader_bind_addr(leader_p.bind_addr().into());

    let mut repl_p = spawn_server_process(&repl_env);
    let mut repl_client_h = ClientStreamHandler::new(repl_p.bind_addr()).await;

    repl_p.wait_for_message(&leader_p.heartbeat_msg(0), 1).unwrap();
    leader_p.wait_for_message(&repl_p.heartbeat_msg(0), 1).unwrap();

    // set different values on leader and future_leader
    future_leader_h.send(&common::array(vec!["SET", "fo2", "bar"])).await;
    leader_client_h.send(&common::array(vec!["SET", "foo", "bar"])).await;

    // check followers have the same value with the leader
    repl_p
        .timed_wait_for_message(
            vec![
                "[INFO] Received log entry with log index up to 1",
                "[INFO] Received commit offset 1",
            ],
            1,
            2000,
        )
        .unwrap();

    leader_p
        .timed_wait_for_message(
            vec!["[INFO] Received acks for log index num: 1", "[INFO] Sending commit request on 1"],
            1,
            2000,
        )
        .unwrap();

    // WHEN
    // switch replication to future_leader
    repl_client_h
        .send(&common::array(vec!["REPLICAOF", future_leader_p.bind_addr().as_str()]))
        .await;

    repl_p.wait_for_message("[INFO] Received Leader State - length 1", 1).unwrap();
    assert_eq!(repl_client_h.send_and_get(&array(vec!["KEYS", "*"])).await, array(vec!["foo"]));
}
