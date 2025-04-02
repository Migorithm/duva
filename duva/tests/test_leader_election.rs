mod common;
use std::{thread::sleep, time::Duration};

use common::{ServerEnv, array, check_internodes_communication, spawn_server_process};
use duva::clients::ClientStreamHandler;

#[tokio::test]
async fn test_leader_election() {
    // GIVEN
    let mut leader_p = spawn_server_process(&ServerEnv::default());

    let mut follower_p1 = spawn_server_process(
        &ServerEnv::default().with_leader_bind_addr(leader_p.bind_addr().into()),
    );

    let mut follower_p2 = spawn_server_process(
        &ServerEnv::default().with_leader_bind_addr(leader_p.bind_addr().into()),
    );
    const DEFAULT_HOP_COUNT: usize = 0;
    const TIMEOUT_IN_MILLIS: u128 = 2000;
    let processes = &mut [&mut leader_p, &mut follower_p1, &mut follower_p2];
    check_internodes_communication(processes, DEFAULT_HOP_COUNT, TIMEOUT_IN_MILLIS).unwrap();

    // WHEN
    leader_p.kill().unwrap();
    sleep(Duration::from_secs(2));

    // THEN
    let mut handler = ClientStreamHandler::new(follower_p1.bind_addr()).await;
    let mut handler2 = ClientStreamHandler::new(follower_p2.bind_addr()).await;

    let response1 = handler.send_and_get(&array(vec!["info", "replication"])).await;
    let response2 = handler2.send_and_get(&array(vec!["info", "replication"])).await;

    // THEN - one of the replicas should become the leader
    assert!([response1, response2].iter().any(|d| d.contains("role:leader")));
}

/// following test is to see if election works even after the first election.
#[tokio::test]
async fn test_leader_election_twice() {
    // GIVEN
    let mut leader_p = spawn_server_process(&ServerEnv::default());

    let mut follower_p1 = spawn_server_process(
        &ServerEnv::default().with_leader_bind_addr(leader_p.bind_addr().into()),
    );

    let mut follower_p2 = spawn_server_process(
        &ServerEnv::default().with_leader_bind_addr(leader_p.bind_addr().into()),
    );
    const DEFAULT_HOP_COUNT: usize = 0;
    const TIMEOUT_IN_MILLIS: u128 = 2000;
    let processes = &mut [&mut leader_p, &mut follower_p1, &mut follower_p2];
    check_internodes_communication(processes, DEFAULT_HOP_COUNT, TIMEOUT_IN_MILLIS).unwrap();

    // WHEN
    leader_p.kill().unwrap();
    sleep(Duration::from_secs(1));
    let mut handler = ClientStreamHandler::new(follower_p1.bind_addr()).await;
    let mut handler2 = ClientStreamHandler::new(follower_p2.bind_addr()).await;
    let response1 = handler.send_and_get(&array(vec!["info", "replication"])).await;

    if response1.contains("role:leader") {
        let follower_p3 = spawn_server_process(
            &ServerEnv::default().with_leader_bind_addr(follower_p1.bind_addr().into()),
        );
        sleep(Duration::from_secs(1));
        follower_p1.kill().unwrap();
        sleep(Duration::from_secs(1));
        let mut handler3 = ClientStreamHandler::new(follower_p3.bind_addr()).await;

        let response2 = handler2.send_and_get(&array(vec!["info", "replication"])).await;
        let response3 = handler3.send_and_get(&array(vec!["info", "replication"])).await;
        // THEN - one of the replicas should become the leader

        assert!([response2, response3].iter().any(|d| d.contains("role:leader")));
    } else {
        let follower_p3 = spawn_server_process(
            &ServerEnv::default().with_leader_bind_addr(follower_p2.bind_addr().into()),
        );
        sleep(Duration::from_secs(1));
        follower_p2.kill().unwrap();
        sleep(Duration::from_secs(1));

        let mut handler3 = ClientStreamHandler::new(follower_p3.bind_addr()).await;

        let response1 = handler.send_and_get(&array(vec!["info", "replication"])).await;
        let response3 = handler3.send_and_get(&array(vec!["info", "replication"])).await;

        // THEN - one of the replicas should become the leader
        assert!([response1, response3].iter().any(|d| d.contains("role:leader")));
    };
}
