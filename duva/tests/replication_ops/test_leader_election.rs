use std::{thread::sleep, time::Duration};

use crate::common::{ServerEnv, array, check_internodes_communication, spawn_server_process};
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
    let mut flag = false;
    for f in [&follower_p1, &follower_p2] {
        let mut handler = ClientStreamHandler::new(f.bind_addr()).await;
        let response1 = handler.send_and_get(&array(vec!["info", "replication"])).await;
        if response1.contains("role:leader") {
            flag = true;
            break;
        }
    }
    assert!(flag, "No leader found after the first leader was killed");
}

// ! EDGE case : when last_log_term is not updated, after the election, first write operation succeeds but second one doesn't
// ! This is because the leader doesn't have the last_log_term of the first write operation
// ! This test is to see if the leader can set the value twice after the election
#[tokio::test]
async fn test_set_twice_after_election() {
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

    let mut flag = false;
    for f in [&follower_p1, &follower_p2] {
        let mut handler = ClientStreamHandler::new(f.bind_addr()).await;
        let res = handler.send_and_get(&array(vec!["info", "replication"])).await;
        if res.contains("role:leader") {
            // THEN - one of the replicas should become the leader
            let _ = handler.send_and_get(&array(vec!["set", "1", "2"])).await;
            let _ = handler.send_and_get(&array(vec!["set", "2", "3"])).await;
            flag = true;
            break;
        }
    }
    assert!(flag, "No leader found after the first leader was killed");
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

    // !first leader is killed -> election happens
    leader_p.kill().unwrap();
    sleep(Duration::from_secs(1));

    let mut processes = vec![];

    for mut f in [follower_p1, follower_p2] {
        let mut handler = ClientStreamHandler::new(f.bind_addr()).await;
        let res = handler.send_and_get(&array(vec!["info", "replication"])).await;
        if !res.contains("role:leader") {
            processes.push(f);
            continue;
        }
        let new_process =
            spawn_server_process(&ServerEnv::default().with_leader_bind_addr(f.bind_addr().into()));
        sleep(Duration::from_secs(1));

        // WHEN
        // ! second leader is killed -> election happens
        f.kill().unwrap();
        processes.push(new_process);
    }
    assert_eq!(processes.len(), 2);

    let mut flag = false;
    for f in processes.iter() {
        let mut handler = ClientStreamHandler::new(f.bind_addr()).await;
        let res = handler.send_and_get(&array(vec!["info", "replication"])).await;
        if res.contains("role:leader") {
            flag = true;
            break;
        }
    }
    assert!(flag, "No leader found after the second leader was killed");
}
