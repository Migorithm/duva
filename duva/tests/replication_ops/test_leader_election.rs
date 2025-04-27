use tokio::time::{Duration, sleep};

use crate::common::{Client, ServerEnv, check_internodes_communication, spawn_server_process};
use duva::domains::cluster_actors::heartbeats::scheduler::LEADER_HEARTBEAT_INTERVAL_MAX;

#[tokio::test]
async fn test_leader_election() {
    // GIVEN
    let leaver_env = ServerEnv::default();
    let mut leader_p = spawn_server_process(&leaver_env).await?;

    let follower_env1 = ServerEnv::default().with_leader_bind_addr(leader_p.bind_addr().into());
    let mut follower_p1 = spawn_server_process(&follower_env1).await?;

    let follower_env2 = ServerEnv::default().with_leader_bind_addr(leader_p.bind_addr().into());

    let mut follower_p2 = spawn_server_process(&follower_env2).await?;
    const DEFAULT_HOP_COUNT: usize = 0;
    const TIMEOUT_IN_MILLIS: u128 = 2000;
    let processes = &mut [&mut leader_p, &mut follower_p1, &mut follower_p2];
    check_internodes_communication(processes, DEFAULT_HOP_COUNT, TIMEOUT_IN_MILLIS).await.unwrap();

    // WHEN
    leader_p.kill().await.unwrap();
    sleep(Duration::from_millis(LEADER_HEARTBEAT_INTERVAL_MAX)).await;

    // THEN
    let mut flag = false;
    for f in [&follower_p1, &follower_p2] {
        let mut handler = Client::new(f.port);
        let response1 = handler.send_and_get("info replication", 4).await;
        if response1.contains(&"role:leader".to_string()) {
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
    let leaver_env = ServerEnv::default();
    let mut leader_p = spawn_server_process(&leaver_env).await?;

    let follower_env1 = ServerEnv::default().with_leader_bind_addr(leader_p.bind_addr().into());
    let mut follower_p1 = spawn_server_process(&follower_env1).await?;

    let follower_env2 = ServerEnv::default().with_leader_bind_addr(leader_p.bind_addr().into());
    let mut follower_p2 = spawn_server_process(&follower_env2).await?;
    const DEFAULT_HOP_COUNT: usize = 0;
    const TIMEOUT_IN_MILLIS: u128 = 2000;
    let processes = &mut [&mut leader_p, &mut follower_p1, &mut follower_p2];
    check_internodes_communication(processes, DEFAULT_HOP_COUNT, TIMEOUT_IN_MILLIS).await?;

    // WHEN
    leader_p.kill().await?;
    sleep(Duration::from_millis(LEADER_HEARTBEAT_INTERVAL_MAX)).await;

    let mut flag = false;
    for f in [&follower_p1, &follower_p2] {
        let mut handler = Client::new(f.port);
        let res = handler.send_and_get("info replication", 4).await;
        if res.contains(&"role:leader".to_string()) {
            // THEN - one of the replicas should become the leader
            assert_eq!(handler.send_and_get("set 1 2", 1).await.first().unwrap(), "OK");
            assert_eq!(handler.send_and_get("set 2 3", 1).await.first().unwrap(), "OK");

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
    let leaver_env = ServerEnv::default();
    let mut leader_p = spawn_server_process(&leaver_env).await?;

    let follower_env1 = ServerEnv::default().with_leader_bind_addr(leader_p.bind_addr().into());
    let mut follower_p1 = spawn_server_process(&follower_env1).await?;

    let follower_env2 = ServerEnv::default().with_leader_bind_addr(leader_p.bind_addr().into());
    let mut follower_p2 = spawn_server_process(&follower_env2).await?;
    const DEFAULT_HOP_COUNT: usize = 0;
    const TIMEOUT_IN_MILLIS: u128 = 2000;
    let processes = &mut [&mut leader_p, &mut follower_p1, &mut follower_p2];
    check_internodes_communication(processes, DEFAULT_HOP_COUNT, TIMEOUT_IN_MILLIS).await?;

    // !first leader is killed -> election happens
    leader_p.kill().await.unwrap();
    sleep(Duration::from_millis(LEADER_HEARTBEAT_INTERVAL_MAX)).await;

    let mut processes = vec![];

    for mut f in [follower_p1, follower_p2] {
        let mut handler = Client::new(f.port);
        let res = handler.send_and_get("info replication", 4).await;
        if !res.contains(&"role:leader".to_string()) {
            processes.push(f);
            continue;
        }

        let follower_env3 = ServerEnv::default().with_leader_bind_addr(f.bind_addr().into());
        let new_process = spawn_server_process(&follower_env3).await?;
        sleep(Duration::from_millis(LEADER_HEARTBEAT_INTERVAL_MAX)).await;

        // WHEN
        // ! second leader is killed -> election happens
        f.kill().await?;
        sleep(Duration::from_millis(LEADER_HEARTBEAT_INTERVAL_MAX)).await;
        processes.push(new_process);
    }
    assert_eq!(processes.len(), 2);

    let mut flag = false;
    for f in processes.iter() {
        let mut handler = Client::new(f.port);
        let res = handler.send_and_get("info replication", 4).await;
        if res.contains(&"role:leader".to_string()) {
            flag = true;
            break;
        }
    }
    assert!(flag, "No leader found after the second leader was killed");
}
