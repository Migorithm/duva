use std::{thread::sleep, time::Duration};

use crate::common::{Client, ServerEnv, check_internodes_communication, spawn_server_process};
use duva::domains::cluster_actors::heartbeats::scheduler::LEADER_HEARTBEAT_INTERVAL_MAX;

fn run_leader_election(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let leader_env = ServerEnv::default().with_append_only(with_append_only);
    let mut leader_p = spawn_server_process(&leader_env, true)?;

    let follower_env1 = ServerEnv::default()
        .with_bind_addr(leader_p.bind_addr())
        .with_append_only(with_append_only);
    let mut follower_p1 = spawn_server_process(&follower_env1, true)?;

    let follower_env2 = ServerEnv::default()
        .with_bind_addr(leader_p.bind_addr())
        .with_append_only(with_append_only);

    let mut follower_p2 = spawn_server_process(&follower_env2, true)?;
    const DEFAULT_HOP_COUNT: usize = 0;
    const TIMEOUT_IN_MILLIS: u128 = 2000;
    let processes = &mut [&mut leader_p, &mut follower_p1, &mut follower_p2];
    check_internodes_communication(processes, DEFAULT_HOP_COUNT, TIMEOUT_IN_MILLIS)?;

    // WHEN
    leader_p.kill()?;
    sleep(Duration::from_millis(LEADER_HEARTBEAT_INTERVAL_MAX));

    // THEN
    let mut flag = false;
    for f in [&follower_p1, &follower_p2] {
        let mut handler = Client::new(f.port);
        let response1 = handler.send_and_get("info replication", 4);
        if response1.contains(&"role:leader".to_string()) {
            flag = true;
            break;
        }
    }
    assert!(flag, "No leader found after the first leader was killed");

    Ok(())
}

// ! EDGE case : when last_log_term is not updated, after the election, first write operation succeeds but second one doesn't
// ! This is because the leader doesn't have the last_log_term of the first write operation
// ! This test is to see if the leader can set the value twice after the election
fn run_set_twice_after_election(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let leader_env = ServerEnv::default().with_append_only(with_append_only);
    let mut leader_p = spawn_server_process(&leader_env, true)?;

    let follower_env1 = ServerEnv::default()
        .with_bind_addr(leader_p.bind_addr())
        .with_append_only(with_append_only);
    let mut follower_p1 = spawn_server_process(&follower_env1, true)?;

    let follower_env2 = ServerEnv::default()
        .with_bind_addr(leader_p.bind_addr())
        .with_append_only(with_append_only);
    let mut follower_p2 = spawn_server_process(&follower_env2, true)?;
    const DEFAULT_HOP_COUNT: usize = 0;
    const TIMEOUT_IN_MILLIS: u128 = 2000;
    let processes = &mut [&mut leader_p, &mut follower_p1, &mut follower_p2];
    check_internodes_communication(processes, DEFAULT_HOP_COUNT, TIMEOUT_IN_MILLIS)?;

    // WHEN
    leader_p.kill()?;
    sleep(Duration::from_millis(LEADER_HEARTBEAT_INTERVAL_MAX));

    let mut flag = false;
    for f in [&follower_p1, &follower_p2] {
        let mut handler = Client::new(f.port);
        let res = handler.send_and_get("info replication", 4);
        if res.contains(&"role:leader".to_string()) {
            // THEN - one of the replicas should become the leader
            assert_eq!(handler.send_and_get("set 1 2", 1).first().unwrap(), "OK");
            assert_eq!(handler.send_and_get("set 2 3", 1).first().unwrap(), "OK");

            flag = true;
            break;
        }
    }
    assert!(flag, "No leader found after the first leader was killed");

    Ok(())
}

/// following test is to see if election works even after the first election.
fn run_leader_election_twice(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let leader_env = ServerEnv::default().with_append_only(with_append_only);
    let mut leader_p = spawn_server_process(&leader_env, true)?;

    let follower_env1 = ServerEnv::default()
        .with_bind_addr(leader_p.bind_addr())
        .with_append_only(with_append_only);
    let mut follower_p1 = spawn_server_process(&follower_env1, true)?;

    let follower_env2 = ServerEnv::default()
        .with_bind_addr(leader_p.bind_addr())
        .with_append_only(with_append_only);
    let mut follower_p2 = spawn_server_process(&follower_env2, true)?;
    const DEFAULT_HOP_COUNT: usize = 0;
    const TIMEOUT_IN_MILLIS: u128 = 2000;
    let processes = &mut [&mut leader_p, &mut follower_p1, &mut follower_p2];
    check_internodes_communication(processes, DEFAULT_HOP_COUNT, TIMEOUT_IN_MILLIS)?;

    // !first leader is killed -> election happens
    leader_p.kill()?;
    sleep(Duration::from_millis(LEADER_HEARTBEAT_INTERVAL_MAX));

    let mut processes = vec![];

    for mut f in [follower_p1, follower_p2] {
        let mut handler = Client::new(f.port);
        let res = handler.send_and_get("info replication", 4);
        if !res.contains(&"role:leader".to_string()) {
            processes.push(f);
            continue;
        }

        let follower_env3 =
            ServerEnv::default().with_bind_addr(f.bind_addr()).with_append_only(with_append_only);
        let new_process = spawn_server_process(&follower_env3, true)?;
        sleep(Duration::from_millis(LEADER_HEARTBEAT_INTERVAL_MAX));

        // WHEN
        // ! second leader is killed -> election happens
        f.kill()?;
        sleep(Duration::from_millis(LEADER_HEARTBEAT_INTERVAL_MAX));
        processes.push(new_process);
    }
    assert_eq!(processes.len(), 2);

    let mut flag = false;
    for f in processes.iter() {
        let mut handler = Client::new(f.port);
        let res = handler.send_and_get("info replication", 4);
        if res.contains(&"role:leader".to_string()) {
            flag = true;
            break;
        }
    }
    assert!(flag, "No leader found after the second leader was killed");

    Ok(())
}

#[test]
fn test_leader_election() -> anyhow::Result<()> {
    run_leader_election(false)?;
    run_leader_election(true)?;

    Ok(())
}

#[test]
fn test_set_twice_after_election() -> anyhow::Result<()> {
    run_set_twice_after_election(false)?;
    run_set_twice_after_election(true)?;

    Ok(())
}

#[test]
fn test_leader_election_twice() -> anyhow::Result<()> {
    run_leader_election_twice(false)?;
    run_leader_election_twice(true)?;

    Ok(())
}
