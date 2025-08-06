use crate::common::{Client, ServerEnv, form_cluster, spawn_server_process};
use duva::prelude::LEADER_HEARTBEAT_INTERVAL_MAX;
use std::{thread::sleep, time::Duration};

fn run_leader_election(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let mut leader_env = ServerEnv::default().with_append_only(with_append_only);
    let mut follower_env1 = ServerEnv::default().with_append_only(with_append_only);
    let mut follower_env2 = ServerEnv::default().with_append_only(with_append_only);

    let [mut leader_p, follower_p1, follower_p2] =
        form_cluster([&mut leader_env, &mut follower_env1, &mut follower_env2]);

    // WHEN
    leader_p.kill()?;

    // THEN
    panic_if_election_not_done(follower_p1.port, follower_p2.port);

    Ok(())
}

// ! EDGE case : when last_log_term is not updated, after the election, first write operation succeeds but second one doesn't
// ! This is because the leader doesn't have the last_log_term of the first write operation
// ! This test is to see if the leader can set the value twice after the election
fn run_set_twice_after_election(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let mut leader_env = ServerEnv::default().with_append_only(with_append_only);
    let mut follower_env1 = ServerEnv::default().with_append_only(with_append_only);
    let mut follower_env2 = ServerEnv::default().with_append_only(with_append_only);

    let [mut leader_p, follower_p1, follower_p2] =
        form_cluster([&mut leader_env, &mut follower_env1, &mut follower_env2]);

    // WHEN
    leader_p.kill()?;
    sleep(Duration::from_millis(LEADER_HEARTBEAT_INTERVAL_MAX + 300));

    // THEN

    let mut h1 = Client::new(follower_p1.port);
    let mut h2 = Client::new(follower_p2.port);

    panic_if_election_not_done(follower_p1.port, follower_p2.port);

    let res = h1.send_and_get_vec("role", 3);

    if res.contains(&format!("127.0.0.1:{}:{}", follower_p1.port, "leader")) {
        // THEN - one of the replicas should become the leader
        assert_eq!(h1.send_and_get("set 1 2"), "OK");
        assert_eq!(h1.send_and_get("set 2 3"), "OK");
    }

    if res.contains(&format!("127.0.0.1:{}:{}", follower_p2.port, "leader")) {
        // THEN - one of the replicas should become the leader
        assert_eq!(h2.send_and_get("set 1 2"), "OK");
        assert_eq!(h2.send_and_get("set 2 3"), "OK");
    }
    Ok(())
}

/// following test is to see if election works even after the first election.
fn run_leader_election_twice(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let server_env = ServerEnv::default().with_append_only(with_append_only);
    let server_env = server_env;
    let mut leader_env = server_env;
    let mut follower_env1 = ServerEnv::default().with_append_only(with_append_only);
    let mut follower_env2 = ServerEnv::default().with_append_only(with_append_only);

    let [mut leader_p, follower_p1, follower_p2] =
        form_cluster([&mut leader_env, &mut follower_env1, &mut follower_env2]);

    // !first leader is killed -> election happens
    leader_p.kill()?;
    let mut tmp_h = Client::new(follower_p1.port);
    tmp_h.send_and_get(format!("cluster forget 127.0.0.1:{}", leader_p.port));

    panic_if_election_not_done(follower_p1.port, follower_p2.port);

    let mut processes = vec![];
    for mut f in [follower_p1, follower_p2] {
        let mut handler = Client::new(f.port);
        let res = handler.send_and_get_vec("info replication", 4);
        if !res.contains(&"role:leader".to_string()) {
            processes.push(f);
            continue;
        }

        let follower_env3 =
            ServerEnv::default().with_bind_addr(f.bind_addr()).with_append_only(with_append_only);
        let new_process = spawn_server_process(&follower_env3)?;

        let mut tmp_h = Client::new(follower_env3.port);
        tmp_h.send_and_get(format!("cluster forget 127.0.0.1:{}", f.port));

        // WHEN
        // ! second leader is killed -> election happens
        f.kill()?;

        processes.push(new_process);
    }
    assert_eq!(processes.len(), 2);

    panic_if_election_not_done(processes[0].port, processes[1].port);

    Ok(())
}

fn panic_if_election_not_done(port1: u16, port2: u16) {
    let mut first_election_cnt = 0;
    let mut flag = false;
    let mut h1 = Client::new(port1);

    let start = std::time::Instant::now();
    while first_election_cnt < 50 {
        let res = h1.send_and_get_vec("role", 2);
        println!(
            "[{}ms] Poll {}: port1={} port2={} res={:?}",
            start.elapsed().as_millis(),
            first_election_cnt,
            port1,
            port2,
            res,
        );
        if res.contains(&format!("127.0.0.1:{}:{}", port1, "leader"))
            || res.contains(&format!("127.0.0.1:{}:{}", port2, "leader"))
        {
            flag = true;
            break;
        }
        first_election_cnt += 1;
        sleep(Duration::from_millis(500));
    }
    if !flag {
        println!(
            "[{}ms] Leader election failed after {} attempts (ports: {}, {})",
            start.elapsed().as_millis(),
            first_election_cnt,
            port1,
            port2
        );
    }
    assert!(flag, "first election fail");
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
