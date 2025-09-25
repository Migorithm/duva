use crate::{
    common::{Client, ServerEnv, form_cluster, spawn_server_process},
    replication_ops::panic_if_election_not_done,
};

/// following test is to see if election works even after the first election.
fn run_leader_election_twice(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let mut leader_env = ServerEnv::default().with_append_only(with_append_only).with_hf(10);
    let mut follower_env1 = ServerEnv::default().with_append_only(with_append_only).with_hf(10);
    let mut follower_env2 = ServerEnv::default().with_append_only(with_append_only).with_hf(10);

    let [leader_p, follower_p1, follower_p2] =
        form_cluster([&mut leader_env, &mut follower_env1, &mut follower_env2]);

    // WHEN: The first leader is killed, triggering an election between F1 and F2
    drop(leader_p);
    panic_if_election_not_done("first", follower_p1.port, follower_p2.port, 3);

    let mut processes = vec![];
    for f in [follower_p1, follower_p2] {
        let mut handler = Client::new(f.port);
        let res = handler.send_and_get_vec("role", 3);
        if !res.contains(&format!("127.0.0.1:{}:{}", f.port, "leader")) {
            processes.push(f);
            continue;
        }

        let follower_env3 = ServerEnv::default()
            .with_bind_addr(f.bind_addr())
            .with_append_only(with_append_only)
            .with_hf(10);
        let new_process = spawn_server_process(&follower_env3)?;

        // WHEN
        // ! second leader is killed -> election happens
        drop(f);

        processes.push(new_process);
    }
    assert_eq!(processes.len(), 2);

    panic_if_election_not_done("second", processes[0].port, processes[1].port, 3);

    Ok(())
}

#[test]
fn test_leader_election_twice() -> anyhow::Result<()> {
    run_leader_election_twice(true)?;

    Ok(())
}
