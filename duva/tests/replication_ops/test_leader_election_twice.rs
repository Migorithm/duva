use crate::{
    common::{Client, ServerEnv, form_cluster, spawn_server_process},
    replication_ops::panic_if_election_not_done,
};

/// following test is to see if election works even after the first election.
fn run_leader_election_twice(with_append_only: bool) -> anyhow::Result<()> {
    // GIVEN
    let server_env = ServerEnv::default().with_append_only(with_append_only);
    let server_env = server_env;
    let mut leader_env = server_env;
    let mut follower_env1 = ServerEnv::default().with_append_only(with_append_only);
    let mut follower_env2 = ServerEnv::default().with_append_only(with_append_only);

    let [leader_p, follower_p1, follower_p2] =
        form_cluster([&mut leader_env, &mut follower_env1, &mut follower_env2]);

    // !first leader is killed -> election happens

    drop(leader_p);
    panic_if_election_not_done("first", follower_p1.port, follower_p2.port);

    let mut processes = vec![];
    for f in [follower_p1, follower_p2] {
        let mut handler = Client::new(f.port);
        let res = handler.send_and_get_vec("info replication", 4);
        if !res.contains(&"role:leader".to_string()) {
            processes.push(f);
            continue;
        }

        let follower_env3 =
            ServerEnv::default().with_bind_addr(f.bind_addr()).with_append_only(with_append_only);
        let new_process = spawn_server_process(&follower_env3)?;

        // WHEN
        // ! second leader is killed -> election happens
        drop(f);

        processes.push(new_process);
    }
    assert_eq!(processes.len(), 2);

    panic_if_election_not_done("second", processes[0].port, processes[1].port);

    Ok(())
}

#[test]
fn test_leader_election_twice() -> anyhow::Result<()> {
    run_leader_election_twice(true)?;

    Ok(())
}
