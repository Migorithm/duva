//! This file contains tests for heartbeat between leader and follower
//! Any interconnected system should have a heartbeat mechanism to ensure that the connection is still alive
//! In this case, the server will send PING message to the follower and the follower will respond with PONG message

use crate::common::{
    ServerEnv, check_internodes_communication, form_cluster, spawn_server_process,
};

fn run_heartbeat_hop_count_decreases_over_time(with_append_only: bool) -> anyhow::Result<()> {
    const DEFAULT_HOP_COUNT: usize = 0;
    const TIMEOUT_IN_MILLIS: u128 = 2000;
    // GIVEN
    let mut env = ServerEnv::default().with_append_only(with_append_only);
    let mut repl_env = ServerEnv::default().with_append_only(with_append_only);
    let mut repl_env2 = ServerEnv::default().with_append_only(with_append_only);

    // Form cluster with leader and replica
    let cluster = form_cluster([&mut env, &mut repl_env, &mut repl_env2], true);

    // WHEN run third follower
    let repl_env3 = ServerEnv::default()
        .with_bind_addr(cluster[0].bind_addr())
        .with_append_only(with_append_only);
    let follower_p3 = spawn_server_process(&repl_env3, true)?;

    let mut cluster: Vec<_> = cluster.into();
    cluster.push(follower_p3);

    // THEN - some of the followers will have hop_count 1 and some will have hop_count 0
    let res = check_internodes_communication(
        &mut cluster.iter_mut().collect::<Vec<_>>(),
        DEFAULT_HOP_COUNT + 1,
        TIMEOUT_IN_MILLIS,
    );
    assert!(res.is_ok());

    Ok(())
}

fn run_heartbeat_hop_count_starts_with_0(with_append_only: bool) -> anyhow::Result<()> {
    const DEFAULT_HOP_COUNT: usize = 0;
    const TIMEOUT_IN_MILLIS: u128 = 2000;

    // GIVEN
    let mut env = ServerEnv::default().with_append_only(with_append_only);
    let mut repl_env = ServerEnv::default().with_append_only(with_append_only);
    let mut repl_env2 = ServerEnv::default().with_append_only(with_append_only);

    // WHEN
    // Form cluster with leader and replica
    let mut cluster = form_cluster([&mut env, &mut repl_env, &mut repl_env2], true);

    // THEN
    // ! no node should have hop_count 1
    let res = check_internodes_communication(
        &mut cluster.iter_mut().collect::<Vec<_>>().as_mut(),
        DEFAULT_HOP_COUNT + 1,
        TIMEOUT_IN_MILLIS,
    );
    assert!(res.is_err());

    Ok(())
}

#[test]
fn test_heartbeat_hop_count_decreases_over_time() -> anyhow::Result<()> {
    run_heartbeat_hop_count_decreases_over_time(false)?;
    run_heartbeat_hop_count_decreases_over_time(true)?;

    Ok(())
}

#[test]
fn test_heartbeat_hop_count_starts_with_0() -> anyhow::Result<()> {
    run_heartbeat_hop_count_starts_with_0(false)?;
    run_heartbeat_hop_count_starts_with_0(true)?;
    Ok(())
}
