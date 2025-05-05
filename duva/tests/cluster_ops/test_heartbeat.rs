//! This file contains tests for heartbeat between leader and follower
//! Any interconnected system should have a heartbeat mechanism to ensure that the connection is still alive
//! In this case, the server will send PING message to the follower and the follower will respond with PONG message
use crate::common::{ServerEnv, check_internodes_communication, spawn_server_process};

async fn run_heartbeat_hop_count_decreases_over_time(with_append_only: bool) -> anyhow::Result<()> {
    const DEFAULT_HOP_COUNT: usize = 0;
    const TIMEOUT_IN_MILLIS: u128 = 2000;
    // GIVEN

    let env = ServerEnv::default().with_append_only(with_append_only);
    let mut leader_p = spawn_server_process(&env).await?;
    let leader_bind_addr = leader_p.bind_addr().clone();
    let repl_env = ServerEnv::default()
        .with_leader_bind_addr(leader_bind_addr.clone())
        .with_append_only(with_append_only);
    let mut follower_p1 = spawn_server_process(&repl_env).await?;

    let repl_env2 = ServerEnv::default()
        .with_leader_bind_addr(leader_bind_addr.clone())
        .with_append_only(with_append_only);
    let mut follower_p2 = spawn_server_process(&repl_env2).await?;
    let mut processes = vec![&mut leader_p, &mut follower_p1, &mut follower_p2];

    check_internodes_communication(&mut processes, DEFAULT_HOP_COUNT, TIMEOUT_IN_MILLIS)
        .await
        .unwrap();

    // WHEN run Third follower
    let repl_env3 = ServerEnv::default()
        .with_leader_bind_addr(leader_bind_addr.clone())
        .with_append_only(with_append_only);
    let mut follower_p3 = spawn_server_process(&repl_env3).await?;
    processes.push(&mut follower_p3);

    // THEN - some of the followers will have hop_count 1 and some will have hop_count 0
    let res =
        check_internodes_communication(&mut processes, DEFAULT_HOP_COUNT + 1, TIMEOUT_IN_MILLIS)
            .await;
    assert!(res.is_ok());

    Ok(())
}

async fn run_heartbeat_hop_count_starts_with_0(with_append_only: bool) -> anyhow::Result<()> {
    const DEFAULT_HOP_COUNT: usize = 0;
    const TIMEOUT_IN_MILLIS: u128 = 2000;

    // GIVEN
    let env = ServerEnv::default().with_append_only(with_append_only);
    let mut leader_p = spawn_server_process(&env).await?;

    // WHEN
    let repl_env = ServerEnv::default()
        .with_leader_bind_addr(leader_p.bind_addr())
        .with_append_only(with_append_only);
    let mut follower_p1 = spawn_server_process(&repl_env).await?;
    let repl_env2 = ServerEnv::default()
        .with_leader_bind_addr(leader_p.bind_addr())
        .with_append_only(with_append_only);
    let mut follower_p2 = spawn_server_process(&repl_env2).await?;

    let processes = &mut [&mut leader_p, &mut follower_p1, &mut follower_p2];
    // THEN

    check_internodes_communication(processes, DEFAULT_HOP_COUNT, TIMEOUT_IN_MILLIS).await?;

    // no node should have hop_count 1
    let res =
        check_internodes_communication(processes, DEFAULT_HOP_COUNT + 1, TIMEOUT_IN_MILLIS).await;
    assert!(res.is_err());

    Ok(())
}

#[tokio::test]
async fn test_heartbeat_hop_count_decreases_over_time() -> anyhow::Result<()> {
    run_heartbeat_hop_count_decreases_over_time(false).await?;
    run_heartbeat_hop_count_decreases_over_time(true).await?;

    Ok(())
}

#[tokio::test]
async fn test_heartbeat_hop_count_starts_with_0() -> anyhow::Result<()> {
    run_heartbeat_hop_count_starts_with_0(false).await?;
    run_heartbeat_hop_count_starts_with_0(true).await?;

    Ok(())
}
