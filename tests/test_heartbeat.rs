//! This file contains tests for heartbeat between leader and follower
//! Any interconnected system should have a heartbeat mechanism to ensure that the connection is still alive
//! In this case, the server will send PING message to the follower and the follower will respond with PONG message

mod common;

use common::{check_internodes_communication, spawn_server_as_follower, spawn_server_process};

#[tokio::test]
async fn test_heartbeat_hop_count_decreases_over_time() {
    const DEFAULT_HOP_COUNT: usize = 0;
    const TIMEOUT: u64 = 2;
    // GIVEN
    let mut leader_p = spawn_server_process();
    let leader_bind_addr = leader_p.bind_addr();
    let mut follower_p1 = spawn_server_as_follower(leader_bind_addr.clone());
    let mut follower_p2 = spawn_server_as_follower(leader_bind_addr.clone());
    let mut processes = vec![&mut leader_p, &mut follower_p1, &mut follower_p2];

    check_internodes_communication(&mut processes, DEFAULT_HOP_COUNT, TIMEOUT).unwrap();

    // WHEN run Third follower
    let mut follower_p3 = spawn_server_as_follower(leader_bind_addr);
    processes.push(&mut follower_p3);

    // THEN - some of the followers will have hop_count 1 and some will have hop_count 0
    let res = check_internodes_communication(&mut processes, DEFAULT_HOP_COUNT + 1, TIMEOUT);
    assert!(res.is_ok());
}

#[tokio::test]
async fn test_heartbeat_hop_count_starts_with_0() {
    const DEFAULT_HOP_COUNT: usize = 0;
    const TIMEOUT: u64 = 2;

    // GIVEN
    let mut leader_p = spawn_server_process();
    let leader_bind_addr = leader_p.bind_addr();
    // WHEN
    let mut follower_p1 = spawn_server_as_follower(leader_bind_addr.clone());
    let mut follower_p2 = spawn_server_as_follower(leader_bind_addr.clone());

    let processes = &mut [&mut leader_p, &mut follower_p1, &mut follower_p2];
    // THEN

    check_internodes_communication(processes, DEFAULT_HOP_COUNT, TIMEOUT).unwrap();

    // no node should have hop_count 1
    let res = check_internodes_communication(processes, DEFAULT_HOP_COUNT + 1, TIMEOUT);
    assert!(res.is_err());
}
