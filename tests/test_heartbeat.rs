//! This file contains tests for heartbeat between master and replica
//! Any interconnected system should have a heartbeat mechanism to ensure that the connection is still alive
//! In this case, the server will send PING message to the replica and the replica will respond with PONG message

mod common;

use common::{check_internodes_communication, spawn_server_as_slave, spawn_server_process};

#[tokio::test]
async fn test_heartbeat_hop_count_decreases_over_time() {
    const DEFAULT_HOP_COUNT: usize = 0;
    const TIMEOUT: u64 = 2;
    // GIVEN
    let master_p = spawn_server_process();
    let repl_p1 = spawn_server_as_slave(&master_p);
    let repl_p2 = spawn_server_as_slave(&master_p);
    let mut processes = vec![master_p, repl_p1, repl_p2];
    check_internodes_communication(&mut processes, DEFAULT_HOP_COUNT, TIMEOUT).unwrap();

    // WHEN run Third replica
    let repl_p3 = spawn_server_as_slave(processes.first().unwrap());
    processes.push(repl_p3);

    // THEN - some of the replicas will have hop_count 1 and some will have hop_count 0
    let res = check_internodes_communication(&mut processes, DEFAULT_HOP_COUNT + 1, TIMEOUT);
    assert!(res.is_ok());
}

#[tokio::test]
async fn test_heartbeat_hop_count_starts_with_0() {
    const DEFAULT_HOP_COUNT: usize = 0;
    const TIMEOUT: u64 = 2;

    // GIVEN
    let master_p = spawn_server_process();

    // WHEN
    let repl_p1 = spawn_server_as_slave(&master_p);
    let repl_p2 = spawn_server_as_slave(&master_p);

    // THEN
    let mut processes = vec![master_p, repl_p1, repl_p2];
    check_internodes_communication(&mut processes, DEFAULT_HOP_COUNT, TIMEOUT).unwrap();

    // no node should have hop_count 1
    let res = check_internodes_communication(&mut processes, DEFAULT_HOP_COUNT + 1, TIMEOUT);
    assert!(res.is_err());
}
