//! This file contains tests for heartbeat between master and replica
//! Any interconnected system should have a heartbeat mechanism to ensure that the connection is still alive
//! In this case, the server will send PING message to the replica and the replica will respond with PONG message

mod common;
use common::{check_cross_heartbeat, spawn_server_as_slave, spawn_server_process};

#[tokio::test]
async fn test_heartbeat() {
    // GIVEN
    let master_process = spawn_server_process();
    let mut replica_process = spawn_server_as_slave(&master_process);

    //WHEN & THEN
    replica_process.wait_for_message(&master_process.heartbeat_msg(0), 2);
}

#[tokio::test]
async fn test_heartbeat_sent_to_multiple_replicas() {
    // GIVEN
    let master_p = spawn_server_process();
    let message = master_p.heartbeat_msg(0);

    // WHEN
    let mut r1 = spawn_server_as_slave(&master_p);
    let mut r2 = spawn_server_as_slave(&master_p);

    let t_h1 = std::thread::spawn({
        let message = message.clone();
        move || {
            r1.wait_for_message(&message, 2);
        }
    });

    let t_h2 = std::thread::spawn(move || {
        r2.wait_for_message(&message, 2);
    });

    //Then it should finish
    t_h1.join().unwrap();
    t_h2.join().unwrap();
}

#[tokio::test]
async fn test_master_slave_both_send_heartbeats() {
    // GIVEN
    let mut master_process = spawn_server_process();
    let mut replica_process = spawn_server_as_slave(&master_process);

    //WHEN
    replica_process.wait_for_message(&master_process.heartbeat_msg(0), 2);

    //THEN
    master_process.wait_for_message(&replica_process.heartbeat_msg(0), 2);
}

#[tokio::test]
async fn test_slave_to_slave_heartbeat() {
    const DEFAULT_HOP_COUNT: usize = 0;
    // GIVEN
    let master_p = spawn_server_process();
    let mut repl_p1 = spawn_server_as_slave(&master_p);
    repl_p1.wait_for_message(&master_p.heartbeat_msg(DEFAULT_HOP_COUNT), 1);

    // WHEN run SECOND replica
    let mut repl_p2 = spawn_server_as_slave(&master_p);

    // THEN - replica1 and replica2 should send heartbeat to each other
    repl_p1.wait_for_message(&repl_p2.heartbeat_msg(DEFAULT_HOP_COUNT), 1);

    // Read stdout from the replica process
    repl_p2.wait_for_message(&master_p.heartbeat_msg(DEFAULT_HOP_COUNT), 1);
    repl_p2.wait_for_message(&repl_p1.heartbeat_msg(DEFAULT_HOP_COUNT), 1);
}

#[tokio::test]
async fn test_heartbeat_hop_count() {
    const DEFAULT_HOP_COUNT: usize = 0;

    // GIVEN

    let mut master_p = spawn_server_process();

    let mut repl_p1 = spawn_server_as_slave(&master_p);
    let mut repl_p2 = spawn_server_as_slave(&master_p);

    check_cross_heartbeat(&mut [&mut master_p, &mut repl_p1, &mut repl_p2], DEFAULT_HOP_COUNT);

    // WHEN run Third replica
    let mut repl_p3 = spawn_server_as_slave(&master_p);

    // THEN - hop_count_starts_from 1

    repl_p3.wait_for_message(&repl_p1.heartbeat_msg(DEFAULT_HOP_COUNT + 1), 1);
    repl_p1.wait_for_message(&master_p.heartbeat_msg(DEFAULT_HOP_COUNT + 1), 1);
    repl_p2.wait_for_message(&repl_p1.heartbeat_msg(DEFAULT_HOP_COUNT + 1), 1);
    master_p.wait_for_message(&repl_p1.heartbeat_msg(DEFAULT_HOP_COUNT + 1), 1);

    //TODO - the following must pass!
    // cross_heartbeat(
    //     &mut [&mut master_p, &mut repl_p1, &mut repl_p2, &mut repl_p3],
    //     DEFAULT_HOP_COUNT + 1,
    // );
}

#[tokio::test]
async fn test_heartbeat_hop_count_decreases_over_time() {
    const DEFAULT_HOP_COUNT: usize = 0;
    // GIVEN
    let mut master_p = spawn_server_process();

    let mut repl_p1 = spawn_server_as_slave(&master_p);

    let mut repl_p2 = spawn_server_as_slave(&master_p);

    check_cross_heartbeat(&mut [&mut master_p, &mut repl_p1, &mut repl_p2], DEFAULT_HOP_COUNT);

    // WHEN run Third replica
    let mut repl_p3 = spawn_server_as_slave(&master_p);

    // THEN - some of the replicas will have hop_count 1 and some will have hop_count 0
    //TODO replace with cross_heartbeat
    repl_p1.wait_for_message(&master_p.heartbeat_msg(DEFAULT_HOP_COUNT + 1), 1);
    repl_p1.wait_for_message(&master_p.heartbeat_msg(DEFAULT_HOP_COUNT), 1);

    repl_p2.wait_for_message(&repl_p1.heartbeat_msg(DEFAULT_HOP_COUNT), 1);
    repl_p2.wait_for_message(&repl_p1.heartbeat_msg(DEFAULT_HOP_COUNT + 1), 1);

    repl_p3.wait_for_message(&repl_p1.heartbeat_msg(DEFAULT_HOP_COUNT), 1);
    repl_p3.wait_for_message(&repl_p1.heartbeat_msg(DEFAULT_HOP_COUNT + 1), 1);
}
