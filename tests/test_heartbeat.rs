//! This file contains tests for heartbeat between master and replica
//! Any interconnected system should have a heartbeat mechanism to ensure that the connection is still alive
//! In this case, the server will send PING message to the replica and the replica will respond with PONG message

mod common;
use common::{spawn_server_as_slave, spawn_server_process, wait_for_message};

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
    // GIVEN
    let master_process = spawn_server_process();

    let mut repl_p1 = spawn_server_as_slave(&master_process);
    let mut repl1_stdout = repl_p1.stdout.take().unwrap();

    wait_for_message(&mut repl1_stdout, "[INFO] from master rh:0", 1);
    let mut repl_p2 = spawn_server_as_slave(&master_process);
    wait_for_message(&mut repl1_stdout, "[INFO] from replica rh:0", 1);

    let mut repl2_stdout = repl_p2.stdout.take().unwrap();
    wait_for_message(&mut repl2_stdout, "[INFO] from replica rh:0", 1);

    // WHEN run Third replica
    let mut repl_p3 = spawn_server_as_slave(&master_process);
    let mut repl3_stdout = repl_p3.stdout.take().unwrap();

    // THEN - hop_count_starts_from 1
    wait_for_message(&mut repl1_stdout, "[INFO] from master rh:1", 1);
    wait_for_message(&mut repl2_stdout, "[INFO] from replica rh:1", 1);
    wait_for_message(&mut repl3_stdout, "[INFO] from replica rh:1", 1);
}

#[tokio::test]
async fn test_heartbeat_hop_count_decreases_over_time() {
    // GIVEN
    let master_process = spawn_server_process();

    let mut repl_p1 = spawn_server_as_slave(&master_process);
    let mut repl1_stdout = repl_p1.stdout.take().unwrap();

    wait_for_message(&mut repl1_stdout, "[INFO] from master rh:0", 1);
    let mut repl_p2 = spawn_server_as_slave(&master_process);
    wait_for_message(&mut repl1_stdout, "[INFO] from replica rh:0", 1);

    let mut repl2_stdout = repl_p2.stdout.take().unwrap();
    wait_for_message(&mut repl2_stdout, "[INFO] from replica rh:0", 1);

    // WHEN run Third replica
    let mut repl_p3 = spawn_server_as_slave(&master_process);
    let mut repl3_stdout = repl_p3.stdout.take().unwrap();

    // THEN - some of the replicas will have hop_count 1 and some will have hop_count 0
    wait_for_message(&mut repl1_stdout, "[INFO] from master rh:1", 1);
    wait_for_message(&mut repl1_stdout, "[INFO] from master rh:0", 1);

    wait_for_message(&mut repl2_stdout, "[INFO] from replica rh:0", 1);
    wait_for_message(&mut repl2_stdout, "[INFO] from replica rh:1", 1);

    wait_for_message(&mut repl3_stdout, "[INFO] from replica rh:0", 1);
    wait_for_message(&mut repl3_stdout, "[INFO] from replica rh:1", 1);
}
