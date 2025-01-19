//! This file contains tests for heartbeat between master and replica
//! Any interconnected system should have a heartbeat mechanism to ensure that the connection is still alive
//! In this case, the server will send PING message to the replica and the replica will respond with PONG message

mod common;
use common::{spawn_server_as_slave, spawn_server_process, wait_for_message};

#[tokio::test]
async fn test_heartbeat() {
    // GIVEN

    let process = spawn_server_process();
    let mut replica_process = spawn_server_as_slave(&process);

    //WHEN & THEN
    let mut stdout = replica_process.stdout.take();

    wait_for_message(stdout.take().unwrap(), "[INFO] Received ping from master", 2);
}

#[tokio::test]
async fn test_heartbeat_sent_to_multiple_replicas() {
    // GIVEN
    let process = spawn_server_process();

    // WHEN
    let mut r1 = spawn_server_as_slave(&process);
    let mut r2 = spawn_server_as_slave(&process);

    let t_h1 = std::thread::spawn(move || {
        let mut stdout = r1.stdout.take();
        wait_for_message(stdout.take().unwrap(), "[INFO] Received ping from master", 2);
    });

    let t_h2 = std::thread::spawn(move || {
        let mut stdout = r2.stdout.take();
        wait_for_message(stdout.take().unwrap(), "[INFO] Received ping from master", 2);
    });

    //Then it should finish
    t_h1.join().unwrap();
    t_h2.join().unwrap();
}

#[tokio::test]
async fn test_heartbeat_master_receives_slave_heartbeat() {
    // GIVEN
    let mut master_process = spawn_server_process();
    let mut replica_process = spawn_server_as_slave(&master_process);

    //WHEN
    let mut stdout = replica_process.stdout.take();

    wait_for_message(stdout.take().unwrap(), "[INFO] Received ping from master", 2);

    //THEN
    wait_for_message(master_process.stdout.take().unwrap(), "[INFO] Received ping from slave", 2);
}
