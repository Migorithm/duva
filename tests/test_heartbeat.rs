//! This file contains tests for heartbeat between master and replica
//! Any interconnected system should have a heartbeat mechanism to ensure that the connection is still alive
//! In this case, the server will send PING message to the replica and the replica will respond with PONG message

mod common;
use common::{array, spawn_server_as_slave, spawn_server_process, wait_for_message};
use duva::client_utils::ClientStreamHandler;

#[tokio::test]
async fn test_heartbeat() {
    // GIVEN

    let process = spawn_server_process();
    let mut replica_process = spawn_server_as_slave(&process);

    //WHEN & THEN
    let mut stdout = replica_process.stdout.take().unwrap();

    wait_for_message(&mut stdout, "[INFO] Received peer state from master", 2);
}

#[tokio::test]
async fn test_heartbeat_sent_to_multiple_replicas() {
    // GIVEN
    let process = spawn_server_process();

    // WHEN
    let mut r1 = spawn_server_as_slave(&process);
    let mut r2 = spawn_server_as_slave(&process);

    let t_h1 = std::thread::spawn(move || {
        let mut stdout = r1.stdout.take().unwrap();
        wait_for_message(&mut stdout, "[INFO] Received peer state from master", 2);
    });

    let t_h2 = std::thread::spawn(move || {
        let mut stdout = r2.stdout.take().unwrap();
        wait_for_message(&mut stdout, "[INFO] Received peer state from master", 2);
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
    let mut stdout = replica_process.stdout.take().unwrap();

    wait_for_message(&mut stdout, "[INFO] Received peer state from master", 2);

    //THEN
    let mut master_stdout = master_process.stdout.take().unwrap();
    wait_for_message(&mut master_stdout, "[INFO] Received peer state from slave", 2);
}

#[tokio::test]
async fn test_slave_to_slave_heartbeat() {
    // GIVEN
    let master_process = spawn_server_process();
    let mut replica_process = spawn_server_as_slave(&master_process);
    let mut replica1_stdout = replica_process.stdout.take().unwrap();

    wait_for_message(&mut replica1_stdout, "[INFO] Received peer state from master", 1);

    // WHEN run SECOND replica
    let mut replica2_process = spawn_server_as_slave(&master_process);

    // THEN - replica1 and replica2 should send heartbeat to each other
    wait_for_message(&mut replica1_stdout, "[INFO] Received peer state from slave", 1);

    // Read stdout from the replica process
    let mut replica2_std_out = replica2_process.stdout.take().unwrap();
    wait_for_message(&mut replica2_std_out, "[INFO] Received peer state from master", 1);
    wait_for_message(&mut replica2_std_out, "[INFO] Received peer state from slave", 1);
}
