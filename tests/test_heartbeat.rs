//! This file contains tests for heartbeat between master and replica
//! Any interconnected system should have a heartbeat mechanism to ensure that the connection is still alive
//! In this case, the server will send PING message to the replica and the replica will respond with PONG message

mod common;
use common::{get_available_port, run_server_process, wait_for_message};

#[tokio::test]
async fn test_heartbeat() {
    // GIVEN
    // run the random server on a random port

    let master_port = get_available_port();

    let mut master_process = run_server_process(master_port, None);
    let master_stdout = master_process.stdout.take();
    wait_for_message(
        master_stdout.expect("failed to take stdout"),
        format!("listening peer connection on localhost:{}...", master_port + 10000).as_str(),
        1,
    );

    let replica_port = get_available_port();
    let mut replica_process =
        run_server_process(replica_port, Some(format!("localhost:{}", master_port)));

    let mut stdout = replica_process.stdout.take();
    wait_for_message(stdout.take().unwrap(), "[INFO] Received ping from master", 2);
}

#[tokio::test]
async fn test_heartbeat_sent_to_multiple_replicas() {
    // GIVEN
    // run the random server on a random port

    let master_port = get_available_port();

    let mut master_process = run_server_process(master_port, None);
    let master_stdout = master_process.stdout.take();
    wait_for_message(
        master_stdout.expect("failed to take stdout"),
        format!("listening peer connection on localhost:{}...", master_port + 10000).as_str(),
        1,
    );

    // To prevent port race condition, we need to preallocate the ports
    let replica_port1 = 60002;
    let replica_port2 = 60003;

    // WHEN
    let mut r1 = run_server_process(replica_port1, Some(format!("localhost:{}", master_port)));

    let mut r2 = run_server_process(replica_port2, Some(format!("localhost:{}", master_port)));

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
