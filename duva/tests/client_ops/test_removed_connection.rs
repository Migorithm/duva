use crate::common::{Client, ServerEnv, spawn_server_process};
use duva::prelude::ELECTION_TIMEOUT_MAX;
use std::thread::sleep;
use std::time::Duration;

fn run_removed_connection(env1: ServerEnv, env2: ServerEnv) -> anyhow::Result<()> {
    // GIVEN
    let process1 = spawn_server_process(&env1)?;
    let process2 = spawn_server_process(&env2)?;
    let mut client1 = Client::new(process1.port);

    client1.send_and_get(format!("CLUSTER MEET {} eager", process2.bind_addr()));
    sleep(Duration::from_millis(500));

    // WHEN
    drop(process2);

    // Wait for broker to detect error, sleep through ELECTION_TIMEOUT_MAX, and attempt discovery
    sleep(Duration::from_millis(ELECTION_TIMEOUT_MAX + 1000));

    // THEN
    match client1.child.try_wait() {
        Ok(Some(status)) => {
            println!("Process exited with status: {}", status);
        },
        Ok(None) => {
            panic!("Process is still running");
        },
        Err(err) => {
            println!("Error attempting to wait: {}", err);
        },
    }
    Ok(())
}

#[test]
fn test_removed_connection() -> anyhow::Result<()> {
    for (node_1, node_2) in
        [(ServerEnv::default().with_append_only(true), ServerEnv::default().with_append_only(true))]
    {
        run_removed_connection(node_1, node_2)?;
    }
    Ok(())
}
