use crate::common::{Client, ServerEnv, spawn_server_process};
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

    sleep(Duration::from_millis(200));

    // THEN
    let mut results = Vec::new();
    for i in 1..100 {
        // Try to send a command to the removed node
        let response = client1.send_and_get(format!("SET {i} {i}"));
        results.push(response);
    }

    assert!(results.iter().any(|x| x.contains(&"(error) Failed to route command".to_string())));
    Ok(())
}

#[test]
fn test_removed_connection() -> anyhow::Result<()> {
    for (node_1, node_2) in [
        (ServerEnv::default(), ServerEnv::default()),
        (ServerEnv::default().with_append_only(true), ServerEnv::default().with_append_only(true)),
    ] {
        run_removed_connection(node_1, node_2)?;
    }
    Ok(())
}
