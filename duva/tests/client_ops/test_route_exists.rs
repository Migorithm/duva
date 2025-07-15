use std::{thread::sleep, time::Duration};

use crate::common::{Client, ServerEnv, spawn_server_process};

fn run_route_exists_keys(
    append_only: bool,
    num_keys: u32,
    duration: Duration,
) -> anyhow::Result<()> {
    // GIVEN
    let env = ServerEnv::default().with_append_only(append_only);
    let process = spawn_server_process(&env)?;

    let env2 = ServerEnv::default().with_append_only(append_only);
    let process2 = spawn_server_process(&env2)?;

    let mut h = Client::new_with_cluster_mode(process.port, true);
    let mut h2 = Client::new_with_cluster_mode(process2.port, true);

    for key in 0..num_keys {
        if key % 2 == 0 {
            h.send_and_get(format!("SET {key} {key}"));
        }
        if key % 2 == 1 {
            h2.send_and_get(format!("SET {key} {key}"));
        }
    }

    // WHEN cluster meet
    h.send_and_get(format!("CLUSTER MEET {} eager", process2.bind_addr()));
    sleep(duration);

    // THEN Fire keys command: both handlers should get the same keys
    let joined_keys = (0..num_keys).map(|k| format!("{k}")).collect::<Vec<_>>().join(" ");
    let command = format!("EXISTS {}", joined_keys);
    let res = h.send_and_get(command.clone());
    let res2 = h2.send_and_get(command);

    assert_eq!(res, res2);
    assert_eq!(res, format!("(integer) {}", num_keys));

    Ok(())
}

#[test]
fn run_route_exists() -> anyhow::Result<()> {
    run_route_exists_keys(true, 4, Duration::from_millis(200))?;
    run_route_exists_keys(false, 4, Duration::from_millis(200))?;
    run_route_exists_keys(true, 100, Duration::from_millis(500))?;
    run_route_exists_keys(false, 100, Duration::from_millis(500))?;

    Ok(())
}
