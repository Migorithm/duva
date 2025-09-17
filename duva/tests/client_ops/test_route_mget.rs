use std::{thread::sleep, time::Duration};

use crate::common::{Client, ServerEnv, spawn_server_process};

fn run_route_mget_keys(append_only: bool, num_keys: u32, duration: Duration) -> anyhow::Result<()> {
    // GIVEN
    let env = ServerEnv::default().with_append_only(append_only);
    let process = spawn_server_process(&env)?;

    let env2 = ServerEnv::default().with_append_only(append_only);
    let process2 = spawn_server_process(&env2)?;

    let mut h = Client::new(process.port);
    let mut h2 = Client::new(process2.port);

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
    let command = format!("MGET {}", joined_keys);
    let res = h.send_and_get_vec(command.clone(), num_keys);
    let res2 = h2.send_and_get_vec(command, num_keys);

    assert_eq!(res.len(), num_keys as usize);
    assert_eq!(res2.len(), num_keys as usize);

    Ok(())
}

#[test]
fn run_route_mget() -> anyhow::Result<()> {
    run_route_mget_keys(true, 4, Duration::from_millis(200))?;
    run_route_mget_keys(true, 100, Duration::from_millis(500))?;
    Ok(())
}
