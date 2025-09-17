use std::{thread::sleep, time::Duration};

use crate::common::{Client, ServerEnv, spawn_server_process};

fn run_route_keys_with_num_of_keys(
    append_only: bool,
    num_keys: u32,
    duration: Duration,
) -> anyhow::Result<()> {
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
    let res = h.send_and_get_vec("KEYS *", num_keys);
    let res2 = h2.send_and_get_vec("KEYS *", num_keys);

    assert_eq!(res.len(), num_keys as usize);
    assert_eq!(res2.len(), num_keys as usize);

    // THEN Fire each key individually
    for key in 0..num_keys {
        let res = h.send_and_get(format!("GET {key}"));
        let res2 = h2.send_and_get(format!("GET {key}"));

        assert_eq!(res, res2);
        assert_eq!(res, format!("{key}"));
    }

    Ok(())
}

#[test]
fn run_route_keys() -> anyhow::Result<()> {
    run_route_keys_with_num_of_keys(true, 4, Duration::from_millis(500))?;

    Ok(())
}
