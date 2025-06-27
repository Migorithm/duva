use crate::common::{Client, ServerEnv, spawn_server_process};

fn run_keys(env: ServerEnv) -> anyhow::Result<()> {
    // GIVEN
    let process = spawn_server_process(&env)?;

    let mut h = Client::new(process.port);

    let num_keys_to_store = 2000;

    // WHEN set 500 keys with the value `bar`.
    for key in 0..num_keys_to_store {
        h.send_and_get(format!("SET {key} bar"));
    }

    // Fire keys command
    let res = h.send_and_get_vec("KEYS *", num_keys_to_store);

    assert!(res.len() >= num_keys_to_store as usize);

    Ok(())
}

#[test]
fn test_keys() -> anyhow::Result<()> {
    for env in [ServerEnv::default(), ServerEnv::default().with_append_only(true)] {
        run_keys(env)?;
    }

    Ok(())
}
