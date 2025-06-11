use crate::common::{Client, ServerEnv, spawn_server_process};

fn run_type_string() -> anyhow::Result<()> {
    // GIVEN
    let env = ServerEnv::default();
    let process = spawn_server_process(&env)?;

    let mut h = Client::new(process.port);

    // WHEN - set key with expiry
    assert_eq!(h.send_and_get("SET key1 1"), "OK");

    // THEN
    let res = h.send_and_get("TYPE key1");
    assert_eq!(res, "string");

    Ok(())
}

fn run_type_none() -> anyhow::Result<()> {
    // GIVEN
    let env = ServerEnv::default();
    let process = spawn_server_process(&env)?;

    let mut h = Client::new(process.port);

    // WHEN - set key with expiry

    // THEN
    let res = h.send_and_get("TYPE key");
    assert_eq!(res, "none");

    Ok(())
}

#[test]
fn test_type() -> anyhow::Result<()> {
    run_type_string()?;
    run_type_none()?;

    Ok(())
}
