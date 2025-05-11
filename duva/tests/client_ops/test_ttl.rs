use crate::common::{Client, ServerEnv, spawn_server_process};

fn run_ttl(env: ServerEnv) -> anyhow::Result<()> {
    // GIVEN
    let process = spawn_server_process(&env, false)?;

    let mut h = Client::new(process.port);

    // WHEN - set key with expiry
    assert_eq!(h.send_and_get("SET somanyrand bar PX 5000"), "OK");
    std::thread::sleep(tokio::time::Duration::from_millis(10)); // slight delay so seconds gets floored
    let res = h.send_and_get("TTL somanyrand");

    // THEN
    assert_eq!(res, "(integer) 4");
    assert_eq!(h.send_and_get("TTL non_existing_key"), "(integer) -1");

    Ok(())
}

#[test]
fn test_ttl() -> anyhow::Result<()> {
    for env in [ServerEnv::default(), ServerEnv::default().with_append_only(true)] {
        run_ttl(env)?;
    }

    Ok(())
}
